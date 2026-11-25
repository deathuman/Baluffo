"""Discovery service discovery service watch.

AI boundary owns: bridge-owned discovery task launch, config persistence, and auto-sync watch behavior.
AI boundary implement in: this discovery_service_watch.py leaf.
AI boundary search before contracts: discovery routes, task launch API, source discovery config, and admin discovery frontend callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused discovery service tests.
"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any

from src.bridge.active_task_snapshot import upsert_snapshot_rows
from src.bridge.discovery_service_core import DiscoveryServiceState
from src.bridge.lifecycle_cleanup import DISCOVERY_STALL_CAP_SECONDS
from src.bridge.task_abort_evidence import normalize_owner_abort_result, row_abort_requested
from src.shared.json_shapes import as_json_object

DISCOVERY_WATCH_RETRY_S = 1.0
# The snapshot is a 45s polling cache, not the liveness lease: refresh it well
# inside that window instead of rewriting it on every 0.8s watch tick.
DISCOVERY_SNAPSHOT_REFRESH_S = 30.0
_DISCOVERY_WATCH_ERRORS = (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError)


def _discovery_progress_signature(report: dict[str, Any], *, run_matches: bool) -> str:
    """Return a digest of the report's progress counters, or "" when it is not ours.

    Only ``taskProgress.counts`` is hashed. That object is the progress contract:
    counters and stage keys that move when the worker advances, never a timestamp.
    Hashing the whole report would let any new cosmetic or timestamped field
    refresh the heartbeat on the worker's write cadence, silently defeating the
    stall cap; here an unlisted field simply cannot count as progress.
    """
    if not run_matches:
        return ""
    progress = as_json_object(report.get("taskProgress"))
    return json.dumps(
        as_json_object(progress.get("counts")),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _progress_idle_seconds(since: str, now: str, *, parse_iso: Any) -> float:
    since_dt = parse_iso(since)
    now_dt = parse_iso(now)
    if since_dt is None or now_dt is None:
        return 0.0
    try:
        return float(max(0.0, (now_dt - since_dt).total_seconds()))
    except TypeError:
        # Mixed naive/aware timestamps cannot be compared; do not invent a stall.
        return 0.0


class DiscoveryServiceWatchMixin(DiscoveryServiceState):
    def _discovery_progress_view(
        self, *, report: dict[str, Any], run_id: str, started_at: str
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        """Return (stage, progress, summary) for this run's report, else empties."""
        if not self._discovery_report_matches_run(
            report=report, run_id=run_id, started_at=started_at
        ):
            return "", {}, {}
        progress = as_json_object(report.get("taskProgress"))
        summary = as_json_object(report.get("summary"))
        stage = str(
            summary.get("currentStageKey") or summary.get("phaseKey") or summary.get("phase") or ""
        ).strip()
        return stage, progress, summary

    def _refresh_discovery_task_heartbeat(
        self, *, run_id: str, started_at: str, report: dict[str, Any] | None = None
    ) -> None:
        now = self._deps.now_iso()
        if report is None:
            report = self._deps.normalize_discovery_report_contract(self._read_discovery_report())
        stage, progress, summary = self._discovery_progress_view(
            report=report, run_id=run_id, started_at=started_at
        )
        self._deps.heartbeat_lifecycle_run(
            run_id,
            "discovery",
            heartbeat_at=now,
            stage=stage or "running",
            progress=progress or None,
            summary=summary or None,
        )

    def _refresh_discovery_snapshot_if_due(
        self,
        *,
        run_id: str,
        started_at: str,
        report: dict[str, Any],
        now: str,
        last_snapshot_at: str,
        progressed: bool,
    ) -> str:
        """Refresh the hot polling snapshot on progress and every 30s while quiet.

        The snapshot is a 45s polling cache, not the liveness lease: a quiet
        discovery phase keeps the admin UI on the fast path without refreshing
        the lifecycle row the reaper judges. A stalled row stops refreshing it,
        so the admin falls back to the lifecycle projection that says the run is
        stalled.
        """
        if not progressed and (
            _progress_idle_seconds(last_snapshot_at, now, parse_iso=self._deps.parse_iso)
            < DISCOVERY_SNAPSHOT_REFRESH_S
        ):
            return last_snapshot_at
        if self._paths.active_task_snapshot is not None:
            stage, progress, summary = self._discovery_progress_view(
                report=report, run_id=run_id, started_at=started_at
            )
            upsert_snapshot_rows(
                self._paths.active_task_snapshot,
                [
                    {
                        "taskType": "discovery",
                        "type": "discovery",
                        "runId": run_id,
                        "id": run_id,
                        "active": True,
                        "status": "running",
                        "startedAt": started_at,
                        "heartbeatAt": now,
                        "finishedAt": "",
                        "stage": stage or "running",
                        "taskProgress": progress,
                        "summary": summary,
                        "outputs": {"report": str(self._paths.report)},
                    }
                ],
                snapshot_at=now,
            )
        return now

    def _discovery_report_matches_run(
        self, *, report: dict[str, Any], run_id: str, started_at: str
    ) -> bool:
        report_run_id = str(report.get("runId") or "").strip()
        report_started_at = str(report.get("startedAt") or "").strip()
        report_started_dt = self._deps.parse_iso(report_started_at)
        started_dt = self._deps.parse_iso(started_at)
        return bool(
            report_run_id == run_id
            and (
                not report_started_at
                or not started_dt
                or not report_started_dt
                or report_started_dt >= started_dt
            )
        )

    def _abort_stalled_discovery_owner(self, *, run_id: str, pid: int) -> dict[str, Any]:
        """Kill the still-live owner so the failed row has no second writer.

        The pid check only proves the process exists. A worker that hung keeps
        its pid after the row fails, so it can rewrite the repaired report or
        race a new run over the same report and registry files. Terminating the
        registered child before the terminal writes makes them the last word.
        """
        abort = self._deps.abort_discovery_owner
        if not callable(abort):
            self._deps.bridge_log(
                "warn", "discovery_stalled_owner_abort_unavailable", runId=run_id, pid=pid
            )
            return {"exited": False, "warnings": ["abort_unavailable"]}
        try:
            normalized = normalize_owner_abort_result(abort(run_id))
        except Exception as exc:
            # A failed kill must not keep the row open: the watcher still owns
            # the terminal write, and the failure is recorded on the row.
            self._deps.bridge_log(
                "warn",
                "discovery_stalled_owner_abort_failed",
                runId=run_id,
                pid=pid,
                error=f"{type(exc).__name__}: {exc}",
            )
            return {"exited": False, "warnings": ["abort_raised"]}
        self._deps.bridge_log(
            "warn",
            "discovery_stalled_owner_abort",
            runId=run_id,
            pid=pid,
            exited=normalized["exited"],
            warnings=normalized["warnings"],
        )
        return normalized

    def _stalled_discovery_summary(
        self,
        *,
        report: dict[str, Any],
        stalled_at: str,
        last_progress_at: str,
        owner_abort: dict[str, Any],
    ) -> dict[str, Any]:
        idle_seconds = int(
            _progress_idle_seconds(last_progress_at, stalled_at, parse_iso=self._deps.parse_iso)
        )
        return {
            **as_json_object(report.get("summary")),
            "error": "discovery_progress_stalled",
            "stalledForSeconds": idle_seconds,
            "lastProgressAt": last_progress_at,
            "ownerAbort": owner_abort,
        }

    def _mark_stalled_discovery_run(
        self,
        *,
        run_id: str,
        report: dict[str, Any],
        stalled_at: str,
        last_progress_at: str,
        owner_abort: dict[str, Any],
    ) -> None:
        """Keep the row active but stalled while its owner may still be writing.

        Failing the row here would admit a new discovery run against the same
        report and registry files while the old worker is alive. The row stays
        non-terminal with `stage: "stalled"` and a heartbeat pinned to the last
        progress time, so `trigger_discovery_task` still refuses a duplicate
        start and the reaper's lease can orphan the row if the owner never exits.
        """
        progress = as_json_object(report.get("taskProgress"))
        summary = self._stalled_discovery_summary(
            report=report,
            stalled_at=stalled_at,
            last_progress_at=last_progress_at,
            owner_abort=owner_abort,
        )
        self._deps.heartbeat_lifecycle_run(
            run_id,
            "discovery",
            heartbeat_at=last_progress_at,
            stage="stalled",
            progress=progress,
            summary=summary,
        )
        if self._paths.active_task_snapshot is not None:
            upsert_snapshot_rows(
                self._paths.active_task_snapshot,
                [
                    {
                        "taskType": "discovery",
                        "type": "discovery",
                        "runId": run_id,
                        "id": run_id,
                        "active": True,
                        "status": "running",
                        "startedAt": str(report.get("startedAt") or ""),
                        "heartbeatAt": last_progress_at,
                        "finishedAt": "",
                        "stage": "stalled",
                        "taskProgress": {**progress, "updatedAt": last_progress_at},
                        "summary": summary,
                        "outputs": {"report": str(self._paths.report)},
                    }
                ],
                snapshot_at=stalled_at,
            )
        self._deps.bridge_log(
            "warn",
            "discovery_task_stall_unresolved",
            runId=run_id,
            stalledForSeconds=summary["stalledForSeconds"],
            capSeconds=int(DISCOVERY_STALL_CAP_SECONDS),
            lastProgressAt=last_progress_at,
            ownerAbort=owner_abort,
        )

    def _fail_stalled_discovery_run(
        self,
        *,
        run_id: str,
        pid: int,
        report: dict[str, Any],
        stalled_at: str,
        last_progress_at: str,
        owner_abort: dict[str, Any],
    ) -> None:
        """Fail a hung discovery row once its owner is gone.

        The owner is aborted first; this closes the row with
        `terminalReason: "stalled"` and repairs the report so the run reads as
        failed everywhere. It only runs once the owner exited (or its pid is
        gone), because a terminal row admits a new run against the same report
        and registry files.
        """
        progress = as_json_object(report.get("taskProgress"))
        summary = self._stalled_discovery_summary(
            report=report,
            stalled_at=stalled_at,
            last_progress_at=last_progress_at,
            owner_abort=owner_abort,
        )
        self._deps.fail_lifecycle_run(
            run_id,
            "discovery",
            finished_at=stalled_at,
            terminal_reason="stalled",
            summary=dict(summary),
            progress=dict(progress),
        )
        self._repair_terminal_discovery_report_from_row(
            {
                "runId": run_id,
                "taskType": "discovery",
                "status": "failed",
                "finishedAt": stalled_at,
                "terminalReason": "stalled",
                "summary": summary,
                "taskProgress": progress,
            },
            report,
            finished_at=stalled_at,
        )
        if self._paths.active_task_snapshot is not None:
            upsert_snapshot_rows(
                self._paths.active_task_snapshot,
                [
                    {
                        "taskType": "discovery",
                        "type": "discovery",
                        "runId": run_id,
                        "id": run_id,
                        "active": False,
                        "status": "error",
                        "startedAt": str(report.get("startedAt") or ""),
                        "heartbeatAt": stalled_at,
                        "finishedAt": stalled_at,
                        "stage": "stalled",
                        "terminalReason": "stalled",
                        "taskProgress": {**progress, "active": False, "updatedAt": stalled_at},
                        "summary": summary,
                        "outputs": {"report": str(self._paths.report)},
                    }
                ],
                snapshot_at=stalled_at,
            )
        self._deps.bridge_log(
            "warn",
            "discovery_task_stalled",
            runId=run_id,
            pid=pid,
            stalledForSeconds=summary["stalledForSeconds"],
            capSeconds=int(DISCOVERY_STALL_CAP_SECONDS),
            lastProgressAt=last_progress_at,
            ownerAbort=owner_abort,
        )

    def _discovery_report_finished_since(self, report: dict[str, Any], started_dt: Any) -> bool:
        finished_at = str(report.get("finishedAt") or "")
        finished_dt = self._deps.parse_iso(finished_at)
        return bool(finished_dt and finished_dt >= started_dt)

    @staticmethod
    def _abort_reason_from_row(row: dict[str, Any] | None) -> str:
        summary = (row or {}).get("summary")
        return str((summary if isinstance(summary, dict) else {}).get("abortReason") or "")

    def _log_lifecycle_watch_retry(self, *, run_id: str, stage: str, error: Exception) -> None:
        try:
            self._deps.bridge_log(
                "warn",
                "task_lifecycle_watch_retry",
                taskType="discovery",
                runId=run_id,
                stage=stage,
                error=str(error),
                retryInMs=int(DISCOVERY_WATCH_RETRY_S * 1000),
            )
        except _DISCOVERY_WATCH_ERRORS:
            return

    def _closeout_dead_discovery_owner(
        self,
        *,
        run_id: str,
        report: dict[str, Any],
        lifecycle_row: dict[str, Any] | None,
    ) -> None:
        """Close the row of an owner that is no longer running."""
        if row_abort_requested(lifecycle_row):
            self._cancel_discovery_run(
                run_id=run_id,
                reason=self._abort_reason_from_row(lifecycle_row),
            )
            return
        failed_at = self._deps.now_iso()
        self._deps.fail_lifecycle_run(
            run_id,
            "discovery",
            finished_at=failed_at,
            terminal_reason="owner_inactive_without_terminal_report",
            summary={"error": "owner_inactive_without_terminal_report"},
        )
        self._repair_terminal_discovery_report_from_row(
            {
                "runId": run_id,
                "taskType": "discovery",
                "status": "failed",
                "finishedAt": failed_at,
                "terminalReason": "owner_inactive_without_terminal_report",
                "summary": {"error": "owner_inactive_without_terminal_report"},
            },
            report,
            finished_at=failed_at,
        )

    def _stalled_discovery_watch_done(
        self,
        *,
        run_id: str,
        pid: int,
        report: dict[str, Any],
        lifecycle_row: dict[str, Any] | None,
        stall_state: dict[str, Any],
    ) -> bool:
        """Return True when a stalled run no longer needs this watcher.

        The reaper may close the row while the owner is unreachable; the watcher
        must not write over that. Once the owner is gone, fail the row as stalled.
        """
        if self._lifecycle_row_is_terminal(lifecycle_row):
            return True
        if self._deps.pid_is_running(pid):
            return False
        self._fail_stalled_discovery_run(
            run_id=run_id,
            pid=pid,
            report=report,
            stalled_at=str(self._deps.now_iso() or ""),
            last_progress_at=stall_state["last_progress_at"],
            owner_abort=stall_state["owner_abort"],
        )
        return True

    def _start_stall_closeout(
        self,
        *,
        run_id: str,
        pid: int,
        report: dict[str, Any],
        now: str,
        last_progress_at: str,
    ) -> dict[str, Any] | None:
        """Abort a hung owner; return stall state while the row stays active.

        ``None`` means the owner is gone and the row was failed as stalled.
        """
        owner_abort = self._abort_stalled_discovery_owner(run_id=run_id, pid=pid)
        if owner_abort.get("exited") or not self._deps.pid_is_running(pid):
            self._fail_stalled_discovery_run(
                run_id=run_id,
                pid=pid,
                report=report,
                stalled_at=now,
                last_progress_at=last_progress_at,
                owner_abort=owner_abort,
            )
            return None
        self._mark_stalled_discovery_run(
            run_id=run_id,
            report=report,
            stalled_at=now,
            last_progress_at=last_progress_at,
            owner_abort=owner_abort,
        )
        return {"last_progress_at": last_progress_at, "owner_abort": owner_abort}

    def _wait_for_discovery_watch_report(
        self,
        *,
        run_id: str,
        pid: int,
        started_at: str,
        started_dt: Any,
    ) -> dict[str, Any] | None:
        """Watch a live discovery owner until it reports, dies, or stalls.

        The row's heartbeat is refreshed only when the report's work counters
        advance. A live pid proves the process exists, not that it is working: a
        hung worker keeps its pid but stops moving counters, so its heartbeat goes
        cold and the watcher acts once progress has been idle for
        `DISCOVERY_STALL_CAP_SECONDS`. It aborts the owner; the row is failed only
        once that owner is gone, and otherwise stays active-but-stalled so no new
        run can start against the same report and registry files.
        """
        last_progress_signature = ""
        last_progress_at = str(self._deps.now_iso() or "")
        last_snapshot_at = last_progress_at
        # Set when the stall abort could not confirm the owner exited; the row is
        # then left active-but-stalled instead of terminalized.
        stall_state: dict[str, Any] | None = None
        while True:
            stage = "watch"
            try:
                lifecycle_row = self._deps.get_lifecycle_row(run_id, "discovery")
                report = self._deps.normalize_discovery_report_contract(
                    self._read_discovery_report()
                )
                if self._discovery_report_finished_since(
                    report, started_dt
                ) and self._discovery_report_finalization_settled(report):
                    return report
                if stall_state is not None:
                    if self._stalled_discovery_watch_done(
                        run_id=run_id,
                        pid=pid,
                        report=report,
                        lifecycle_row=lifecycle_row,
                        stall_state=stall_state,
                    ):
                        return None
                    # The owner survived the abort. Skip the heartbeat so the
                    # reaper's lease stays armed on the last progress time.
                    time.sleep(0.8)
                    continue
                if not self._deps.pid_is_running(pid):
                    stage = "terminal_closeout"
                    self._closeout_dead_discovery_owner(
                        run_id=run_id, report=report, lifecycle_row=lifecycle_row
                    )
                    return None
                stage = "heartbeat"
                now = str(self._deps.now_iso() or "")
                progress_signature = _discovery_progress_signature(
                    report,
                    run_matches=self._discovery_report_matches_run(
                        report=report, run_id=run_id, started_at=started_at
                    ),
                )
                progressed = progress_signature != last_progress_signature
                if progressed:
                    self._refresh_discovery_task_heartbeat(
                        run_id=run_id, started_at=started_at, report=report
                    )
                    last_progress_at = now
                last_progress_signature = progress_signature
                last_snapshot_at = self._refresh_discovery_snapshot_if_due(
                    run_id=run_id,
                    started_at=started_at,
                    report=report,
                    now=now,
                    last_snapshot_at=last_snapshot_at,
                    progressed=progressed,
                )
                if (
                    _progress_idle_seconds(last_progress_at, now, parse_iso=self._deps.parse_iso)
                    >= DISCOVERY_STALL_CAP_SECONDS
                ):
                    stage = "stall_closeout"
                    stall_state = self._start_stall_closeout(
                        run_id=run_id,
                        pid=pid,
                        report=report,
                        now=now,
                        last_progress_at=last_progress_at,
                    )
                    if stall_state is None:
                        return None
            except _DISCOVERY_WATCH_ERRORS as exc:
                self._log_lifecycle_watch_retry(run_id=run_id, stage=stage, error=exc)
                time.sleep(DISCOVERY_WATCH_RETRY_S)
                continue
            time.sleep(0.8)

    def _handle_discovery_completion_auto_sync(
        self,
        *,
        run_id: str,
        finished_at: str,
        report: dict[str, Any],
        summary: dict[str, Any],
    ) -> None:
        self._drain_reconciliation_retries()
        queued = int(summary.get("queuedCandidateCount") or summary.get("newCandidateCount") or 0)
        saved_config = self.get_saved_discovery_config_payload()
        report, auto_approved, _persisted = self._reconcile_terminal_discovery_registry_state(
            run_id=run_id,
            finished_at=finished_at,
            report=report,
            saved_config_enabled=bool(saved_config.get("autoApproveHealthyPendingOnComplete")),
        )
        self._deps.save_json_atomic(self._paths.report, report)
        runtime = as_json_object(report.get("runtime"))
        runtime_auto = as_json_object(runtime.get("autoApproval"))
        auto_approve_enabled = self._terminal_report_auto_approval_enabled(
            report,
            saved_config_enabled=bool(saved_config.get("autoApproveHealthyPendingOnComplete")),
        )
        self._deps.bridge_log(
            "info",
            "discovery_auto_approval_completed",
            runId=run_id,
            enabled=auto_approve_enabled,
            approved=int(auto_approved or runtime_auto.get("approvedCount") or 0),
        )
        if queued <= 0 and auto_approved <= 0:
            self._deps.mark_discovery_sync_finished(finished_at)
            return
        runtime_state = self._deps.load_sync_runtime_state()
        if str(runtime_state.get("lastDiscoverySyncFinishedAt") or "") == finished_at:
            return
        if self._deps.maybe_trigger_auto_sync_push("discovery_completed"):
            self._deps.mark_discovery_sync_finished(finished_at)
            self._deps.bridge_log(
                "info",
                "sync_auto_push_started",
                runId=run_id,
                reason="discovery_completed",
                queued=queued,
                autoApproved=int(auto_approved),
            )

    def watch_discovery_run_for_auto_sync(self, run_id: str, pid: int, started_at: str) -> None:
        started_dt = self._deps.parse_iso(started_at) or self._deps.now_utc()
        report = self._wait_for_discovery_watch_report(
            run_id=run_id,
            pid=pid,
            started_at=started_at,
            started_dt=started_dt,
        )
        if report is None:
            return
        finished_at = ""
        summary: dict[str, Any] = {}
        while True:
            stage = "finalize"
            try:
                finished_at = str(report.get("finishedAt") or "")
                finished_dt = self._deps.parse_iso(finished_at)
                if not finished_dt or finished_dt < started_dt:
                    return
                lifecycle_row = self._deps.get_lifecycle_row(run_id, "discovery")
                if row_abort_requested(lifecycle_row):
                    stage = "abort_closeout"
                    self._cancel_discovery_run(
                        run_id=run_id,
                        finished_at=self._deps.now_iso(),
                        reason=self._abort_reason_from_row(lifecycle_row),
                    )
                    return
                summary = as_json_object(report.get("summary"))
                self._finalize_discovery_run(
                    run_id=run_id,
                    started_at=started_at,
                    finished_at=finished_at,
                    summary=summary,
                )
                break
            except _DISCOVERY_WATCH_ERRORS as exc:
                self._log_lifecycle_watch_retry(run_id=run_id, stage=stage, error=exc)
                time.sleep(DISCOVERY_WATCH_RETRY_S)
                continue
        try:
            self._handle_discovery_completion_auto_sync(
                run_id=run_id,
                finished_at=finished_at,
                report=report,
                summary=summary,
            )
        except _DISCOVERY_WATCH_ERRORS as exc:
            self._deps.bridge_log(
                "warn",
                "sync_auto_push_skipped",
                runId=run_id,
                reason="discovery_completed",
                error=str(exc),
            )
