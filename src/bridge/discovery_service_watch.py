"""Discovery service discovery service watch.

AI boundary owns: bridge-owned discovery task launch, config persistence, and auto-sync watch behavior.
AI boundary implement in: this discovery_service_watch.py leaf.
AI boundary search before contracts: discovery routes, task launch API, source discovery config, and admin discovery frontend callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused discovery service tests.
"""

from __future__ import annotations

import threading
from typing import Any

from src.bridge.active_task_snapshot import upsert_snapshot_rows
from src.bridge.discovery_service_core import DiscoveryServiceState
from src.bridge.task_abort_evidence import row_abort_requested
from src.shared.json_shapes import as_json_object

_DISCOVERY_WATCH_ERRORS = (RuntimeError, OSError, TypeError, ValueError)


class DiscoveryServiceWatchMixin(DiscoveryServiceState):
    def _refresh_discovery_task_heartbeat(self, *, run_id: str, pid: int, started_at: str) -> None:
        now = self._deps.now_iso()
        report = self._deps.normalize_discovery_report_contract(self._read_discovery_report())
        report_run_id = str(report.get("runId") or "").strip()
        report_started_at = str(report.get("startedAt") or "").strip()
        report_started_dt = self._deps.parse_iso(report_started_at)
        started_dt = self._deps.parse_iso(started_at)
        report_matches_run = bool(
            report_run_id == run_id
            and (
                not report_started_at
                or not started_dt
                or not report_started_dt
                or report_started_dt >= started_dt
            )
        )
        progress = as_json_object(report.get("taskProgress")) if report_matches_run else {}
        summary = as_json_object(report.get("summary")) if report_matches_run else {}
        stage = str(
            summary.get("currentStageKey") or summary.get("phaseKey") or summary.get("phase") or ""
        ).strip()
        self._deps.heartbeat_lifecycle_run(
            run_id,
            "discovery",
            heartbeat_at=now,
            stage=stage or "running",
            progress=progress or None,
            summary=summary or None,
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

    def _discovery_report_finished_since(self, report: dict[str, Any], started_dt: Any) -> bool:
        finished_at = str(report.get("finishedAt") or "")
        finished_dt = self._deps.parse_iso(finished_at)
        return bool(finished_dt and finished_dt >= started_dt)

    @staticmethod
    def _abort_reason_from_row(row: dict[str, Any] | None) -> str:
        summary = (row or {}).get("summary")
        return str((summary if isinstance(summary, dict) else {}).get("abortReason") or "")

    def _wait_for_discovery_watch_report(
        self,
        *,
        run_id: str,
        pid: int,
        started_at: str,
        started_dt: Any,
    ) -> dict[str, Any] | None:
        while True:
            lifecycle_row = self._deps.get_lifecycle_row(run_id, "discovery")
            report = self._deps.normalize_discovery_report_contract(self._read_discovery_report())
            if self._discovery_report_finished_since(
                report, started_dt
            ) and self._discovery_report_finalization_settled(report):
                return report
            if not self._deps.pid_is_running(pid):
                if row_abort_requested(lifecycle_row):
                    self._cancel_discovery_run(
                        run_id=run_id,
                        reason=self._abort_reason_from_row(lifecycle_row),
                    )
                    return None
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
                return None
            self._refresh_discovery_task_heartbeat(
                run_id=run_id,
                pid=pid,
                started_at=started_at,
            )
            threading.Event().wait(0.8)

    def _handle_discovery_completion_auto_sync(
        self,
        *,
        run_id: str,
        finished_at: str,
        report: dict[str, Any],
        summary: dict[str, Any],
    ) -> None:
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
        try:
            finished_at = str(report.get("finishedAt") or "")
            finished_dt = self._deps.parse_iso(finished_at)
            if not finished_dt or finished_dt < started_dt:
                return
            lifecycle_row = self._deps.get_lifecycle_row(run_id, "discovery")
            if row_abort_requested(lifecycle_row):
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
