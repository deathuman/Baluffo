"""Discovery service for source discovery operations.

This module provides DiscoveryService for managing source discovery
tasks and auto-sync watch functionality.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from src.bridge.task_abort_evidence import (
    ABORT_TERMINAL_REASON,
    repair_discovery_canceled_evidence,
    row_abort_requested,
)
from src.bridge.task_admission import (
    build_duplicate_start_payload,
    get_active_lifecycle_task_metadata,
)
from src.shared.json_shapes import as_json_object
from src.source_registry import (
    _pending_row_is_auto_approvable as registry_pending_row_is_auto_approvable,
)
from src.source_registry import (
    apply_discovery_auto_approval,
)

BridgeLogFunc = Callable[..., None]


@dataclass(frozen=True)
class DiscoveryPaths:
    report: Any
    candidates: Any
    pending: Any
    log: Any
    settings: Any
    approval_state: Any
    task_state: Any | None = None


@dataclass(frozen=True)
class DiscoveryDeps:
    schema_version: int
    now_iso: Callable[[], str]
    now_utc: Callable[[], Any]
    parse_iso: Callable[[Any], Any]
    pid_is_running: Callable[[int], bool]
    bridge_log: BridgeLogFunc
    load_json_object: Callable[[Any, Any], Any]
    save_json_atomic: Callable[[Any, Any], None]
    run_background_script: Callable[..., int]
    append_run_history: Callable[[dict[str, Any]], dict[str, Any]]
    upsert_run_history: Callable[..., dict[str, Any]]
    prune_started_rows_for_type: Callable[..., None]
    clear_task_state: Callable[[str], None]
    normalize_discovery_report_contract: Callable[[dict[str, Any]], dict[str, Any]]
    load_state: Callable[[], dict[str, list[dict[str, Any]]]]
    persist_state_and_auto_sync: Callable[..., dict[str, list[dict[str, Any]]]]
    load_sync_runtime_state: Callable[[], dict[str, Any]]
    maybe_trigger_auto_sync_push: Callable[[str], bool]
    mark_discovery_sync_finished: Callable[[str], None]
    task_state_lock: Any | None = None
    start_lifecycle_run: Callable[..., dict[str, Any]] = lambda **_kwargs: {}
    heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None] = lambda *_args, **_kwargs: None
    finish_lifecycle_run: Callable[..., dict[str, Any]] = lambda *_args, **_kwargs: {}
    fail_lifecycle_run: Callable[..., dict[str, Any]] = lambda *_args, **_kwargs: {}
    cancel_lifecycle_run: Callable[..., dict[str, Any]] = lambda *_args, **_kwargs: {}
    get_lifecycle_current_runs: Callable[[], list[dict[str, Any]]] = lambda: []
    get_lifecycle_row: Callable[[str, str], dict[str, Any] | None] = lambda _run_id, _task_type: (
        None
    )
    load_runtime_evidence: Callable[[Any, Any], Any] | None = None


class DiscoveryService:
    def __init__(self, *, paths: DiscoveryPaths, deps: DiscoveryDeps) -> None:
        self._paths = paths
        self._deps = deps

    def _schema_version_int(self) -> int:
        """Return a safe integer schema version, tolerating string values like '1.0'."""
        raw = self._deps.schema_version
        try:
            return int(raw)
        except (TypeError, ValueError):
            try:
                return int(float(str(raw)))
            except (TypeError, ValueError):
                return 1

    def _finalize_discovery_run(
        self,
        *,
        run_id: str,
        started_at: str,
        finished_at: str,
        summary: dict[str, Any],
    ) -> None:
        self._deps.finish_lifecycle_run(
            run_id,
            "discovery",
            finished_at=finished_at,
            summary=dict(summary or {}),
            terminal_reason="completed",
        )

    def _cancel_discovery_run(
        self,
        *,
        run_id: str,
        finished_at: str = "",
        reason: str = "",
    ) -> dict[str, Any]:
        canceled_at = str(finished_at or self._deps.now_iso() or "")
        report = repair_discovery_canceled_evidence(
            report_path=self._paths.report,
            run_id=run_id,
            finished_at=canceled_at,
            load_json_object=self._deps.load_json_object,
            save_json_atomic=self._deps.save_json_atomic,
            normalize_report=self._deps.normalize_discovery_report_contract,
            reason=reason,
            overwrite_finished=True,
        )
        self._deps.cancel_lifecycle_run(
            run_id,
            "discovery",
            finished_at=str(report.get("finishedAt") or canceled_at),
            terminal_reason=ABORT_TERMINAL_REASON,
            summary=dict(report.get("summary") or {}),
            progress=dict(report.get("taskProgress") or {}),
        )
        return report

    @staticmethod
    def _normalize_discovery_settings(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = payload if isinstance(payload, dict) else {}
        raw = data.get("autoApproveHealthyPendingOnComplete", True)
        if isinstance(raw, bool):
            enabled = raw
        else:
            enabled = str(raw or "").strip().lower() not in {"", "0", "false", "no", "off"}
        return {"autoApproveHealthyPendingOnComplete": bool(enabled)}

    def load_saved_discovery_settings(self) -> dict[str, Any]:
        raw = self._deps.load_json_object(self._paths.settings, {})
        if isinstance(raw, dict) and "autoApproveHealthyPendingOnComplete" in raw:
            return self._normalize_discovery_settings(raw)
        return {}

    def get_saved_discovery_config_payload(self) -> dict[str, Any]:
        settings = self.load_saved_discovery_settings()
        if "autoApproveHealthyPendingOnComplete" in settings:
            return {
                "autoApproveHealthyPendingOnComplete": bool(
                    settings.get("autoApproveHealthyPendingOnComplete")
                )
            }
        return self._normalize_discovery_settings({})

    def get_discovery_config_payload(self) -> dict[str, Any]:
        return {
            "ok": True,
            "savedConfig": self.get_saved_discovery_config_payload(),
        }

    def _read_discovery_report(self) -> dict[str, Any]:
        reader = (
            self._deps.load_runtime_evidence
            if callable(self._deps.load_runtime_evidence)
            else self._deps.load_json_object
        )
        return reader(self._paths.report, {})

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

    @staticmethod
    def _lifecycle_status_token(row: dict[str, Any]) -> str:
        return str(row.get("lifecycleStatus") or row.get("status") or "").strip().lower()

    @classmethod
    def _lifecycle_row_is_terminal(cls, row: dict[str, Any] | None) -> bool:
        if not isinstance(row, dict):
            return False
        if str(row.get("finishedAt") or "").strip():
            return True
        return cls._lifecycle_status_token(row) in {
            "succeeded",
            "failed",
            "canceled",
            "cancelled",
            "orphaned",
            "ok",
            "error",
            "completed",
        }

    @classmethod
    def _report_status_from_lifecycle_row(cls, row: dict[str, Any]) -> str:
        status = cls._lifecycle_status_token(row)
        if status in {"canceled", "cancelled"}:
            return "canceled"
        if status in {"succeeded", "ok", "completed"}:
            return "ok"
        return "error"

    @staticmethod
    def _terminal_phase_label(status: str) -> str:
        if status == "canceled":
            return "Discovery canceled"
        if status == "ok":
            return "Discovery completed"
        return "Discovery failed"

    @staticmethod
    def _terminal_phase_key(status: str) -> str:
        if status == "canceled":
            return "canceled"
        if status == "ok":
            return "completed"
        return "failed"

    def _repair_terminal_discovery_report_from_row(
        self,
        row: dict[str, Any],
        report: dict[str, Any],
        *,
        finished_at: str = "",
    ) -> dict[str, Any] | None:
        if not isinstance(report, dict):
            return None
        run_id = str(report.get("runId") or row.get("runId") or row.get("id") or "").strip()
        if not run_id:
            return None
        row_run_id = str(row.get("runId") or row.get("id") or "").strip()
        if row_run_id and row_run_id != run_id:
            return None
        if str(report.get("finishedAt") or "").strip():
            return dict(report)

        terminal_at = str(finished_at or row.get("finishedAt") or self._deps.now_iso() or "")
        terminal_reason = str(row.get("terminalReason") or "").strip()
        status = self._report_status_from_lifecycle_row(row)
        phase_key = self._terminal_phase_key(status)
        phase_label = self._terminal_phase_label(status)

        row_summary = as_json_object(row.get("summary"))
        summary = {**as_json_object(report.get("summary")), **row_summary}
        if terminal_reason:
            summary["terminalReason"] = terminal_reason
        summary["status"] = status
        if status != "ok" and not str(summary.get("error") or "").strip():
            summary["error"] = terminal_reason or "discovery_task_terminal_without_report"

        row_progress = as_json_object(row.get("taskProgress") or row.get("progress"))
        report_progress = as_json_object(report.get("taskProgress"))
        progress = {**row_progress, **report_progress}
        row_counts = as_json_object(row_progress.get("counts"))
        report_counts = as_json_object(report_progress.get("counts"))
        counts = {**row_counts, **report_counts}
        progress.update(
            {
                "active": False,
                "phaseKey": phase_key,
                "phaseLabel": phase_label,
                "mode": progress.get("mode") or "indeterminate",
                "updatedAt": terminal_at,
            }
        )
        if counts:
            progress["counts"] = counts

        runtime = as_json_object(report.get("runtime"))
        lifecycle = as_json_object(runtime.get("lifecycle"))
        runtime["lifecycle"] = {
            **lifecycle,
            "owner": lifecycle.get("owner") or "discovery_report",
            "heartbeatAt": terminal_at,
            "terminalReason": terminal_reason,
        }

        repaired = {
            **dict(report),
            "runId": run_id,
            "finishedAt": terminal_at,
            "status": status,
            "terminalReason": terminal_reason,
            "summary": summary,
            "taskProgress": progress,
            "runtime": runtime,
        }
        normalized = self._deps.normalize_discovery_report_contract(repaired)
        self._deps.save_json_atomic(self._paths.report, normalized)
        self._deps.bridge_log(
            "warn",
            "discovery_report_repaired_from_terminal_lifecycle",
            runId=run_id,
            status=status,
            terminalReason=terminal_reason,
            finishedAt=terminal_at,
        )
        return dict(normalized)

    def _reconcile_terminal_discovery_report_from_state(self) -> dict[str, Any] | None:
        raw = self._read_discovery_report()
        if not isinstance(raw, dict):
            return None
        report = self._deps.normalize_discovery_report_contract(raw)
        run_id = str(report.get("runId") or "").strip()
        if not run_id or str(report.get("finishedAt") or "").strip():
            return None
        row = self._deps.get_lifecycle_row(run_id, "discovery")
        if not self._lifecycle_row_is_terminal(row):
            return None
        return self._repair_terminal_discovery_report_from_row(row or {}, report)

    def reconcile_terminal_discovery_report_from_state(self) -> dict[str, Any] | None:
        return self._reconcile_terminal_discovery_report_from_state()

    @staticmethod
    def _run_background_script_with_identity(
        run_background_script: Callable[..., int],
        script_name: str,
        args: list[str],
        *,
        extra_env: dict[str, str],
        run_id: str,
        task_type: str,
        metadata: dict[str, Any],
    ) -> int:
        try:
            return run_background_script(
                script_name,
                args,
                extra_env=extra_env,
                run_id=run_id,
                task_type=task_type,
                metadata=metadata,
            )
        except TypeError as exc:
            message = str(exc)
            if "unexpected keyword argument" not in message or "run_id" not in message:
                raise
            return run_background_script(script_name, args, extra_env=extra_env)

    def update_saved_discovery_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_discovery_settings(payload)
        self._deps.save_json_atomic(self._paths.settings, normalized)
        return normalized

    @classmethod
    def _pending_row_is_auto_approvable(cls, row: dict[str, Any]) -> bool:
        return registry_pending_row_is_auto_approvable(row)

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
            if self._discovery_report_finished_since(report, started_dt):
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
                return
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
        auto_approve_enabled = bool(saved_config.get("autoApproveHealthyPendingOnComplete"))
        state, auto_approved = apply_discovery_auto_approval(
            self._deps.load_state(),
            report,
            auto_approve_enabled=auto_approve_enabled,
            approval_state_path=self._paths.approval_state,
            now_iso_fn=self._deps.now_iso,
        )
        if auto_approved > 0:
            self._deps.persist_state_and_auto_sync(state, reason="discovery_auto_approve")
        self._deps.save_json_atomic(self._paths.report, report)
        self._deps.bridge_log(
            "info",
            "discovery_auto_approval_completed",
            runId=run_id,
            enabled=auto_approve_enabled,
            approved=int(auto_approved),
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
        except Exception as exc:  # noqa: BLE001
            self._deps.bridge_log(
                "warn",
                "sync_auto_push_skipped",
                runId=run_id,
                reason="discovery_completed",
                error=str(exc),
            )

    def trigger_discovery_task(
        self,
        *,
        route_name: str,
        payload: dict[str, Any] | None = None,
        enable_auto_sync_watch: bool = True,
    ) -> tuple[int, dict[str, Any]]:
        data = payload if isinstance(payload, dict) else {}
        preset = str(data.get("preset") or "default").strip().lower()
        lock_context = (
            self._deps.task_state_lock if self._deps.task_state_lock is not None else nullcontext()
        )
        with lock_context:
            self._reconcile_terminal_discovery_report_from_state()
            active_metadata = get_active_lifecycle_task_metadata(
                "discovery",
                lifecycle_rows=list(self._deps.get_lifecycle_current_runs() or []),
                pid_is_running=self._deps.pid_is_running,
            )
            if active_metadata:
                response = build_duplicate_start_payload(
                    "source_discovery", "discovery", active_metadata
                )
                self._deps.bridge_log(
                    "info",
                    "task_start_attached_existing",
                    task="source_discovery",
                    taskType="discovery",
                    route=route_name,
                    runId=str(response.get("runId") or ""),
                    pid=int(response.get("pid") or 0),
                )
                return 409, response

            run_id = f"discovery_{uuid.uuid4().hex[:10]}"
            started_at = self._deps.now_iso()
            self._deps.bridge_log(
                "info",
                "discovery_launch_started",
                runId=run_id,
                route=route_name,
                preset=preset or "default",
            )
            self._deps.save_json_atomic(
                self._paths.report,
                {
                    "schemaVersion": self._schema_version_int(),
                    "runId": run_id,
                    "mode": "dynamic",
                    "startedAt": started_at,
                    "finishedAt": "",
                    "summary": {
                        "foundEndpointCount": 0,
                        "probedCandidateCount": 0,
                        "queuedCandidateCount": 0,
                        "failedProbeCount": 0,
                        "skippedDuplicateCount": 0,
                        "skippedLowEvidenceProbeCount": 0,
                        "phase": "starting",
                        "phaseKey": "starting",
                        "phaseLabel": "Spawning discovery worker",
                    },
                    "taskProgress": {
                        "active": True,
                        "phaseKey": "starting",
                        "phaseLabel": "Spawning discovery worker",
                        "mode": "indeterminate",
                        "ratio": 0.0,
                        "targetLabel": "Spawning discovery worker",
                        "updatedAt": started_at,
                        "counts": {
                            "foundEndpoints": 0,
                            "generatedCandidates": 0,
                            "survivedDedupeCandidates": 0,
                            "probedCandidates": 0,
                            "probeTotal": 0,
                            "queuedCandidates": 0,
                            "deferredCandidates": 0,
                            "failedProbes": 0,
                            "stageIndex": 0,
                            "stageTotal": 0,
                            "completedStages": 0,
                        },
                    },
                    "candidates": [],
                    "failures": [],
                    "topFailures": [],
                    "outputs": {
                        "report": str(self._paths.report),
                        "candidates": str(self._paths.candidates),
                        "pending": str(self._paths.pending),
                    },
                    "runtime": {
                        "lifecycle": {
                            "owner": "discovery_report",
                            "heartbeatAt": started_at,
                        },
                        "autoApproval": {
                            "enabled": bool(
                                self.get_saved_discovery_config_payload().get(
                                    "autoApproveHealthyPendingOnComplete"
                                )
                            ),
                            "approvedCount": 0,
                        },
                    },
                },
            )
            try:
                self._paths.log.parent.mkdir(parents=True, exist_ok=True)
                self._paths.log.write_text(
                    f"[{started_at}] Launching source discovery task...\n", encoding="utf-8"
                )
            except OSError:
                pass
            spawn_args = ["--mode", "dynamic"]
            if preset == "uncapped":
                spawn_args.extend(["--top", "0", "--preset", "uncapped"])
            else:
                preset = "default"
                spawn_args.extend(["--preset", "default"])
            try:
                pid = self._run_background_script_with_identity(
                    self._deps.run_background_script,
                    "source_discovery.py",
                    spawn_args,
                    extra_env={
                        "BALUFFO_DISCOVERY_RUN_ID": run_id,
                        "BALUFFO_DISCOVERY_STARTED_AT": started_at,
                    },
                    run_id=run_id,
                    task_type="discovery",
                    metadata={"task": "source_discovery", "preset": preset},
                )
            except Exception as exc:  # noqa: BLE001
                failed_at = self._deps.now_iso()
                self._deps.fail_lifecycle_run(
                    run_id,
                    "discovery",
                    finished_at=failed_at,
                    terminal_reason="launch_failed",
                    summary={"error": str(exc), "failedProbeCount": 1},
                )
                self._deps.save_json_atomic(
                    self._paths.report,
                    {
                        "schemaVersion": self._schema_version_int(),
                        "runId": run_id,
                        "mode": "dynamic",
                        "startedAt": started_at,
                        "finishedAt": failed_at,
                        "summary": {
                            "foundEndpointCount": 0,
                            "probedCandidateCount": 0,
                            "queuedCandidateCount": 0,
                            "failedProbeCount": 1,
                        },
                        "candidates": [],
                        "failures": [
                            {
                                "name": "source_discovery.py",
                                "adapter": "bridge",
                                "error": str(exc),
                                "stage": "launch",
                            }
                        ],
                        "topFailures": [{"key": "bridge:launch", "count": 1}],
                        "outputs": {
                            "report": str(self._paths.report),
                            "candidates": str(self._paths.candidates),
                            "pending": str(self._paths.pending),
                        },
                        "runtime": {
                            "lifecycle": {
                                "owner": "discovery_report",
                                "heartbeatAt": failed_at,
                            },
                            "autoApproval": {
                                "enabled": bool(
                                    self.get_saved_discovery_config_payload().get(
                                        "autoApproveHealthyPendingOnComplete"
                                    )
                                ),
                                "approvedCount": 0,
                            },
                        },
                    },
                )
                try:
                    with self._paths.log.open("a", encoding="utf-8") as handle:
                        handle.write(f"[{self._deps.now_iso()}] Launch failed: {str(exc)}\n")
                except OSError:
                    pass
                self._deps.bridge_log(
                    "error",
                    "task_start_failed",
                    runId=run_id,
                    task="source_discovery",
                    mode="dynamic",
                    route=route_name,
                    error=str(exc),
                )
                return 500, {
                    "started": False,
                    "task": "source_discovery",
                    "mode": "dynamic",
                    "preset": preset,
                    "route": route_name,
                    "error": str(exc),
                }
            if enable_auto_sync_watch:
                watcher = threading.Thread(
                    target=self.watch_discovery_run_for_auto_sync,
                    args=(run_id, pid, started_at),
                    name=f"discovery-sync-watch-{run_id}",
                    daemon=True,
                )
                watcher.start()
            self._deps.bridge_log(
                "info",
                "task_started",
                runId=run_id,
                task="source_discovery",
                mode="dynamic",
                preset=preset,
                route=route_name,
                pid=int(pid),
            )
            self._deps.start_lifecycle_run(
                run_id=run_id,
                task_type="discovery",
                started_at=started_at,
                stage="starting",
                owner_kind="process",
                owner_pid=int(pid),
                progress={
                    "active": True,
                    "phaseKey": "starting",
                    "phaseLabel": "Spawning discovery worker",
                    "mode": "indeterminate",
                    "ratio": 0.0,
                    "counts": {},
                    "updatedAt": started_at,
                },
                summary={},
            )
            return 200, {
                "started": True,
                "runId": run_id,
                "task": "source_discovery",
                "mode": "dynamic",
                "preset": preset,
                "args": spawn_args,
                "route": route_name,
                "startedAt": started_at,
                "pid": int(pid),
            }


__all__ = ["DiscoveryDeps", "DiscoveryPaths", "DiscoveryService"]
