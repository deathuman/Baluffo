"""Pipeline completion, post-publish, and terminal lifecycle machinery.

AI boundary owns: completion notification, post-publish callbacks, stage ledger, and
terminal lifecycle emission for bridge-managed pipeline runs.
AI boundary implement in: this mixin leaf for completion/lifecycle mechanics; control,
child coordination, status reconciliation, and stage orchestration stay in sibling
mixin leaves consumed by ``PipelineService``.
"""

from __future__ import annotations

from typing import Any

from src.bridge.pipeline_control_files import (
    clear_abort_request,
)
from src.bridge.task_abort_evidence import ABORT_TERMINAL_REASON

from .pipeline_service_types import PipelineServiceState

PIPELINE_COMPLETION_NOTIFICATION_MIN_SECONDS = 60.0


class _PipelineServiceLifecycleMixin(PipelineServiceState):
    def _completion_duration_seconds(self, started_at: str, finished_at: str) -> float:
        if not started_at or not finished_at:
            return 0.0
        try:
            started = self._parse_iso(started_at)
            finished = self._parse_iso(finished_at)
            duration = (finished - started).total_seconds()
        except (TypeError, ValueError):
            return 0.0
        try:
            return max(0.0, float(duration))
        except (TypeError, ValueError):
            return 0.0

    def _notify_pipeline_completion(self, payload: dict[str, Any] | None) -> None:
        if not payload or not callable(self._pipeline_completion_notifier):
            return
        run_id = str(payload.get("runId") or "")
        try:
            result = self._pipeline_completion_notifier(dict(payload))
            if isinstance(result, dict):
                self._bridge_log(
                    "info",
                    "jobs_pipeline_completion_attention",
                    runId=run_id,
                    notified=bool(result.get("notified")),
                    reason=str(result.get("reason") or ""),
                    hwnd=int(result.get("hwnd") or 0),
                )
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            self._bridge_log(
                "warning",
                "jobs_pipeline_completion_attention_failed",
                runId=run_id,
                error=str(exc),
            )

    def _run_post_publish_callback(self, payload: dict[str, Any] | None) -> None:
        if not payload or not callable(self._pipeline_post_publish_callback):
            return
        run_id = str(payload.get("runId") or "")
        try:
            result = self._pipeline_post_publish_callback(dict(payload))
            sweep = result.get("sweep") if isinstance(result, dict) else {}
            self._bridge_log(
                "info",
                "jobs_pipeline_post_publish_completed",
                runId=run_id,
                projected=int((result or {}).get("projected") or 0)
                if isinstance(result, dict)
                else 0,
                sweepStarted=int((sweep or {}).get("started") or 0)
                if isinstance(sweep, dict)
                else 0,
            )
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            self._bridge_log(
                "warning",
                "jobs_pipeline_post_publish_failed",
                runId=run_id,
                error=type(exc).__name__,
            )

    def _claim_post_publish_run(self, run_id: str, status: str) -> bool:
        if not run_id or status not in {"ok", "warning"}:
            return False
        if run_id == self._post_publish_run_id:
            return False
        self._post_publish_run_id = run_id
        return True

    def _build_post_publish_payload(
        self,
        *,
        run_id: str,
        status: str,
        started_at: str,
        finished_at: str,
        stage: str,
        completed_with_warnings: bool,
    ) -> dict[str, Any] | None:
        if not self._claim_post_publish_run(run_id, status):
            return None
        return {
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "status": stage,
            "completedWithWarnings": completed_with_warnings,
        }

    def _append_terminal_stage_ledger_entry(self, finished_at: str) -> None:
        with self._lock:
            terminal_stage = str(self._status.get("stage") or "")
            ledger = self._status.get("_stageLedger")
            if not isinstance(ledger, list) or not terminal_stage:
                return
            if ledger and str(ledger[-1].get("stage") or "") == terminal_stage:
                return
            ledger.append({"stage": terminal_stage, "enteredAt": finished_at, "label": ""})
            if len(ledger) > 64:
                del ledger[:-64]

    def _snapshot_stage_ledger(self) -> list[dict[str, Any]]:
        # ponytail: snapshot ledger once; shared by all 3 terminal summaries
        return [
            dict(entry)
            for entry in list(self._status.get("_stageLedger") or [])
            if isinstance(entry, dict)
        ]

    def _emit_terminal_lifecycle(
        self,
        *,
        run_id: str,
        status: str,
        canceled: bool,
        completed_with_warnings: bool,
        finished_at: str,
        baseline: int,
        loaded: int,
        final_output_count: int,
        updates_found: bool,
        error: str,
        clean_warnings: list[dict[str, Any]],
        clean_sync_warning: dict[str, Any],
        stage_ledger: list[dict[str, Any]],
        progress: Any,
    ) -> None:
        base_summary = {
            "baselineOutputCount": baseline,
            "jobsPageLoadedCount": loaded,
            "finalOutputCount": final_output_count,
            "updatesFound": updates_found,
            "stageLedger": stage_ledger,
        }
        if callable(self._cancel_lifecycle_run) and canceled:
            self._cancel_lifecycle_run(
                run_id,
                "pipeline",
                finished_at=finished_at,
                terminal_reason=ABORT_TERMINAL_REASON,
                summary={
                    "terminalReason": ABORT_TERMINAL_REASON,
                    **base_summary,
                    **self._abort_metadata(run_id),
                },
                progress=progress,
            )
            return
        if callable(self._fail_lifecycle_run) and status == "error":
            self._fail_lifecycle_run(
                run_id,
                "pipeline",
                finished_at=finished_at,
                terminal_reason="failed",
                summary={"error": error, **base_summary},
                progress=progress,
            )
            return
        if callable(self._finish_lifecycle_run):
            self._finish_lifecycle_run(
                run_id,
                "pipeline",
                finished_at=finished_at,
                terminal_reason=(
                    "completed_with_warnings" if completed_with_warnings else "completed"
                ),
                summary={
                    **base_summary,
                    "warnings": clean_warnings,
                    "syncWarning": clean_sync_warning,
                    "completedWithWarnings": bool(completed_with_warnings),
                },
                progress=progress,
            )

    def _set_completed(
        self,
        *,
        status: str,
        final_output_count: int = 0,
        error: str = "",
        warnings: list[dict[str, Any]] | None = None,
        sync_warning: dict[str, Any] | None = None,
    ) -> None:
        completion_notification: dict[str, Any] | None = None
        post_publish_payload: dict[str, Any] | None = None
        with self._lock:
            pending_run_id = str(self._status.get("runId") or "")
        if status == "canceled" and self._has_live_abortable_child(pending_run_id):
            self._mark_abort_pending(pending_run_id)
            return
        with self._lock:
            run_id = str(self._status.get("runId") or "")
            baseline = int(self._status.get("baselineOutputCount") or 0)
            loaded = int(self._status.get("jobsPageLoadedCount") or 0)
            compare_base = max(baseline, loaded)
            updates_found = int(final_output_count or 0) > compare_base
            canceled = status == "canceled"
            completed_with_warnings = status == "warning"
            clean_warnings = [dict(item) for item in (warnings or []) if isinstance(item, dict)]
            clean_sync_warning = dict(sync_warning or {}) if isinstance(sync_warning, dict) else {}
            self._status.update(
                {
                    "active": False,
                    "stage": "canceled"
                    if canceled
                    else (
                        "completed_with_warnings"
                        if completed_with_warnings
                        else ("completed" if status != "error" else "error")
                    ),
                    "progress": self._pipeline_progress(
                        3,
                        3,
                        "Pipeline canceled"
                        if canceled
                        else (
                            "Pipeline completed with warnings"
                            if completed_with_warnings
                            else ("Pipeline completed" if status != "error" else "Pipeline failed")
                        ),
                    ),
                    "finishedAt": self._now_iso(),
                    "error": str(error or ""),
                    "warnings": clean_warnings,
                    "syncWarning": clean_sync_warning,
                    "syncStatus": "warning"
                    if clean_sync_warning
                    else (
                        "failed"
                        if status == "error" and str(error or "").startswith("sync_push:")
                        else ""
                    ),
                    "completedWithWarnings": bool(completed_with_warnings),
                    "activeChildren": [],
                    "activeChildTaskType": "",
                    "activeChildRunId": "",
                    "activeChildPhaseLabel": "",
                    "activeChildDisplayLabel": "",
                    "finalOutputCount": int(final_output_count or 0),
                    "updatesFound": bool(updates_found),
                    "refreshRecommended": bool(updates_found),
                }
            )
            finished_at = str(self._status.get("finishedAt") or "")
            started_at = str(self._status.get("startedAt") or "")
            stage = str(self._status.get("stage") or "")
            duration_seconds = self._completion_duration_seconds(started_at, finished_at)
            if (
                run_id
                and run_id != self._completion_notification_run_id
                and duration_seconds >= PIPELINE_COMPLETION_NOTIFICATION_MIN_SECONDS
            ):
                self._completion_notification_run_id = run_id
                completion_notification = {
                    "runId": run_id,
                    "startedAt": started_at,
                    "finishedAt": finished_at,
                    "durationSeconds": duration_seconds,
                    "status": stage,
                    "error": str(error or ""),
                    "updatesFound": bool(updates_found),
                    "warnings": clean_warnings,
                    "syncWarning": clean_sync_warning,
                    "completedWithWarnings": bool(completed_with_warnings),
                }
            self._append_terminal_stage_ledger_entry(finished_at)
            post_publish_payload = self._build_post_publish_payload(
                run_id=run_id,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                stage=stage,
                completed_with_warnings=completed_with_warnings,
            )
            progress = self._pipeline_lifecycle_progress(dict(self._status))
            stage_ledger = self._snapshot_stage_ledger()
            status_snapshot = dict(self._status)
            if run_id:
                self._emit_terminal_lifecycle(
                    run_id=run_id,
                    status=status,
                    canceled=canceled,
                    completed_with_warnings=completed_with_warnings,
                    finished_at=finished_at,
                    baseline=baseline,
                    loaded=loaded,
                    final_output_count=int(final_output_count or 0),
                    updates_found=bool(updates_found),
                    error=str(error or ""),
                    clean_warnings=clean_warnings,
                    clean_sync_warning=clean_sync_warning,
                    stage_ledger=stage_ledger,
                    progress=progress,
                )
            self._runtime.active_run_id = ""
            if self._runtime.abort_requests is not None:
                self._runtime.abort_requests.pop(run_id, None)
        self._write_control_status(status_snapshot)
        if self._control_data_dir is not None and run_id:
            clear_abort_request(self._control_data_dir, run_id)
        if status != "canceled":
            self._notify_pipeline_completion(completion_notification)
        self._run_post_publish_callback(post_publish_payload)
