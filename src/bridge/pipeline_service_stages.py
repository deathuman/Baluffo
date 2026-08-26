"""Pipeline stage orchestration and worker loop.

AI boundary owns: discovery/fetch/registry-conflict/sync-push stage runners, report
completion waits, the worker loop, and task start for bridge-managed pipeline runs.
AI boundary implement in: this mixin leaf for stage orchestration; control, lifecycle,
child coordination, and status reconciliation stay in sibling mixin leaves consumed by
``PipelineService``.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from typing import Any

from .pipeline_service_types import (
    PipelineAbortRequested,
    PipelineServiceState,
)

_PIPELINE_OPERATIONAL_ERRORS = (RuntimeError, OSError, TypeError, ValueError)


class _PipelineServiceStageMixin(PipelineServiceState):
    def _run_discovery_stage(self, run_id: str) -> None:
        self._mark_stage(
            stage="discovery", current_step=1, total_steps=3, label="Running discovery..."
        )
        discovery_status, discovery_result = self._trigger_discovery_child()
        discovery_attached = int(discovery_status) == 409 and self._is_duplicate_task_response(
            discovery_result
        )
        if self._discovery_launch_failed(discovery_status, discovery_result, discovery_attached):
            raise RuntimeError(
                f"discovery_launch: {discovery_result.get('error') or 'discovery start failed'}"
            )
        discovery_started_at = str(discovery_result.get("startedAt") or self._now_iso())
        discovery_run_id = str(discovery_result.get("runId") or "").strip()
        self._attach_lifecycle_child_row(
            run_id=run_id,
            task_type="discovery",
            child_run_id=discovery_run_id,
            child_started_at=discovery_started_at,
        )
        if discovery_attached:
            self._log_attached_child(
                run_id=run_id, task_type="discovery", child_run_id=discovery_run_id
            )
        report = self._wait_for_child_report(
            phase="discovery_wait",
            report_path=self._discovery_report_path,
            started_at=discovery_started_at,
            timeout_s=900.0,
            report_name="discovery report",
            load_json_object=self._load_runtime_evidence,
            report_is_stale_in_progress=lambda *_args, **_kwargs: False,
            task_type="discovery",
            task_run_id=discovery_run_id,
        )
        report_status = str(report.get("status") or "").strip().lower()
        if report_status in {"error", "failed", "failure"}:
            summary_raw = report.get("summary")
            summary = summary_raw if isinstance(summary_raw, dict) else {}
            error = str(summary.get("error") or "discovery failed").strip()
            raise RuntimeError(f"discovery_wait: {error}")
        self._wait_for_discovery_auto_approval(report)

    def _wait_for_discovery_auto_approval(self, report: dict[str, Any]) -> None:
        runtime_value = report.get("runtime")
        runtime = runtime_value if isinstance(runtime_value, dict) else {}
        auto_approval_value = runtime.get("autoApproval")
        auto_approval = auto_approval_value if isinstance(auto_approval_value, dict) else {}
        registry_finalization_value = runtime.get("registryFinalization")
        registry_finalization = (
            registry_finalization_value if isinstance(registry_finalization_value, dict) else {}
        )
        status = str(auto_approval.get("status") or "completed").strip().lower()
        registry_status = str(registry_finalization.get("status") or "completed").strip().lower()
        auto_approval_running = bool(auto_approval.get("enabled")) and status == "running"
        registry_finalization_running = registry_status == "running"
        if not auto_approval_running and not registry_finalization_running:
            return
        from datetime import UTC, datetime, timedelta
        from threading import Event

        started_wait = datetime.now(UTC)
        while True:
            with self._lock:
                pipeline_run_id = str(self._status.get("runId") or "").strip()
            self._check_abort(pipeline_run_id)
            wait_label = (
                "Finalizing discovery registry..."
                if registry_finalization_running
                else "Applying discovery auto-approval..."
            )
            self._mark_stage(
                stage=(
                    "discovery_registry_finalization"
                    if registry_finalization_running
                    else "discovery_auto_approval"
                ),
                current_step=1,
                total_steps=3,
                label=wait_label,
            )
            self._heartbeat_pipeline_wait()
            latest = self._load_runtime_evidence(self._discovery_report_path, {})
            latest_report = latest if isinstance(latest, dict) else {}
            runtime_value = latest_report.get("runtime")
            runtime = runtime_value if isinstance(runtime_value, dict) else {}
            auto_approval_value = runtime.get("autoApproval")
            auto_approval = auto_approval_value if isinstance(auto_approval_value, dict) else {}
            registry_finalization_value = runtime.get("registryFinalization")
            registry_finalization = (
                registry_finalization_value if isinstance(registry_finalization_value, dict) else {}
            )
            status = str(auto_approval.get("status") or "completed").strip().lower()
            registry_status = (
                str(registry_finalization.get("status") or "completed").strip().lower()
            )
            auto_approval_running = bool(auto_approval.get("enabled")) and status == "running"
            registry_finalization_running = registry_status == "running"
            if not auto_approval_running and not registry_finalization_running:
                return
            if datetime.now(UTC) - started_wait >= timedelta(minutes=10):
                self._bridge_log(
                    "warn",
                    "discovery_finalization_wait_timed_out",
                    autoApprovalStatus=status or "running",
                    registryFinalizationStatus=registry_status or "running",
                )
                raise RuntimeError(
                    "Discovery registry finalization did not settle within the timeout; "
                    "refusing to run fetch against an unfinalized registry."
                )
            Event().wait(1.0)

    def _run_fetch_stage(self, run_id: str) -> None:
        self._mark_stage(stage="fetch", current_step=2, total_steps=3, label="Running fetch...")
        fetch_result = self._start_fetch_child()
        fetch_attached = self._is_duplicate_task_response(fetch_result)
        if not bool(fetch_result.get("started")) and not fetch_attached:
            raise RuntimeError(f"fetch_launch: {fetch_result.get('error') or 'fetch start failed'}")
        fetch_started_at = str(fetch_result.get("startedAt") or self._now_iso())
        fetch_run_id = str(fetch_result.get("runId") or "").strip()
        self._attach_lifecycle_child_row(
            run_id=run_id,
            task_type="fetch",
            child_run_id=fetch_run_id,
            child_started_at=fetch_started_at,
        )
        if fetch_attached:
            self._log_attached_child(run_id=run_id, task_type="fetch", child_run_id=fetch_run_id)
        self._wait_for_child_report(
            phase="fetch_wait",
            report_path=self._fetch_report_path,
            started_at=fetch_started_at,
            timeout_s=1200.0,
            report_name="fetch report",
            load_json_object=self._load_runtime_evidence,
            report_is_stale_in_progress=lambda *_args, **_kwargs: False,
            task_type="fetch",
            task_run_id=fetch_run_id,
        )

    def _run_registry_conflict_adjudication_stage(self, run_id: str) -> None:
        if not callable(self._run_registry_conflict_adjudication):
            return
        with self._lock:
            enabled = bool(self._status.get("runRegistryConflictAdjudication"))
        if not enabled:
            self._bridge_log(
                "info",
                "registry_conflict_adjudication_skipped",
                runId=run_id,
                reason="not_enabled_for_pipeline",
            )
            return
        try:
            self._mark_stage(
                stage="registry_conflicts",
                current_step=2,
                total_steps=3,
                label="Checking registry conflicts...",
            )
            result = self._run_registry_conflict_adjudication(
                {
                    "applyAutopilot": True,
                    "trigger": "jobs_pipeline",
                    "pipelineRunId": run_id,
                }
            )
            if self._abort_requested(run_id):
                self._bridge_log(
                    "warn",
                    "jobs_pipeline_registry_adjudication_completed_after_abort",
                    runId=run_id,
                )
                raise PipelineAbortRequested("pipeline abort requested")
            self._bridge_log(
                "info",
                "registry_conflict_adjudication_finished",
                runId=run_id,
                demoted=int(result.get("demoted") or 0),
                checkedFamilyCount=int(result.get("checkedFamilyCount") or 0),
            )
        except _PIPELINE_OPERATIONAL_ERRORS as exc:
            self._bridge_log(
                "warn",
                "registry_conflict_adjudication_failed",
                runId=run_id,
                error=str(exc),
            )

    def _run_sync_push_stage(self, run_id: str) -> dict[str, Any] | None:
        self._mark_stage(
            stage="sync_push", current_step=3, total_steps=3, label="Running sync push..."
        )
        sync_result = self._start_sync_push_child()
        if not bool(sync_result.get("started")):
            sync_error = str(sync_result.get("error") or "sync push failed to start")
            warning = self._sync_warning_payload(sync_error)
            self._bridge_log(
                "warning",
                "jobs_pipeline_sync_push_warning",
                runId=run_id,
                warningKind=warning["kind"],
                error=sync_error,
            )
            return warning
        sync_run_id = str(sync_result.get("runId") or "")
        self._attach_lifecycle_child_row(
            run_id=run_id,
            task_type="sync",
            child_run_id=sync_run_id,
            child_started_at=str(sync_result.get("startedAt") or self._now_iso()),
        )
        sync_row = self._wait_for_sync_push_row(sync_run_id)
        if self._abort_requested(run_id):
            raise PipelineAbortRequested("pipeline abort requested")
        sync_status = str(sync_row.get("status") or "").strip().lower()
        if sync_status == "error":
            sync_error = str((sync_row.get("summary") or {}).get("error") or "sync push failed")
            warning = self._sync_warning_payload(sync_error)
            self._bridge_log(
                "warning",
                "jobs_pipeline_sync_push_warning",
                runId=run_id,
                childRunId=sync_run_id,
                warningKind=warning["kind"],
                error=sync_error,
            )
            return warning
        return None

    def wait_for_report_completion(
        self,
        *,
        report_path: Any,
        started_at: str,
        timeout_s: float,
        report_name: str,
        load_json_object: Callable[[Any, Any], Any],
        report_is_stale_in_progress: Callable[..., bool] | None = None,
        fail_on_stale: bool = False,
        task_type: str = "",
        task_run_id: str = "",
    ) -> dict[str, Any]:
        from datetime import timedelta

        stale_guard = report_is_stale_in_progress or (lambda *_args, **_kwargs: False)
        quiet_window_s = max(10.0, float(timeout_s))
        absolute_window_s = max(quiet_window_s * 4.0, quiet_window_s + 3600.0)
        now = self._report_wait_now()
        quiet_deadline = now + timedelta(seconds=quiet_window_s)
        absolute_deadline = now + timedelta(seconds=absolute_window_s)
        started_dt = self._parse_iso(started_at)
        while True:
            now = self._report_wait_now()
            report = load_json_object(report_path, {})
            normalized_report = report if isinstance(report, dict) else {}
            with self._lock:
                pipeline_run_id = str(self._status.get("runId") or "").strip()
            completed_report = self._finish_matching_terminal_child_report(
                normalized_report,
                started_dt=started_dt,
                started_at=started_at,
                task_type=task_type,
                task_run_id=task_run_id,
            )
            if completed_report is not None:
                return completed_report
            terminal_snapshot = self._child_terminal_snapshot(task_type, task_run_id)
            if terminal_snapshot is not None:
                self._raise_for_terminal_child_without_report(
                    report_name=report_name,
                    task_type=task_type,
                    task_run_id=task_run_id,
                    snapshot=terminal_snapshot,
                    report=normalized_report,
                )
            refreshed_child_heartbeat = self._refresh_child_lifecycle_evidence(
                task_type, task_run_id, started_at
            )
            child_live = bool(
                refreshed_child_heartbeat
                or (
                    str(task_type or "").strip()
                    and str(task_run_id or "").strip()
                    and self._child_task_has_live_evidence(task_type, task_run_id)
                )
            )
            if child_live:
                quiet_deadline = now + timedelta(seconds=quiet_window_s)
                absolute_deadline = now + timedelta(seconds=absolute_window_s)
                self._heartbeat_pipeline_wait()
            self._check_abort(pipeline_run_id)
            if fail_on_stale and stale_guard(
                "fetch" if "fetch" in report_name else "discovery",
                report_path,
                normalized_report,
            ):
                raise RuntimeError(f"{report_name} became stale before completion")
            if now >= absolute_deadline:
                error = f"{report_name} exceeded absolute safety cap"
                self._fail_child_lifecycle(
                    task_type,
                    task_run_id,
                    terminal_reason="absolute_safety_cap_exceeded",
                    error=error,
                )
                raise TimeoutError(error)
            if now >= quiet_deadline and not child_live:
                error = f"{report_name} had no live evidence before completion"
                self._fail_child_lifecycle(
                    task_type,
                    task_run_id,
                    terminal_reason="quiet_timeout_no_live_evidence",
                    error=error,
                )
                raise TimeoutError(error)
            # ponytail: piggyback child phase observations onto the existing
            # wait loop so the ledger sees sub-stages (e.g. fetch/loading_state,
            # fetch/scraping_adapter) without an extra thread or extra I/O —
            # the report file was already loaded this iteration.
            self._record_child_phase_observation(task_type, normalized_report)
            self._report_wait_sleep(1.0)

    def _record_child_phase_observation(
        self, task_type: str, report: dict[str, Any] | None
    ) -> None:
        """Append a sub-stage ledger entry when the child's taskProgress.phaseKey changes.

        Reads report["taskProgress"]["phaseKey"] / phaseLabel. If the phaseKey
        is unchanged from the ledger's last entry, no-op. The ledger entry's
        ``stage`` field is "<task_type>/<phaseKey>" so downstream tooling can
        split sub-stage from top-level stage by "/" without a schema change.
        """
        if not isinstance(report, dict):
            return
        progress = report.get("taskProgress")
        if not isinstance(progress, dict):
            return
        phase_key = str(progress.get("phaseKey") or "").strip()
        if not phase_key:
            return
        clean_task = str(task_type or "").strip().lower()
        sub_stage = f"{clean_task}/{phase_key}" if clean_task else phase_key
        label = str(progress.get("phaseLabel") or "").strip() or phase_key
        with self._lock:
            ledger = self._status.setdefault("_stageLedger", [])
            if not isinstance(ledger, list):
                return
            if ledger and str(ledger[-1].get("stage") or "") == sub_stage:
                return
            ledger.append({"stage": sub_stage, "enteredAt": self._now_iso(), "label": label})
            if len(ledger) > 64:
                del ledger[:-64]

    def _run_worker(self, run_id: str) -> None:
        try:
            self._check_abort(run_id)
            self._run_discovery_stage(run_id)
            self._check_abort(run_id)
            self._run_fetch_stage(run_id)
            self._check_abort(run_id)
            self._run_registry_conflict_adjudication_stage(run_id)
            self._check_abort(run_id)
            sync_warning = self._run_sync_push_stage(run_id)
            self._check_abort(run_id)
            final_output_count = self._current_fetch_output_count()
            if sync_warning:
                self._set_completed(
                    status="warning",
                    final_output_count=final_output_count,
                    warnings=[sync_warning],
                    sync_warning=sync_warning,
                )
            else:
                self._set_completed(status="ok", final_output_count=final_output_count)
        except PipelineAbortRequested:
            self._bridge_log("info", "jobs_pipeline_canceled", runId=run_id)
            self._set_completed(
                status="canceled",
                final_output_count=self._current_fetch_output_count(),
            )
        except _PIPELINE_OPERATIONAL_ERRORS as exc:
            self._bridge_log("error", "jobs_pipeline_failed", runId=run_id, error=str(exc))
            self._set_completed(
                status="error",
                final_output_count=self._current_fetch_output_count(),
                error=str(exc),
            )
        except Exception as exc:
            self._bridge_log("error", "jobs_pipeline_failed", runId=run_id, error=str(exc))
            self._set_completed(
                status="error",
                final_output_count=self._current_fetch_output_count(),
                error=str(exc),
            )
            raise

    def start_task(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            if bool(self._status.get("active")) and str(self._status.get("runId") or ""):
                return {
                    "started": False,
                    "error": "Jobs pipeline already running",
                    "runId": str(self._status.get("runId") or ""),
                    "stage": str(self._status.get("stage") or "running"),
                }
            if self._sync_task_running():
                return {
                    "started": False,
                    "error": "Another sync task is already running",
                    "runId": "",
                    "stage": "blocked",
                }

            run_id = f"pipeline_{uuid.uuid4().hex[:10]}"
            started_at = self._now_iso()
            jobs_page_loaded_count = int((payload or {}).get("jobsPageLoadedCount") or 0)
            baseline_output_count = self._current_fetch_output_count()
            self._status.update(
                {
                    "active": True,
                    "runId": run_id,
                    "stage": "starting",
                    "progress": self._pipeline_progress(0, 3, "Starting pipeline..."),
                    "startedAt": started_at,
                    "finishedAt": "",
                    "error": "",
                    "warnings": [],
                    "syncWarning": {},
                    "syncStatus": "",
                    "completedWithWarnings": False,
                    "updatesFound": False,
                    "refreshRecommended": False,
                    "activeChildren": [],
                    "activeChildTaskType": "",
                    "activeChildRunId": "",
                    "activeChildPhaseLabel": "",
                    "activeChildDisplayLabel": "",
                    "_stageLedger": [],
                    "_stageTransitions": [],
                    "runRegistryConflictAdjudication": bool(
                        (payload or {}).get("runRegistryConflictAdjudication")
                    ),
                    "baselineOutputCount": int(baseline_output_count),
                    "finalOutputCount": 0,
                    "jobsPageLoadedCount": int(max(0, jobs_page_loaded_count)),
                }
            )
            if self._runtime.abort_requests is not None:
                self._runtime.abort_requests.pop(run_id, None)
            status_snapshot = dict(self._status)
        self._write_control_status(status_snapshot)
        if callable(self._start_lifecycle_run):
            self._start_lifecycle_run(
                run_id=run_id,
                task_type="pipeline",
                started_at=started_at,
                stage="starting",
                owner_kind="pipeline",
                progress=self._pipeline_lifecycle_progress(status_snapshot),
                summary={
                    "baselineOutputCount": int(baseline_output_count),
                    "jobsPageLoadedCount": int(max(0, jobs_page_loaded_count)),
                    "stage": "starting",
                },
            )
        worker = threading.Thread(
            target=self._run_worker,
            args=(run_id,),
            name=f"jobs-pipeline-{run_id}",
            daemon=True,
        )
        self._runtime.active_run_id = run_id
        self._runtime.active_thread = worker
        worker.start()
        self._bridge_log(
            "info",
            "jobs_pipeline_started",
            runId=run_id,
            baseline=baseline_output_count,
            jobsPageLoadedCount=jobs_page_loaded_count,
        )
        return {
            "started": True,
            "runId": run_id,
            "stage": "starting",
            "progress": dict(status_snapshot.get("progress") or {}),
        }
