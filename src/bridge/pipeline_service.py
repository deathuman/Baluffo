"""Jobs pipeline orchestration service used by the admin bridge."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.bridge.ops_live_payload import build_pipeline_task_progress


@dataclass
class PipelineRuntime:
    active_run_id: str = ""
    active_thread: threading.Thread | None = None


class PipelineService:
    def __init__(
        self,
        *,
        pipeline_state_lock: threading.RLock,
        pipeline_status: dict[str, Any],
        runtime: PipelineRuntime,
        bridge_log: Callable[..., None],
        now_iso: Callable[[], str],
        parse_iso: Callable[[Any], Any],
        append_run_history: Callable[[dict[str, Any]], dict[str, Any]],
        upsert_run_history: Callable[..., dict[str, Any]],
        task_running_from_state: Callable[[str], bool],
        sync_task_running: Callable[[], bool],
        current_fetch_output_count: Callable[[], int],
        load_json_object: Callable[[Any, Any], Any],
        load_runtime_evidence: Callable[[Any, Any], Any] | None = None,
        wait_for_sync_completion: Callable[[str, float], dict[str, Any]],
        discovery_report_path: Any,
        fetch_report_path: Any,
        trigger_discovery_task: Callable[..., Any],
        start_fetcher_task: Callable[..., dict[str, Any]],
        start_sync_task: Callable[..., dict[str, Any]],
        get_app_version: Callable[[], str],
        child_run_is_live: Callable[[str, str], bool] | None = None,
        get_projected_run_history: Callable[[], Any] | None = None,
        run_registry_conflict_adjudication: Callable[[dict[str, Any]], dict[str, Any]]
        | None = None,
        refresh_child_task_heartbeat: Callable[[str, str, str], bool] | None = None,
        start_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None] | None = None,
        finish_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
        fail_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
        attach_lifecycle_child: Callable[..., dict[str, Any] | None] | None = None,
        clear_task_state: Callable[[str], None] | None = None,
    ) -> None:
        self._lock = pipeline_state_lock
        self._status = pipeline_status
        self._runtime = runtime
        self._bridge_log = bridge_log
        self._now_iso = now_iso
        self._parse_iso = parse_iso
        self._sync_task_running = sync_task_running
        self._current_fetch_output_count = current_fetch_output_count
        self._load_json_object = load_json_object
        if load_runtime_evidence is None:
            self._load_runtime_evidence = self._load_json_object
        else:
            self._load_runtime_evidence = load_runtime_evidence
        self._wait_for_sync_completion = wait_for_sync_completion
        self._discovery_report_path = discovery_report_path
        self._fetch_report_path = fetch_report_path
        self._trigger_discovery_task = trigger_discovery_task
        self._start_fetcher_task = start_fetcher_task
        self._start_sync_task = start_sync_task
        self._get_app_version = get_app_version
        self._child_run_is_live = child_run_is_live
        self._get_projected_run_history = get_projected_run_history
        self._run_registry_conflict_adjudication = run_registry_conflict_adjudication
        self._refresh_child_task_heartbeat = refresh_child_task_heartbeat
        self._start_lifecycle_run = start_lifecycle_run
        self._heartbeat_lifecycle_run = heartbeat_lifecycle_run
        self._finish_lifecycle_run = finish_lifecycle_run
        self._fail_lifecycle_run = fail_lifecycle_run
        self._attach_lifecycle_child = attach_lifecycle_child

    @staticmethod
    def _pipeline_progress(current_step: int, total_steps: int, label: str) -> dict[str, Any]:
        safe_total = max(1, int(total_steps or 1))
        safe_current = max(0, min(int(current_step or 0), safe_total))
        return {
            "currentStep": safe_current,
            "totalSteps": safe_total,
            "percent": int(round((safe_current / safe_total) * 100)),
            "label": str(label or ""),
        }

    @staticmethod
    def _pipeline_lifecycle_progress(status: dict[str, Any]) -> dict[str, Any]:
        return build_pipeline_task_progress(status)

    def _mark_stage(
        self, *, stage: str, current_step: int, total_steps: int, label: str, error: str = ""
    ) -> None:
        with self._lock:
            self._status["stage"] = str(stage or "unknown")
            self._status["progress"] = self._pipeline_progress(current_step, total_steps, label)
            progress = self._pipeline_lifecycle_progress(dict(self._status))
            run_id = str(self._status.get("runId") or "")
            if error:
                self._status["error"] = str(error)
        if run_id and callable(self._heartbeat_lifecycle_run):
            self._heartbeat_lifecycle_run(
                run_id,
                "pipeline",
                stage=str(stage or "unknown"),
                progress=progress,
                summary={"stage": str(stage or "unknown")},
            )

    def _set_completed(self, *, status: str, final_output_count: int = 0, error: str = "") -> None:
        with self._lock:
            run_id = str(self._status.get("runId") or "")
            baseline = int(self._status.get("baselineOutputCount") or 0)
            loaded = int(self._status.get("jobsPageLoadedCount") or 0)
            compare_base = max(baseline, loaded)
            updates_found = int(final_output_count or 0) > compare_base
            self._status.update(
                {
                    "active": False,
                    "stage": "completed" if status != "error" else "error",
                    "progress": self._pipeline_progress(
                        3, 3, "Pipeline completed" if status != "error" else "Pipeline failed"
                    ),
                    "finishedAt": self._now_iso(),
                    "error": str(error or ""),
                    "finalOutputCount": int(final_output_count or 0),
                    "updatesFound": bool(updates_found),
                    "refreshRecommended": bool(updates_found),
                }
            )
            finished_at = str(self._status.get("finishedAt") or "")
            progress = self._pipeline_lifecycle_progress(dict(self._status))
            if run_id:
                if callable(self._fail_lifecycle_run) and status == "error":
                    self._fail_lifecycle_run(
                        run_id,
                        "pipeline",
                        finished_at=finished_at,
                        terminal_reason="failed",
                        summary={
                            "error": str(error or ""),
                            "baselineOutputCount": baseline,
                            "jobsPageLoadedCount": loaded,
                            "finalOutputCount": int(final_output_count or 0),
                            "updatesFound": bool(updates_found),
                        },
                        progress=progress,
                    )
                elif callable(self._finish_lifecycle_run):
                    self._finish_lifecycle_run(
                        run_id,
                        "pipeline",
                        finished_at=finished_at,
                        terminal_reason="completed",
                        summary={
                            "baselineOutputCount": baseline,
                            "jobsPageLoadedCount": loaded,
                            "finalOutputCount": int(final_output_count or 0),
                            "updatesFound": bool(updates_found),
                        },
                        progress=progress,
                    )
            self._runtime.active_run_id = ""

    def _get_child_task_snapshot(self, task_type: str, run_id: str = "") -> Any:
        if not callable(self._get_projected_run_history):
            return None
        try:
            projection = self._get_projected_run_history()
        except (AttributeError, RuntimeError, TypeError, ValueError):
            return None
        child_tasks = getattr(projection, "child_tasks", {})
        if not isinstance(child_tasks, dict):
            return None
        snapshot = child_tasks.get(str(task_type or "").strip().lower())
        if snapshot is None:
            return None
        snapshot_run_id = str(getattr(snapshot, "run_id", "") or "").strip()
        if run_id and snapshot_run_id and snapshot_run_id != run_id:
            return None
        return snapshot

    def _child_task_is_active(self, task_type: str, run_id: str = "") -> bool:
        snapshot = self._get_child_task_snapshot(task_type, run_id)
        return bool(snapshot and getattr(snapshot, "active", False))

    def _child_task_has_live_evidence(self, task_type: str, run_id: str = "") -> bool:
        checked_child_liveness = bool(
            callable(self._child_run_is_live) and str(run_id or "").strip()
        )
        if checked_child_liveness:
            try:
                if bool(self._child_run_is_live(task_type, run_id)):
                    return True
            except (RuntimeError, TypeError, ValueError):
                return False
        if checked_child_liveness:
            return False
        return self._child_task_is_active(task_type, run_id)

    @staticmethod
    def _is_duplicate_task_response(result: dict[str, Any] | None) -> bool:
        return bool(isinstance(result, dict) and result.get("alreadyRunning"))

    def _wait_for_child_report(self, *, phase: str, **kwargs: Any) -> dict[str, Any]:
        try:
            return self.wait_for_report_completion(**kwargs)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"{phase}: {exc}") from exc

    def _wait_for_sync_push_row(self, run_id: str) -> dict[str, Any]:
        try:
            return self._wait_for_sync_completion(run_id, 900.0)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"sync_push: {exc}") from exc

    def _trigger_discovery_child(self) -> Any:
        try:
            return self._trigger_discovery_task(
                route_name="/tasks/run-jobs-pipeline",
                enable_auto_sync_watch=False,
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"discovery_launch: {exc}") from exc

    def _start_fetch_child(self) -> dict[str, Any]:
        try:
            return self._start_fetcher_task({"preset": "default"})
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"fetch_launch: {exc}") from exc

    def _start_sync_push_child(self) -> dict[str, Any]:
        try:
            return self._start_sync_task("push", reason="jobs_pipeline", automatic=False)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"sync_push: {exc}") from exc

    def get_status_payload(self) -> dict[str, Any]:
        self._recover_inactive_worker_after_terminal_child()
        with self._lock:
            payload = dict(self._status)
            progress = payload.get("progress")
            payload["progress"] = (
                dict(progress)
                if isinstance(progress, dict)
                else self._pipeline_progress(0, 3, "Idle")
            )
            payload["active"] = bool(payload.get("active"))
            payload["appVersion"] = self._get_app_version()
            return payload

    def _refresh_child_lifecycle_evidence(
        self,
        task_type: str,
        task_run_id: str,
        started_at: str,
    ) -> bool:
        clean_task_type = str(task_type or "").strip()
        clean_run_id = str(task_run_id or "").strip()
        if (
            not callable(self._refresh_child_task_heartbeat)
            or not clean_task_type
            or not clean_run_id
        ):
            return False
        try:
            return bool(
                self._refresh_child_task_heartbeat(
                    clean_task_type,
                    clean_run_id,
                    str(started_at or "").strip(),
                )
            )
        except (RuntimeError, OSError, TypeError, ValueError) as exc:
            self._bridge_log(
                "warn",
                "jobs_pipeline_child_heartbeat_refresh_failed",
                taskType=clean_task_type,
                childRunId=clean_run_id,
                error=str(exc),
            )
            return False

    def _report_matches_started_run(
        self,
        report: dict[str, Any],
        *,
        started_dt: Any,
        task_run_id: str,
    ) -> bool:
        from datetime import timedelta

        report_run_id = str(report.get("runId") or "").strip()
        clean_run_id = str(task_run_id or "").strip()
        run_id_matches = bool(not clean_run_id or report_run_id == clean_run_id)
        report_started = self._parse_iso(report.get("startedAt"))
        return bool(
            run_id_matches
            and started_dt
            and report_started
            and report_started >= (started_dt - timedelta(seconds=1))
        )

    def _finish_child_lifecycle_from_report(
        self,
        task_type: str,
        task_run_id: str,
        report: dict[str, Any],
    ) -> None:
        if not task_type or not task_run_id:
            return
        report_status = str(report.get("status") or "").strip().lower()
        terminal_summary = report.get("summary")
        if not isinstance(terminal_summary, dict):
            terminal_summary = {}
        terminal_progress = report.get("taskProgress")
        if isinstance(terminal_progress, dict):
            terminal_progress = {**terminal_progress, "active": False}
        else:
            terminal_progress = None
        clean_run_id = str(task_run_id).strip()
        clean_task_type = str(task_type).strip()
        if report_status in {"error", "failed", "failure"}:
            if not callable(self._fail_lifecycle_run):
                return
            self._fail_lifecycle_run(
                clean_run_id,
                clean_task_type,
                finished_at=report.get("finishedAt"),
                terminal_reason="failed",
                summary=terminal_summary,
                progress=terminal_progress,
            )
            return
        if not callable(self._finish_lifecycle_run):
            return
        self._finish_lifecycle_run(
            clean_run_id,
            clean_task_type,
            finished_at=report.get("finishedAt"),
            terminal_reason="completed",
            summary=terminal_summary,
            progress=terminal_progress,
        )

    def _terminal_report_matches_child(
        self,
        report: dict[str, Any],
        *,
        task_run_id: str,
        started_at: str,
    ) -> bool:
        if not isinstance(report, dict):
            return False
        if not str(report.get("finishedAt") or "").strip():
            return False
        clean_run_id = str(task_run_id or "").strip()
        if clean_run_id and str(report.get("runId") or "").strip() != clean_run_id:
            return False
        started_dt = self._parse_iso(started_at)
        if not started_dt:
            return True
        return self._report_matches_started_run(
            report,
            started_dt=started_dt,
            task_run_id=clean_run_id,
        )

    def _recover_inactive_worker_after_terminal_child(self) -> None:
        with self._lock:
            pipeline_active = bool(self._status.get("active"))
            pipeline_stage = str(self._status.get("stage") or "").strip().lower()
        if not pipeline_active or pipeline_stage != "fetch":
            if pipeline_stage == "sync_push":
                self._recover_inactive_worker_after_terminal_sync()
            return
        worker = self._runtime.active_thread
        if worker is not None and worker.is_alive():
            return
        snapshot = self._get_child_task_snapshot("fetch")
        child_run_id = str(getattr(snapshot, "run_id", "") or "").strip()
        child_started_at = str(getattr(snapshot, "started_at", "") or "").strip()
        report = self._load_runtime_evidence(self._fetch_report_path, {})
        if not isinstance(report, dict):
            return
        if not child_run_id:
            child_run_id = str(report.get("runId") or "").strip()
        if not self._terminal_report_matches_child(
            report,
            task_run_id=child_run_id,
            started_at=child_started_at,
        ):
            return
        self._bridge_log(
            "warn",
            "jobs_pipeline_recovered_terminal_fetch_after_worker_inactive",
            childRunId=child_run_id,
            finishedAt=str(report.get("finishedAt") or ""),
        )
        self._finish_child_lifecycle_from_report("fetch", child_run_id, report)
        self._set_completed(
            status="error",
            final_output_count=self._current_fetch_output_count(),
            error="pipeline_worker_inactive_after_fetch_completed",
        )

    def _recover_inactive_worker_after_terminal_sync(self) -> None:
        worker = self._runtime.active_thread
        if worker is not None and worker.is_alive():
            return
        snapshot = self._get_child_task_snapshot("sync")
        if snapshot is None or bool(getattr(snapshot, "active", False)):
            return
        child_run_id = str(getattr(snapshot, "run_id", "") or "").strip()
        finished_at = str(getattr(snapshot, "finished_at", "") or "").strip()
        if not child_run_id or not finished_at:
            return
        terminal_status = str(getattr(snapshot, "terminal_status", "") or "").strip().lower()
        summary = getattr(snapshot, "summary", {})
        if not isinstance(summary, dict):
            summary = {}
        error = str(summary.get("error") or "sync push failed").strip()
        self._bridge_log(
            "warn",
            "jobs_pipeline_recovered_terminal_sync_after_worker_inactive",
            childRunId=child_run_id,
            finishedAt=finished_at,
            terminalStatus=terminal_status,
        )
        if terminal_status in {"error", "failed", "failure"}:
            self._set_completed(
                status="error",
                final_output_count=self._current_fetch_output_count(),
                error=f"sync_push: {error}",
            )
            return
        self._set_completed(status="ok", final_output_count=self._current_fetch_output_count())

    def _fail_child_lifecycle(
        self,
        task_type: str,
        task_run_id: str,
        *,
        terminal_reason: str,
        error: str,
    ) -> None:
        if callable(self._fail_lifecycle_run) and task_type and task_run_id:
            self._fail_lifecycle_run(
                str(task_run_id).strip(),
                str(task_type).strip(),
                terminal_reason=terminal_reason,
                summary={"error": error},
            )

    def _heartbeat_pipeline_wait(self) -> None:
        with self._lock:
            run_id = str(self._status.get("runId") or "").strip()
            stage = str(self._status.get("stage") or "running").strip() or "running"
            progress = self._pipeline_lifecycle_progress(dict(self._status))
        if run_id and callable(self._heartbeat_lifecycle_run):
            self._heartbeat_lifecycle_run(
                run_id,
                "pipeline",
                stage=stage,
                progress=progress,
                summary={"stage": stage},
            )

    def _attach_lifecycle_child_row(
        self,
        *,
        run_id: str,
        task_type: str,
        child_run_id: str,
    ) -> None:
        if child_run_id and callable(self._attach_lifecycle_child):
            self._attach_lifecycle_child(
                run_id=child_run_id,
                task_type=task_type,
                parent_run_id=run_id,
                parent_task_type="pipeline",
                owner_kind="pipeline",
            )

    def _log_attached_child(self, *, run_id: str, task_type: str, child_run_id: str) -> None:
        self._bridge_log(
            "info",
            "jobs_pipeline_attached_existing_child_task",
            runId=run_id,
            childTask=task_type,
            childRunId=child_run_id,
        )

    def _discovery_launch_failed(
        self,
        discovery_status: int,
        discovery_result: dict[str, Any],
        discovery_attached: bool,
    ) -> bool:
        return bool(
            int(discovery_status) >= 300
            and not discovery_attached
            or (
                int(discovery_status) < 300
                and not bool((discovery_result or {}).get("started"))
                and not discovery_attached
            )
        )

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
            run_id=run_id, task_type="discovery", child_run_id=discovery_run_id
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
        self._wait_for_discovery_auto_approval(report)

    def _wait_for_discovery_auto_approval(self, report: dict[str, Any]) -> None:
        runtime = report.get("runtime") if isinstance(report.get("runtime"), dict) else {}
        auto_approval = (
            runtime.get("autoApproval") if isinstance(runtime.get("autoApproval"), dict) else {}
        )
        registry_finalization = (
            runtime.get("registryFinalization")
            if isinstance(runtime.get("registryFinalization"), dict)
            else {}
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
            runtime = (
                latest_report.get("runtime")
                if isinstance(latest_report.get("runtime"), dict)
                else {}
            )
            auto_approval = (
                runtime.get("autoApproval") if isinstance(runtime.get("autoApproval"), dict) else {}
            )
            registry_finalization = (
                runtime.get("registryFinalization")
                if isinstance(runtime.get("registryFinalization"), dict)
                else {}
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
                return
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
            run_id=run_id, task_type="fetch", child_run_id=fetch_run_id
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
            self._bridge_log(
                "info",
                "registry_conflict_adjudication_finished",
                runId=run_id,
                demoted=int(result.get("demoted") or 0),
                checkedFamilyCount=int(result.get("checkedFamilyCount") or 0),
            )
        except Exception as exc:  # noqa: BLE001
            self._bridge_log(
                "warn",
                "registry_conflict_adjudication_failed",
                runId=run_id,
                error=str(exc),
            )

    def _run_sync_push_stage(self, run_id: str) -> None:
        self._mark_stage(
            stage="sync_push", current_step=3, total_steps=3, label="Running sync push..."
        )
        sync_result = self._start_sync_push_child()
        if not bool(sync_result.get("started")):
            raise RuntimeError(
                f"sync_push: {sync_result.get('error') or 'sync push failed to start'}"
            )
        sync_run_id = str(sync_result.get("runId") or "")
        self._attach_lifecycle_child_row(run_id=run_id, task_type="sync", child_run_id=sync_run_id)
        sync_row = self._wait_for_sync_push_row(sync_run_id)
        sync_status = str(sync_row.get("status") or "").strip().lower()
        if sync_status == "error":
            sync_error = str((sync_row.get("summary") or {}).get("error") or "sync push failed")
            raise RuntimeError(f"sync_push: {sync_error}")

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
        from datetime import UTC, datetime, timedelta
        from threading import Event

        stale_guard = report_is_stale_in_progress or (lambda *_args, **_kwargs: False)
        quiet_window_s = max(10.0, float(timeout_s))
        quiet_deadline = datetime.now(UTC) + timedelta(seconds=quiet_window_s)
        absolute_deadline = datetime.now(UTC) + timedelta(
            seconds=max(quiet_window_s * 4.0, quiet_window_s + 3600.0)
        )
        started_dt = self._parse_iso(started_at)
        while True:
            now = datetime.now(UTC)
            report = load_json_object(report_path, {})
            normalized_report = report if isinstance(report, dict) else {}
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
                self._heartbeat_pipeline_wait()
            if self._report_matches_started_run(
                normalized_report, started_dt=started_dt, task_run_id=task_run_id
            ):
                report_started = self._parse_iso(normalized_report.get("startedAt"))
                report_finished = self._parse_iso(normalized_report.get("finishedAt"))
                if report_finished and report_started and report_finished >= report_started:
                    self._finish_child_lifecycle_from_report(
                        task_type, task_run_id, normalized_report
                    )
                    return normalized_report
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
            Event().wait(1.0)

    def _run_worker(self, run_id: str) -> None:
        try:
            self._run_discovery_stage(run_id)
            self._run_fetch_stage(run_id)
            self._run_registry_conflict_adjudication_stage(run_id)
            self._run_sync_push_stage(run_id)
            final_output_count = self._current_fetch_output_count()
            self._set_completed(status="ok", final_output_count=final_output_count)
        except Exception as exc:  # noqa: BLE001
            self._bridge_log("error", "jobs_pipeline_failed", runId=run_id, error=str(exc))
            self._set_completed(
                status="error",
                final_output_count=self._current_fetch_output_count(),
                error=str(exc),
            )

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
                    "updatesFound": False,
                    "refreshRecommended": False,
                    "runRegistryConflictAdjudication": bool(
                        (payload or {}).get("runRegistryConflictAdjudication")
                    ),
                    "baselineOutputCount": int(baseline_output_count),
                    "finalOutputCount": 0,
                    "jobsPageLoadedCount": int(max(0, jobs_page_loaded_count)),
                }
            )
            if callable(self._start_lifecycle_run):
                self._start_lifecycle_run(
                    run_id=run_id,
                    task_type="pipeline",
                    started_at=started_at,
                    stage="starting",
                    owner_kind="pipeline",
                    progress=self._pipeline_lifecycle_progress(dict(self._status)),
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
                "progress": dict(self._status.get("progress") or {}),
            }


__all__ = ["PipelineRuntime", "PipelineService"]
