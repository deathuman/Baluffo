"""Jobs pipeline orchestration service used by the admin bridge."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.bridge.ops_live_payload import build_pipeline_task_progress
from src.bridge.task_abort_evidence import ABORT_TERMINAL_REASON, row_abort_requested

PIPELINE_COMPLETION_NOTIFICATION_MIN_SECONDS = 60.0


@dataclass
class PipelineRuntime:
    active_run_id: str = ""
    active_thread: threading.Thread | None = None
    abort_requests: dict[str, dict[str, Any]] | None = None


class PipelineAbortRequested(Exception):
    """Raised for cooperative pipeline cancellation."""


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
        cancel_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
        attach_lifecycle_child: Callable[..., dict[str, Any] | None] | None = None,
        clear_task_state: Callable[[str], None] | None = None,
        pipeline_completion_notifier: Callable[[dict[str, Any]], Any] | None = None,
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
        self._cancel_lifecycle_run = cancel_lifecycle_run
        self._attach_lifecycle_child = attach_lifecycle_child
        self._pipeline_completion_notifier = pipeline_completion_notifier
        self._completion_notification_run_id = ""
        if self._runtime.abort_requests is None:
            self._runtime.abort_requests = {}

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

    def _abort_requested(self, run_id: str) -> bool:
        if not str(run_id or "").strip():
            return False
        requests = self._runtime.abort_requests or {}
        return str(run_id or "").strip() in requests

    def _abort_metadata(self, run_id: str) -> dict[str, Any]:
        requests = self._runtime.abort_requests or {}
        return dict(requests.get(str(run_id or "").strip()) or {})

    def _check_abort(self, run_id: str, *, defer_sync: bool = False) -> None:
        clean_run_id = str(run_id or "").strip()
        if not self._abort_requested(clean_run_id):
            return
        with self._lock:
            stage = str(self._status.get("stage") or "").strip().lower()
        if defer_sync and stage in {"sync_push", "abort_pending_sync"}:
            return
        raise PipelineAbortRequested("pipeline abort requested")

    def request_abort(
        self,
        run_id: str,
        *,
        reason: str = "",
        requested_at: str = "",
    ) -> dict[str, Any]:
        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            return {"ok": False, "error": "missing_run_id", "state": "missing"}
        requested = str(requested_at or self._now_iso() or "")
        with self._lock:
            active_run_id = str(self._status.get("runId") or "").strip()
            if not bool(self._status.get("active")) or active_run_id != clean_run_id:
                return {"ok": False, "error": "pipeline_not_active", "state": "missing"}
            requests = (
                self._runtime.abort_requests if self._runtime.abort_requests is not None else {}
            )
            requests[clean_run_id] = {
                "requestedAt": requested,
                "reason": str(reason or "").strip(),
            }
            self._runtime.abort_requests = requests
            current_stage = str(self._status.get("stage") or "").strip().lower()
            deferred = current_stage in {"sync_push", "abort_pending_sync"}
            next_stage = "abort_pending_sync" if deferred else "aborting"
            self._status["stage"] = next_stage
            self._status["progress"] = self._pipeline_progress(
                3 if deferred else 0,
                3,
                "Abort after sync..." if deferred else "Aborting...",
            )
            progress = self._pipeline_lifecycle_progress(dict(self._status))
        if callable(self._heartbeat_lifecycle_run):
            self._heartbeat_lifecycle_run(
                clean_run_id,
                "pipeline",
                stage=next_stage,
                progress=progress,
                summary={
                    "stage": next_stage,
                    "abortRequestedAt": requested,
                    "abortReason": str(reason or "").strip(),
                },
            )
        return {
            "ok": True,
            "runId": clean_run_id,
            "state": next_stage,
            "deferred": deferred,
        }

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

    def _set_completed(self, *, status: str, final_output_count: int = 0, error: str = "") -> None:
        completion_notification: dict[str, Any] | None = None
        with self._lock:
            run_id = str(self._status.get("runId") or "")
            baseline = int(self._status.get("baselineOutputCount") or 0)
            loaded = int(self._status.get("jobsPageLoadedCount") or 0)
            compare_base = max(baseline, loaded)
            updates_found = int(final_output_count or 0) > compare_base
            canceled = status == "canceled"
            self._status.update(
                {
                    "active": False,
                    "stage": "canceled"
                    if canceled
                    else ("completed" if status != "error" else "error"),
                    "progress": self._pipeline_progress(
                        3,
                        3,
                        "Pipeline canceled"
                        if canceled
                        else ("Pipeline completed" if status != "error" else "Pipeline failed"),
                    ),
                    "finishedAt": self._now_iso(),
                    "error": str(error or ""),
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
                }
            progress = self._pipeline_lifecycle_progress(dict(self._status))
            if run_id:
                if callable(self._cancel_lifecycle_run) and canceled:
                    self._cancel_lifecycle_run(
                        run_id,
                        "pipeline",
                        finished_at=finished_at,
                        terminal_reason=ABORT_TERMINAL_REASON,
                        summary={
                            "terminalReason": ABORT_TERMINAL_REASON,
                            "baselineOutputCount": baseline,
                            "jobsPageLoadedCount": loaded,
                            "finalOutputCount": int(final_output_count or 0),
                            "updatesFound": bool(updates_found),
                            **self._abort_metadata(run_id),
                        },
                        progress=progress,
                    )
                elif callable(self._fail_lifecycle_run) and status == "error":
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
            if self._runtime.abort_requests is not None:
                self._runtime.abort_requests.pop(run_id, None)
        if status != "canceled":
            self._notify_pipeline_completion(completion_notification)

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

    def _child_abort_requested(self, task_type: str, run_id: str = "") -> bool:
        clean_task_type = str(task_type or "").strip().lower()
        clean_run_id = str(run_id or "").strip()
        if not clean_task_type or not clean_run_id or not callable(self._get_projected_run_history):
            return False
        try:
            projection = self._get_projected_run_history()
        except (AttributeError, RuntimeError, TypeError, ValueError):
            return False
        rows = getattr(projection, "rows", [])
        if not isinstance(rows, list):
            return False
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_task_type = str(row.get("taskType") or row.get("type") or "").strip().lower()
            row_run_id = str(row.get("runId") or row.get("id") or "").strip()
            if row_task_type == clean_task_type and row_run_id == clean_run_id:
                return row_abort_requested(row)
        return False

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
            if pipeline_stage in {"sync_push", "abort_pending_sync"}:
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
        if self._abort_requested(str(self._status.get("runId") or "")):
            self._set_completed(
                status="canceled", final_output_count=self._current_fetch_output_count()
            )
        else:
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
        if self._abort_requested(run_id):
            raise PipelineAbortRequested("pipeline abort requested")
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
            with self._lock:
                pipeline_run_id = str(self._status.get("runId") or "").strip()
            if child_live:
                quiet_deadline = now + timedelta(seconds=quiet_window_s)
                self._heartbeat_pipeline_wait()
            if self._report_matches_started_run(
                normalized_report, started_dt=started_dt, task_run_id=task_run_id
            ):
                report_started = self._parse_iso(normalized_report.get("startedAt"))
                report_finished = self._parse_iso(normalized_report.get("finishedAt"))
                if report_finished and report_started and report_finished >= report_started:
                    if self._child_abort_requested(task_type, task_run_id):
                        raise PipelineAbortRequested("pipeline child abort requested")
                    self._finish_child_lifecycle_from_report(
                        task_type, task_run_id, normalized_report
                    )
                    return normalized_report
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
            Event().wait(1.0)

    def _run_worker(self, run_id: str) -> None:
        try:
            self._check_abort(run_id)
            self._run_discovery_stage(run_id)
            self._check_abort(run_id)
            self._run_fetch_stage(run_id)
            self._check_abort(run_id)
            self._run_registry_conflict_adjudication_stage(run_id)
            self._check_abort(run_id)
            self._run_sync_push_stage(run_id)
            self._check_abort(run_id)
            final_output_count = self._current_fetch_output_count()
            self._set_completed(status="ok", final_output_count=final_output_count)
        except PipelineAbortRequested:
            self._bridge_log("info", "jobs_pipeline_canceled", runId=run_id)
            self._set_completed(
                status="canceled",
                final_output_count=self._current_fetch_output_count(),
            )
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
            if self._runtime.abort_requests is not None:
                self._runtime.abort_requests.pop(run_id, None)
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
