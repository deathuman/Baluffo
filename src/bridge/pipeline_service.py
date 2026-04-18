"""Jobs pipeline orchestration service used by the admin bridge."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


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
        wait_for_sync_completion: Callable[[str, float], dict[str, Any]],
        discovery_report_path: Any,
        fetch_report_path: Any,
        trigger_discovery_task: Callable[..., Any],
        start_fetcher_task: Callable[..., dict[str, Any]],
        start_sync_task: Callable[..., dict[str, Any]],
        get_app_version: Callable[[], str],
        get_projected_run_history: Callable[[], Any] | None = None,
    ) -> None:
        self._lock = pipeline_state_lock
        self._status = pipeline_status
        self._runtime = runtime
        self._bridge_log = bridge_log
        self._now_iso = now_iso
        self._parse_iso = parse_iso
        self._append_run_history = append_run_history
        self._upsert_run_history = upsert_run_history
        self._task_running_from_state = task_running_from_state
        self._sync_task_running = sync_task_running
        self._current_fetch_output_count = current_fetch_output_count
        self._load_json_object = load_json_object
        self._wait_for_sync_completion = wait_for_sync_completion
        self._discovery_report_path = discovery_report_path
        self._fetch_report_path = fetch_report_path
        self._trigger_discovery_task = trigger_discovery_task
        self._start_fetcher_task = start_fetcher_task
        self._start_sync_task = start_sync_task
        self._get_app_version = get_app_version
        self._get_projected_run_history = get_projected_run_history

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

    def _mark_stage(
        self, *, stage: str, current_step: int, total_steps: int, label: str, error: str = ""
    ) -> None:
        with self._lock:
            self._status["stage"] = str(stage or "unknown")
            self._status["progress"] = self._pipeline_progress(current_step, total_steps, label)
            if error:
                self._status["error"] = str(error)

    def _set_completed(self, *, status: str, final_output_count: int = 0, error: str = "") -> None:
        with self._lock:
            run_id = str(self._status.get("runId") or "")
            started_at = str(self._status.get("startedAt") or "")
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
            if run_id:
                started_dt = self._parse_iso(started_at)
                finished_dt = self._parse_iso(finished_at)
                duration_ms = (
                    int(max(0.0, (finished_dt - started_dt).total_seconds() * 1000))
                    if started_dt and finished_dt
                    else 0
                )
                self._upsert_run_history(
                    {
                        "id": run_id,
                        "runId": run_id,
                        "type": "pipeline",
                        "status": "error" if status == "error" else "ok",
                        "startedAt": started_at,
                        "finishedAt": finished_at,
                        "durationMs": duration_ms,
                        "summary": {
                            "error": str(error or ""),
                            "baselineOutputCount": baseline,
                            "jobsPageLoadedCount": loaded,
                            "finalOutputCount": int(final_output_count or 0),
                            "updatesFound": bool(updates_found),
                        },
                    },
                    dedupe_fields=("id",),
                )
            self._runtime.active_run_id = ""

    def _get_child_task_snapshot(self, task_type: str, run_id: str = "") -> Any:
        if not callable(self._get_projected_run_history):
            return None
        try:
            projection = self._get_projected_run_history()
        except Exception:  # noqa: BLE001
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

    def _has_projected_blocking_child_work(self) -> bool:
        return self._child_task_is_active("fetch") or self._child_task_is_active("discovery")

    def get_status_payload(self) -> dict[str, Any]:
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
        deadline = datetime.now(UTC) + timedelta(seconds=max(10.0, float(timeout_s)))
        started_dt = self._parse_iso(started_at)
        while True:
            report = load_json_object(report_path, {})
            normalized_report = report if isinstance(report, dict) else {}
            report_started = self._parse_iso(normalized_report.get("startedAt"))
            report_finished = self._parse_iso(normalized_report.get("finishedAt"))
            child_active = self._child_task_is_active(task_type, task_run_id)
            if (
                started_dt
                and report_started
                and report_started >= (started_dt - timedelta(seconds=1))
            ):
                if report_finished and report_finished >= report_started:
                    if not child_active:
                        return normalized_report
            if fail_on_stale and stale_guard(
                "fetch" if "fetch" in report_name else "discovery",
                report_path,
                normalized_report,
            ):
                raise RuntimeError(f"{report_name} became stale before completion")
            if datetime.now(UTC) >= deadline and not child_active:
                raise TimeoutError(f"{report_name} did not finish within timeout")
            Event().wait(1.0)

    def _run_worker(self, run_id: str) -> None:
        try:
            self._mark_stage(
                stage="discovery", current_step=1, total_steps=3, label="Running discovery..."
            )
            discovery_status, discovery_result = self._trigger_discovery_task(
                route_name="/tasks/run-jobs-pipeline",
                enable_auto_sync_watch=False,
            )
            if int(discovery_status) >= 300 or not bool(discovery_result.get("started")):
                raise RuntimeError(str(discovery_result.get("error") or "discovery start failed"))
            discovery_started_at = str(discovery_result.get("startedAt") or self._now_iso())
            discovery_run_id = str(discovery_result.get("runId") or "").strip()
            self.wait_for_report_completion(
                report_path=self._discovery_report_path,
                started_at=discovery_started_at,
                timeout_s=900.0,
                report_name="discovery report",
                load_json_object=self._load_json_object,
                report_is_stale_in_progress=lambda *_args, **_kwargs: False,
                task_type="discovery",
                task_run_id=discovery_run_id,
            )

            self._mark_stage(stage="fetch", current_step=2, total_steps=3, label="Running fetch...")
            fetch_result = self._start_fetcher_task({"preset": "default"})
            fetch_started_at = str(fetch_result.get("startedAt") or self._now_iso())
            fetch_run_id = str(fetch_result.get("runId") or "").strip()
            self.wait_for_report_completion(
                report_path=self._fetch_report_path,
                started_at=fetch_started_at,
                timeout_s=1200.0,
                report_name="fetch report",
                load_json_object=self._load_json_object,
                report_is_stale_in_progress=lambda *_args, **_kwargs: False,
                task_type="fetch",
                task_run_id=fetch_run_id,
            )

            self._mark_stage(
                stage="sync_push", current_step=3, total_steps=3, label="Running sync push..."
            )
            sync_result = self._start_sync_task("push", reason="jobs_pipeline", automatic=False)
            if not bool(sync_result.get("started")):
                raise RuntimeError(str(sync_result.get("error") or "sync push failed to start"))
            sync_row = self._wait_for_sync_completion(str(sync_result.get("runId") or ""), 900.0)
            sync_status = str(sync_row.get("status") or "").strip().lower()
            if sync_status == "error":
                sync_error = str((sync_row.get("summary") or {}).get("error") or "sync push failed")
                raise RuntimeError(sync_error)

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
            if (
                self._task_running_from_state("fetch")
                or self._task_running_from_state("discovery")
                or self._sync_task_running()
                or self._has_projected_blocking_child_work()
            ):
                return {
                    "started": False,
                    "error": "Another fetch/discovery/sync task is already running",
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
                    "baselineOutputCount": int(baseline_output_count),
                    "finalOutputCount": 0,
                    "jobsPageLoadedCount": int(max(0, jobs_page_loaded_count)),
                }
            )
            self._append_run_history(
                {
                    "id": run_id,
                    "runId": run_id,
                    "type": "pipeline",
                    "status": "started",
                    "startedAt": started_at,
                    "finishedAt": "",
                    "durationMs": 0,
                    "summary": {
                        "baselineOutputCount": int(baseline_output_count),
                        "jobsPageLoadedCount": int(max(0, jobs_page_loaded_count)),
                        "stage": "starting",
                    },
                }
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
