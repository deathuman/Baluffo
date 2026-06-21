"""Jobs pipeline orchestration service used by the admin bridge.

AI boundary owns: bridge-managed jobs pipeline lifecycle, status payloads, and worker coordination.
AI boundary implement in: this file for bridge pipeline orchestration; job fetching stages stay in src.jobs leaves.
AI boundary search before contracts: pipeline task routes, task launch API, pipeline control files, and admin pipeline frontend callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused pipeline service tests.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.bridge.active_task_snapshot import (
    pipeline_status_to_task_row,
    upsert_snapshot_rows,
    write_snapshot,
)
from src.bridge.ops_live_payload import build_pipeline_task_progress
from src.bridge.pipeline_control_files import (
    clear_abort_request,
    read_abort_request,
    write_pipeline_status,
)
from src.bridge.task_abort_evidence import ABORT_TERMINAL_REASON, row_abort_requested

PIPELINE_COMPLETION_NOTIFICATION_MIN_SECONDS = 60.0
CONTROL_STATUS_HEARTBEAT_MIN_SECONDS = 10.0
SYNC_REMOTE_CONFLICT_KIND = "recoverable_remote_conflict"
SYNC_PUSH_WARNING_KIND = "sync_push_failed"
_EXPECTED_PIPELINE_CHILD_BOUNDARY_EXCEPTIONS = (RuntimeError, OSError, ValueError)
_PIPELINE_OPERATIONAL_ERRORS = (RuntimeError, OSError, TypeError, ValueError)


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
        abort_child_run: Callable[[str, str, str], Any] | None = None,
        start_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
        heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None] | None = None,
        finish_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
        fail_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
        cancel_lifecycle_run: Callable[..., dict[str, Any]] | None = None,
        attach_lifecycle_child: Callable[..., dict[str, Any] | None] | None = None,
        clear_task_state: Callable[[str], None] | None = None,
        pipeline_completion_notifier: Callable[[dict[str, Any]], Any] | None = None,
        control_data_dir: Path | None = None,
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
        self._abort_child_run = abort_child_run
        self._start_lifecycle_run = start_lifecycle_run
        self._heartbeat_lifecycle_run = heartbeat_lifecycle_run
        self._finish_lifecycle_run = finish_lifecycle_run
        self._fail_lifecycle_run = fail_lifecycle_run
        self._cancel_lifecycle_run = cancel_lifecycle_run
        self._attach_lifecycle_child = attach_lifecycle_child
        self._pipeline_completion_notifier = pipeline_completion_notifier
        self._completion_notification_run_id = ""
        self._control_data_dir = Path(control_data_dir) if control_data_dir is not None else None
        self._control_status_last_write_monotonic = 0.0
        if self._runtime.abort_requests is None:
            self._runtime.abort_requests = {}

    def _write_control_status(self, payload: dict[str, Any] | None = None) -> None:
        if self._control_data_dir is None:
            return
        if payload is None:
            with self._lock:
                status = dict(self._status)
                progress = status.get("progress")
                if isinstance(progress, dict):
                    status["progress"] = dict(progress)
                status["activeChildren"] = self._copy_control_children(status)
        else:
            status = dict(payload)
            progress = status.get("progress")
            if isinstance(progress, dict):
                status["progress"] = dict(progress)
            status["activeChildren"] = self._copy_control_children(status)
        try:
            snapshot_at = str(self._now_iso() or "")
            write_pipeline_status(
                self._control_data_dir,
                status,
                now_iso=snapshot_at,
            )
            snapshot_status = {**status, "snapshotAt": str(status.get("snapshotAt") or snapshot_at)}
            if bool(status.get("active")):
                upsert_snapshot_rows(
                    self._control_data_dir / "admin-active-task-snapshot.json",
                    [pipeline_status_to_task_row(snapshot_status)],
                    snapshot_at=str(snapshot_status.get("snapshotAt") or snapshot_at),
                )
            elif str(status.get("runId") or "").strip():
                write_snapshot(
                    self._control_data_dir / "admin-active-task-snapshot.json",
                    [pipeline_status_to_task_row(snapshot_status)],
                    snapshot_at=str(snapshot_status.get("snapshotAt") or snapshot_at),
                )
        except OSError:
            self._bridge_log("warn", "jobs_pipeline_control_status_write_failed")

    def _maybe_write_control_status_heartbeat(self) -> None:
        if self._control_data_dir is None:
            return
        now_monotonic = time.monotonic()
        if (
            now_monotonic - self._control_status_last_write_monotonic
            < CONTROL_STATUS_HEARTBEAT_MIN_SECONDS
        ):
            return
        with self._lock:
            status_snapshot = dict(self._status)
            if not bool(status_snapshot.get("active")):
                return
        self._control_status_last_write_monotonic = now_monotonic
        self._write_control_status(status_snapshot)

    def _ingest_control_abort_request(self, run_id: str) -> bool:
        if self._control_data_dir is None:
            return False
        request = read_abort_request(self._control_data_dir, run_id)
        if not request:
            return False
        requests = self._runtime.abort_requests if self._runtime.abort_requests is not None else {}
        requests[str(run_id or "").strip()] = {
            "requestedAt": str(request.get("requestedAt") or self._now_iso() or ""),
            "reason": str(request.get("reason") or "container_gateway_abort").strip(),
        }
        self._runtime.abort_requests = requests
        return True

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

    @staticmethod
    def _copy_control_children(status: dict[str, Any]) -> list[dict[str, Any]]:
        children = status.get("activeChildren")
        if not isinstance(children, list):
            return []
        rows: list[dict[str, Any]] = []
        for child in children:
            if not isinstance(child, dict):
                continue
            row = dict(child)
            progress = row.get("taskProgress")
            row["taskProgress"] = dict(progress) if isinstance(progress, dict) else {}
            summary = row.get("summary")
            row["summary"] = dict(summary) if isinstance(summary, dict) else {}
            rows.append(row)
        return rows[:3]

    @staticmethod
    def _child_stage_task_type(stage: str) -> str:
        normalized = str(stage or "").strip().lower()
        if normalized == "sync_push":
            return "sync"
        if normalized in {"discovery", "fetch"}:
            return normalized
        return ""

    @staticmethod
    def _child_phase_label(task_type: str) -> str:
        labels = {
            "discovery": "Discovery running",
            "fetch": "Fetch running",
            "sync": "Sync push running",
        }
        return labels.get(str(task_type or "").strip().lower(), "Task running")

    def _build_control_child_row(
        self,
        *,
        run_id: str,
        task_type: str,
        child_run_id: str,
        started_at: str = "",
    ) -> dict[str, Any]:
        clean_task_type = str(task_type or "").strip().lower()
        clean_child_run_id = str(child_run_id or "").strip()
        phase_label = self._child_phase_label(clean_task_type)
        return {
            "id": clean_child_run_id,
            "runId": clean_child_run_id,
            "type": clean_task_type,
            "taskType": clean_task_type,
            "active": True,
            "status": "running",
            "displayStatus": "running",
            "startedAt": str(started_at or "").strip(),
            "finishedAt": "",
            "parentRunId": str(run_id or "").strip(),
            "parentTaskType": "pipeline",
            "ownerKind": "pipeline",
            "controlPlaneSource": "pipeline-status",
            "displayOnly": True,
            "taskProgress": {
                "active": True,
                "phaseKey": clean_task_type,
                "phaseLabel": phase_label,
                "mode": "indeterminate",
                "ratio": 0,
                "counts": {},
            },
            "summary": {
                "stage": clean_task_type,
                "pipelineRunId": str(run_id or "").strip(),
                "controlPlane": True,
            },
        }

    def _set_control_child_task(
        self,
        *,
        run_id: str,
        task_type: str,
        child_run_id: str,
        started_at: str = "",
    ) -> None:
        clean_task_type = str(task_type or "").strip().lower()
        clean_child_run_id = str(child_run_id or "").strip()
        if clean_task_type not in {"discovery", "fetch", "sync"} or not clean_child_run_id:
            return
        child_row = self._build_control_child_row(
            run_id=run_id,
            task_type=clean_task_type,
            child_run_id=clean_child_run_id,
            started_at=started_at,
        )
        with self._lock:
            if str(self._status.get("runId") or "").strip() != str(run_id or "").strip():
                return
            if not bool(self._status.get("active")):
                return
            self._status["activeChildren"] = [child_row]
            self._status["activeChildTaskType"] = clean_task_type
            self._status["activeChildRunId"] = clean_child_run_id
            self._status["activeChildPhaseLabel"] = child_row["taskProgress"]["phaseLabel"]
            self._status["activeChildDisplayLabel"] = (
                f"{clean_task_type.title()}: {child_row['taskProgress']['phaseLabel']}"
            )
            status_snapshot = dict(self._status)
        self._write_control_status(status_snapshot)

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
            stage_child_type = self._child_stage_task_type(str(stage or ""))
            if stage_child_type:
                self._status["activeChildren"] = [
                    child
                    for child in self._copy_control_children(self._status)
                    if str(child.get("taskType") or child.get("type") or "").strip().lower()
                    == stage_child_type
                ]
            else:
                self._status["activeChildren"] = []
                self._status["activeChildTaskType"] = ""
                self._status["activeChildRunId"] = ""
                self._status["activeChildPhaseLabel"] = ""
                self._status["activeChildDisplayLabel"] = ""
            status_snapshot = dict(self._status)
        self._write_control_status(status_snapshot)
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
        self._ingest_control_abort_request(run_id)
        requests = self._runtime.abort_requests or {}
        return str(run_id or "").strip() in requests

    def _abort_metadata(self, run_id: str) -> dict[str, Any]:
        requests = self._runtime.abort_requests or {}
        return dict(requests.get(str(run_id or "").strip()) or {})

    @staticmethod
    def _normalized_abortable_child_row(row: dict[str, Any]) -> dict[str, Any] | None:
        task_type = str(row.get("taskType") or row.get("type") or "").strip().lower()
        child_run_id = str(row.get("runId") or row.get("id") or "").strip()
        if task_type not in {"fetch", "discovery"} or not child_run_id:
            return None
        return {**row, "taskType": task_type, "runId": child_run_id}

    @staticmethod
    def _pipeline_parent_run_id(row: dict[str, Any]) -> str:
        summary = row.get("summary") if isinstance(row.get("summary"), dict) else {}
        return str(row.get("parentRunId") or summary.get("pipelineRunId") or "").strip()

    @staticmethod
    def _row_is_active_unfinished(row: dict[str, Any]) -> bool:
        return row.get("active") is not False and not str(row.get("finishedAt") or "").strip()

    def _append_abortable_child_row(
        self,
        rows: list[dict[str, Any]],
        seen: set[tuple[str, str]],
        row: dict[str, Any],
    ) -> None:
        normalized = self._normalized_abortable_child_row(row)
        if normalized is None:
            return
        key = (str(normalized["taskType"]), str(normalized["runId"]))
        if key in seen:
            return
        seen.add(key)
        rows.append(normalized)

    def _projected_run_history_rows(self) -> list[dict[str, Any]]:
        if not callable(self._get_projected_run_history):
            return []
        try:
            projection = self._get_projected_run_history()
        except (AttributeError, RuntimeError, TypeError, ValueError):
            return []
        projected_rows = getattr(projection, "rows", []) if projection is not None else []
        if not isinstance(projected_rows, list):
            return []
        return [row for row in projected_rows if isinstance(row, dict)]

    def _active_abortable_child_rows(self, run_id: str) -> list[dict[str, Any]]:
        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            return []
        rows: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        with self._lock:
            for child in self._copy_control_children(self._status):
                if self._pipeline_parent_run_id(child) == clean_run_id:
                    self._append_abortable_child_row(rows, seen, child)
        for row in self._projected_run_history_rows():
            if self._pipeline_parent_run_id(row) != clean_run_id:
                continue
            if self._row_is_active_unfinished(row):
                self._append_abortable_child_row(rows, seen, row)
        return rows

    def _request_active_child_aborts(self, run_id: str) -> None:
        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            return
        reason = str(self._abort_metadata(clean_run_id).get("reason") or "pipeline_abort").strip()
        for child in self._active_abortable_child_rows(clean_run_id):
            task_type = str(child.get("taskType") or "").strip().lower()
            child_run_id = str(child.get("runId") or "").strip()
            if (
                not task_type
                or not child_run_id
                or self._child_abort_requested(task_type, child_run_id)
            ):
                continue
            if not callable(self._abort_child_run):
                continue
            try:
                self._abort_child_run(task_type, child_run_id, reason)
            except (RuntimeError, TypeError, ValueError, OSError) as exc:
                self._bridge_log(
                    "warn",
                    "jobs_pipeline_child_abort_request_failed",
                    runId=clean_run_id,
                    childTask=task_type,
                    childRunId=child_run_id,
                    error=str(exc),
                )

    def _has_live_abortable_child(self, run_id: str) -> bool:
        for child in self._active_abortable_child_rows(run_id):
            task_type = str(child.get("taskType") or "").strip().lower()
            child_run_id = str(child.get("runId") or "").strip()
            if self._child_task_has_live_evidence(task_type, child_run_id):
                return True
        return False

    def _mark_abort_pending(self, run_id: str, *, defer_sync: bool = False) -> None:
        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            return
        metadata = self._abort_metadata(clean_run_id)
        requested_at = str(metadata.get("requestedAt") or self._now_iso() or "").strip()
        reason = str(metadata.get("reason") or "").strip()
        with self._lock:
            if str(self._status.get("runId") or "").strip() != clean_run_id:
                return
            if not bool(self._status.get("active")):
                return
            next_stage = "abort_pending_sync" if defer_sync else "aborting"
            self._status["stage"] = next_stage
            self._status["progress"] = self._pipeline_progress(
                3 if defer_sync else 0,
                3,
                "Abort after sync..." if defer_sync else "Aborting...",
            )
            progress = self._pipeline_lifecycle_progress(dict(self._status))
            status_snapshot = dict(self._status)
        self._write_control_status(status_snapshot)
        if callable(self._heartbeat_lifecycle_run):
            self._heartbeat_lifecycle_run(
                clean_run_id,
                "pipeline",
                stage=next_stage,
                progress=progress,
                summary={
                    "stage": next_stage,
                    "abortRequestedAt": requested_at,
                    "abortReason": reason,
                },
            )

    def _check_abort(self, run_id: str, *, defer_sync: bool = False) -> None:
        clean_run_id = str(run_id or "").strip()
        if not self._abort_requested(clean_run_id):
            return
        with self._lock:
            stage = str(self._status.get("stage") or "").strip().lower()
        if defer_sync and stage in {"sync_push", "abort_pending_sync"}:
            self._mark_abort_pending(clean_run_id, defer_sync=True)
            return
        self._mark_abort_pending(clean_run_id)
        self._request_active_child_aborts(clean_run_id)
        if self._has_live_abortable_child(clean_run_id):
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
            status_snapshot = dict(self._status)
        self._write_control_status(status_snapshot)
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
        if self._control_data_dir is not None:
            clear_abort_request(self._control_data_dir, clean_run_id)
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
            progress = self._pipeline_lifecycle_progress(dict(self._status))
            status_snapshot = dict(self._status)
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
                        terminal_reason=(
                            "completed_with_warnings" if completed_with_warnings else "completed"
                        ),
                        summary={
                            "baselineOutputCount": baseline,
                            "jobsPageLoadedCount": loaded,
                            "finalOutputCount": int(final_output_count or 0),
                            "updatesFound": bool(updates_found),
                            "warnings": clean_warnings,
                            "syncWarning": clean_sync_warning,
                            "completedWithWarnings": bool(completed_with_warnings),
                        },
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

    def _child_terminal_snapshot(self, task_type: str, run_id: str = "") -> Any:
        snapshot = self._get_child_task_snapshot(task_type, run_id)
        if snapshot is None or bool(getattr(snapshot, "active", False)):
            return None
        finished_at = str(getattr(snapshot, "finished_at", "") or "").strip()
        terminal_status = str(getattr(snapshot, "terminal_status", "") or "").strip().lower()
        if finished_at or terminal_status or bool(getattr(snapshot, "explicit_dead", False)):
            return snapshot
        return None

    @staticmethod
    def _child_terminal_error(snapshot: Any, report_name: str) -> tuple[str, str]:
        terminal_status = str(getattr(snapshot, "terminal_status", "") or "").strip().lower()
        summary = getattr(snapshot, "summary", {})
        if not isinstance(summary, dict):
            summary = {}
        terminal_reason = str(
            summary.get("terminalReason")
            or summary.get("terminal_reason")
            or getattr(snapshot, "terminal_reason", "")
            or terminal_status
            or "child_terminal_without_terminal_report"
        ).strip()
        error = str(summary.get("error") or "").strip()
        if not error:
            error = f"{report_name} child ended before terminal report"
        return terminal_reason, error

    def _raise_for_terminal_child_without_report(
        self,
        *,
        report_name: str,
        task_type: str,
        task_run_id: str,
        snapshot: Any,
        report: dict[str, Any],
    ) -> None:
        terminal_reason, error = self._child_terminal_error(snapshot, report_name)
        child_run_id = str(getattr(snapshot, "run_id", "") or task_run_id or "").strip()
        terminal_status = str(getattr(snapshot, "terminal_status", "") or "").strip().lower()
        self._bridge_log(
            "warn",
            "jobs_pipeline_child_terminal_without_terminal_report",
            taskType=str(task_type or "").strip(),
            childRunId=child_run_id,
            terminalStatus=terminal_status,
            terminalReason=terminal_reason,
            reportName=report_name,
            reportRunId=str((report or {}).get("runId") or ""),
            reportFinishedAt=str((report or {}).get("finishedAt") or ""),
        )
        if terminal_status in {"canceled", "cancelled"} or terminal_reason == ABORT_TERMINAL_REASON:
            raise PipelineAbortRequested("pipeline child abort requested")
        self._fail_child_lifecycle(
            task_type,
            task_run_id,
            terminal_reason=terminal_reason or "child_terminal_without_terminal_report",
            error=error,
        )
        raise TimeoutError(error)

    @staticmethod
    def _is_duplicate_task_response(result: dict[str, Any] | None) -> bool:
        return bool(isinstance(result, dict) and result.get("alreadyRunning"))

    def _wait_for_child_report(self, *, phase: str, **kwargs: Any) -> dict[str, Any]:
        try:
            return self.wait_for_report_completion(**kwargs)
        except PipelineAbortRequested:
            raise
        except _EXPECTED_PIPELINE_CHILD_BOUNDARY_EXCEPTIONS as exc:
            raise RuntimeError(f"{phase}: {exc}") from exc

    @staticmethod
    def _report_wait_now() -> Any:
        from datetime import UTC, datetime

        return datetime.now(UTC)

    @staticmethod
    def _report_wait_sleep(seconds: float) -> None:
        from threading import Event

        Event().wait(seconds)

    def _wait_for_sync_push_row(self, run_id: str) -> dict[str, Any]:
        try:
            return self._wait_for_sync_completion(run_id, 900.0)
        except _EXPECTED_PIPELINE_CHILD_BOUNDARY_EXCEPTIONS as exc:
            raise RuntimeError(f"sync_push: {exc}") from exc

    def _trigger_discovery_child(self) -> Any:
        try:
            return self._trigger_discovery_task(
                route_name="/tasks/run-jobs-pipeline",
                enable_auto_sync_watch=False,
            )
        except _EXPECTED_PIPELINE_CHILD_BOUNDARY_EXCEPTIONS as exc:
            raise RuntimeError(f"discovery_launch: {exc}") from exc

    def _start_fetch_child(self) -> dict[str, Any]:
        try:
            return self._start_fetcher_task({"preset": "default"})
        except _EXPECTED_PIPELINE_CHILD_BOUNDARY_EXCEPTIONS as exc:
            raise RuntimeError(f"fetch_launch: {exc}") from exc

    def _start_sync_push_child(self) -> dict[str, Any]:
        try:
            return self._start_sync_task("push", reason="jobs_pipeline", automatic=False)
        except _EXPECTED_PIPELINE_CHILD_BOUNDARY_EXCEPTIONS as exc:
            raise RuntimeError(f"sync_push: {exc}") from exc

    @staticmethod
    def _is_recoverable_sync_conflict(message: str) -> bool:
        text = str(message or "").strip().lower()
        if not text:
            return False
        return bool(
            "remote_conflict" in text
            or "sha does not match" in text
            or ("is at " in text and " but expected " in text)
            or "not a fast-forward" in text
            or "remote write conflict" in text
            or "manifest moved" in text
        )

    @staticmethod
    def _sync_warning_payload(message: str) -> dict[str, Any]:
        clean_message = str(message or "sync push failed").strip() or "sync push failed"
        kind = (
            SYNC_REMOTE_CONFLICT_KIND
            if PipelineService._is_recoverable_sync_conflict(clean_message)
            else SYNC_PUSH_WARNING_KIND
        )
        return {
            "kind": kind,
            "stage": "sync_push",
            "message": clean_message,
            "recommendedAction": "Review the Sync tab, pull the latest remote state, then retry sync.",
            "blocking": False,
            "clearedBy": "successful sync pull/push or a later pipeline sync stage",
        }

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

    def _finish_matching_terminal_child_report(
        self,
        report: dict[str, Any],
        *,
        started_dt: Any,
        started_at: str,
        task_type: str,
        task_run_id: str,
    ) -> dict[str, Any] | None:
        if not self._report_matches_started_run(
            report,
            started_dt=started_dt,
            task_run_id=task_run_id,
        ):
            return None
        report_started = self._parse_iso(report.get("startedAt"))
        report_finished = self._parse_iso(report.get("finishedAt"))
        if not (report_finished and report_started and report_finished >= report_started):
            return None
        if self._child_abort_requested(task_type, task_run_id):
            raise PipelineAbortRequested("pipeline child abort requested")
        if self._refresh_child_lifecycle_evidence(task_type, task_run_id, started_at):
            self._heartbeat_pipeline_wait()
        self._finish_child_lifecycle_from_report(task_type, task_run_id, report)
        return report

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
            warning = self._sync_warning_payload(error)
            self._set_completed(
                status="warning",
                final_output_count=self._current_fetch_output_count(),
                warnings=[warning],
                sync_warning=warning,
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
        self._maybe_write_control_status_heartbeat()

    def _attach_lifecycle_child_row(
        self,
        *,
        run_id: str,
        task_type: str,
        child_run_id: str,
        child_started_at: str = "",
    ) -> None:
        self._set_control_child_task(
            run_id=run_id,
            task_type=task_type,
            child_run_id=child_run_id,
            started_at=child_started_at,
        )
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
            summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
            error = str(summary.get("error") or "discovery failed").strip()
            raise RuntimeError(f"discovery_wait: {error}")
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
            self._report_wait_sleep(1.0)

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


__all__ = ["PipelineRuntime", "PipelineService"]
