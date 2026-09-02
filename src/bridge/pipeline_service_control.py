"""Pipeline control-status writes and cooperative abort machinery.

AI boundary owns: control status/heartbeat writes, stage marking, and cooperative
abort request handling for bridge-managed pipeline runs.
AI boundary implement in: this mixin leaf for control/abort mechanics; lifecycle,
child coordination, status reconciliation, and stage orchestration stay in sibling
mixin leaves consumed by ``PipelineService``.
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
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

from .pipeline_service_types import PipelineAbortRequested, PipelineServiceState

CONTROL_STATUS_HEARTBEAT_MIN_SECONDS = 10.0


class _PipelineServiceControlMixin(PipelineServiceState):
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
            self._status["heartbeatAt"] = self._now_iso()
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

    def _normalize_child_progress_for_refresh(
        self,
        clean_task_type: str,
        report: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Resolve and normalize the child's taskProgress before locking.

        ponytail: the fetch report is intentionally sparse during
        executing_sources (0.2.114 Umbrel pressure fix — the report only gets
        written at phase changes), so its taskProgress counts freeze at the
        phase-entry snapshot while the run grinds on. The live counts live in
        the sibling task-state file (jobs-fetch-tasks.json) — the same surface
        the admin ops live page reads — so prefer it for the fetch child and
        the jobs caption tracks real resolved/total/rate/ETA during the run.
        """
        progress = report.get("taskProgress")
        if not isinstance(progress, dict):
            return None
        if clean_task_type == "fetch":
            live_progress = self._load_live_fetch_task_progress(
                report_run_id=str(report.get("runId") or "").strip()
            )
            if live_progress is not None:
                progress = live_progress
        progress = {**progress, "active": True}
        if not str(progress.get("updatedAt") or "").strip():
            progress["updatedAt"] = self._now_iso()
        counts = progress.get("counts")
        progress["counts"] = dict(counts) if isinstance(counts, dict) else {}
        return progress

    @staticmethod
    def _project_child_progress_counts(
        child: dict[str, Any],
        progress: dict[str, Any],
        *,
        now_iso: str,
    ) -> dict[str, Any]:
        """Stamp countsUpdatedAt onto progress for an already-locked child row."""
        previous = child.get("taskProgress")
        if not isinstance(previous, dict):
            previous = {}
        previous_counts_value = previous.get("counts")
        previous_counts: dict[str, Any] = (
            dict(previous_counts_value) if isinstance(previous_counts_value, dict) else {}
        )
        # ponytail: the child report's taskProgress.updatedAt is its *heartbeat*
        # stamp, which is refreshed even when counters are frozen. Track the last
        # moment the counters actually moved so the UI can reassure the user a
        # quiet-but-alive stage is still working.
        if (
            previous_counts != progress["counts"]
            or not str(previous.get("countsUpdatedAt") or "").strip()
        ):
            progress["countsUpdatedAt"] = now_iso
        else:
            progress["countsUpdatedAt"] = str(previous.get("countsUpdatedAt") or now_iso)
        return progress

    def _refresh_live_child_task_progress(
        self,
        task_type: str,
        report: dict[str, Any],
    ) -> None:
        """Project the child's live taskProgress onto the active-child row.

        The pipeline wait loops already load the child report (discovery/fetch/sync)
        on every iteration, so this reuses that read to keep the pipeline status
        payload's active child carrying real counters / ratio / ETA without extra
        I/O. For fetch, the report stays sparse during source execution (Umbrel
        pressure fix), so the live task-state sibling is preferred — the same
        surface the admin ops live page reads. The update is in-memory only; the
        control-status file is written on the existing bounded heartbeat cadence.
        This is what lets the jobs CTA render determinate sub-progress
        ("128/431 sources · ETA 4m") for the whole stage instead of a flat 2/3
        fill.
        """
        clean_task_type = str(task_type or "").strip().lower()
        if clean_task_type not in {"discovery", "fetch", "sync"}:
            return
        progress = self._normalize_child_progress_for_refresh(clean_task_type, report)
        if progress is None:
            return
        now_iso = self._now_iso()
        with self._lock:
            if not bool(self._status.get("active")):
                return
            children = self._status.get("activeChildren")
            if not isinstance(children, list) or not children:
                return
            child = children[0]
            if not isinstance(child, dict):
                return
            existing_type = str(child.get("taskType") or child.get("type") or "").strip().lower()
            if existing_type != clean_task_type:
                return
            progress = self._project_child_progress_counts(child, progress, now_iso=now_iso)
            child["taskProgress"] = progress
            phase_label = str(progress.get("phaseLabel") or "").strip()
            self._status["activeChildPhaseLabel"] = phase_label
            self._status["activeChildDisplayLabel"] = (
                f"{clean_task_type.title()}: {phase_label}" if phase_label else ""
            )

    def _load_live_fetch_task_progress(
        self,
        *,
        report_run_id: str = "",
    ) -> dict[str, Any] | None:
        """Return the live fetch task-state taskProgress, or None when unavailable.

        The task-state file (``jobs-fetch-tasks.json``, sibling of the fetch
        report) is written on a 5 s cadence by the running fetch stage, while the
        report itself stays sparse during source execution. Returns None when the
        file is missing, empty, carries no counts, or belongs to a different run
        (stale file from a previous fetch).
        """
        try:
            report_path = self._fetch_report_path
            if not report_path:
                return None
            task_state_path = Path(report_path).with_name("jobs-fetch-tasks.json")
            payload = self._load_runtime_evidence(task_state_path, {})
        except (OSError, TypeError, ValueError):
            return None
        if not isinstance(payload, dict):
            return None
        task_run_id = str(payload.get("runId") or "").strip()
        if report_run_id and task_run_id and report_run_id != task_run_id:
            return None
        progress = payload.get("taskProgress")
        if not isinstance(progress, dict):
            return None
        counts = progress.get("counts")
        if not isinstance(counts, dict) or not counts:
            return None
        return {**progress, "active": True}

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

    # ponytail: keep this inline with the four user-visible stages, no config knob.
    _TOP_LEVEL_STAGES = {"preflight", "discovery", "fetch", "sync_push", "completed"}

    def _mark_stage(
        self, *, stage: str, current_step: int, total_steps: int, label: str, error: str = ""
    ) -> None:
        with self._lock:
            self._status["heartbeatAt"] = self._now_iso()
            prev_stage = str(self._status.get("stage") or "").strip()
            new_stage = str(stage or "unknown")
            self._status["stage"] = new_stage
            self._status["progress"] = self._pipeline_progress(current_step, total_steps, label)
            progress = self._pipeline_lifecycle_progress(dict(self._status))
            run_id = str(self._status.get("runId") or "")
            entered_at = self._now_iso()
            ledger = self._status.setdefault("_stageLedger", [])
            # ponytail: hard cap, no config knob — see plan
            if isinstance(ledger, list):
                ledger.append(
                    {
                        "stage": new_stage,
                        "enteredAt": entered_at,
                        "label": str(label or ""),
                    }
                )
                if len(ledger) > 64:
                    del ledger[:-64]
            # ponytail: track top-level user-visible transitions (separate from sub-stage
            # ledger), used by the jobs CTA / admin to show "source discovery ->
            # fetching" moments without parsing stageLedger each tick.
            transitions = self._status.setdefault("_stageTransitions", [])
            if (
                isinstance(transitions, list)
                and new_stage in self._TOP_LEVEL_STAGES
                and new_stage != prev_stage
            ):
                transitions.append(
                    {
                        "from": prev_stage,
                        "to": new_stage,
                        "at": entered_at,
                    }
                )
                if len(transitions) > 16:
                    del transitions[:-16]
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
        summary_value = row.get("summary")
        summary = summary_value if isinstance(summary_value, dict) else {}
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

    @staticmethod
    def _append_unique_warning(warnings: list[str], warning: str) -> None:
        text = str(warning or "").strip()
        if text and text not in warnings:
            warnings.append(text)

    @staticmethod
    def _child_abort_result_warnings(result: Any, task_type: str, child_run_id: str) -> list[str]:
        if not isinstance(result, Mapping):
            return []
        warnings = [
            str(item).strip() for item in result.get("warnings", []) if str(item or "").strip()
        ]
        if not bool(result.get("abortAccepted") or result.get("ok")):
            detail = str(result.get("error") or result.get("warning") or "not_accepted").strip()
            warnings.append(f"child_abort_not_accepted:{task_type}:{child_run_id}:{detail}")
        return warnings

    def _request_active_child_aborts(self, run_id: str) -> list[str]:
        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            return []
        reason = str(self._abort_metadata(clean_run_id).get("reason") or "pipeline_abort").strip()
        warnings: list[str] = []
        for child in self._active_abortable_child_rows(clean_run_id):
            task_type = str(child.get("taskType") or "").strip().lower()
            child_run_id = str(child.get("runId") or "").strip()
            if not task_type or not child_run_id:
                continue
            if not callable(self._abort_child_run):
                warning = f"child_abort_unavailable:{task_type}:{child_run_id}"
                self._append_unique_warning(warnings, warning)
                self._bridge_log(
                    "warn",
                    "jobs_pipeline_child_abort_unavailable",
                    runId=clean_run_id,
                    childTask=task_type,
                    childRunId=child_run_id,
                )
                continue
            try:
                result = self._abort_child_run(task_type, child_run_id, reason)
            except (RuntimeError, TypeError, ValueError, OSError) as exc:
                warning = (
                    f"child_abort_request_failed:{task_type}:{child_run_id}:{type(exc).__name__}"
                )
                self._append_unique_warning(warnings, warning)
                self._bridge_log(
                    "warn",
                    "jobs_pipeline_child_abort_request_failed",
                    runId=clean_run_id,
                    childTask=task_type,
                    childRunId=child_run_id,
                    error=str(exc),
                )
                continue
            result_warnings = self._child_abort_result_warnings(result, task_type, child_run_id)
            for warning in result_warnings:
                self._append_unique_warning(warnings, warning)
            self._bridge_log(
                "info",
                "jobs_pipeline_child_abort_requested",
                runId=clean_run_id,
                childTask=task_type,
                childRunId=child_run_id,
                warnings=result_warnings,
            )
        return warnings

    def _has_live_abortable_child(self, run_id: str) -> bool:
        for child in self._active_abortable_child_rows(run_id):
            task_type = str(child.get("taskType") or "").strip().lower()
            child_run_id = str(child.get("runId") or "").strip()
            if self._child_task_has_live_evidence(task_type, child_run_id):
                return True
        return False

    def _mark_abort_pending(
        self,
        run_id: str,
        *,
        defer_sync: bool = False,
        warnings: Sequence[str] | None = None,
    ) -> None:
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
            current_warnings = list(self._status.get("warnings") or [])
            for warning in warnings or ():
                text = str(warning or "").strip()
                if text and text not in current_warnings:
                    current_warnings.append(text)
            if current_warnings:
                self._status["warnings"] = current_warnings
            progress = self._pipeline_lifecycle_progress(dict(self._status))
            status_snapshot = dict(self._status)
        self._write_control_status(status_snapshot)
        if callable(self._heartbeat_lifecycle_run):
            summary: dict[str, Any] = {
                "stage": next_stage,
                "abortRequestedAt": requested_at,
                "abortReason": reason,
            }
            if current_warnings:
                summary["warnings"] = list(current_warnings)
            self._heartbeat_lifecycle_run(
                clean_run_id,
                "pipeline",
                stage=next_stage,
                progress=progress,
                summary=summary,
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
        warnings = self._request_active_child_aborts(clean_run_id)
        self._mark_abort_pending(clean_run_id, warnings=warnings)
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
