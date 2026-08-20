"""Ops API task_state — task-state payloads and compaction/enrichment helpers.

AI boundary owns: task-state payloads and compaction/enrichment helpers.
AI boundary implement in: this leaf for the OpsApi mixin group; the coordinator
composes `OpsApi` from the mixin leaves and keeps the public construction surface.
AI boundary verify: `npm run lint:repo-guardrails` plus focused ops API tests.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from src.bridge import active_task_snapshot as _active_task_snapshot
from src.bridge import ops_live_payload as _ops_live_payload
from src.bridge import ops_task_live as _ops_task_live
from src.bridge.ops_api_core import OpsApiState, _run_id, _task_type
from src.bridge.ops_api_live import _merge_bounded_fetch_summary_for_task_state_row
from src.bridge.task_abort_evidence import row_abort_requested
from src.shared.json_shapes import as_json_object
from src.shared.live_task import (
    LiveTaskProgress,
    TaskStatePayload,
    TaskStateRow,
)


def _child_progress_label(child_row: dict[str, Any]) -> str:
    progress = as_json_object(child_row.get("taskProgress"))
    summary = as_json_object(child_row.get("summary"))
    return str(
        progress.get("phaseLabel")
        or progress.get("phaseKey")
        or summary.get("phaseLabel")
        or summary.get("phase")
        or ""
    ).strip()


def _enrich_pipeline_row_with_child(
    pipeline_row: dict[str, Any],
    child_row: dict[str, Any],
) -> dict[str, Any]:
    child_type = str(child_row.get("type") or child_row.get("taskType") or "").strip().lower()
    child_label = _child_progress_label(child_row)
    if not child_type or not child_label:
        return pipeline_row
    display_type = {
        "discovery": "Discovery",
        "fetch": "Fetch",
        "sync": "Sync",
    }.get(child_type, child_type.title())
    child_display_label = f"{display_type}: {child_label}"
    progress = as_json_object(pipeline_row.get("taskProgress"))
    summary = as_json_object(pipeline_row.get("summary"))
    return {
        **pipeline_row,
        "taskProgress": {
            **progress,
            "active": True,
            "phaseKey": f"{child_type}_child",
            "phaseLabel": child_display_label,
            "activeChildTaskType": child_type,
            "activeChildRunId": str(child_row.get("runId") or child_row.get("id") or "").strip(),
        },
        "summary": {
            **summary,
            "activeChildTaskType": child_type,
            "activeChildRunId": str(child_row.get("runId") or child_row.get("id") or "").strip(),
            "activeChildPhaseLabel": child_label,
            "activeChildDisplayLabel": child_display_label,
        },
    }


def _enrich_active_row_for_type(
    route_row: dict[str, Any],
    *,
    task_type: str,
    run_id: str,
    row: dict[str, Any],
    pipeline_row: Mapping[str, Any],
    pipeline_run_id: str,
    pipeline_stage: str,
    fetch_live: dict[str, Any],
    fetch_run_id: str,
    discovery_live: dict[str, Any],
    discovery_run_id: str,
    sync_live: dict[str, Any],
    sync_run_id: str,
) -> dict[str, Any]:
    if task_type == "pipeline" and pipeline_run_id and run_id == pipeline_run_id:
        route_row = {**route_row, **pipeline_row, "active": True, "finishedAt": ""}
        route_row["stage"] = pipeline_stage or str(route_row.get("stage") or "")
        route_row["summary"] = {
            **as_json_object(pipeline_row.get("summary")),
            **as_json_object(row.get("summary")),
            "stage": pipeline_stage or str(as_json_object(row.get("summary")).get("stage") or ""),
        }
    elif task_type in ("fetch", "discovery", "sync"):
        live = {
            "fetch": fetch_live,
            "discovery": discovery_live,
            "sync": sync_live,
        }.get(task_type, {})
        live_run_id = {
            "fetch": fetch_run_id,
            "discovery": discovery_run_id,
            "sync": sync_run_id,
        }.get(task_type, "")
        if live_run_id and run_id == live_run_id and bool(live.get("active", False)):
            route_row = {
                **route_row,
                **live,
                "id": run_id,
                "runId": run_id,
                "type": task_type,
                "taskType": task_type,
                "active": True,
                "finishedAt": "",
                "lifecycleStatus": str(
                    row.get("lifecycleStatus") or row.get("status") or ""
                ).strip(),
                "parentRunId": str(row.get("parentRunId") or "").strip(),
                "parentTaskType": str(row.get("parentTaskType") or "").strip().lower(),
                "ownerKind": str(row.get("ownerKind") or "").strip().lower(),
                "ownerPid": row.get("ownerPid"),
                "stage": str(row.get("stage") or "").strip(),
            }
    return route_row


def _enrich_pipeline_rows_with_children(
    task_by_key: dict[tuple[str, str], dict[str, Any]],
) -> None:
    active_children = [
        row
        for row in task_by_key.values()
        if str(row.get("parentTaskType") or "").strip().lower() == "pipeline"
        and str(row.get("parentRunId") or "").strip()
        and bool(row.get("active"))
    ]
    if not active_children:
        return
    child_priority = {"discovery": 0, "fetch": 1, "sync": 2}
    active_children.sort(
        key=lambda row: child_priority.get(
            str(row.get("type") or row.get("taskType") or "").strip().lower(),
            99,
        )
    )
    for key, row in list(task_by_key.items()):
        task_type, run_id = key
        if task_type != "pipeline" or not run_id:
            continue
        child = next(
            (
                candidate
                for candidate in active_children
                if str(candidate.get("parentRunId") or "").strip() == run_id
            ),
            None,
        )
        if child is not None:
            task_by_key[key] = _enrich_pipeline_row_with_child(row, child)


def _pipeline_status_to_task_row(pipeline_status: dict[str, Any]) -> TaskStateRow:
    status = pipeline_status if isinstance(pipeline_status, dict) else {}
    active = bool(status.get("active"))
    return {
        "taskType": "pipeline",
        "type": "pipeline",
        "runId": str(status.get("runId") or "").strip(),
        "id": str(status.get("runId") or "").strip(),
        "active": active,
        "startedAt": str(status.get("startedAt") or "").strip(),
        "heartbeatAt": str(
            status.get("heartbeatAt")
            or as_json_object(status.get("runtime")).get("heartbeatAt")
            or ""
        ).strip(),
        "finishedAt": "" if active else str(status.get("finishedAt") or "").strip(),
        "status": "running" if active else str(status.get("stage") or "").strip().lower(),
        "lifecycleStatus": "running" if active else "",
        "stage": str(status.get("stage") or "").strip().lower(),
        "taskProgress": cast(
            LiveTaskProgress, _ops_live_payload.build_pipeline_task_progress(status)
        ),
        "summary": {
            "stage": str(status.get("stage") or "").strip().lower(),
            "updatesFound": bool(status.get("updatesFound")),
            "refreshRecommended": bool(status.get("refreshRecommended")),
        },
        "outputs": {},
    }


_TASK_STATE_SUMMARY_KEYS = {
    "taskType",
    "type",
    "runId",
    "id",
    "active",
    "startedAt",
    "heartbeatAt",
    "finishedAt",
    "status",
    "lifecycleStatus",
    "stage",
    "parentRunId",
    "parentTaskType",
    "ownerKind",
    "ownerPid",
    "taskProgress",
    "progress",
    "summary",
    "outputs",
    "error",
    "label",
}


def _compact_task_state_row(row: dict[str, Any]) -> TaskStateRow:
    compact = {key: row.get(key) for key in _TASK_STATE_SUMMARY_KEYS if key in row}
    work_items = row.get("workItems")
    if isinstance(work_items, list):
        compact["workItemCount"] = len(work_items)
        compact["workItemsTruncated"] = len(work_items) > 0
    recent_events = row.get("recentEvents")
    if isinstance(recent_events, list):
        compact["recentEventCount"] = len(recent_events)
        compact["recentEvents"] = list(recent_events[-5:])
        compact["recentEventsTruncated"] = len(recent_events) > 5
    return cast(TaskStateRow, compact)


def _compact_task_state_payload(payload: dict[str, Any]) -> TaskStatePayload:
    tasks = [
        _compact_task_state_row(row) for row in payload.get("tasks", []) if isinstance(row, dict)
    ]
    compact: dict[str, Any] = {
        **{key: value for key, value in payload.items() if key not in {"tasks", "count"}},
        "tasks": tasks,
        "count": len(tasks),
        "summary": True,
    }
    return cast(TaskStatePayload, compact)


class OpsApiTaskStateMixin(OpsApiState):
    def get_current_task_state_payload(self) -> TaskStatePayload:
        lifecycle_current = self._current_lifecycle_rows()
        projection = self.get_projected_run_history()
        fetch_live_payload = _ops_task_live.get_task_live_payload(
            self._task_live_context(),
            "fetch",
            projection=projection,
        )
        fetch_live_run_id = _run_id(fetch_live_payload)
        discovery_live_payload = _ops_task_live.get_task_live_payload(
            self._task_live_context(),
            "discovery",
            projection=projection,
        )
        discovery_live_run_id = _run_id(discovery_live_payload)
        sync_live_payload = _ops_task_live.get_task_live_payload(
            self._task_live_context(),
            "sync",
            projection=projection,
        )
        sync_live_run_id = _run_id(sync_live_payload)
        pipeline_status = self._deps.get_jobs_pipeline_status_payload()
        pipeline_row = cast(
            dict[str, Any],
            _pipeline_status_to_task_row(pipeline_status)
            if isinstance(pipeline_status, dict) and bool(pipeline_status.get("active"))
            else {},
        )
        pipeline_run_id = _run_id(pipeline_row)
        pipeline_stage = str(pipeline_row.get("stage") or "").strip().lower()

        parent_stage_by_run_id = {
            _run_id(row): str(
                row.get("stage") or as_json_object(row.get("summary")).get("stage") or ""
            )
            .strip()
            .lower()
            for row in lifecycle_current
            if _task_type(row) == "pipeline" and _run_id(row)
        }
        if pipeline_run_id and pipeline_stage:
            parent_stage_by_run_id[pipeline_run_id] = pipeline_stage

        task_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        diagnostics: list[dict[str, Any]] = []
        for row in lifecycle_current:
            task_type = _task_type(row)
            run_id = _run_id(row)
            if not task_type or not run_id:
                continue
            parent_task_type = str(row.get("parentTaskType") or "").strip().lower()
            parent_run_id = str(row.get("parentRunId") or "").strip()
            if parent_task_type == "pipeline":
                parent_stage = parent_stage_by_run_id.get(parent_run_id, "")
                if not parent_stage and row_abort_requested(row):
                    diagnostics.append(
                        {
                            "code": "pipeline_child_parent_inactive_after_abort",
                            "taskType": task_type,
                            "runId": run_id,
                            "parentRunId": parent_run_id,
                        }
                    )
                elif not parent_stage or parent_stage != task_type:
                    diagnostics.append(
                        {
                            "code": "pipeline_child_stage_mismatch",
                            "taskType": task_type,
                            "runId": run_id,
                            "parentRunId": parent_run_id,
                            "parentStage": parent_stage,
                        }
                    )
                    continue
            route_row = {**row, "active": True, "finishedAt": ""}
            route_row = _enrich_active_row_for_type(
                route_row,
                task_type=task_type,
                run_id=run_id,
                row=row,
                pipeline_row=pipeline_row,
                pipeline_run_id=pipeline_run_id,
                pipeline_stage=pipeline_stage,
                fetch_live=fetch_live_payload,
                fetch_run_id=fetch_live_run_id,
                discovery_live=discovery_live_payload,
                discovery_run_id=discovery_live_run_id,
                sync_live=sync_live_payload,
                sync_run_id=sync_live_run_id,
            )
            task_by_key[(task_type, run_id)] = route_row
        if pipeline_row and pipeline_run_id:
            key = ("pipeline", pipeline_run_id)
            existing = task_by_key.get(key)
            if existing is None:
                task_by_key[key] = pipeline_row
            else:
                task_by_key[key] = {**existing, **pipeline_row, "active": True, "finishedAt": ""}
                task_by_key[key]["stage"] = pipeline_stage or str(existing.get("stage") or "")
        _enrich_pipeline_rows_with_children(task_by_key)
        tasks: list[TaskStateRow] = cast(
            list[TaskStateRow],
            sorted(
                list(task_by_key.values()),
                key=lambda row: str(row.get("startedAt") or ""),
                reverse=True,
            ),
        )
        return {
            "tasks": tasks,
            "count": len(tasks),
            "diagnostics": diagnostics,
        }

    def get_current_task_state_summary_payload(self) -> TaskStatePayload:
        pipeline_status = self._deps.get_jobs_pipeline_status_payload()
        snapshot = self._fresh_active_task_snapshot()
        if self._should_use_hot_snapshot(snapshot, pipeline_status):
            hot_payload = _active_task_snapshot.task_state_summary_from_snapshot(
                snapshot,
                pipeline_status=pipeline_status,
            )
            if hot_payload is not None:
                return cast(TaskStatePayload, hot_payload)

        lifecycle_current = self._current_lifecycle_rows()
        pipeline_row = cast(
            dict[str, Any],
            _pipeline_status_to_task_row(pipeline_status)
            if isinstance(pipeline_status, dict) and bool(pipeline_status.get("active"))
            else {},
        )
        pipeline_run_id = _run_id(pipeline_row)
        pipeline_stage = str(pipeline_row.get("stage") or "").strip().lower()
        parent_stage_by_run_id = {
            _run_id(row): str(
                row.get("stage") or as_json_object(row.get("summary")).get("stage") or ""
            )
            .strip()
            .lower()
            for row in lifecycle_current
            if _task_type(row) == "pipeline" and _run_id(row)
        }
        if pipeline_run_id and pipeline_stage:
            parent_stage_by_run_id[pipeline_run_id] = pipeline_stage

        task_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        diagnostics: list[dict[str, Any]] = []
        for row in lifecycle_current:
            task_type = _task_type(row)
            run_id = _run_id(row)
            if not task_type or not run_id:
                continue
            parent_task_type = str(row.get("parentTaskType") or "").strip().lower()
            parent_run_id = str(row.get("parentRunId") or "").strip()
            if parent_task_type == "pipeline":
                parent_stage = parent_stage_by_run_id.get(parent_run_id, "")
                if not parent_stage and row_abort_requested(row):
                    diagnostics.append(
                        {
                            "code": "pipeline_child_parent_inactive_after_abort",
                            "taskType": task_type,
                            "runId": run_id,
                            "parentRunId": parent_run_id,
                        }
                    )
                elif not parent_stage or parent_stage != task_type:
                    diagnostics.append(
                        {
                            "code": "pipeline_child_stage_mismatch",
                            "taskType": task_type,
                            "runId": run_id,
                            "parentRunId": parent_run_id,
                            "parentStage": parent_stage,
                        }
                    )
                    continue
            route_row = {
                **row,
                "id": run_id,
                "runId": run_id,
                "type": task_type,
                "taskType": task_type,
                "active": True,
                "finishedAt": "",
            }
            route_row = _merge_bounded_fetch_summary_for_task_state_row(
                route_row,
                paths=self._paths,
                task_type=task_type,
                run_id=run_id,
                lifecycle_row=row,
            )
            task_by_key[(task_type, run_id)] = cast(
                dict[str, Any], _compact_task_state_row(route_row)
            )
        if pipeline_row and pipeline_run_id:
            key = ("pipeline", pipeline_run_id)
            existing = task_by_key.get(key)
            task_by_key[key] = cast(
                dict[str, Any],
                _compact_task_state_row(
                    {**(existing or {}), **pipeline_row, "active": True, "finishedAt": ""}
                ),
            )
        _enrich_pipeline_rows_with_children(task_by_key)
        tasks = sorted(
            list(task_by_key.values()),
            key=lambda row: str(row.get("startedAt") or ""),
            reverse=True,
        )
        return _compact_task_state_payload({"tasks": tasks, "diagnostics": diagnostics})
