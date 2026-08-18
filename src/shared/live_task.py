"""Shared live task payload normalization helpers.

AI boundary owns: cross-surface live task payload normalization and compact progress fields.
AI boundary implement in: this file for generic live-task normalization; bridge leaves own route-specific summaries.
AI boundary search before contracts: ops live payload helpers, active task snapshot, and frontend task-run view models.
AI boundary verify: `npm run lint:repo-guardrails` plus focused live-task tests.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypedDict, cast

from src.shared.json_shapes import (
    as_json_object,
    copy_json_object,
    json_object_rows,
    json_object_values,
)
from src.shared.text_utils import clean_text, norm_text

LIVE_TASK_EVENT_SCHEMA_VERSION = 1
LIVE_TASK_EVENT_DEFAULT_NAME = "live_task_event"
LIVE_TASK_RUNNING_SOURCE_NAME_LIMIT = 5
LIVE_TASK_ARRAY_COUNT_KEYS = {"runningSourceNames"}


class LiveTaskProgress(TypedDict, total=False):
    active: bool
    phaseKey: str
    phaseLabel: str
    mode: str
    ratio: float
    counts: dict[str, Any]
    targetLabel: str
    targetUrl: str
    waitReason: str
    updatedAt: str


class LiveTaskWorkItem(TypedDict, total=False):
    id: str
    name: str
    status: str
    startedAt: str
    finishedAt: str
    durationMs: int
    heartbeatAt: str
    error: str
    progress: LiveTaskProgress


class LiveTaskEvent(TypedDict, total=False):
    schemaVersion: int
    timestamp: str
    level: str
    event: str
    taskType: str
    runId: str
    workItemId: str
    phaseKey: str
    message: str
    target: str
    targetUrl: str


class LiveTaskPayload(TypedDict, total=False):
    source: str
    taskType: str
    status: str
    active: bool
    runId: str
    startedAt: str
    finishedAt: str
    heartbeatAt: str
    taskProgress: LiveTaskProgress
    summary: dict[str, Any]
    workItems: list[LiveTaskWorkItem]
    workItemCount: int
    recentEvents: list[LiveTaskEvent]
    outputs: dict[str, Any]


class TaskStateRow(LiveTaskPayload, total=False):
    type: str
    id: str
    lifecycleStatus: str
    stage: str
    parentRunId: str
    parentTaskType: str
    ownerKind: str
    ownerPid: int
    progress: LiveTaskProgress
    workItemsTruncated: bool
    recentEventCount: int
    recentEventsTruncated: bool
    error: str
    label: str


class TaskStatePayload(TypedDict, total=False):
    source: str
    tasks: list[TaskStateRow]
    count: int
    diagnostics: list[dict[str, Any]]
    summary: bool


def _clamped_int(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return int(default)


def _normalize_progress_count_array(key: str, value: Any) -> list[str] | None:
    if key not in LIVE_TASK_ARRAY_COUNT_KEYS or not isinstance(value, list):
        return None
    names: list[str] = []
    for item in value:
        text = clean_text(item)
        if text:
            names.append(text)
        if len(names) >= LIVE_TASK_RUNNING_SOURCE_NAME_LIMIT:
            break
    return names


def normalize_live_task_progress(payload: Mapping[str, Any] | None) -> LiveTaskProgress:
    src = as_json_object(payload)
    mode = clean_text(src.get("mode")).lower()
    if mode not in {"determinate", "indeterminate"}:
        mode = "indeterminate"
    wait_reason = clean_text(src.get("waitReason")).lower()
    if wait_reason not in {"domain_gate", "listing_batch", "detail_batch", "parsing"}:
        wait_reason = ""
    counts_src = as_json_object(src.get("counts"))
    counts: dict[str, Any] = {}
    for key, value in counts_src.items():
        clean_key = clean_text(key)
        if not clean_key:
            continue
        array_value = _normalize_progress_count_array(clean_key, value)
        if array_value is not None:
            counts[clean_key] = array_value
            continue
        if isinstance(value, bool):
            counts[clean_key] = bool(value)
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            counts[clean_key] = _clamped_int(value, 0)
            continue
        text = clean_text(value)
        if text:
            counts[clean_key] = text
    ratio_value = src.get("ratio")
    try:
        ratio = float(ratio_value) if ratio_value is not None else 0.0
    except (TypeError, ValueError):
        ratio = 0.0
    ratio = max(0.0, min(1.0, ratio))
    return {
        "active": bool(src.get("active")),
        "phaseKey": clean_text(src.get("phaseKey")),
        "phaseLabel": clean_text(src.get("phaseLabel")),
        "mode": mode,
        "ratio": ratio,
        "counts": counts,
        "targetLabel": clean_text(src.get("targetLabel")),
        "targetUrl": clean_text(src.get("targetUrl")),
        "waitReason": wait_reason,
        "updatedAt": clean_text(src.get("updatedAt")),
    }


def normalize_live_task_work_item(payload: dict[str, Any] | None) -> LiveTaskWorkItem:
    src = as_json_object(payload)
    item_id = clean_text(src.get("id")) or clean_text(src.get("name"))
    return {
        "id": item_id,
        "name": clean_text(src.get("name")) or item_id,
        "status": norm_text(src.get("status")) or "queued",
        "startedAt": clean_text(src.get("startedAt")),
        "finishedAt": clean_text(src.get("finishedAt")),
        "durationMs": _clamped_int(src.get("durationMs"), 0),
        "heartbeatAt": clean_text(src.get("heartbeatAt")),
        "error": clean_text(src.get("error")),
        "progress": normalize_live_task_progress(src.get("progress")),
    }


def normalize_live_task_event(
    payload: dict[str, Any] | None,
    *,
    default_task_type: str = "",
    default_run_id: str = "",
) -> LiveTaskEvent:
    src = as_json_object(payload)
    level = clean_text(src.get("level")).lower() or "info"
    if level not in {"debug", "muted", "info", "warn", "error", "success", "warning"}:
        level = "info"
    phase_key = clean_text(src.get("phaseKey"))
    event = clean_text(src.get("event")) or phase_key or LIVE_TASK_EVENT_DEFAULT_NAME
    return {
        "schemaVersion": LIVE_TASK_EVENT_SCHEMA_VERSION,
        "timestamp": clean_text(src.get("timestamp")),
        "level": level,
        "event": event,
        "taskType": clean_text(src.get("taskType")) or clean_text(default_task_type),
        "runId": clean_text(src.get("runId")) or clean_text(default_run_id),
        "workItemId": clean_text(src.get("workItemId")),
        "phaseKey": phase_key,
        "message": clean_text(src.get("message")),
        "target": clean_text(src.get("target")),
        "targetUrl": clean_text(src.get("targetUrl")),
    }


def normalize_live_task_payload(
    payload: Mapping[str, Any] | None,
    *,
    task_type: str = "",
    run_id: str = "",
    started_at: str = "",
    finished_at: str = "",
) -> LiveTaskPayload:
    src = as_json_object(payload)
    rows = src.get("workItems")
    if not isinstance(rows, list):
        rows = src.get("tasks")
    events = src.get("recentEvents")
    normalized_rows = [normalize_live_task_work_item(row) for row in json_object_rows(rows)]
    normalized_events = [
        normalize_live_task_event(
            row,
            default_task_type=task_type or clean_text(src.get("taskType")),
            default_run_id=run_id or clean_text(src.get("runId")),
        )
        for row in json_object_rows(events)
    ]
    return {
        "taskType": clean_text(src.get("taskType")) or clean_text(task_type),
        "status": norm_text(src.get("status")),
        "active": bool(src.get("active")),
        "runId": clean_text(src.get("runId")) or clean_text(run_id),
        "startedAt": clean_text(src.get("startedAt")) or clean_text(started_at),
        "finishedAt": clean_text(src.get("finishedAt")) or clean_text(finished_at),
        "heartbeatAt": clean_text(src.get("heartbeatAt")),
        "taskProgress": normalize_live_task_progress(src.get("taskProgress")),
        "summary": copy_json_object(src.get("summary")),
        "workItems": normalized_rows,
        "recentEvents": normalized_events,
        "outputs": copy_json_object(src.get("outputs")),
    }


def build_live_task_contract_fields(
    payload: Mapping[str, Any] | None,
) -> LiveTaskPayload:
    normalized = normalize_live_task_payload(payload)
    work_items = [
        cast(LiveTaskWorkItem, dict(row)) for row in json_object_rows(normalized.get("workItems"))
    ]
    recent_events = [
        cast(LiveTaskEvent, dict(row)) for row in json_object_rows(normalized.get("recentEvents"))
    ]
    return {
        "heartbeatAt": clean_text(normalized.get("heartbeatAt")),
        "taskProgress": normalize_live_task_progress(normalized.get("taskProgress")),
        "workItems": work_items,
        "recentEvents": recent_events,
    }


def snapshot_live_task_work_items(
    payload: dict[str, dict[str, Any]] | list[dict[str, Any]] | None,
) -> list[LiveTaskWorkItem]:
    return [cast(LiveTaskWorkItem, dict(row)) for row in json_object_values(payload)]


def build_live_task_progress_payload(
    *,
    active: bool,
    phase_key: str = "",
    phase_label: str = "",
    mode: str = "indeterminate",
    ratio: float = 0.0,
    counts: dict[str, Any] | None = None,
    target_label: str = "",
    target_url: str = "",
    wait_reason: str = "",
    updated_at: str = "",
) -> LiveTaskProgress:
    return normalize_live_task_progress(
        {
            "active": bool(active),
            "phaseKey": clean_text(phase_key),
            "phaseLabel": clean_text(phase_label),
            "mode": clean_text(mode),
            "ratio": ratio,
            "counts": dict(counts or {}),
            "targetLabel": clean_text(target_label),
            "targetUrl": clean_text(target_url),
            "waitReason": clean_text(wait_reason),
            "updatedAt": clean_text(updated_at),
        }
    )


def build_live_task_work_item(
    *,
    item_id: str,
    name: str,
    status: str,
    started_at: str = "",
    finished_at: str = "",
    duration_ms: int = 0,
    heartbeat_at: str = "",
    error: str = "",
    progress: LiveTaskProgress | dict[str, Any] | None = None,
) -> LiveTaskWorkItem:
    return normalize_live_task_work_item(
        {
            "id": item_id,
            "name": name,
            "status": status,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "durationMs": duration_ms,
            "heartbeatAt": heartbeat_at,
            "error": error,
            "progress": progress or {},
        }
    )


def build_live_task_payload(
    *,
    task_type: str,
    active: bool,
    run_id: str = "",
    started_at: str = "",
    finished_at: str = "",
    heartbeat_at: str = "",
    status: str = "",
    task_progress: LiveTaskProgress | dict[str, Any] | None = None,
    summary: dict[str, Any] | None = None,
    work_items: list[dict[str, Any]] | None = None,
    recent_events: list[dict[str, Any]] | None = None,
    outputs: dict[str, Any] | None = None,
) -> LiveTaskPayload:
    return normalize_live_task_payload(
        {
            "taskType": clean_text(task_type),
            "active": bool(active),
            "runId": clean_text(run_id),
            "startedAt": clean_text(started_at),
            "finishedAt": clean_text(finished_at),
            "heartbeatAt": clean_text(heartbeat_at),
            "status": clean_text(status),
            "taskProgress": task_progress or {},
            "summary": dict(summary or {}),
            "workItems": list(work_items or []),
            "recentEvents": list(recent_events or []),
            "outputs": dict(outputs or {}),
        },
        task_type=task_type,
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
    )


def append_live_task_event(
    events: list[dict[str, Any]] | None,
    event: dict[str, Any] | None,
    *,
    limit: int = 120,
) -> list[dict[str, Any]]:
    current = [dict(item) for item in json_object_rows(events)]
    normalized = normalize_live_task_event(event)
    if not normalized.get("message"):
        return current
    current.append(cast(dict[str, Any], normalized))
    max_events = max(1, int(limit or 1))
    if len(current) > max_events:
        current = current[-max_events:]
    return current
