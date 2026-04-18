from __future__ import annotations

from typing import Any

from src.shared.text_utils import clean_text, norm_text


def _clamped_int(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return int(default)


def normalize_live_task_progress(payload: dict[str, Any] | None) -> dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    mode = clean_text(src.get("mode")).lower()
    if mode not in {"determinate", "indeterminate"}:
        mode = "indeterminate"
    counts_src = src.get("counts") if isinstance(src.get("counts"), dict) else {}
    counts: dict[str, Any] = {}
    for key, value in counts_src.items():
        clean_key = clean_text(key)
        if not clean_key:
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
    try:
        ratio = float(src.get("ratio"))
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
        "updatedAt": clean_text(src.get("updatedAt")),
    }


def normalize_live_task_work_item(payload: dict[str, Any] | None) -> dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
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
) -> dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    level = clean_text(src.get("level")).lower() or "info"
    if level not in {"debug", "muted", "info", "warn", "error", "success", "warning"}:
        level = "info"
    return {
        "timestamp": clean_text(src.get("timestamp")),
        "level": level,
        "taskType": clean_text(src.get("taskType")) or clean_text(default_task_type),
        "runId": clean_text(src.get("runId")) or clean_text(default_run_id),
        "workItemId": clean_text(src.get("workItemId")),
        "phaseKey": clean_text(src.get("phaseKey")),
        "message": clean_text(src.get("message")),
    }


def normalize_live_task_payload(
    payload: dict[str, Any] | None,
    *,
    task_type: str = "",
    run_id: str = "",
    started_at: str = "",
    finished_at: str = "",
) -> dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    rows = src.get("workItems")
    if not isinstance(rows, list):
        rows = src.get("tasks")
    events = src.get("recentEvents")
    normalized_rows = [
        normalize_live_task_work_item(row) for row in rows if isinstance(row, dict)
    ] if isinstance(rows, list) else []
    normalized_events = [
        normalize_live_task_event(
            row,
            default_task_type=task_type or clean_text(src.get("taskType")),
            default_run_id=run_id or clean_text(src.get("runId")),
        )
        for row in events
        if isinstance(row, dict)
    ] if isinstance(events, list) else []
    summary = src.get("summary") if isinstance(src.get("summary"), dict) else {}
    outputs = src.get("outputs") if isinstance(src.get("outputs"), dict) else {}
    return {
        "taskType": clean_text(src.get("taskType")) or clean_text(task_type),
        "status": norm_text(src.get("status")),
        "active": bool(src.get("active")),
        "runId": clean_text(src.get("runId")) or clean_text(run_id),
        "startedAt": clean_text(src.get("startedAt")) or clean_text(started_at),
        "finishedAt": clean_text(src.get("finishedAt")) or clean_text(finished_at),
        "heartbeatAt": clean_text(src.get("heartbeatAt")),
        "taskProgress": normalize_live_task_progress(src.get("taskProgress")),
        "summary": dict(summary),
        "workItems": normalized_rows,
        "recentEvents": normalized_events,
        "outputs": dict(outputs),
    }


def build_live_task_contract_fields(
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    normalized = normalize_live_task_payload(payload)
    work_items = [dict(row) for row in (normalized.get("workItems") or []) if isinstance(row, dict)]
    recent_events = [dict(row) for row in (normalized.get("recentEvents") or []) if isinstance(row, dict)]
    return {
        "heartbeatAt": clean_text(normalized.get("heartbeatAt")),
        "taskProgress": normalize_live_task_progress(normalized.get("taskProgress")),
        "workItems": work_items,
        "recentEvents": recent_events,
    }


def snapshot_live_task_work_items(
    payload: dict[str, dict[str, Any]] | list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        rows = payload.values()
    elif isinstance(payload, list):
        rows = payload
    else:
        rows = []
    return [dict(row) for row in rows if isinstance(row, dict)]


def build_live_task_progress_payload(
    *,
    active: bool,
    phase_key: str = "",
    phase_label: str = "",
    counts: dict[str, Any] | None = None,
    target_label: str = "",
    target_url: str = "",
    updated_at: str = "",
) -> dict[str, Any]:
    return normalize_live_task_progress(
        {
            "active": bool(active),
            "phaseKey": clean_text(phase_key),
            "phaseLabel": clean_text(phase_label),
            "mode": "indeterminate",
            "ratio": 0.0,
            "counts": dict(counts or {}),
            "targetLabel": clean_text(target_label),
            "targetUrl": clean_text(target_url),
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
    progress: dict[str, Any] | None = None,
) -> dict[str, Any]:
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
    task_progress: dict[str, Any] | None = None,
    summary: dict[str, Any] | None = None,
    work_items: list[dict[str, Any]] | None = None,
    recent_events: list[dict[str, Any]] | None = None,
    outputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
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
    current = [dict(item) for item in (events or []) if isinstance(item, dict)]
    normalized = normalize_live_task_event(event)
    if not normalized.get("message"):
        return current
    current.append(normalized)
    max_events = max(1, int(limit or 1))
    if len(current) > max_events:
        current = current[-max_events:]
    return current
