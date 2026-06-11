from __future__ import annotations

from contextlib import suppress
from typing import Any

from src.bridge.api import BridgeApi


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _best_effort(default: Any, func: Any) -> Any:
    with suppress(Exception):
        return func()
    return default


def _compact_task_row(row: dict[str, Any]) -> dict[str, Any]:
    run_id = _text(row.get("runId") or row.get("id"))
    task_type = _text(row.get("taskType") or row.get("type")).lower()
    progress = _as_dict(row.get("taskProgress") or row.get("progress"))
    summary = _as_dict(row.get("summary"))
    return {
        "id": run_id,
        "runId": run_id,
        "type": task_type,
        "taskType": task_type,
        "status": _text(row.get("status") or row.get("lifecycleStatus") or "running"),
        "lifecycleStatus": _text(row.get("lifecycleStatus") or row.get("status") or "running"),
        "active": bool(row.get("active", True)),
        "stage": _text(row.get("stage") or summary.get("stage")),
        "startedAt": _text(row.get("startedAt")),
        "heartbeatAt": _text(row.get("heartbeatAt")),
        "finishedAt": _text(row.get("finishedAt")),
        "parentRunId": _text(row.get("parentRunId")),
        "parentTaskType": _text(row.get("parentTaskType")).lower(),
        "terminalReason": _text(row.get("terminalReason")),
        "taskProgress": progress,
        "summary": summary,
    }


def _pipeline_task_row(payload: dict[str, Any]) -> dict[str, Any]:
    run_id = _text(payload.get("runId"))
    if not run_id or not bool(payload.get("active")):
        return {}
    runtime = _as_dict(payload.get("runtime"))
    progress = _as_dict(payload.get("progress") or payload.get("taskProgress"))
    stage = _text(payload.get("stage") or runtime.get("stage"))
    return _compact_task_row(
        {
            "runId": run_id,
            "taskType": "pipeline",
            "status": "running",
            "lifecycleStatus": "running",
            "active": True,
            "stage": stage,
            "startedAt": _text(payload.get("startedAt") or runtime.get("startedAt")),
            "heartbeatAt": _text(payload.get("heartbeatAt") or runtime.get("heartbeatAt")),
            "taskProgress": progress,
            "summary": {"stage": stage} if stage else {},
        }
    )


def _current_task_rows(api: BridgeApi) -> list[dict[str, Any]]:
    rows = [
        _compact_task_row(row)
        for row in _as_list(api.get_lifecycle_current_runs())
        if isinstance(row, dict)
    ]
    by_key = {
        (_text(row.get("taskType")), _text(row.get("runId"))): row
        for row in rows
        if _text(row.get("taskType")) and _text(row.get("runId"))
    }
    pipeline_row = _pipeline_task_row(
        _as_dict(_best_effort({}, api.get_jobs_pipeline_status_payload))
    )
    if pipeline_row:
        by_key[(_text(pipeline_row.get("taskType")), _text(pipeline_row.get("runId")))] = (
            pipeline_row
        )
    return sorted(
        by_key.values(),
        key=lambda row: _text(row.get("startedAt") or row.get("heartbeatAt")),
        reverse=True,
    )


def _recent_task_rows(api: BridgeApi, *, limit: int = 2) -> list[dict[str, Any]]:
    rows = [
        _compact_task_row({**row, "active": False})
        for row in _as_list(api.get_lifecycle_recent_runs())
        if isinstance(row, dict)
    ]
    rows.sort(key=lambda row: _text(row.get("finishedAt") or row.get("startedAt")), reverse=True)
    return rows[: max(0, int(limit))]


def _sync_summary(api: BridgeApi) -> dict[str, Any]:
    config = _as_dict(_best_effort({"ready": False, "enabled": False}, api.sync_config_status))
    enabled = bool(config.get("enabled"))
    return {
        "ok": True,
        "summaryView": True,
        "config": config,
        "savedConfig": {"enabled": enabled},
        "runtime": {},
    }


def _overview_summary(api: BridgeApi) -> dict[str, Any]:
    overview = _as_dict(
        _best_effort(
            {},
            lambda: api.desktop_local_data_store().get_admin_overview(detail="summary"),
        )
    )
    if overview:
        return overview
    return {
        "users": [],
        "totals": {},
        "detailLevel": "summary",
        "error": "overview unavailable",
    }


def _session_summary(api: BridgeApi) -> dict[str, Any]:
    desktop_session = _as_dict(_best_effort({}, api.get_desktop_session_payload))
    user = _best_effort(None, lambda: api.desktop_local_data_store().get_current_user())
    return {
        "desktopSession": desktop_session,
        "user": user,
    }


def get_admin_bootstrap_payload(api: BridgeApi) -> dict[str, Any]:
    current = _current_task_rows(api)
    recent = _recent_task_rows(api, limit=2)
    session = _session_summary(api)
    runtime_config = getattr(api, "runtime_config", None)
    desktop_mode = bool(getattr(runtime_config, "desktop_mode", False))
    owner_state = _as_dict(session.get("desktopSession"))
    startup_ready = True if not desktop_mode else bool(owner_state.get("startedAt"))
    generated_at = api.now_iso()
    return {
        "ok": True,
        "summaryView": True,
        "generatedAt": generated_at,
        "app": {
            "version": _text(getattr(api, "app_version", "") or ""),
            "desktopMode": desktop_mode,
            "startupReady": startup_ready,
        },
        "session": session,
        "overview": _overview_summary(api),
        "tasks": {
            "current": current,
            "recent": recent,
            "currentCount": len(current),
            "recentCount": len(recent),
        },
        "sync": _sync_summary(api),
    }
