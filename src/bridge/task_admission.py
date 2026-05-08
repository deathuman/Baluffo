"""Helpers for duplicate task admission checks in the admin bridge."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def _safe_pid(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def get_active_task_metadata(
    task_type: str,
    *,
    load_json_object: Callable[[Any, Any], Any],
    task_state_path: Any,
    pid_is_running: Callable[[int], bool],
) -> dict[str, Any]:
    if not task_state_path:
        return {}
    state = load_json_object(task_state_path, {})
    if not isinstance(state, dict):
        return {}
    entry = state.get(str(task_type or "").strip().lower())
    if not isinstance(entry, dict):
        return {}
    pid = _safe_pid(entry.get("pid"))
    if pid <= 0 or not pid_is_running(pid):
        return {}
    return {
        "taskType": str(task_type or "").strip().lower(),
        "runId": str(entry.get("runId") or "").strip(),
        "startedAt": str(entry.get("startedAt") or "").strip(),
        "pid": pid,
        "status": "running",
    }


def get_active_lifecycle_task_metadata(
    task_type: str,
    *,
    lifecycle_rows: list[dict[str, Any]],
    pid_is_running: Callable[[int], bool],
) -> dict[str, Any]:
    normalized_type = str(task_type or "").strip().lower()
    for row in reversed(lifecycle_rows if isinstance(lifecycle_rows, list) else []):
        if not isinstance(row, dict):
            continue
        row_type = str(row.get("type") or row.get("taskType") or "").strip().lower()
        if row_type != normalized_type:
            continue
        if str(row.get("finishedAt") or "").strip():
            continue
        lifecycle_status = str(row.get("lifecycleStatus") or row.get("status") or "").lower()
        if lifecycle_status and lifecycle_status not in {"queued", "running", "started"}:
            continue
        pid = _safe_pid(row.get("ownerPid") or row.get("pid"))
        owner_kind = str(row.get("ownerKind") or "").strip().lower()
        if owner_kind in {"process", "child_process"} and (pid <= 0 or not pid_is_running(pid)):
            continue
        if pid > 0 and not pid_is_running(pid):
            continue
        return {
            "taskType": normalized_type,
            "runId": str(row.get("runId") or row.get("id") or "").strip(),
            "startedAt": str(row.get("startedAt") or "").strip(),
            "pid": pid,
            "status": "running",
        }
    return {}


def build_duplicate_start_payload(
    task: str, task_type: str, metadata: dict[str, Any]
) -> dict[str, Any]:
    return {
        "started": False,
        "alreadyRunning": True,
        "task": str(task or "").strip(),
        "taskType": str(task_type or "").strip().lower(),
        "runId": str((metadata or {}).get("runId") or "").strip(),
        "startedAt": str((metadata or {}).get("startedAt") or "").strip(),
        "pid": _safe_pid((metadata or {}).get("pid")),
        "status": "running",
    }


__all__ = [
    "build_duplicate_start_payload",
    "get_active_lifecycle_task_metadata",
    "get_active_task_metadata",
]
