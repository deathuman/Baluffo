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


__all__ = ["build_duplicate_start_payload", "get_active_task_metadata"]
