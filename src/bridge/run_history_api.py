"""Run-history and task-state API: thin wrappers over TaskHistoryManager with optional injected deps."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

from src.bridge.task_history import TaskHistoryManager


def load_run_history(manager: TaskHistoryManager) -> List[Dict[str, Any]]:
    return manager.load_run_history()


def save_run_history(manager: TaskHistoryManager, rows: List[Dict[str, Any]]) -> None:
    manager.save_run_history(rows)


def append_run_history(manager: TaskHistoryManager, row: Dict[str, Any]) -> Dict[str, Any]:
    return manager.append_run_history(row)


def upsert_run_history(
    manager: TaskHistoryManager,
    entry: Dict[str, Any],
    *,
    dedupe_fields: Tuple[str, ...],
) -> Dict[str, Any]:
    return manager.upsert_run_history(entry, dedupe_fields=dedupe_fields)


def prune_started_rows_for_type(
    manager: TaskHistoryManager,
    run_type: str,
    *,
    keep_started_at: str = "",
    finished_at: str = "",
) -> None:
    manager.prune_started_rows_for_type(
        run_type, keep_started_at=keep_started_at, finished_at=finished_at
    )


def clear_task_state(manager: TaskHistoryManager, task_type: str) -> None:
    manager.clear_task_state(task_type)


def task_running_from_state(
    task_type: str,
    load_json_object: Callable[[Any, Dict[str, Any]], Dict[str, Any]],
    task_state_path: Any,
    pid_is_running: Callable[[int], bool],
) -> bool:
    state = load_json_object(task_state_path, {})
    if not isinstance(state, dict):
        return False
    entry = state.get(str(task_type))
    if not isinstance(entry, dict):
        return False
    pid = int(entry.get("pid") or 0)
    return pid_is_running(pid)


def report_is_stale_in_progress(
    task_type: str,
    path: Path,
    report: Dict[str, Any],
    *,
    load_json_object: Callable[[Any, Dict[str, Any]], Dict[str, Any]],
    task_state_path: Any,
    parse_iso: Callable[[Any], datetime | None],
    now_utc: Callable[[], datetime],
    pid_is_running: Callable[[int], bool],
    max_age_minutes: int = 5,
    max_mtime_idle_minutes: float = 0.35,
) -> bool:
    started_raw = str(report.get("startedAt") or "")
    finished_raw = str(report.get("finishedAt") or "")
    # Not stale when: no start (not an in-progress report) or task already finished.
    if not started_raw or finished_raw:
        return False
    started_dt = parse_iso(started_raw)
    if not started_dt:
        return False
    age_minutes = (now_utc() - started_dt).total_seconds() / 60.0
    if task_running_from_state(task_type, load_json_object, task_state_path, pid_is_running):
        return False
    state = load_json_object(task_state_path, {})
    if isinstance(state, dict) and isinstance(state.get(task_type), dict):
        return age_minutes >= 0.5
    try:
        mtime_dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        idle_minutes = (now_utc() - mtime_dt).total_seconds() / 60.0
        if idle_minutes >= float(max_mtime_idle_minutes):
            return True
    except OSError:
        pass
    return age_minutes >= float(max_age_minutes)
