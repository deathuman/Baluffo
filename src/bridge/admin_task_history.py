"""Admin-bridge task history facade."""

from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.bridge import run_history_api as _run_history_api
from src.bridge.task_history import TaskHistoryManager


class AdminTaskHistory:
    """Owns the admin bridge task-history manager and related helpers."""

    def __init__(
        self,
        *,
        history_path: Callable[[], Path],
        task_state_path: Callable[[], Path],
        max_rows: Callable[[], int],
        lock: threading.RLock,
        load_json_array: Callable[[Path, list[Any]], list[Any]],
        save_json_atomic: Callable[[Path, Any], None],
        load_json_object: Callable[[Path, dict[str, Any]], dict[str, Any]],
        parse_iso: Callable[[Any], Any],
        now_utc: Callable[[], Any],
        pid_is_running: Callable[[int], bool],
    ) -> None:
        self._history_path = history_path
        self._task_state_path = task_state_path
        self._max_rows = max_rows
        self._lock = lock
        self._load_json_array = load_json_array
        self._save_json_atomic = save_json_atomic
        self._load_json_object = load_json_object
        self._parse_iso = parse_iso
        self._now_utc = now_utc
        self._pid_is_running = pid_is_running
        self._manager: TaskHistoryManager | None = None
        self._manager_key: tuple[Path, Path, int] | None = None

    def _current_key(self) -> tuple[Path, Path, int]:
        return (
            Path(self._history_path()),
            Path(self._task_state_path()),
            max(1, int(self._max_rows())),
        )

    def _get_manager(self) -> TaskHistoryManager:
        key = self._current_key()
        if self._manager is None or self._manager_key != key:
            self._manager_key = key
            self._manager = TaskHistoryManager(
                key[0],
                key[1],
                key[2],
                self._lock,
                load_json_array=self._load_json_array,
                save_json_atomic=self._save_json_atomic,
                load_json_object=self._load_json_object,
            )
        return self._manager

    def load(self) -> list[dict[str, Any]]:
        return self._get_manager().load_run_history()

    def append(self, row: dict[str, Any]) -> dict[str, Any]:
        return self._get_manager().append_run_history(row)

    def upsert(self, entry: dict[str, Any], *, dedupe_fields: tuple[str, ...]) -> dict[str, Any]:
        return self._get_manager().upsert_run_history(entry, dedupe_fields=dedupe_fields)

    def prune_started_rows_for_type(
        self,
        entry_type: str,
        *,
        keep_started_at: str = "",
        finished_at: str = "",
    ) -> None:
        self._get_manager().prune_started_rows_for_type(
            entry_type, keep_started_at=keep_started_at, finished_at=finished_at
        )

    def save_run_history(self, rows: list[dict[str, Any]]) -> None:
        self._get_manager().save_run_history(rows)

    def clear_task_state(self, task_type: str) -> None:
        self._get_manager().clear_task_state(task_type)

    def clear_task_state_locked(self, task_type: str) -> None:
        self._get_manager()._clear_task_state_locked(task_type)

    def report_is_stale_in_progress(
        self,
        task_type: str,
        path: Path,
        report: dict[str, Any],
        *,
        max_age_minutes: int = 5,
        max_mtime_idle_minutes: float = 0.35,
    ) -> bool:
        return _run_history_api.report_is_stale_in_progress(
            task_type,
            path,
            report,
            parse_iso=self._parse_iso,
            now_utc=self._now_utc,
            max_age_minutes=max_age_minutes,
            max_mtime_idle_minutes=max_mtime_idle_minutes,
        )
