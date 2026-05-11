"""Run history and task state persistence for bridge operations."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any


def _parse_iso(text: str) -> datetime | None:
    if not text:
        return None
    try:
        return datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except ValueError:
        return None


class TaskHistoryManager:
    """Manages run history (admin-run-history.json) and task state (admin-task-state.json)."""

    def __init__(
        self,
        history_path: Path,
        task_state_path: Path,
        max_rows: int,
        lock: threading.RLock,
        *,
        load_json_array: Callable[[Path, list[Any]], list[Any]],
        save_json_atomic: Callable[[Path, Any], None],
        load_json_object: Callable[[Path, dict[str, Any]], dict[str, Any]],
    ) -> None:
        self._history_path = history_path
        self._task_state_path = task_state_path
        self._max_rows = max(1, int(max_rows))
        self._lock = lock
        self._load_json_array = load_json_array
        self._save_json_atomic = save_json_atomic
        self._load_json_object = load_json_object

    def load_run_history(self) -> list[dict[str, Any]]:
        rows = self._load_json_array(self._history_path, [])
        cleaned: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if not row.get("type"):
                continue
            cleaned.append(dict(row))
        cleaned.sort(key=lambda item: str(item.get("startedAt") or item.get("finishedAt") or ""))
        return cleaned[-self._max_rows :]

    def save_run_history(self, rows: list[dict[str, Any]]) -> None:
        self._history_path.parent.mkdir(parents=True, exist_ok=True)
        self._save_json_atomic(self._history_path, rows[-self._max_rows :])

    def append_run_history(self, row: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            history = self.load_run_history()
            entry = dict(row)
            entry.setdefault("id", f"run_{uuid.uuid4().hex[:12]}")
            history.append(entry)
            self.save_run_history(history)
            return entry

    def upsert_run_history(
        self, entry: dict[str, Any], *, dedupe_fields: tuple[str, ...]
    ) -> dict[str, Any]:
        with self._lock:
            history = self.load_run_history()
            match_idx = -1
            for idx, row in enumerate(history):
                if all(
                    str(row.get(field) or "") == str(entry.get(field) or "")
                    for field in dedupe_fields
                ):
                    match_idx = idx
                    break
            if match_idx >= 0:
                merged = {**history[match_idx], **entry}
                merged.setdefault(
                    "id", history[match_idx].get("id") or f"run_{uuid.uuid4().hex[:12]}"
                )
                history[match_idx] = merged
                self.save_run_history(history)
                return merged
            history.append({**entry, "id": str(entry.get("id") or f"run_{uuid.uuid4().hex[:12]}")})
            self.save_run_history(history)
            return history[-1]

    def prune_started_rows_for_type(
        self,
        run_type: str,
        *,
        keep_started_at: str = "",
        finished_at: str = "",
    ) -> None:
        with self._lock:
            history = self.load_run_history()
            keep_started_token = str(keep_started_at or "")
            finished_dt = _parse_iso(finished_at) if finished_at else None
            next_rows: list[dict[str, Any]] = []
            for row in history:
                if str(row.get("type") or "") != run_type:
                    next_rows.append(row)
                    continue
                if str(row.get("status") or "").lower() != "started":
                    next_rows.append(row)
                    continue
                row_started = str(row.get("startedAt") or "")
                if keep_started_token and row_started == keep_started_token:
                    next_rows.append(row)
                    continue
                if finished_dt:
                    row_started_dt = _parse_iso(row_started)
                    if not row_started_dt or row_started_dt <= finished_dt:
                        continue
                    next_rows.append(row)
                    continue
                if not keep_started_token:
                    continue
                if keep_started_token and row_started and row_started < keep_started_token:
                    continue
                next_rows.append(row)
            self.save_run_history(next_rows)

    def clear_task_state(self, task_type: str) -> None:
        with self._lock:
            self._clear_task_state_locked(task_type)

    def _clear_task_state_locked(self, task_type: str) -> None:
        # admin-task-state.json writes are deprecated in favor of the
        # lifecycle ledger (data/admin-task-lifecycle.json).  No production
        # task launch path calls clear_task_state; the lifecycle ledger APIs
        # manage state transitions directly.
        pass
