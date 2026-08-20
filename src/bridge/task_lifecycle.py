"""Persisted admin task lifecycle ledger — thin coordinator.

AI boundary owns: admin task lifecycle persistence, run state transitions, event rows, and task status recovery.
AI boundary implement in: this coordinator.
AI boundary search before contracts: task runtime storage, run history API, lifecycle cleanup, and task lifecycle tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused bridge task lifecycle tests.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.bridge.task_lifecycle_core import (
    ACTIVE_STATUSES,
    TERMINAL_STATUSES,
    TaskLifecycleCoreMixin,
)
from src.bridge.task_lifecycle_legacy import TaskLifecycleLegacyMixin
from src.bridge.task_lifecycle_rows import TaskLifecycleRowsMixin
from src.bridge.task_lifecycle_runs import TaskLifecycleRunsMixin


class TaskLifecycleService(
    TaskLifecycleCoreMixin,
    TaskLifecycleRowsMixin,
    TaskLifecycleRunsMixin,
    TaskLifecycleLegacyMixin,
):
    """Owns canonical Admin task lifecycle rows."""

    def __init__(
        self,
        *,
        path: Path,
        lock: threading.RLock,
        load_json_object: Callable[[Path, dict[str, Any]], dict[str, Any]],
        save_json_atomic: Callable[[Path, Any], None],
        now_iso: Callable[[], str],
        parse_iso: Callable[[Any], Any],
        task_runtime_store: Callable[[], Any] | None = None,
        record_storage_diagnostic: Callable[..., None] | None = None,
        max_rows: int = 240,
    ) -> None:
        self._path = Path(path)
        self._lock = lock
        self._load_json_object = load_json_object
        self._save_json_atomic = save_json_atomic
        self._now_iso = now_iso
        self._parse_iso = parse_iso
        self._task_runtime_store = task_runtime_store
        self._record_storage_diagnostic = record_storage_diagnostic
        self._max_rows = max(1, int(max_rows or 1))


__all__ = [
    "ACTIVE_STATUSES",
    "TERMINAL_STATUSES",
    "TaskLifecycleService",
]
