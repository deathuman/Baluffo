"""Shared reaper test support: a real AdminTaskLifecycle for ledger transitions."""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from src.admin_bridge import MAX_HISTORY_ROWS
from src.bridge.admin_task_lifecycle import AdminTaskLifecycle
from src.bridge.lifecycle_cleanup import cleanup_orphaned_startup_tasks


def _save_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_json_object(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(default)
    return dict(payload) if isinstance(payload, dict) else dict(default)


def _parse_iso(value: object) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None


class RecordingTimer:
    """Timer stand-in that records start/cancel instead of scheduling a thread."""

    def __init__(self, delay_s: float, callback: Callable[[], None]) -> None:
        self.delay_s = delay_s
        self.callback = callback
        self.started = False
        self.cancelled = False

    def start(self) -> None:
        self.started = True

    def cancel(self) -> None:
        self.cancelled = True


def build_lifecycle(data_dir: Path, *, now_iso: Callable[[], str]) -> AdminTaskLifecycle:
    """Build the real lifecycle service the cleanup and reaper paths write through."""
    return AdminTaskLifecycle(
        lifecycle_path=lambda: data_dir / "admin-task-lifecycle.json",
        max_rows=lambda: MAX_HISTORY_ROWS,
        lock=threading.RLock(),
        load_json_object=_load_json_object,
        save_json_atomic=_save_json,
        now_iso=now_iso,
        parse_iso=_parse_iso,
    )


def reap(
    data_dir: Path,
    *,
    pid_is_running: Callable[[int], bool],
    now_iso: Callable[[], str],
    current_runs: Callable[[], list[dict[str, Any]]] | None = None,
    orphan_run: Callable[..., dict[str, Any] | None] | None = None,
    cancel_run: Callable[..., dict[str, Any] | None] | None = None,
    sync_compaction_hold: Callable[[], dict[str, Any]] | None = None,
    abort_discovery_owner: Callable[[str], dict[str, Any]] | None = None,
    startup: bool = True,
) -> dict[str, Any]:
    lifecycle = build_lifecycle(data_dir, now_iso=now_iso)
    return cleanup_orphaned_startup_tasks(
        data_dir,
        pid_is_running=pid_is_running,
        now_iso=now_iso,
        current_runs=current_runs,
        orphan_run=orphan_run or lifecycle.orphan_run,
        cancel_run=cancel_run or lifecycle.cancel_run,
        sync_compaction_hold=sync_compaction_hold or lifecycle.sync_compaction_hold,
        abort_discovery_owner=abort_discovery_owner,
        startup=startup,
    )
