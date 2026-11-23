"""A failed start must release its hold without hiding the write error."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import pytest

from src.bridge.admin_task_lifecycle import AdminTaskLifecycle
from src.bridge.storage_health import (
    set_storage_compaction_hold,
    storage_compaction_hold_active,
)
from tests.helpers.json_files import load_json_object
from tests.helpers.report_state import parse_iso


def _failing_lifecycle(tmp_path: Path) -> AdminTaskLifecycle:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"

    def fail_save(_path: Path, _payload: Any) -> None:
        raise OSError("lifecycle write failed")

    return AdminTaskLifecycle(
        lifecycle_path=lambda: lifecycle_path,
        max_rows=lambda: 240,
        lock=threading.RLock(),
        load_json_object=load_json_object,
        save_json_atomic=fail_save,
        now_iso=lambda: "2026-05-06T19:00:00+00:00",
        parse_iso=parse_iso,
    )


def test_failed_start_run_releases_the_compaction_hold(tmp_path: Path) -> None:
    lifecycle = _failing_lifecycle(tmp_path)
    try:
        set_storage_compaction_hold(True)

        with pytest.raises(OSError, match="lifecycle write failed"):
            lifecycle.start_run(run_id="fetch_failed", task_type="fetch")

        assert storage_compaction_hold_active() is False
    finally:
        set_storage_compaction_hold(False)


def test_failed_start_preserves_write_error_when_hold_sync_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lifecycle = _failing_lifecycle(tmp_path)
    sync_calls: list[str] = []

    def fail_sync() -> None:
        sync_calls.append("sync")
        raise RuntimeError("hold sync failed")

    monkeypatch.setattr(lifecycle, "_sync_compaction_hold", fail_sync)
    try:
        set_storage_compaction_hold(True)

        with pytest.raises(OSError, match="lifecycle write failed"):
            lifecycle.start_run(run_id="fetch_failed", task_type="fetch")

        assert sync_calls == ["sync"]
        # This sync failed before it could release the hold.
        assert storage_compaction_hold_active() is True
    finally:
        set_storage_compaction_hold(False)
