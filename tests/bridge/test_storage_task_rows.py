from __future__ import annotations

import time
from typing import Any

import pytest

from src.bridge import storage_health, storage_task_rows
from tests.helpers.task_lifecycle import admin_task_lifecycle, lifecycle_file


class _Recorder:
    """The slice of the lifecycle facade a storage watcher calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def start_run(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("start", kwargs))
        return {"runId": str(kwargs.get("run_id") or "")}

    def heartbeat_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("heartbeat", {"run_id": run_id, "task_type": task_type, **kwargs}))
        return {}

    def finish_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("finish", {"run_id": run_id, "task_type": task_type, **kwargs}))
        return {}

    def fail_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("fail", {"run_id": run_id, "task_type": task_type, **kwargs}))
        return {}

    def cancel_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("cancel", {"run_id": run_id, "task_type": task_type, **kwargs}))
        return {}

    def named(self, name: str) -> list[dict[str, Any]]:
        return [payload for call, payload in self.calls if call == name]


def _wait_for(predicate: Any, timeout_s: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


@pytest.fixture(autouse=True)
def _fast_storage_watchers(monkeypatch):
    """Run watchers at test speed and let a previous test's thread leave."""
    monkeypatch.setattr(storage_task_rows, "POLL_S", 0.01)
    monkeypatch.setattr(storage_task_rows, "HEARTBEAT_S", 0.01)
    monkeypatch.setattr(storage_task_rows, "MAINTENANCE_HANDOFF_GRACE_S", 0.0)
    _wait_for(lambda: storage_task_rows.has_active_watchers() is False, 2.0)
    storage_task_rows.set_task_lifecycle(None)
    yield
    _wait_for(lambda: storage_task_rows.has_active_watchers() is False, 2.0)
    storage_task_rows.set_task_lifecycle(None)


def test_watch_storage_task_starts_heartbeats_and_finishes() -> None:
    recorder = _Recorder()
    status: dict[str, Any] = {
        "running": True,
        "status": "running",
        "phase": "copying",
        "counts": {"copiedRows": 4},
    }

    assert storage_task_rows.watch_storage_task(
        task_type="storage_rebuild",
        run_id="rebuild_1",
        started_at="2026-03-01T00:00:00+00:00",
        stage="queued",
        summary={"mode": "rebuild"},
        read_status=lambda: dict(status),
        lifecycle=recorder,
    )

    start = recorder.named("start")[0]
    assert start["task_type"] == "storage_rebuild"
    assert start["owner_kind"] == "bridge_thread"
    assert start["started_at"] == "2026-03-01T00:00:00+00:00"
    assert start["hold_compaction"] is False
    assert _wait_for(
        lambda: any(item["stage"] == "copying" for item in recorder.named("heartbeat"))
    )
    heartbeat = next(item for item in recorder.named("heartbeat") if item["stage"] == "copying")
    assert heartbeat["progress"]["counts"] == {"copiedRows": 4}

    status.update(running=False, status="complete", phase="complete")

    assert _wait_for(lambda: recorder.named("finish"))
    finish = recorder.named("finish")[0]
    assert finish["run_id"] == "rebuild_1"
    assert finish["terminal_reason"] == "completed"
    assert finish["summary"]["phase"] == "complete"
    assert finish["progress"]["active"] is False


@pytest.mark.parametrize(
    ("terminal_status", "expected_call"),
    [
        ("failed", "fail"),
        ("interrupted", "fail"),
        ("cancelled", "cancel"),
    ],
)
def test_terminal_storage_statuses_map_to_fail_and_cancel(
    terminal_status: str, expected_call: str
) -> None:
    recorder = _Recorder()
    status = {"running": False, "status": terminal_status, "phase": terminal_status}

    assert storage_task_rows.watch_storage_task(
        task_type="storage_reclaim",
        run_id="reclaim_1",
        read_status=lambda: dict(status),
        lifecycle=recorder,
    )

    assert _wait_for(lambda: recorder.named(expected_call))
    call = recorder.named(expected_call)[0]
    assert call["run_id"] == "reclaim_1"
    assert call["summary"]["phase"] == terminal_status


def test_storage_rows_do_not_hold_the_compaction_gate(tmp_path) -> None:
    lifecycle = admin_task_lifecycle(lifecycle_file(tmp_path))

    lifecycle.start_run(
        run_id="retention_1", task_type="storage_maintenance", hold_compaction=False
    )
    assert storage_health.storage_compaction_hold_active() is False

    # A rebuild's own copy aborts when a bridge task holds the gate, so its row
    # must not be that task.
    lifecycle.start_run(run_id="rebuild_1", task_type="storage_rebuild", hold_compaction=False)
    assert storage_health.storage_compaction_hold_active() is False

    # A real task row still holds it, and the storage rows do not release it.
    lifecycle.start_run(run_id="fetch_1", task_type="fetch")
    assert storage_health.storage_compaction_hold_active() is True

    lifecycle.finish_run("fetch_1", "fetch")
    assert storage_health.storage_compaction_hold_active() is False


def test_watch_storage_task_requires_an_installed_lifecycle() -> None:
    assert (
        storage_task_rows.watch_storage_task(
            task_type="storage_reclaim",
            run_id="reclaim_1",
            read_status=lambda: {},
        )
        is False
    )
    assert storage_task_rows.has_active_watchers() is False


def test_installed_lifecycle_is_used_and_the_registry_reports_the_watcher() -> None:
    recorder = _Recorder()
    status: dict[str, Any] = {"running": True, "status": "running", "phase": "copying"}
    storage_task_rows.set_task_lifecycle(recorder)
    try:
        assert storage_task_rows.has_active_watchers() is False
        assert storage_task_rows.watch_storage_task(
            task_type="storage_reclaim",
            run_id="reclaim_1",
            read_status=lambda: dict(status),
        )
        assert storage_task_rows.has_active_watchers() is True
        # A second watcher for the same run is refused instead of racing the row.
        assert (
            storage_task_rows.watch_storage_task(
                task_type="storage_reclaim",
                run_id="reclaim_1",
                read_status=lambda: dict(status),
            )
            is False
        )

        status.update(running=False, status="complete", phase="complete")

        assert _wait_for(lambda: recorder.named("finish"))
        assert _wait_for(lambda: storage_task_rows.has_active_watchers() is False)
    finally:
        storage_task_rows.set_task_lifecycle(None)
