from __future__ import annotations

import time

import pytest

from src.storage import BaluffoStore
from tests.helpers.temp_paths import workspace_tmpdir


def test_deferred_store_records_maintenance_without_starting_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fail_wal(self: BaluffoStore, *, reason: str, mode: str) -> None:
        calls.append(f"wal:{reason}:{mode}")

    def fail_vacuum(self: BaluffoStore, *, reason: str = "") -> dict[str, object]:
        calls.append(f"vacuum:{reason}")
        return {}

    monkeypatch.setattr(BaluffoStore, "_run_wal_maintenance", fail_wal)
    monkeypatch.setattr(BaluffoStore, "vacuum", fail_vacuum)

    with workspace_tmpdir("baluffo-store-deferred-maintenance") as data_dir:
        with BaluffoStore(
            data_dir,
            defer_maintenance=True,
            wal_checkpoint_threshold_bytes=1024 * 1024,
            wal_truncate_threshold_bytes=2 * 1024 * 1024,
            vacuum_freelist_threshold_bytes=1024 * 1024,
        ) as store:
            assert store.maintenance_deferred is True
            store.wal_checkpoint_threshold_bytes = 1
            store.wal_truncate_threshold_bytes = 2
            store.vacuum_freelist_threshold_bytes = 1
            # This test is about released work running at all, so it pins the
            # idle gate open; the gate itself is covered separately below.
            store.vacuum_idle_s = 0.0
            store.file_sizes = lambda: {  # type: ignore[method-assign]
                "databaseBytes": 1000,
                "walBytes": 4,
                "shmBytes": 0,
                "freelistBytes": 900,
            }

            assert store.schedule_wal_maintenance_if_needed(reason="test") is False
            assert store.schedule_compaction_if_needed(reason="test") is False
            assert calls == []
            assert store.wal_maintenance_status()["status"] == "deferred"
            assert store.wal_maintenance_status()["running"] is False
            assert store.compaction_status()["status"] == "deferred"

            assert store.resume_maintenance_after_serving(reason="test-serve", grace_s=0.0) is True
            thread = store._maintenance_thread
            if thread is not None:
                thread.join(timeout=5.0)

            assert store.maintenance_deferred is False
            assert any(call.startswith("wal:") for call in calls)
            assert any(call.startswith("vacuum:") for call in calls)
            assert store.resume_maintenance_after_serving(reason="again", grace_s=0.0) is False


def test_post_serve_vacuum_waits_for_a_quiet_write_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def record_vacuum(self: BaluffoStore, *, reason: str = "") -> dict[str, object]:
        calls.append(reason)
        return {}

    monkeypatch.setattr(BaluffoStore, "vacuum", record_vacuum)

    with workspace_tmpdir("baluffo-store-idle-gate") as data_dir:
        with BaluffoStore(
            data_dir, defer_maintenance=True, vacuum_freelist_threshold_bytes=1
        ) as store:
            store.vacuum_idle_s = 5.0
            store.vacuum_idle_max_wait_s = 30.0
            store.vacuum_idle_poll_s = 0.05
            store.file_sizes = lambda: {  # type: ignore[method-assign]
                "databaseBytes": 1000,
                "walBytes": 0,
                "shmBytes": 0,
                "freelistBytes": 900,
            }
            store._last_write_monotonic = time.monotonic()

            assert store.resume_maintenance_after_serving(reason="serve", grace_s=0.0) is True
            thread = store._maintenance_thread
            assert thread is not None
            thread.join(timeout=0.4)

            assert thread.is_alive() is True
            assert calls == []
            assert store.compaction_status()["status"] != "ok"

            store._last_write_monotonic = time.monotonic() - 60.0
            thread.join(timeout=5.0)

            assert calls == ["serve"]


def test_busy_store_records_skipped_vacuum_instead_of_stalling_writers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def record_vacuum(self: BaluffoStore, *, reason: str = "") -> dict[str, object]:
        calls.append(reason)
        return {}

    monkeypatch.setattr(BaluffoStore, "vacuum", record_vacuum)

    with workspace_tmpdir("baluffo-store-busy-skip") as data_dir:
        with BaluffoStore(data_dir, vacuum_freelist_threshold_bytes=1) as store:
            store.vacuum_idle_s = 30.0
            store.vacuum_idle_max_wait_s = 0.2
            store.vacuum_idle_poll_s = 0.05
            store.file_sizes = lambda: {  # type: ignore[method-assign]
                "databaseBytes": 2000,
                "walBytes": 0,
                "shmBytes": 0,
                "freelistBytes": 1800,
            }
            store._last_write_monotonic = time.monotonic()

            assert store._run_compaction_when_idle(reason="write") is False
            assert calls == []

            status = store.compaction_status()
            assert status["status"] == "skipped-busy"
            assert status["reason"] == "write"
            assert status["reclaimedBytes"] == 0
            assert status["idleForS"] < status["effectiveIdleGateS"]
            assert status["effectiveIdleGateS"] == store.vacuum_idle_s
            assert status["databaseBytes"] == 2000
            assert status["freelistBytes"] == 1800

            write_started = time.perf_counter()
            store.write(lambda conn: conn.execute("SELECT 1"))
            assert time.perf_counter() - write_started < 5.0


def test_large_database_requires_the_raised_freelist_ratio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def record_vacuum(self: BaluffoStore, *, reason: str = "") -> dict[str, object]:
        calls.append(reason)
        return {}

    monkeypatch.setattr(BaluffoStore, "vacuum", record_vacuum)

    with workspace_tmpdir("baluffo-store-large-ratio") as data_dir:
        with BaluffoStore(
            data_dir,
            vacuum_freelist_threshold_bytes=1,
            vacuum_freelist_ratio=0.2,
            vacuum_large_database_bytes=1000,
            vacuum_large_database_ratio=0.5,
            # This test is about the ratio, so it pins both idle gates open.
            vacuum_idle_s=0.0,
            vacuum_large_database_idle_s=0.0,
        ) as store:
            assert store.effective_vacuum_freelist_ratio(999) == 0.2
            assert store.effective_vacuum_freelist_ratio(1000) == 0.5

            store.file_sizes = lambda: {  # type: ignore[method-assign]
                "databaseBytes": 2000,
                "walBytes": 0,
                "shmBytes": 0,
                "freelistBytes": 600,
            }
            assert store.schedule_compaction_if_needed(reason="write") is False
            assert calls == []

            store.file_sizes = lambda: {  # type: ignore[method-assign]
                "databaseBytes": 2000,
                "walBytes": 0,
                "shmBytes": 0,
                "freelistBytes": 1200,
            }
            assert store.schedule_compaction_if_needed(reason="write") is True
            thread = store._maintenance_thread
            assert thread is not None
            thread.join(timeout=5.0)
            assert calls == ["write"]


def test_large_database_also_asks_for_a_longer_quiet_window() -> None:
    with workspace_tmpdir("baluffo-store-large-idle") as data_dir:
        with BaluffoStore(
            data_dir,
            vacuum_large_database_bytes=1000,
            vacuum_idle_s=5.0,
            vacuum_large_database_idle_s=60.0,
        ) as store:
            assert store.effective_vacuum_idle_s(999) == 5.0
            assert store.effective_vacuum_idle_s(1000) == 60.0
            assert store.compaction_status()["idleGateS"] == 5.0
            assert store.compaction_status()["largeDatabaseIdleS"] == 60.0


def test_large_database_gate_wait_is_reported_on_skip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def record_vacuum(self: BaluffoStore, *, reason: str = "") -> dict[str, object]:
        calls.append(reason)
        return {}

    monkeypatch.setattr(BaluffoStore, "vacuum", record_vacuum)

    with workspace_tmpdir("baluffo-store-large-idle-skip") as data_dir:
        with BaluffoStore(
            data_dir,
            vacuum_freelist_threshold_bytes=1,
            vacuum_large_database_bytes=1000,
            vacuum_idle_s=1.0,
            vacuum_large_database_idle_s=60.0,
            vacuum_idle_max_wait_s=0.05,
            vacuum_idle_poll_s=0.05,
        ) as store:
            store.file_sizes = lambda: {  # type: ignore[method-assign]
                "databaseBytes": 2000,
                "walBytes": 0,
                "shmBytes": 0,
                "freelistBytes": 1800,
            }
            store._last_write_monotonic = time.monotonic()

            assert store._run_compaction_when_idle(reason="write") is False
            assert calls == []
            status = store.compaction_status()
            assert status["status"] == "skipped-busy"
            assert status["effectiveIdleGateS"] == 60.0
            assert status["idleGateS"] == 1.0
            assert status["largeDatabaseIdleS"] == 60.0


def test_deferred_compaction_record_uses_the_effective_ratio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def record_vacuum(self: BaluffoStore, *, reason: str = "") -> dict[str, object]:
        calls.append(reason)
        return {}

    monkeypatch.setattr(BaluffoStore, "vacuum", record_vacuum)

    with workspace_tmpdir("baluffo-store-deferred-ratio") as data_dir:
        with BaluffoStore(
            data_dir,
            defer_maintenance=True,
            vacuum_freelist_threshold_bytes=1,
            vacuum_freelist_ratio=0.2,
            vacuum_large_database_bytes=1000,
            vacuum_large_database_ratio=0.5,
        ) as store:
            store.file_sizes = lambda: {  # type: ignore[method-assign]
                "databaseBytes": 2000,
                "walBytes": 0,
                "shmBytes": 0,
                "freelistBytes": 1200,
            }

            assert store.schedule_compaction_if_needed(reason="write") is False
            status = store.compaction_status()
            assert status["status"] == "deferred"
            assert status["freelistBytes"] == 1200
            assert status["databaseBytes"] == 2000
            assert status["largeDatabaseFreelistRatio"] == 0.5
            assert status["idleGateS"] == store.vacuum_idle_s
            assert status["lastWriteAgoS"] >= 0.0
            assert calls == []


def _bloated_sizes() -> dict[str, int]:
    return {"databaseBytes": 2000, "walBytes": 0, "shmBytes": 0, "freelistBytes": 1800}


def test_active_task_hold_records_the_rebuild_without_waiting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def record_vacuum(self: BaluffoStore, *, reason: str = "") -> dict[str, object]:
        calls.append(reason)
        return {}

    monkeypatch.setattr(BaluffoStore, "vacuum", record_vacuum)

    with workspace_tmpdir("baluffo-store-task-hold") as data_dir:
        with BaluffoStore(
            data_dir,
            compaction_hold=True,
            vacuum_freelist_threshold_bytes=1,
            vacuum_idle_s=30.0,
            vacuum_idle_max_wait_s=600.0,
        ) as store:
            store.file_sizes = lambda: _bloated_sizes()  # type: ignore[method-assign]
            store._last_write_monotonic = time.monotonic()

            started = time.perf_counter()
            assert store._run_compaction_when_idle(reason="write") is False
            waited = time.perf_counter() - started

            # Held means held, not "wait 600 s and then rebuild".
            assert waited < 1.0
            assert calls == []
            status = store.compaction_status()
            assert status["status"] == "held-active-task"
            assert status["reason"] == "write"
            assert status["heldByActiveTask"] is True
            assert status["databaseBytes"] == 2000
            assert status["freelistBytes"] == 1800


def test_releasing_the_task_hold_reschedules_a_due_rebuild(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def record_vacuum(self: BaluffoStore, *, reason: str = "") -> dict[str, object]:
        calls.append(reason)
        return {}

    monkeypatch.setattr(BaluffoStore, "vacuum", record_vacuum)

    with workspace_tmpdir("baluffo-store-task-release") as data_dir:
        with BaluffoStore(
            data_dir,
            compaction_hold=True,
            vacuum_freelist_threshold_bytes=1,
            vacuum_idle_s=0.0,
        ) as store:
            store.file_sizes = lambda: _bloated_sizes()  # type: ignore[method-assign]
            assert store.compaction_held is True

            assert store.set_compaction_hold(False) is True
            thread = store._maintenance_thread
            assert thread is not None
            thread.join(timeout=5.0)

            assert store.compaction_held is False
            assert calls == ["compaction-hold-released"]
            # A second release is a no-op, so a repeated terminal transition
            # cannot queue a second rebuild.
            assert store.set_compaction_hold(False) is False


def test_releasing_the_task_hold_without_a_bloated_freelist_schedules_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def record_vacuum(self: BaluffoStore, *, reason: str = "") -> dict[str, object]:
        calls.append(reason)
        return {}

    monkeypatch.setattr(BaluffoStore, "vacuum", record_vacuum)

    with workspace_tmpdir("baluffo-store-task-release-idle") as data_dir:
        with BaluffoStore(
            data_dir,
            compaction_hold=True,
            vacuum_freelist_threshold_bytes=1,
        ) as store:
            store.file_sizes = lambda: {  # type: ignore[method-assign]
                "databaseBytes": 2000,
                "walBytes": 0,
                "shmBytes": 0,
                "freelistBytes": 10,
            }

            assert store.set_compaction_hold(False) is False
            assert calls == []


def test_deferred_store_skips_released_maintenance_when_not_needed() -> None:
    with workspace_tmpdir("baluffo-store-deferred-idle") as data_dir:
        with BaluffoStore(data_dir, defer_maintenance=True) as store:
            store.set_maintenance_deferred(True)
            assert store.wal_maintenance_status()["deferred"] is True

            assert store.resume_maintenance_after_serving(reason="idle-serve", grace_s=0.0) is True
            thread = store._maintenance_thread
            if thread is not None:
                thread.join(timeout=5.0)

            assert store.maintenance_deferred is False
            assert store.wal_maintenance_status()["deferred"] is False
            assert store.health()["healthy"] is True
