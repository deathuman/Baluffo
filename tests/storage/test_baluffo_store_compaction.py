from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime

import pytest

from src.storage import BaluffoStore, SourceRuntimeStore
from tests.helpers.temp_paths import workspace_tmpdir


def test_vacuum_on_a_large_store_rebuilds_through_a_compact_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def record_compact(self: BaluffoStore, *, reason: str = "") -> dict[str, object]:
        calls.append(f"compact:{reason}")
        return {}

    monkeypatch.setattr(BaluffoStore, "compact_database", record_compact)

    with workspace_tmpdir("baluffo-store-compact-copy") as data_dir:
        with BaluffoStore(data_dir, vacuum_large_database_bytes=1) as store:
            store.vacuum(reason="retention")

            assert calls == ["compact:retention"]


def test_compact_database_skips_when_free_space_cannot_hold_the_live_bytes() -> None:
    with workspace_tmpdir("baluffo-store-compact-space") as data_dir:
        with BaluffoStore(data_dir) as store:
            store._free_disk_bytes = lambda: 1024  # type: ignore[method-assign]

            status = store.compact_database(reason="test")

            assert status["status"] == "skipped-no-space"
            assert status["method"] == "vacuum-into"
            assert status["freeBytes"] == 1024
            assert status["requiredBytes"] > 1024
            assert status["reclaimedBytes"] == 0
            assert store.compaction_status()["status"] == "skipped-no-space"
            assert store.execute_scalar("SELECT COUNT(*) FROM schema_migrations") >= 1


def test_interrupted_compact_copy_keeps_the_original(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def interrupted(self: BaluffoStore, staging: object) -> None:
        raise sqlite3.OperationalError("interrupted")

    monkeypatch.setattr(BaluffoStore, "_vacuum_into_copy", interrupted)

    with workspace_tmpdir("baluffo-store-compact-budget") as data_dir:
        with BaluffoStore(data_dir, compaction_budget_s=0.0) as store:
            store._free_disk_bytes = lambda: 10**12  # type: ignore[method-assign]
            store.write(lambda conn: conn.execute("CREATE TABLE marker(value TEXT)"))
            store.write(lambda conn: conn.execute("INSERT INTO marker(value) VALUES ('kept')"))

            status = store.compact_database(reason="test")

            assert status["status"] == "budget-exceeded"
            assert status["error"] == "interrupted"
            assert store.execute_read("SELECT value FROM marker") == [{"value": "kept"}]
            assert list(store.data_dir.glob("*.compact-*.tmp")) == []


def test_compact_database_reclaims_free_pages_and_keeps_rows() -> None:
    with workspace_tmpdir("baluffo-store-compact-reclaim") as data_dir:
        with BaluffoStore(data_dir) as store:
            store.write(lambda conn: conn.execute("CREATE TABLE junk(value TEXT)"))
            store.bulk_execute(
                "INSERT INTO junk(value) VALUES (?)",
                [("x" * 2048,) for _ in range(3000)],
                batch_size=250,
            )
            store.write(lambda conn: conn.execute("DELETE FROM junk WHERE rowid > 5"))
            before = store.file_sizes()
            assert before["freelistBytes"] > 0

            status = store.compact_database(reason="retention")

            assert status["status"] == "ok", status
            assert status["method"] == "vacuum-into"
            assert status["afterBytes"] < status["beforeBytes"]
            assert status["reclaimedBytes"] > 0
            assert store.execute_scalar("SELECT COUNT(*) FROM junk") == 5
            assert store.migration_version() != ""
            assert store.compaction_status()["method"] == "vacuum-into"


def test_retention_deletions_schedule_a_compaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduled: list[str] = []

    def fake_retention(store: BaluffoStore, *, reason: str = "", **_: object) -> dict[str, object]:
        return {
            "status": "ok",
            "reason": reason,
            "deletedRunCount": 2,
            "deletedRowCount": 40,
        }

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", fake_retention)

    with workspace_tmpdir("baluffo-store-retention-compaction") as data_dir:
        with BaluffoStore(data_dir) as store:

            def record_compaction(*, reason: str = "") -> bool:
                scheduled.append(reason)
                return False

            monkeypatch.setattr(store, "schedule_compaction_if_needed", record_compaction)

            assert store._run_runtime_retention(reason="post-serve") is True

            assert scheduled == ["post-serve:retention"]


def test_retention_pass_without_deletions_retries_due_compaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduled: list[str] = []

    def fake_retention(store: BaluffoStore, *, reason: str = "", **_: object) -> dict[str, object]:
        return {
            "status": "nothing-to-prune",
            "reason": reason,
            "deletedRunCount": 0,
            "deletedRowCount": 0,
        }

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", fake_retention)

    with workspace_tmpdir("baluffo-store-retention-retry-compaction") as data_dir:
        with BaluffoStore(data_dir) as store:

            def record_compaction(*, reason: str = "") -> bool:
                scheduled.append(reason)
                return False

            monkeypatch.setattr(store, "schedule_compaction_if_needed", record_compaction)

            assert store._run_runtime_retention(reason="post-serve") is True

            assert scheduled == ["post-serve:retention"]


def test_truncated_retention_schedules_followup_before_compaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduled: list[str] = []

    def fake_retention(store: BaluffoStore, *, reason: str = "", **_: object) -> dict[str, object]:
        return {
            "status": "ok",
            "reason": reason,
            "deletedRunCount": 2,
            "deletedRowCount": 40,
            "truncated": True,
        }

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", fake_retention)

    with workspace_tmpdir("baluffo-store-retention-followup") as data_dir:
        with BaluffoStore(data_dir) as store:

            def record_compaction(*, reason: str = "") -> bool:
                scheduled.append(reason)
                return False

            monkeypatch.setattr(store, "schedule_compaction_if_needed", record_compaction)

            assert store._run_runtime_retention(reason="post-serve") is True
            status = store.runtime_retention_status()

            assert scheduled == []
            assert status["timerScheduled"] is True
            assert 0 < status["nextRunInS"] <= 30
            assert status["done"] is False


def test_runtime_retention_status_reports_live_phase(monkeypatch: pytest.MonkeyPatch) -> None:
    started = threading.Event()
    release = threading.Event()

    def fake_retention(store: BaluffoStore, *, reason: str = "", on_progress=None):
        assert on_progress is not None
        on_progress(
            {
                "phase": "prune-source-runs",
                "stage": "deleting",
                "processedRunCount": 2,
                "totalRunCount": 10,
                "deletedRunCount": 2,
                "deletedRowCount": 20,
            }
        )
        started.set()
        assert release.wait(5)
        return {
            "status": "ok",
            "reason": reason,
            "deletedRunCount": 2,
            "deletedRowCount": 20,
        }

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", fake_retention)

    with workspace_tmpdir("baluffo-store-retention-phase") as data_dir:
        with BaluffoStore(data_dir) as store:
            monkeypatch.setattr(store, "schedule_compaction_if_needed", lambda **_: False)
            worker = threading.Thread(
                target=lambda: store._run_runtime_retention(reason="post-serve")
            )
            worker.start()
            try:
                assert started.wait(5)
                live = store.runtime_retention_status()
                assert live["running"] is True
                assert live["done"] is False
                assert live["status"] == "running"
                assert live["phase"] == "prune-source-runs"
                assert live["processedRunCount"] == 2
                assert live["totalRunCount"] == 10
                assert live["elapsedMs"] >= 0
            finally:
                release.set()
                worker.join(5)

            final = store.runtime_retention_status()
            if store._retention_timer is not None:
                store._retention_timer.cancel()
            assert final["running"] is False
            assert final["status"] == "ok"
            assert final["phase"] == "complete"
            assert final["durationMs"] >= 0


def test_runtime_retention_start_clears_previous_job_generation_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[dict[str, object]] = []

    def fake_retention(
        store: BaluffoStore, *, reason: str = "", on_progress=None, **_: object
    ) -> dict[str, object]:
        live = store.runtime_retention_status()
        observed.append(
            {
                "jobGenerations": live.get("jobGenerations"),
                "truncated": live.get("truncated"),
            }
        )
        return {"status": "ok", "reason": reason}

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", fake_retention)

    with workspace_tmpdir("baluffo-store-retention-stale-jobs") as data_dir:
        with BaluffoStore(data_dir) as store:
            monkeypatch.setattr(store, "schedule_compaction_if_needed", lambda **_: False)
            store._last_retention = {
                **store._last_retention,
                "jobGenerations": {"deletedGenerationCount": 4},
                "truncated": True,
            }

            assert store._run_runtime_retention(reason="post-serve") is True
            if store._retention_timer is not None:
                store._retention_timer.cancel()

            assert observed == [{"jobGenerations": {}, "truncated": False}]


def test_runtime_retention_clears_stale_job_generation_counts_for_current_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[dict[str, object]] = []

    def fake_retention(
        store: BaluffoStore, *, reason: str = "", on_progress=None, **_: object
    ) -> dict[str, object]:
        live = store.runtime_retention_status()
        observed.append(
            {
                "deletedGenerationCount": live.get("deletedGenerationCount"),
                "jobGenerations": live.get("jobGenerations"),
            }
        )
        if on_progress is not None:
            on_progress(
                {
                    "phase": "prune-job-generations",
                    "stage": "deleting",
                    "deletedGenerationCount": 1,
                    "deletedRowCount": 2,
                }
            )
        return {
            "status": "ok",
            "reason": reason,
            "deletedGenerationCount": 1,
            "jobGenerations": {
                "status": "ok",
                "deletedGenerationCount": 1,
                "deletedRowCount": 2,
                "keepGenerations": 1,
                "truncated": False,
            },
            "truncated": False,
        }

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", fake_retention)

    with workspace_tmpdir("baluffo-store-retention-current-jobs") as data_dir:
        with BaluffoStore(data_dir) as store:
            monkeypatch.setattr(store, "schedule_compaction_if_needed", lambda **_: False)
            store._last_retention = {
                **store._last_retention,
                "deletedGenerationCount": 0,
                "jobGenerations": {
                    "deletedGenerationCount": 4,
                    "deletedRowCount": 8,
                    "truncated": True,
                },
                "truncated": True,
            }

            assert store._run_runtime_retention(reason="post-serve") is True
            if store._retention_timer is not None:
                store._retention_timer.cancel()

            final = store.runtime_retention_status()

    assert observed == [{"deletedGenerationCount": 0, "jobGenerations": {}}]
    assert final["deletedGenerationCount"] == 1
    assert final["jobGenerations"]["deletedGenerationCount"] == 1
    assert final["jobGenerations"]["deletedRowCount"] == 2
    assert final["truncated"] is False


def test_runtime_retention_bounds_event_and_sync_deletes() -> None:
    with workspace_tmpdir("source-runtime-bounded-events") as data_dir:
        with BaluffoStore(data_dir) as store:
            old = "2026-01-01T00:00:00Z"
            store.write(
                lambda conn: conn.execute(
                    """
                    INSERT INTO task_runs(run_id, task_type, status, started_at)
                    VALUES ('old_run', 'fetch', 'finished', ?)
                    """,
                    (old,),
                )
            )
            store.write(
                lambda conn: conn.executemany(
                    "INSERT INTO task_events(run_id, event, created_at) VALUES (?, 'old', ?)",
                    [("old_run", old) for _ in range(3)],
                )
            )
            store.write(
                lambda conn: conn.executemany(
                    "INSERT INTO sync_runs(run_id, status, started_at) VALUES (?, 'ok', ?)",
                    [(f"sync_{index}", old) for index in range(3)],
                )
            )
            runtime = SourceRuntimeStore(store)
            now = datetime(2026, 3, 15, tzinfo=UTC)

            first = runtime.prune_old_runs(
                retention_days=30,
                keep_runs=1,
                max_runs=10,
                max_event_rows=2,
                max_sync_runs=2,
                now=now,
            )
            second = runtime.prune_old_runs(
                retention_days=30,
                keep_runs=1,
                max_runs=10,
                max_event_rows=2,
                max_sync_runs=2,
                now=now,
            )

            assert first["deletedEventCount"] == 2
            assert first["deletedSyncRunCount"] == 2
            assert first["truncated"] is True
            assert second["deletedEventCount"] == 1
            assert second["deletedSyncRunCount"] == 1
            assert second["truncated"] is False
