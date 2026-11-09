from __future__ import annotations

import sqlite3

import pytest

from src.storage import BaluffoStore
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

    def fake_retention(store: BaluffoStore, *, reason: str = "") -> dict[str, object]:
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
