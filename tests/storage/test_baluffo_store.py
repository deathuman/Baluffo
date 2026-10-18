"""Store lifecycle, health, backup and space-reclamation behavior of ``BaluffoStore``.

The single-call write/read path (busy retries, checkpointing, slow-write
journaling) lives in ``test_baluffo_store_write_path.py`` so each module stays
under the test line budget and owns one concern.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.storage import BaluffoStore, BaluffoStoreError
from tests.helpers.temp_paths import workspace_tmpdir


def test_store_initializes_wal_mode_health_and_authority_defaults() -> None:
    with workspace_tmpdir("baluffo-store") as data_dir:
        with BaluffoStore(data_dir) as store:
            health = store.health()

        assert Path(str(health["databasePath"])).parent == data_dir.resolve()
        assert health["migrationVersion"] == "008"
        assert health["walMode"] == "wal"
        assert health["foreignKeys"] == 1
        assert health["quickCheck"] == "ok"
        assert health["quickCheckScope"] == "limited"
        assert health["quickCheckDeferred"] is False
        assert health["healthy"] is True
        assert health["databaseBytes"] > 0
        assert health["walBytes"] >= 0
        assert health["walMaintenance"]["checkpointThresholdBytes"] > 0
        assert health["authorityModes"] == {
            "taskRuns": "sqlite",
            "taskEvents": "sqlite",
            "syncRuns": "sqlite",
            "sourceRuns": "sqlite",
            "jobsFeed": "sqlite",
            "sourceRegistry": "sqlite",
        }


def test_store_startup_does_not_run_quick_check(monkeypatch: pytest.MonkeyPatch) -> None:
    limits: list[int | None] = []

    def fake_quick_check(self: BaluffoStore, *, limit: int | None = None) -> str:
        limits.append(limit)
        return "ok"

    monkeypatch.setattr(BaluffoStore, "quick_check", fake_quick_check)

    with workspace_tmpdir("baluffo-store-limited-quick-check") as data_dir:
        with BaluffoStore(data_dir):
            pass

    assert limits == []


def test_health_defers_quick_check_for_large_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("baluffo-store-deferred-quick-check") as data_dir:
        with BaluffoStore(data_dir, quick_check_max_database_bytes=10) as store:

            def fail_quick_check(*_args, **_kwargs) -> str:
                raise AssertionError("quick_check should be deferred")

            monkeypatch.setattr(store, "quick_check", fail_quick_check)
            health = store.health()

        assert health["quickCheck"] == "deferred-large-database"
        assert health["quickCheckScope"] == "deferred"
        assert health["quickCheckDeferred"] is True
        assert health["healthy"] is True


def test_wal_maintenance_is_scheduled_for_oversized_wal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def fake_run(self: BaluffoStore, *, reason: str, mode: str) -> None:
        calls.append((reason, mode))

    monkeypatch.setattr(BaluffoStore, "_run_wal_maintenance", fake_run)

    with workspace_tmpdir("baluffo-store-wal-maintenance") as data_dir:
        with BaluffoStore(
            data_dir,
            wal_checkpoint_threshold_bytes=1024 * 1024,
            wal_truncate_threshold_bytes=2 * 1024 * 1024,
        ) as store:
            store.wal_checkpoint_threshold_bytes = 1
            store.wal_truncate_threshold_bytes = 2
            store.db_path.with_name(f"{store.db_path.name}-wal").write_bytes(b"wal")

            scheduled = store.schedule_wal_maintenance_if_needed(reason="test")
            if store._maintenance_thread is not None:
                store._maintenance_thread.join(timeout=1)

            assert scheduled is True
            assert store.wal_maintenance_status()["status"] in {"scheduled", "ok"}

    assert ("test", "TRUNCATE") in calls


def test_bulk_execute_partitions_rows_into_bounded_transactions() -> None:
    with workspace_tmpdir("baluffo-store-batch") as data_dir:
        with BaluffoStore(data_dir) as store:
            store.write(
                lambda conn: conn.execute(
                    "CREATE TABLE batch_probe (id INTEGER PRIMARY KEY, label TEXT NOT NULL)"
                )
            )

            inserted = store.bulk_execute(
                "INSERT INTO batch_probe(id, label) VALUES (?, ?)",
                ((idx, f"row-{idx}") for idx in range(1500)),
                batch_size=500,
            )

            assert inserted == 1500
            assert store.execute_scalar("SELECT COUNT(*) FROM batch_probe") == 1500


def test_backup_restore_round_trip_uses_sqlite_backup_api() -> None:
    with workspace_tmpdir("baluffo-store-backup") as data_dir:
        backup_path = data_dir / "backup.db"
        restore_dir = data_dir / "restored"
        with BaluffoStore(data_dir / "source") as store:
            store.write(
                lambda conn: conn.execute(
                    "CREATE TABLE backup_probe (id INTEGER PRIMARY KEY, label TEXT NOT NULL)"
                )
            )
            store.write(
                lambda conn: conn.execute(
                    "INSERT INTO backup_probe(id, label) VALUES (?, ?)", (1, "preserved")
                )
            )
            store.backup_to(backup_path)

        with BaluffoStore.restore_backup(backup_path, restore_dir) as restored:
            rows = restored.execute_read("SELECT id, label FROM backup_probe")

        assert rows == [{"id": 1, "label": "preserved"}]


def test_backup_and_restore_validation_errors() -> None:
    with workspace_tmpdir("baluffo-store-backup-errors") as data_dir:
        with BaluffoStore(data_dir / "source") as store:

            def _fake_quick_check(*, limit: int | None = None) -> str:
                return "corrupt"

            store.quick_check = _fake_quick_check  # type: ignore[method-assign]

            with pytest.raises(BaluffoStoreError, match="quick_check failed before backup"):
                store.backup_to(data_dir / "backup.db")

        with pytest.raises(BaluffoStoreError, match="backup file not found"):
            BaluffoStore.restore_backup(data_dir / "missing.db", data_dir / "restore")

        invalid = data_dir / "invalid.db"
        invalid.write_text("not sqlite", encoding="utf-8")
        with pytest.raises(sqlite3.DatabaseError):
            BaluffoStore.restore_backup(invalid, data_dir / "restore-invalid")


def test_store_freelist_info_and_vacuum_reclaims_pages() -> None:
    with workspace_tmpdir("baluffo-store-vacuum") as data_dir:
        with BaluffoStore(data_dir) as store:
            store.write(lambda conn: conn.execute("CREATE TABLE test_bloat (id INT, data TEXT)"))
            # Insert large payload rows to create enough pages for a meaningful vacuum
            large_text = "x" * 20000
            bloat_rows: list[tuple[int, str]] = [(i, large_text) for i in range(100)]

            def insert_bloat(conn: sqlite3.Connection) -> None:
                conn.executemany("INSERT INTO test_bloat VALUES (?, ?)", bloat_rows)

            store.write(insert_bloat)

            store.checkpoint_required(mode="TRUNCATE")
            sizes_before_delete = store.file_sizes()
            assert sizes_before_delete["databaseBytes"] > 100_000

            # Delete rows and checkpoint to create freelist pages in the db file
            store.write(lambda conn: conn.execute("DELETE FROM test_bloat"))
            store.checkpoint_required(mode="TRUNCATE")
            info = store.freelist_info()
            assert info["freelistCount"] > 0
            assert info["freelistBytes"] > 0
            assert store.file_sizes()["freelistBytes"] == info["freelistBytes"]

            # Vacuum
            vac_result = store.vacuum(reason="unit-test")
            assert vac_result["status"] == "ok"
            assert vac_result["reclaimedBytes"] > 0

            info_after = store.freelist_info()
            assert info_after["freelistCount"] == 0
            assert info_after["freelistBytes"] == 0

            health = store.health()
            assert health["compaction"]["status"] == "ok"
            assert health["compaction"]["reason"] == "unit-test"
