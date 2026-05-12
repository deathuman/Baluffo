from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.storage import BaluffoStore, BaluffoStoreError
from tests.helpers.temp_paths import workspace_tmpdir


def test_store_initializes_wal_mode_health_and_json_authority_defaults() -> None:
    with workspace_tmpdir("baluffo-store") as data_dir:
        with BaluffoStore(data_dir) as store:
            health = store.health()

        assert Path(str(health["databasePath"])).parent == data_dir.resolve()
        assert health["migrationVersion"] == "005"
        assert health["walMode"] == "wal"
        assert health["foreignKeys"] == 1
        assert health["quickCheck"] == "ok"
        assert health["healthy"] is True
        assert health["authorityModes"] == {
            "taskRuns": "json",
            "taskEvents": "json",
            "syncRuns": "json",
            "sourceRuns": "json",
            "jobsFeed": "json",
            "sourceRegistry": "json",
        }


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


def test_busy_retry_handles_transient_write_busy_error() -> None:
    with workspace_tmpdir("baluffo-store-busy") as data_dir:
        with BaluffoStore(
            data_dir,
            busy_timeout_ms=5,
            busy_retry_attempts=8,
            busy_retry_base_ms=10,
            busy_retry_max_ms=20,
        ) as store:
            store.write(
                lambda conn: conn.execute(
                    "CREATE TABLE busy_probe (id INTEGER PRIMARY KEY, label TEXT NOT NULL)"
                )
            )
            attempts = 0

            def flaky_insert(conn: sqlite3.Connection) -> None:
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise sqlite3.OperationalError("database is locked")
                conn.execute(
                    "INSERT INTO busy_probe(id, label) VALUES (?, ?)",
                    (1, "ok"),
                )

            store.write(flaky_insert)

            assert store.execute_scalar("SELECT COUNT(*) FROM busy_probe") == 1
            assert attempts == 2
            assert store.health()["busyCount"] >= 1


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


def test_required_checkpoint_failure_marks_store_unhealthy() -> None:
    with workspace_tmpdir("baluffo-store-checkpoint") as data_dir:
        with BaluffoStore(
            data_dir,
            busy_retry_attempts=2,
            busy_retry_base_ms=1,
            busy_retry_max_ms=1,
        ) as store:
            store._execute_checkpoint = lambda mode: (1, 2, 0)  # type: ignore[method-assign]

            with pytest.raises(BaluffoStoreError, match="checkpoint failed"):
                store.checkpoint_required()

            health = store.health()
            assert health["healthy"] is False
            assert health["lastWriteError"].startswith("SQLite WAL checkpoint busy")
