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


def test_write_reraises_non_busy_operational_error_without_retry() -> None:
    with workspace_tmpdir("baluffo-store-non-busy") as data_dir:
        with BaluffoStore(data_dir, busy_retry_attempts=3) as store:
            attempts = 0

            def fail(conn: sqlite3.Connection) -> None:
                nonlocal attempts
                attempts += 1
                raise sqlite3.OperationalError("syntax exploded")

            with pytest.raises(sqlite3.OperationalError, match="syntax exploded"):
                store.write(fail)

            assert attempts == 1
            health = store.health()
            assert health["healthy"] is False
            assert health["lastWriteError"] == "syntax exploded"


def test_write_busy_retry_exhaustion_marks_store_unhealthy() -> None:
    with workspace_tmpdir("baluffo-store-busy-exhaustion") as data_dir:
        with BaluffoStore(
            data_dir,
            busy_retry_attempts=2,
            busy_retry_base_ms=1,
            busy_retry_max_ms=1,
        ) as store:
            store._sleep_for_retry = lambda _attempt: None  # type: ignore[method-assign]

            with pytest.raises(BaluffoStoreError, match="busy retry attempts"):
                store.write(
                    lambda _conn: (_ for _ in ()).throw(
                        sqlite3.OperationalError("database is locked")
                    )
                )

            health = store.health()
            assert health["healthy"] is False
            assert health["busyCount"] == 2
            assert health["lastWriteError"] == "database is locked"


def test_checkpoint_validation_and_non_busy_error_paths() -> None:
    with workspace_tmpdir("baluffo-store-checkpoint-errors") as data_dir:
        with BaluffoStore(data_dir, busy_retry_attempts=2) as store:
            with pytest.raises(ValueError, match="Unsupported SQLite checkpoint mode"):
                store.checkpoint_required(mode="invalid")

            store._execute_checkpoint = lambda _mode: (_ for _ in ()).throw(  # type: ignore[method-assign]
                sqlite3.OperationalError("checkpoint syntax exploded")
            )

            with pytest.raises(sqlite3.OperationalError, match="checkpoint syntax exploded"):
                store.checkpoint_required(mode="PASSIVE")

            assert store.health()["healthy"] is False
            assert store.health()["lastWriteError"] == "checkpoint syntax exploded"


def test_backup_and_restore_validation_errors() -> None:
    with workspace_tmpdir("baluffo-store-backup-errors") as data_dir:
        with BaluffoStore(data_dir / "source") as store:
            store.quick_check = lambda: "corrupt"  # type: ignore[method-assign]

            with pytest.raises(BaluffoStoreError, match="quick_check failed before backup"):
                store.backup_to(data_dir / "backup.db")

        with pytest.raises(BaluffoStoreError, match="backup file not found"):
            BaluffoStore.restore_backup(data_dir / "missing.db", data_dir / "restore")

        invalid = data_dir / "invalid.db"
        invalid.write_text("not sqlite", encoding="utf-8")
        with pytest.raises(sqlite3.DatabaseError):
            BaluffoStore.restore_backup(invalid, data_dir / "restore-invalid")


def test_execute_read_and_scalar_reuse_cached_read_connection() -> None:
    with workspace_tmpdir("baluffo-store-read-cache") as data_dir:
        store = BaluffoStore(data_dir)
        try:
            store.execute_read("SELECT 1 AS value")
            reader = store._reader

            assert reader is not None
            assert store.execute_scalar("SELECT 2") == 2
            assert store._reader is reader
        finally:
            store.close()

        assert store._reader is None


def test_execute_read_retries_transient_busy_error() -> None:
    with workspace_tmpdir("baluffo-store-read-busy") as data_dir:
        with BaluffoStore(
            data_dir,
            busy_retry_attempts=3,
            busy_retry_base_ms=1,
            busy_retry_max_ms=1,
        ) as store:
            store._sleep_for_retry = lambda _attempt: None  # type: ignore[method-assign]
            real_fetchall = store._execute_read_fetchall
            attempts = 0

            def flaky_fetchall(conn, sql, parameters):  # noqa: ANN001
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise sqlite3.OperationalError("database is locked")
                return real_fetchall(conn, sql, parameters)

            store._execute_read_fetchall = flaky_fetchall  # type: ignore[method-assign]

            assert store.execute_read("SELECT 1 AS value") == [{"value": 1}]
            assert attempts == 2
            health = store.health()
            assert health["busyCount"] >= 1
            assert health["lastWriteError"] == ""


def test_execute_read_busy_retry_exhaustion_marks_store_unhealthy() -> None:
    with workspace_tmpdir("baluffo-store-read-busy-exhaustion") as data_dir:
        with BaluffoStore(
            data_dir,
            busy_retry_attempts=2,
            busy_retry_base_ms=1,
            busy_retry_max_ms=1,
        ) as store:
            store._sleep_for_retry = lambda _attempt: None  # type: ignore[method-assign]
            real_fetchall = store._execute_read_fetchall

            def locked_fetchall(_conn, _sql, _parameters):  # noqa: ANN001
                raise sqlite3.OperationalError("database is locked")

            store._execute_read_fetchall = locked_fetchall  # type: ignore[method-assign]

            with pytest.raises(BaluffoStoreError, match="SQLite read failed"):
                store.execute_read("SELECT 1 AS value")

            store._execute_read_fetchall = real_fetchall  # type: ignore[method-assign]
            health = store.health()
            assert health["healthy"] is False
            assert health["busyCount"] >= 2
            assert health["lastWriteError"] == "database is locked"


def test_execute_read_reraises_non_busy_operational_error_without_retry() -> None:
    with workspace_tmpdir("baluffo-store-read-non-busy") as data_dir:
        with BaluffoStore(data_dir, busy_retry_attempts=3) as store:
            attempts = 0

            def broken_fetchall(_conn, _sql, _parameters):  # noqa: ANN001
                nonlocal attempts
                attempts += 1
                raise sqlite3.OperationalError("read syntax exploded")

            store._execute_read_fetchall = broken_fetchall  # type: ignore[method-assign]

            with pytest.raises(sqlite3.OperationalError, match="read syntax exploded"):
                store.execute_read("SELECT 1 AS value")

            assert attempts == 1
