"""Write/read path behavior of ``BaluffoStore``: retries, checkpoints, slow writes.

Split out of ``test_baluffo_store.py`` so each module stays under the test line
budget and owns one concern: this module covers what happens on a single
``write()``/``execute_read()`` call, while ``test_baluffo_store.py`` covers
store lifecycle, health, backup and space reclamation.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import cast

import pytest

from src.storage import BaluffoStore, BaluffoStoreError
from tests.helpers.temp_paths import workspace_tmpdir


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


def test_required_checkpoint_failure_marks_store_unhealthy() -> None:
    with workspace_tmpdir("baluffo-store-checkpoint") as data_dir:
        with BaluffoStore(
            data_dir,
            busy_retry_attempts=2,
            busy_retry_base_ms=1,
            busy_retry_max_ms=1,
        ) as store:
            store._execute_checkpoint = lambda mode: cast(sqlite3.Row, (1, 2, 0))  # type: ignore[method-assign]

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
            store._sleep_for_retry = lambda attempt: None  # type: ignore[method-assign]

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

            store._execute_checkpoint = lambda mode: (_ for _ in ()).throw(  # type: ignore[method-assign]
                sqlite3.OperationalError("checkpoint syntax exploded")
            )

            with pytest.raises(sqlite3.OperationalError, match="checkpoint syntax exploded"):
                store.checkpoint_required(mode="PASSIVE")

            assert store.health()["healthy"] is False
            assert store.health()["lastWriteError"] == "checkpoint syntax exploded"


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
            store._sleep_for_retry = lambda attempt: None  # type: ignore[method-assign]
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
            store._sleep_for_retry = lambda attempt: None  # type: ignore[method-assign]
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


def _slow_store_write_rows(data_dir: Path) -> list[dict[str, object]]:
    path = data_dir / "storage-metrics.jsonl"
    if not path.exists():
        return []
    return [
        row
        for row in (
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
        if row.get("type") == "sqliteSlowWrite"
    ]


def test_slow_store_write_records_thread_and_stack() -> None:
    with workspace_tmpdir("baluffo-store-slow-write") as data_dir:
        with BaluffoStore(data_dir, slow_write_warn_ms=1) as store:

            def slow_query(conn):
                time.sleep(0.02)
                return conn.execute("SELECT 1").fetchone()

            store.write(slow_query)

        rows = _slow_store_write_rows(data_dir)
        write_rows = [row for row in rows if row.get("operation") == "write" and row.get("holdMs")]

        assert write_rows, "a write held past the threshold must be journaled"
        assert write_rows[-1]["thread"] == threading.current_thread().name
        assert int(cast(int, write_rows[-1]["holdMs"])) >= 1
        assert "test_slow_store_write_records_thread_and_stack" in str(write_rows[-1]["stack"])


def test_fast_store_write_stays_out_of_the_slow_write_journal() -> None:
    with workspace_tmpdir("baluffo-store-fast-write") as data_dir:
        with BaluffoStore(data_dir) as store:
            store.write(lambda conn: conn.execute("SELECT 1").fetchone())

        stacks = "".join(str(row.get("stack") or "") for row in _slow_store_write_rows(data_dir))

        assert "test_fast_store_write_stays_out_of_the_slow_write_journal" not in stacks
