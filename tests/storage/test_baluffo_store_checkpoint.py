"""Checkpoint contract of ``BaluffoStore``: passive-first escalation and busy states.

Split out of ``test_baluffo_store_write_path.py`` so each module owns one concern.
This module pins what a checkpoint reports and whether it may mark the store
unhealthy; the write path's retries, slow writes and recovery live in the write-path
module.
"""

from __future__ import annotations

import sqlite3
from typing import cast

import pytest

from src.storage import BaluffoStore
from tests.helpers.temp_paths import workspace_tmpdir


def test_busy_checkpoint_is_a_maintenance_state_not_a_store_failure() -> None:
    # A TRUNCATE checkpoint needs the WAL quiescent, and the store's own reader plus
    # the pipeline's reads routinely prevent that. Latching the store unhealthy on that
    # busy result made every later health read report an unattributable failure while
    # the WAL kept growing.
    with workspace_tmpdir("baluffo-store-checkpoint-busy") as data_dir:
        with BaluffoStore(
            data_dir,
            busy_retry_attempts=2,
            busy_retry_base_ms=1,
            busy_retry_max_ms=1,
        ) as store:
            store._execute_checkpoint = lambda mode: cast(sqlite3.Row, (1, 2, 0))  # type: ignore[method-assign]

            result = store.checkpoint_required()

            assert result["busy"] == 1
            assert result["complete"] is False
            assert result["detail"] == "passive-incomplete"

            health = store.health()
            assert health["healthy"] is True
            assert health["unhealthyReason"] == {}
            assert health["lastWriteError"].startswith("SQLite WAL checkpoint busy")


def test_busy_checkpoint_records_a_datable_busy_status() -> None:
    # The panel has to be able to say when a busy checkpoint was observed and that
    # it was a maintenance state rather than a failure, while keeping the WAL and
    # database sizes that make disk pressure actionable.
    with workspace_tmpdir("baluffo-store-checkpoint-status-busy") as data_dir:
        with BaluffoStore(data_dir, busy_retry_attempts=1) as store:
            store._execute_checkpoint = lambda mode: cast(sqlite3.Row, (1, 8, 4))  # type: ignore[method-assign]

            store._run_wal_maintenance(reason="test", mode="TRUNCATE")

            status = store.wal_maintenance_status()
            assert status["status"] == "busy"
            assert status["attemptedAt"]
            assert status["error"] == ""
            assert status["walBytes"] >= 0
            assert store.health()["healthy"] is True


def test_checkpoint_escalates_only_after_a_passive_pass_completed_every_frame() -> None:
    with workspace_tmpdir("baluffo-store-checkpoint-escalation") as data_dir:
        with BaluffoStore(data_dir, busy_retry_attempts=1) as store:
            modes: list[str] = []
            rows = {"PASSIVE": (0, 8, 8), "TRUNCATE": (0, 8, 8)}

            def record(mode: str) -> sqlite3.Row:
                modes.append(mode)
                return cast(sqlite3.Row, rows[mode])

            store._execute_checkpoint = record  # type: ignore[method-assign]

            result = store.checkpoint_required(mode="TRUNCATE")

            assert modes == ["PASSIVE", "TRUNCATE"]
            assert result["complete"] is True
            assert result["mode"] == "TRUNCATE"

            # Frames a reader is pinning make truncation pointless, so the escalated
            # mode is never attempted and the state stays retryable.
            modes.clear()
            rows["PASSIVE"] = (0, 8, 4)

            result = store.checkpoint_required(mode="TRUNCATE")

            assert modes == ["PASSIVE"]
            assert result["complete"] is False
            assert result["detail"] == "passive-incomplete"


def test_terminal_passive_busy_checkpoint_does_not_count_a_retry() -> None:
    with workspace_tmpdir("baluffo-store-passive-busy-terminal") as data_dir:
        with BaluffoStore(data_dir, busy_retry_attempts=1) as store:
            store._execute_checkpoint = lambda mode: cast(sqlite3.Row, (1, 8, 4))  # type: ignore[method-assign]

            def fail_retry(attempt: int) -> None:
                pytest.fail("a terminal busy checkpoint must not schedule a retry")

            store._sleep_for_retry = fail_retry  # type: ignore[method-assign]

            result = store.checkpoint_required(mode="PASSIVE")

            assert result["busy"] == 1
            assert result["complete"] is False
            status = store.wal_maintenance_status()
            assert status["passiveCheckpointBusyRetries"] == 0
            assert status["passiveCheckpoint"]["count"] == 0


def test_busy_operational_error_retry_increments_passive_busy_retries() -> None:
    with workspace_tmpdir("baluffo-store-passive-operational-error") as data_dir:
        with BaluffoStore(data_dir, busy_retry_attempts=2, busy_retry_base_ms=1) as store:
            attempts = 0

            def flaky_checkpoint(mode: str) -> sqlite3.Row:
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise sqlite3.OperationalError("database is locked")
                return cast(sqlite3.Row, (0, 8, 8))

            sleeps: list[int] = []
            store._execute_checkpoint = flaky_checkpoint  # type: ignore[method-assign]
            store._sleep_for_retry = sleeps.append  # type: ignore[assignment]

            result = store.checkpoint_required(mode="PASSIVE")

            assert result["busy"] == 0
            assert result["logFrames"] == 8
            assert result["checkpointedFrames"] == 8
            assert result["complete"] is True
            assert attempts == 2
            assert sleeps == [0]
            status = store.wal_maintenance_status()
            assert status["passiveCheckpointBusyRetries"] == 1
