"""A large store gets the integrity reading its live health check cannot produce.

The health quick check is capped at 64 MiB, so on the container's ~280 MB store it
returned `deferred-large-database` forever and `healthy: true` was a write-path verdict
with no integrity check behind it. These pin the background verification that replaced
it: it reads the whole file on its own connection inside a budget, records when it ran,
and says `never` when it has not run at all.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from src.storage.baluffo_store import BaluffoStore


def _deferred_store(tmp_path: Path, **kwargs: Any) -> BaluffoStore:
    # A 10-byte cap defers every real database, which is the state the live container
    # reports at 260 MB.
    return BaluffoStore(tmp_path, quick_check_max_database_bytes=10, **kwargs)


def _run_scheduled(store: BaluffoStore, *, force: bool = False) -> None:
    assert store.schedule_integrity_check_if_due(force=force) is True
    thread = store._integrity_thread
    assert thread is not None
    thread.join(timeout=30)


def test_a_deferred_store_reports_never_before_any_verification(tmp_path: Path) -> None:
    with _deferred_store(tmp_path) as store:
        status = store.integrity_check_status()

        assert status["status"] == "never"
        assert status["checkedAt"] == ""
        assert status["ageSeconds"] is None
        assert status["due"] is True


def test_the_background_check_verifies_the_file_and_records_when(tmp_path: Path) -> None:
    with _deferred_store(tmp_path) as store:
        _run_scheduled(store)

        status = store.integrity_check_status()

        assert status["status"] == "ok"
        assert status["checkedAt"]
        assert status["ageSeconds"] is not None
        assert status["ageSeconds"] >= 0
        assert status["databaseBytes"] > 0
        assert status["running"] is False


def test_a_fresh_verification_is_not_due_again(tmp_path: Path) -> None:
    with _deferred_store(tmp_path) as store:
        _run_scheduled(store)

        assert store.integrity_check_status()["due"] is False
        assert store.schedule_integrity_check_if_due() is False


def test_a_forced_run_reruns_the_check(tmp_path: Path) -> None:
    with _deferred_store(tmp_path) as store:
        _run_scheduled(store)

        _run_scheduled(store, force=True)
        assert store.integrity_check_status()["status"] == "ok"


def test_a_zero_interval_disables_the_schedule(tmp_path: Path) -> None:
    with _deferred_store(tmp_path, integrity_check_interval_s=0) as store:
        assert store.integrity_check_status()["due"] is False
        assert store.schedule_integrity_check_if_due() is False


def test_the_health_payload_keeps_the_deferred_verdict_and_adds_the_record(
    tmp_path: Path,
) -> None:
    with _deferred_store(tmp_path) as store:
        _run_scheduled(store)

        health = store.health()

        # The live field keeps its old meaning, so a consumer that gates on it is not
        # silently switched to a different check.
        assert health["quickCheck"] == "deferred-large-database"
        assert health["quickCheckDeferred"] is True
        assert health["integrityCheck"]["status"] == "ok"
        assert health["integrityCheck"]["checkedAt"]


def test_a_failed_verification_latches_the_store_and_names_the_reason(
    tmp_path: Path,
) -> None:
    with _deferred_store(tmp_path) as store:
        store._integrity_probe = lambda conn: "*** in database main ***"  # type: ignore[method-assign]
        _run_scheduled(store)

        status = store.integrity_check_status()
        health = store.health()

        assert status["status"] == "failed"
        assert "database main" in status["result"]
        assert health["healthy"] is False
        assert health["unhealthyReason"]["code"] == "integrity_check"


def test_an_interrupted_check_is_not_corruption(tmp_path: Path) -> None:
    def _interrupted(conn: sqlite3.Connection) -> str:
        raise sqlite3.OperationalError("interrupted")

    with _deferred_store(tmp_path) as store:
        store._integrity_probe = _interrupted  # type: ignore[method-assign]
        _run_scheduled(store)

        status = store.integrity_check_status()

        assert status["status"] == "incomplete"
        assert "budget" in status["error"]
        # An unfinished read proves nothing about the file, so it must not latch.
        assert store.health()["healthy"] is True
