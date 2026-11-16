"""Rehearsal (§9): the dry run measures a copy and installs nothing.

These cases pin the gates a rehearsal shares with a real run — retention stopped
before the copy, the run limit, cancel cleanup, and the safety blockers it still
refuses — plus the report it leaves in the reclaim journal.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from src.bridge import storage_health, storage_rebuild, storage_reclaim
from src.bridge.storage_rebuild import close_storage_rebuild_jobs
from src.storage import BaluffoStore, JobRuntimeStore, current_feed_rebuild
from src.storage.baluffo_store import DEFAULT_DB_NAME
from src.storage.current_feed_rebuild import (
    CONTENT_PARITY_TABLES,
    CurrentFeedRebuildError,
    rebuild_backup_path,
    rebuild_journal_path,
    rebuild_staging_path,
)
from tests.helpers.temp_paths import workspace_tmpdir


def _row(index: int) -> dict[str, object]:
    return {
        "id": index,
        "title": f"Job {index}",
        "company": "Studio",
        "city": "Amsterdam",
        "country": "Netherlands",
        "jobLink": f"https://example.test/jobs/{index}",
        "source": "Example",
        "sourceJobId": f"job-{index}",
        "status": "active",
        "firstSeenAt": "2026-11-15T10:00:00Z",
        "lastSeenAt": "2026-11-15T10:00:00Z",
        "sourceBundle": [],
    }


def _seed(data_dir: Path, *, generations: int = 2) -> None:
    with BaluffoStore(data_dir) as store:
        runtime = JobRuntimeStore(store, now_iso=lambda: "2026-11-15T10:00:00Z")
        for index in range(generations):
            runtime.replace_feed(run_id=f"fetch_{index}", rows=[_row(index)])


MIB = 1024 * 1024
GIB = 1024 * MIB
TERMINAL = storage_reclaim.RECLAIM_TERMINAL_STATUSES


def _bloat_verdict(*, blockers: list[str] | None = None) -> dict[str, Any]:
    return {
        "verdict": "bloat",
        "ratio": 0.2,
        "reclaimable": 2 * GIB,
        "databaseBytes": 10 * GIB,
        "freelistBytes": 0,
        "currentGenerationEstimatedBytes": 40 * MIB,
        "historicalGenerationEstimatedBytes": 2 * GIB,
        "historicalGenerationCount": 2,
        "estimatedReclaimableBytes": 2 * GIB,
        "reclaimableBasis": "payload-sample+freelist",
        "candidateGenerationCount": 0,
        "capReached": False,
        "blockers": list(blockers or []),
        "cooldownRemainingHours": 0.0,
    }


def _wait_for(predicate: Callable[[], bool], timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def _status(data_dir: Path) -> str:
    return str(storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]["status"])


def test_a_rehearsal_measures_the_copy_and_installs_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("reclaim-rehearsal") as data_dir:
        _seed(data_dir, generations=2)
        db_path = data_dir / DEFAULT_DB_NAME
        # A rehearsal is the step that precedes enabling auto mode, so an `ok`
        # verdict and an `auto-disabled` blocker must still let it measure.
        monkeypatch.setattr(
            storage_reclaim,
            "_compute_verdict",
            lambda *_args, **_kwargs: {
                **_bloat_verdict(blockers=["auto-disabled"]),
                "verdict": "ok",
            },
        )
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir, rehearsal=True)
            assert started["started"] is True, started
            assert started["storageReclaim"]["mode"] == "rehearsal"
            assert _wait_for(lambda: _status(data_dir) in TERMINAL, 60.0)
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]
            journal = storage_reclaim.read_reclaim_journal(data_dir)
            live_jobs = sqlite3.connect(db_path).execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            staging_exists = rebuild_staging_path(db_path, run_id=str(started["runId"])).exists()
            scratch_exists = storage_reclaim.rehearsal_journal_path(data_dir).exists()
            rebuild_journal_exists = rebuild_journal_path(db_path).exists()
            backup_exists = rebuild_backup_path(db_path).exists()
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    assert payload["status"] == "complete", payload
    assert payload["mode"] == "rehearsal"
    assert journal is not None
    assert journal["mode"] == "rehearsal"
    assert journal["status"] == "complete"
    assert journal["finishedAt"]
    result = journal["result"]
    assert result["stagingBytes"] > 0
    assert result["reclaimedBytes"] >= 0
    assert result["durationMs"] >= 0
    assert result["tableCounts"]["jobs"] == 1
    assert result["validation"]["quickCheck"] == "ok"
    assert set(result["copyDigests"]) == set(CONTENT_PARITY_TABLES)
    assert result["freeBytes"] > 0
    assert result["requiredFreeBytes"] > 0
    # A rehearsal keeps none of what it made and changes nothing it read.
    assert live_jobs == 2
    assert staging_exists is False
    assert scratch_exists is False
    assert rebuild_journal_exists is False
    assert backup_exists is False


def test_a_rehearsal_stops_runtime_retention_before_copying(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    real_copy_table = current_feed_rebuild._copy_table

    def stop_retention(_store: Any) -> bool:
        events.append("retention")
        return False

    def recording_copy_table(*args: Any, **kwargs: Any) -> int:
        events.append("copy")
        return real_copy_table(*args, **kwargs)

    with workspace_tmpdir("reclaim-rehearsal-retention") as data_dir:
        _seed(data_dir, generations=2)
        monkeypatch.setattr(storage_rebuild, "stop_runtime_retention_for_copy", stop_retention)
        monkeypatch.setattr(current_feed_rebuild, "_copy_table", recording_copy_table)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir, rehearsal=True)
            assert started["started"] is True, started
            assert _wait_for(lambda: _status(data_dir) in TERMINAL, 60.0)
            status = _status(data_dir)
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    assert status == "complete"
    # A real run stops retention before its copy; the rehearsal must not read a
    # database the reclaimer is still deleting from.
    assert events[:2] == ["retention", "copy"]


def test_a_rehearsal_fails_when_retention_will_not_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("reclaim-rehearsal-retention-stuck") as data_dir:
        _seed(data_dir, generations=2)
        db_path = data_dir / DEFAULT_DB_NAME

        def refusing_stop(_store: Any) -> bool:
            raise CurrentFeedRebuildError(
                "the background retention pass did not stop before the rebuild wait expired"
            )

        monkeypatch.setattr(storage_rebuild, "stop_runtime_retention_for_copy", refusing_stop)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir, rehearsal=True)
            assert started["started"] is True, started
            assert _wait_for(lambda: _status(data_dir) in TERMINAL, 60.0)
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]
            journal = storage_reclaim.read_reclaim_journal(data_dir)
            live_jobs = sqlite3.connect(db_path).execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            leftovers = sorted(
                path.name
                for path in data_dir.iterdir()
                if path.name.endswith(".tmp") or "rehearsal" in path.name
            )
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    assert payload["status"] == "failed", payload
    assert journal is not None
    assert "did not stop" in journal["error"]
    # Nothing was copied, so nothing was left behind either.
    assert live_jobs == 2
    assert leftovers == []


def test_a_rehearsal_fails_at_the_run_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    with workspace_tmpdir("reclaim-rehearsal-limit") as data_dir:
        _seed(data_dir, generations=2)
        db_path = data_dir / DEFAULT_DB_NAME
        monkeypatch.setattr(storage_reclaim, "RECLAIM_MAX_RUN_S", 0.0)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir, rehearsal=True)
            assert started["started"] is True, started
            assert _wait_for(lambda: _status(data_dir) in TERMINAL, 60.0)
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]
            journal = storage_reclaim.read_reclaim_journal(data_dir)
            live_jobs = sqlite3.connect(db_path).execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            leftovers = sorted(
                path.name
                for path in data_dir.iterdir()
                if path.name.endswith(".tmp") or "rehearsal" in path.name
            )
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    assert payload["status"] == "failed", payload
    assert journal is not None
    assert "run limit" in journal["error"]
    # The limit stops the copy and still cleans up after it.
    assert live_jobs == 2
    assert leftovers == []


def test_a_cancelled_rehearsal_removes_its_staging_and_scratch_journal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_copy_table = current_feed_rebuild._copy_table
    cancelled: list[int] = []

    with workspace_tmpdir("reclaim-rehearsal-cancel") as data_dir:
        _seed(data_dir, generations=2)
        db_path = data_dir / DEFAULT_DB_NAME

        def cancel_on_first_table(*args: Any, **kwargs: Any) -> int:
            if not cancelled:
                cancelled.append(1)
                journal = storage_reclaim.read_reclaim_journal(data_dir) or {}
                storage_reclaim.cancel_storage_reclaim(data_dir, str(journal.get("runId") or ""))
            return real_copy_table(*args, **kwargs)

        monkeypatch.setattr(current_feed_rebuild, "_copy_table", cancel_on_first_table)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir, rehearsal=True)
            assert started["started"] is True, started
            assert _wait_for(lambda: _status(data_dir) in TERMINAL, 60.0)
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]
            journal = storage_reclaim.read_reclaim_journal(data_dir)
            live_jobs = sqlite3.connect(db_path).execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            leftovers = sorted(
                path.name
                for path in data_dir.iterdir()
                if path.name.endswith(".tmp") or "rehearsal" in path.name
            )
            hold = storage_health.storage_reclaim_hold_status()
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    assert payload["status"] == "cancelled", payload
    assert journal is not None
    assert journal["status"] == "cancelled"
    assert live_jobs == 2
    # The staging copy and the scratch journal both belong to the run.
    assert leftovers == []
    assert hold["active"] is False


def test_a_failed_rehearsal_still_removes_its_staging_and_scratch_journal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("reclaim-rehearsal-failure") as data_dir:
        _seed(data_dir, generations=2)
        db_path = data_dir / DEFAULT_DB_NAME

        def exploding_copy(
            db_path: Path,
            staging: Path,
            *,
            journal_path: Path,
            **_kwargs: Any,
        ) -> dict[str, Any]:
            # Leave behind exactly what a real copy would before it fails.
            staging.write_text("partial", encoding="utf-8")
            journal_path.write_text("{", encoding="utf-8")
            raise CurrentFeedRebuildError("the copy exploded")

        monkeypatch.setattr(storage_reclaim, "rebuild_current_feed_database", exploding_copy)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir, rehearsal=True)
            assert started["started"] is True, started
            assert _wait_for(lambda: _status(data_dir) in TERMINAL, 60.0)
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]
            journal = storage_reclaim.read_reclaim_journal(data_dir)
            live_jobs = sqlite3.connect(db_path).execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            leftovers = sorted(
                path.name
                for path in data_dir.iterdir()
                if path.name.endswith(".tmp") or "rehearsal" in path.name
            )
            hold = storage_health.storage_reclaim_hold_status()
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    assert payload["status"] == "failed", payload
    assert journal is not None
    assert "the copy exploded" in journal["error"]
    assert live_jobs == 2
    assert leftovers == []
    # A failure must release the container hold in the same finally.
    assert hold["active"] is False


def test_a_cancel_during_the_retention_stop_settles_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def stop_then_cancel(_store: Any) -> bool:
        journal = storage_reclaim.read_reclaim_journal(data_dir) or {}
        storage_reclaim.cancel_storage_reclaim(data_dir, str(journal.get("runId") or ""))
        # The retention pass may still refuse to stop after the cancel.
        raise CurrentFeedRebuildError("the background retention pass did not stop")

    with workspace_tmpdir("reclaim-rehearsal-cancel-stop") as data_dir:
        _seed(data_dir, generations=2)
        monkeypatch.setattr(storage_rebuild, "stop_runtime_retention_for_copy", stop_then_cancel)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir, rehearsal=True)
            assert started["started"] is True, started
            assert _wait_for(lambda: _status(data_dir) in TERMINAL, 60.0)
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]
            journal = storage_reclaim.read_reclaim_journal(data_dir)
            hold = storage_health.storage_reclaim_hold_status()
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    assert payload["status"] == "cancelled", payload
    assert journal is not None
    assert journal["status"] == "cancelled"
    assert hold["active"] is False


def test_a_rehearsal_still_refuses_a_safety_blocker(monkeypatch: pytest.MonkeyPatch) -> None:
    with workspace_tmpdir("reclaim-rehearsal-blocked") as data_dir:
        _seed(data_dir)
        monkeypatch.setattr(
            storage_reclaim,
            "_compute_verdict",
            lambda *_args, **_kwargs: _bloat_verdict(blockers=["active-task"]),
        )

        result = storage_reclaim.start_storage_reclaim(data_dir, rehearsal=True)
        journal = storage_reclaim.read_reclaim_journal(data_dir)

    assert result["started"] is False
    assert result["reason"] == "active-task"
    assert journal is None
