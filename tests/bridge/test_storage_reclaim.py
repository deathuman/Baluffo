"""GET /ops/storage-reclaim: verdict, status, journal, and the 60 s cache.

The route is what makes the verdict reachable, so these cases pin the properties
the panel depends on: a verdict survives a restart's journal, the cache does not
outlive its window or override an explicit refresh, and an unreadable store
refuses through blockers instead of failing the request.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from src.bridge import storage_health, storage_reclaim
from src.bridge.storage_health import (
    clear_storage_reclaim_hold,
    heartbeat_storage_reclaim_hold,
    set_storage_reclaim_hold,
    storage_reclaim_hold_status,
)
from src.bridge.storage_rebuild import close_storage_rebuild_jobs
from src.storage import BaluffoStore, BaluffoStoreError, JobRuntimeStore
from src.storage.baluffo_store import DEFAULT_DB_NAME
from src.storage.current_feed_rebuild import (
    CONTENT_PARITY_TABLES,
    rebuild_backup_path,
)
from src.storage.runtime_retention import preview_runtime_retention
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


def _write_journal(data_dir: Path, payload: dict[str, Any]) -> None:
    path = storage_reclaim.reclaim_journal_path(data_dir)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _finished_hours_ago(hours: float) -> str:
    return (datetime.now(UTC) - timedelta(hours=hours)).isoformat()


def test_verdict_is_reachable_for_a_real_store() -> None:
    with workspace_tmpdir("reclaim-route-verdict") as data_dir:
        _seed(data_dir)

        payload = storage_reclaim.get_storage_reclaim_payload(data_dir)

    reclaim = payload["storageReclaim"]
    assert payload["ok"] is True
    assert reclaim["readOnly"] is True
    assert reclaim["scope"] == "modal"
    assert reclaim["status"] == "idle"
    assert reclaim["journal"] is None
    assert reclaim["journalPresent"] is False
    assert reclaim["verdictCached"] is False
    assert reclaim["verdict"]["verdict"] == "ok"
    assert reclaim["verdict"]["currentGenerationEstimatedBytes"] > 0
    assert reclaim["verdict"]["historicalGenerationCount"] == 1


def _counting_preview(monkeypatch: pytest.MonkeyPatch, calls: list[int]) -> None:
    def counting_preview(store: Any) -> dict[str, Any]:
        calls.append(1)
        return preview_runtime_retention(store)

    monkeypatch.setattr(storage_reclaim, "preview_runtime_retention", counting_preview)


def test_verdict_is_cached_and_refresh_bypasses_the_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("reclaim-route-cache") as data_dir:
        _seed(data_dir)
        calls: list[int] = []
        _counting_preview(monkeypatch, calls)

        first = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]
        second = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

        assert len(calls) == 1
        assert first["verdictCached"] is False
        assert second["verdictCached"] is True
        assert second["verdictAgeMs"] >= 0
        assert second["verdict"] == first["verdict"]

        refreshed = storage_reclaim.get_storage_reclaim_payload(data_dir, refresh=True)[
            "storageReclaim"
        ]
        assert len(calls) == 2
        assert refreshed["verdictCached"] is False
        assert refreshed["verdictAgeMs"] == 0


def test_verdict_cache_expires_at_its_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    with workspace_tmpdir("reclaim-route-ttl") as data_dir:
        _seed(data_dir)
        calls: list[int] = []
        _counting_preview(monkeypatch, calls)
        monkeypatch.setattr(storage_reclaim, "RECLAIM_VERDICT_TTL_S", 0.0)

        storage_reclaim.get_storage_reclaim_payload(data_dir)
        storage_reclaim.get_storage_reclaim_payload(data_dir)

        assert len(calls) == 2


def test_journal_status_drives_the_live_status() -> None:
    with workspace_tmpdir("reclaim-route-journal") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {
                "status": "complete",
                "phase": "complete",
                "runId": "reclaim-1",
                "mode": "reclaim",
                "finishedAt": _finished_hours_ago(1),
            },
        )

        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    assert reclaim["status"] == "complete"
    assert reclaim["phase"] == "complete"
    assert reclaim["runId"] == "reclaim-1"
    assert reclaim["journalPresent"] is True
    assert reclaim["journal"]["mode"] == "reclaim"
    # The last non-rehearsal finish starts the 168 h anti-thrash window.
    assert 166.0 <= reclaim["verdict"]["cooldownRemainingHours"] <= 168.0
    assert "cooldown" in reclaim["verdict"]["blockers"]


def test_journaled_running_is_reported_interrupted_after_a_restart() -> None:
    with workspace_tmpdir("reclaim-route-interrupted") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {"status": "running", "phase": "copying", "runId": "reclaim-2", "mode": "reclaim"},
        )

        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    # No run survives the restart, so a journaled `running` is not activity.
    assert reclaim["status"] == "interrupted"
    assert reclaim["phase"] == "copying"


def test_a_journaled_failure_shows_its_sentence_after_a_restart() -> None:
    with workspace_tmpdir("reclaim-route-error-text") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {
                "status": "failed",
                "mode": "rehearsal",
                "error": "CurrentFeedRebuildError: the copy exploded",
                "operatorError": "the copy failed validation, so nothing was installed.",
                "finishedAt": _finished_hours_ago(1),
            },
        )

        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    # The panel gets the sentence; the journal keeps the exception for diagnosis.
    assert reclaim["error"] == "the copy failed validation, so nothing was installed."
    assert reclaim["journal"]["error"] == "CurrentFeedRebuildError: the copy exploded"


def test_a_journaled_failure_without_a_sentence_still_hides_the_class_name() -> None:
    # A journal written before the split carries only the raw exception text.
    with workspace_tmpdir("reclaim-route-error-raw") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {
                "status": "failed",
                "mode": "rehearsal",
                "error": "CurrentFeedRebuildError: the copy exploded",
                "finishedAt": _finished_hours_ago(1),
            },
        )

        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    assert reclaim["error"] == "the run failed before it finished. The journal has the detail."


def test_a_rehearsal_does_not_start_the_cooldown() -> None:
    with workspace_tmpdir("reclaim-route-rehearsal") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {
                "status": "complete",
                "mode": "rehearsal",
                "finishedAt": _finished_hours_ago(0.5),
            },
        )

        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    assert reclaim["verdict"]["cooldownRemainingHours"] == 0.0
    assert "cooldown" not in reclaim["verdict"]["blockers"]


@pytest.mark.parametrize("status", sorted(storage_reclaim.RECLAIM_RETRY_STATUSES))
def test_an_unfinished_run_earns_only_a_short_retry(status: str) -> None:
    """A run that did not complete says nothing about bloat, so it cannot earn a week."""

    with workspace_tmpdir(f"reclaim-cooldown-{status}") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {
                "status": status,
                "mode": "reclaim",
                "runId": "reclaim-retry",
                "finishedAt": _finished_hours_ago(0.25),
            },
        )

        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    remaining = reclaim["verdict"]["cooldownRemainingHours"]
    assert 0.0 < remaining <= storage_reclaim.RECLAIM_RETRY_BACKOFF_HOURS
    assert remaining < storage_reclaim.DEFAULT_RECLAIM_MIN_INTERVAL_HOURS
    assert "cooldown" in reclaim["verdict"]["blockers"]


def test_the_short_retry_expires_so_auto_can_try_again() -> None:
    with workspace_tmpdir("reclaim-cooldown-retry-expired") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {
                "status": "failed",
                "mode": "reclaim",
                "runId": "reclaim-retry",
                "finishedAt": _finished_hours_ago(2),
            },
        )

        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    # Two hours after a failure, only the 168 h window could still block a start.
    assert reclaim["verdict"]["cooldownRemainingHours"] == 0.0
    assert "cooldown" not in reclaim["verdict"]["blockers"]


def test_only_a_completed_reclaim_starts_the_full_window() -> None:
    with workspace_tmpdir("reclaim-cooldown-complete") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {
                "status": "complete",
                "mode": "reclaim",
                "runId": "reclaim-done",
                "finishedAt": _finished_hours_ago(2),
            },
        )

        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    remaining = reclaim["verdict"]["cooldownRemainingHours"]
    assert 164.0 <= remaining <= storage_reclaim.DEFAULT_RECLAIM_MIN_INTERVAL_HOURS
    assert "cooldown" in reclaim["verdict"]["blockers"]


def test_a_failed_rehearsal_starts_no_clock_at_all() -> None:
    with workspace_tmpdir("reclaim-cooldown-rehearsal-failed") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {
                "status": "failed",
                "mode": "rehearsal",
                "runId": "rehearsal-failed",
                "finishedAt": _finished_hours_ago(0.25),
            },
        )

        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    assert reclaim["verdict"]["cooldownRemainingHours"] == 0.0
    assert "cooldown" not in reclaim["verdict"]["blockers"]


def test_an_unreadable_store_refuses_through_blockers(monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_store(*_args: Any, **_kwargs: Any) -> Any:
        raise BaluffoStoreError("store unavailable")

    with workspace_tmpdir("reclaim-route-unavailable") as data_dir:
        monkeypatch.setattr(storage_reclaim, "get_storage_store", failing_store)
        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    assert reclaim["verdict"]["verdict"] == "ok"
    assert reclaim["verdict"]["ratio"] == 0.0
    assert "store-unavailable" in reclaim["verdict"]["blockers"]
    assert "sample-unavailable" in reclaim["verdict"]["blockers"]


def test_scope_is_echoed_and_an_unknown_scope_falls_back_to_modal() -> None:
    with workspace_tmpdir("reclaim-route-scope") as data_dir:
        _seed(data_dir, generations=1)

        inline = storage_reclaim.get_storage_reclaim_payload(data_dir, scope="inline")[
            "storageReclaim"
        ]
        unknown = storage_reclaim.get_storage_reclaim_payload(data_dir, scope="browser")[
            "storageReclaim"
        ]

    assert inline["scope"] == "inline"
    assert unknown["scope"] == "modal"


def test_journal_path_is_beside_the_runtime_store(tmp_path: Path) -> None:
    assert storage_reclaim.reclaim_journal_path(tmp_path).name == (
        f"{DEFAULT_DB_NAME}.reclaim.json"
    )


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


def test_a_reclaim_run_rebuilds_verifies_and_deletes_the_retained_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("reclaim-run-complete") as data_dir:
        _seed(data_dir, generations=2)
        monkeypatch.setattr(
            storage_reclaim, "_compute_verdict", lambda *_args, **_kwargs: _bloat_verdict()
        )
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir)

            assert started["started"] is True
            assert _wait_for(lambda: _status(data_dir) in TERMINAL, 30.0)
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]
            journal = storage_reclaim.read_reclaim_journal(data_dir)
            backup = rebuild_backup_path(data_dir / DEFAULT_DB_NAME)
            assert not backup.exists()
            with BaluffoStore(data_dir) as store:
                preview = preview_runtime_retention(store)
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    assert payload["status"] == "complete"
    assert payload["phase"] == "complete"
    assert payload["mode"] == "reclaim"
    assert journal is not None
    assert journal["status"] == "complete"
    assert journal["rebuildRunId"].startswith("current_feed_rebuild_")
    assert journal["expectedBackupBytes"] > 0
    assert journal["finishedAt"]
    # The copy's immutable-table digests are the evidence the delete leaned on.
    assert set(journal["copyDigests"]) == set(CONTENT_PARITY_TABLES)
    assert all(len(digest) == 64 for digest in journal["copyDigests"].values())
    # The copy kept only the published generation, so retention has nothing left.
    assert preview["historicalGenerationCount"] == 0


def test_a_content_mismatch_keeps_the_retained_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("reclaim-run-content-parity") as data_dir:
        _seed(data_dir, generations=2)
        monkeypatch.setattr(
            storage_reclaim, "_compute_verdict", lambda *_args, **_kwargs: _bloat_verdict()
        )
        monkeypatch.setattr(
            storage_reclaim,
            "validate_installed_content",
            lambda *_args, **_kwargs: {"sources": "deadbeef"},
        )
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir)
            assert started["started"] is True
            assert _wait_for(lambda: _status(data_dir) in TERMINAL, 30.0)
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]
            journal = storage_reclaim.read_reclaim_journal(data_dir)
            backup_exists = rebuild_backup_path(data_dir / DEFAULT_DB_NAME).is_file()
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    assert payload["status"] == "failed"
    assert journal is not None
    assert "non-feed-mismatch: sources" in journal["error"]
    assert journal["copyDigests"]
    # The delete never started, so the reviewed byte count was never set.
    assert journal["expectedBackupBytes"] == 0
    assert backup_exists is True


def test_a_safety_blocker_refuses_the_start_and_writes_no_journal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("reclaim-run-blocked") as data_dir:
        _seed(data_dir, generations=2)
        monkeypatch.setattr(
            storage_reclaim,
            "_compute_verdict",
            lambda *_args, **_kwargs: _bloat_verdict(blockers=["active-task"]),
        )

        result = storage_reclaim.start_storage_reclaim(data_dir)
        journal = storage_reclaim.read_reclaim_journal(data_dir)

    assert result["started"] is False
    assert result["reason"] == "active-task"
    assert result["blockers"] == ["active-task"]
    assert journal is None


def test_cooldown_and_auto_disabled_gate_only_the_automatic_trigger() -> None:
    guarded = _bloat_verdict(blockers=["cooldown", "auto-disabled"])
    assert storage_reclaim._preflight_refusal(guarded, auto=True) == ("cooldown", False)
    assert storage_reclaim._preflight_refusal(guarded, auto=False) == ("", True)

    safety = _bloat_verdict(blockers=["cooldown", "free-space"])
    assert storage_reclaim._preflight_refusal(safety, auto=False) == ("free-space", True)
    assert storage_reclaim._preflight_refusal(_bloat_verdict(), auto=False) == ("", False)

    quiet = {**_bloat_verdict(), "verdict": "ok"}
    assert storage_reclaim._preflight_refusal(quiet, auto=False) == ("not-bloated", False)


def test_an_explicit_start_overrides_the_cooldown_and_cancel_stops_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = threading.Event()
    calls: list[int] = []

    def verdict(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        calls.append(1)
        if len(calls) == 2:
            # The evaluating phase waits here so the cancel lands before the copy.
            release.wait(5.0)
        return _bloat_verdict(blockers=["cooldown"] if len(calls) == 1 else [])

    with workspace_tmpdir("reclaim-run-cancel") as data_dir:
        _seed(data_dir, generations=2)
        monkeypatch.setattr(storage_reclaim, "_compute_verdict", verdict)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir)
            journal = storage_reclaim.read_reclaim_journal(data_dir)
            assert started["started"] is True
            assert journal is not None
            assert journal["triggerOverride"] is True
            assert journal["auto"] is False

            cancel = storage_reclaim.cancel_storage_reclaim(data_dir, started["runId"])
            assert cancel["cancelling"] is True
            assert cancel["stale"] is False

            release.set()
            assert _wait_for(lambda: _status(data_dir) == "cancelled", 10.0)
            settled = storage_reclaim.read_reclaim_journal(data_dir)
        finally:
            release.set()
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    assert settled is not None
    assert settled["status"] == "cancelled"
    assert settled["finishedAt"]


def test_get_reports_the_live_run_rather_than_the_journal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = threading.Event()
    calls: list[int] = []

    def verdict(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        calls.append(1)
        if len(calls) == 2:
            release.wait(5.0)
        return _bloat_verdict()

    with workspace_tmpdir("reclaim-run-live") as data_dir:
        _seed(data_dir, generations=2)
        monkeypatch.setattr(storage_reclaim, "_compute_verdict", verdict)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir)
            assert started["started"] is True
            assert _wait_for(lambda: len(calls) >= 2, 5.0)

            live = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

            assert live["status"] == "running"
            assert live["phase"] == "evaluating"
            assert live["runId"] == started["runId"]
            assert live["journal"]["status"] == "running"

            storage_reclaim.cancel_storage_reclaim(data_dir, started["runId"])
            release.set()
            assert _wait_for(lambda: _status(data_dir) == "cancelled", 10.0)
        finally:
            release.set()
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()


def test_cancel_reports_a_stale_or_missing_run_id(tmp_path: Path) -> None:
    stale = storage_reclaim.cancel_storage_reclaim(tmp_path, "storage_reclaim_old")
    missing = storage_reclaim.cancel_storage_reclaim(tmp_path, "")

    assert stale["stale"] is True
    assert stale["reason"] == "no-running-run"
    assert missing["stale"] is False
    assert missing["cancelling"] is False


def test_cancel_is_refused_once_the_install_phase_begins(tmp_path: Path) -> None:
    # The install, verification and delete phases are not interruptible, and a
    # multi-GB copy is not something a test can hold at `installing` honestly.
    job = storage_reclaim.StorageReclaimJob(tmp_path.resolve())
    run_id = "storage_reclaim_installing"
    with job._lock:
        job._state.update({"status": "running", "phase": "installing", "runId": run_id})

    result = job.cancel(run_id)

    assert result["ok"] is False
    assert result["reason"] == "phase-not-cancellable"
    assert result["stale"] is False
    assert job.status_fields()["status"] == "running"


def test_the_hold_expires_by_stall_and_hard_maximum(monkeypatch: pytest.MonkeyPatch) -> None:
    try:
        assert set_storage_reclaim_hold("storage_reclaim_1")["active"] is True
        assert storage_reclaim_hold_status()["runId"] == "storage_reclaim_1"
        assert heartbeat_storage_reclaim_hold("storage_reclaim_1")["active"] is True
        assert heartbeat_storage_reclaim_hold("someone-else") == {}

        monkeypatch.setattr(storage_health, "RECLAIM_HOLD_HEARTBEAT_TIMEOUT_S", 0.0)
        stalled = storage_reclaim_hold_status()
        assert stalled["active"] is False
        assert stalled["releasedBy"] == "watchdog"
        assert stalled["releasedRunId"] == "storage_reclaim_1"

        # A heartbeat never renews the hard maximum.
        set_storage_reclaim_hold("storage_reclaim_2")
        heartbeat_storage_reclaim_hold("storage_reclaim_2")
        monkeypatch.setattr(storage_health, "RECLAIM_HOLD_HEARTBEAT_TIMEOUT_S", 60.0)
        monkeypatch.setattr(storage_health, "RECLAIM_HOLD_MAX_S", 0.0)
        expired = storage_reclaim_hold_status()
        assert expired["active"] is False
        assert expired["releasedBy"] == "ttl"
    finally:
        clear_storage_reclaim_hold("terminal")


def test_a_reclaim_run_holds_and_releases_task_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = threading.Event()
    calls: list[int] = []

    def verdict(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        calls.append(1)
        if len(calls) == 2:
            release.wait(5.0)
        return _bloat_verdict()

    with workspace_tmpdir("reclaim-run-hold") as data_dir:
        _seed(data_dir, generations=2)
        monkeypatch.setattr(storage_reclaim, "_compute_verdict", verdict)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir)
            assert started["started"] is True
            assert _wait_for(lambda: len(calls) >= 2, 5.0)

            held = storage_reclaim_hold_status()
            assert held["active"] is True
            assert held["runId"] == started["runId"]

            storage_reclaim.cancel_storage_reclaim(data_dir, started["runId"])
            release.set()
            assert _wait_for(lambda: _status(data_dir) == "cancelled", 10.0)
            # The hold is released in the same finally that clears the thread.
            assert _wait_for(lambda: storage_reclaim_hold_status()["active"] is False, 5.0)

            released = storage_reclaim_hold_status()
            assert released["releasedBy"] == "terminal"
        finally:
            release.set()
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()
            clear_storage_reclaim_hold("terminal")


def test_a_desktop_run_takes_no_hold(monkeypatch: pytest.MonkeyPatch) -> None:
    release = threading.Event()
    calls: list[int] = []

    def verdict(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        calls.append(1)
        if len(calls) == 2:
            release.wait(5.0)
        return _bloat_verdict()

    with workspace_tmpdir("reclaim-run-inline") as data_dir:
        _seed(data_dir, generations=2)
        monkeypatch.setattr(storage_reclaim, "_compute_verdict", verdict)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir, hold=False)
            assert started["started"] is True
            assert _wait_for(lambda: len(calls) >= 2, 5.0)
            assert storage_reclaim_hold_status()["active"] is False

            storage_reclaim.cancel_storage_reclaim(data_dir, started["runId"])
            release.set()
            assert _wait_for(lambda: _status(data_dir) == "cancelled", 10.0)
        finally:
            release.set()
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()


def test_the_watchdog_settles_a_run_whose_thread_is_gone(tmp_path: Path) -> None:
    job = storage_reclaim.StorageReclaimJob(tmp_path.resolve())
    run_id = "storage_reclaim_dead"
    with job._lock:
        job._state.update(
            {"status": "running", "phase": "copying", "runId": run_id, "mode": "reclaim"}
        )
        job._hold_taken = True
    set_storage_reclaim_hold(run_id)
    try:
        job.watchdog()

        assert job.status_fields()["status"] == "interrupted"
        assert job.status_fields()["phase"] == "copying"
        hold = storage_reclaim_hold_status()
        assert hold["active"] is False
        assert hold["releasedBy"] == "watchdog"
    finally:
        clear_storage_reclaim_hold("terminal")


def test_a_release_is_scoped_to_the_run_that_took_the_hold() -> None:
    try:
        set_storage_reclaim_hold("storage_reclaim_new")

        unchanged = clear_storage_reclaim_hold("terminal", run_id="storage_reclaim_old")

        assert unchanged["active"] is True
        assert unchanged["runId"] == "storage_reclaim_new"
        assert clear_storage_reclaim_hold("terminal", run_id="storage_reclaim_new")["active"] is (
            False
        )
    finally:
        clear_storage_reclaim_hold("terminal")


def test_shutdown_releases_a_hold_it_took(tmp_path: Path) -> None:
    job = storage_reclaim.StorageReclaimJob(tmp_path.resolve())
    set_storage_reclaim_hold("storage_reclaim_shutdown")
    job._hold_taken = True

    job.shutdown()

    assert storage_reclaim_hold_status()["releasedBy"] == "shutdown"


def test_a_rebuild_refusal_settles_cancelled_not_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("reclaim-run-refused") as data_dir:
        _seed(data_dir, generations=2)
        monkeypatch.setattr(
            storage_reclaim, "_compute_verdict", lambda *_args, **_kwargs: _bloat_verdict()
        )
        monkeypatch.setattr(
            storage_reclaim.rebuild_mod,
            "start_storage_rebuild",
            lambda _data_dir: {
                "ok": True,
                "started": False,
                "reason": "backup-exists",
                "storageRebuild": {"status": "idle"},
            },
        )
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir)
            assert started["started"] is True
            assert _wait_for(lambda: _status(data_dir) in TERMINAL, 10.0)
            journal = storage_reclaim.read_reclaim_journal(data_dir)
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    assert journal is not None
    assert journal["status"] == "cancelled"
    assert "backup-exists" in journal["error"]


def test_active_task_is_read_from_the_bridge_hold(monkeypatch: pytest.MonkeyPatch) -> None:
    with workspace_tmpdir("reclaim-hold-blocker") as data_dir:
        _seed(data_dir, generations=1)
        monkeypatch.setattr(storage_reclaim, "storage_compaction_hold_active", lambda: False)
        idle = storage_reclaim.get_storage_reclaim_payload(data_dir, refresh=True)[
            "storageReclaim"
        ]["verdict"]
        monkeypatch.setattr(storage_reclaim, "storage_compaction_hold_active", lambda: True)
        held = storage_reclaim.get_storage_reclaim_payload(data_dir, refresh=True)[
            "storageReclaim"
        ]["verdict"]

    assert "active-task" not in idle["blockers"]
    assert "active-task" in held["blockers"]
    assert held["verdict"] == idle["verdict"]
