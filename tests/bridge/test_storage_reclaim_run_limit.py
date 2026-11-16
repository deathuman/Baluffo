"""The run limit ends the copy it was following, not just the reclaim.

Settling a reclaim releases the hold that keeps task starts away from it, so a
deadline that settled while the rebuild was still copying would let a fetch land
mid-copy: the install becomes `superseded` and the hours the limit cut short are
thrown away. These cases pin the order — cancel the copy, wait for it to leave,
then settle — and the ceiling that decides when the limit is reached.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import pytest

from src.bridge import storage_health, storage_rebuild, storage_reclaim
from src.bridge.storage_health import storage_reclaim_hold_status
from src.bridge.storage_rebuild import close_storage_rebuild_jobs
from src.storage import BaluffoStore, JobRuntimeStore
from tests.helpers.temp_paths import workspace_tmpdir

MIB = 1024 * 1024
GIB = 1024 * MIB
TERMINAL = storage_reclaim.RECLAIM_TERMINAL_STATUSES


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


def _bloat_verdict() -> dict[str, Any]:
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
        "blockers": [],
        "cooldownRemainingHours": 0.0,
    }


def _wait_for(predicate: Callable[[], bool], timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def _status(data_dir) -> str:
    return str(storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]["status"])


def _rebuild_status(data_dir) -> str:
    return str(
        storage_rebuild.get_storage_rebuild_payload(data_dir)["storageRebuild"]["status"] or ""
    )


def test_the_ceiling_outlasts_a_multi_hour_copy() -> None:
    # Eight hours: a 200 GB copy on a slow disk needs more than the old two, and
    # the heartbeat, not this ceiling, is what notices a run that stopped working.
    assert storage_health.RECLAIM_HOLD_MAX_S == 8 * 60 * 60
    assert storage_reclaim.RECLAIM_MAX_RUN_S == storage_health.RECLAIM_HOLD_MAX_S


def test_the_run_limit_stops_the_copy_before_it_settles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("reclaim-run-limit") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-11-15T10:00:00Z")
            runtime.replace_feed(run_id="fetch_0", rows=[_row(0)])
        monkeypatch.setattr(
            storage_reclaim, "_compute_verdict", lambda *_args, **_kwargs: _bloat_verdict()
        )
        # A zero limit trips the deadline on the first poll, while the rebuild it
        # just started is still copying.
        monkeypatch.setattr(storage_reclaim, "RECLAIM_MAX_RUN_S", 0.0)
        monkeypatch.setattr(storage_reclaim, "RECLAIM_STOP_WAIT_S", 10.0)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir)

            assert started["started"] is True
            assert _wait_for(lambda: _status(data_dir) in TERMINAL, 30.0)
            assert _wait_for(
                lambda: _rebuild_status(data_dir) not in {"running", "cancelling"}, 10.0
            )
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]
            journal = storage_reclaim.read_reclaim_journal(data_dir)
            rebuild_status = _rebuild_status(data_dir)
            hold_active = storage_reclaim_hold_status()["active"]
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    assert payload["status"] == "failed"
    assert journal is not None
    assert "did not finish within the run limit" in journal["error"]
    # The copy this run followed ended with it: no rebuild left copying behind a
    # settled run whose hold is already gone.
    assert rebuild_status not in {"running", "cancelling"}
    assert hold_active is False


def test_a_copy_that_finishes_during_the_stop_is_adopted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The run limit must not report a failure for a database that is installed.

    The cancel is a request: a rebuild already installing can refuse it and
    complete while the run limit is stopping it. Its completion is the truth —
    the copy is in place — so the reclaim adopts the snapshot and goes on to
    verify and delete instead of settling "did not finish within the run limit".
    """
    with workspace_tmpdir("reclaim-run-limit-race") as data_dir:
        job = storage_reclaim._job_for(data_dir)
        with job._lock:
            job._state["status"] = "running"
            job._state["phase"] = "copying"
            job._state["runId"] = "storage_reclaim_race"
            job._state["rebuildRunId"] = "current_feed_rebuild_race"
        # The copy is still working when the limit hits, and installs before the
        # stop lands — the race this pins.
        statuses = iter(["running", "running"])
        completed = {
            "status": "complete",
            "currentGeneration": "gen-race",
            "stagingBytes": 4096,
        }
        stop_requests: list[str] = []

        def fake_payload(_data_dir):
            return {"storageRebuild": {**completed, "status": next(statuses, "complete")}}

        monkeypatch.setattr(
            storage_reclaim.rebuild_mod, "get_storage_rebuild_payload", fake_payload
        )

        def fake_stop(_data_dir, run_id):
            stop_requests.append(run_id)
            return {"ok": True}

        monkeypatch.setattr(storage_reclaim.rebuild_mod, "cancel_storage_rebuild", fake_stop)
        monkeypatch.setattr(storage_reclaim, "RECLAIM_MAX_RUN_S", 0.0)
        try:
            adopted = job._await_rebuild()
            status_after = job.status_fields()["status"]
            journal = storage_reclaim.read_reclaim_journal(data_dir)
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    # It did ask the copy to stop, and then adopted what the copy reported.
    assert stop_requests == ["current_feed_rebuild_race"]
    assert adopted is not None
    assert adopted["currentGeneration"] == "gen-race"
    # No run-limit failure was settled over an installed database.
    assert status_after == "running"
    assert journal is None or "run limit" not in str(journal.get("error") or "")
