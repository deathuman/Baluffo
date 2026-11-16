"""§3 full-disk order: measure both gates, remove what no live copy owns, re-ask.

The order exists because freeing space needs space. These cases pin the two levers
the plan allows — a file that is not the live database, and the measured gate —
and the things the order must never touch: the live database, its WAL, the
rebuild journal, and the retained copy (step 5 is a click, never automatic).
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from src.bridge import storage_reclaim, storage_reclaim_space
from src.bridge.storage_rebuild import (
    close_storage_rebuild_jobs,
    get_storage_rebuild_payload,
    start_storage_rebuild,
)
from src.storage import BaluffoStore, JobRuntimeStore
from src.storage.baluffo_store import DEFAULT_DB_NAME
from src.storage.current_feed_rebuild import rebuild_backup_path
from tests.helpers.temp_paths import workspace_tmpdir

MIB = 1024 * 1024
GIB = 1024 * MIB


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


def _seed(data_dir: Path) -> None:
    with BaluffoStore(data_dir) as store:
        runtime = JobRuntimeStore(store, now_iso=lambda: "2026-11-15T10:00:00Z")
        for index in range(2):
            runtime.replace_feed(run_id=f"fetch_{index}", rows=[_row(index)])


def _wait_for(predicate: Callable[[], bool], timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def test_relief_removes_only_what_no_live_copy_owns() -> None:
    with workspace_tmpdir("reclaim-space-artifacts") as data_dir:
        db = data_dir / DEFAULT_DB_NAME
        db.write_bytes(b"live-database")
        backup = rebuild_backup_path(db)
        backup.write_bytes(b"retained-copy")
        stale = data_dir / f"{db.name}.dead.rebuild.tmp"
        stale.write_bytes(b"x" * 4096)
        companions = [
            stale.with_name(f"{stale.name}-wal"),
            stale.with_name(f"{stale.name}-shm"),
        ]
        for companion in companions:
            companion.write_bytes(b"y" * 512)
        scratch = [
            data_dir / f"{db.name}.rebuild.json.tmp",
            data_dir / f"{db.name}.reclaim-rehearsal.json",
            data_dir / f"{db.name}.reclaim-rehearsal.json.tmp",
        ]
        for path in scratch:
            path.write_bytes(b"z")
        live = data_dir / f"{db.name}.live.rebuild.tmp"
        live.write_bytes(b"owned")
        claim_live_artifacts = storage_reclaim_space.claim_live_artifacts
        claim_live_artifacts([live])
        try:
            candidates = storage_reclaim_space.orphaned_artifacts(data_dir, db)
            space = storage_reclaim_space.relieve_free_space(data_dir, db)
        finally:
            storage_reclaim_space.release_live_artifacts([live])

        expected = {
            stale.name,
            *(path.name for path in companions),
            *(path.name for path in scratch),
        }
        assert {path.name for path in candidates} == expected
        assert space["artifactsRemoved"] == sorted(expected)
        assert space["relievedBytes"] == 4096 + 512 * 2 + 3
        assert db.exists() and db.read_bytes() == b"live-database"
        assert backup.exists(), "the retained copy is step 5, never relief"
        assert live.exists(), "a claimed staging belongs to a live copy"


def _write_reclaim_journal(data_dir: Path, **fields: Any) -> None:
    path = storage_reclaim.reclaim_journal_path(data_dir)
    path.write_text(json.dumps(fields), encoding="utf-8")


def test_the_measured_gate_is_null_until_a_rehearsal_reports_a_copy() -> None:
    measured = storage_reclaim_space.measured_required_bytes

    assert measured(staging_bytes=None) is None
    assert measured(staging_bytes=0) is None
    assert measured(staging_bytes="not a number") is None
    # 1.25 x the copy the host actually made, plus the fixed margin.
    assert measured(staging_bytes=GIB) == int(GIB * 1.25) + 64 * MIB


def test_space_for_copy_relieves_and_measures_both_gates(monkeypatch) -> None:
    class _Store:
        def file_sizes(self) -> dict[str, int]:
            return {"databaseBytes": 10 * GIB, "freelistBytes": 0}

    with workspace_tmpdir("reclaim-space-copy") as data_dir:
        db = data_dir / DEFAULT_DB_NAME
        db.write_bytes(b"live")
        orphan = data_dir / f"{db.name}.dead.rebuild.tmp"
        orphan.write_bytes(b"x" * 4096)
        _write_reclaim_journal(data_dir, measuredStagingBytes=100 * MIB)
        readings = iter([150 * MIB, 200 * MIB])
        monkeypatch.setattr(storage_reclaim_space, "free_space_bytes", lambda _path: next(readings))
        space = storage_reclaim_space.space_for_copy(data_dir, db, _Store())

    assert space["artifactsRemoved"] == [orphan.name]
    assert space["relievedBytes"] == 4096
    assert space["freeBytes"] == 200 * MIB
    # 1% of 10 GiB is below the 256 MiB floor, so the heuristic asks 320 MiB; a
    # measured 100 MiB copy asks 189 MiB and is what lets this start.
    assert space["requiredBytes"] == 320 * MIB
    assert space["measuredRequiredBytes"] == int(100 * MIB * 1.25) + 64 * MIB
    assert space["enough"] is True


def test_a_gate_built_from_a_payload_sample_is_not_offered() -> None:
    """A copy 2.4-4.5x the payload estimate is why the anchor is a real copy."""

    class _Store:
        def file_sizes(self) -> dict[str, int]:
            return {"databaseBytes": 10 * GIB, "freelistBytes": 0}

    with workspace_tmpdir("reclaim-space-unmeasured") as data_dir:
        db = data_dir / DEFAULT_DB_NAME
        db.write_bytes(b"live")
        space = storage_reclaim_space.space_for_copy(data_dir, db, _Store())
        anchor = storage_reclaim_space.measured_anchor(data_dir, db)

    assert space["measuredRequiredBytes"] is None
    assert anchor.staging_bytes is None
    assert anchor.measured_at == ""
    assert anchor.gate_adequate is None


def test_the_rebuild_starts_on_the_measured_gate_when_the_heuristic_is_short(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("reclaim-space-rebuild") as data_dir:
        try:
            _seed(data_dir)
            # Below the 320 MiB heuristic for a small database, above the measured
            # gate: a host that rehearsed a 1 MiB copy is asked 1.25 MiB + 64 MiB.
            _write_reclaim_journal(data_dir, measuredStagingBytes=1 * MIB)
            monkeypatch.setattr(storage_reclaim_space, "free_space_bytes", lambda _path: 300 * MIB)
            started = start_storage_rebuild(data_dir)
            assert started["started"] is True, started
            assert _wait_for(
                lambda: (
                    str(get_storage_rebuild_payload(data_dir)["storageRebuild"]["status"])
                    not in {"running", "cancelling"}
                ),
                60.0,
            )
            rebuild = get_storage_rebuild_payload(data_dir)["storageRebuild"]
        finally:
            close_storage_rebuild_jobs()

    assert rebuild["status"] == "complete", rebuild
    assert rebuild["measuredRequiredBytes"] is not None
    assert rebuild["measuredRequiredBytes"] < rebuild["requiredFreeBytes"]


def test_the_verdict_drops_free_space_when_the_measured_gate_closes_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("reclaim-space-verdict") as data_dir:
        _seed(data_dir)
        # 1 MiB measured copy -> a 65.25 MiB gate, below the 300 MiB reading and
        # far below the heuristic, so only the measured gate can clear it.
        _write_reclaim_journal(data_dir, measuredStagingBytes=1 * MIB)
        monkeypatch.setattr(storage_reclaim, "free_space_bytes", lambda _path: 300 * MIB)
        verdict = storage_reclaim._compute_verdict(data_dir, None)

    assert verdict["requiredBytes"] > 300 * MIB
    assert verdict["measuredRequiredBytes"] == int(1 * MIB * 1.25) + 64 * MIB
    assert "free-space" not in verdict["blockers"]
    assert verdict["measuredFreeSpaceOk"] is True


def test_a_rehearsal_removes_an_unowned_staging_before_it_copies() -> None:
    with workspace_tmpdir("reclaim-space-rehearsal") as data_dir:
        _seed(data_dir)
        stale = data_dir / f"{DEFAULT_DB_NAME}.dead.rebuild.tmp"
        stale.write_bytes(b"x" * 4096)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir, rehearsal=True)
            assert started["started"] is True, started
            assert _wait_for(
                lambda: (
                    str(
                        storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"][
                            "status"
                        ]
                    )
                    in {"complete", "failed", "cancelled", "superseded", "interrupted"}
                ),
                60.0,
            )
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]
            still_there = stale.exists()
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    assert payload["status"] == "complete", payload
    assert still_there is False
    assert payload["space"]["artifactsRemoved"] == [stale.name]
    assert payload["space"]["relievedBytes"] == 4096


def test_a_rehearsal_reports_whether_the_measured_gate_covers_its_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("reclaim-space-gate-coverage") as data_dir:
        _seed(data_dir)
        # The host measured a 1 MiB copy last time, so this run starts on a gate
        # and can be checked against the copy it makes now.
        _write_reclaim_journal(data_dir, measuredStagingBytes=1 * MIB)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir, rehearsal=True)
            assert started["started"] is True, started
            assert _wait_for(
                lambda: (
                    str(
                        storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"][
                            "status"
                        ]
                    )
                    in {"complete", "failed", "cancelled", "superseded", "interrupted"}
                ),
                60.0,
            )
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir, refresh=True)[
                "storageReclaim"
            ]
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    result = payload["journal"]["result"]
    assert payload["status"] == "complete", payload
    # The gate the run was allowed on is the one it reports against, and the copy
    # it actually made fits: 1.25 x the last measured copy plus 64 MiB.
    assert result["measuredGateBytes"] == payload["space"]["measuredRequiredBytes"]
    assert result["measuredGateBytes"] == int(1 * MIB * 1.25) + 64 * MIB
    assert result["stagingBytes"] <= result["measuredGateBytes"]
    assert result["gateAdequate"] is True
    # And this run replaces the anchor with the copy it just measured, which the
    # verdict carries in both forms the panel reads.
    assert payload["journal"]["measuredStagingBytes"] == result["stagingBytes"]
    assert payload["verdict"]["measuredStagingBytes"] == result["stagingBytes"]
    assert payload["verdict"]["lastRehearsalGateAdequate"] is True
    assert payload["journal"]["measuredStagingAt"]
    assert payload["verdict"]["measuredStagingAgeS"] >= 0
    assert payload["verdict"]["measuredStagingStale"] is False


def test_a_rehearsal_without_a_prior_measurement_reports_no_gate() -> None:
    with workspace_tmpdir("reclaim-space-gate-first") as data_dir:
        _seed(data_dir)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir, rehearsal=True)
            assert started["started"] is True, started
            assert _wait_for(
                lambda: (
                    str(
                        storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"][
                            "status"
                        ]
                    )
                    in {"complete", "failed", "cancelled", "superseded", "interrupted"}
                ),
                60.0,
            )
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    result = payload["journal"]["result"]
    assert payload["status"] == "complete", payload
    # The first rehearsal has nothing to be checked against, but it becomes the
    # anchor every later gate is built from.
    assert result["measuredGateBytes"] is None
    assert result["gateAdequate"] is None
    assert payload["journal"]["measuredStagingBytes"] == result["stagingBytes"] > 0


def test_a_rehearsal_flags_a_measured_gate_below_its_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("reclaim-space-gate-short") as data_dir:
        _seed(data_dir)
        monkeypatch.setattr(storage_reclaim, "measured_required_bytes", lambda **_kwargs: 1024)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir, rehearsal=True)
            assert started["started"] is True, started
            assert _wait_for(
                lambda: (
                    str(
                        storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"][
                            "status"
                        ]
                    )
                    in {"complete", "failed", "cancelled", "superseded", "interrupted"}
                ),
                60.0,
            )
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir, refresh=True)[
                "storageReclaim"
            ]
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    result = payload["journal"]["result"]
    assert payload["status"] == "complete", payload
    assert result["measuredGateBytes"] == 1024
    assert result["stagingBytes"] > 1024
    # The run is not failed for it, but the report says the gate was too small —
    # and the verdict carries the warning the panel shows.
    assert result["gateAdequate"] is False
    assert payload["verdict"]["lastRehearsalGateAdequate"] is False


def test_a_run_refuses_when_relief_cannot_close_the_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    verdict = {
        "verdict": "bloat",
        "blockers": [],
        "requiredBytes": 10 * GIB,
        "measuredRequiredBytes": None,
    }
    with workspace_tmpdir("reclaim-space-refusal") as data_dir:
        _seed(data_dir)
        monkeypatch.setattr(
            storage_reclaim, "_compute_verdict", lambda *_args, **_kwargs: dict(verdict)
        )
        monkeypatch.setattr(storage_reclaim_space, "free_space_bytes", lambda _path: 1 * GIB)
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir, rehearsal=True)
            assert started["started"] is True, started
            assert _wait_for(
                lambda: (
                    str(
                        storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"][
                            "status"
                        ]
                    )
                    in {"complete", "failed", "cancelled", "superseded", "interrupted"}
                ),
                60.0,
            )
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    assert payload["status"] == "cancelled", payload
    assert "free-space" in payload["error"]
    assert payload["space"]["requiredBytes"] == 10 * GIB
    assert payload["space"]["freeBytes"] == 1 * GIB


def test_a_completed_real_run_seeds_the_measured_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real run copies the live database too, so it measures the host itself.

    Without this, a host that only ever runs reclaims would never earn a measured
    gate and would keep asking the 1 % heuristic forever.
    """
    with workspace_tmpdir("reclaim-space-real-anchor") as data_dir:
        _seed(data_dir)
        monkeypatch.setattr(
            storage_reclaim,
            "_compute_verdict",
            lambda *_args, **_kwargs: {
                "verdict": "bloat",
                "blockers": [],
                "requiredBytes": 0,
                "measuredRequiredBytes": None,
            },
        )
        try:
            started = storage_reclaim.start_storage_reclaim(data_dir)
            assert started["started"] is True, started
            assert _wait_for(
                lambda: (
                    str(
                        storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"][
                            "status"
                        ]
                    )
                    in {"complete", "failed", "cancelled", "superseded", "interrupted"}
                ),
                60.0,
            )
            # The stubbed verdict above cannot show the panel's fields; read the
            # real one now that the run has written the measurement it seeds.
            monkeypatch.undo()
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir, refresh=True)[
                "storageReclaim"
            ]
        finally:
            storage_reclaim.close_storage_reclaim_jobs()
            close_storage_rebuild_jobs()

    assert payload["status"] == "complete", payload
    assert payload["mode"] == "reclaim"
    anchor = int(payload["journal"]["measuredStagingBytes"])
    assert anchor > 0
    # A reclaim is its own measurement but not a rehearsal: the run reports the
    # size it copied, and the warning stays empty until a rehearsal is checked.
    assert payload["journal"]["lastRehearsalGateAdequate"] is None
    assert payload["verdict"]["measuredStagingBytes"] == anchor
    assert payload["verdict"]["measuredRequiredBytes"] == int(anchor * 1.25) + 64 * MIB
    assert payload["verdict"]["lastRehearsalGateAdequate"] is None
    # The run stamps the anchor, so the panel can age it from the first moment.
    assert payload["journal"]["measuredStagingAt"]
    age = payload["verdict"]["measuredStagingAgeS"]
    assert isinstance(age, int) and 0 <= age < 3600
    assert payload["verdict"]["measuredStagingStale"] is False


def test_the_verdict_warns_about_an_old_anchor_before_the_next_rehearsal() -> None:
    """A stale anchor is a warning the panel can show now, not at the next copy."""
    old_stamp = (datetime.now(UTC) - timedelta(days=40)).isoformat().replace("+00:00", "Z")
    with workspace_tmpdir("reclaim-space-stale-anchor") as data_dir:
        _seed(data_dir)
        _write_reclaim_journal(
            data_dir,
            measuredStagingBytes=2 * MIB,
            measuredStagingAt=old_stamp,
            lastRehearsalGateAdequate=True,
        )
        try:
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir, refresh=True)[
                "storageReclaim"
            ]
        finally:
            storage_reclaim.close_storage_reclaim_jobs()

    verdict = payload["verdict"]
    assert payload["journal"]["measuredStagingAt"] == old_stamp
    assert verdict["measuredStagingAgeS"] >= 40 * 24 * 60 * 60 - 60
    assert verdict["measuredStagingStale"] is True
    # Stale is a warning about the anchor's age: the gate it earned still applies.
    assert verdict["measuredRequiredBytes"] == int(2 * MIB * 1.25) + 64 * MIB
    assert verdict["lastRehearsalGateAdequate"] is True


def test_an_anchor_from_an_unstamped_journal_reads_unknown_not_fresh() -> None:
    """Upgrade path: a journal written before ``measuredStagingAt`` existed.

    The anchor is real, so the gate it earned still applies, but its age cannot
    be known — and an unknown age must not be reported as a healthy one.
    """
    with workspace_tmpdir("reclaim-space-unstamped-anchor") as data_dir:
        _seed(data_dir)
        _write_reclaim_journal(
            data_dir, measuredStagingBytes=2 * MIB, lastRehearsalGateAdequate=True
        )
        try:
            payload = storage_reclaim.get_storage_reclaim_payload(data_dir, refresh=True)[
                "storageReclaim"
            ]
        finally:
            storage_reclaim.close_storage_reclaim_jobs()

    verdict = payload["verdict"]
    assert verdict["measuredStagingBytes"] == 2 * MIB
    assert "measuredStagingAt" not in payload["journal"]
    assert verdict["measuredStagingAgeS"] is None
    assert verdict["measuredStagingStale"] is None
    # Unknown age is not a reason to drop the measurement the journal did record.
    assert verdict["measuredRequiredBytes"] == int(2 * MIB * 1.25) + 64 * MIB
    assert verdict["lastRehearsalGateAdequate"] is True
