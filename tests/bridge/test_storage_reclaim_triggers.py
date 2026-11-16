"""Automatic reclaim triggers (§4): the closeout hook and the startup check.

These cases pin the properties the triggers rely on: an attempt only happens when
auto mode is enabled, it goes through the orchestrator with ``auto=True`` so every
blocker and the cooldown still apply, and a retention pass defers it into exactly
one re-check that waits for the pass instead of burning a copy.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from src.bridge import storage_reclaim, storage_reclaim_space, storage_reclaim_triggers
from src.storage.baluffo_store import DEFAULT_DB_NAME
from src.storage.current_feed_rebuild import rebuild_required_free_bytes
from tests.helpers.temp_paths import workspace_tmpdir

GIB = 1024**3


def _anchor_journal(data_dir: Path, staging_bytes: int, *, age_days: float = 0) -> None:
    measured_at = (datetime.now(UTC) - timedelta(days=age_days)).isoformat()
    _journal(data_dir, {"measuredStagingBytes": staging_bytes, "measuredStagingAt": measured_at})


def _journal(data_dir: Path, payload: dict[str, Any]) -> None:
    storage_reclaim.reclaim_journal_path(data_dir).write_text(json.dumps(payload), encoding="utf-8")


def _recording_start(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[dict[str, Any]], threading.Event]:
    calls: list[dict[str, Any]] = []
    started = threading.Event()

    def fake_start(
        data_dir: Path | str,
        *,
        auto: bool = False,
        hold: bool = True,
        rehearsal: bool = False,
    ) -> dict[str, Any]:
        calls.append(
            {
                "dataDir": str(data_dir),
                "auto": auto,
                "hold": hold,
                "rehearsal": rehearsal,
            }
        )
        started.set()
        return {"ok": True, "started": True, "runId": "reclaim_trigger", "storageReclaim": {}}

    monkeypatch.setattr(storage_reclaim, "start_storage_reclaim", fake_start)
    return calls, started


def test_a_task_closeout_asks_for_an_automatic_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "1")
    monkeypatch.setattr(storage_reclaim_triggers, "_retention_running", lambda _dir: False)
    calls, started = _recording_start(monkeypatch)

    with workspace_tmpdir("reclaim-trigger-closeout") as data_dir:
        result = storage_reclaim_triggers.on_task_idle(data_dir, hold=True)

        assert result == {"scheduled": True, "reason": "task-closeout"}
        assert started.wait(5.0) is True

    assert calls == [
        {
            "dataDir": str(Path(data_dir).resolve()),
            "auto": True,
            "hold": True,
            "rehearsal": False,
        }
    ]


def test_the_trigger_takes_the_hold_its_presentation_needs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "1")
    monkeypatch.setattr(storage_reclaim_triggers, "_retention_running", lambda _dir: False)
    calls, started = _recording_start(monkeypatch)

    with workspace_tmpdir("reclaim-trigger-hold-container") as container_dir:
        monkeypatch.delenv("BALUFFO_DESKTOP_MODE", raising=False)
        storage_reclaim_triggers.on_task_idle(container_dir)
        assert started.wait(5.0) is True

    with workspace_tmpdir("reclaim-trigger-hold-desktop") as desktop_dir:
        monkeypatch.setenv("BALUFFO_DESKTOP_MODE", "1")
        storage_reclaim_triggers.on_task_idle(desktop_dir)
        assert started.wait(5.0) is True

    assert [call["hold"] for call in calls] == [True, False]


def test_a_closeout_during_retention_waits_for_it_then_asks_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "1")
    monkeypatch.setattr(storage_reclaim_triggers, "RECLAIM_TRIGGER_RETENTION_POLL_S", 0.01)
    retention = {"running": True}
    seen: list[bool] = []

    def retention_running(_dir: Path) -> bool:
        seen.append(retention["running"])
        return retention["running"]

    monkeypatch.setattr(storage_reclaim_triggers, "_retention_running", retention_running)
    calls, started = _recording_start(monkeypatch)

    with workspace_tmpdir("reclaim-trigger-retention") as data_dir:
        storage_reclaim_triggers.on_task_idle(data_dir, hold=False)

        # Still inside the retention pass: nothing may start yet.
        assert started.wait(0.1) is False
        assert seen and seen[0] is True

        retention["running"] = False
        assert started.wait(5.0) is True

    assert len(calls) == 1
    assert calls[0]["auto"] is True
    assert calls[0]["hold"] is False


def test_a_second_trigger_while_a_check_is_pending_rides_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "1")
    monkeypatch.setattr(storage_reclaim_triggers, "_retention_running", lambda _dir: False)
    calls, started = _recording_start(monkeypatch)

    with workspace_tmpdir("reclaim-trigger-single-flight") as data_dir:
        first = storage_reclaim_triggers.schedule_startup_check(data_dir, hold=True, delay_s=0.3)
        second = storage_reclaim_triggers.on_task_idle(data_dir, hold=True)

        assert first == {"scheduled": True, "reason": "startup"}
        assert second == {"scheduled": False, "reason": "already-scheduled"}
        assert started.wait(5.0) is True

    assert len(calls) == 1


def test_the_startup_check_waits_before_asking(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "1")
    monkeypatch.setattr(storage_reclaim_triggers, "_retention_running", lambda _dir: False)
    calls, started = _recording_start(monkeypatch)

    with workspace_tmpdir("reclaim-trigger-startup-delay") as data_dir:
        result = storage_reclaim_triggers.schedule_startup_check(data_dir, hold=True, delay_s=0.3)

        assert result == {"scheduled": True, "reason": "startup"}
        # The first paint is not competing with a multi-GB copy: no attempt yet.
        assert started.wait(0.05) is False
        assert started.wait(5.0) is True

    assert len(calls) == 1


def test_triggers_do_nothing_when_auto_mode_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, raising=False)
    calls, started = _recording_start(monkeypatch)

    with workspace_tmpdir("reclaim-trigger-disabled") as data_dir:
        closeout = storage_reclaim_triggers.on_task_idle(data_dir, hold=True)
        startup = storage_reclaim_triggers.schedule_startup_check(data_dir, hold=True, delay_s=0.0)

        assert closeout == {"scheduled": False, "reason": "auto-disabled"}
        assert startup == {"scheduled": False, "reason": "auto-disabled"}
        assert started.wait(0.1) is False

    assert calls == []


def test_a_recheck_gives_up_when_retention_never_stops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "1")
    monkeypatch.setattr(storage_reclaim_triggers, "RECLAIM_TRIGGER_RETENTION_POLL_S", 0.01)
    monkeypatch.setattr(storage_reclaim_triggers, "RECLAIM_TRIGGER_RECHECK_MAX_S", 0.0)
    monkeypatch.setattr(storage_reclaim_triggers, "_retention_running", lambda _dir: True)
    calls, started = _recording_start(monkeypatch)

    with workspace_tmpdir("reclaim-trigger-give-up") as data_dir:
        storage_reclaim_triggers.on_task_idle(data_dir, hold=True)

        # Retention outlived the re-check's cap; the trigger declines quietly.
        assert started.wait(0.3) is False

    assert calls == []


def test_the_idle_probe_asks_when_free_space_is_near_the_measured_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "1")
    monkeypatch.setattr(storage_reclaim_triggers, "_retention_running", lambda _dir: False)
    calls, started = _recording_start(monkeypatch)

    with workspace_tmpdir("reclaim-idle-probe-near") as data_dir:
        _anchor_journal(data_dir, GIB)
        gate = storage_reclaim_space.measured_required_bytes(staging_bytes=GIB)
        assert gate is not None
        # 1.2x the gate is inside the 1.5x headroom, so the verdict is worth its price.
        monkeypatch.setattr(
            storage_reclaim_space, "free_space_bytes", lambda _path: int(gate * 1.2)
        )

        result = storage_reclaim_triggers.probe_idle_space(data_dir, hold=True)

        assert result == {"scheduled": True, "reason": "idle-probe"}
        assert started.wait(5.0) is True

    assert [call["auto"] for call in calls] == [True]
    assert calls[0]["hold"] is True


def test_the_idle_probe_declines_while_the_disk_has_room(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "1")
    calls, started = _recording_start(monkeypatch)
    reads: list[str] = []

    with workspace_tmpdir("reclaim-idle-probe-far") as data_dir:
        _anchor_journal(data_dir, GIB)
        gate = storage_reclaim_space.measured_required_bytes(staging_bytes=GIB)
        assert gate is not None

        def free_space(_path: Any) -> int:
            reads.append("free-space")
            return int(gate * 10)

        monkeypatch.setattr(storage_reclaim_space, "free_space_bytes", free_space)

        result = storage_reclaim_triggers.probe_idle_space(data_dir, hold=True)

        assert result == {"scheduled": False, "reason": "gate-not-near"}
        assert started.wait(0.2) is False

    # One statvfs, no verdict, no start: that is the whole cost of a quiet beat.
    assert reads == ["free-space"]
    assert calls == []


def test_an_unmeasured_host_probes_against_the_heuristic_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(storage_reclaim_triggers, "file_size", lambda _path: 100 * GIB)
    gate = rebuild_required_free_bytes(100 * GIB)

    with workspace_tmpdir("reclaim-idle-probe-heuristic") as data_dir:
        monkeypatch.setattr(
            storage_reclaim_space, "free_space_bytes", lambda _path: int(gate * 1.1)
        )
        assert storage_reclaim_triggers.near_gate(data_dir) is True

        monkeypatch.setattr(storage_reclaim_space, "free_space_bytes", lambda _path: gate * 5)
        assert storage_reclaim_triggers.near_gate(data_dir) is False

        # An unreadable disk is not a reason to start a copy on a guess.
        monkeypatch.setattr(storage_reclaim_space, "free_space_bytes", lambda _path: None)
        assert storage_reclaim_triggers.near_gate(data_dir) is False


def test_the_idle_probe_is_silent_when_auto_mode_is_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, raising=False)
    calls, started = _recording_start(monkeypatch)

    def must_not_read(_path: Path) -> int:
        raise AssertionError("a disabled idle probe must not touch the disk")

    monkeypatch.setattr(storage_reclaim_space, "free_space_bytes", must_not_read)

    with workspace_tmpdir("reclaim-idle-probe-disabled") as data_dir:
        assert storage_reclaim_triggers.probe_idle_space(data_dir, hold=True) == {
            "scheduled": False,
            "reason": "auto-disabled",
        }
        assert started.wait(0.1) is False

    assert calls == []


def test_the_idle_probe_beats_once_per_interval_and_only_once_at_a_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    beats: list[float] = []
    beaten = threading.Event()

    def probe(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        beats.append(time.monotonic())
        beaten.set()
        return {"scheduled": False, "reason": "gate-not-near"}

    monkeypatch.setattr(storage_reclaim_triggers, "probe_idle_space", probe)
    monkeypatch.setattr(storage_reclaim_triggers, "maintain_auto", lambda *_a, **_k: {})

    with workspace_tmpdir("reclaim-idle-probe-loop") as data_dir:
        assert storage_reclaim_triggers.start_idle_probe(data_dir, interval_s=0.02) is True
        try:
            # One loop per process: a second caller rides the running one.
            assert storage_reclaim_triggers.start_idle_probe(data_dir, interval_s=0.02) is False
            assert beaten.wait(5.0) is True
        finally:
            storage_reclaim_triggers.stop_idle_probe(timeout_s=5.0)

        settled = len(beats)
        assert settled >= 1
        time.sleep(0.1)
        assert len(beats) == settled

    # Stopping released the loop, so a later start is allowed again.
    assert storage_reclaim_triggers.start_idle_probe(data_dir, interval_s=60) is True
    storage_reclaim_triggers.stop_idle_probe(timeout_s=5.0)


def test_an_unarmed_host_schedules_its_first_rehearsal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, raising=False)
    monkeypatch.setattr(storage_reclaim_triggers, "_retention_running", lambda _dir: False)
    calls, started = _recording_start(monkeypatch)

    with workspace_tmpdir("reclaim-measure-first") as data_dir:
        monkeypatch.setattr(storage_reclaim_space, "free_space_bytes", lambda _path: 100 * GIB)

        result = storage_reclaim_triggers.maintain_auto(data_dir, hold=True)

        assert result == {"scheduled": True, "reason": "arming"}
        assert started.wait(5.0) is True

    assert calls == [
        {
            "dataDir": str(Path(data_dir).resolve()),
            "auto": True,
            "hold": True,
            # A rehearsal measures; it never installs or deletes.
            "rehearsal": True,
        }
    ]


def test_a_stale_anchor_schedules_a_rehearsal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "1")
    monkeypatch.setattr(storage_reclaim_triggers, "_retention_running", lambda _dir: False)
    calls, started = _recording_start(monkeypatch)

    with workspace_tmpdir("reclaim-measure-stale") as data_dir:
        # Past the 30-day staleness window the panel warns about.
        _anchor_journal(data_dir, GIB, age_days=40)
        monkeypatch.setattr(storage_reclaim_space, "free_space_bytes", lambda _path: 100 * GIB)

        result = storage_reclaim_triggers.maintain_auto(data_dir, hold=True)

        assert result == {"scheduled": True, "reason": "anchor-heal"}
        assert started.wait(5.0) is True

    assert [call["rehearsal"] for call in calls] == [True]


def test_a_fresh_anchor_schedules_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "1")
    calls, started = _recording_start(monkeypatch)

    with workspace_tmpdir("reclaim-measure-fresh") as data_dir:
        _anchor_journal(data_dir, GIB, age_days=1)

        result = storage_reclaim_triggers.maintain_auto(data_dir, hold=True)

        assert result == {"scheduled": False, "reason": "anchor-fresh"}
        assert started.wait(0.1) is False

    assert calls == []


def test_an_armed_host_without_a_measurement_schedules_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, raising=False)
    calls, started = _recording_start(monkeypatch)

    with workspace_tmpdir("reclaim-measure-unmeasured") as data_dir:
        _journal(data_dir, {"autoArm": {"armed": True, "reason": "rehearsal-adequate"}})

        result = storage_reclaim_triggers.maintain_auto(data_dir, hold=True)

        assert result == {"scheduled": False, "reason": "anchor-unmeasured"}
        assert started.wait(0.1) is False

    assert calls == []


def test_the_kill_switch_blocks_measuring_a_stale_anchor_too(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "0")
    calls, started = _recording_start(monkeypatch)

    with workspace_tmpdir("reclaim-measure-killed") as data_dir:
        _anchor_journal(data_dir, GIB, age_days=40)

        result = storage_reclaim_triggers.maintain_auto(data_dir, hold=True)

        assert result == {"scheduled": False, "reason": "kill-switch"}
        assert started.wait(0.1) is False

    assert calls == []


def test_a_run_in_flight_blocks_the_rehearsal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "1")
    monkeypatch.setattr(storage_reclaim, "reclaim_run_active", lambda _dir: True)
    calls, started = _recording_start(monkeypatch)

    with workspace_tmpdir("reclaim-measure-active") as data_dir:
        _anchor_journal(data_dir, GIB, age_days=40)
        monkeypatch.setattr(storage_reclaim_space, "free_space_bytes", lambda _path: 100 * GIB)

        result = storage_reclaim_triggers.maintain_auto(data_dir, hold=True)

        assert result == {"scheduled": False, "reason": "run-active"}
        assert started.wait(0.1) is False

    assert calls == []


def test_the_rehearsal_waits_for_free_space(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, raising=False)
    calls, started = _recording_start(monkeypatch)

    with workspace_tmpdir("reclaim-measure-no-space") as data_dir:
        monkeypatch.setattr(storage_reclaim_space, "free_space_bytes", lambda _path: 1024)

        result = storage_reclaim_triggers.maintain_auto(data_dir, hold=True)

        assert result == {"scheduled": False, "reason": "insufficient-free-space"}
        assert started.wait(0.1) is False

    assert calls == []


def test_a_nearly_full_disk_measures_when_an_unowned_copy_covers_the_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, raising=False)
    monkeypatch.setattr(storage_reclaim_triggers, "_retention_running", lambda _dir: False)
    calls, started = _recording_start(monkeypatch)
    mib = 1024**2

    with workspace_tmpdir("reclaim-measure-relief") as data_dir:
        # A 1 MiB anchor makes the measured gate small enough to build by hand.
        _anchor_journal(data_dir, mib)
        gate = storage_reclaim_space.measured_required_bytes(staging_bytes=mib)
        assert gate is not None
        # Raw free space is a MiB short of the gate: without relief the host would
        # skip measuring itself on exactly the disk that needs it.
        monkeypatch.setattr(storage_reclaim_space, "free_space_bytes", lambda _path: gate - mib)
        orphan = data_dir / f"{DEFAULT_DB_NAME}.deadbeef.rebuild.tmp"
        orphan.write_bytes(b"0" * (mib + 1))

        result = storage_reclaim_triggers.maintain_auto(data_dir, hold=True)

        assert result == {"scheduled": True, "reason": "arming"}
        assert started.wait(5.0) is True

        # The run removes that copy before copying, so the relief is real: without
        # the file the same free reading refuses.
        orphan.unlink()
        assert storage_reclaim_triggers._copy_affordable(data_dir) is False

    assert [call["rehearsal"] for call in calls] == [True]


def test_a_stale_anchor_heals_when_an_unowned_copy_covers_the_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "1")
    monkeypatch.setattr(storage_reclaim_triggers, "_retention_running", lambda _dir: False)
    calls, started = _recording_start(monkeypatch)
    mib = 1024**2

    with workspace_tmpdir("reclaim-heal-relief") as data_dir:
        # Past the 30-day staleness window, so the beat wants a re-measurement.
        _anchor_journal(data_dir, mib, age_days=40)
        gate = storage_reclaim_space.measured_required_bytes(staging_bytes=mib)
        assert gate is not None
        # Raw free space is a MiB short of the gate; the unowned copy below is what
        # a run would delete first, so the heal must count it too (not only arming).
        monkeypatch.setattr(storage_reclaim_space, "free_space_bytes", lambda _path: gate - mib)
        orphan = data_dir / f"{DEFAULT_DB_NAME}.deadbeef.rebuild.tmp"
        orphan.write_bytes(b"0" * (mib + 1))

        result = storage_reclaim_triggers.maintain_auto(data_dir, hold=True)

        assert result == {"scheduled": True, "reason": "anchor-heal"}
        assert started.wait(5.0) is True

        orphan.unlink()
        assert storage_reclaim_triggers._copy_affordable(data_dir) is False

    assert [call["rehearsal"] for call in calls] == [True]


def test_a_recent_rehearsal_rate_limits_the_next_measurement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "1")
    calls, started = _recording_start(monkeypatch)
    anchor = (datetime.now(UTC) - timedelta(days=40)).isoformat()
    hour_ago = (datetime.now(UTC) - timedelta(hours=1)).isoformat()

    with workspace_tmpdir("reclaim-measure-rate-limited") as data_dir:
        _journal(
            data_dir,
            {
                "mode": "rehearsal",
                "status": "failed",
                "measuredStagingBytes": GIB,
                "measuredStagingAt": anchor,
                "finishedAt": hour_ago,
            },
        )
        monkeypatch.setattr(storage_reclaim_space, "free_space_bytes", lambda _path: 100 * GIB)

        result = storage_reclaim_triggers.maintain_auto(data_dir, hold=True)

        # A copy that just failed must not be repeated on the next beat.
        assert result == {"scheduled": False, "reason": "rate-limited"}
        assert started.wait(0.1) is False

    assert calls == []


def test_the_rate_limit_expires_so_the_next_beat_can_measure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "1")
    monkeypatch.setattr(storage_reclaim_triggers, "_retention_running", lambda _dir: False)
    calls, started = _recording_start(monkeypatch)
    anchor = (datetime.now(UTC) - timedelta(days=40)).isoformat()
    yesterday = (datetime.now(UTC) - timedelta(hours=25)).isoformat()

    with workspace_tmpdir("reclaim-measure-retry") as data_dir:
        _journal(
            data_dir,
            {
                "mode": "rehearsal",
                "status": "failed",
                "measuredStagingBytes": GIB,
                "measuredStagingAt": anchor,
                "finishedAt": yesterday,
            },
        )
        monkeypatch.setattr(storage_reclaim_space, "free_space_bytes", lambda _path: 100 * GIB)

        result = storage_reclaim_triggers.maintain_auto(data_dir, hold=True)

        assert result == {"scheduled": True, "reason": "anchor-heal"}
        assert started.wait(5.0) is True

    assert [call["rehearsal"] for call in calls] == [True]
