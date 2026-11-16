"""Automatic reclaim triggers (§4): the closeout hook and the startup check.

These cases pin the properties the triggers rely on: an attempt only happens when
auto mode is enabled, it goes through the orchestrator with ``auto=True`` so every
blocker and the cooldown still apply, and a retention pass defers it into exactly
one re-check that waits for the pass instead of burning a copy.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import pytest

from src.bridge import storage_reclaim, storage_reclaim_triggers
from tests.helpers.temp_paths import workspace_tmpdir


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
