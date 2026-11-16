"""Self-arming at the journal boundary: the recorded decision is what enables auto.

The decision itself is pinned in `tests/storage/test_reclaim_auto_arm.py`. These
cases pin how the bridge reads it: unset `BALUFFO_RECLAIM_AUTO_ENABLED` defers to the
journal's `autoArm`, `0` overrides it, and a restart that interrupts a rehearsal
re-decides the arm instead of leaving a stale one in place.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.bridge import storage_reclaim, storage_reclaim_triggers
from tests.bridge.test_storage_reclaim import _finished_hours_ago, _seed, _write_journal
from tests.helpers.temp_paths import workspace_tmpdir


def _armed_record() -> dict[str, Any]:
    return {
        "armed": True,
        "reason": "rehearsal-adequate",
        "decidedAt": "2026-11-16T10:00:00Z",
        "evidence": {
            "runId": "rehearsal-arm",
            "stagingBytes": 900 * 1024**2,
            "gateBytes": None,
            "gateAdequate": None,
            "anchorAgeS": 0,
        },
    }


def test_a_rehearsal_journal_arms_auto_without_the_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, raising=False)
    with workspace_tmpdir("reclaim-auto-armed") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {
                "status": "complete",
                "mode": "rehearsal",
                "runId": "rehearsal-arm",
                "finishedAt": _finished_hours_ago(2),
                "measuredStagingBytes": 900 * 1024**2,
                "measuredStagingAt": _finished_hours_ago(2),
                "autoArm": _armed_record(),
            },
        )

        assert storage_reclaim.auto_reclaim_enabled(data_dir) is True
        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    assert "auto-disabled" not in reclaim["verdict"]["blockers"]
    assert reclaim["journal"]["autoArm"]["reason"] == "rehearsal-adequate"
    # The verdict explains the arm without anyone opening the raw journal.
    auto = reclaim["verdict"]["autoStart"]
    assert auto["enabled"] is True
    assert auto["source"] == "rehearsal"
    assert auto["reason"] == "rehearsal-adequate"
    assert auto["decidedAt"] == "2026-11-16T10:00:00Z"
    assert auto["evidence"]["runId"] == "rehearsal-arm"
    assert auto["evidence"]["stagingBytes"] == 900 * 1024**2


def test_the_kill_switch_overrides_a_recorded_arm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "0")
    with workspace_tmpdir("reclaim-auto-killed") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {
                "status": "complete",
                "mode": "rehearsal",
                "runId": "rehearsal-arm",
                "finishedAt": _finished_hours_ago(2),
                "autoArm": _armed_record(),
            },
        )

        assert storage_reclaim.auto_reclaim_enabled(data_dir) is False
        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    # The measurement stands; the operator's switch still wins.
    assert "auto-disabled" in reclaim["verdict"]["blockers"]
    auto = reclaim["verdict"]["autoStart"]
    assert auto["reason"] == "kill-switch"
    assert auto["switch"] == "off"
    assert auto["armed"] is True


def test_a_host_that_never_armed_stays_auto_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, raising=False)
    with workspace_tmpdir("reclaim-auto-unarmed") as data_dir:
        _seed(data_dir)
        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

        assert storage_reclaim.auto_reclaim_enabled(data_dir) is False

    assert "auto-disabled" in reclaim["verdict"]["blockers"]
    auto = reclaim["verdict"]["autoStart"]
    assert auto == {
        "enabled": False,
        "source": "none",
        "switch": "unset",
        "armed": False,
        "reason": "not-armed",
        "decidedAt": "",
        "evidence": None,
    }


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on", " On "])
def test_the_switch_reads_the_recognised_on_spellings(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, value)

    assert storage_reclaim.auto_reclaim_switch() is True


@pytest.mark.parametrize("value", ["0", "false", "FALSE", "no", "off", "disable"])
def test_the_switch_reads_explicit_off_and_typos_as_off(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, value)

    # A failed attempt to configure the kill switch is not consent.
    assert storage_reclaim.auto_reclaim_switch() is False


def test_an_unset_or_blank_switch_delegates_to_the_host(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, raising=False)
    assert storage_reclaim.auto_reclaim_switch() is None

    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "   ")
    assert storage_reclaim.auto_reclaim_switch() is None


def test_an_unrecognised_value_cannot_enable_an_armed_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, "banana")
    with workspace_tmpdir("reclaim-auto-typo") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {
                "status": "complete",
                "mode": "rehearsal",
                "runId": "rehearsal-arm",
                "finishedAt": _finished_hours_ago(2),
                "autoArm": _armed_record(),
            },
        )

        # The measurement stands; the misspelt switch still refuses everything.
        assert storage_reclaim.auto_reclaim_enabled(data_dir) is False
        assert storage_reclaim_triggers.maintain_auto(data_dir, hold=True) == {
            "scheduled": False,
            "reason": "kill-switch",
        }
        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    assert "auto-disabled" in reclaim["verdict"]["blockers"]
    assert reclaim["verdict"]["autoStart"]["reason"] == "kill-switch"


def test_an_interrupted_rehearsal_records_a_disarmed_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, raising=False)
    with workspace_tmpdir("reclaim-auto-rehearsal-interrupted") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {
                "status": "running",
                "phase": "copying",
                "mode": "rehearsal",
                "runId": "rehearsal-dead",
                "measuredStagingAt": _finished_hours_ago(2),
                "autoArm": _armed_record(),
            },
        )

        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    # A restart settled the copy as interrupted, so the old arm is re-decided.
    assert reclaim["journal"]["autoArm"]["armed"] is False
    assert reclaim["journal"]["autoArm"]["reason"] == "rehearsal-failed"
