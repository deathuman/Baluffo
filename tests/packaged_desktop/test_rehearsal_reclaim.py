"""Focused contracts for the packaged automatic-reclaim startup lane."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.ship.packaged_smoke import rehearsal_reclaim
from tests.packaged_desktop._rehearsal_shared import smoke, workspace_tmpdir

pytestmark = pytest.mark.packaging


def _payload(
    *,
    arm: dict | None = None,
    mode: str = "",
    status: str = "",
    auto: bool = False,
    run_id: str = "",
) -> dict:
    journal = {
        "mode": mode,
        "status": status,
        "auto": auto,
        "runId": run_id,
    }
    if arm is not None:
        journal["autoArm"] = arm
    return {"storageReclaim": {"journal": journal}}


def test_seed_unarmed_bloated_data_root_leaves_freelist_and_no_journal() -> None:
    with workspace_tmpdir("packaged-reclaim-seed") as tmp:
        data_dir = Path(tmp) / "runtime-data"
        seeded = rehearsal_reclaim.seed_unarmed_bloated_data_root(
            data_dir,
            freelist_target_bytes=2 * 1024 * 1024,
        )

        assert seeded["journalPresent"] is False
        assert not Path(seeded["journalPath"]).exists()
        assert seeded["freelistBytes"] >= 1024 * 1024
        with sqlite3.connect(data_dir / "baluffo-runtime.db") as conn:
            freelist_count = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        assert freelist_count > 0


def test_auto_reclaim_state_does_not_pass_on_a_rehearsal_alone() -> None:
    rehearsal_arm = {"armed": True, "reason": "rehearsal-adequate"}

    rehearsal_only = rehearsal_reclaim._auto_reclaim_state(
        _payload(
            arm=rehearsal_arm,
            mode="rehearsal",
            status="complete",
            auto=True,
        )
    )
    assert rehearsal_only["rehearsalComplete"] is True
    assert rehearsal_only["autoReclaimStarted"] is False

    automatic = rehearsal_reclaim._auto_reclaim_state(
        _payload(
            arm=rehearsal_arm,
            mode="reclaim",
            status="running",
            auto=True,
            run_id="reclaim-1",
        )
    )
    assert automatic["autoReclaimStarted"] is True
    assert automatic["runId"] == "reclaim-1"

    manual = rehearsal_reclaim._auto_reclaim_state(
        _payload(
            arm=rehearsal_arm,
            mode="reclaim",
            status="running",
            auto=False,
            run_id="reclaim-manual",
        )
    )
    assert manual["autoReclaimStarted"] is False

    unarmed = rehearsal_reclaim._auto_reclaim_state(
        _payload(
            arm={"armed": False, "reason": "rehearsal-inadequate"},
            mode="rehearsal",
            status="complete",
        )
    )
    assert unarmed["armed"] is False
    assert unarmed["autoReclaimStarted"] is False


def test_auto_reclaim_state_fails_fast_only_for_an_automatic_terminal_failure() -> None:
    failed = rehearsal_reclaim._auto_reclaim_state(
        _payload(
            arm={"armed": True, "reason": "rehearsal-adequate"},
            mode="reclaim",
            status="failed",
            auto=True,
            run_id="reclaim-failed",
        )
    )
    assert failed["terminalFailure"] is True

    manual_failed = rehearsal_reclaim._auto_reclaim_state(
        _payload(
            arm={"armed": True, "reason": "rehearsal-adequate"},
            mode="reclaim",
            status="failed",
            auto=False,
            run_id="reclaim-manual",
        )
    )
    assert manual_failed["terminalFailure"] is False


def test_cli_exposes_admin_reclaim_startup_rehearsal() -> None:
    args = smoke.parse_args(["--admin-reclaim-startup-rehearsal"])
    assert args.admin_reclaim_startup_rehearsal is True
