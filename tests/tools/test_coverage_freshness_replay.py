"""A board skipped often enough must still be fetched again.

Before the fix, `refresh_next_eligible_check_at` ran for every terminal status including
`excluded`, so a skip pushed a board's next-eligible time out by another freshness window
while `lastSuccessAt` stayed frozen. With a 720-minute empty-source window and a run cadence
shorter than that, the deadline slid on every run and the board was never fetched again. The
live report showed the consequence: 1,892 of 2,013 registered boards skipped, median last
successful fetch 121 days.

These tests replay the real decision function over a simulated cadence rather than asserting
the arithmetic, so they fail if the policy changes shape. The sliding policy is replayed
alongside as the control -- it must fetch nothing, or the replay is not measuring anything.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

_TOOLS = Path(__file__).resolve().parents[2] / "tools"
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))

_spec = importlib.util.spec_from_file_location(
    "coverage_freshness_replay", _TOOLS / "coverage_freshness_replay.py"
)
assert _spec and _spec.loader
replay = importlib.util.module_from_spec(_spec)
sys.modules["coverage_freshness_replay"] = replay
_spec.loader.exec_module(replay)

_REPLAY = replay._replay


def test_a_board_is_refetched_under_the_current_policy() -> None:
    result = _REPLAY(days=30, cadence_minutes=60, sliding=False)
    assert result["fetched"] > 0, result
    assert result["fetched"] >= 30, result


def test_the_pre_fix_policy_never_fetches_and_the_replay_notices() -> None:
    """The control. If this ever fetches, the replay has stopped measuring the policy."""
    result = _REPLAY(days=30, cadence_minutes=60, sliding=True)
    assert result["fetched"] == 0, result


def test_a_cadence_matching_the_window_still_refetches() -> None:
    """A run exactly as often as the window expires must not deadlock."""
    result = _REPLAY(days=30, cadence_minutes=720, sliding=False)
    assert result["fetched"] >= 15, result


def test_the_two_policies_diverge_at_every_cadence_tested() -> None:
    for cadence in (30, 60, 180, 360, 720):
        fixed = _REPLAY(days=30, cadence_minutes=cadence, sliding=False)["fetched"]
        broken = _REPLAY(days=30, cadence_minutes=cadence, sliding=True)["fetched"]
        assert fixed > broken, f"cadence {cadence}: {fixed} vs {broken}"


def test_the_replay_is_measuring_something_at_all() -> None:
    """Every round is accounted for, so a silently short loop cannot pass as a fix."""
    for sliding in (False, True):
        result: dict[str, Any] = _REPLAY(days=7, cadence_minutes=60, sliding=sliding)
        assert result["fetched"] + result["skipped"] == result["rounds"], result
        assert result["rounds"] == 168, result
