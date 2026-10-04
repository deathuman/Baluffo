#!/usr/bin/env python3
"""Does the freshness-window fix actually stop boards being skipped forever?

The fix is a one-line policy change with a large claimed effect, and the claim is the part
that needs evidence. Before it, `refresh_next_eligible_check_at` ran for every terminal
status including `excluded`, so each skip pushed a board's deadline out by another window
while its success timestamp stayed frozen. In the live report that left 1,892 of 2,013
registered boards skipped as `cache_within_freshness_window`, with a median last successful
fetch 121 days old.

This drives the real decision function over a simulated run cadence rather than asserting
the arithmetic. Two cadences are replayed against the same policy -- the current one, and the
pre-fix one -- so the difference is attributable to the policy and nothing else.

It is not production: it cannot be. It answers "does a run cadence shorter than the freshness
window now fetch the board, and did it not before", which is the mechanism the live numbers
were read from. Whether the skipped count in a live run falls is still only observable there.

Usage:
  python tools/coverage_freshness_replay.py --days 30 --cadence-minutes 60
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.jobs import state_incremental as si  # noqa: E402
from src.jobs.state_source_records import (  # noqa: E402
    excluded_source_skip_reason,
    refresh_next_eligible_check_at,
)

_SOURCE = "static_source::static:listing_url:https://example.test/careers"
_SKIP_REASON = "cache_within_freshness_window"


def _replay(*, days: int, cadence_minutes: int, sliding: bool) -> dict[str, Any]:
    """Run `days` of fetch rounds at `cadence_minutes` and count what was fetched.

    `sliding=True` reproduces the pre-fix policy, where a skip advanced the deadline.

    `get_incremental_cache_decision` compares `nextEligibleCheckAt` against the real clock, so
    the replay simulates elapsed time by *backdating* the stored deadline each round: with
    `remaining` simulated minutes left on the window it writes `real_now + remaining`, which
    reaches the past exactly when the window expires.

    The policy is then the only difference between the runs. `remaining` is reset to the full
    window by a real fetch and -- only when `sliding` is set -- by a skip. That is the whole
    bug: before the fix a skip restarted the countdown, so a board skipped often enough was
    never fetched again.

    Two earlier versions measured nothing and both looked like null results: one rewrote the
    deadline every round so it could never expire, and one never rewrote it so real time never
    reached it.
    """
    real_now = datetime.now(UTC)
    window = si.DEFAULT_INCREMENTAL_EMPTY_SOURCE_MINUTES
    entry: dict[str, Any] = {
        "lastStatus": "ok",
        "lastKeptCount": 0,
        # Last real fetch 121 days ago, as the live report showed.
        "lastSuccessAt": (real_now - timedelta(days=121)).isoformat(),
        "lastCheckedAt": (real_now - timedelta(days=121)).isoformat(),
        "consecutiveZeroKept": 1,
    }
    rounds = max(1, int(days * 24 * 60 / cadence_minutes))
    fetched = 0
    skipped = 0
    remaining = window
    for _step in range(rounds):
        entry["nextEligibleCheckAt"] = (real_now + timedelta(minutes=remaining)).isoformat()
        decision = si.get_incremental_cache_decision(_SOURCE, {_SOURCE: entry}, adapter="static")
        if decision.get("cacheDecision") == "skip_fresh":
            skipped += 1
            entry["lastStatus"] = "excluded"
            # Time passes either way; only the pre-fix policy restarts the countdown.
            remaining = max(0, remaining - cadence_minutes)
            if sliding:
                remaining = window
            continue
        fetched += 1
        entry["lastStatus"] = "ok"
        entry["consecutiveZeroKept"] = 1
        entry["lastSuccessAt"] = real_now.isoformat()
        entry["lastCheckedAt"] = real_now.isoformat()
        remaining = window
    return {
        "rounds": rounds,
        "fetched": fetched,
        "skipped": skipped,
        "windowMinutes": window,
        "cadenceMinutes": cadence_minutes,
        "deadlineAdvancedOnSkip": sliding,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--cadence-minutes", type=int, default=60)
    args = parser.parse_args()

    print(f"replaying {args.days} days at a {args.cadence_minutes}-minute cadence")
    print(f"empty-source freshness window: {si.DEFAULT_INCREMENTAL_EMPTY_SOURCE_MINUTES} minutes\n")
    print(f"{'policy':<34}{'rounds':>7}{'fetched':>9}{'skipped':>9}")
    for sliding, label in (
        (False, "current (skip leaves deadline)"),
        (True, "pre-fix (skip slides it)"),
    ):
        r = _replay(days=args.days, cadence_minutes=args.cadence_minutes, sliding=sliding)
        print(f"{label:<34}{r['rounds']:>7}{r['fetched']:>9}{r['skipped']:>9}")

    print("\nthe policy itself, on one board whose deadline is already 12 hours out:")
    entry: dict[str, Any] = {
        "lastStatus": "excluded",
        "lastKeptCount": 0,
        "lastSuccessAt": "2026-06-05T00:00:00+00:00",
        "nextEligibleCheckAt": "2026-10-05T00:00:00+00:00",
    }
    before = entry["nextEligibleCheckAt"]
    refresh_next_eligible_check_at(
        entry,
        source_name=_SOURCE,
        finished_at="2026-10-04T12:00:00+00:00",
        skip_reason=excluded_source_skip_reason({"exclusionReason": _SKIP_REASON}),
    )
    print(f"  deadline before        : {before}")
    print(f"  after a skip           : {entry['nextEligibleCheckAt']}  (unchanged)")
    entry["lastStatus"] = "ok"
    refresh_next_eligible_check_at(
        entry, source_name=_SOURCE, finished_at="2026-10-06T09:30:00+00:00"
    )
    print(f"  after a real fetch     : {entry['nextEligibleCheckAt']}  (advanced)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
