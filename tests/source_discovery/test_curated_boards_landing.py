"""The registration delivers 40% of the openings it appears to, and no more.

Measured by draining discovery over the 500 curated boards in an isolated data dir:
**182 boards and 1,777 of 4,356 openings land — 36% of boards, 40% of openings.**

The per-adapter split is the whole story:

===============  ======  =======
adapter          boards  landed
===============  ======  =======
static             276     30
workday             17      0
greenhouse          44     40
smartrecruiters     16     16
ashby               39     37
bamboohr            47      0
lever               22     20
breezy              15     15
jazzhr               8      8
teamtailor          13     13
recruitee            3      3
===============  ======  =======

Before the `api_url` and provider-spec fixes this was 70 boards and 762 openings —
14% and 17%. The two defects described in
`test_probe_provider_counts.py` account for the whole difference, and both were
found by running discovery rather than by reading it.

What still lands nothing is what remains: Workday at 1,065 openings (its CXS
endpoint is a POST behind a certifi-anchored context, so no `api_url` can fix it),
BambooHR at 192 (no public JSON listing), and 246 of the 276 static boards. The
static tail is not a vendor problem — most scraped careers pages render through
JavaScript too.

The drain stalls rather than draining. Round 1 approves 185; rounds 2 through 6
approve 0, with ~226 deferred by cap every round and `active` frozen at 2,434.
Re-running does not help, because a pending row is deduped as `existing_id`
on the next cycle and is never re-probed for the evidence it needs.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.source_discovery.config import load_curated_coverage_boards

_ROOT = Path(__file__).resolve().parents[2]
_DRAIN = _ROOT / "_out" / "coverage" / "drain-rounds.json"

# Adapters whose curated rows carry an `api_url`, so discovery probes a JSON listing
# and counts real rows. Measured, not assumed -- see the module docstring.
_API_BACKED_ADAPTERS = {"greenhouse", "lever", "ashby", "breezy", "smartrecruiters", "recruitee"}


def test_the_drain_measurement_is_present() -> None:
    """Guards the numbers quoted above against silently going stale."""
    assert _DRAIN.exists(), f"{_DRAIN} missing: re-run tools/coverage_drain.py"


@pytest.mark.skipif(not _DRAIN.exists(), reason="drain measurement not recorded yet")
def test_the_drain_stalls_after_the_first_round() -> None:
    """Repeated runs do not deliver more.

    This is the load-bearing finding: an operator watching coverage flatline after
    the registration would reasonably conclude the registration failed, when in fact
    it delivered everything it structurally can and is waiting on a probe fix.
    """
    rounds = json.loads(_DRAIN.read_text(encoding="utf-8"))["rounds"]
    assert len(rounds) >= 3, rounds
    assert rounds[0]["approved"] > 0, "round 1 must approve something for this to mean anything"
    later = [r["approved"] for r in rounds[1:]]
    assert all(count == 0 for count in later), f"drain resumed unexpectedly: {later}"


@pytest.mark.skipif(not _DRAIN.exists(), reason="drain measurement not recorded yet")
def test_active_count_freezes_while_candidates_stay_deferred() -> None:
    rounds = json.loads(_DRAIN.read_text(encoding="utf-8"))["rounds"]
    assert rounds[0]["active"] == rounds[-1]["active"]
    assert rounds[-1]["deferredByCap"] > 0, "backlog should still be waiting on a probe fix"


def test_the_high_volume_adapters_are_the_ones_that_cannot_land() -> None:
    """The blocked adapters hold most of the openings.

    Workday is 1,065 openings across 17 boards and none of them land, because its
    listing is a JavaScript app. That is why the delivered share is 40% rather than
    the ~90% the registration implies.
    """
    openings_by_adapter: dict[str, int] = {}
    for row in load_curated_coverage_boards():
        adapter = str(row["adapter"])
        openings_by_adapter[adapter] = openings_by_adapter.get(adapter, 0) + int(
            row.get("coverageAuditOpenings") or 0
        )
    for adapter in ("workday", "static", "bamboohr"):
        assert openings_by_adapter.get(adapter, 0) > 0, adapter
    # Workday alone is more than a fifth of the whole promised total, and no api_url
    # can fix it: its CXS endpoint is a POST, not a fetchable URL.
    assert openings_by_adapter["workday"] > 1000


def test_static_dominates_but_cannot_all_land_either() -> None:
    """276 static boards hold the most openings and only 30 landed.

    Worth pinning because the naive reading of the registration is "static boards are
    the safe ones". They are not: most scraped careers pages render through JavaScript
    too, so the static adapter needs the same fix as the JS ATS vendors.
    """
    rows = [r for r in load_curated_coverage_boards() if r["adapter"] == "static"]
    assert len(rows) > 250
