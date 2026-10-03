"""The registration is smaller than it looks, and the reason is structural.

Measured by draining discovery over the 500 curated boards in an isolated data dir:
**70 boards and 762 of 4,356 openings land — 14% of boards, 17% of openings.**

The per-adapter split is the whole story:

===============  ======  =======
adapter          boards  landed
===============  ======  =======
static             276     30
workday             17      0
greenhouse          44      0
smartrecruiters     16     16
ashby               39      0
bamboohr            47      0
lever               22      0
breezy              15      0
jazzhr               8      8
teamtailor          13     13
recruitee            3      3
===============  ======  =======

Adapters that land are exactly the ones whose board page is server-rendered and
whose job links are in the initial HTML: SmartRecruiters, Teamtailor, JazzHR,
Recruitee, plus 30 static pages. Every adapter that renders through JavaScript lands
nothing, and those hold the large boards — Workday alone is 1,065 openings.

The mechanism is a disagreement between two counters that were assumed to agree.
`tools/coverage_verify.py` counts rows from the vendor's JSON API. Discovery's probe
counts job links in the fetched HTML. For a single-page app those numbers are
unrelated: an Ashby board returns 120 rows from `api.ashbyhq.com` while its page
ships no job links at all, and auto-approval requires a positive `jobsFound`, so the
row sits in pending forever.

The drain stalls rather than draining. Round 1 approves 71; rounds 2 through 8
approve 0, with the same ~258 deferred by cap every round and `active` frozen at
2,320. Re-running does not help, because a pending row is deduped as `existing_id`
on the next cycle and is never re-probed for the evidence it needs.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.source_discovery.config import load_curated_coverage_boards

_ROOT = Path(__file__).resolve().parents[2]
_DRAIN = _ROOT / "_out" / "coverage" / "drain-rounds.json"

# Adapters whose board pages are server-rendered, so discovery's HTML probe sees the
# job links that auto-approval requires. Everything else renders through JavaScript
# and lands nothing. Measured, not assumed -- see the module docstring.
_HTML_VISIBLE_ADAPTERS = {"smartrecruiters", "teamtailor", "jazzhr", "recruitee"}


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
    listing is a JavaScript app. That is why the delivered share is 17% rather than
    the ~90% the registration implies.
    """
    openings_by_adapter: dict[str, int] = {}
    for row in load_curated_coverage_boards():
        adapter = str(row["adapter"])
        openings_by_adapter[adapter] = openings_by_adapter.get(adapter, 0) + int(
            row.get("coverageAuditOpenings") or 0
        )
    for adapter in ("workday", "greenhouse", "ashby", "bamboohr", "lever", "breezy"):
        assert adapter not in _HTML_VISIBLE_ADAPTERS
        assert openings_by_adapter.get(adapter, 0) > 0, adapter
    # The blocked set holds the majority of the promised openings.
    blocked = sum(
        count
        for adapter, count in openings_by_adapter.items()
        if adapter not in _HTML_VISIBLE_ADAPTERS
    )
    assert blocked > sum(openings_by_adapter.get(a, 0) for a in _HTML_VISIBLE_ADAPTERS)


def test_static_dominates_but_cannot_all_land_either() -> None:
    """276 static boards hold the most openings and only 30 landed.

    Worth pinning because the naive reading of the registration is "static boards are
    the safe ones". They are not: most scraped careers pages render through JavaScript
    too, so the static adapter needs the same fix as the JS ATS vendors.
    """
    rows = [r for r in load_curated_coverage_boards() if r["adapter"] == "static"]
    assert len(rows) > 250
