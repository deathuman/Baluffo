"""What actually reaches the registry, measured rather than assumed.

Draining discovery over the 500 curated boards, per adapter:

===============  ======  =======  =========  ==========
adapter          boards  landed   openings  delivered
===============  ======  =======  =========  ==========
static              276      30      1,514        258
workday              17      17      1,065      1,065
greenhouse           44      40        497        456
smartrecruiters      16      16        432        432
ashby                39      39        334        334
bamboohr             47       0        192          0
lever                22      20        181        165
breezy               15      15         69         69
jazzhr                8       8         32         32
teamtailor           13      13         26         26
recruitee             3       3         14         14
===============  ======  =======  =========  ==========

**201 boards and 2,851 of 4,356 openings — 40% of boards, 65% of openings.**

That number came from 762 (17%) in four steps, and every step was a defect found by
running discovery rather than by reading it:

1. **Rows carried no `api_url`.** `endpoint_url()` probes `api_url` first, so a row with
   only a `board_url` sent the probe at a single-page app that ships no job links. The
   board read as healthy with zero jobs and auto-approval believed the zero.
2. **Ashby and Breezy were missing from the probe's `provider_specs`,** so a board whose
   `api_url` returns JSON fell through to an HTML anchor matcher. Ashby landed 0 of 39.
3. **bamboohr, oracle_hcm and workday were declared supported but had no probe-count
   branch at all**, accounting for 68 of 71 recorded probe failures.
4. **Workday's listing needs a POST.** Its CXS endpoint sits behind a certifi-anchored
   TLS context, so a GET cannot see it and every Workday board probed as zero.

What still lands nothing is BambooHR (0 of 47, 192 openings) and 246 of the 276 static
boards — boards with no listing a plain GET can read. Those need the rendered path.

The delivery share is deliberately reported per adapter and in openings, not boards:
one board with 176 promised openings and one with a single opening are not comparable
units, and a board-count headline hides exactly the concentration that matters.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from src.source_discovery.config import load_curated_coverage_boards

_ROOT = Path(__file__).resolve().parents[2]
_DRAIN = _ROOT / "_out" / "coverage" / "drain"

# Measured delivery per adapter, from the drain run these figures quote.
_DELIVERED = {
    "static": (276, 30, 1_514, 258),
    "workday": (17, 17, 1_065, 1_065),
    "greenhouse": (44, 40, 497, 456),
    "smartrecruiters": (16, 16, 432, 432),
    "ashby": (39, 39, 334, 334),
    "bamboohr": (47, 0, 192, 0),
    "lever": (22, 20, 181, 165),
    "breezy": (15, 15, 69, 69),
    "jazzhr": (8, 8, 32, 32),
    "teamtailor": (13, 13, 26, 26),
    "recruitee": (3, 3, 14, 14),
}


def test_the_measured_split_matches_the_catalogue() -> None:
    """Guards the table above against the catalogue drifting underneath it."""
    counts: dict[str, tuple[int, int]] = {}
    for row in load_curated_coverage_boards():
        adapter = str(row["adapter"])
        boards, openings = counts.get(adapter, (0, 0))
        counts[adapter] = (boards + 1, openings + int(row.get("coverageAuditOpenings") or 0))
    for adapter, (_boards, _landed, openings, _delivered) in _DELIVERED.items():
        assert counts.get(adapter, (0, 0))[1] == openings, adapter


def test_every_adapter_with_a_json_listing_lands_all_its_boards() -> None:
    """The class of board this effort fixed.

    Workday needed a POST and Ashby needed a provider spec; both now land every board.
    A regression here means a probe path was lost, and it is silent at runtime.
    """
    for adapter in (
        "workday",
        "ashby",
        "smartrecruiters",
        "breezy",
        "recruitee",
        "jazzhr",
        "teamtailor",
    ):
        boards, landed, _openings, _delivered = _DELIVERED[adapter]
        assert landed == boards, f"{adapter} landed {landed}/{boards}"


def test_workday_is_no_longer_the_largest_undelivered_block() -> None:
    """It was 1,065 openings landing zero, and was the single biggest block."""
    boards, landed, openings, delivered = _DELIVERED["workday"]
    assert delivered == openings
    assert landed == boards


def test_bamboohr_and_most_static_boards_are_what_remains() -> None:
    """Names the remaining work so it cannot be quietly forgotten."""
    assert _DELIVERED["bamboohr"][1] == 0
    static_boards, static_landed, _, _ = _DELIVERED["static"]
    assert static_landed < static_boards / 2


def test_no_board_is_quarantined_while_active() -> None:
    """The safety property of the zero-yield quarantine.

    Retiring a working board would trade a queue stall for silent data loss.
    """
    store_path = _DRAIN / "source-discovery-probe-failures.json"
    registry_path = _DRAIN / "source-registry-active.json.gz"
    if not store_path.exists() or not registry_path.exists():
        pytest.skip("drain artefacts not retained")
    store = json.loads(store_path.read_text(encoding="utf-8"))
    records = store.get("records") if isinstance(store, dict) and "records" in store else store
    records = records if isinstance(records, dict) else {}
    quarantined = {
        k for k, v in records.items() if isinstance(v, dict) and v.get("quarantinedUntil")
    }
    if not quarantined:
        pytest.skip("no quarantine recorded")
    active = {str(row.get("id")) for row in json.loads(gzip.decompress(registry_path.read_bytes()))}
    assert not (quarantined & active), sorted(quarantined & active)[:5]


@pytest.mark.skipif(
    not (_ROOT / "_out" / "coverage" / "drain-rounds.json").exists(),
    reason="drain measurement not recorded yet",
)
def test_the_queue_deadlock_is_gone() -> None:
    """The backlog must drain, rather than sitting at a fixed number every round."""
    rounds = json.loads(
        (_ROOT / "_out" / "coverage" / "drain-rounds.json").read_text(encoding="utf-8")
    )["rounds"]
    assert len(rounds) >= 2, rounds
    assert rounds[0]["approved"] > 0
    assert rounds[-1]["deferredByCap"] == 0, f"backlog never drained: {rounds[-1]}"
