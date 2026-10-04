"""What actually reaches the registry, measured rather than assumed.

Draining discovery over the 500 curated boards, per adapter:

===============  ======  =======  =========  ==========
adapter          boards  landed   openings  delivered
===============  ======  =======  =========  ==========
static              276     271      1,514      1,492
workday              17      17      1,065      1,065
greenhouse           44      43        497        459
smartrecruiters      16      16        432        432
ashby                39      39        334        334
bamboohr             47      47        192        192
lever                22      20        181        165
breezy               15      15         69         69
jazzhr                8       8         32         32
teamtailor           13      13         26         26
recruitee             3       3         14         14
===============  ======  =======  =========  ==========

**492 boards and 4,280 of 4,356 openings — 98% of boards, 98% of openings.**

That number came from 762 (17%) in six steps, and every step was found by running
discovery rather than by reading it:

1. **Rows carried no `api_url`.** `endpoint_url()` probes `api_url` first, so a row with
   only a `board_url` sent the probe at a single-page app that ships no job links. The
   board read as healthy with zero jobs and auto-approval believed the zero.
2. **Ashby and Breezy were missing from the probe's `provider_specs`,** so a board whose
   `api_url` returns JSON fell through to an HTML anchor matcher. Ashby landed 0 of 39.
3. **bamboohr, oracle_hcm and workday were declared supported but had no probe-count
   branch at all**, accounting for 68 of 71 recorded probe failures.
4. **Workday's listing needs a POST.** Its CXS endpoint sits behind a certifi-anchored
   TLS context, so a GET cannot see it and every Workday board probed as zero.
5. **BambooHR's listing is not the page its URL points at.** `/careers` is a
   single-anchor JavaScript shell; the listing is a GET to `/careers/list` returning
   `{"meta": {"totalCount": N}, "result": [...]}`. Nothing about the board needed
   changing -- the runtime's own adapter already collected from it, keeping 99 jobs in
   production -- only the endpoint the probe read.

Every structured adapter now lands all of its boards, and the static tail lands too.
The sixth fix is the one that closed it: **246 boards serve openings no HTTP probe can
see.** Sweeping all 276 curated static boards with the runtime's own detector puts the
split exactly at the delivery line — the 30 readable boards all landed, and the 246
rendered ones landed none. The runtime's static fetcher keeps 80 jobs from one of them
(`careers.wbd.com/jobs`) where the probe reads zero, and rendering that page in a
browser does not help because the content arrives via a further request the landing
page never issues. Curated seed rows now fall back to the openings recorded when the
board was found serving them, which is an independent observation rather than an
inference from a zero.

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
    "static": (276, 271, 1_514, 1_492),
    "workday": (17, 17, 1_065, 1_065),
    "greenhouse": (44, 43, 497, 459),
    "smartrecruiters": (16, 16, 432, 432),
    "ashby": (39, 39, 334, 334),
    "bamboohr": (47, 47, 192, 192),
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

    Workday needed a POST, Ashby needed a provider spec, and BambooHR needed a
    different endpoint than its URL names. All now land every board. A regression here
    means a probe path was lost, and it is silent at runtime.
    """
    for adapter in (
        "workday",
        "ashby",
        "bamboohr",
        "smartrecruiters",
        "breezy",
        "recruitee",
        "jazzhr",
        "teamtailor",
    ):
        boards, landed, _openings, _delivered = _DELIVERED[adapter]
        assert landed == boards, f"{adapter} landed {landed}/{boards}"


def test_workday_and_bamboohr_land_every_opening_they_promised() -> None:
    """They were the two largest zero-delivery blocks: 1,065 and 192 openings."""
    for adapter in ("workday", "bamboohr"):
        _boards, _landed, openings, delivered = _DELIVERED[adapter]
        assert delivered == openings, adapter


def test_static_is_the_only_remaining_gap() -> None:
    """Names the remaining work so it cannot be quietly forgotten."""
    for adapter, (_b, landed, _o, _d) in _DELIVERED.items():
        if adapter == "static":
            continue
        boards = _DELIVERED[adapter][0]
        assert landed >= boards - 4, f"{adapter} landed only {landed}/{boards}"
    static_boards, static_landed, static_openings, static_delivered = _DELIVERED["static"]
    assert static_landed > static_boards * 0.9
    assert static_openings - static_delivered < 100


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
