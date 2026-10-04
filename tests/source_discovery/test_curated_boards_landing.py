"""What actually reaches the registry, measured rather than assumed.

Draining discovery over the 673 curated boards, per adapter:

===============  ======  =======  =========  ==========
adapter          boards  landed   openings  delivered
===============  ======  =======  =========  ==========
static              412     408      3,259      3,244
workday              17      17      1,065      1,065
greenhouse           52      51        655        617
workable             29      29        440        440
smartrecruiters      16      16        432        432
ashby                39      39        334        334
bamboohr             47      47        192        192
lever                22      20        181        165
breezy               15      15         69         69
jazzhr                8       8         32         32
teamtailor           13      13         26         26
recruitee             3       3         14         14
===============  ======  =======  =========  ==========

The table is 557 rows rather than 558 because AppLovin's Greenhouse board was proposed by
the audit while the hand-curated literal already carried it. The two tables are
concatenated, so a board in both is fetched twice and double-counted; four such boards
were already known (Voodoo, 2K Czech, Hangar 13, Yggdrasil) and this was the fifth.
`_drop_already_curated` now removes the class by board identity rather than by studio label.

**666 boards and 6,630 of 6,699 openings — 99% of boards, 99% of openings.**

The seven that still do not land are two empty Greenhouse and Lever boards and five thin
static boards, one of which (`vivastudios.com`) disconnects mid-response.

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

Five further findings came out of measuring the later waves rather than trusting the
labels, and all five were the harness lying rather than the boards failing.

**A board's JSON API lives on a different host from its career page**, so all 39 Ashby
boards register as `ashby:api_url:https://api.ashbyhq.com/posting-api/job-board/…`, which
resolved to `('ashby', 'api.ashbyhq.com', '')` — host and tenant both lost — and read as
39 unregistered boards worth 334 openings. Greenhouse, Lever and SmartRecruiters have the
same shape. `registry_identity` now folds a known API host onto its canonical career-page
host and recovers the tenant from the API path.

**hrmos needed no adapter at all.** It was labelled an unsupported vendor because no
dedicated adapter exists, but its tenant listing pages are server-rendered, the runtime's
detector reads all 32 tenants, and 4 were already registered as static rows. The remaining
833 openings are 28 static rows, and all 28 land.

**Workable was lost twice to a wrong URL.** The probe read
`/api/v1/accounts/<a>/jobs`, which answers HTTP 400, and the row it then built carried
only `account`, which `endpoint_url` cannot resolve — so all 29 boards failed as "missing
adapter or URL". Both now use the runtime's own `JsonFeedSpec` template, and all 29 land
440 openings. The lesson is the one this effort keeps teaching: the probe was looking
somewhere the openings were not, and the zero was recorded as an answer.

**Greenhouse's EU hosts were invisible to the host rules**, so 158 openings on 8 tenants
collapsed onto a single static host-root row. `job-boards.eu.greenhouse.io` does not match
`(^|\.)job-boards\.greenhouse\.io$` — it ends `eu.greenhouse.io`, not
`job-boards.greenhouse.io` — so it fell through to `static`. The runtime's API serves every
one of those tenants: tripledotstudios 92 jobs, sportygroup 38, growe 15, kambi 10.

**A static candidate's listing URL is its host root, which is often not the careers page.**
`tools/coverage_listing_discovery.py` derives the listing from the URLs of the openings that
were missed on the board — the board was found because those specific openings exist — and
finds a readable one where the root has none: `koeitecmo.co.jp/recruit/career` serves 43
rows. The boards it cannot improve return HTTP 200 with zero anchors, so they are
registered on their recorded openings rather than on a probe, as the 246 boards above were.
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
#
# Two registration waves beyond the original 500, both of which turned out to be
# measurement faults rather than missing adapters.
#
# The 28 hrmos rows: hrmos was labelled an unsupported vendor purely because no dedicated
# adapter exists, but its tenant listing pages are server-rendered and the runtime's own
# detector reads all 32 tenants -- cgames 100 rows, capcom 92, gamefreak 59 -- and
# `hrmos.co/pages/cygames` was already a registered static row keeping 340 jobs in
# production. So the 833 remaining openings are 28 static rows, not a vendor integration.
#
# The 29 workable rows: 440 openings that read as unreadable twice over. The verify tool
# probed `/api/v1/accounts/<a>/jobs`, which answers HTTP 400, and the registry rows it
# then built carried only `account`, which `endpoint_url` cannot resolve -- so all 29
# failed the probe as "missing adapter or URL" and registered 0 of 29. Both now use the
# runtime's own JsonFeedSpec template. keywords-intl1 serves 282 jobs and sideinc 378.
_DELIVERED = {
    "static": (412, 408, 3_259, 3_244),
    "workday": (17, 17, 1_065, 1_065),
    "greenhouse": (52, 51, 655, 617),
    "workable": (29, 29, 440, 440),
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
        "workable",
    ):
        boards, landed, _openings, _delivered = _DELIVERED[adapter]
        assert landed == boards, f"{adapter} landed {landed}/{boards}"


def test_workday_bamboohr_and_workable_land_every_opening_they_promised() -> None:
    """Workday and BambooHR were the two largest zero-delivery blocks: 1,065 and 192.

    Workable joins them because 440 openings were lost twice to a wrong URL -- once in the
    probe and once in the row it built -- and it now lands all 29 boards and every opening.
    """
    for adapter in ("workday", "bamboohr", "workable"):
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
