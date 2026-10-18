from __future__ import annotations

from typing import Any

from src.jobs import dedup_targeting
from src.jobs.dedup import deduplicate_jobs
from src.jobs.models import CanonicalJob

# The Elevato pair scan used to walk every accumulated row once per Elevato-shaped row: 40 such
# rows among 40,247 cost 791,402 pair checks -- 57% of dedup's CPU, for 0.1% of the feed. A
# growth ratio cannot catch that shape: the scan is O(E x N) with E fixed by the feed, so on live
# rows it reads as a large constant. Pair checks are the scale-free property. A pair is one
# static-Elevato row and one google-sheets row on an Elevato link, so the candidates are the
# *opposite* side only; carrying 24x more non-Elevato rows must not add a single check.
_ELEVATO_PAIRS = 4
_ELEVATO_SMALL_FEED = 1_000
_ELEVATO_LARGE_FEED = 4_000
_ELEVATO_QLOC_SOURCE = "static_source::static:listing_url:https://qloc.elevato.net/en/"
_ELEVATO_ROLES = ("Technical Artist", "Senior Animator", "Gameplay Programmer", "Level Designer")
# A candidate list rebuilt per lookup would keep the pair-check count bounded while re-classifying
# every accumulated row for every Elevato row. Side classification is what that costs, so the
# count is bounded per row of the feed and must not move when the feed carries more Elevato rows.
# Measured on 1,000 rows: 3.02 classifications per row at 4 pairs and 3.08 at 16; a
# rebuild-per-lookup reads 6.00 and 17.90, so both the per-row budget and the cross-arm ratio
# separate the two without a wall clock.
_ELEVATO_CLASSIFICATIONS_PER_ROW = 4
_ELEVATO_WIDE_PAIRS = 16


def _elevato_title(index: int) -> str:
    return f"{_ELEVATO_ROLES[index % len(_ELEVATO_ROLES)]} {index}"


# The live pair: both rows carry a location, so the sparse-identity path blocks on it
# (``_find_sparse_merge_target``) and the Elevato scan is what merges them.
def _elevato_static_row(index: int) -> CanonicalJob:
    return CanonicalJob.from_mapping(
        {
            "title": _elevato_title(index),
            "company": "QLOC",
            "city": "Warsaw",
            "country": "Poland",
            "workType": "Onsite",
            "contractType": "Full-time",
            "jobLink": f"https://qloc.elevato.net/en/role,j,{index}",
            "source": _ELEVATO_QLOC_SOURCE,
            "sourceJobId": f"elevato:{index}",
            "adapter": "static",
        }
    )


def _elevato_sheet_row(index: int) -> CanonicalJob:
    return CanonicalJob.from_mapping(
        {
            "title": _elevato_title(index),
            "company": "Qloc careers",
            "city": "Warsaw",
            "country": "Poland",
            "workType": "Onsite",
            "contractType": "Full-time",
            "jobLink": f"https://qloc.elevato.net/pl/role,j,{100 + index}",
            "source": "google_sheets",
            "sourceJobId": f"sheet:qloc:{index}",
        }
    )


def _non_elevato_row(index: int) -> CanonicalJob:
    return CanonicalJob.from_mapping(
        {
            "title": f"Studio Role {index}",
            "company": f"Studio {index}",
            "city": "Remote",
            "country": "Unknown",
            "jobLink": f"https://example.com/careers/jobs/{index}",
            "source": "greenhouse",
            "sourceJobId": f"greenhouse-{index}",
        }
    )


def _elevato_feed(population: int, pairs: int = _ELEVATO_PAIRS) -> list[CanonicalJob]:
    """``pairs`` pairs among ``population`` rows, the rest non-Elevato filler.

    The pairs sit about half a feed deep, which is where live pairing lives: the sheet side is
    not near the front of the accumulated rows, so a full scan pays the whole walk to reach it.
    """
    filler_count = population - 2 * pairs
    lead = filler_count // 2
    rows = [_non_elevato_row(index) for index in range(lead)]
    rows += [_elevato_sheet_row(index) for index in range(pairs)]
    rows += [_elevato_static_row(index) for index in range(pairs)]
    rows += [_non_elevato_row(index) for index in range(lead, filler_count)]
    return rows


def _count_elevato_pair_checks(monkeypatch: Any) -> dict[str, int]:
    """Count pair checks, and the ones that matched, without changing the answer."""
    counts = {"checks": 0, "hits": 0}
    real = dedup_targeting._is_elevato_static_google_sheets_pair

    def counting(left: Any, right: Any) -> bool:
        counts["checks"] += 1
        matched = real(left, right)
        if matched:
            counts["hits"] += 1
        return matched

    monkeypatch.setattr(dedup_targeting, "_is_elevato_static_google_sheets_pair", counting)
    return counts


def _count_elevato_side_classifications(monkeypatch: Any) -> dict[str, int]:
    """Count the targeting module's own side classifications, leaving the answer unchanged."""
    counts = {"classifications": 0}
    real_static = dedup_targeting._is_elevato_static_row
    real_sheet = dedup_targeting._is_google_sheets_elevato_row

    def counting_static(row: Any) -> bool:
        counts["classifications"] += 1
        return real_static(row)

    def counting_sheet(row: Any) -> bool:
        counts["classifications"] += 1
        return real_sheet(row)

    monkeypatch.setattr(dedup_targeting, "_is_elevato_static_row", counting_static)
    monkeypatch.setattr(dedup_targeting, "_is_google_sheets_elevato_row", counting_sheet)
    return counts


def test_deduplicate_jobs_elevato_side_classification_is_paid_once_per_row(monkeypatch) -> None:
    counts = _count_elevato_side_classifications(monkeypatch)

    narrow_rows, narrow_stats = deduplicate_jobs(_elevato_feed(_ELEVATO_SMALL_FEED))
    narrow = counts["classifications"]
    counts["classifications"] = 0
    wide_rows, wide_stats = deduplicate_jobs(
        _elevato_feed(_ELEVATO_SMALL_FEED, pairs=_ELEVATO_WIDE_PAIRS)
    )
    wide = counts["classifications"]

    assert (narrow_stats["mergedCount"], wide_stats["mergedCount"]) == (
        _ELEVATO_PAIRS,
        _ELEVATO_WIDE_PAIRS,
    )
    assert (len(narrow_rows), len(wide_rows)) == (
        _ELEVATO_SMALL_FEED - _ELEVATO_PAIRS,
        _ELEVATO_SMALL_FEED - _ELEVATO_WIDE_PAIRS,
    )
    # One pass over the feed, whatever the feed carries:
    budget = _ELEVATO_CLASSIFICATIONS_PER_ROW * _ELEVATO_SMALL_FEED
    assert narrow <= budget
    assert wide <= budget
    # Four times the Elevato rows must not multiply the work a rebuild-per-lookup does per row.
    assert wide < 2 * narrow


def test_deduplicate_jobs_elevato_pair_checks_do_not_scale_with_feed_size(monkeypatch) -> None:
    counts = _count_elevato_pair_checks(monkeypatch)

    small_rows, small_stats = deduplicate_jobs(_elevato_feed(_ELEVATO_SMALL_FEED))
    small_checks, small_hits = counts["checks"], counts["hits"]
    counts["checks"] = counts["hits"] = 0
    large_rows, large_stats = deduplicate_jobs(_elevato_feed(_ELEVATO_LARGE_FEED))

    # The scan has to do the merging for the assertion below to mean anything.
    assert (small_hits, counts["hits"]) == (_ELEVATO_PAIRS, _ELEVATO_PAIRS)
    assert (small_stats["mergedCount"], large_stats["mergedCount"]) == (
        _ELEVATO_PAIRS,
        _ELEVATO_PAIRS,
    )
    assert (len(small_rows), len(large_rows)) == (
        _ELEVATO_SMALL_FEED - _ELEVATO_PAIRS,
        _ELEVATO_LARGE_FEED - _ELEVATO_PAIRS,
    )
    # The ceiling is one check per opposite-side row per pairing row, whatever the population.
    assert small_checks <= _ELEVATO_PAIRS * _ELEVATO_PAIRS
    assert counts["checks"] == small_checks


# The live pair, as the fetcher sees it: a stale sheet row and the live static row whose
# location blocks the sparse-identity path, so this is a pair only the Elevato scan can merge.
_QLOC_SOURCE = "static_source::static:listing_url:https://qloc.elevato.net/en/"
_QLOC_STATIC = {
    "title": "Technical Artist",
    "company": "QLOC",
    "city": "Warsaw",
    "country": "Poland",
    "workType": "Onsite",
    "contractType": "Full-time",
    "jobLink": "https://qloc.elevato.net/en/technical-artist,j,240",
    "sector": "Game",
    "source": _QLOC_SOURCE,
    "sourceJobId": "elevato:240",
    "postedAt": "2026-06-01",
    "adapter": "static",
}
_QLOC_SHEET = {
    **_QLOC_STATIC,
    "company": "Qloc careers",
    "jobLink": "https://qloc.elevato.net/pl/technical-artist,j,229",
    "source": "google_sheets",
    "sourceJobId": "sheet:qloc:technical-artist",
    "postedAt": "2026-05-01",
    "adapter": "",
    "sourceBundleCount": 1,
    "sourceBundle": [
        {
            "source": "google_sheets",
            "sourceJobId": "sheet:qloc:technical-artist",
            "jobLink": "https://qloc.elevato.net/pl/technical-artist,j,229",
            "adapter": "csv",
            "studio": "Qloc careers",
        }
    ],
}
_QLOC_SECOND_SHEET = {
    **_QLOC_SHEET,
    "jobLink": "https://qloc.elevato.net/pl/technical-artist,j,229?variant=2",
    "sourceJobId": "sheet:qloc:technical-artist-2",
}


def _sheet_role_bucket_row(index: int, title: str) -> dict[str, Any]:
    """The generic-role guard shape: a bucket title on a shared studio link."""
    return {
        "title": title,
        "company": "Bucket Studio",
        "city": "Remote",
        "country": "Unknown",
        "jobLink": "https://example.com/careers/animation",
        "source": "google_sheets",
        "sourceJobId": f"bucket-{index}",
    }


def _full_scan_target(current: CanonicalJob, merged_rows: list[CanonicalJob], _sides: Any):
    """The implementation the side index replaces: every accumulated row, in order."""
    if not (
        dedup_targeting._is_elevato_static_row(current)
        or dedup_targeting._is_google_sheets_elevato_row(current)
    ):
        return None
    for idx, target in enumerate(merged_rows):
        if dedup_targeting._is_elevato_static_google_sheets_pair(current, target):
            return idx
    return None


def test_elevato_side_index_refresh_reclassifies_a_row_a_merge_rewrote() -> None:
    rows = [_non_elevato_row(0)]
    index = dedup_targeting._ElevatoSideIndex()
    index.sync(rows)

    sheet_row = _elevato_sheet_row(0)
    static_row = _elevato_static_row(0)
    assert index.candidates(static_row) == []

    # A merge rewrote row 0 into the sheet side: it is only still a candidate because the
    # merge re-derived its membership.
    rows[0] = sheet_row
    index.refresh(0, rows[0])
    assert index.candidates(static_row) == [0]

    # And back off the side: a stale index would keep handing back a row that cannot pair.
    rows[0] = _non_elevato_row(0)
    index.refresh(0, rows[0])
    assert index.candidates(static_row) == []

    # A later sync must not re-index the row it has already passed.
    index.sync(rows)
    assert index.candidates(static_row) == []


def test_deduplicate_jobs_elevato_side_index_matches_a_full_scan(monkeypatch) -> None:
    rows: list[CanonicalJob | dict[str, Any]] = [
        dict(_QLOC_STATIC),
        dict(_QLOC_SHEET),
        dict(_QLOC_SECOND_SHEET),
        _sheet_role_bucket_row(0, "Animation"),
        _sheet_role_bucket_row(1, "Localization"),
        *[_non_elevato_row(index) for index in range(300)],
    ]

    indexed_merged, indexed_stats = deduplicate_jobs(rows)

    monkeypatch.setattr(
        dedup_targeting, "_find_elevato_static_google_sheets_target", _full_scan_target
    )
    scanned_merged, scanned_stats = deduplicate_jobs(rows)

    # The pair has to have merged, or both arms agree on doing nothing.
    assert indexed_stats["mergedCount"] >= 2
    assert [row.to_dict() for row in indexed_merged] == [row.to_dict() for row in scanned_merged]
    assert indexed_stats == scanned_stats
