"""The preflight must not report a row as lost when the ids differ only in letter case.

RTL is the live example: the seed spelled the tenant `.../RTL/` and the store `.../rtl/`, so an
exact-match set difference called a healthy, actively-fetching row "absent from the store entirely"
and sent it down the lost-row path. The disagreement is real and still reported -- as its own
finding -- but it is not data loss, and a seed restore must not add a second row for one board.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tools.repo_health.source_registry_preflight import check_seed_store_split_consistency

UPPER = "phenom:listing_url:https://jobsearch.createyourowncareer.com/RTL/"
LOWER = "phenom:listing_url:https://jobsearch.createyourowncareer.com/rtl/"


def _row(source_id: str, **extra: Any) -> dict[str, Any]:
    return {
        "id": source_id,
        "studio": "RTL Enterprises",
        "adapter": "phenom",
        "registryState": "active",
        "listing_url": "https://jobsearch.createyourowncareer.com/RTL/job/x/1/",
        **extra,
    }


def _check(live: list[dict], seed: list[dict], pending: list[dict] | None = None) -> dict:
    return check_seed_store_split_consistency(
        Path("."), live_rows=live, seed_rows=seed, pending_rows=pending or []
    )


def test_id_case_difference_is_not_reported_as_a_lost_row() -> None:
    result = _check(live=[_row(LOWER)], seed=[_row(UPPER)])

    assert result["seed_row_lost_from_store"] == []
    assert result["seed_row_demoted_in_store"] == []
    assert result["seed_row_id_case_mismatch"] == [UPPER]


def test_a_genuinely_lost_row_is_still_reported() -> None:
    result = _check(live=[], seed=[_row(UPPER)])

    assert result["seed_row_lost_from_store"] == [UPPER]
    assert result["seed_row_id_case_mismatch"] == []


def test_pending_is_not_folded_so_a_demoted_row_still_reads_as_demoted() -> None:
    """Folding pending as well was tried and reverted: it is far too loose in the other direction.

    A seed id that case-matches any *parked* row would read as a case mismatch (40 spurious findings
    on the live registry) and a genuinely demoted row would stop being reported as demoted. Only the
    active view is folded.
    """
    result = _check(live=[], seed=[_row(UPPER)], pending=[_row(LOWER)])

    assert result["seed_row_lost_from_store"] == [UPPER]
    assert result["seed_row_demoted_in_store"] == []
    assert result["seed_row_id_case_mismatch"] == []


def test_matching_ids_produce_no_case_finding() -> None:
    result = _check(live=[_row(LOWER)], seed=[_row(LOWER)])

    assert result["seed_row_id_case_mismatch"] == []
    assert result["seed_row_lost_from_store"] == []
