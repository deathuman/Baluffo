"""A rejected row that reappears in the live active store is a hygiene signal.

The preflight's resurrection finding exists because a rejection is removed from the *seed* but not
tombstoned in the store, so nothing stops discovery from re-approving the same source id. When that
happens the row lands in both buckets: rejected by the adjudication, active by re-discovery -- and
the running registry silently keeps fetching a board an operator already removed. The RTL
`non_game_employer_scope` rejection of 2026-09-26 left exactly that exposure open.
"""

from __future__ import annotations

from tools.repo_health.source_registry_preflight import find_resurrected_rejected_rows

RTL = {"id": "phenom:listing_url:https://jobsearch.createyourowncareer.com/rtl/"}
OTHER = {"id": "static:listing_url:https://example.test/careers"}


def test_a_row_in_both_buckets_is_reported() -> None:
    assert find_resurrected_rejected_rows([RTL, OTHER], [RTL]) == [
        "phenom:listing_url:https://jobsearch.createyourowncareer.com/rtl/"
    ]


def test_a_rejected_row_not_in_active_is_fine() -> None:
    assert find_resurrected_rejected_rows([OTHER], [RTL]) == []


def test_case_drift_is_the_same_row_not_a_disagreement() -> None:
    """An id that differs only in case across the buckets is the same row, flagged once."""
    upper = {"id": "phenom:listing_url:https://jobsearch.createyourowncareer.com/RTL/"}
    assert find_resurrected_rejected_rows([upper], [RTL]) == [RTL["id"]]


def test_no_report_when_a_bucket_is_missing() -> None:
    assert find_resurrected_rejected_rows([], [RTL]) == []
    assert find_resurrected_rejected_rows([RTL], []) == []


def test_rows_without_ids_are_ignored() -> None:
    assert find_resurrected_rejected_rows([OTHER], [{"adapter": "static"}]) == []
