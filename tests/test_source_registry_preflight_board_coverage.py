"""Preflight board-coverage finding: a board with no active row, split by why.

A board whose every registration sits in pending is only a defect when a conflict resolution
emptied it. The same shape is the normal state for a deliberately parked source, so the two cases
are counted separately -- otherwise a real coverage gap hides inside several hundred expected rows,
which is exactly how 452 parked rows once read as 452 stranded boards.

Board identity is matched on host rather than the studio label, because labels drift: one board is
registered as both ``Lost Boys Interactive`` and ``Lost Boys Interactive (Embracer Group)``, and a
label-keyed test calls that two studios and reports a covered board as stranded.
"""

from __future__ import annotations

from typing import Any

from tools.repo_health.source_registry_preflight import (
    CONFLICT_DEMOTE_REASONS,
    _host_is_covered,
    board_host,
    find_boards_without_active_row,
)

CONFLICT = "registry_conflict_safe_auto_demote"
ADJUDICATION = "registry_conflict_adjudication_auto_demote"
ZERO_JOBS = "repeated_zero_jobs"
FETCH_FAILURE = "fetch_failure_demote"


def _row(source_id: str, url: str, reason: str = "") -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": source_id,
        "studio": "Some Studio",
        "adapter": "static",
        "listing_url": url,
    }
    if reason:
        row["pendingReason"] = reason
    return row


def test_board_host_is_www_insensitive_and_prefers_board_url() -> None:
    assert board_host({"board_url": "https://WWW.Example.com/careers"}) == "example.com"
    assert board_host({"listing_url": "https://www.example.com/jobs"}) == "example.com"
    # A board_url wins over listing_url, matching how the rest of the repo keys a row.
    assert (
        board_host({"board_url": "https://a.example/x", "listing_url": "https://b.example/y"})
        == "a.example"
    )
    assert board_host({}) == ""


def test_conflict_demoted_board_is_reported_as_emptied() -> None:
    active = [_row("active:1", "https://covered.example/careers")]
    pending = [
        _row("static:emptied", "https://stranded.example/careers", CONFLICT),
        _row("static:emptied2", "https://stranded2.example/careers", ADJUDICATION),
    ]

    result = find_boards_without_active_row(active, pending)

    assert result["boardNoActiveRowConflictDemoted"] == ["static:emptied", "static:emptied2"]
    assert result["boardNoActiveRowParked"] == []


def test_parked_reasons_are_counted_separately_and_never_as_defects() -> None:
    active = [_row("active:1", "https://covered.example/careers")]
    pending = [
        _row("static:zero", "https://zero.example/careers", ZERO_JOBS),
        _row("static:fail", "https://fail.example/careers", FETCH_FAILURE),
        _row("static:bare", "https://bare.example/careers"),
    ]

    result = find_boards_without_active_row(active, pending)

    assert result["boardNoActiveRowConflictDemoted"] == []
    assert result["boardNoActiveRowParked"] == ["static:bare", "static:fail", "static:zero"]


def test_a_pending_row_sharing_a_host_with_an_active_row_is_covered() -> None:
    active = [_row("jazzhr:1", "https://lostboys.example/apply")]
    pending = [_row("static:1", "https://lostboys.example/apply", CONFLICT)]

    result = find_boards_without_active_row(active, pending)

    assert result["boardNoActiveRowConflictDemoted"] == []


def test_apex_and_careers_subdomain_count_as_the_same_board() -> None:
    """One board split across hosts must not read as two, in either direction."""
    active = [_row("teamtailor:1", "https://careers.10chambers.com/jobs")]
    pending = [_row("static:1", "https://10chambers.com", CONFLICT)]

    result = find_boards_without_active_row(active, pending)

    assert result["boardNoActiveRowConflictDemoted"] == []


def test_suffix_containment_is_bidirectional_and_not_a_public_suffix_truncation() -> None:
    assert _host_is_covered("example.com", {"careers.example.com"})
    assert _host_is_covered("careers.example.com", {"example.com"})
    # Two different co.uk studios must not collapse onto each other via the shared suffix.
    assert not _host_is_covered("foo.co.uk", {"bar.co.uk"})


def test_rows_without_a_usable_host_are_ignored() -> None:
    active = [_row("active:1", "https://covered.example/careers")]
    pending = [
        {"id": "static:nourl", "studio": "x", "pendingReason": CONFLICT},
        {"id": "", "listing_url": "https://stranded.example/", "pendingReason": CONFLICT},
    ]

    result = find_boards_without_active_row(active, pending)

    assert result["boardNoActiveRowConflictDemoted"] == []
    assert result["boardNoActiveRowParked"] == []


def test_conflict_reasons_are_the_two_documented_demotions() -> None:
    assert CONFLICT in CONFLICT_DEMOTE_REASONS
    assert ADJUDICATION in CONFLICT_DEMOTE_REASONS
    assert ZERO_JOBS not in CONFLICT_DEMOTE_REASONS
    assert FETCH_FAILURE not in CONFLICT_DEMOTE_REASONS
