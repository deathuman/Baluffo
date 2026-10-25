"""The second way a probe retires: a validator that cannot survive its read.

A board that recomputes its tag per request answers a conditional 200 with one tag and then
publishes another in the read that follows, so no stored tag can ever match. These tests pin
the pair the two flows record, the third-value rule that keeps an alternating board on its
probe, and the two-window confirmation the state update applies.

Split from ``test_conditional_revalidation.py`` to stay inside the test line budget.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.jobs import transport
from src.jobs.state_source_state import update_source_state_rows


def _apply_state(
    rows: dict[str, Any], report: dict[str, Any], *, finished_at: str
) -> dict[str, Any]:
    return update_source_state_rows(
        source_state_rows=rows,
        source_reports=[report],
        canonical_rows=[],
        finished_at=finished_at,
        circuit_breaker_failures=3,
        circuit_breaker_cooldown_minutes=720,
    )


def _probe_then_read(
    name: str, *, sent: str, probe: str, read: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """One window of a conditional ask followed by the read it sent the source into."""
    entry: dict[str, Any] = {"name": name, "status": "ok"}
    transport.apply_conditional_revalidate_result(
        entry,
        {"statusCode": 200, "notModified": False, "etag": probe, "lastModified": ""},
        sent_etag=sent,
    )
    transport.apply_full_read_response_headers(
        entry, {"status": "200", "etag": read, "last-modified": ""}
    )
    return {"name": name, "adapter": "greenhouse", "status": "ok", "details": [entry]}, entry


def test_a_validator_that_churns_between_probe_and_read_retires_the_probe() -> None:
    """A third tag seconds later means no stored tag can match, so the HEAD is pure cost."""
    name = "Churn Feed"
    first_at = datetime.now(UTC).isoformat()
    first, entry = _probe_then_read(name, sent='"old"', probe='"p1"', read='"r1"')
    assert entry["httpProbeAsked"] is True
    rows = _apply_state({}, first, finished_at=first_at)

    assert rows[name]["lastHttpProbeUnstableAt"] == entry["httpProbeUnstableAt"]
    assert "lastHttpProbeIgnoredAt" not in rows[name], (
        "one publish between the two requests looks the same as churn"
    )

    # The same report is still one window, not a confirmation.
    rows = _apply_state(rows, first, finished_at=first_at)
    assert "lastHttpProbeIgnoredAt" not in rows[name]

    second, second_entry = _probe_then_read(name, sent='"r1"', probe='"p2"', read='"r2"')
    rows = _apply_state(rows, second, finished_at=second_entry["httpProbeUnstableAt"])

    assert rows[name]["lastHttpProbeIgnoredAt"] == second_entry["httpProbeUnstableAt"]
    assert transport.probe_retirement_active(rows[name])

    cleared = {
        "name": name,
        "adapter": "greenhouse",
        "status": "excluded",
        "details": [
            {
                "name": name,
                "status": "excluded",
                "httpStatus": 304,
                "clearHttpProbeIgnored": True,
                "clearHttpProbeUnstable": True,
            }
        ],
    }
    rows = _apply_state(rows, cleared, finished_at=second_entry["httpProbeUnstableAt"])

    assert "lastHttpProbeUnstableAt" not in rows[name]
    assert "lastHttpProbeIgnoredAt" not in rows[name], "a 304 gives the probe back"


def test_a_board_that_only_alternates_between_edges_keeps_its_probe() -> None:
    """The read answered with the tag that was asked about: an edge flip, not churn."""
    name = "Two Edge Feed"
    rows: dict[str, Any] = {}
    for _ in range(2):
        report, _entry = _probe_then_read(name, sent='"edge-a"', probe='"edge-b"', read='"edge-a"')
        rows = _apply_state(rows, report, finished_at=datetime.now(UTC).isoformat())
        assert "lastHttpProbeUnstableAt" not in rows[name], (
            "a board that still answers 304 from its other edge is not retired"
        )


def test_a_validator_that_survives_the_read_is_an_ordinary_change() -> None:
    """One publish, then stability: the next window asks with the new tag and 304s."""
    name = "Published Once Feed"
    report, entry = _probe_then_read(name, sent='"old"', probe='"new"', read='"new"')
    assert "httpProbeUnstableAt" not in entry
    assert entry["clearHttpProbeUnstable"] is True

    rows = _apply_state({}, report, finished_at=datetime.now(UTC).isoformat())
    assert "lastHttpProbeUnstableAt" not in rows[name]


def test_a_head_that_withholds_its_tag_is_not_churn() -> None:
    """The GET publishes the validator the HEAD refused to; the pair is not a third value."""
    name = "HEAD Blind Feed"
    entry: dict[str, Any] = {"name": name, "status": "ok"}
    transport.apply_conditional_revalidate_result(
        entry,
        {"statusCode": 200, "notModified": False, "etag": "", "lastModified": ""},
        sent_etag='"stored"',
    )
    transport.apply_full_read_response_headers(
        entry, {"status": "200", "etag": '"from-the-read"', "last-modified": ""}
    )

    assert entry["httpProbeEtag"] == ""
    assert "httpProbeUnstableAt" not in entry
    assert entry["clearHttpProbeUnstable"] is True


def test_a_read_without_a_probe_records_nothing_about_stability() -> None:
    entry: dict[str, Any] = {"name": "Plain Read Feed", "status": "ok"}
    transport.apply_full_read_response_headers(
        entry, {"status": "200", "etag": '"read-only"', "last-modified": ""}
    )

    assert "httpProbeUnstableAt" not in entry
    assert "clearHttpProbeUnstable" not in entry, (
        "a run_now read is not evidence about a probe that never ran"
    )
