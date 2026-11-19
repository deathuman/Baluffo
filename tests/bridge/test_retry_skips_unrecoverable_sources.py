"""Retry must skip the failures a re-run cannot fix, and keep the rest.

The 2026-11-18 runs retried 556 static sources at once. 41 of those boards were
provably dead (host root and URL both 404) and another 90 registered URLs returned 404
on a live host: re-queuing them guaranteed another all-red run and hid the sources that
might actually recover. The class comes from the report row, so this is asserted at the
selector rather than in the launcher.
"""

from __future__ import annotations

from src.bridge.report_normalizer import failed_source_names_from_report
from src.shared.fetch_report_normalization_primitives import (
    normalize_jobs_fetch_report_source_row_base,
)

REPORT = {
    "sources": [
        {
            "name": "static_source::dead",
            "status": "error",
            "keptCount": 0,
            "error": "static_source::dead: HTTP 404 for https://gone.example/careers",
        },
        {
            "name": "static_source::blocked",
            "status": "error",
            "keptCount": 0,
            "error": "HTTP 403 Forbidden",
        },
        {
            "name": "static_source::slow",
            "status": "error",
            "keptCount": 0,
            "error": "request deadline exceeded (20.0s)",
        },
        {
            "name": "static_source::empty",
            "status": "error",
            "keptCount": 0,
            "error": "no jobs extracted from source pages",
        },
        {"name": "static_source::ok", "status": "ok", "keptCount": 4, "error": ""},
    ]
}


def test_retry_list_skips_dead_boards() -> None:
    assert failed_source_names_from_report(REPORT) == [
        "static_source::blocked",
        "static_source::empty",
        "static_source::slow",
    ]


def test_allowed_names_still_narrows_the_list() -> None:
    names = failed_source_names_from_report(
        REPORT, allowed_names={"static_source::blocked", "static_source::dead"}
    )
    assert names == ["static_source::blocked"]


def test_a_row_without_a_class_is_still_retried() -> None:
    report = {
        "sources": [
            {
                "name": "static_source::legacy",
                "status": "error",
                "keptCount": 0,
                "error": "something nobody has classified yet",
            }
        ]
    }
    assert failed_source_names_from_report(report) == ["static_source::legacy"]


def test_normalized_row_carries_the_class() -> None:
    row = normalize_jobs_fetch_report_source_row_base(
        {"status": "error", "keptCount": 0, "error": "HTTP 404 for https://gone.example/x"}
    )
    assert row["failureClass"] == "dead_board"
    assert row["status"] == "error"
