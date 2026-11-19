"""A failure class answers one question: can re-running this URL change anything?

The class is written per source row and read by Retry, so the two directions that
matter are tested here: a deterministic outcome (dead board, malformed URL, redirect
loop, provider signature) must be recognised from either the structured per-page
details or the aggregated error text, and a transient one (blocked, rate-limited,
timeout) must stay retryable even when a dead page appears alongside it.
"""

from __future__ import annotations

import pytest

from src.shared.failure_class import (
    ATS_MISREGISTERED,
    BLOCKED,
    DEAD_BOARD,
    INVALID_URL,
    NO_JOBS,
    RATE_LIMITED,
    REDIRECT_LOOP,
    TIMEOUT,
    classify_source_failure,
    is_retryable_failure_class,
    resolve_failure_class,
)


def _classify(**overrides: object) -> str:
    payload: dict[str, object] = {"status": "error", "kept_count": 0}
    payload.update(overrides)
    return classify_source_failure(**payload)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        ("static_source::x: static:X: https://x: HTTP 404 for https://x", DEAD_BOARD),
        ("HTTP 410 Gone", DEAD_BOARD),
        ("HTTP 403 forbidden", BLOCKED),
        ("HTTP 429 Too Many Requests", RATE_LIMITED),
        ("time budget exceeded (90s)", TIMEOUT),
        ("request deadline exceeded (20.0s)", TIMEOUT),
        (
            "HTML contains greenhouse signature — consider adapter reclassification",
            ATS_MISREGISTERED,
        ),
        ("Unsafe static redirect from https://x", REDIRECT_LOOP),
        ("Invalid IPv6 URL", INVALID_URL),
        ("no jobs extracted from source pages", NO_JOBS),
    ],
)
def test_error_text_maps_to_class(error: str, expected: str) -> None:
    assert _classify(error=error) == expected


@pytest.mark.parametrize("name", [DEAD_BOARD, ATS_MISREGISTERED, REDIRECT_LOOP, INVALID_URL])
def test_deterministic_classes_are_not_retryable(name: str) -> None:
    assert not is_retryable_failure_class(name)


@pytest.mark.parametrize("name", [BLOCKED, RATE_LIMITED, TIMEOUT, NO_JOBS])
def test_transient_classes_stay_retryable(name: str) -> None:
    assert is_retryable_failure_class(name)


def test_unclassified_rows_stay_retryable() -> None:
    assert is_retryable_failure_class("")
    assert is_retryable_failure_class("not-a-real-class")


def test_success_and_excluded_carry_no_class() -> None:
    assert classify_source_failure(status="ok", kept_count=5, error="HTTP 404 for one board") == ""
    assert classify_source_failure(status="excluded", kept_count=0, error="skip_fresh") == ""


def test_details_dominate_over_aggregated_error_text() -> None:
    details = [
        {"status": "error", "httpStatus": 404, "error": "HTTP 404"},
        {"status": "error", "httpStatus": 404, "error": "HTTP 404"},
        {"status": "error", "httpStatus": 200, "error": "request deadline exceeded"},
    ]
    assert _classify(error="HTTP 404; request deadline exceeded", details=details) == DEAD_BOARD


def test_one_dead_page_does_not_park_a_timing_out_source() -> None:
    details = [
        {"status": "error", "httpStatus": 404, "error": "HTTP 404"},
        {"status": "error", "error": "request deadline exceeded"},
        {"status": "error", "error": "request deadline exceeded"},
    ]
    assert _classify(details=details) == TIMEOUT


def test_one_dead_board_does_not_park_a_family_of_empty_boards() -> None:
    details = [
        {"status": "error", "httpStatus": 404, "error": "HTTP 404"},
        *({"status": "error", "httpStatus": 200, "error": "no jobs extracted"} for _ in range(5)),
    ]
    assert _classify(details=details) == NO_JOBS
    assert is_retryable_failure_class(NO_JOBS)


def test_a_404_in_the_source_error_outvotes_an_empty_detail_entry() -> None:
    # The 2026-11-18 report's shape: the only detail row says "no jobs extracted",
    # while the aggregated source error carries the HTTP 404 for the registered URL.
    details = [
        {"status": "error", "httpStatus": 200, "error": "no jobs extracted from source pages"}
    ]
    assert (
        _classify(
            error="static_source::x: HTTP 404 for https://gone.example/careers; no jobs extracted",
            details=details,
        )
        == DEAD_BOARD
    )


def test_a_summarized_error_is_not_counted_twice_against_the_details() -> None:
    # The error text and the detail row describe the same page. Counting both would
    # let this 404 outvote the five empty boards alongside it.
    details = [
        {"status": "error", "httpStatus": 404, "error": "HTTP 404"},
        *({"status": "error", "httpStatus": 200, "error": "no jobs extracted"} for _ in range(5)),
    ]
    assert _classify(error="HTTP 404; no jobs extracted", details=details) == NO_JOBS


def test_zero_yield_without_evidence_is_no_jobs() -> None:
    assert _classify(error="") == NO_JOBS


def test_failure_bucket_backfills_missing_error_text() -> None:
    assert _classify(error="", failure_bucket="anti_bot_or_challenge") == BLOCKED
    assert _classify(error="", failure_bucket="timeout") == TIMEOUT


def test_a_written_class_is_trusted() -> None:
    row = {"status": "error", "keptCount": 0, "error": "HTTP 404", "failureClass": TIMEOUT}
    assert resolve_failure_class(row) == TIMEOUT


def test_a_fresh_zero_yield_row_resolves_from_its_own_evidence() -> None:
    row = {"status": "error", "keptCount": 0, "error": "Invalid IPv6 URL"}
    assert resolve_failure_class(row) == INVALID_URL
