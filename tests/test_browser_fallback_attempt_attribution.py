"""Each refused fallback row keeps the cause captured for its own attempt.

The breaker used to keep a run-scoped copy that was last-wins, so two environment failures
in one run left an earlier refusal quoting the later cause. A fallback result now carries
the cause that was live for that call, and the static runner writes it beside the attempt
error. A row whose lane recorded no cause stays empty: the run's last failure is not
evidence about a different attempt.
"""

from __future__ import annotations

from typing import Any

from src.jobs.browser_fallback import BrowserFallbackCircuitBreaker, BrowserFallbackResult
from src.jobs.pipeline_source_loop import _stamp_browser_fallback_cause

_ENVIRONMENT_ERROR = "Executable doesn't exist at C:\\Users\\x\\chromium"
_SECOND_ENVIRONMENT_ERROR = "Browser has been closed"
_EMPTY_PAGE_TEXT = "browser fallback returned empty content"
_REFUSAL_TEXT = "browser fallback unavailable (cooldown active)"


def _tripped(cooldown_minutes: int = 30) -> BrowserFallbackCircuitBreaker:
    breaker = BrowserFallbackCircuitBreaker(cooldown_minutes=cooldown_minutes)
    breaker.wrap(lambda url, timeout: ("", _ENVIRONMENT_ERROR))("https://x.test/jobs", 5)
    return breaker


def test_a_refusal_result_carries_the_cause_that_was_live_for_that_attempt() -> None:
    breaker = _tripped()
    refused = breaker.wrap(lambda url, timeout: ("", ""))("https://x.test/1", 5)
    breaker.record_health_probe_success()
    breaker.wrap(lambda url, timeout: ("", _SECOND_ENVIRONMENT_ERROR))("https://x.test/2", 5)

    assert refused.lane_cause == _ENVIRONMENT_ERROR


def test_the_result_only_offers_a_cause_for_its_own_lane_error() -> None:
    assert (
        BrowserFallbackResult(("", _REFUSAL_TEXT), _ENVIRONMENT_ERROR).lane_cause
        == _ENVIRONMENT_ERROR
    )
    assert BrowserFallbackResult(("", _EMPTY_PAGE_TEXT), _ENVIRONMENT_ERROR).lane_cause == ""
    assert BrowserFallbackResult(("", _REFUSAL_TEXT), "").lane_cause == ""


def test_two_environment_failures_leave_each_refused_row_on_its_own_cause() -> None:
    """The first refusal must not be rewritten by a later, different environment failure."""
    breaker = _tripped()
    first_attempt = breaker.wrap(lambda url, timeout: ("", ""))("https://x.test/1", 5)
    first_row: dict[str, Any] = {
        "name": "first-refusal",
        "browserFallbackAttempted": True,
        "browserFallbackAttemptError": _REFUSAL_TEXT,
        "browserFallbackLastError": first_attempt.lane_cause,
    }

    breaker.record_health_probe_success()
    breaker.wrap(lambda url, timeout: ("", _SECOND_ENVIRONMENT_ERROR))("https://x.test/2", 5)
    second_attempt = breaker.wrap(lambda url, timeout: ("", ""))("https://x.test/3", 5)
    second_row: dict[str, Any] = {
        "name": "second-refusal",
        "browserFallbackAttempted": True,
        "browserFallbackAttemptError": _REFUSAL_TEXT,
        "browserFallbackLastError": second_attempt.lane_cause,
    }

    _stamp_browser_fallback_cause([first_row, second_row], breaker)

    assert first_row["browserFallbackLastError"] == _ENVIRONMENT_ERROR
    assert second_row["browserFallbackLastError"] == _SECOND_ENVIRONMENT_ERROR
    # The run-level fields still name the run's single latest cause.
    assert first_row["browserFallbackRunLastError"] == _SECOND_ENVIRONMENT_ERROR
    assert second_row["browserFallbackRunLastError"] == _SECOND_ENVIRONMENT_ERROR


def test_a_detail_level_attempt_cause_beats_the_runs_latest_cause() -> None:
    breaker = _tripped()
    rows: list[dict[str, Any]] = [
        {
            "name": "detail",
            "details": [
                {
                    "browserFallbackAttempted": True,
                    "browserFallbackAttemptError": _REFUSAL_TEXT,
                    "browserFallbackLastError": _ENVIRONMENT_ERROR,
                }
            ],
        }
    ]
    breaker.record_health_probe_success()
    breaker.wrap(lambda url, timeout: ("", _SECOND_ENVIRONMENT_ERROR))("https://x.test/2", 5)

    _stamp_browser_fallback_cause(rows, breaker)

    assert rows[0]["browserFallbackLastError"] == _ENVIRONMENT_ERROR


def test_an_attempt_without_its_own_cause_does_not_borrow_the_runs() -> None:
    """The run's last failure is last-wins, so it cannot explain a different attempt."""
    breaker = _tripped()
    rows: list[dict[str, Any]] = [
        {
            "browserFallbackAttempted": True,
            "browserFallbackAttemptError": _REFUSAL_TEXT,
        }
    ]

    _stamp_browser_fallback_cause(rows, breaker)

    assert "browserFallbackLastError" not in rows[0]
    assert rows[0]["browserFallbackRunLastError"] == _ENVIRONMENT_ERROR
