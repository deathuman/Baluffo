"""A refusal records why, and an "empty" render says which kind of empty it was.

Two findings from the v8 full-seed replay (843 fallback attempts, 716 refused, 6 served
empty) made the browser-fallback population unreadable, and both are the same mistake in
different clothes: one number standing in for two opposite findings.

- **A refusal had no recorded reason.** The breaker returned "browser fallback unavailable
  (cooldown active)" and wrote nothing, so a closed breaker and a missing browser were the
  same event as far as the report was concerned.
- **`demand_served_empty` did not say which empty it was.** A browser that could not launch
  (an environment failure, and the only thing that trips the cooldown) was counted beside a
  page that rendered with no jobs in it (a fact about the board).

`last_error` keeps meaning the environment cause and is never overwritten by a refusal -- it
is the only thing that explains why the breaker is closed. The refusal reason lives beside it.

The run-level cause is then stamped onto rows that *recommend* fallback, because a row
recording a need and a breaker recording an outcome were never visible together: the 8 ashby
boards that errored with "no jobs extracted from ashby board html" while asking for fallback
read as boards nobody had tried.
"""

from __future__ import annotations

from typing import Any

from src.jobs.browser_fallback import (
    BROWSER_FALLBACK_STATE_KEY,
    REFUSAL_COOLDOWN,
    BrowserFallbackCircuitBreaker,
)
from src.jobs.common.contracts_runtime import normalize_runtime_payload
from src.jobs.pipeline_source_loop import _stamp_browser_fallback_cause

_ENVIRONMENT_ERROR = "Executable doesn't exist at C:\\Users\\x\\chromium"
_EMPTY_PAGE_TEXT = "browser fallback returned empty content"


def _tripped(cooldown_minutes: int = 30) -> BrowserFallbackCircuitBreaker:
    """A breaker already closed by an environment failure, as a run's would be."""
    breaker = BrowserFallbackCircuitBreaker(cooldown_minutes=cooldown_minutes)
    breaker.wrap(lambda url, timeout: ("", _ENVIRONMENT_ERROR))("https://x.test/jobs", 5)
    return breaker


# --- the refusal records its reason, without erasing the cause -------------------------


def test_a_refusal_records_the_reason() -> None:
    breaker = _tripped()
    html, error = breaker.wrap(lambda url, timeout: ("", ""))("https://x.test/jobs", 5)
    assert html == ""
    assert "cooldown" in error
    assert breaker.demand_refused == 1
    assert breaker.last_refusal_reason == REFUSAL_COOLDOWN
    assert breaker.last_refused_at


def test_a_refusal_does_not_overwrite_the_environment_cause() -> None:
    """`last_error` is why the breaker is closed. Replacing it with "cooldown active"
    would leave a closed breaker with no explanation."""
    breaker = _tripped()
    cause_before = breaker.last_error
    breaker.wrap(lambda url, timeout: ("", ""))("https://x.test/jobs", 5)
    assert cause_before == _ENVIRONMENT_ERROR
    assert breaker.last_error == cause_before


def test_an_environment_failure_clears_a_stale_refusal_reason() -> None:
    """Otherwise a run's report reads "refused: cooldown" for a fresh environment failure.

    The sequence is the real one: a run trips the breaker, a later run is refused while the
    cooldown holds, and once it expires the browser fails to launch again -- at which point
    "refused: cooldown" is stale and would misreport the cause.
    """
    breaker = _tripped()
    breaker.disabled_until_at = "2000-01-01T00:00:00+00:00"  # cooldown expired
    breaker.wrap(lambda url, timeout: ("", ""))("https://x.test/jobs", 5)
    breaker.disabled_until_at = "2000-01-01T00:00:00+00:00"
    breaker.wrap(lambda url, timeout: ("", _ENVIRONMENT_ERROR))("https://y.test/jobs", 5)
    assert breaker.last_error == _ENVIRONMENT_ERROR
    assert breaker.last_refusal_reason == ""


def test_the_refusal_fields_survive_a_state_round_trip() -> None:
    breaker = _tripped()
    breaker.wrap(lambda url, timeout: ("", ""))("https://x.test/jobs", 5)
    row = breaker.to_state_row()
    assert row["browserFallbackLastRefusalReason"] == REFUSAL_COOLDOWN
    assert row["browserFallbackLastRefusedAt"]
    restored = BrowserFallbackCircuitBreaker.from_state(
        {BROWSER_FALLBACK_STATE_KEY: row}, cooldown_minutes=30
    )
    assert restored.last_refusal_reason == REFUSAL_COOLDOWN
    assert restored.last_refused_at == breaker.last_refused_at
    assert restored.last_error == breaker.last_error


# --- "empty" says which empty ----------------------------------------------------------


def test_a_content_empty_render_is_a_page_finding_and_does_not_cooldown() -> None:
    breaker = BrowserFallbackCircuitBreaker()
    breaker.wrap(lambda url, timeout: ("", "browser fallback returned empty content"))(
        "https://x.test/jobs", 5
    )
    assert breaker.demand_served_empty == 1
    assert breaker.demand_served_empty_page == 1
    assert breaker.demand_served_empty_environment == 0
    assert breaker.is_available()


def test_a_launch_failure_is_an_environment_finding_and_does_cooldown() -> None:
    breaker = _tripped()
    assert breaker.demand_served_empty == 1
    assert breaker.demand_served_empty_environment == 1
    assert breaker.demand_served_empty_page == 0
    assert not breaker.is_available()


def test_a_successful_render_counts_neither_empty_bucket() -> None:
    breaker = BrowserFallbackCircuitBreaker()
    breaker.wrap(lambda url, timeout: ("<html>jobs</html>", ""))("https://x.test/jobs", 5)
    assert breaker.demand_pages == 1
    assert breaker.demand_served_empty_environment == 0
    assert breaker.demand_served_empty_page == 0


def test_the_split_adds_up_to_the_total() -> None:
    """A split that does not sum is a second way of losing the population."""
    breaker = BrowserFallbackCircuitBreaker()
    wrapped = breaker.wrap(lambda url, timeout: ("", "browser fallback returned empty content"))
    wrapped("https://x.test/1", 5)
    breaker.wrap(lambda url, timeout: ("", _ENVIRONMENT_ERROR))("https://x.test/2", 5)
    breaker.wrap(lambda url, timeout: ("<html>ok</html>", ""))("https://x.test/3", 5)
    assert (
        breaker.demand_served_empty_environment + breaker.demand_served_empty_page
        == breaker.demand_served_empty
    )
    assert breaker.demand_attempts == breaker.demand_refused + breaker.demand_pages
    assert breaker.demand_pages == (breaker.demand_served_with_html + breaker.demand_served_empty)


# --- the pipeline surfaces both --------------------------------------------------------


def test_the_run_payload_carries_the_split() -> None:
    payload: dict[str, Any] = {
        "browserFallbackDemand": {
            "attempts": 843,
            "refused": 716,
            "pages": 127,
            "servedWithHtml": 121,
            "servedEmpty": 6,
            "servedEmptyEnvironment": 1,
            "servedEmptyPage": 5,
        }
    }
    normalized = normalize_runtime_payload(payload, selected_source_count=10)
    demand = normalized["browserFallbackDemand"]
    assert demand["pages"] == 127
    assert demand["servedEmptyEnvironment"] == 1
    assert demand["servedEmptyPage"] == 5


def test_the_split_is_clamped_and_junk_is_zero_not_raised() -> None:
    payload: dict[str, Any] = {
        "browserFallbackDemand": {
            "servedEmptyEnvironment": "not a number",
            "servedEmptyPage": -4,
        }
    }
    demand = normalize_runtime_payload(payload, selected_source_count=1)["browserFallbackDemand"]
    assert demand["servedEmptyEnvironment"] == 0
    assert demand["servedEmptyPage"] == 0


def test_a_report_missing_the_split_still_normalizes() -> None:
    """A report written before this change must not lose its demand block."""
    demand = normalize_runtime_payload(
        {"browserFallbackDemand": {"attempts": 10, "refused": 2}}, selected_source_count=1
    )["browserFallbackDemand"]
    assert demand["attempts"] == 10
    assert demand["pages"] == 8
    assert demand["servedEmptyEnvironment"] == 0
    assert demand["servedEmptyPage"] == 0


# --- the run's cause reaches the rows that asked for it --------------------------------


def test_the_cause_lands_on_rows_that_recommend_fallback() -> None:
    rows: list[dict[str, Any]] = [
        {"name": "needy", "browserFallbackRecommended": True},
        {"name": "content", "browserFallbackRecommended": False},
    ]
    _stamp_browser_fallback_cause(rows, _tripped())
    assert rows[0]["browserFallbackRunLastError"] == _ENVIRONMENT_ERROR
    assert "browserFallbackRunLastError" not in rows[1]
    # A recommendation is a need, not an attempt: this row must not claim a row-level cause.
    assert "browserFallbackLastError" not in rows[0]


def test_a_refusal_reason_reaches_the_rows_too() -> None:
    breaker = _tripped()
    breaker.wrap(lambda url, timeout: ("", ""))("https://x.test/jobs", 5)
    rows: list[dict[str, Any]] = [{"browserFallbackRecommended": True}]
    _stamp_browser_fallback_cause(rows, breaker)
    assert rows[0]["browserFallbackRunRefusalReason"] == REFUSAL_COOLDOWN


def test_a_refused_rows_own_record_carries_the_cause() -> None:
    """The v8 replay's 988 refusals said "cooldown active" and nothing else.

    The run-level field was already there; the row that actually hit the cooldown was not,
    which left the refusal unattributable from the record it appears in.
    """
    breaker = _tripped()
    breaker.wrap(lambda url, timeout: ("", ""))("https://x.test/jobs", 5)
    rows: list[dict[str, Any]] = [
        {
            "name": "refused",
            "browserFallbackRecommended": True,
            "browserFallbackAttempted": True,
            "browserFallbackAttemptError": "browser fallback unavailable (cooldown active)",
        }
    ]
    _stamp_browser_fallback_cause(rows, breaker)
    assert rows[0]["browserFallbackLastError"] == _ENVIRONMENT_ERROR
    # The run-level fields keep their own meaning and are still written beside it.
    assert rows[0]["browserFallbackRunLastError"] == _ENVIRONMENT_ERROR
    assert rows[0]["browserFallbackRunRefusalReason"] == REFUSAL_COOLDOWN


def test_an_attempted_row_that_never_recommended_still_gets_the_cause() -> None:
    """Escalation paths attempt fallback without setting the recommendation flag."""
    rows: list[dict[str, Any]] = [
        {
            "name": "escalated",
            "browserFallbackAttempted": True,
            "browserFallbackAttemptError": "browser fallback unavailable (environment blocked)",
        }
    ]
    _stamp_browser_fallback_cause(rows, _tripped())
    assert rows[0]["browserFallbackLastError"] == _ENVIRONMENT_ERROR


def test_a_served_attempt_does_not_inherit_the_runs_cause() -> None:
    """An attempt that rendered records no error, so there is no per-row cause to carry."""
    rows: list[dict[str, Any]] = [
        {
            "name": "served",
            "browserFallbackAttempted": True,
            "browserFallbackUsed": True,
            "browserFallbackAttemptError": "",
        }
    ]
    _stamp_browser_fallback_cause(rows, _tripped())
    assert "browserFallbackLastError" not in rows[0]
    assert rows[0]["browserFallbackRunLastError"] == _ENVIRONMENT_ERROR


def test_a_detail_level_attempt_error_is_enough_to_carry_the_cause() -> None:
    rows: list[dict[str, Any]] = [
        {
            "name": "family",
            "details": [
                {
                    "browserFallbackAttempted": True,
                    "browserFallbackAttemptError": (
                        "browser fallback unavailable (cooldown active)"
                    ),
                }
            ],
        }
    ]
    _stamp_browser_fallback_cause(rows, _tripped())
    assert rows[0]["browserFallbackLastError"] == _ENVIRONMENT_ERROR


def test_a_detail_level_recommendation_is_enough_to_stamp() -> None:
    """The v9 false green: 95 rows recommended fallback, 0 got the run's cause.

    The flag is written on the row's **detail** during the fetch and hoisted to the top level
    later, by report normalization. Reading only the top level stamped nothing at all.
    """
    rows: list[dict[str, Any]] = [
        {
            "name": "gismart",
            "details": [
                {"name": "Gismart (Sheet)", "browserFallbackRecommended": True, "status": "error"}
            ],
        }
    ]
    _stamp_browser_fallback_cause(rows, _tripped())
    assert rows[0]["browserFallbackRunLastError"] == _ENVIRONMENT_ERROR


def test_a_top_level_recommendation_still_counts() -> None:
    rows: list[dict[str, Any]] = [{"browserFallbackRecommended": True, "details": []}]
    _stamp_browser_fallback_cause(rows, _tripped())
    assert rows[0]["browserFallbackRunLastError"] == _ENVIRONMENT_ERROR


def test_a_healthy_run_stamps_nothing() -> None:
    rows: list[dict[str, Any]] = [{"browserFallbackRecommended": True}]
    _stamp_browser_fallback_cause(rows, BrowserFallbackCircuitBreaker())
    assert rows == [{"browserFallbackRecommended": True}]


def test_no_guard_or_no_rows_is_not_an_error() -> None:
    _stamp_browser_fallback_cause(None, _tripped())
    _stamp_browser_fallback_cause([], None)


# --- a refused row keeps its cause when the lane recovers later in the run -------------

_REFUSAL_TEXT = "browser fallback unavailable (cooldown active)"


def test_the_refusal_cause_is_captured_at_the_refusal() -> None:
    breaker = _tripped()
    breaker.wrap(lambda url, timeout: ("", ""))("https://x.test/jobs", 5)
    assert breaker.last_empty_cause == _ENVIRONMENT_ERROR


def test_a_refused_row_keeps_its_cause_after_the_lane_recovers() -> None:
    """The stamp runs after the source loop, so it must not read the breaker's live state.

    The run trips the breaker at 09:00 and refuses 40 boards, a health probe reopens the lane,
    and the same run then renders pages. Reading `last_error` at report time answers with the
    recovered state and leaves every refused row saying "cooldown active" and nothing else.
    """
    breaker = _tripped()
    breaker.wrap(lambda url, timeout: ("", ""))("https://x.test/1", 5)
    breaker.record_health_probe_success()
    html, error = breaker.wrap(lambda url, timeout: ("<html>jobs</html>", ""))(
        "https://x.test/2", 5
    )
    assert (html, error) == ("<html>jobs</html>", "")
    assert breaker.last_error == ""
    assert breaker.last_refusal_reason == ""

    rows: list[dict[str, Any]] = [
        {
            "name": "refused",
            "browserFallbackAttempted": True,
            "browserFallbackAttemptError": _REFUSAL_TEXT,
        }
    ]
    _stamp_browser_fallback_cause(rows, breaker)
    assert rows[0]["browserFallbackLastError"] == _ENVIRONMENT_ERROR


def test_a_mid_run_probe_success_does_not_erase_the_cause() -> None:
    """The health probe reopens the breaker and clears its error; the cause stays stamped."""
    breaker = BrowserFallbackCircuitBreaker()
    breaker.mark_environment_blocked(_ENVIRONMENT_ERROR)
    breaker.record_health_probe_success()
    assert breaker.last_error == ""

    rows: list[dict[str, Any]] = [
        {
            "browserFallbackAttempted": True,
            "browserFallbackAttemptError": "browser fallback unavailable (environment blocked)",
        }
    ]
    _stamp_browser_fallback_cause(rows, breaker)
    assert rows[0]["browserFallbackLastError"] == _ENVIRONMENT_ERROR


def test_an_empty_page_row_is_not_given_the_runs_launch_error() -> None:
    """A page with no jobs in it is a finding about the board, not the browser.

    The two empties are split into `servedEmptyPage` and `servedEmptyEnvironment` precisely
    so one is not read as the other. Stamping the run's later launch error on the page-empty
    row put the conflation straight back into the per-row field.
    """
    breaker = BrowserFallbackCircuitBreaker()
    breaker.wrap(lambda url, timeout: ("", _EMPTY_PAGE_TEXT))("https://x.test/1", 5)
    breaker.wrap(lambda url, timeout: ("", _ENVIRONMENT_ERROR))("https://x.test/2", 5)
    assert breaker.demand_served_empty_page == 1
    assert breaker.demand_served_empty_environment == 1
    assert breaker.last_error == _ENVIRONMENT_ERROR

    rows: list[dict[str, Any]] = [
        {
            "name": "empty-page",
            "browserFallbackAttempted": True,
            "browserFallbackAttemptError": _EMPTY_PAGE_TEXT,
        }
    ]
    _stamp_browser_fallback_cause(rows, breaker)
    assert "browserFallbackLastError" not in rows[0]
    # The run-level field still describes the run, which did fail at the lane.
    assert rows[0]["browserFallbackRunLastError"] == _ENVIRONMENT_ERROR


def test_a_refused_row_beside_an_empty_page_row_keeps_its_cause() -> None:
    """One run, two rows, two different findings: only the refused row gets the cause."""
    breaker = _tripped()
    rows: list[dict[str, Any]] = [
        {
            "name": "empty-page",
            "browserFallbackAttempted": True,
            "browserFallbackAttemptError": _EMPTY_PAGE_TEXT,
        },
        {
            "name": "refused",
            "browserFallbackAttempted": True,
            "browserFallbackAttemptError": _REFUSAL_TEXT,
        },
    ]
    _stamp_browser_fallback_cause(rows, breaker)
    assert "browserFallbackLastError" not in rows[0]
    assert rows[1]["browserFallbackLastError"] == _ENVIRONMENT_ERROR


def test_the_captured_cause_is_not_persisted_into_the_next_run() -> None:
    """Persisted, it would explain a run that never had a fallback failure."""
    breaker = _tripped()
    breaker.wrap(lambda url, timeout: ("", ""))("https://x.test/jobs", 5)
    row = breaker.to_state_row()
    assert "browserFallbackLastEmptyCause" not in row

    restored = BrowserFallbackCircuitBreaker.from_state(
        {BROWSER_FALLBACK_STATE_KEY: row}, cooldown_minutes=30
    )
    assert restored.last_empty_cause == ""
    # The persisted breach itself still explains a refusal in the new run.
    assert restored.last_error == _ENVIRONMENT_ERROR
