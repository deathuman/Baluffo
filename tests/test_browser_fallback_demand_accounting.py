"""Browser-fallback demand accounting (saturation visibility, 2026-09-15).

Every wrapped ``try_playwright`` call is demand; the circuit breaker refusing
one in cooldown used to surface only as a silent ``got_html=False``
escalation log line — 4,010 refused escalations in a single full pass with no
counter anywhere. The breaker now accounts attempts / refused / served-with-
html / served-empty, the pipeline stamps the snapshot into the runtime
payload (surfacing in the fetch report's ``runtime.browserFallbackDemand``),
and the contracts normalizer passes the block through clamped.
"""

from __future__ import annotations

from typing import Any

from src.jobs.browser_fallback import BrowserFallbackCircuitBreaker
from src.jobs.common.contracts_runtime import normalize_runtime_payload


def _breaker(**kwargs: Any) -> BrowserFallbackCircuitBreaker:
    return BrowserFallbackCircuitBreaker(**kwargs)


def test_no_demand_reports_zeroes() -> None:
    breaker = _breaker()
    assert breaker.demand_attempts == 0
    assert breaker.demand_refused == 0
    assert breaker.demand_served_with_html == 0
    assert breaker.demand_served_empty == 0


def test_served_with_html_counts_success() -> None:
    breaker = _breaker()
    wrapped = breaker.wrap(lambda url, timeout: (f"<html>{url}</html>", ""))
    html, error = wrapped("https://x.com/careers", 5)
    assert html
    assert error == ""
    assert breaker.demand_attempts == 1
    assert breaker.demand_refused == 0
    assert breaker.demand_served_with_html == 1
    assert breaker.demand_served_empty == 0


def test_served_empty_counts_failure_outcome() -> None:
    breaker = _breaker()
    wrapped = breaker.wrap(lambda url, timeout: ("", "browser fallback returned empty content"))
    html, error = wrapped("https://x.com/careers", 5)
    assert html == ""
    assert "empty" in error
    assert breaker.demand_attempts == 1
    assert breaker.demand_served_with_html == 0
    assert breaker.demand_served_empty == 1
    # A content-empty render is not an environment failure: no cooldown.
    assert breaker.is_available()


def test_refused_in_cooldown_counts_refused_demand() -> None:
    breaker = _breaker(cooldown_minutes=30)
    breaker.disabled_until_at = "2099-01-01T00:00:00+00:00"
    calls: list[str] = []

    def _must_not_run(url: str, timeout: int) -> tuple[str, str]:
        calls.append(url)
        return "", "should never run"

    wrapped = breaker.wrap(_must_not_run)
    html, error = wrapped("https://x.com/careers", 5)

    assert html == ""
    assert error == "browser fallback unavailable (cooldown active)"
    assert calls == []  # refused before the delegate ran
    assert breaker.demand_attempts == 1
    assert breaker.demand_refused == 1
    assert breaker.demand_served_with_html == 0
    assert breaker.demand_served_empty == 0


def test_mixed_run_accounting_adds_up() -> None:
    breaker = _breaker()
    outcomes = [
        ("a", ""),  # served with html
        ("", "browser fallback returned empty content"),  # served empty (content)
        ("b", ""),  # served with html
        ("", "browser has been closed"),  # served empty (environment failure → opens)
    ]
    calls = {"n": 0}

    def delegate(url: str, timeout: int) -> tuple[str, str]:
        html, err = outcomes[calls["n"]]
        calls["n"] += 1
        return html, err

    wrapped = breaker.wrap(delegate)
    for i in range(4):
        wrapped(f"https://x.com/{i + 1}", 5)
    assert breaker.demand_attempts == 4
    assert breaker.demand_served_with_html == 2
    assert breaker.demand_served_empty == 2
    assert breaker.demand_refused == 0
    assert breaker.is_available() is False  # the environment failure opened the breaker

    # Demand after the breaker opened is refused before reaching the delegate.
    html, error = wrapped("https://x.com/5", 5)
    assert html == ""
    assert error == "browser fallback unavailable (cooldown active)"
    assert calls["n"] == 4  # the delegate never ran for the refused call
    assert breaker.demand_attempts == 5
    assert breaker.demand_refused == 1
    # The invariant: attempts == refused + served.
    assert breaker.demand_attempts == (
        breaker.demand_refused + breaker.demand_served_with_html + breaker.demand_served_empty
    )


def test_runtime_payload_stamping_shape() -> None:
    """The pipeline stamps the breaker snapshot under browserFallbackDemand;
    the contracts normalizer passes it through clamped and drops junk."""
    payload: dict[str, Any] = {
        "maxWorkers": 4,
        "browserFallbackEnabled": True,
        "browserFallbackCap": 4,
        "browserFallbackDemand": {
            "attempts": 4030,
            "refused": 4010,
            "servedWithHtml": 20,
            "servedEmpty": 0,
        },
    }
    normalized = normalize_runtime_payload(payload, selected_source_count=3)
    demand = normalized["browserFallbackDemand"]
    assert demand == {
        "attempts": 4030,
        "refused": 4010,
        "servedWithHtml": 20,
        "servedEmpty": 0,
    }

    # Absent block → zeroed block (allowlist shape, not Optional).
    normalized_absent = normalize_runtime_payload({"maxWorkers": 4}, selected_source_count=3)
    assert normalized_absent["browserFallbackDemand"] == {
        "attempts": 0,
        "refused": 0,
        "servedWithHtml": 0,
        "servedEmpty": 0,
    }

    # Junk values clamp to 0, unknown keys dropped.
    normalized_junk = normalize_runtime_payload(
        {
            "browserFallbackDemand": {
                "attempts": "not-a-number",
                "refused": -5,
                "servedWithHtml": 7.0,
                "bogus": "x",
            }
        },
        selected_source_count=3,
    )
    assert normalized_junk["browserFallbackDemand"] == {
        "attempts": 0,
        "refused": 0,
        "servedWithHtml": 7,
        "servedEmpty": 0,
    }


def test_breaker_snapshot_matches_payload_stamping() -> None:
    """The fields the pipeline stamps are exactly the four demand counters."""
    breaker = _breaker()
    wrapped = breaker.wrap(lambda url, timeout: (f"<html>{url}</html>", ""))
    wrapped("https://x.com/1", 5)
    stamped = {
        "attempts": int(breaker.demand_attempts),
        "refused": int(breaker.demand_refused),
        "servedWithHtml": int(breaker.demand_served_with_html),
        "servedEmpty": int(breaker.demand_served_empty),
    }
    assert stamped == {"attempts": 1, "refused": 0, "servedWithHtml": 1, "servedEmpty": 0}
