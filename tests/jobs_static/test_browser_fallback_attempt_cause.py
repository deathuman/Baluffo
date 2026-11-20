"""The static runner records the cause captured for its own fallback attempt.

The breaker carries the cause on the attempt result. The runner writes it beside
``browserFallbackAttemptError``, so the pipeline can keep two different environment
failures in one run from collapsing onto the last one.
"""

from __future__ import annotations

import pytest

import src.jobs_fetcher as jf
from src.jobs.adapters.static_listing_runner import StaticFetchRunner
from src.jobs.adapters.static_runtime import StaticRunDeps
from src.jobs.browser_fallback import BrowserFallbackCircuitBreaker, BrowserFallbackResult
from src.jobs.common.contracts_fetch_report import normalize_fetch_report_payload
from src.jobs.pipeline_source_loop import _stamp_browser_fallback_cause
from tests.jobs_static.test_static_zero_kept_guard import _make_static_context

_REFUSAL_TEXT = "browser fallback unavailable (cooldown active)"
_CAUSE = "Executable doesn't exist at /usr/bin/chromium"
_EMPTY_PAGE_TEXT = "browser fallback returned empty content"


def _runner(try_playwright) -> StaticFetchRunner:
    ctx = _make_static_context(fetch_text=lambda _url, _timeout: "")
    ctx.run_deps = StaticRunDeps(
        fetch_text=ctx.run_deps.fetch_text,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        try_playwright=try_playwright,
    )
    return StaticFetchRunner(ctx)


def test_the_runner_records_the_attempt_cause_beside_its_error() -> None:
    runner = _runner(lambda _url, _budget: BrowserFallbackResult(("", _REFUSAL_TEXT), _CAUSE))

    with pytest.raises(RuntimeError):
        runner._sync_browser_fallback("https://x.test/careers", "HTTP 403 forbidden")

    assert runner.entry_report["browserFallbackAttempted"] is True
    assert runner.entry_report["browserFallbackAttemptError"] == _REFUSAL_TEXT
    assert runner.entry_report["browserFallbackLastError"] == _CAUSE


def test_a_page_empty_attempt_does_not_claim_a_lane_cause() -> None:
    runner = _runner(lambda _url, _budget: BrowserFallbackResult(("", _EMPTY_PAGE_TEXT), _CAUSE))

    with pytest.raises(RuntimeError):
        runner._sync_browser_fallback("https://x.test/careers", "HTTP 403 forbidden")

    assert runner.entry_report["browserFallbackAttemptError"] == _EMPTY_PAGE_TEXT
    assert "browserFallbackLastError" not in runner.entry_report


def test_a_refused_static_row_keeps_its_attempt_cause_after_normalization() -> None:
    """The row's own cause survives the runner, stamping and report normalization.

    The breaker is reopened later in the run, so its live error is gone by the time the
    pipeline stamps the rows. The only surviving evidence is the cause the runner wrote
    beside its attempt error; normalization must hoist that detail, not drop it.
    """
    breaker = BrowserFallbackCircuitBreaker(cooldown_minutes=30)
    breaker.wrap(lambda _url, _budget: ("", _CAUSE))("https://seed.test/jobs", 5)
    assert not breaker.is_available()

    source = {
        "id": "static:refused-check",
        "name": "Refused Check",
        "studio": "Refused Check",
        "adapter": "static",
        "pages": ["https://refused.test/careers"],
        "enabledByDefault": True,
    }
    jf.SOURCE_DIAGNOSTICS.clear()

    def refuse_fetch(_url: str, _timeout: int) -> str:
        raise RuntimeError("HTTP 403 forbidden")

    def browser_must_not_run(_url: str, _timeout: int) -> tuple[str, str]:
        raise AssertionError("a refused fallback reached the browser lane")

    rows = jf.run_static_studio_pages_source(
        fetch_text=refuse_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
        sources=[source],
        try_playwright=breaker.wrap(browser_must_not_run),
    )
    assert rows == []

    details = jf.SOURCE_DIAGNOSTICS["static_studio_pages"]["details"]
    assert len(details) == 1
    detail = details[0]
    assert detail["browserFallbackAttempted"] is True
    assert detail["browserFallbackAttemptError"] == _REFUSAL_TEXT
    assert detail["browserFallbackLastError"] == _CAUSE

    # A successful mid-run health probe reopens the lane and clears the breaker's copy.
    breaker.record_health_probe_success()
    assert breaker.last_error == ""
    assert breaker.last_refusal_reason == ""

    row = {
        "name": source["name"],
        "adapter": "static",
        "status": "error",
        "error": "HTTP 403 forbidden",
        "keptCount": 0,
        "details": [detail],
    }
    _stamp_browser_fallback_cause([row], breaker)
    # The recovered breaker has nothing to stamp; the per-attempt cause is only in detail.
    assert "browserFallbackLastError" not in row

    normalized = normalize_fetch_report_payload({"sources": [row]})["sources"][0]
    assert normalized["browserFallbackAttempted"] is True
    assert normalized["browserFallbackAttemptError"] == _REFUSAL_TEXT
    assert normalized["browserFallbackLastError"] == _CAUSE
    assert "browserFallbackRunLastError" not in normalized, (
        "a recovered run has no run-level cause; the row must not claim one"
    )
