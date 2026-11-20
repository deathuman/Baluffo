"""The static runner records the cause captured for its own fallback attempt.

The breaker carries the cause on the attempt result. The runner writes it beside
``browserFallbackAttemptError``, so the pipeline can keep two different environment
failures in one run from collapsing onto the last one.
"""

from __future__ import annotations

import pytest

from src.jobs.adapters.static_listing_runner import StaticFetchRunner
from src.jobs.adapters.static_runtime import StaticRunDeps
from src.jobs.browser_fallback import BrowserFallbackResult
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
