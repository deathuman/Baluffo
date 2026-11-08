"""Mid-run browser fallback recovery: a cooldown refusal is not final for the run.

The end-of-run health probe only helps the next run. These tests cover the
demand-triggered check that lets a long fetch reopen the breaker once a fresh
browser proves it can render, without relaunching a browser per refused source.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from src.jobs import pipeline_source_loop
from src.jobs.browser_fallback import REFUSAL_ERROR_TEXT, BrowserFallbackCircuitBreaker

_HEALTHY_PROBE = {"ok": True, "stage": "ready", "durationMs": 3}
_BROKEN_PROBE = {"ok": False, "stage": "launch", "error": "still broken", "durationMs": 2}


def _breaker_in_cooldown() -> BrowserFallbackCircuitBreaker:
    breaker = BrowserFallbackCircuitBreaker(cooldown_minutes=30)
    breaker.wrap(lambda _url, _timeout: ("", "browser fallback unavailable (spawn EPERM)"))(
        "https://board.example/careers", 5
    )
    assert not breaker.is_available()
    return breaker


class _StubPool:
    def __init__(self, health: dict[str, Any]) -> None:
        self._health = health
        self.calls = 0

    def health_probe(self) -> dict[str, Any]:
        self.calls += 1
        return dict(self._health)


def test_mid_run_probe_reopens_the_breaker_and_retries_the_refused_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages: list[str] = []
    monkeypatch.setattr(pipeline_source_loop, "emit_progress_line", messages.append)
    breaker = _breaker_in_cooldown()
    pool = _StubPool(_HEALTHY_PROBE)
    rendered: list[str] = []

    def try_playwright(url: str, _timeout: int) -> tuple[str, str]:
        rendered.append(url)
        return "<html>jobs</html>", ""

    guarded = pipeline_source_loop._with_mid_run_recovery(
        breaker.wrap(try_playwright),
        pipeline_source_loop._MidRunBrowserProbe(cast(Any, pool), breaker),
    )
    html, error = guarded("https://board.example/careers", 5)

    assert (html, error) == ("<html>jobs</html>", "")
    # The refused attempt never reached the browser; only the retry did.
    assert rendered == ["https://board.example/careers"]
    assert pool.calls == 1
    assert breaker.is_available()
    assert breaker.demand_refused == 1
    assert breaker.demand_served_with_html == 1
    assert any("browserMidRunRecovery ok=true" in message for message in messages)


def test_mid_run_probe_is_rate_limited_while_the_browser_stays_broken(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline_source_loop, "emit_progress_line", lambda _message: None)
    breaker = _breaker_in_cooldown()
    cooldown = breaker.disabled_until_at
    pool = _StubPool(_BROKEN_PROBE)
    now = [1000.0]
    probe = pipeline_source_loop._MidRunBrowserProbe(
        cast(Any, pool), breaker, interval_seconds=60, clock=lambda: now[0]
    )
    guarded = pipeline_source_loop._with_mid_run_recovery(
        breaker.wrap(lambda _url, _timeout: ("", "")), probe
    )

    assert guarded("https://a.example", 5) == ("", REFUSAL_ERROR_TEXT)
    assert pool.calls == 1
    now[0] += 30
    assert guarded("https://b.example", 5) == ("", REFUSAL_ERROR_TEXT)
    assert pool.calls == 1
    assert breaker.disabled_until_at == cooldown
    now[0] += 31
    assert guarded("https://c.example", 5) == ("", REFUSAL_ERROR_TEXT)
    assert pool.calls == 2


def test_mid_run_probe_ignores_page_errors_and_healthy_guards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline_source_loop, "emit_progress_line", lambda _message: None)
    breaker = BrowserFallbackCircuitBreaker(cooldown_minutes=30)
    pool = _StubPool(_HEALTHY_PROBE)
    probe = pipeline_source_loop._MidRunBrowserProbe(cast(Any, pool), breaker)
    guarded = pipeline_source_loop._with_mid_run_recovery(
        breaker.wrap(lambda _url, _timeout: ("", "page.goto: net::ERR_CONNECTION_RESET")),
        probe,
    )

    assert guarded("https://board.example/careers", 5) == (
        "",
        "page.goto: net::ERR_CONNECTION_RESET",
    )
    assert pool.calls == 0
    assert probe.probe_count == 0


def test_mid_run_probe_checks_a_persisted_cooldown_that_has_no_recorded_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline_source_loop, "emit_progress_line", lambda _message: None)
    breaker = BrowserFallbackCircuitBreaker(
        cooldown_minutes=30, disabled_until_at="2999-01-01T00:00:00+00:00"
    )
    pool = _StubPool(_HEALTHY_PROBE)

    # The end-of-run probe keeps its evidence gate: no recorded error, no probe.
    assert pipeline_source_loop._browser_health_probe(cast(Any, pool), breaker) is None
    assert pool.calls == 0

    probe = pipeline_source_loop._MidRunBrowserProbe(cast(Any, pool), breaker)
    assert probe.maybe_recover() is True
    assert pool.calls == 1
    assert breaker.is_available()


def test_mid_run_probe_recovers_the_capture_lane(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline_source_loop, "emit_progress_line", lambda _message: None)
    breaker = _breaker_in_cooldown()
    pool = _StubPool(_HEALTHY_PROBE)
    rendered: list[str] = []

    def try_capture(url: str, _timeout: int) -> tuple[str, str, list[tuple[str, str]]]:
        rendered.append(url)
        return "<html></html>", "", [("https://api.example/jobs", "{}")]

    guarded = pipeline_source_loop._with_mid_run_recovery_capture(
        breaker.wrap_capture(try_capture),
        pipeline_source_loop._MidRunBrowserProbe(cast(Any, pool), breaker),
    )
    html, error, payloads = guarded("https://board.example/careers", 5)

    assert html == "<html></html>"
    assert error == ""
    assert payloads == [("https://api.example/jobs", "{}")]
    assert rendered == ["https://board.example/careers"]


def test_mid_run_probe_failure_does_not_escape_the_fallback_wrapper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline_source_loop, "emit_progress_line", lambda _message: None)
    breaker = _breaker_in_cooldown()

    class ExplodingPool:
        def health_probe(self) -> dict[str, Any]:
            raise TypeError("probe exploded")

    probe = pipeline_source_loop._MidRunBrowserProbe(cast(Any, ExplodingPool()), breaker)
    guarded = pipeline_source_loop._with_mid_run_recovery(
        breaker.wrap(lambda _url, _timeout: ("", "")), probe
    )

    assert guarded("https://board.example/careers", 5) == ("", REFUSAL_ERROR_TEXT)
    assert probe.last_health is None


def test_mid_run_recovery_wiring_covers_both_fallback_lanes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline_source_loop, "emit_progress_line", lambda _message: None)
    breaker = _breaker_in_cooldown()
    pool = _StubPool(_HEALTHY_PROBE)

    html_lane, capture_lane = pipeline_source_loop._wire_mid_run_browser_recovery(
        cast(Any, pool),
        breaker,
        breaker.wrap(lambda _url, _timeout: ("<html>jobs</html>", "")),
        breaker.wrap_capture(lambda _url, _timeout: ("", "", [("https://api.example/jobs", "{}")])),
    )

    assert html_lane is not None
    assert capture_lane is not None
    assert html_lane("https://board.example/careers", 5) == ("<html>jobs</html>", "")
    # The first lane's probe reopened the shared breaker, so the second lane is served
    # directly -- one probe for the whole run, not one per lane.
    assert capture_lane("https://board.example/careers", 5)[1] == ""
    assert pool.calls == 1


def test_mid_run_recovery_wiring_is_a_noop_without_a_browser_pool() -> None:
    breaker = BrowserFallbackCircuitBreaker(cooldown_minutes=30)
    guarded = breaker.wrap(lambda _url, _timeout: ("", ""))

    html_lane, capture_lane = pipeline_source_loop._wire_mid_run_browser_recovery(
        None, breaker, guarded, None
    )

    assert html_lane is guarded
    assert capture_lane is None
