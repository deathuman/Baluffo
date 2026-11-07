"""Browser fallback diagnostics: preserve raw launch/navigation failures.

The circuit breaker intentionally keeps only a normalized environment-error
token, so the pool keeps the original Playwright message/type separately. The
health probe then answers whether Chromium can still launch and navigate
without depending on any board URL.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

import src.jobs.browser_fallback_pool as pool_mod
from src.jobs import pipeline_source_loop
from src.jobs.browser_fallback_pool import BrowserFallbackPool
from src.jobs.common.contracts_runtime import normalize_runtime_payload


class _FailingFuture:
    def __init__(self, message: str) -> None:
        self._message = message

    def result(self, timeout: float | None = None) -> None:
        raise RuntimeError(self._message)


class _CompletedFuture:
    def result(self, timeout: float | None = None) -> None:
        return None


class _FakeLoop:
    def call_soon_threadsafe(self, callback: Any) -> None:
        callback()

    def close(self) -> None:
        return

    def stop(self) -> None:
        return


def test_fetch_preserves_original_launch_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fail_launch(_self: BrowserFallbackPool) -> None:
        raise RuntimeError("Executable doesn't exist at /ms-playwright/chromium-1200")

    monkeypatch.setattr(BrowserFallbackPool, "_start_browser", fail_launch)
    pool = BrowserFallbackPool()
    try:
        html, error = pool.fetch("https://example.com/careers", 1)
        metrics = pool.metrics.snapshot()
    finally:
        pool.close()

    assert html == ""
    assert error
    assert metrics["last_failure_stage"] == "launch"
    assert "Executable doesn't exist" in metrics["last_launch_error"]
    assert metrics["last_launch_error_type"] == "RuntimeError"
    # The launch failure must not be relabelled as a navigation failure.
    assert metrics["last_navigation_error"] == ""


def test_health_probe_reports_launch_failure_without_a_board(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_launch(_self: BrowserFallbackPool) -> None:
        raise RuntimeError("browserType.launch: missing system dependencies")

    monkeypatch.setattr(BrowserFallbackPool, "_start_browser", fail_launch)
    pool = BrowserFallbackPool()
    try:
        health = pool.health_probe(timeout_s=1)
    finally:
        pool.close()

    assert health["ok"] is False
    assert health["stage"] == "launch"
    assert health["errorType"] == "RuntimeError"
    assert "missing system dependencies" in health["error"]
    assert "missing system dependencies" in health["lastLaunchError"]
    assert health["lastNavigationError"] == ""
    assert health["probeUrl"].startswith("data:text/html")


def test_fetch_and_health_probe_preserve_navigation_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message = "page.goto: net::ERR_CONNECTION_RESET at https://board.example/careers"

    def fail_navigation(coro: Any, _loop: Any) -> _FailingFuture:
        coro.close()
        return _FailingFuture(message)

    monkeypatch.setattr(pool_mod.asyncio, "run_coroutine_threadsafe", fail_navigation)
    pool = BrowserFallbackPool()
    pool._ensure_started = lambda: None  # type: ignore[method-assign]
    pool._loop = _FakeLoop()  # type: ignore[assignment]
    try:
        html, error, captured = pool.fetch_captured("https://board.example/careers", 1)
        health = pool.health_probe(timeout_s=1)
    finally:
        pool.close()

    assert (html, captured) == ("", [])
    assert error
    assert health["ok"] is False
    assert health["stage"] == "navigation"
    assert "ERR_CONNECTION_RESET" in health["error"]
    assert "ERR_CONNECTION_RESET" in health["lastNavigationError"]
    assert health["lastNavigationErrorType"] == "RuntimeError"


def test_health_probe_reports_ready_and_keeps_prior_navigation_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def complete(coro: Any, _loop: Any) -> _CompletedFuture:
        coro.close()
        return _CompletedFuture()

    monkeypatch.setattr(pool_mod.asyncio, "run_coroutine_threadsafe", complete)
    pool = BrowserFallbackPool()
    pool._ensure_started = lambda: None  # type: ignore[method-assign]
    pool._loop = _FakeLoop()  # type: ignore[assignment]
    pool._record_failure("navigation", RuntimeError("page.goto: net::ERR_CERT_AUTHORITY_INVALID"))
    try:
        health = pool.health_probe(timeout_s=1)
        metrics = pool.metrics.snapshot()
    finally:
        pool.close()

    assert health["ok"] is True
    assert health["stage"] == "ready"
    assert health["error"] == ""
    assert health["lastNavigationError"] == "page.goto: net::ERR_CERT_AUTHORITY_INVALID"
    assert metrics["last_health_probe"]["ok"] is True


def test_health_probe_resets_an_unavailable_browser_under_the_start_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The health probe must not call the locked close helper unlocked.

    A stale unavailable read used to enter ``_hard_close_locked`` directly while
    a concurrent close or restart could already own the pool handoff.
    """

    def complete(coro: Any, _loop: Any) -> _CompletedFuture:
        coro.close()
        return _CompletedFuture()

    monkeypatch.setattr(pool_mod.asyncio, "run_coroutine_threadsafe", complete)
    pool = BrowserFallbackPool()
    pool._available = False
    typed_pool = cast(Any, pool)
    typed_pool._loop = _FakeLoop()
    typed_pool._ensure_started = lambda: None
    close_lock_states: list[bool] = []

    def record_close() -> None:
        close_lock_states.append(pool._start_lock.locked())

    monkeypatch.setattr(pool, "_hard_close_locked", record_close)
    try:
        health = pool.health_probe(timeout_s=1)
        assert close_lock_states == [True]
        assert health["ok"] is True
    finally:
        # The patched close is a no-op; detach the fake loop so atexit does not try
        # to schedule a real shutdown coroutine on it.
        pool.close()
        pool._loop = None
        pool._thread = None


def test_runtime_payload_normalizes_browser_health_diagnostics() -> None:
    payload = normalize_runtime_payload(
        {
            "browserFallbackHealth": {
                "ok": False,
                "stage": "navigation",
                "error": "page.goto: net::ERR_FAILED",
                "errorType": "Error",
                "durationMs": -3,
                "probeUrl": "data:text/html,marker",
                "lastFailureStage": "instrumentation",
                "lastFailureAt": "2026-11-06T12:00:00Z",
                "lastLaunchError": "",
                "lastLaunchErrorType": "",
                "lastNavigationError": "page.goto: net::ERR_FAILED",
                "lastNavigationErrorType": "Error",
            }
        },
        selected_source_count=0,
    )

    health = payload["browserFallbackHealth"]
    assert health["ok"] is False
    assert health["stage"] == "navigation"
    assert health["durationMs"] == 0
    assert health["probeUrl"] == "data:text/html,marker"
    assert health["lastNavigationErrorType"] == "Error"


def test_runtime_payload_omits_absent_browser_health() -> None:
    payload = normalize_runtime_payload({}, selected_source_count=0)
    assert "browserFallbackHealth" not in payload


def test_browser_health_probe_runs_only_after_environment_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages: list[str] = []
    monkeypatch.setattr(pipeline_source_loop, "emit_progress_line", messages.append)

    class FakePool:
        calls = 0

        def health_probe(self) -> dict[str, Any]:
            self.calls += 1
            return {
                "ok": False,
                "stage": "launch",
                "error": "Executable doesn't exist",
                "durationMs": 4,
                "lastFailureStage": "launch",
                "lastLaunchError": "Executable doesn't exist at /ms-playwright/chromium-1200",
                "lastNavigationError": "",
            }

    healthy_guard = SimpleNamespace(demand_served_empty_environment=0, last_error="")
    assert (
        pipeline_source_loop._browser_health_probe(cast(Any, FakePool()), cast(Any, healthy_guard))
        is None
    )
    assert messages == []

    failed_guard = SimpleNamespace(
        demand_served_empty_environment=1, last_error="browser launch failed"
    )
    pool = FakePool()
    health = pipeline_source_loop._browser_health_probe(cast(Any, pool), cast(Any, failed_guard))

    assert pool.calls == 1
    assert health is not None
    assert health["stage"] == "launch"
    assert len(messages) == 1
    assert "browserHealth ok=false stage=launch" in messages[0]
    assert "Executable doesn't exist" in messages[0]
    assert "lastFailureStage=launch" in messages[0]
    assert "lastFailure=" in messages[0]
    assert "/ms-playwright/chromium-1200" in messages[0]
