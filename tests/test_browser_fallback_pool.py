"""Tests for BrowserFallbackPool.

Drives the pool from worker threads through its public sync ``fetch()``
surface (exactly what the fetch ThreadPoolExecutor does in prod). Pool
internals (asyncio loop thread, Chromium launch) are exercised end-to-end
against a stdlib http.server on localhost.
"""

from __future__ import annotations

import gc
import sys
import threading
import warnings
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from src.jobs.browser_fallback import (
    REFUSAL_ERROR_TEXT,
    BrowserFallbackCircuitBreaker,
    is_browser_fallback_environment_error,
)
from src.jobs.browser_fallback_pool import (
    BrowserFallbackPool,
    browser_pool_enabled,
)
from src.shared.browser_launch_args import browser_fallback_chromium_args


class _FixtureServer(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        return

    def do_GET(self):
        if self.path.startswith("/set-cookie"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Set-Cookie", "session=token123; Path=/")
            self.end_headers()
            self.wfile.write(b"<html><body>cookie was set</body></html>")
            return
        if self.path.startswith("/echo-cookie"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            cookie = self.headers.get("Cookie") or "none"
            self.wfile.write(f"<html><body>cookie={cookie}</body></html>".encode())
            return
        if self.path.startswith("/slow"):
            import time

            time.sleep(3)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"<html><body>slow</body></html>")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(f"<html><body>page:{self.path}</body></html>".encode())


@pytest.fixture(scope="module")
def http_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FixtureServer)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    yield f"http://{host}:{port}"
    server.shutdown()
    server.server_close()


class _FakeBrowser:
    """The slice of Playwright's Browser the pool actually touches."""

    def __init__(self) -> None:
        self.closed = False

    def is_connected(self) -> bool:
        return not self.closed

    async def close(self) -> None:
        self.closed = True


class _TargetClosedError(Exception):
    """Playwright's error type by name, without importing Playwright here."""


def _install_fake_launcher(pool: BrowserFallbackPool) -> list[_FakeBrowser]:
    """Replace the Chromium launch with a fake so these cases stay fast."""
    launches: list[_FakeBrowser] = []

    async def start() -> None:
        browser = _FakeBrowser()
        launches.append(browser)
        pool._browser = browser
        pool._playwright = None

    pool._start_browser = start  # type: ignore[method-assign]
    return launches


def test_pool_replaces_a_disconnected_handle_before_use() -> None:
    # S2.2: a non-None handle is not evidence of a live browser. Chromium can close
    # underneath the pool and leave `_browser` set; the next start must notice.
    pool = BrowserFallbackPool()
    launches = _install_fake_launcher(pool)
    pool._ensure_started()
    launches[0].closed = True

    pool._ensure_started()

    assert len(launches) == 2, "a disconnected handle must be replaced, not reused"
    assert pool._browser is launches[1]
    pool.close()


def test_pool_relaunches_a_dead_browser_and_retries_the_fetch() -> None:
    pool = BrowserFallbackPool()
    launches = _install_fake_launcher(pool)
    attempts = {"count": 0}

    async def fetch(
        url: str, timeout_s: int, *, settle_ms: int
    ) -> tuple[str, list[tuple[str, str]]]:
        attempts["count"] += 1
        if attempts["count"] == 1:
            # The 0.3.062 shape: the browser is closed, and the failure arrives at
            # new_page before any navigation has been attempted.
            raise _TargetClosedError(
                "BrowserContext.new_page: Target page, context or browser has been closed"
            )
        return "<html>board</html>", []

    pool._fetch_captured = fetch  # type: ignore[method-assign]

    def health_probe(_timeout_s: int = 0) -> dict[str, object]:
        return {"ok": True, "stage": "ready", "durationMs": 1}

    pool.health_probe = health_probe  # type: ignore[method-assign, assignment]
    try:
        html, error, captured = pool.fetch_captured("http://boards.test/list", 5, settle_ms=0)
        # Later fallbacks in the same run must keep using the replacement.
        later_html, later_error, _ = pool.fetch_captured("http://boards.test/other", 5, settle_ms=0)
    finally:
        pool.close()

    assert (html, error, captured) == ("<html>board</html>", "", [])
    assert (later_html, later_error) == ("<html>board</html>", "")
    assert attempts["count"] == 3, "the death must be retried once on a fresh browser"
    assert len(launches) == 2, "the pool must launch a replacement, not reuse the dead handle"
    assert pool.metrics.snapshot()["pool_relaunch_on_death"] == 1


def test_pool_refusal_in_cooldown_is_not_counted_as_a_crash() -> None:
    breaker = BrowserFallbackCircuitBreaker(cooldown_minutes=30)
    breaker.disabled_until_at = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()
    pool = BrowserFallbackPool()
    reached_pool: list[str] = []

    def delegate(url: str, timeout_s: int) -> tuple[str, str]:
        reached_pool.append(url)
        return pool.fetch(url, timeout_s)

    html, error = breaker.wrap(delegate)("http://boards.test/list", 5)
    pool.close()

    assert (html, error) == ("", REFUSAL_ERROR_TEXT)
    assert reached_pool == [], "a refusal must never start or touch the browser pool"
    assert breaker.demand_attempts == 1
    assert breaker.demand_refused == 1
    metrics = pool.metrics.snapshot()
    assert metrics["pool_acquisitions"] == 0
    assert metrics["pool_relaunch_on_death"] == 0


def test_browser_pool_enabled_defaults_on_and_respects_kill_switch():
    assert browser_pool_enabled()
    assert not browser_pool_enabled({"BALUFFO_BROWSER_POOL": "0"})
    assert not browser_pool_enabled({"BALUFFO_BROWSER_POOL": "false"})
    assert browser_pool_enabled({"BALUFFO_BROWSER_POOL": "1"})


def test_browser_fallback_chromium_args_keep_gpu_in_process_on_linux():
    args = browser_fallback_chromium_args(platform="linux")
    assert "--in-process-gpu" in args
    assert "--disable-gpu" in args
    assert "--disable-dev-shm-usage" in args
    assert "--single-process" not in args


def test_browser_fallback_chromium_args_leave_non_linux_gpu_path_unchanged():
    args = browser_fallback_chromium_args(platform="win32")
    assert "--in-process-gpu" not in args
    assert "--disable-gpu" in args


@pytest.mark.slow  # launches a real headless Chromium; excluded from CI/quick runs
def test_pool_fetch_returns_html_and_counts_acquisition(http_server) -> None:
    pool = BrowserFallbackPool()
    try:
        html, error = pool.fetch(f"{http_server}/landing", 15)
        assert error == ""
        assert "page:/landing" in html
        metrics = pool.metrics.snapshot()
        assert metrics["pool_acquisitions"] == 1
        assert metrics["pool_startup_ms"] > 0
    finally:
        pool.close()


@pytest.mark.slow  # launches a real headless Chromium; excluded from CI/quick runs
def test_pool_concurrent_fetches_from_worker_threads(http_server) -> None:
    pool = BrowserFallbackPool()
    results: dict[int, tuple[str, str]] = {}
    try:
        with __import__("concurrent.futures", fromlist=["ThreadPoolExecutor"]).ThreadPoolExecutor(
            max_workers=4
        ) as executor:
            futures = {
                executor.submit(pool.fetch, f"{http_server}/item/{index}", 15): index
                for index in range(6)
            }
            for future in futures:
                index = futures[future]
                results[index] = future.result()
        for index, (html, error) in results.items():
            assert error == "", f"worker {index} errored: {error}"
            assert f"page:/item/{index}" in html
        assert pool.metrics.snapshot()["pool_acquisitions"] == 6
    finally:
        pool.close()


@pytest.mark.slow  # launches a real headless Chromium; excluded from CI/quick runs
def test_pool_health_probe_reports_ready_with_real_chromium() -> None:
    """The probe's success path against a real Chromium launch.

    The fast tests cover the launch/navigation failure shapes; this one proves
    `stage=ready` only after a real browser launched and rendered the local
    data-URL marker, with no stale failure diagnostics attached.
    """
    pool = BrowserFallbackPool()
    try:
        health = pool.health_probe(timeout_s=15)
        metrics = pool.metrics.snapshot()
    finally:
        pool.close()

    assert health["ok"] is True
    assert health["stage"] == "ready"
    assert health["error"] == ""
    assert health["errorType"] == ""
    assert health["probeUrl"].startswith("data:text/html")
    assert int(health["durationMs"]) > 0
    assert health["lastLaunchError"] == ""
    assert health["lastNavigationError"] == ""
    assert metrics["pool_health_probes"] == 1
    assert metrics["last_health_probe"]["ok"] is True


@pytest.mark.slow  # launches a real headless Chromium; excluded from CI/quick runs
def test_pool_context_isolation_no_cookie_bleed(http_server) -> None:
    pool = BrowserFallbackPool()
    try:
        html, error = pool.fetch(f"{http_server}/set-cookie", 15)
        assert error == ""
        assert "cookie was set" in html
        # Second acquire must NOT carry the first context's cookie.
        html2, error2 = pool.fetch(f"{http_server}/echo-cookie", 15)
        assert error2 == ""
        assert "cookie=none" in html2
    finally:
        pool.close()


@pytest.mark.slow  # launches a real headless Chromium; excluded from CI/quick runs
def test_pool_close_leaves_no_unclosed_asyncio_transports(http_server, monkeypatch) -> None:
    """pool.close() must run playwright's real shutdown path on the pool loop.

    If the node-driver subprocess pipe transports or the pool event loop are
    left unclosed, their __del__ fires at GC time: a ResourceWarning on
    selector loops, or -- on Windows proactor, where the pipe handle is
    already gone and repr() inside the warning raises ValueError -- an
    unraisable exception. Both are enforced as errors here so the regression
    cannot reappear silently.
    """
    unraisable: list[object] = []
    monkeypatch.setattr(sys, "unraisablehook", lambda args: unraisable.append(args))

    pool = BrowserFallbackPool()
    try:
        html, error = pool.fetch(f"{http_server}/x", 15)
        assert error == ""
        assert html
    finally:
        pool.close()

    with warnings.catch_warnings():
        warnings.simplefilter("error", ResourceWarning)
        gc.collect()  # collect the pool loop and any abandoned transports
        gc.collect()  # second pass for __del__ chains that spawn new garbage
    assert unraisable == [], f"unclosed asyncio transports at GC: {unraisable!r}"


@pytest.mark.slow  # launches a real headless Chromium; excluded from CI/quick runs
def test_pool_double_close_is_idempotent(http_server) -> None:
    pool = BrowserFallbackPool()
    try:
        html, _ = pool.fetch(f"{http_server}/x", 15)
        assert html
    finally:
        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            pool.close()
            gc.collect()  # force Task.__del__: pending tasks warn here
    pool.close()  # second call must not raise
    assert pool._loop is None
    assert pool._thread is None


def test_pool_error_strings_match_circuit_breaker_tokens(http_server) -> None:
    # Browser-death errors normalize to a circuit-breaker-recognized token.
    assert is_browser_fallback_environment_error(
        "browser fallback unavailable (playwright transport closed)"
    )
    assert is_browser_fallback_environment_error("browser has been closed")
    # End-to-end: a browser that cannot start yields an environment error, so the
    # breaker sees one failed attempt and arms its cooldown. A single death no
    # longer disables the lane: S2.1/S2.3 relaunch it instead.
    pool = BrowserFallbackPool()

    async def fail_to_start() -> None:
        raise RuntimeError("browserType.launch: Executable doesn't exist at /snap/chromium")

    pool._start_browser = fail_to_start  # type: ignore[method-assign]
    try:
        breaker = BrowserFallbackCircuitBreaker(cooldown_minutes=30)
        wrapped = breaker.wrap(pool.fetch)
        html, error = wrapped(f"{http_server}/z", 15)
        assert html == ""
        assert error
        assert breaker.failure_count == 1
        assert breaker.disabled_until_at  # cooldown armed
    finally:
        pool.close()


@pytest.mark.slow  # launches a real headless Chromium; excluded from CI/quick runs
def test_pool_fetch_bad_url_returns_error_not_raise(http_server) -> None:
    pool = BrowserFallbackPool()
    try:
        html, error = pool.fetch(f"{http_server}/slow", 1)
        assert html == ""
        assert error  # TimeoutError normalized to a string
    finally:
        pool.close()


@pytest.mark.slow  # launches a real headless Chromium; excluded from CI/quick runs
def test_pool_recycles_browser_after_acquisition_threshold(http_server, monkeypatch) -> None:
    monkeypatch.setenv("BALUFFO_BROWSER_POOL_RECYCLE_ACQUISITIONS", "1")
    pool = BrowserFallbackPool()
    try:
        html1, err1 = pool.fetch(f"{http_server}/a", 15)
        assert err1 == ""
        first_browser = pool._browser
        assert pool._acquisitions_since_launch == 1

        # Second fetch crosses the threshold (limit=1): the pre-fetch recycle
        # drops the old browser and _ensure_started launches a fresh one.
        html2, err2 = pool.fetch(f"{http_server}/b", 15)
        assert err2 == ""
        assert html2
        assert pool._browser is not None and pool._browser is not first_browser
        assert pool._acquisitions_since_launch == 1  # counter reset post-recycle
        metrics = pool.metrics.snapshot()
        assert int(metrics["pool_relaunch_count"]) >= 1
    finally:
        pool.close()
