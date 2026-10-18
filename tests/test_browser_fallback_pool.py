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
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from src.jobs.browser_fallback import (
    BrowserFallbackCircuitBreaker,
    is_browser_fallback_environment_error,
)
from src.jobs.browser_fallback_pool import (
    BrowserFallbackPool,
    browser_pool_enabled,
)


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


def test_browser_pool_enabled_defaults_on_and_respects_kill_switch():
    assert browser_pool_enabled()
    assert not browser_pool_enabled({"BALUFFO_BROWSER_POOL": "0"})
    assert not browser_pool_enabled({"BALUFFO_BROWSER_POOL": "false"})
    assert browser_pool_enabled({"BALUFFO_BROWSER_POOL": "1"})


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


@pytest.mark.slow  # launches a real headless Chromium; excluded from CI/quick runs
def test_pool_error_strings_match_circuit_breaker_tokens(http_server) -> None:
    # Browser-death errors normalize to a circuit-breaker-recognized token.
    assert is_browser_fallback_environment_error(
        "browser fallback unavailable (playwright transport closed)"
    )
    assert is_browser_fallback_environment_error("browser has been closed")
    # End-to-end: a fetch on a pool whose browser is gone trips the breaker.
    pool = BrowserFallbackPool()
    try:
        pool.fetch(f"{http_server}/y", 15)
        pool._mark_unavailable()  # simulate detected browser death
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
