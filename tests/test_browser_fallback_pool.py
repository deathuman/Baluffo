"""Tests for BrowserFallbackPool.

Drives the pool from worker threads through its public sync ``fetch()``
surface (exactly what the fetch ThreadPoolExecutor does in prod). Pool
internals (asyncio loop thread, Chromium launch) are exercised end-to-end
against a stdlib http.server on localhost.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from src.jobs.browser_fallback import (
    BrowserFallbackCircuitBreaker,
    is_browser_fallback_environment_error,
)
from src.jobs.browser_fallback_pool import BrowserFallbackPool, browser_pool_enabled


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


def test_pool_concurrent_fetches_from_worker_threads(http_server) -> None:
    pool = BrowserFallbackPool()
    results: dict[str, tuple[str, str]] = {}
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


def test_pool_double_close_is_idempotent(http_server) -> None:
    pool = BrowserFallbackPool()
    try:
        html, _ = pool.fetch(f"{http_server}/x", 15)
        assert html
    finally:
        pool.close()
    pool.close()  # second call must not raise
    assert pool._loop is None
    assert pool._thread is None


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


def test_pool_fetch_bad_url_returns_error_not_raise(http_server) -> None:
    pool = BrowserFallbackPool()
    try:
        html, error = pool.fetch(f"{http_server}/slow", 1)
        assert html == ""
        assert error  # TimeoutError normalized to a string
    finally:
        pool.close()
