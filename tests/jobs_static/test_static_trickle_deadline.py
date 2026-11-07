"""Static fetch deadlines against a real trickling HTTP peer.

``test_static_hard_deadline`` pins the deadline plumbing with doubles; this
file proves it on real sockets. The server sends response headers immediately,
then one small chunk per interval for longer than either bound, so the
per-operation timeouts (``urlopen``'s socket timeout, httpx's re-armed read
timeout) never fire on their own. Both static fetch lanes must still end at
their own deadline instead of waiting for the peer to finish.
"""

from __future__ import annotations

import http.server
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from src.jobs import transport
from src.jobs.adapters.static_runtime_support import StaticHtmlFetcher
from src.jobs.common.http import (
    HttpStatusError,
    _read_response_body,
    fetch_max_bytes_for_url,
)
from src.shared import http_batch

_DRIP_INTERVAL_S = 0.15
_DRIP_S = 8.0
_SLOW_REDIRECT_DELAY_S = 1.25
_REQUEST_TIMEOUT_S = 1
_SOURCE_DEADLINE_S = 2.0
_CEILING_S = 5.0


class _DripHandler(http.server.BaseHTTPRequestHandler):
    """Headers at once, then a chunk per interval far past the request deadline."""

    connections = 0
    paths: list[str] = []

    def do_GET(self) -> None:  # noqa: N802 - http.server's naming
        type(self).connections += 1
        type(self).paths.append(self.path)
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "/drip")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path == "/slow-redirect":
            time.sleep(_SLOW_REDIRECT_DELAY_S)
            self.send_response(302)
            self.send_header("Location", "/short")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path == "/short":
            body = b"<html>short redirect target</html>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        chunk = b"<html>" + b" " * 64
        deadline = time.monotonic() + _DRIP_S
        try:
            while time.monotonic() < deadline:
                self.wfile.write(b"%x\r\n" % len(chunk) + chunk + b"\r\n")
                self.wfile.flush()
                time.sleep(_DRIP_INTERVAL_S)
            self.wfile.write(b"0\r\n\r\n")
        except (BrokenPipeError, ConnectionResetError, OSError, ValueError):
            return

    def log_message(self, *args: Any) -> None:
        return


@contextmanager
def _drip_server() -> Iterator[str]:
    _DripHandler.connections = 0
    _DripHandler.paths = []
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _DripHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/drip"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Surface a 3xx as HTTPError instead of letting urllib follow it."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001 - stdlib hook
        return None


_NO_REDIRECT_OPENER = urllib.request.build_opener(_NoRedirectHandler)


def _fetch_no_redirect(url: str, timeout_s: int) -> str:
    """Real-socket fetch that hands redirects to ``StaticHtmlFetcher``'s hop chain.

    Production urllib follows redirects inside ``urlopen``, but the static
    fetcher expects a 3xx to arrive as ``HttpStatusError`` so it can validate and
    follow the hop itself. This seam supplies that contract over a real socket
    and keeps the production total-read bound, so the drip target cannot re-arm
    the socket timeout.
    """
    request = urllib.request.Request(url)
    try:
        with _NO_REDIRECT_OPENER.open(request, timeout=timeout_s) as response:
            body = _read_response_body(
                response, cap=fetch_max_bytes_for_url(url), timeout_s=timeout_s
            )
            return body.decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        code = int(exc.code)
        location = str(exc.headers.get("Location") or "") if exc.headers else ""
        exc.close()
        if 300 <= code < 400:
            raise HttpStatusError(code, url, location=location) from exc
        raise


def test_urllib_static_fetch_ends_at_its_request_deadline() -> None:
    with _drip_server() as url:
        fetcher = StaticHtmlFetcher(
            fetch_text=transport.default_fetch_text,
            timeout_s=_REQUEST_TIMEOUT_S,
            retries=0,
            backoff_s=0.0,
        )
        # A source deadline far enough out that the per-request bound is what fires.
        fetcher.set_source_deadline(time.perf_counter() + 60.0)
        started = time.perf_counter()
        with pytest.raises(RuntimeError, match="request deadline exceeded"):
            fetcher.fetch_html_cached(url)
        elapsed = time.perf_counter() - started
    assert _DripHandler.connections >= 1, "the fetch must have used the real socket"
    assert elapsed < _CEILING_S  # timing-ok: 1 s deadline, 5 s ceiling, 8 s trickle


def test_static_redirect_hop_target_drip_shares_the_source_deadline() -> None:
    """A redirect hop is not a new budget: the target drip gets the same clock.

    The 10 s request timeout outlives the target's 8 s drip, so a hop that did
    not carry the source deadline would finish the drip and return a body; only
    the shared 2 s source deadline turns the target into
    ``time_budget_exceeded`` at ~2 s.
    """
    with _drip_server() as url:
        redirect_url = url.replace("/drip", "/redirect")
        fetcher = StaticHtmlFetcher(
            fetch_text=_fetch_no_redirect,
            timeout_s=10,
            retries=0,
            backoff_s=0.0,
        )
        fetcher.set_source_deadline(time.perf_counter() + _SOURCE_DEADLINE_S)
        started = time.perf_counter()
        with pytest.raises(TimeoutError, match="time_budget_exceeded"):
            fetcher.fetch_html_cached(redirect_url)
        elapsed = time.perf_counter() - started
    assert _DripHandler.paths == ["/redirect", "/drip"]
    assert elapsed < _CEILING_S  # timing-ok: 2 s source deadline, 5 s ceiling, 8 s dripped target


def test_redirect_target_does_not_reset_the_budget_after_a_slow_first_hop() -> None:
    """A fresh per-hop budget would fetch /short and succeed; the shared clock refuses it.

    The first hop spends 1.25 s of the 2 s source budget before redirecting. Less
    than the one-second minimum remains, so the target must never be requested. A
    per-hop reset would give /short a fresh budget and return its body instead.
    """
    with _drip_server() as url:
        redirect_url = url.replace("/drip", "/slow-redirect")
        fetcher = StaticHtmlFetcher(
            fetch_text=_fetch_no_redirect,
            timeout_s=10,
            retries=0,
            backoff_s=0.0,
        )
        fetcher.set_source_deadline(time.perf_counter() + _SOURCE_DEADLINE_S)
        started = time.perf_counter()
        with pytest.raises(TimeoutError, match="time_budget_exceeded"):
            fetcher.fetch_html_cached(redirect_url)
        elapsed = time.perf_counter() - started
    assert _DripHandler.paths == ["/slow-redirect"], (
        "a fresh per-hop budget would have fetched /short and succeeded"
    )
    assert elapsed >= _SLOW_REDIRECT_DELAY_S
    assert elapsed < _CEILING_S  # timing-ok: 2 s source deadline, 5 s ceiling, immediate /short


def test_async_static_listing_batch_ends_at_the_source_deadline() -> None:
    """The static listing lane awaits one coroutine per page through
    ``fetch_pages_batched``; its bound is the source deadline, not httpx's
    re-armed read timeout."""

    if transport.httpx is None:
        pytest.skip("httpx is not installed")
    with _drip_server() as url:
        started = time.perf_counter()
        results = http_batch.fetch_pages_batched(
            1,
            [{"url": url, "payload": {}}],
            sync_fetch=lambda _job, _url, _timeout_s: "",
            async_fetch=lambda client, _job, target, timeout_s: transport.async_fetch_text_httpx(
                client, target, timeout_s
            ),
            total_concurrency=1,
            per_host_concurrency=1,
            deadline_monotonic=time.perf_counter() + 1.0,
        )
        elapsed = time.perf_counter() - started
    assert len(results) == 1
    assert results[0]["ok"] is False, "a response that never completes is never a success"
    assert "deadline exceeded" in str(results[0]["error"])
    assert _DripHandler.connections >= 1, "the fetch must have used the real socket"
    assert elapsed < _CEILING_S  # timing-ok: 1 s source deadline, 5 s ceiling, 8 s trickle


def test_async_static_fetch_ends_at_its_request_deadline() -> None:
    if transport.httpx is None:
        pytest.skip("httpx is not installed")
    fetcher = transport.AsyncHttpTextFetcher(max_connections=2)
    try:
        with _drip_server() as url:
            started = time.perf_counter()
            with pytest.raises(TimeoutError, match="request deadline exceeded"):
                fetcher.fetch_text(url, _REQUEST_TIMEOUT_S)
            elapsed = time.perf_counter() - started
    finally:
        fetcher.close()
    assert _DripHandler.connections >= 1, "the fetch must have used the real socket"
    assert elapsed < _CEILING_S  # timing-ok: 1 s deadline, 5 s ceiling, 8 s trickle
