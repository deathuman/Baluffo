"""Listener backlog and accept-stall telemetry.

Guards the packaged-Windows Admin stall: a page-load burst of ~25 bridge
requests against a 5-deep listen backlog surfaced as `ERR_CONNECTION_REFUSED`
because Windows refuses new SYNs when the backlog is full, whereas Linux drops
them and the client silently retries.

AI boundary owns: backlog sizing, accept counters, and idle-connection handling.
AI boundary search before contracts: bridge httpd, bridge handler, desktop site launcher, container gateway, and performance profile.
AI boundary verify: focused tests in this file plus bridge server and packaged desktop lifecycle suites.
"""

from __future__ import annotations

import json
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler

from src.bridge.performance_profile import snapshot_performance_profile
from src.bridge.server.handler import (
    HANDLER_IDLE_TIMEOUT_S,
    HANDLER_PROTOCOL_VERSION,
    make_handler,
)
from src.bridge.server.httpd import _BridgeHttpServer
from src.shared.http_server_capacity import (
    DEFAULT_ACCEPT_STALL_MS,
    DEFAULT_REQUEST_QUEUE_SIZE,
    CapacityThreadingHTTPServer,
    reset_accept_metrics,
    snapshot_accept_metrics,
)
from src.ship.runtime_launcher import _DesktopSiteServer


class _Api:
    """Minimal ServerHandlerApi surface for the real production handler."""

    runtime_config = None

    def bridge_log(self, level: str, event: str, **fields: object) -> None:
        return

    def mark_desktop_session_activity(self, path: str) -> None:
        return


class _QuietHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        return


def _start_server() -> tuple[_BridgeHttpServer, int]:
    server = _BridgeHttpServer(("127.0.0.1", 0), make_handler(api=_Api()))
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.05},
        daemon=True,
    )
    thread.start()
    time.sleep(0.2)
    return server, int(server.server_address[1])


def _read_response(sock: socket.socket) -> tuple[str, str]:
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
    head, _, rest = data.partition(b"\r\n\r\n")
    content_length = 0
    for line in head.split(b"\r\n"):
        if line.lower().startswith(b"content-length:"):
            content_length = int(line.split(b":", 1)[1])
    while len(rest) < content_length:
        rest += sock.recv(4096)
    status = head.split()[1].decode() if head.split() else ""
    return status, rest.decode()


def test_listeners_raise_the_socketserver_default_backlog() -> None:
    # socketserver's default is 5. Every Baluffo local listener must override it.
    assert DEFAULT_REQUEST_QUEUE_SIZE > 5
    assert CapacityThreadingHTTPServer.request_queue_size == DEFAULT_REQUEST_QUEUE_SIZE
    assert _BridgeHttpServer.request_queue_size == DEFAULT_REQUEST_QUEUE_SIZE
    assert _DesktopSiteServer.request_queue_size == DEFAULT_REQUEST_QUEUE_SIZE


def test_default_accept_stall_threshold_is_strictly_positive() -> None:
    assert DEFAULT_ACCEPT_STALL_MS > 0.0


def test_accept_metrics_snapshot_shape_and_reset() -> None:
    reset_accept_metrics()

    snapshot = snapshot_accept_metrics()

    assert snapshot["acceptedCount"] == 0
    assert snapshot["stallCount"] == 0
    assert snapshot["maxAcceptWaitMs"] == 0.0
    assert snapshot["stallThresholdMs"] == DEFAULT_ACCEPT_STALL_MS
    assert snapshot["listeners"] == {}
    # Must be JSON-serializable because it rides /ops/performance-profile.
    assert json.loads(json.dumps(snapshot)) == snapshot


def test_bound_listener_registers_its_backlog() -> None:
    reset_accept_metrics()
    server = CapacityThreadingHTTPServer(("127.0.0.1", 0), _QuietHandler)
    try:
        assert snapshot_accept_metrics()["listeners"] == {"server": DEFAULT_REQUEST_QUEUE_SIZE}
    finally:
        server.server_close()


def test_bridge_handler_uses_http11_keepalive_with_an_idle_timeout() -> None:
    handler = make_handler(api=_Api())
    assert handler.protocol_version == HANDLER_PROTOCOL_VERSION
    assert HANDLER_PROTOCOL_VERSION == "HTTP/1.1"
    # Keep-alive parks sockets between requests; an idle cap stops a pooled
    # socket from pinning its worker thread indefinitely.
    assert HANDLER_IDLE_TIMEOUT_S > 0.0


def test_performance_profile_exposes_accept_metrics() -> None:
    reset_accept_metrics()
    server = CapacityThreadingHTTPServer(("127.0.0.1", 0), _QuietHandler)
    try:
        payload = snapshot_performance_profile()
    finally:
        server.server_close()

    assert payload["acceptMetrics"]["stallThresholdMs"] == DEFAULT_ACCEPT_STALL_MS
    assert payload["acceptMetrics"]["listeners"] == {"server": DEFAULT_REQUEST_QUEUE_SIZE}


def test_keepalive_serves_repeated_requests_on_one_socket() -> None:
    server, port = _start_server()
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
            for _ in range(3):
                sock.sendall(b"GET /nope HTTP/1.1\r\nHost: x\r\n\r\n")
                status, body = _read_response(sock)
                # A 404 still proves the socket stayed open: the same connection
                # served all three requests.
                assert status == "404"
                assert body == '{"error": "Not found"}'
    finally:
        server.shutdown()
        server.server_close()


def test_page_load_burst_is_not_refused() -> None:
    """One Admin navigation issues far more requests than the old 5-deep backlog."""
    server, port = _start_server()
    results = {"ok": 0, "refused": 0, "error": 0}
    lock = threading.Lock()
    burst = 30

    def hit(_index: int) -> None:
        sock = socket.socket()
        sock.settimeout(10)
        try:
            sock.connect(("127.0.0.1", port))
            sock.sendall(b"GET /app/ready?view=summary HTTP/1.1\r\nHost: x\r\n\r\n")
            key = "ok" if sock.recv(64) else "error"
        except ConnectionRefusedError:
            key = "refused"
        except OSError:
            key = "error"
        finally:
            sock.close()
        with lock:
            results[key] += 1

    try:
        with ThreadPoolExecutor(max_workers=burst) as pool:
            list(pool.map(hit, range(burst)))
    finally:
        server.shutdown()
        server.server_close()

    assert results == {"ok": burst, "refused": 0, "error": 0}
