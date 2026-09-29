"""Leaf HTTP listener backlog and accept-stall telemetry for local servers.

The desktop site, bridge, and container-gateway listeners all inherited the
socketserver default `request_queue_size` of 5. One Admin navigation issues far
more simultaneous bridge requests than that, so the backlog was the first thing
to overflow under burst load: Windows refuses new SYNs once it is full and the
browser reports `net::ERR_CONNECTION_REFUSED`, while Linux drops them and the
client silently retries. That is why this only ever showed up in the packaged
Windows app.

Accept latency is recorded here because that failure is otherwise invisible. The
acceptor is a single thread that must win the GIL between `accept()` calls, and
each stall is a window where the browser gets connection errors instead of slow
responses, which no per-route duration can explain.

AI boundary owns: listener backlog sizing and accept-stall counters.
AI boundary implement in: this file for server-class and counter behavior; callers own routing, logging, and diagnostics routes.
AI boundary search before contracts: bridge httpd, bridge handler, desktop site launcher, container gateway, and ops performance diagnostics.
AI boundary verify: `tests/test_http_server_capacity.py` plus the bridge and packaged desktop suites.
"""

from __future__ import annotations

import socket
import time
from datetime import UTC, datetime
from http.server import ThreadingHTTPServer
from threading import Lock
from typing import Any

DEFAULT_REQUEST_QUEUE_SIZE = 128
DEFAULT_ACCEPT_STALL_MS = 250.0


class _AcceptMetrics:
    """Process-wide accept counters shared by every capacity-tuned listener."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._accepted = 0
        self._stalls = 0
        self._max_wait_ms = 0.0
        self._last_wait_ms = 0.0
        self._last_accepted_at = ""
        self._listeners: dict[str, int] = {}

    def register_listener(self, name: str, request_queue_size: int) -> None:
        with self._lock:
            self._listeners[str(name or "unknown")] = int(request_queue_size)

    def record_accept(self, wait_ms: float, stall_threshold_ms: float) -> None:
        wait = max(0.0, float(wait_ms or 0.0))
        with self._lock:
            self._accepted += 1
            self._last_wait_ms = round(wait, 3)
            self._last_accepted_at = datetime.now(UTC).isoformat()
            if wait > self._max_wait_ms:
                self._max_wait_ms = round(wait, 3)
            if stall_threshold_ms > 0.0 and wait > stall_threshold_ms:
                self._stalls += 1

    def reset(self) -> None:
        with self._lock:
            self._accepted = self._stalls = 0
            self._max_wait_ms = self._last_wait_ms = 0.0
            self._last_accepted_at = ""
            self._listeners = {}

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "acceptedCount": self._accepted,
                "stallCount": self._stalls,
                "maxAcceptWaitMs": self._max_wait_ms,
                "lastAcceptWaitMs": self._last_wait_ms,
                "lastAcceptedAt": self._last_accepted_at,
                "stallThresholdMs": DEFAULT_ACCEPT_STALL_MS,
                "listeners": dict(self._listeners),
            }


_METRICS = _AcceptMetrics()


def snapshot_accept_metrics() -> dict[str, object]:
    """Return a JSON-safe snapshot of accept counters and listener backlogs."""
    return _METRICS.snapshot()


def reset_accept_metrics() -> None:
    _METRICS.reset()


class CapacityThreadingHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer with a real backlog and accept-stall telemetry.

    The acceptor is the process-wide bottleneck for a local-only HTTP server:
    one thread accepts, then hands each connection to a worker. A deep backlog
    absorbs bursts while that thread is briefly descheduled instead of turning
    them into refused connections.
    """

    request_queue_size = DEFAULT_REQUEST_QUEUE_SIZE
    accept_metrics_name = "server"
    accept_stall_threshold_ms = DEFAULT_ACCEPT_STALL_MS
    daemon_threads = True

    def server_bind(self) -> None:
        super().server_bind()
        _METRICS.register_listener(self.accept_metrics_name, int(self.request_queue_size))

    def get_request(self) -> tuple[socket.socket, Any]:
        started = time.perf_counter()
        request = super().get_request()
        _METRICS.record_accept(
            (time.perf_counter() - started) * 1000,
            float(self.accept_stall_threshold_ms),
        )
        return request


__all__ = [
    "DEFAULT_ACCEPT_STALL_MS",
    "DEFAULT_REQUEST_QUEUE_SIZE",
    "CapacityThreadingHTTPServer",
    "reset_accept_metrics",
    "snapshot_accept_metrics",
]
