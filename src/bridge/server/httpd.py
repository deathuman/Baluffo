from __future__ import annotations

import sys
import threading
import time
import traceback
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler
from typing import Any, Protocol

from src.bridge import startup_work_budget
from src.shared.http_server_capacity import (
    CapacityThreadingHTTPServer,
    snapshot_accept_metrics,
)


class HttpServerApi(Protocol):
    def bridge_log(self, level: str, event: str, **fields: Any) -> None: ...


_EXPECTED_ON_STARTED_EXCEPTIONS = (OSError, RuntimeError, ValueError)


class _BridgeHttpServer(CapacityThreadingHTTPServer):
    """Bridge listener: default 5-deep backlog could not absorb a page-load burst."""

    accept_metrics_name = "bridge"


def _append_phase_metric(api: HttpServerApi, event: str, **fields: Any) -> None:
    """Record a startup phase without letting telemetry break the listener."""
    append = getattr(api, "append_startup_metric", None)
    if not callable(append):
        return
    try:
        append(event, fields or None)
    except (OSError, RuntimeError, TypeError, ValueError):
        return


def _accepted_count() -> int:
    try:
        raw = snapshot_accept_metrics().get("acceptedCount")
        return int(raw) if isinstance(raw, (int, float, str)) else 0
    except (TypeError, ValueError, OSError):
        return 0


def _start_on_started_thread(
    api: HttpServerApi, on_started: Callable[[], Any] | None
) -> threading.Thread | None:
    """Run post-bind startup work off the accept path.

    `startupReady` is already true once the owner session is registered, but the
    listener could not answer until this call returned: the accept loop only ran
    after it. Every startup step here (journal cleanup, stale-task cleanup,
    startup sync scheduling) therefore used to sit directly in front of the
    browser's first request. It still runs, on its own thread, so the page can
    load and read bridge state while it completes.
    """
    if not callable(on_started):
        return None

    def runner() -> None:
        started = time.perf_counter()
        _append_phase_metric(api, "desktop_bridge_started_hooks_started")
        try:
            on_started()
        except _EXPECTED_ON_STARTED_EXCEPTIONS as exc:
            api.bridge_log(
                "warn",
                "admin_bridge_on_started_failed",
                error=str(exc),
            )
        finally:
            # An exception still in flight here is an unexpected startup failure:
            # log it so it reaches the bridge journal, then let the thread end.
            failure = sys.exc_info()
            if failure[0] is not None:
                api.bridge_log(
                    "error",
                    "admin_bridge_on_started_unexpected_failed",
                    error=str(failure[1]),
                    detail="".join(traceback.format_exception(*failure)),
                )
            _append_phase_metric(
                api,
                "desktop_bridge_started_hooks_finished",
                elapsedMs=int((time.perf_counter() - started) * 1000),
            )

    thread = threading.Thread(
        target=runner,
        name="baluffo-bridge-on-started",
        daemon=True,
    )
    thread.start()
    return thread


def run_http_server(
    *,
    api: HttpServerApi,
    host: str,
    port: int,
    handler_cls: type[BaseHTTPRequestHandler],
    on_started: Callable[[], Any] | None = None,
) -> int:
    listen_started = time.perf_counter()
    try:
        server = _BridgeHttpServer((host, port), handler_cls)
    except OSError as exc:
        api.bridge_log(
            "error",
            "admin_bridge_start_failed",
            host=host,
            port=port,
            error=str(exc),
        )
        return 1
    server.timeout = 0.25
    startup_work_budget.start_startup_work_budget()
    _append_phase_metric(
        api,
        "desktop_bridge_socket_bound",
        elapsedMs=int((time.perf_counter() - listen_started) * 1000),
        host=str(host),
        port=int(port),
    )
    should_exit_for_owner_timeout = getattr(api, "should_exit_for_owner_timeout", None)
    accepted_at_bind = _accepted_count()
    first_request_recorded = False
    try:
        banner_fn = getattr(api, "startup_banner", None)
        if callable(banner_fn):
            banner_fn(getattr(api, "runtime_config", None))
        _append_phase_metric(
            api,
            "desktop_bridge_accept_loop_started",
            elapsedMs=int((time.perf_counter() - listen_started) * 1000),
        )
        _start_on_started_thread(api, on_started)
        try:
            while True:
                server.handle_request()
                accepted_now = _accepted_count()
                # The accept loop is the only place that sees request arrivals, so
                # it also drives the first-paint barrier: heavy startup work is
                # admitted once this burst goes quiet (or the budget expires).
                startup_work_budget.note_accepted_requests(accepted_now)
                if not first_request_recorded and accepted_now > accepted_at_bind:
                    first_request_recorded = True
                    _append_phase_metric(
                        api,
                        "desktop_bridge_first_request_served",
                        elapsedMs=int((time.perf_counter() - listen_started) * 1000),
                    )
                if callable(should_exit_for_owner_timeout) and should_exit_for_owner_timeout():
                    api.bridge_log("info", "admin_bridge_owner_timeout_shutdown")
                    break
        except KeyboardInterrupt:
            api.bridge_log("info", "admin_bridge_shutdown_requested", signal="keyboard_interrupt")
    finally:
        server.server_close()
        api.bridge_log("info", "admin_bridge_stopped")
    return 0
