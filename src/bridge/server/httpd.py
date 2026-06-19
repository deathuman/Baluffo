from __future__ import annotations

from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from src.bridge.api import BridgeApi

_EXPECTED_ON_STARTED_EXCEPTIONS = (OSError, RuntimeError, ValueError)


def run_http_server(
    *,
    api: BridgeApi,
    host: str,
    port: int,
    handler_cls: type[BaseHTTPRequestHandler],
    on_started: Callable[[], Any] | None = None,
) -> int:
    try:
        server = ThreadingHTTPServer((host, port), handler_cls)
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
    should_exit_for_owner_timeout = getattr(api, "should_exit_for_owner_timeout", None)
    try:
        banner_fn = getattr(api, "startup_banner", None)
        if callable(banner_fn):
            banner_fn(getattr(api, "runtime_config", None))
        if callable(on_started):
            try:
                on_started()
            except _EXPECTED_ON_STARTED_EXCEPTIONS as exc:
                api.bridge_log(
                    "warn",
                    "admin_bridge_on_started_failed",
                    error=str(exc),
                )
        try:
            while True:
                server.handle_request()
                if callable(should_exit_for_owner_timeout) and should_exit_for_owner_timeout():
                    api.bridge_log("info", "admin_bridge_owner_timeout_shutdown")
                    break
        except KeyboardInterrupt:
            api.bridge_log("info", "admin_bridge_shutdown_requested", signal="keyboard_interrupt")
    finally:
        server.server_close()
        api.bridge_log("info", "admin_bridge_stopped")
    return 0
