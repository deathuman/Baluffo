from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from src.bridge.api import BridgeApi


def run_http_server(
    *, api: BridgeApi, host: str, port: int, handler_cls: type[BaseHTTPRequestHandler]
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
    banner_fn = getattr(api, "startup_banner", None)
    if callable(banner_fn):
        banner_fn(getattr(api, "runtime_config", None))
    server.timeout = 1.0
    should_exit_for_owner_timeout = getattr(api, "should_exit_for_owner_timeout", None)
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
