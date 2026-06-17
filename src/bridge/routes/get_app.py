"""App GET route handlers."""

from __future__ import annotations

from typing import Any

from src.bridge.api import BridgeApi
from src.bridge.container_mode import is_container_runtime, send_container_unavailable
from src.bridge.performance_profile import time_operation
from src.bridge.routes.error_boundary import send_json_boundary
from src.bridge.routes.response_writer import BridgeResponseWriter


def _json_error(exc: Exception) -> dict[str, Any]:
    return {"ok": False, "error": str(exc)}


def handle_app_routes(
    handler: BridgeResponseWriter, *, api: BridgeApi, path: str, query: dict[str, list[str]]
) -> bool:
    del query
    if path == "/app/ready":
        with time_operation("app.ready.route_payload"):
            handler.send_json(api.compute_ops_health_ready())
        return True

    if path == "/app/update-status":
        if is_container_runtime(api):
            send_container_unavailable(handler)
            return True
        send_json_boundary(
            handler,
            api.get_update_status_payload,
            error_status=500,
            error_payload=_json_error,
        )
        return True

    return False
