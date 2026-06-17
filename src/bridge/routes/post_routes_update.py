from __future__ import annotations

from typing import Any

from src.bridge.api import BridgeApi
from src.bridge.container_mode import is_container_runtime, send_container_unavailable
from src.bridge.routes.error_boundary import send_json_boundary
from src.bridge.routes.response_writer import BridgeResponseWriter


def _update_route_error(exc: Exception) -> dict[str, Any]:
    return {"started": False, "error": str(exc)}


def handle_post(handler: BridgeResponseWriter, *, api: BridgeApi, path: str, payload: Any) -> bool:
    if path == "/app/check-for-update":
        if is_container_runtime(api):
            send_container_unavailable(handler)
            return True

        def _payload() -> dict[str, Any]:
            force = bool((payload or {}).get("force")) if isinstance(payload, dict) else False
            return api.check_for_update(force=force)

        send_json_boundary(handler, _payload, error_status=500, error_payload=_update_route_error)
        return True

    if path == "/app/download-update":
        if is_container_runtime(api):
            send_container_unavailable(handler)
            return True
        send_json_boundary(
            handler,
            api.download_update,
            error_status=500,
            error_payload=_update_route_error,
        )
        return True

    if path == "/app/install-update":
        if is_container_runtime(api):
            send_container_unavailable(handler)
            return True
        send_json_boundary(
            handler,
            api.install_update,
            error_status=500,
            error_payload=_update_route_error,
        )
        return True

    return False
