from __future__ import annotations

from typing import Any

from src.bridge.api import BridgeApi
from src.bridge.container_mode import is_container_runtime, send_container_unavailable
from src.bridge.routes.response_writer import BridgeResponseWriter


def handle_post(handler: BridgeResponseWriter, *, api: BridgeApi, path: str, payload: Any) -> bool:
    if path == "/app/check-for-update":
        if is_container_runtime(api):
            send_container_unavailable(handler)
            return True
        try:
            force = bool((payload or {}).get("force")) if isinstance(payload, dict) else False
            handler.send_json(api.check_for_update(force=force))
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"started": False, "error": str(exc)}, status=500)
        return True

    if path == "/app/download-update":
        if is_container_runtime(api):
            send_container_unavailable(handler)
            return True
        try:
            result = api.download_update()
            handler.send_json(result, status=200)
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"started": False, "error": str(exc)}, status=500)
        return True

    if path == "/app/install-update":
        if is_container_runtime(api):
            send_container_unavailable(handler)
            return True
        try:
            result = api.install_update()
            handler.send_json(result, status=200)
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"started": False, "error": str(exc)}, status=500)
        return True

    return False
