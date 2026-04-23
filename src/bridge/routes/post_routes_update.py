from __future__ import annotations

from typing import Any

from src.bridge.api import BridgeApi


def handle_post(handler: Any, *, api: BridgeApi, path: str, payload: Any) -> bool:
    if path == "/app/check-for-update":
        try:
            force = bool((payload or {}).get("force")) if isinstance(payload, dict) else False
            handler._send_json(api.check_for_update(force=force))  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"started": False, "error": str(exc)}, status=500)  # noqa: SLF001
        return True

    if path == "/app/download-update":
        try:
            result = api.download_update()
            handler._send_json(result, status=200)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"started": False, "error": str(exc)}, status=500)  # noqa: SLF001
        return True

    if path == "/app/install-update":
        try:
            result = api.install_update()
            handler._send_json(result, status=200)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"started": False, "error": str(exc)}, status=500)  # noqa: SLF001
        return True

    return False
