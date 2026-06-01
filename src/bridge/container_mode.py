from __future__ import annotations

from typing import Any

CONTAINER_UNAVAILABLE_ERROR = "not available in container mode"
CONTAINER_UNAVAILABLE_PAYLOAD = {"ok": False, "error": CONTAINER_UNAVAILABLE_ERROR}


def is_container_runtime(api: Any) -> bool:
    runtime_config = getattr(api, "runtime_config", None)
    return bool(getattr(runtime_config, "container_mode", False))


def send_container_unavailable(handler: Any, *, status: int = 409) -> None:
    handler.send_json(dict(CONTAINER_UNAVAILABLE_PAYLOAD), status=status)
