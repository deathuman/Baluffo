"""App GET route handlers.

AI boundary owns: `/app/ready` and `/app/update-status` GET route response wiring only.
AI boundary implement in: ops health readiness and desktop update service leaves.
AI boundary search before contracts: frontend callers, bridge route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET route tests.
"""

from __future__ import annotations

from typing import Any, Protocol

from src.bridge.container_mode import is_container_runtime, send_container_unavailable
from src.bridge.performance_profile import time_operation
from src.bridge.routes.error_boundary import send_json_boundary
from src.bridge.routes.response_writer import BridgeResponseWriter


class _AppRouteApi(Protocol):
    runtime_config: Any

    def compute_ops_health_ready(self) -> dict[str, Any]: ...

    def get_update_status_payload(self) -> dict[str, Any]: ...


def _json_error(exc: Exception) -> dict[str, Any]:
    return {"ok": False, "error": str(exc)}


def handle_app_routes(
    handler: BridgeResponseWriter, *, api: _AppRouteApi, path: str, query: dict[str, list[str]]
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
