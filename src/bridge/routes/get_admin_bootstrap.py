"""Admin bootstrap GET route wiring."""

from __future__ import annotations

import os

from src.bridge.admin_bootstrap import get_admin_bootstrap_payload
from src.bridge.api import BridgeApi
from src.bridge.performance_profile import time_operation
from src.bridge.routes.response_writer import BridgeResponseWriter

_ADMIN_BOOTSTRAP_SMOKE_FAIL_ONCE_CONSUMED = False


def _consume_admin_bootstrap_smoke_fail_once() -> bool:
    if str(os.getenv("BALUFFO_PACKAGED_SMOKE_RUNTIME") or "").strip() != "1":
        return False
    requested = str(os.getenv("BALUFFO_PACKAGED_SMOKE_ADMIN_BOOTSTRAP_FAIL_ONCE") or "")
    if requested.strip().lower() not in {"1", "true", "yes", "on"}:
        return False
    global _ADMIN_BOOTSTRAP_SMOKE_FAIL_ONCE_CONSUMED
    if _ADMIN_BOOTSTRAP_SMOKE_FAIL_ONCE_CONSUMED:
        return False
    _ADMIN_BOOTSTRAP_SMOKE_FAIL_ONCE_CONSUMED = True
    return True


def handle_admin_bootstrap_routes(
    handler: BridgeResponseWriter, *, api: BridgeApi, path: str, query: dict[str, list[str]]
) -> bool:
    """Handle admin bootstrap GET routes."""
    del query

    if path == "/admin/bootstrap":
        with time_operation("admin.bootstrap.route_payload"):
            if _consume_admin_bootstrap_smoke_fail_once():
                handler.send_json(
                    {
                        "ok": False,
                        "error": "packaged smoke forced admin bootstrap timeout",
                        "smokeFailure": True,
                    },
                    status=504,
                )
                return True
            handler.send_json(get_admin_bootstrap_payload(api))
        return True

    return False
