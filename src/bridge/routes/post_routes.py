"""POST route registration surface for the admin bridge.

AI boundary owns: POST family dispatch only.
AI boundary implement in: `post_routes_{admin,local_data,update}.py` leaves.
AI boundary search before contracts: frontend builders, route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused POST tests.
"""

from __future__ import annotations

import webbrowser
from typing import Any

from src.bridge.api import BridgeApi
from src.bridge.routes.response_writer import BridgeResponseWriter

from . import post_routes_admin as post_routes_admin_mod
from . import post_routes_local_data as post_routes_local_data_mod
from . import post_routes_update as post_routes_update_mod


def handle_post(handler: BridgeResponseWriter, *, api: BridgeApi, path: str, payload: Any) -> bool:
    """Handle POST routes for the admin bridge."""

    return (
        post_routes_local_data_mod.handle_post(
            handler,
            api=api,
            path=path,
            payload=payload,
            open_url=webbrowser.open,
        )
        or post_routes_update_mod.handle_post(handler, api=api, path=path, payload=payload)
        or post_routes_admin_mod.handle_post(handler, api=api, path=path, payload=payload)
    )
