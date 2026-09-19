"""POST route registration surface for the admin bridge.

AI boundary owns: POST family dispatch only.
AI boundary implement in: `post_routes_{admin,local_data,update}.py` leaves.
AI boundary search before contracts: frontend builders, route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused POST tests.
"""

from __future__ import annotations

import webbrowser
from collections.abc import Callable
from typing import Any, Protocol

from src.bridge.routes.response_writer import BridgeResponseWriter

from . import post_routes_admin as post_routes_admin_mod
from . import post_routes_local_data as post_routes_local_data_mod
from . import post_routes_update as post_routes_update_mod


class _PostRouteApi(
    post_routes_admin_mod._AdminPostRouteApi,
    post_routes_local_data_mod._LocalDataPostRouteApi,
    post_routes_update_mod._UpdatePostRouteApi,
    Protocol,
):
    """Composed capability set required by the public POST route delegator."""


# Ordered first-match-wins dispatch table. Each entry pairs a handler with a
# factory for its extra keyword arguments. The factory is called at dispatch
# time so `webbrowser.open` is resolved per request (tests patch it there).
# The sequence is contract: do not reorder.
_POST_ROUTE_HANDLERS: tuple[tuple[Callable[..., bool], Callable[[], dict[str, Any]]], ...] = (
    (post_routes_local_data_mod.handle_post, lambda: {"open_url": webbrowser.open}),
    (post_routes_update_mod.handle_post, lambda: {}),
    (post_routes_admin_mod.handle_post, lambda: {}),
)


def handle_post(
    handler: BridgeResponseWriter, *, api: _PostRouteApi, path: str, payload: Any
) -> bool:
    """Handle POST routes for the admin bridge."""

    for route_handler, extra_kwargs in _POST_ROUTE_HANDLERS:
        if route_handler(handler, api=api, path=path, payload=payload, **extra_kwargs()):
            return True

    return False
