"""Shared exception boundaries for bridge route handlers.

AI boundary owns: route exception-to-JSON boundaries and safe bridge logging.
AI boundary implement in: route leaves and bridge logging/runtime helpers.
AI boundary search before contracts: route callers, error payload tests, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused route helper tests.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from src.bridge.routes.response_writer import BridgeResponseWriter

_EXPECTED_BRIDGE_LOG_EXCEPTIONS = (AttributeError, OSError, TypeError, ValueError)


class _BridgeLogApi(Protocol):
    def bridge_log(self, level: str, event: str, **fields: Any) -> None: ...


def safe_bridge_log(api: _BridgeLogApi, level: str, event: str, **fields: Any) -> None:
    try:
        api.bridge_log(level, event, **fields)
    except _EXPECTED_BRIDGE_LOG_EXCEPTIONS:
        pass


def send_json_boundary(
    handler: BridgeResponseWriter,
    action: Callable[[], Any],
    *,
    error_status: int,
    error_payload: Callable[[Exception], dict[str, Any]],
    success_status: int = 200,
) -> None:
    try:
        handler.send_json(action(), status=success_status)
    except Exception as exc:  # noqa: BLE001 - HTTP route boundary converts failures to JSON.
        handler.send_json(error_payload(exc), status=error_status)


def run_route_boundary(
    handler: BridgeResponseWriter,
    action: Callable[[], None],
    *,
    error_status: int,
    error_payload: Callable[[Exception], dict[str, Any]],
    error_sender: Callable[[Exception], None] | None = None,
) -> None:
    try:
        action()
    except Exception as exc:  # noqa: BLE001 - HTTP route boundary converts failures to JSON.
        if error_sender is not None:
            error_sender(exc)
            return
        handler.send_json(error_payload(exc), status=error_status)
