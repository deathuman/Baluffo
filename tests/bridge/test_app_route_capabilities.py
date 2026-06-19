from __future__ import annotations

from types import SimpleNamespace

from src.bridge.routes.get_app import handle_app_routes
from tests.helpers.bridge_api import FakeHandler


class MinimalAppRouteApi:
    def __init__(self, *, container_mode: bool = False) -> None:
        self.runtime_config = SimpleNamespace(container_mode=container_mode)

    def compute_ops_health_ready(self) -> dict[str, object]:
        return {"ok": True, "ready": True}

    def get_update_status_payload(self) -> dict[str, object]:
        return {"ok": True, "availability": "available"}


def test_app_get_routes_accept_minimal_capability_object() -> None:
    api = MinimalAppRouteApi()

    ready_handler = FakeHandler()
    assert handle_app_routes(ready_handler, api=api, path="/app/ready", query={}) is True
    assert ready_handler.sent[-1]["payload"] == {"ok": True, "ready": True}

    update_handler = FakeHandler()
    assert handle_app_routes(update_handler, api=api, path="/app/update-status", query={}) is True
    assert update_handler.sent[-1]["payload"] == {"ok": True, "availability": "available"}


def test_app_update_status_minimal_capability_object_preserves_container_unavailable() -> None:
    handler = FakeHandler()

    assert (
        handle_app_routes(
            handler,
            api=MinimalAppRouteApi(container_mode=True),
            path="/app/update-status",
            query={},
        )
        is True
    )

    assert handler.sent[-1]["status"] == 409
    assert handler.sent[-1]["payload"] == {"ok": False, "error": "not available in container mode"}
