from __future__ import annotations

from pathlib import Path

from src.bridge.performance_profile import (
    clear_performance_profile,
    record_operation_duration,
    record_route_duration,
)
from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_ops_performance_profile_route_returns_bounded_profile(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    clear_performance_profile()
    record_route_duration("GET", "/ops/dashboard-health?token=hidden", 125, status=200)
    record_operation_duration("ops.dashboard.history", 42)

    try:
        handler = FakeHandler()
        result = handle_get(handler, api=api, path="/ops/performance-profile", query={})

        assert result is True
        assert handler.sent[-1]["status"] == 200
        payload = handler.sent[-1]["payload"]
        assert payload["ok"] is True
        assert payload["runtime"]["runtimeMode"] == "desktop"
        assert payload["routeTimings"]["routes"][0]["label"] == "GET /ops/dashboard-health"
        assert payload["operationTimings"]["operations"][0]["label"] == "ops.dashboard.history"
        assert "token" not in payload["routeTimings"]["routes"][0]["label"]
    finally:
        clear_performance_profile()
