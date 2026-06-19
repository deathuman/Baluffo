from __future__ import annotations

from typing import Any

from src.bridge.routes.get_sync import handle_sync_routes
from tests.helpers.bridge_api import FakeHandler


class MinimalSyncRouteApi:
    def get_sync_status_payload(self) -> dict[str, Any]:
        return {"ok": True, "detailLevel": "full"}

    def load_sync_runtime_state(self) -> dict[str, Any]:
        return {
            "state": "idle",
            "lastPull": {"result": "ok", "finishedAt": "2026-06-19T09:00:00Z"},
            "lastPush": {"result": "skipped", "finishedAt": "2026-06-19T09:05:00Z"},
        }

    def sync_config_status(self) -> dict[str, Any]:
        return {
            "enabled": True,
            "state": "ready",
            "ready": True,
            "repo": "owner/repo",
            "branch": "main",
            "path": "data",
        }


class RuntimeStateFailureSyncRouteApi(MinimalSyncRouteApi):
    def load_sync_runtime_state(self) -> dict[str, Any]:
        raise OSError("runtime state unavailable")


def test_sync_get_route_accepts_minimal_capability_object_for_full_view() -> None:
    handler = FakeHandler()

    assert (
        handle_sync_routes(
            handler,
            api=MinimalSyncRouteApi(),
            path="/sync/status",
            query={},
        )
        is True
    )

    assert handler.sent[-1]["payload"] == {"ok": True, "detailLevel": "full"}


def test_sync_get_route_accepts_minimal_capability_object_for_summary_view() -> None:
    handler = FakeHandler()

    assert (
        handle_sync_routes(
            handler,
            api=MinimalSyncRouteApi(),
            path="/sync/status",
            query={"view": ["summary"]},
        )
        is True
    )

    payload = handler.sent[-1]["payload"]
    assert payload["summaryView"] is True
    assert payload["config"]["enabled"] is True
    assert payload["runtime"]["state"] == "idle"
    assert payload["runtime"]["lastPull"]["result"] == "ok"


def test_sync_summary_minimal_capability_object_preserves_runtime_state_fallback() -> None:
    handler = FakeHandler()

    assert (
        handle_sync_routes(
            handler,
            api=RuntimeStateFailureSyncRouteApi(),
            path="/sync/status",
            query={"view": ["summary"]},
        )
        is True
    )

    payload = handler.sent[-1]["payload"]
    assert payload["summaryView"] is True
    assert payload["runtime"]["state"] == ""


def test_sync_get_route_minimal_capability_object_preserves_unsupported_view() -> None:
    handler = FakeHandler()

    assert (
        handle_sync_routes(
            handler,
            api=MinimalSyncRouteApi(),
            path="/sync/status",
            query={"view": ["everything"]},
        )
        is True
    )

    assert handler.sent[-1]["status"] == 400
    assert handler.sent[-1]["payload"] == {
        "ok": False,
        "error": "unsupported sync status view: everything",
    }
