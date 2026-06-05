from __future__ import annotations

from pathlib import Path
from typing import Any

from src.bridge.performance_profile import clear_performance_profile, snapshot_performance_profile
from src.bridge.routes.post_routes import handle_post
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


class AdminOverviewStore(FakeDesktopLocalDataStore):
    def get_admin_overview(self, detail: str = "full") -> dict[str, Any]:
        return {
            "users": [{"uid": "user_1", "name": "Test User"}],
            "totals": {"usersCount": 1},
            "detailLevel": str(detail or "full"),
            "attachmentSizeBasis": "filesystem" if str(detail or "full") == "full" else "metadata",
        }


def _post_admin_overview(tmp_path: Path, payload: dict[str, Any]) -> FakeHandler:
    api = make_stub_bridge_api(tmp_path, AdminOverviewStore())
    handler = FakeHandler()
    assert handle_post(
        handler,
        api=api,
        path="/desktop-local-data/admin/overview",
        payload=payload,
    )
    return handler


def test_admin_overview_route_defaults_to_full_detail(tmp_path: Path) -> None:
    handler = _post_admin_overview(tmp_path, {})

    assert handler.sent[-1]["status"] == 200
    overview = handler.sent[-1]["payload"]["overview"]
    assert overview["detailLevel"] == "full"
    assert overview["attachmentSizeBasis"] == "filesystem"


def test_admin_overview_route_accepts_summary_detail_and_profiles_operation(
    tmp_path: Path,
) -> None:
    clear_performance_profile()
    try:
        handler = _post_admin_overview(tmp_path, {"detail": "summary"})

        assert handler.sent[-1]["status"] == 200
        overview = handler.sent[-1]["payload"]["overview"]
        assert overview["detailLevel"] == "summary"
        assert overview["attachmentSizeBasis"] == "metadata"
        profile = snapshot_performance_profile()
        labels = [row["label"] for row in profile["operationTimings"]["operations"]]
        assert "localdata.adminoverview.summary" in labels
    finally:
        clear_performance_profile()


def test_admin_overview_route_rejects_unknown_detail(tmp_path: Path) -> None:
    handler = _post_admin_overview(tmp_path, {"detail": "deep"})

    assert handler.sent[-1]["status"] == 400
    assert handler.sent[-1]["payload"]["ok"] is False
    assert "Invalid admin overview detail" in handler.sent[-1]["payload"]["error"]
