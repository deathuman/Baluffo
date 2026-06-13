from __future__ import annotations

from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_ops_task_live_summary_route_uses_summary_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    calls: list[tuple[str, bool]] = []

    def _task_live_payload(task_type: str, *, summary: bool = False) -> dict[str, object]:
        calls.append((task_type, summary))
        return {
            "taskType": task_type,
            "runId": "fetch_1",
            "active": True,
            "summaryView": bool(summary),
            "workItems": [],
        }

    api.get_task_live_payload = _task_live_payload

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/task-live/fetch", query={"view": ["summary"]})

    assert result is True
    assert calls == [("fetch", True)]
    payload = handler.sent[-1]["payload"]
    assert payload["summaryView"] is True
    assert payload["workItems"] == []
