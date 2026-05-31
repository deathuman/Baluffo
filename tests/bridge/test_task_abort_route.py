"""Route-level tests for task abort requests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.bridge.routes.post_routes import handle_post
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_task_abort_route_forwards_status_and_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    calls: list[dict[str, Any]] = []

    def abort_task(payload: dict[str, Any] | None) -> tuple[int, dict[str, Any]]:
        calls.append(dict(payload or {}))
        return 200, {
            "ok": True,
            "abortAccepted": True,
            "taskType": "fetch",
            "runId": "fetch_1",
            "state": "aborting",
        }

    api.abort_task = abort_task

    handler = FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/tasks/abort",
        payload={"taskType": "fetch", "runId": "fetch_1"},
    )

    assert result is True
    assert calls == [{"taskType": "fetch", "runId": "fetch_1"}]
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["abortAccepted"] is True


def test_task_abort_route_rejects_sync(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.abort_task = lambda _payload=None: (
        400,
        {"ok": False, "error": "unsupported_task_abort", "taskType": "sync"},
    )

    handler = FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/tasks/abort",
        payload={"taskType": "sync", "runId": "sync_1"},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert handler.sent[-1]["payload"]["error"] == "unsupported_task_abort"
