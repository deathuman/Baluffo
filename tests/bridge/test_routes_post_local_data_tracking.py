from __future__ import annotations

from pathlib import Path

from src.bridge.routes.post_routes import handle_post
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_update_tracking_success(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    store.sign_in("Test User")
    store.save_job_for_user("user_123", {"title": "Test"}, {})
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/saved-jobs/tracking",
        payload={
            "uid": "user_123",
            "jobKey": "job_0",
            "tracking": {"pipelinePhase": "applied", "outcomeStatus": "active"},
            "options": {},
        },
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert store.saved_jobs["user_123"][0]["pipelinePhase"] == "applied"
