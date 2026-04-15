"""Tests for bridge GET routes - improving coverage for get_routes.py."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.bridge.routes.get_routes import handle_get
from tests.bridge.conftest import _FakeDesktopLocalDataStore, _FakeHandler, _make_api


def test_session_with_user(tmp_path: Path) -> None:
    """Test session endpoint when user is signed in."""
    store = _FakeDesktopLocalDataStore()
    store.sign_in("Test User")
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/desktop-local-data/session", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True
    assert handler.sent[-1]["payload"]["user"]["name"] == "Test User"
    assert handler.sent[-1]["payload"]["desktopSession"]["sessionId"] == "desktop-session-1"


def test_session_no_user(tmp_path: Path) -> None:
    """Test session endpoint when no user is signed in."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/desktop-local-data/session", query={})

    assert result is True
    assert handler.sent[-1]["payload"]["user"] is None
    assert handler.sent[-1]["payload"]["desktopSession"]["ownerToken"] == "desktop-owner-1"


def test_saved_jobs_endpoint_exists(tmp_path: Path) -> None:
    """Test saved jobs endpoint exists."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/desktop-local-data/saved-jobs",
        query={},
    )

    assert result is True


def test_app_update_status(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    api.get_update_status_payload = lambda: {
        "currentVersion": "0.1.0",
        "latestVersion": "0.2.0",
        "updateAvailable": True,
        "availability": "available",
        "downloadState": "idle",
        "installState": "idle",
        "releaseNotesUrl": "https://example.com/releases/v0.2.0",
        "releaseNotesTitle": "Baluffo v0.2.0",
        "releaseNotesBody": "### Fixed\n- Notes",
        "releaseNotesPublishedAt": "2026-04-15T10:00:00Z",
    }

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/app/update-status", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["latestVersion"] == "0.2.0"
    assert handler.sent[-1]["payload"]["releaseNotesTitle"] == "Baluffo v0.2.0"


@pytest.mark.parametrize(
    "path,expected_key",
    [
        pytest.param("/registry/active", "sources", id="active"),
        pytest.param("/registry/pending", "sources", id="pending"),
        pytest.param("/registry/rejected", "sources", id="rejected"),
        pytest.param("/registry/summary", "summary", id="summary"),
    ],
)
def test_registry_endpoints(tmp_path: Path, path: str, expected_key: str) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path=path, query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert expected_key in handler.sent[-1]["payload"]


def test_discovery_log_with_content(tmp_path: Path) -> None:
    """Test /discovery/log endpoint with log content."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    api.DISCOVERY_LOG_PATH.write_text("log line 1\nlog line 2\n")

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/log", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert "text" in handler.sent[-1]["payload"]


def test_discovery_log_empty(tmp_path: Path) -> None:
    """Test /discovery/log endpoint with empty log."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/log", query={})

    assert result is True
    assert handler.sent[-1]["payload"]["text"] == ""


def test_discovery_log_with_offset(tmp_path: Path) -> None:
    """Test /discovery/log endpoint with offset query param."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    api.DISCOVERY_LOG_PATH.write_text("line1\nline2\nline3\n")

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/log", query={"offset": ["5"]})

    assert result is True
    assert handler.sent[-1]["payload"]["offset"] == 5


def test_discovery_log_invalid_offset(tmp_path: Path) -> None:
    """Test /discovery/log with invalid offset."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    api.DISCOVERY_LOG_PATH.write_text("content")

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/log", query={"offset": ["abc"]})

    assert result is True
    assert handler.sent[-1]["payload"]["offset"] == 0


def test_discovery_config_returns_saved_payload(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/config", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True
    assert handler.sent[-1]["payload"]["savedConfig"]["autoApproveHealthyPendingOnComplete"] is True


def test_fetcher_log_with_content(tmp_path: Path) -> None:
    """Test /fetcher/log endpoint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    api.FETCHER_LOG_PATH.write_text("fetcher log content")

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/fetcher/log", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_ops_health(tmp_path: Path) -> None:
    """Test /ops/health endpoint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/ops/health", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert "ok" in handler.sent[-1]["payload"]


def test_ops_history_default_limit(tmp_path: Path) -> None:
    """Test /ops/history with default limit."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/ops/history", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_ops_history_custom_limit(tmp_path: Path) -> None:
    """Test /ops/history with custom limit."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/ops/history", query={"limit": ["50"]})

    assert result is True


def test_ops_history_invalid_limit(tmp_path: Path) -> None:
    """Test /ops/history with invalid limit."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/ops/history", query={"limit": ["invalid"]})

    assert result is True


def test_ops_task_state(tmp_path: Path) -> None:
    """Test /ops/task-state endpoint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    api.get_current_task_state_payload = lambda: {
        "tasks": [{"taskType": "fetch", "runId": "fetch_1", "active": True}],
        "count": 1,
    }

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/ops/task-state", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["count"] == 1


def test_ops_fetcher_metrics_default(tmp_path: Path) -> None:
    """Test /ops/fetcher-metrics with default window."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/ops/fetcher-metrics", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_ops_fetcher_metrics_custom_window(tmp_path: Path) -> None:
    """Test /ops/fetcher-metrics with custom window."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/ops/fetcher-metrics",
        query={"windowRuns": ["50"]},
    )

    assert result is True


def test_ops_fetch_report(tmp_path: Path) -> None:
    """Test /ops/fetch-report endpoint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/ops/fetch-report", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_sync_status(tmp_path: Path) -> None:
    """Test /sync/status endpoint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/sync/status", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_pipeline_status(tmp_path: Path) -> None:
    """Test /tasks/run-jobs-pipeline-status endpoint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/tasks/run-jobs-pipeline-status", query={})

    assert result is True


def test_unknown_route_returns_false(tmp_path: Path) -> None:
    """Test that unknown route returns False."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/unknown/route", query={})

    assert result is False
