"""Tests for bridge GET routes - improving coverage for get_routes.py."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.bridge.routes.get_routes import handle_get
from tests.bridge.conftest import _FakeDesktopLocalDataStore, _FakeHandler, _make_api


def _assert_log_response(
    handler: _FakeHandler,
    *,
    text: str,
    offset: int,
    next_offset: int,
) -> None:
    payload = handler.sent[-1]["payload"]
    assert handler.sent[-1]["status"] == 200
    assert payload["text"] == text
    assert payload["offset"] == offset
    assert payload["nextOffset"] == next_offset
    assert payload["hasMore"] is False


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


def test_profiles_returns_sorted_profiles_with_current_flag(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    store.sign_in("Zed")
    store.sign_in("Andrea")
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/desktop-local-data/profiles", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True
    profiles = handler.sent[-1]["payload"]["profiles"]
    assert [row["displayName"] for row in profiles] == ["Andrea", "Zed"]
    assert profiles[0]["isCurrent"] is True


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


def test_startup_metrics_endpoint_returns_versioned_rows(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    api.read_startup_metrics = lambda limit: [
        {
            "schemaVersion": 1,
            "ts": "2026-04-17T09:00:00+00:00",
            "event": "desktop_site_ready",
            "category": "site",
            "fields": {"elapsedMs": 400},
        }
    ]

    handler = _FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/desktop-local-data/startup-metrics",
        query={"limit": ["25"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"] == {
        "ok": True,
        "rows": [
            {
                "schemaVersion": 1,
                "ts": "2026-04-17T09:00:00+00:00",
                "event": "desktop_site_ready",
                "category": "site",
                "fields": {"elapsedMs": 400},
            }
        ],
    }


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
    content = "log line 1\nlog line 2\n"

    api.DISCOVERY_LOG_PATH.write_text(content, encoding="utf-8", newline="\n")

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/log", query={})

    assert result is True
    _assert_log_response(handler, text=content, offset=0, next_offset=len(content))


def test_discovery_log_empty(tmp_path: Path) -> None:
    """Test /discovery/log endpoint with empty log."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/log", query={})

    assert result is True
    _assert_log_response(handler, text="", offset=0, next_offset=0)


def test_discovery_log_with_offset(tmp_path: Path) -> None:
    """Test /discovery/log endpoint with offset query param."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    content = "line1\nline2\nline3\n"

    api.DISCOVERY_LOG_PATH.write_text(content, encoding="utf-8", newline="\n")

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/log", query={"offset": ["5"]})

    assert result is True
    _assert_log_response(handler, text=content[5:], offset=5, next_offset=len(content))


def test_discovery_log_invalid_offset(tmp_path: Path) -> None:
    """Test /discovery/log with invalid offset."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    api.DISCOVERY_LOG_PATH.write_text("content", encoding="utf-8", newline="\n")

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/log", query={"offset": ["abc"]})

    assert result is True
    _assert_log_response(handler, text="content", offset=0, next_offset=len("content"))


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
    content = "fetcher log content"

    api.FETCHER_LOG_PATH.write_text(content, encoding="utf-8", newline="\n")

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/fetcher/log", query={})

    assert result is True
    _assert_log_response(handler, text=content, offset=0, next_offset=len(content))


def test_fetcher_log_missing_file_returns_empty_payload(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/fetcher/log", query={})

    assert result is True
    _assert_log_response(handler, text="", offset=0, next_offset=0)


@pytest.mark.parametrize(
    ("path_attr", "path"),
    [
        pytest.param("DISCOVERY_LOG_PATH", "/discovery/log", id="discovery"),
        pytest.param("FETCHER_LOG_PATH", "/fetcher/log", id="fetcher"),
    ],
)
def test_log_routes_defer_trailing_partial_utf8_bytes(
    tmp_path: Path,
    path_attr: str,
    path: str,
) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    prefix = "prefix "
    getattr(api, path_attr).write_bytes(prefix.encode("utf-8") + b"\xe2\x82")

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path=path, query={})

    assert result is True
    _assert_log_response(handler, text=prefix, offset=0, next_offset=len(prefix))


def test_fetcher_log_replaces_malformed_utf8_bytes_in_middle(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    api.FETCHER_LOG_PATH.write_bytes(b"good\xfftail")
    expected = "good\ufffdtail"

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/fetcher/log", query={})

    assert result is True
    _assert_log_response(handler, text=expected, offset=0, next_offset=len(expected))


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


def test_ops_fetch_report_live_view_omits_source_details(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    api.load_json_object = lambda path, default: {
        "runId": "fetch_live_1",
        "summary": {"outputCount": 12, "failedSources": 0, "sourceCount": 1},
        "taskProgress": {"active": True, "counts": {"resolvedSources": 1, "sourceCount": 1}},
        "sources": [
            {
                "name": "studio_a",
                "status": "ok",
                "durationMs": 1234,
                "details": [{"url": "https://example.com/job/1", "status": "ok"}],
            }
        ],
    }

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/ops/fetch-report", query={"view": ["live"]})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["summary"]["outputCount"] == 12
    assert payload["taskProgress"]["counts"]["resolvedSources"] == 1
    assert payload["sources"][0]["durationMs"] == 1234
    assert "details" not in payload["sources"][0]


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
