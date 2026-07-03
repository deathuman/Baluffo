"""Tests for bridge GET routes - improving coverage for get_routes.py."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def _assert_log_response(
    handler: FakeHandler,
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
    store = FakeDesktopLocalDataStore()
    store.sign_in("Test User")
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/desktop-local-data/session", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True
    assert handler.sent[-1]["payload"]["user"]["name"] == "Test User"
    assert handler.sent[-1]["payload"]["desktopSession"]["sessionId"] == "desktop-session-1"
    timing = handler.sent[-1]["payload"]["timing"]
    assert set(timing) == {"sessionPayloadMs", "currentUserReadMs", "payloadBuildMs"}
    assert all(isinstance(value, int) and value >= 0 for value in timing.values())


def test_session_no_user(tmp_path: Path) -> None:
    """Test session endpoint when no user is signed in."""
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/desktop-local-data/session", query={})

    assert result is True
    assert handler.sent[-1]["payload"]["user"] is None
    assert handler.sent[-1]["payload"]["desktopSession"]["ownerToken"] == "desktop-owner-1"
    assert handler.sent[-1]["payload"]["timing"]["payloadBuildMs"] >= 0


def test_profiles_returns_sorted_profiles_with_current_flag(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    store.sign_in("Zed")
    store.sign_in("Andrea")
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/desktop-local-data/profiles", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True
    profiles = handler.sent[-1]["payload"]["profiles"]
    assert [row["displayName"] for row in profiles] == ["Andrea", "Zed"]
    assert profiles[0]["isCurrent"] is True


def test_saved_jobs_endpoint_exists(tmp_path: Path) -> None:
    """Test saved jobs endpoint exists."""
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/desktop-local-data/saved-jobs",
        query={},
    )

    assert result is True


def test_app_update_status(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
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

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/app/update-status", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["latestVersion"] == "0.2.0"
    assert handler.sent[-1]["payload"]["releaseNotesTitle"] == "Baluffo v0.2.0"


def test_startup_metrics_endpoint_returns_versioned_rows(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.read_startup_metrics = lambda limit: [
        {
            "schemaVersion": 1,
            "ts": "2026-04-17T09:00:00+00:00",
            "event": "desktop_site_ready",
            "category": "site",
            "fields": {"elapsedMs": 400},
        }
    ]

    handler = FakeHandler()
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
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path=path, query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert expected_key in handler.sent[-1]["payload"]


def test_registry_summary_uses_lightweight_payload_without_sources(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.load_state = lambda: (_ for _ in ()).throw(AssertionError("load_state not expected"))  # type: ignore[assignment]
    api.get_registry_summary_payload = lambda: {  # type: ignore[assignment]
        "activeCount": 3,
        "pendingCount": 2,
        "rejectedCount": 1,
        "hiddenPendingCount": 1,
        "authorityMode": "json",
        "updatedAt": "2026-06-03T00:00:00+00:00",
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/registry/summary", query={})

    assert result is True
    payload = handler.sent[-1]["payload"]
    assert handler.sent[-1]["status"] == 200
    assert payload["ok"] is True
    assert payload["authorityMode"] == "json"
    assert payload["generatedAt"] == "2026-06-03T00:00:00+00:00"
    assert payload["summary"]["activeCount"] == 3
    assert "sources" not in payload


def test_registry_sources_returns_requested_buckets_from_one_state_load(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    calls = {"load_state": 0}

    def load_state():
        calls["load_state"] += 1
        return {
            "active": [{"id": "active_1", "name": "Active"}],
            "pending": [
                {"id": "pending_visible", "name": "Visible", "jobsFound": 1},
                {
                    "id": "pending_hidden",
                    "name": "Hidden",
                    "jobsFound": 0,
                    "hiddenFromDefault": True,
                },
            ],
            "rejected": [{"id": "rejected_1", "name": "Rejected"}],
        }

    api.load_state = load_state  # type: ignore[assignment]
    api.DISCOVERY_CANDIDATES_PATH = tmp_path / "source-discovery-candidates.json"  # type: ignore[assignment]
    api.DISCOVERY_CANDIDATES_PATH.write_text("[]", encoding="utf-8")

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/registry/sources",
        query={"buckets": ["pending,active"], "includeHiddenPending": ["0"]},
    )

    assert result is True
    payload = handler.sent[-1]["payload"]
    assert handler.sent[-1]["status"] == 200
    assert calls["load_state"] == 1
    assert payload["ok"] is True
    assert set(payload["sources"]) == {"pending", "active"}
    assert [row["id"] for row in payload["sources"]["pending"]] == ["pending_visible"]
    assert [row["id"] for row in payload["sources"]["active"]] == ["active_1"]
    assert payload["summary"]["pendingCount"] == 2
    assert payload["summary"]["hiddenPendingCount"] == 1


def test_registry_sources_can_include_hidden_pending_rows(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.load_state = lambda: {  # type: ignore[assignment]
        "active": [],
        "pending": [
            {"id": "visible", "name": "Visible", "jobsFound": 1},
            {"id": "hidden", "name": "Hidden", "jobsFound": 0, "hiddenFromDefault": True},
        ],
        "rejected": [],
    }
    api.DISCOVERY_CANDIDATES_PATH = tmp_path / "source-discovery-candidates.json"  # type: ignore[assignment]
    api.DISCOVERY_CANDIDATES_PATH.write_text("[]", encoding="utf-8")

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/registry/sources",
        query={"buckets": ["pending"], "includeHiddenPending": ["1"]},
    )

    assert result is True
    payload = handler.sent[-1]["payload"]
    assert [row["id"] for row in payload["sources"]["pending"]] == ["visible", "hidden"]
    assert payload["summary"]["hiddenPendingCount"] == 1


def test_registry_sources_rejects_unknown_bucket(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.load_state = lambda: (_ for _ in ()).throw(AssertionError("load_state not expected"))  # type: ignore[assignment]

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/registry/sources",
        query={"buckets": ["pending,unknown"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert handler.sent[-1]["payload"]["ok"] is False
    assert handler.sent[-1]["payload"]["invalidBuckets"] == "unknown"


def test_log_routes_return_expected_payloads(tmp_path: Path) -> None:
    cases = [
        (
            "discovery-content",
            "DISCOVERY_LOG_PATH",
            "/discovery/log",
            "log line 1\nlog line 2\n",
            {},
            "log line 1\nlog line 2\n",
            0,
            len("log line 1\nlog line 2\n"),
        ),
        ("discovery-empty", "DISCOVERY_LOG_PATH", "/discovery/log", None, {}, "", 0, 0),
        (
            "discovery-offset",
            "DISCOVERY_LOG_PATH",
            "/discovery/log",
            "line1\nline2\nline3\n",
            {"offset": ["5"]},
            "line1\nline2\nline3\n"[5:],
            5,
            len("line1\nline2\nline3\n"),
        ),
        (
            "discovery-invalid-offset",
            "DISCOVERY_LOG_PATH",
            "/discovery/log",
            "content",
            {"offset": ["abc"]},
            "content",
            0,
            len("content"),
        ),
        (
            "fetcher-content",
            "FETCHER_LOG_PATH",
            "/fetcher/log",
            "fetcher log content",
            {},
            "fetcher log content",
            0,
            len("fetcher log content"),
        ),
        ("fetcher-missing", "FETCHER_LOG_PATH", "/fetcher/log", None, {}, "", 0, 0),
    ]

    for case_id, path_attr, route_path, content, query, expected_text, offset, next_offset in cases:
        store = FakeDesktopLocalDataStore()
        api = make_stub_bridge_api(tmp_path / case_id, store)
        if content is not None:
            log_path = getattr(api, path_attr)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text(content, encoding="utf-8", newline="\n")

        handler = FakeHandler()
        result = handle_get(handler, api=api, path=route_path, query=query)

        assert result is True, case_id
        _assert_log_response(handler, text=expected_text, offset=offset, next_offset=next_offset)


def test_discovery_config_returns_saved_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/config", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True
    assert handler.sent[-1]["payload"]["savedConfig"]["autoApproveHealthyPendingOnComplete"] is True


def test_log_routes_handle_utf8_boundaries_and_replacement(tmp_path: Path) -> None:
    prefix = "prefix "
    cases = [
        (
            "discovery-trailing-partial-utf8",
            "DISCOVERY_LOG_PATH",
            "/discovery/log",
            prefix.encode("utf-8") + b"\xe2\x82",
            prefix,
        ),
        (
            "fetcher-trailing-partial-utf8",
            "FETCHER_LOG_PATH",
            "/fetcher/log",
            prefix.encode("utf-8") + b"\xe2\x82",
            prefix,
        ),
        (
            "fetcher-malformed-middle-byte",
            "FETCHER_LOG_PATH",
            "/fetcher/log",
            b"good\xfftail",
            "good\ufffdtail",
        ),
    ]

    for case_id, path_attr, route_path, raw_bytes, expected_text in cases:
        store = FakeDesktopLocalDataStore()
        api = make_stub_bridge_api(tmp_path / case_id, store)
        log_path = getattr(api, path_attr)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_bytes(raw_bytes)

        handler = FakeHandler()
        result = handle_get(handler, api=api, path=route_path, query={})

        assert result is True, case_id
        _assert_log_response(handler, text=expected_text, offset=0, next_offset=len(expected_text))


def test_ops_health(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/health", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert "ok" in handler.sent[-1]["payload"]


def test_ops_history_default_limit(tmp_path: Path) -> None:
    """Test /ops/history with default limit."""
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    calls: list[str] = []
    api.get_lifecycle_run_history_rows = lambda: (
        calls.append("lifecycle")
        or [{"runId": "run_1", "type": "fetch", "finishedAt": "2026-05-07T00:00:00Z"}]
    )
    api.sync_history_from_reports = lambda: (_ for _ in ()).throw(
        AssertionError("legacy history fallback must not be used")
    )

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/history", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert calls == ["lifecycle"]
    assert handler.sent[-1]["payload"]["runs"][0]["runId"] == "run_1"


def test_ops_history_custom_limit(tmp_path: Path) -> None:
    """Test /ops/history with custom limit."""
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/history", query={"limit": ["50"]})

    assert result is True


def test_ops_history_invalid_limit(tmp_path: Path) -> None:
    """Test /ops/history with invalid limit."""
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/history", query={"limit": ["invalid"]})

    assert result is True


def test_ops_task_state(tmp_path: Path) -> None:
    """Test /ops/task-state endpoint."""
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.get_current_task_state_payload = lambda: {
        "tasks": [{"taskType": "fetch", "runId": "fetch_1", "active": True}],
        "count": 1,
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/task-state", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["count"] == 1


def test_ops_task_state_summary_route_uses_bounded_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.get_current_task_state_payload = lambda: {
        "tasks": [{"taskType": "fetch", "runId": "full", "workItems": [{"id": "full"}]}],
        "count": 1,
    }
    api.get_current_task_state_summary_payload = lambda: {
        "tasks": [
            {
                "taskType": "fetch",
                "runId": "fetch_1",
                "active": True,
                "workItemCount": 5000,
                "workItemsTruncated": True,
            }
        ],
        "count": 1,
        "summary": True,
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/task-state", query={"view": ["summary"]})

    assert result is True
    payload = handler.sent[-1]["payload"]
    assert payload["summary"] is True
    assert payload["tasks"][0]["runId"] == "fetch_1"
    assert "workItems" not in payload["tasks"][0]
    assert len(json.dumps(payload).encode("utf-8")) < 256 * 1024


def test_ops_dashboard_health_route_uses_dashboard_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.compute_ops_health = lambda: {"service": "baluffo-bridge", "status": "healthy"}
    api.compute_ops_dashboard_health = lambda: {"alerts": [{"id": "dashboard"}]}

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/dashboard-health", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"] == {"alerts": [{"id": "dashboard"}]}


def test_ops_fetcher_metrics_default(tmp_path: Path) -> None:
    """Test /ops/fetcher-metrics with default window."""
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/fetcher-metrics", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_ops_fetcher_metrics_custom_window(tmp_path: Path) -> None:
    """Test /ops/fetcher-metrics with custom window."""
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/ops/fetcher-metrics",
        query={"windowRuns": ["50"]},
    )

    assert result is True


def test_ops_perf_counters_route_returns_snapshot(tmp_path: Path) -> None:
    """Test /ops/perf-counters endpoint."""
    from src.shared.timing_counters import clear_counters, record_duration

    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    clear_counters()
    record_duration("bridge_request_get_ops_health", 12)

    try:
        handler = FakeHandler()
        result = handle_get(handler, api=api, path="/ops/perf-counters", query={})

        assert result is True
        assert handler.sent[-1]["status"] == 200
        assert handler.sent[-1]["payload"]["ok"] is True
        assert (
            handler.sent[-1]["payload"]["counters"]["bridge_request_get_ops_health"]["count"] == 1
        )
    finally:
        clear_counters()


def test_ops_fetch_report(tmp_path: Path) -> None:
    """Test /ops/fetch-report endpoint."""
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/fetch-report", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_ops_fetch_report_live_view_omits_source_details(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    report = {
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
    import json

    (tmp_path / "jobs-fetch-report.json").write_text(json.dumps(report), encoding="utf-8")

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/fetch-report", query={"view": ["live"]})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["summary"]["outputCount"] == 12
    assert payload["taskProgress"]["counts"]["resolvedSources"] == 1
    assert [payload[k] for k in ("sourceCount", "sources", "sourcesTruncated")] == [1, [], True]
    assert payload["sourceDetailPath"] == "/ops/fetch-report/sources"


def test_sync_status(tmp_path: Path) -> None:
    """Test /sync/status endpoint."""
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/sync/status", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_pipeline_status(tmp_path: Path) -> None:
    """Test /tasks/run-jobs-pipeline-status endpoint."""
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/tasks/run-jobs-pipeline-status", query={})

    assert result is True


def test_unknown_route_returns_false(tmp_path: Path) -> None:
    """Test that unknown route returns False."""
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/unknown/route", query={})

    assert result is False
