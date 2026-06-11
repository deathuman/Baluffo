from __future__ import annotations

import json
from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_discovery_report_summary_returns_bounded_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    report = {
        "runId": "discovery_1",
        "status": "completed",
        "startedAt": "2026-06-06T08:00:00Z",
        "finishedAt": "2026-06-06T08:01:00Z",
        "summary": {
            "endpointCount": 20,
            "probedCount": 10,
            "queuedCandidateCount": 4,
            "candidateCount": 7,
            "failedCount": 1,
            "failureCount": 5,
            "activeCount": 12,
            "pendingCount": 8,
        },
        "taskProgress": {"active": False, "phase": "complete", "percent": 100},
        "runtime": {
            "registryFinalization": {
                "status": "completed",
                "activeCount": 12,
                "pendingCount": 8,
                "rejectedCount": 1,
            },
            "autoApproval": {"enabled": True, "status": "completed", "approvedCount": 2},
        },
        "largePadding": "x" * (1024 * 1024 + 16),
        "candidates": [{"id": str(index), "body": "not returned"} for index in range(4000)],
        "failures": [{"id": str(index), "body": "not returned"} for index in range(3000)],
        "log": [f"row {index}" for index in range(30)],
    }
    api.DISCOVERY_REPORT_PATH.write_text(json.dumps(report), encoding="utf-8")
    api.normalize_discovery_report_contract = lambda _payload: (_ for _ in ()).throw(
        AssertionError("summary view must not normalize the full discovery report")
    )
    api.reconcile_terminal_discovery_report_from_state = lambda: (_ for _ in ()).throw(
        AssertionError("summary view must not reconcile the full discovery report")
    )

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/report", query={"view": ["summary"]})

    assert result is True
    assert handler.bytes_sent[-1]["status"] == 200
    payload = json.loads(handler.bytes_sent[-1]["body"].decode("utf-8"))
    assert payload["summaryView"] is True
    assert payload["detailLevel"] == "summary"
    assert payload["counts"] == {"candidateCount": 7, "failureCount": 5}
    assert payload["summary"]["candidateCount"] == 7
    assert payload["summary"]["failureCount"] == 5
    assert payload["runtime"]["registryFinalization"]["activeCount"] == 12
    assert payload["runtime"]["autoApproval"]["approvedCount"] == 2
    assert payload["recentLog"] == []
    assert "candidates" not in payload
    assert "failures" not in payload


def test_fetch_report_summary_returns_bounded_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    report = {
        "runId": "fetch_1",
        "status": "ok",
        "startedAt": "2026-06-06T08:00:00Z",
        "finishedAt": "2026-06-06T08:02:00Z",
        "headerPadding": "x" * (768 * 1024),
        "summary": {
            "outputCount": 300,
            "keptCount": 250,
            "failedSources": 2,
            "totalSources": 40,
        },
        "taskProgress": {"active": False, "phase": "complete", "percent": 100},
        "sources": [
            {"name": f"source-{index}", "details": {"large": "not returned"}}
            for index in range(2000)
        ],
    }
    api.JOBS_FETCH_REPORT_PATH.write_text(json.dumps(report), encoding="utf-8")

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/fetch-report", query={"view": ["summary"]})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["summaryView"] is True
    assert payload["detailLevel"] == "summary"
    assert payload["runId"] == "fetch_1"
    assert payload["summary"]["keptCount"] == 250
    assert payload["summary"]["failedSources"] == 2
    assert "sources" not in payload


def test_discovery_report_rejects_unknown_view(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/report", query={"view": ["fuller"]})

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert "unsupported discovery report view" in handler.sent[-1]["payload"]["error"]


def test_ops_health_ready_view_uses_ready_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.compute_ops_health = lambda: (_ for _ in ()).throw(
        AssertionError("ready view must not build full ops health")
    )
    api.compute_ops_health_ready = lambda: {
        "service": "baluffo-bridge",
        "startupReady": True,
        "detailLevel": "ready",
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/health", query={"view": ["ready"]})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["detailLevel"] == "ready"


def test_ops_health_rejects_unknown_view(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/health", query={"view": ["diagnostic"]})

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert "unsupported ops health view" in handler.sent[-1]["payload"]["error"]


def test_sync_status_summary_uses_config_only(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.get_sync_status_payload = lambda: (_ for _ in ()).throw(
        AssertionError("summary view must not build full sync status")
    )
    api.sync_config_status = lambda: {
        "enabled": True,
        "ready": True,
        "repo": "deathuman/Baluffo",
        "credentialsPackaged": True,
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/sync/status", query={"view": ["summary"]})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["summaryView"] is True
    assert payload["detailLevel"] == "summary"
    assert payload["config"]["enabled"] is True
    assert payload["savedConfig"]["enabled"] is True
    assert payload["config"]["ready"] is True
    assert payload["config"]["credentialsPackaged"] is True


def test_sync_status_rejects_unknown_view(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/sync/status", query={"view": ["verbose"]})

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert "unsupported sync status view" in handler.sent[-1]["payload"]["error"]
