from __future__ import annotations

import json
from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_ops_dashboard_health_summary_route_uses_summary_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.compute_ops_dashboard_health = lambda: {"detailLevel": "full"}
    api.compute_ops_dashboard_health_summary = lambda: {
        "detailLevel": "summary",
        "summaryView": True,
    }

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/ops/dashboard-health",
        query={"view": ["summary"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"] == {"detailLevel": "summary", "summaryView": True}


def test_ops_dashboard_health_route_rejects_unknown_view(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/ops/dashboard-health",
        query={"view": ["heavy"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert "unsupported dashboard-health view" in handler.sent[-1]["payload"]["error"]


def test_ops_fetch_kpis_summary_route_returns_bounded_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.compute_ops_fetch_kpis_summary = lambda: {
        "ok": True,
        "summaryView": True,
        "detailLevel": "summary",
        "kpis": {
            "lastSuccessfulFetchAt": "2026-06-12T10:00:00+00:00",
            "lastSuccessfulFetchAge": "4m",
            "sevenDayFetchSuccessRate": 0.91,
            "avgFetchDurationMs7d": 12345,
            "failedSourceRatioLatest": 0.22,
            "pendingApprovalsCount": 812,
        },
    }

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/ops/fetch-kpis",
        query={"view": ["summary"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["detailLevel"] == "summary"
    assert payload["kpis"]["lastSuccessfulFetchAge"] == "4m"
    assert payload["kpis"]["pendingApprovalsCount"] == 812
    forbidden = {
        "sourceHealth",
        "providerCoverage",
        "dedupEvidence",
        "performanceProfile",
        "discoveryAuditArtifacts",
        "taskFailureAttempts",
        "history",
        "sources",
        "sourceRows",
    }
    assert forbidden.isdisjoint(payload)
    assert forbidden.isdisjoint(payload["kpis"])


def test_ops_fetch_kpis_route_rejects_unknown_view(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/ops/fetch-kpis",
        query={"view": ["full"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert "unsupported fetch-kpis view" in handler.sent[-1]["payload"]["error"]


def test_admin_ops_tab_counts_summary_returns_pending_for_unbounded_counts(
    tmp_path: Path,
) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.compute_ops_dashboard_health_summary = lambda: {
        "ok": True,
        "summaryView": True,
        "alertsEvaluated": True,
        "alerts": [{"id": "stale_fetch", "severity": "warning"}],
    }
    api.load_registry_conflict_adjudication = lambda: {}
    api.get_registry_auto_heal_report = lambda: {}
    api.SOURCE_POLICY_RECOMMENDATIONS_PATH = tmp_path / "source-policy-recommendations.json"
    api.SOURCE_POLICY_REVIEW_STATE_PATH = tmp_path / "source-policy-review-state.json"
    api.SOURCE_POLICY_RECOMMENDATIONS_PATH.write_text(
        json.dumps(
            {
                "pairs": [
                    {
                        "staticSourceId": "static:one",
                        "providerSourceId": "provider:one",
                        "reviewState": "new",
                    },
                    {
                        "staticSourceId": "static:two",
                        "providerSourceId": "provider:two",
                        "reviewState": "reviewed",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    api.DISCOVERY_REPORT_PATH.write_text(
        json.dumps({"summary": {"candidateCount": 3}}),
        encoding="utf-8",
    )

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/admin/ops-tab-counts",
        query={"view": ["summary"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["summaryView"] is True
    assert payload["badges"]["overview"]["count"] == 1
    assert payload["badges"]["discovery"]["count"] == 3
    assert payload["badges"]["source-policy"]["count"] == 1
    assert payload["badges"]["registry-conflicts"]["loaded"] is True
    assert payload["badges"]["dedup"]["loaded"] is False
    assert "sources" not in payload
    assert "history" not in payload


def test_admin_ops_tab_counts_rejects_unknown_view(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/admin/ops-tab-counts",
        query={"view": ["full"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert "unsupported ops-tab-counts view" in handler.sent[-1]["payload"]["error"]
