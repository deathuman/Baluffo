from __future__ import annotations

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
