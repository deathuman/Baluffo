from __future__ import annotations

from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_registry_summary_default_view_reports_storage_basis(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.get_registry_summary_payload = lambda: {  # type: ignore[assignment]
        "activeCount": 3,
        "summaryExact": False,
        "countBasis": "storage",
        "authorityMode": "json",
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/registry/summary", query={})

    payload = handler.sent[-1]["payload"]
    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert payload["summary"]["activeCount"] == 3
    assert payload["summary"]["summaryExact"] is False
    assert payload["summary"]["countBasis"] == "storage"
    assert "sources" not in payload


def test_registry_summary_exact_view_uses_normalized_summary_payload(
    tmp_path: Path,
) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.get_registry_summary_payload = lambda: (_ for _ in ()).throw(  # type: ignore[assignment]
        AssertionError("cheap summary not expected")
    )
    api.get_registry_exact_summary_payload = lambda: {  # type: ignore[assignment]
        "activeCount": 4,
        "pendingCount": 1,
        "rejectedCount": 0,
        "summaryExact": True,
        "countBasis": "normalized",
        "authorityMode": "json",
        "updatedAt": "2026-06-04T00:00:00+00:00",
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/registry/summary", query={"view": ["exact"]})

    payload = handler.sent[-1]["payload"]
    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert payload["ok"] is True
    assert payload["generatedAt"] == "2026-06-04T00:00:00+00:00"
    assert payload["summary"]["summaryExact"] is True
    assert payload["summary"]["countBasis"] == "normalized"
    assert payload["summary"]["activeCount"] == 4
    assert "sources" not in payload


def test_registry_sources_summary_reports_normalized_basis(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.load_state = lambda: {  # type: ignore[assignment]
        "active": [{"id": "active-1", "name": "Active"}],
        "pending": [],
        "rejected": [],
    }
    api.DISCOVERY_CANDIDATES_PATH = tmp_path / "source-discovery-candidates.json"  # type: ignore[assignment]
    api.DISCOVERY_CANDIDATES_PATH.write_text("[]", encoding="utf-8")

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/registry/sources", query={})

    payload = handler.sent[-1]["payload"]
    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert payload["summary"]["summaryExact"] is True
    assert payload["summary"]["countBasis"] == "normalized"


def test_registry_summary_rejects_unknown_view(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.get_registry_summary_payload = lambda: (_ for _ in ()).throw(  # type: ignore[assignment]
        AssertionError("summary not expected")
    )
    api.get_registry_exact_summary_payload = lambda: (_ for _ in ()).throw(  # type: ignore[assignment]
        AssertionError("exact summary not expected")
    )

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/registry/summary", query={"view": ["rows"]})

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert handler.sent[-1]["payload"]["ok"] is False
    assert handler.sent[-1]["payload"]["invalidView"] == "rows"
