from __future__ import annotations

import json
from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def _install_empty_discovery_candidates(api, tmp_path: Path) -> None:
    api.DISCOVERY_CANDIDATES_PATH = tmp_path / "source-discovery-candidates.json"  # type: ignore[assignment]
    api.DISCOVERY_CANDIDATES_PATH.write_text("[]", encoding="utf-8")


def test_registry_sources_default_view_preserves_full_rows(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.load_state = lambda: {  # type: ignore[assignment]
        "active": [
            {
                "id": "active_1",
                "name": "Active",
                "pages": ["https://active.example/jobs", "https://active.example/jobs/2"],
                "detailPagesSample": ["https://active.example/detail"],
            }
        ],
        "pending": [],
        "rejected": [],
    }
    _install_empty_discovery_candidates(api, tmp_path)

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/registry/sources",
        query={"buckets": ["active"]},
    )

    payload = handler.sent[-1]["payload"]
    active_row = payload["sources"]["active"][0]
    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert "detailLevel" not in payload
    assert active_row["pages"] == [
        "https://active.example/jobs",
        "https://active.example/jobs/2",
    ]
    assert active_row["detailPagesSample"] == ["https://active.example/detail"]


def test_registry_sources_table_view_returns_compact_rows(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    large_detail = "x" * 10_000
    api.load_state = lambda: {  # type: ignore[assignment]
        "active": [
            {
                "id": "active_1",
                "name": "Active",
                "studio": "Studio",
                "company": "Company",
                "adapter": "greenhouse",
                "listing_url": "https://active.example/jobs",
                "pages": ["https://active.example/jobs", "https://active.example/jobs/2"],
                "detailPagesSample": [large_detail],
                "sourceDirectory": {"raw": large_detail},
                "rawLargePayload": {"raw": large_detail},
                "rankReasons": [
                    "low_priority",
                    "existing_family_match",
                    *[f"reason_{index}" for index in range(20)],
                ],
                "reasons": ["existing_registry_match", "because_0", "because_1"],
                "stateChangedBy": "operator",
                "registryState": "active",
            }
        ],
        "pending": [
            {
                "id": "pending_1",
                "sourceId": "source_pending_1",
                "name": "Pending",
                "jobsFound": 3,
                "sampleCount": 3,
                "lastProbeError": "",
                "hiddenFromDefault": False,
                "candidateState": "pending",
                "deferred": True,
                "deferReason": "adapter_cap",
                "weakSignal": True,
                "pages": ["https://pending.example/jobs", "https://pending.example/other"],
            }
        ],
        "rejected": [],
    }
    _install_empty_discovery_candidates(api, tmp_path)

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/registry/sources",
        query={"view": ["table"], "buckets": ["pending,active,rejected"]},
    )

    payload = handler.sent[-1]["payload"]
    active_row = payload["sources"]["active"][0]
    pending_row = payload["sources"]["pending"][0]
    encoded_size = len(json.dumps(payload).encode("utf-8"))
    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert payload["detailLevel"] == "table"
    assert payload["summaryView"] is True
    assert active_row["id"] == "active_1"
    assert active_row["listing_url"] == "https://active.example/jobs"
    assert active_row["stateChangedBy"] == "operator"
    assert active_row["registryState"] == "active"
    assert "pages" not in active_row
    assert active_row["rankReasons"] == ["existing_family_match"]
    assert active_row["reasons"] == ["existing_registry_match"]
    assert pending_row["sourceId"] == "source_pending_1"
    assert pending_row["registryState"] == "pending"
    assert "candidateState" not in pending_row
    assert pending_row["jobsFound"] == 3
    assert pending_row["deferReason"] == "adapter_cap"
    assert pending_row["weakSignal"] is True
    assert pending_row["pages"] == ["https://pending.example/jobs"]
    assert "detailPagesSample" not in active_row
    assert "sourceDirectory" not in active_row
    assert "rawLargePayload" not in active_row
    assert encoded_size < 5_000


def test_registry_sources_table_view_preserves_hidden_pending_filter(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.load_state = lambda: {  # type: ignore[assignment]
        "active": [],
        "pending": [
            {"id": "visible", "name": "Visible", "jobsFound": 1},
            {"id": "hidden", "name": "Hidden", "jobsFound": 0, "hiddenFromDefault": True},
        ],
        "rejected": [],
    }
    _install_empty_discovery_candidates(api, tmp_path)

    hidden_handler = FakeHandler()
    assert handle_get(
        hidden_handler,
        api=api,
        path="/registry/sources",
        query={"view": ["table"], "buckets": ["pending"], "includeHiddenPending": ["0"]},
    )
    shown_handler = FakeHandler()
    assert handle_get(
        shown_handler,
        api=api,
        path="/registry/sources",
        query={"view": ["table"], "buckets": ["pending"], "includeHiddenPending": ["1"]},
    )

    assert [row["id"] for row in hidden_handler.sent[-1]["payload"]["sources"]["pending"]] == [
        "visible"
    ]
    assert [row["id"] for row in shown_handler.sent[-1]["payload"]["sources"]["pending"]] == [
        "visible",
        "hidden",
    ]


def test_registry_sources_table_view_keeps_large_payload_bounded(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    heavy_text = "x" * 20_000
    api.load_state = lambda: {  # type: ignore[assignment]
        "active": [
            {
                "id": f"active_{index}",
                "name": f"Active {index}",
                "listing_url": f"https://active.example/{index}/jobs",
                "pages": [
                    f"https://active.example/{index}/jobs",
                    f"https://active.example/{index}/jobs/2",
                ],
                "detailPagesSample": [heavy_text],
                "sourceDirectory": {"raw": heavy_text},
            }
            for index in range(250)
        ],
        "pending": [],
        "rejected": [],
    }
    _install_empty_discovery_candidates(api, tmp_path)

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/registry/sources",
        query={"view": ["table"], "buckets": ["active"]},
    )

    payload = handler.sent[-1]["payload"]
    encoded_size = len(json.dumps(payload).encode("utf-8"))
    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert len(payload["sources"]["active"]) == 250
    assert "detailPagesSample" not in payload["sources"]["active"][0]
    assert "sourceDirectory" not in payload["sources"]["active"][0]
    assert encoded_size < 128 * 1024


def test_registry_sources_rejects_unknown_view(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.load_state = lambda: (_ for _ in ()).throw(AssertionError("load_state not expected"))  # type: ignore[assignment]

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/registry/sources",
        query={"view": ["compact"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert handler.sent[-1]["payload"]["ok"] is False
    assert handler.sent[-1]["payload"]["invalidView"] == "compact"
