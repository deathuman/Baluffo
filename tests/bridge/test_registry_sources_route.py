from __future__ import annotations

from pathlib import Path
from typing import Any

from src.bridge.routes.get_registry import handle_registry_routes
from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def _compact_stub(rows: dict[str, list[dict[str, Any]]] | None = None):
    calls: dict[str, Any] = {}

    def _payload(**kwargs: Any) -> dict[str, Any]:
        calls.update(kwargs)
        return {
            "ok": True,
            "summaryView": True,
            "detailLevel": "table",
            "source": "registry-json-table",
            "sources": rows or {"pending": [{"id": "pending_1"}], "active": [], "rejected": []},
            "summary": {
                "pendingCount": 1,
                "activeCount": 0,
                "rejectedCount": 0,
                "authorityMode": "json",
            },
        }

    return _payload, calls


def test_registry_sources_single_lane_passes_through_compact_payload(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    payload_fn, calls = _compact_stub()
    api.get_registry_compact_table_payload = payload_fn
    api.load_state = lambda: (_ for _ in ()).throw(AssertionError("load_state must not run"))

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/registry/sources",
        query={
            "view": ["table"],
            "buckets": ["pending,active,rejected"],
            "includeHiddenPending": ["1"],
            "limitPerBucket": ["120"],
        },
    )

    payload = handler.sent[-1]["payload"]
    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert payload["ok"] is True
    assert payload["detailLevel"] == "table"
    assert payload["summaryView"] is True
    assert payload["sources"]["pending"][0]["id"] == "pending_1"
    assert payload["summary"]["authorityMode"] == "json"
    # Single-lane kwargs contract passed straight to the service.
    assert calls == {
        "buckets": ["pending", "active", "rejected"],
        "limit_per_bucket": 120,
        "include_hidden_pending": True,
    }


def test_registry_sources_defaults_limit_and_hidden_filter(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    payload_fn, calls = _compact_stub()
    api.get_registry_compact_table_payload = payload_fn

    assert handle_get(FakeHandler(), api=api, path="/registry/sources", query={})

    assert calls["limit_per_bucket"] == 25
    assert calls["include_hidden_pending"] is False


def test_registry_sources_caps_limit_per_bucket_at_500(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    payload_fn, calls = _compact_stub()
    api.get_registry_compact_table_payload = payload_fn

    assert handle_get(
        FakeHandler(),
        api=api,
        path="/registry/sources",
        query={"limitPerBucket": ["9999"], "buckets": ["active"]},
    )

    assert calls["limit_per_bucket"] == 500
    assert calls["buckets"] == ["active"]


def test_registry_sources_rejects_removed_legacy_params(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())

    for query in (
        {"view": ["table"], "detail": ["full"]},
        {"view": ["table"], "detail": ["summary"]},
        {"activeCompact": ["1"]},
        {"compactActive": ["yes"]},
    ):
        handler = FakeHandler()
        result = handle_get(handler, api=api, path="/registry/sources", query=query)

        payload = handler.sent[-1]["payload"]
        assert result is True
        assert handler.sent[-1]["status"] == 400
        assert payload["ok"] is False
        assert payload["removedParams"]
        assert "compact-table lane" in payload["error"]


def test_registry_sources_rejects_unknown_view(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/registry/sources",
        query={"view": ["compact"]},
    )

    payload = handler.sent[-1]["payload"]
    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert payload["ok"] is False
    assert payload["invalidView"] == "compact"
    assert payload["allowedViews"] == ["table"]


def test_registry_sources_rejects_invalid_bucket(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())

    handler = FakeHandler()
    assert handle_get(
        handler,
        api=api,
        path="/registry/sources",
        query={"buckets": ["pending,nope"]},
    )

    payload = handler.sent[-1]["payload"]
    assert handler.sent[-1]["status"] == 400
    assert payload["invalidBuckets"] == "nope"


def test_registry_summary_exact_and_default_views(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    exact_calls: list[bool] = []

    def _exact() -> dict[str, Any]:
        exact_calls.append(True)
        return {
            "activeCount": 1,
            "updatedAt": "2026-08-24T12:00:00+00:00",
            "authorityMode": "json",
        }

    def _cheap() -> dict[str, Any]:
        raise AssertionError("default view must use get_registry_summary_payload")

    api.get_registry_exact_summary_payload = _exact

    def _summary() -> dict[str, Any]:
        return {"activeCount": 2}

    api.get_registry_summary_payload = _summary
    del _cheap

    handler = FakeHandler()
    assert handle_registry_routes(
        handler, api=api, path="/registry/summary", query={"view": ["exact"]}
    )
    payload = handler.sent[-1]["payload"]
    assert payload["summary"]["activeCount"] == 1
    assert payload["generatedAt"] == "2026-08-24T12:00:00+00:00"
    assert payload["authorityMode"] == "json"

    plain = FakeHandler()
    assert handle_registry_routes(plain, api=api, path="/registry/summary", query={})
    assert plain.sent[-1]["payload"]["summary"]["activeCount"] == 2
    assert exact_calls == [True]


def test_registry_summary_rejects_removed_aliases(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())

    for view in ("cheap", "storage"):
        handler = FakeHandler()
        result = handle_registry_routes(
            handler, api=api, path="/registry/summary", query={"view": [view]}
        )

        payload = handler.sent[-1]["payload"]
        assert result is True
        assert handler.sent[-1]["status"] == 400
        assert payload["invalidView"] == view
        assert payload["allowedViews"] == ["exact"]
