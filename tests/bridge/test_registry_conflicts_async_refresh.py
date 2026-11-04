from __future__ import annotations

from pathlib import Path
from typing import Any

from src.bridge.registry_conflicts_summary import (
    build_registry_conflicts_summary_cache_key,
    write_registry_conflicts_full_cache,
)
from src.bridge.routes import get_registry_conflicts as route_module
from src.bridge.routes.get_registry_conflicts import handle_registry_conflict_routes
from tests.bridge.test_registry_conflicts_route_capability import (
    MinimalRegistryConflictsRouteApi,
)
from tests.helpers.bridge_api import FakeHandler


def _source_state_path(api: MinimalRegistryConflictsRouteApi) -> Path:
    return api.JOBS_FETCH_REPORT_PATH.with_name("jobs-source-state.json")


def _cache_key(api: MinimalRegistryConflictsRouteApi) -> str:
    return build_registry_conflicts_summary_cache_key(
        registry_summary=api.get_registry_summary_payload(),
        source_state_path=_source_state_path(api),
        adjudication_payload={},
    )


def _payload() -> dict[str, Any]:
    return {
        "summary": {"conflictCount": 1, "familyCount": 1, "rowCount": 2},
        "triage": {},
        "review": {},
        "automation": {},
        "conflicts": [
            {
                "familyKey": "family-a",
                "reviewPriority": 1,
                "reviewQueue": "p1_active_provider_static",
                "rowCount": 2,
                "winner": {"id": "winner-1"},
                "losers": [{"id": "loser-1"}],
            }
        ],
        "suppressedIndependentProviderBoards": {},
    }


def _recording_schedule(scheduled: list[str]) -> Any:
    def fake_schedule(**kwargs: Any) -> bool:
        scheduled.append(str(kwargs["cache_key"]))
        return True

    return fake_schedule


def test_detail_serves_stale_cache_and_requests_one_refresh(
    tmp_path: Path, monkeypatch: Any
) -> None:
    api = MinimalRegistryConflictsRouteApi(tmp_path)
    scheduled: list[str] = []

    def fake_schedule(**kwargs: Any) -> bool:
        scheduled.append(kwargs["cache_key"])
        return True

    monkeypatch.setattr(route_module, "schedule_registry_conflicts_refresh", fake_schedule)
    write_registry_conflicts_full_cache(_source_state_path(api), "older-key", _payload())

    handler = FakeHandler()
    assert handle_registry_conflict_routes(
        handler,
        api=api,
        path="/registry/conflicts",
        query={"limit": ["50"]},
    )
    payload = handler.sent[-1]["payload"]
    assert payload["fullCacheStatus"] == "stale"
    assert payload["refreshInFlight"] is True
    assert [card["familyKey"] for card in payload["conflicts"]] == ["family-a"]
    assert scheduled == [_cache_key(api)]


def test_detail_serves_fresh_cache_without_refresh(tmp_path: Path, monkeypatch: Any) -> None:
    api = MinimalRegistryConflictsRouteApi(tmp_path)
    scheduled: list[str] = []
    monkeypatch.setattr(
        route_module,
        "schedule_registry_conflicts_refresh",
        _recording_schedule(scheduled),
    )
    write_registry_conflicts_full_cache(_source_state_path(api), _cache_key(api), _payload())

    handler = FakeHandler()
    assert handle_registry_conflict_routes(handler, api=api, path="/registry/conflicts", query={})
    payload = handler.sent[-1]["payload"]
    assert payload["fullCacheStatus"] == "fresh"
    assert payload["refreshInFlight"] is False
    assert scheduled == []


def test_detail_returns_building_without_cache(tmp_path: Path, monkeypatch: Any) -> None:
    api = MinimalRegistryConflictsRouteApi(tmp_path)
    scheduled: list[str] = []
    monkeypatch.setattr(
        route_module,
        "schedule_registry_conflicts_refresh",
        _recording_schedule(scheduled),
    )

    handler = FakeHandler()
    assert handle_registry_conflict_routes(
        handler, api=api, path="/registry/conflicts", query={"limit": ["50"]}
    )
    payload = handler.sent[-1]["payload"]
    assert payload["detailStatus"] == "building"
    assert payload["fullCacheStatus"] == "building"
    assert payload["refreshInFlight"] is True
    assert payload["conflicts"] == []
    assert scheduled == [_cache_key(api)]
