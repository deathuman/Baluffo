from __future__ import annotations

from pathlib import Path
from typing import Any

from src.bridge.routes.post_routes import handle_post
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api

SAFE_ID = "recruitee:api_url:https://jobs.crazygames.com/api/offers/"
FOCUS_ALIAS_ID = "recruitee:api_url:https://focusentertainment.recruitee.com/api/offers/"
STATIC_SAFE_ID = "static:listing_url:https://www.4a-games.com.mt/careers"
PENDING_PROVIDER_ID = "greenhouse:slug:replace-static"
ACTIVE_STATIC_ID = "static:listing_url:https://replace-static.example/careers"


def _safe_auto_demote_state() -> dict[str, list[dict[str, Any]]]:
    return {
        "active": [
            {
                "id": "recruitee:api_url:https://crazygames.recruitee.com/api/offers/",
                "name": "CrazyGames (Recruitee)",
                "studio": "CrazyGames",
                "adapter": "recruitee",
                "registryState": "active",
                "jobsFound": 6,
                "rankScore": 51,
                "score": 29,
            },
            {
                "id": SAFE_ID,
                "name": "CrazyGames (Recruitee)",
                "studio": "CrazyGames",
                "adapter": "recruitee",
                "registryState": "active",
                "rankScore": 0,
            },
            {
                "id": "recruitee:api_url:https://focushomeinteractive.recruitee.com/api/offers/",
                "name": "Focus Entertainment (Recruitee)",
                "studio": "Focus Entertainment",
                "adapter": "recruitee",
                "registryState": "active",
                "jobsFound": 2,
                "rankScore": 41,
                "score": 24,
            },
            {
                "id": FOCUS_ALIAS_ID,
                "name": "Focus Entertainment (Recruitee)",
                "studio": "Focus Entertainment",
                "adapter": "recruitee",
                "registryState": "active",
                "jobsFound": 2,
                "rankScore": 33,
                "score": 25,
            },
            {
                "id": "static:listing_url:https://4a-games.com.mt/careers",
                "name": "4A Games",
                "studio": "4A Games",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 3,
                "rankScore": 39,
                "score": 23,
            },
            {
                "id": STATIC_SAFE_ID,
                "name": "4A Games",
                "studio": "4A Games",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 3,
                "rankScore": 30,
                "score": 20,
            },
            {
                "id": ACTIVE_STATIC_ID,
                "name": "Replace Static",
                "studio": "Replace Static",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 2,
                "rankScore": 20,
                "score": 10,
            },
        ],
        "pending": [],
        "rejected": [],
    }


def _pending_provider_replacement_state() -> dict[str, list[dict[str, Any]]]:
    state = _safe_auto_demote_state()
    state["pending"] = [
        {
            "id": PENDING_PROVIDER_ID,
            "name": "Replace Static (Greenhouse)",
            "studio": "Replace Static",
            "adapter": "greenhouse",
            "registryState": "pending",
            "jobsFound": 7,
            "rankScore": 0,
            "score": 0,
        },
    ]
    return state


def _seed_api(tmp_path: Path):
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.persist_state_and_auto_sync(_safe_auto_demote_state())
    return api, FakeHandler()


def test_safe_auto_demote_route_demotes_all_eligible_targets(tmp_path: Path) -> None:
    api, handler = _seed_api(tmp_path)

    result = handle_post(
        handler,
        api=api,
        path="/registry/conflicts/auto-demote-safe",
        payload={"action": "auto_demote_same_adapter_provider_alias", "ids": []},
    )

    assert result is True
    payload = handler.sent[-1]["payload"]
    assert payload["ok"] is True
    assert payload["demoted"] == 2
    assert payload["skipped"] == 0
    state = api.load_state()
    assert [row["id"] for row in state["pending"]] == [SAFE_ID, FOCUS_ALIAS_ID]
    assert state["pending"][0]["stateChangedBy"] == "registry_conflict_safe_auto_demote"
    assert FOCUS_ALIAS_ID not in {row["id"] for row in state["active"]}
    assert STATIC_SAFE_ID in {row["id"] for row in state["active"]}


def test_safe_auto_demote_route_demotes_requested_provider_aliases(tmp_path: Path) -> None:
    api, handler = _seed_api(tmp_path)

    result = handle_post(
        handler,
        api=api,
        path="/registry/conflicts/auto-demote-safe",
        payload={
            "action": "auto_demote_same_adapter_provider_alias",
            "ids": [SAFE_ID, FOCUS_ALIAS_ID],
        },
    )

    assert result is True
    payload = handler.sent[-1]["payload"]
    assert payload["demoted"] == 2
    assert payload["skipped"] == 0
    assert payload["skippedRows"] == []
    state = api.load_state()
    assert {row["id"] for row in state["pending"]} == {SAFE_ID, FOCUS_ALIAS_ID}
    assert FOCUS_ALIAS_ID not in {row["id"] for row in state["active"]}


def test_safe_auto_demote_route_demotes_static_normalized_url_aliases(tmp_path: Path) -> None:
    api, handler = _seed_api(tmp_path)

    result = handle_post(
        handler,
        api=api,
        path="/registry/conflicts/auto-demote-safe",
        payload={"action": "auto_demote_static_normalized_url_alias", "ids": []},
    )

    assert result is True
    payload = handler.sent[-1]["payload"]
    assert payload["demoted"] == 1
    assert payload["skipped"] == 0
    assert payload["applied"] == [
        {
            "id": STATIC_SAFE_ID,
            "familyKey": "4a games",
            "action": "auto_demote_static_normalized_url_alias",
        }
    ]
    state = api.load_state()
    assert {row["id"] for row in state["pending"]} == {STATIC_SAFE_ID}
    assert SAFE_ID in {row["id"] for row in state["active"]}


def test_safe_auto_demote_route_promotes_pending_provider_replacement(
    tmp_path: Path,
) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.persist_state_and_auto_sync(_pending_provider_replacement_state())
    handler = FakeHandler()

    result = handle_post(
        handler,
        api=api,
        path="/registry/conflicts/auto-demote-safe",
        payload={"action": "auto_promote_pending_provider_higher_jobs", "ids": []},
    )

    assert result is True
    payload = handler.sent[-1]["payload"]
    assert payload["demoted"] == 1
    assert payload["skipped"] == 0
    assert payload["applied"] == [
        {
            "id": PENDING_PROVIDER_ID,
            "familyKey": "replace static",
            "action": "auto_promote_pending_provider_higher_jobs",
        }
    ]
    state = api.load_state()
    active_ids = {row["id"] for row in state["active"]}
    pending_ids = {row["id"] for row in state["pending"]}
    assert PENDING_PROVIDER_ID in active_ids
    assert ACTIVE_STATIC_ID in pending_ids
    promoted = next(row for row in state["active"] if row["id"] == PENDING_PROVIDER_ID)
    demoted = next(row for row in state["pending"] if row["id"] == ACTIVE_STATIC_ID)
    assert promoted["stateChangedBy"] == "registry_conflict_safe_auto_demote"
    assert demoted["stateChangedBy"] == "registry_conflict_safe_auto_demote"
