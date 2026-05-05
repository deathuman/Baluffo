from __future__ import annotations

from pathlib import Path
from typing import Any

from src.bridge.routes.post_routes import handle_post
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api

SAFE_ID = "recruitee:api_url:https://jobs.crazygames.com/api/offers/"
UNSAFE_ID = "recruitee:api_url:https://focusentertainment.recruitee.com/api/offers/"


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
                "id": UNSAFE_ID,
                "name": "Focus Entertainment (Recruitee)",
                "studio": "Focus Entertainment",
                "adapter": "recruitee",
                "registryState": "active",
                "jobsFound": 2,
                "rankScore": 33,
                "score": 25,
            },
        ],
        "pending": [],
        "rejected": [],
    }


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
    assert payload["demoted"] == 1
    assert payload["skipped"] == 0
    state = api.load_state()
    assert [row["id"] for row in state["pending"]] == [SAFE_ID]
    assert state["pending"][0]["stateChangedBy"] == "registry_conflict_safe_auto_demote"
    assert UNSAFE_ID in {row["id"] for row in state["active"]}


def test_safe_auto_demote_route_skips_requested_unsafe_ids(tmp_path: Path) -> None:
    api, handler = _seed_api(tmp_path)

    result = handle_post(
        handler,
        api=api,
        path="/registry/conflicts/auto-demote-safe",
        payload={"action": "auto_demote_same_adapter_provider_alias", "ids": [SAFE_ID, UNSAFE_ID]},
    )

    assert result is True
    payload = handler.sent[-1]["payload"]
    assert payload["demoted"] == 1
    assert payload["skipped"] == 1
    assert payload["skippedRows"] == [
        {"id": UNSAFE_ID, "reason": "not_currently_safe_auto_demote_eligible"}
    ]
    state = api.load_state()
    assert {row["id"] for row in state["pending"]} == {SAFE_ID}
    assert UNSAFE_ID in {row["id"] for row in state["active"]}
