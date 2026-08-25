from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.bridge.routes.get_registry_conflicts import (
    handle_registry_conflict_routes,
    slice_registry_conflicts_for_query,
)
from tests.bridge.test_registry_conflicts_route_capability import (
    MinimalRegistryConflictsRouteApi,
)
from tests.helpers.bridge_api import FakeHandler


def _card(priority: int, queue: str, family: str) -> dict[str, Any]:
    return {
        "familyKey": family,
        "reviewPriority": priority,
        "reviewQueue": queue,
        "rows": [],
    }


def _payload(cards: list[dict[str, Any]]) -> dict[str, Any]:
    return {"conflicts": list(cards), "summary": {"conflictCount": len(cards)}}


def test_slice_pages_are_stable_and_sorted() -> None:
    cards = [
        _card(3, "p3_low_signal_manual", "zebra"),
        _card(0, "p0_multi_active_provider", "beta"),
        _card(1, "p1_active_provider_static", "alpha"),
        _card(0, "p0_multi_active_provider", "adam"),
    ]
    page = slice_registry_conflicts_for_query(_payload(cards), limit=2, offset=0, queue="")
    assert [card["familyKey"] for card in page["conflicts"]] == ["adam", "beta"]
    assert page["returnedCount"] == 2

    second = slice_registry_conflicts_for_query(_payload(cards), limit=2, offset=2, queue="")
    assert [card["familyKey"] for card in second["conflicts"]] == ["alpha", "zebra"]


def test_slice_filters_by_queue_case_insensitive() -> None:
    cards = [
        _card(3, "p3_low_signal_manual", "zebra"),
        _card(0, "p0_multi_active_provider", "beta"),
        _card(1, "P1_active_provider_static", "alpha"),
    ]
    page = slice_registry_conflicts_for_query(
        _payload(cards), limit=10, offset=0, queue="p1_active_provider_static"
    )
    assert [card["familyKey"] for card in page["conflicts"]] == ["alpha"]
    assert page["returnedCount"] == 1


def test_slice_without_params_leaves_payload_untouched() -> None:
    cards = [_card(1, "q", "a"), _card(0, "q", "b")]
    payload = _payload(list(reversed(cards)))
    sliced = slice_registry_conflicts_for_query(payload, limit=0, offset=0, queue="")
    assert sliced is payload
    assert sliced["conflicts"][0]["familyKey"] == "b"
    assert "returnedCount" not in sliced


def test_route_accepts_paging_params(tmp_path: Path) -> None:
    api = MinimalRegistryConflictsRouteApi(tmp_path)
    handler = FakeHandler()
    assert (
        handle_registry_conflict_routes(
            handler,
            api=api,
            path="/registry/conflicts",
            query={"limit": ["50"], "offset": ["0"]},
        )
        is True
    )
    payload = handler.sent[-1]["payload"]
    assert payload["ok"] is True
    assert isinstance(payload.get("returnedCount"), int)
    assert isinstance(payload.get("conflicts"), list)

    raw = json.dumps(payload)
    assert "registrySummary" in raw


def test_slice_preserves_total_conflict_count() -> None:
    cards = [_card(0, "q0", "a"), _card(1, "q1", "b"), _card(2, "q2", "c")]
    page = slice_registry_conflicts_for_query(_payload(cards), limit=2, offset=0, queue="")
    assert page["summary"]["conflictCount"] == 3


def test_route_garbage_params_stay_byte_compatible(tmp_path: Path) -> None:
    api = MinimalRegistryConflictsRouteApi(tmp_path)

    plain_handler = FakeHandler()
    assert (
        handle_registry_conflict_routes(
            plain_handler, api=api, path="/registry/conflicts", query={}
        )
        is True
    )
    plain_payload = plain_handler.sent[-1]["payload"]

    garbage_handler = FakeHandler()
    assert (
        handle_registry_conflict_routes(
            garbage_handler,
            api=api,
            path="/registry/conflicts",
            query={"limit": ["abc", "-3"], "offset": ["1.5"], "queue": [""]},
        )
        is True
    )
    garbage_payload = garbage_handler.sent[-1]["payload"]
    assert garbage_payload["conflicts"] == plain_payload["conflicts"]
    assert "returnedCount" not in garbage_payload
