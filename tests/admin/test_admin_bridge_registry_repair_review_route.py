"""Admin route that records a registry-hygiene repair decision.

The route's defining property is that it *records* and never *applies*: a
recorded decision must not move a registry row. These tests pin that, because
the whole approval gate rests on the two halves staying separate.
"""

from __future__ import annotations

import json
from pathlib import Path

from src import admin_bridge
from src.bridge.routes.post_routes_admin import handle_post
from src.jobs.common.contracts_registry_repair_review import (
    read_registry_repair_review_artifact,
    registry_finding_fingerprint,
)
from tests.helpers.bridge_api import FakeHandler

_SOURCE_ID = "static:listing_url:https://gone.example/careers"


def _api():
    return admin_bridge.build_bridge_api(admin_bridge.RUNTIME_CONFIG)


def _review_path(data_root: Path) -> Path:
    return data_root / "registry-repair-review.json"


def _active_registry_path(data_root: Path) -> Path:
    return data_root / "source-registry-active.json"


def _fingerprint() -> str:
    return registry_finding_fingerprint(
        source_id=_SOURCE_ID,
        finding_kind="repair_candidate",
        registry_state="active",
        unreachable_evidence="http_404",
        affected_urls=["https://gone.example/careers"],
    )


def _post(api, path: str, payload: dict) -> dict:
    handler = FakeHandler()
    handled = handle_post(handler, api=api, path=path, payload=payload)
    assert handled is True
    return dict(handler.sent[-1]["payload"])


def test_route_records_a_decision_and_reports_it_was_not_applied(
    admin_bridge_entrypoint_root,
) -> None:
    api = _api()
    path = _review_path(admin_bridge_entrypoint_root)
    if path.exists():
        path.unlink()

    response = _post(
        api,
        "/registry/repair-review-action",
        {
            "sourceId": _SOURCE_ID,
            "findingKind": "repair_candidate",
            "registryState": "active",
            "unreachableEvidence": "http_404",
            "decision": "repair_approved",
            "approvedAction": "repoint",
            "approvedTarget": "https://new-board.example/careers",
            "evidenceFingerprint": _fingerprint(),
            "note": "board moved to a new ATS",
        },
    )

    assert response["ok"] is True
    assert response["row"]["approvedAction"] == "repoint"
    assert response["row"]["approvedTarget"] == "https://new-board.example/careers"
    assert response["summary"]["repair_approved"] == 1
    # The load-bearing assertion: recording intent is not executing it.
    assert response["applied"] is False
    assert path.exists()

    stored, warning = read_registry_repair_review_artifact(path)
    assert warning == ""
    assert stored["summary"]["repair_approved"] == 1
    row = next(iter(stored["rows"].values()))
    assert row["decidedBy"] == "admin"
    assert row["evidenceFingerprint"] == _fingerprint()


def test_route_rejects_an_invalid_decision_with_400(
    admin_bridge_entrypoint_root,
) -> None:
    api = _api()
    handler = FakeHandler()
    handled = handle_post(
        handler,
        api=api,
        path="/registry/repair-review-action",
        payload={"sourceId": _SOURCE_ID, "findingKind": "repair_candidate", "decision": "new"},
    )
    assert handled is True
    assert handler.sent[-1]["status"] == 400
    assert handler.sent[-1]["payload"]["ok"] is False


def test_route_does_not_touch_the_registry(admin_bridge_entrypoint_root) -> None:
    """A recorded decision must leave the active registry byte-identical."""
    api = _api()
    active = _active_registry_path(admin_bridge_entrypoint_root)
    before = active.read_bytes() if active.exists() else None

    _post(
        api,
        "/registry/repair-review-action",
        {
            "sourceId": _SOURCE_ID,
            "findingKind": "repair_candidate",
            "decision": "repair_approved",
            "approvedAction": "retire",
            "evidenceFingerprint": _fingerprint(),
        },
    )

    after = active.read_bytes() if active.exists() else None
    assert after == before


def test_route_surfaces_a_warning_when_the_prior_artifact_is_malformed(
    admin_bridge_entrypoint_root,
) -> None:
    api = _api()
    path = _review_path(admin_bridge_entrypoint_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json", encoding="utf-8")

    response = _post(
        api,
        "/registry/repair-review-action",
        {"sourceId": _SOURCE_ID, "findingKind": "repair_candidate", "decision": "acknowledged"},
    )

    assert response["ok"] is True
    assert response["warning"] == "malformed_registry_repair_review"
    # A corrupt gate must not silently swallow the decision being recorded.
    stored = json.loads(path.read_text(encoding="utf-8"))
    assert stored["summary"]["acknowledged"] == 1


def test_route_ignores_unrelated_paths(admin_bridge_entrypoint_root) -> None:
    api = _api()
    handler = FakeHandler()
    handled = handle_post(
        handler, api=api, path="/some/other/route", payload={"decision": "acknowledged"}
    )
    assert handled is False
