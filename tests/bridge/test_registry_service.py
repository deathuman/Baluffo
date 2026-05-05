from __future__ import annotations

import json
from pathlib import Path

from src import source_registry as sr
from src.bridge.registry_service import RegistryPaths, RegistryService


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_registry_service_auto_heals_duplicate_active_pending_source_id(
    tmp_path: Path,
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    duplicate_id = "greenhouse:slug:guerrilla-games"
    active_row = {
        "id": duplicate_id,
        "name": "Guerrilla Games",
        "adapter": "greenhouse",
        "slug": "guerrilla-games",
        "registryState": "active",
        "stateChangedAt": "2026-05-01T00:00:00+00:00",
        "stateChangedBy": "test",
    }
    pending_row = {
        **active_row,
        "registryState": "pending",
        "pendingReason": "discovery_candidate",
        "stateChangedAt": "2026-05-02T00:00:00+00:00",
    }
    _write_json(active_path, [active_row])
    _write_json(pending_path, [pending_row])
    _write_json(rejected_path, [])
    service = RegistryService(
        paths=RegistryPaths(
            active=active_path,
            pending=pending_path,
            rejected=rejected_path,
        ),
        default_active=[],
        normalize_manual_static=lambda row: row,
    )

    state = service.load_state()
    report = service.get_auto_heal_report()

    assert [row["id"] for row in state["active"]] == [duplicate_id]
    assert state["pending"] == []
    assert sr.load_json_array(active_path, [])[0]["registryState"] == "active"
    assert sr.load_json_array(pending_path, []) == []
    assert report["autoHealed"] is True
    assert report["duplicateSourceIdCount"] == 1
    assert report["duplicates"] == [
        {
            "sourceId": duplicate_id,
            "keptBucket": "active",
            "removedBucket": "pending",
            "keptName": "Guerrilla Games",
            "removedName": "Guerrilla Games",
            "mergedFields": [],
        }
    ]
