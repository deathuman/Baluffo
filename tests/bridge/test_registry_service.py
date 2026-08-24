from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from src import source_registry as sr
from src.bridge import registry_service as registry_service_module
from src.bridge.registry_service import RegistryPaths, RegistryService
from src.bridge.storage_health import (
    close_storage_stores,
    get_storage_health_payload,
    get_storage_store,
)
from src.storage.source_registry_runtime import SourceRegistryRuntimeStore


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


def test_registry_service_auto_demotes_safe_static_url_alias_on_load(tmp_path: Path) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    winner_id = "static:listing_url:https://studio.example/careers"
    loser_id = "static:listing_url:https://www.studio.example/careers"
    _write_json(
        active_path,
        [
            {
                "id": winner_id,
                "name": "Static Studio",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 3,
                "rankScore": 40,
                "score": 20,
            },
            {
                "id": loser_id,
                "name": "Static Studio",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 3,
                "rankScore": 20,
                "score": 10,
            },
        ],
    )
    _write_json(pending_path, [])
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

    assert [row["id"] for row in state["active"]] == [winner_id]
    assert [row["id"] for row in state["pending"]] == [loser_id]
    assert state["pending"][0]["pendingReason"] == "registry_conflict_safe_auto_demote"
    assert state["pending"][0]["stateChangedBy"] == "registry_conflict_safe_auto_demote"
    assert [row["id"] for row in sr.load_json_array(active_path, [])] == [winner_id]
    assert [row["id"] for row in sr.load_json_array(pending_path, [])] == [loser_id]
    assert report["autoHealed"] is True
    assert report["duplicateSourceIdCount"] == 0
    assert report["safeAutomation"]["autoDemoted"] is True
    assert report["safeAutomation"]["demoted"] == 1
    assert report["safeAutomation"]["applied"] == [
        {
            "id": loser_id,
            "familyKey": "static studio",
            "action": "auto_demote_static_normalized_url_alias",
        }
    ]


def test_registry_service_load_does_not_auto_demote_discovery_auto_approved_row(
    tmp_path: Path,
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    winner_id = "static:listing_url:https://studio.example/careers"
    loser_id = "static:listing_url:https://www.studio.example/careers"
    _write_json(
        active_path,
        [
            {
                "id": winner_id,
                "name": "Static Studio",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 3,
                "rankScore": 40,
                "score": 20,
            },
            {
                "id": loser_id,
                "name": "Static Studio",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "active",
                "stateChangedBy": "discovery_auto_approve",
                "approvedBy": "registry_migration_v2",
                "jobsFound": 3,
                "rankScore": 20,
                "score": 10,
            },
        ],
    )
    _write_json(pending_path, [])
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

    assert [row["id"] for row in state["active"]] == [winner_id, loser_id]
    assert state["pending"] == []
    assert report["safeAutomation"]["autoDemoted"] is False
    assert report["safeAutomation"]["demoted"] == 0
    assert report["safeAutomation"]["skippedRows"] == [
        {
            "id": loser_id,
            "reason": "protected_from_load_time_safe_auto_demote",
        }
    ]


def test_registry_service_auto_demotes_safe_static_listing_variant_on_load(
    tmp_path: Path,
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    winner_id = "static:listing_url:https://www.rockstargames.com/careers/openings"
    loser_id = "static:listing_url:https://www.rockstargames.com/careers"
    _write_json(
        active_path,
        [
            {
                "id": winner_id,
                "name": "Rockstar Games",
                "studio": "Rockstar Games",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 13,
                "rankScore": 60,
                "score": 27,
            },
            {
                "id": loser_id,
                "name": "Rockstar Games",
                "studio": "Rockstar Games",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 1,
                "rankScore": 10,
                "score": 7,
            },
        ],
    )
    _write_json(pending_path, [])
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

    assert [row["id"] for row in state["active"]] == [winner_id]
    assert [row["id"] for row in state["pending"]] == [loser_id]
    assert report["safeAutomation"]["demoted"] == 1
    assert report["safeAutomation"]["applied"] == [
        {
            "id": loser_id,
            "familyKey": "rockstar games",
            "action": "auto_demote_static_same_host_listing_variant",
        }
    ]


def test_registry_service_shadow_writes_sqlite_projection(tmp_path: Path) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    state = {
        "active": [{"id": "active", "name": "Active", "adapter": "static"}],
        "pending": [{"id": "pending", "name": "Pending", "adapter": "lever"}],
        "rejected": [],
    }
    try:
        store = get_storage_store(tmp_path)
        store.set_authority_mode("sourceRegistry", "shadow", reason="test-shadow")
        service = RegistryService(
            paths=RegistryPaths(
                active=active_path,
                pending=pending_path,
                rejected=rejected_path,
            ),
            default_active=[],
            normalize_manual_static=lambda row: row,
        )

        normalized = service.persist_state(state)
        runtime = SourceRegistryRuntimeStore(store)
        health = get_storage_health_payload(tmp_path)["storage"]

        assert store.get_authority_modes()["sourceRegistry"] == "shadow"
        assert runtime.current_state() == normalized
        assert [
            row["code"] for row in health["diagnostics"] if row["surface"] == "sourceRegistry"
        ] == ["source_registry_projection_match"]
    finally:
        close_storage_stores()


def test_registry_service_shadow_projection_mismatch_rolls_back_to_json(tmp_path: Path) -> None:
    class _FakeAuthorityStore:
        def __init__(self) -> None:
            self.mode = "shadow"

        def get_authority_modes(self) -> dict[str, str]:
            return {"sourceRegistry": self.mode}

        def set_authority_mode(self, surface: str, mode: str, *, reason: str = "") -> None:
            assert surface == "sourceRegistry"
            assert reason == "source_registry_projection_mismatch"
            self.mode = mode

    class _FakeRuntimeStore:
        def __init__(self) -> None:
            self.store = _FakeAuthorityStore()

        def replace_state(self, **_: Any) -> Any:
            return type(
                "Summary",
                (),
                {
                    "generation": "bad",
                    "to_dict": lambda _self: {"generation": "bad"},
                },
            )()

        def parity_hash(self) -> dict[str, str]:
            return {"stateHash": "bad", "tombstoneHash": "bad"}

        def cleanup_old_generations(self) -> int:
            return 0

    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    fake_runtime = _FakeRuntimeStore()
    diagnostics: list[dict[str, Any]] = []

    def _record_diagnostic(_data_dir: Path, **kwargs: Any) -> None:
        diagnostics.append(dict(kwargs))

    service = RegistryService(
        paths=RegistryPaths(
            active=active_path,
            pending=pending_path,
            rejected=rejected_path,
        ),
        default_active=[],
        normalize_manual_static=lambda row: row,
        runtime_store_factory=lambda: cast(SourceRegistryRuntimeStore, fake_runtime),
        record_storage_diagnostic=_record_diagnostic,
    )

    service.persist_state({"active": [{"id": "a", "name": "A"}], "pending": [], "rejected": []})

    assert fake_runtime.store.mode == "json"
    assert diagnostics[-1]["code"] == "source_registry_projection_mismatch"
    assert diagnostics[-1]["ok"] is False


def test_registry_service_sqlite_authority_seeds_and_exports_json(tmp_path: Path) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    _write_json(active_path, [{"id": "a", "name": "Active", "adapter": "static"}])
    _write_json(pending_path, [])
    _write_json(rejected_path, [])
    try:
        store = get_storage_store(tmp_path)
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
        runtime = SourceRegistryRuntimeStore(store)

        assert store.get_authority_modes()["sourceRegistry"] == "sqlite"
        assert runtime.current_state() == state
        assert [row["id"] for row in sr.load_json_array(active_path, [])] == ["a"]
        assert any(
            row["code"] == "source_registry_seeded_from_json"
            for row in get_storage_health_payload(tmp_path)["storage"]["diagnostics"]
        )
    finally:
        close_storage_stores()


def test_registry_service_sqlite_tombstones_update_generation_and_export_json(
    tmp_path: Path,
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    try:
        store = get_storage_store(tmp_path)
        service = RegistryService(
            paths=RegistryPaths(
                active=active_path,
                pending=pending_path,
                rejected=rejected_path,
            ),
            default_active=[{"id": "a", "name": "Active", "adapter": "static"}],
            normalize_manual_static=lambda row: row,
        )
        service.load_state()

        saved = service.save_tombstones(
            {"a": {"sourceId": "a", "reason": "manual", "bucket": "active"}}
        )

        runtime = SourceRegistryRuntimeStore(store)
        assert runtime.current_tombstones() == saved
        assert sr.load_json_object(tmp_path / "source-registry-tombstones.json", {}) == saved
    finally:
        close_storage_stores()


def test_registry_service_sqlite_json_drift_rolls_back_to_json(tmp_path: Path) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    try:
        store = get_storage_store(tmp_path)
        service = RegistryService(
            paths=RegistryPaths(
                active=active_path,
                pending=pending_path,
                rejected=rejected_path,
            ),
            default_active=[{"id": "a", "name": "Active", "adapter": "static"}],
            normalize_manual_static=lambda row: row,
        )
        service.load_state()
        sr.save_json_atomic(
            active_path,
            [{"id": "json-only", "name": "JSON", "adapter": "static"}],
        )

        state = service.load_state()

        assert store.get_authority_modes()["sourceRegistry"] == "json"
        assert [row["id"] for row in state["active"]] == ["json-only"]
        assert any(
            row["code"] == "source_registry_json_sqlite_mismatch"
            for row in get_storage_health_payload(tmp_path)["storage"]["diagnostics"]
        )
    finally:
        close_storage_stores()


def test_registry_service_json_authority_summary_avoids_full_normalization(
    tmp_path: Path, monkeypatch: Any
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    sr.save_json_atomic(active_path, [{"id": "active-1", "name": "Active", "adapter": "static"}])
    sr.save_json_atomic(
        pending_path,
        [
            {"id": "pending-1", "name": "Pending", "adapter": "greenhouse"},
            {"id": "pending-2", "name": "Pending 2", "adapter": "lever"},
        ],
    )
    sr.save_json_atomic(rejected_path, [{"id": "rejected-1", "name": "Rejected"}])

    def fail_safe_demotions(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("summary computed full conflict demotions")

    monkeypatch.setattr(
        registry_service_module,
        "apply_registry_conflict_safe_demotions",
        fail_safe_demotions,
    )
    try:
        store = get_storage_store(tmp_path)
        store.set_authority_mode("sourceRegistry", "json", reason="test-json-authority")
        service = RegistryService(
            paths=RegistryPaths(
                active=active_path,
                pending=pending_path,
                rejected=rejected_path,
            ),
            default_active=[],
            normalize_manual_static=lambda row: row,
        )

        summary = service.get_summary_payload()

        assert summary["generation"] == ""
        assert summary["reason"] == "json_summary"
        assert summary["summaryExact"] is False
        assert summary["activeCount"] == 1
        assert summary["pendingCount"] == 2
        assert summary["rejectedCount"] == 1
        assert summary["countBasis"] == "storage"
        assert summary["stateFingerprint"]
    finally:
        close_storage_stores()


def test_registry_service_exact_summary_uses_normalized_rows_without_saving(
    tmp_path: Path, monkeypatch: Any
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    sr.save_json_atomic(
        active_path,
        [{"id": "active-1", "name": "Active", "adapter": "static"}],
    )
    sr.save_json_atomic(
        pending_path,
        [
            {
                "id": "pending-hidden",
                "name": "Hidden",
                "adapter": "greenhouse",
                "hiddenFromDefault": True,
                "deferReason": "cap",
            },
            {
                "id": "pending-duplicate",
                "name": "Duplicate",
                "adapter": "lever",
                "pendingReason": "duplicate source",
            },
        ],
    )
    sr.save_json_atomic(rejected_path, [{"id": "rejected-1", "name": "Rejected"}])

    def fail_save(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("exact summary must not persist registry state")

    monkeypatch.setattr(registry_service_module, "save_json_atomic", fail_save)
    try:
        store = get_storage_store(tmp_path)
        store.set_authority_mode("sourceRegistry", "json", reason="test-json-authority")
        service = RegistryService(
            paths=RegistryPaths(
                active=active_path,
                pending=pending_path,
                rejected=rejected_path,
            ),
            default_active=[],
            normalize_manual_static=lambda row: row,
        )

        summary = service.get_exact_summary_payload()

        assert summary["summaryExact"] is True
        assert summary["countBasis"] == "normalized"
        assert summary["activeCount"] == 1
        assert summary["pendingCount"] == 2
        assert summary["rejectedCount"] == 1
        assert summary["hiddenPendingCount"] == 1
        assert summary["deferredPendingCount"] == 1
        assert summary["duplicatePendingCount"] == 1
        assert summary["invalidRowsCount"] == 0
        assert summary["stateHash"]
        assert summary["stateFingerprint"] == summary["stateHash"]
        assert "storage" not in summary
    finally:
        close_storage_stores()


def test_registry_service_json_authority_compact_table_serves_rows(tmp_path: Path) -> None:
    """JSON authority must serve real compact rows, not a degraded-empty stub.

    The Admin startup lane always requests /registry/sources?detail=summary,
    which routes to get_compact_table_payload; the old json-mode stub returned
    degraded + zero rows, leaving source tables stuck on "refreshing" forever.
    """
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    sr.save_json_atomic(
        active_path,
        [{"id": f"active-{i}", "name": f"Active {i}", "adapter": "static"} for i in range(3)],
    )
    sr.save_json_atomic(
        pending_path,
        [
            {"id": "pending-1", "name": "Pending", "adapter": "greenhouse"},
            {
                "id": "pending-hidden",
                "name": "Hidden",
                "adapter": "lever",
                "hiddenFromDefault": True,
            },
        ],
    )
    sr.save_json_atomic(rejected_path, [{"id": "rejected-1", "name": "Rejected"}])

    try:
        store = get_storage_store(tmp_path)
        store.set_authority_mode("sourceRegistry", "json", reason="test-json-compact")
        service = RegistryService(
            paths=RegistryPaths(
                active=active_path,
                pending=pending_path,
                rejected=rejected_path,
            ),
            default_active=[],
            normalize_manual_static=lambda row: row,
        )

        payload = service.get_compact_table_payload(
            buckets=["pending", "active", "rejected"],
            limit_per_bucket=2,
            include_hidden_pending=False,
        )

        assert payload["ok"] is True
        assert "degraded" not in payload
        assert payload["source"] == "registry-json-table"
        # Hidden pending filtered by default; active limited to the bucket cap.
        assert [row["id"] for row in payload["sources"]["pending"]] == ["pending-1"]
        assert len(payload["sources"]["active"]) == 2
        assert len(payload["sources"]["rejected"]) == 1
        # Summary counts stay exact regardless of the per-bucket row limit.
        assert payload["summary"]["activeCount"] == 3
        assert payload["summary"]["pendingCount"] == 2
        assert payload["summary"]["rejectedCount"] == 1
        assert payload["summary"]["countBasis"] == "normalized"
        assert payload["summary"]["tableLimitPerBucket"] == 2

        hidden_payload = service.get_compact_table_payload(
            buckets=["pending"],
            limit_per_bucket=5,
            include_hidden_pending=True,
        )
        assert len(hidden_payload["sources"]["pending"]) == 2
    finally:
        close_storage_stores()
