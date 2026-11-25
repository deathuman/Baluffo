"""Active and rejected rows get a read-only fill-only conflict audit."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.bridge.storage_health import close_storage_stores, get_storage_store
from src.source_registry import apply_discovery_approval_to_authority
from src.source_registry_identity import source_identity
from src.storage.source_registry_runtime import SourceRegistryRuntimeStore
from tests.bridge.test_discovery_service_hardening import _make_service
from tests.helpers.mutation import append_and_return

_NOW = "2026-07-01T10:00:00+00:00"

_ACTIVE = {
    "id": "active-row",
    "adapter": "static",
    "name": "Active Authority",
    "careersUrl": "https://active.example/careers",
}
_REJECTED = {
    "id": "rejected-row",
    "adapter": "static",
    "name": "Rejected Authority",
    "careersUrl": "https://rejected.example/careers",
}
_RUN_ROWS = [
    {
        **_ACTIVE,
        "name": "Active Run",
        "careersUrl": "https://active.example/migrated",
    },
    {
        **_REJECTED,
        "name": "Rejected Run",
        "careersUrl": "https://rejected.example/migrated",
    },
]


def _paths(tmp_path: Path) -> dict[str, Path]:
    return {
        "active": tmp_path / "source-registry-active.json",
        "pending": tmp_path / "source-registry-pending.json",
        "rejected": tmp_path / "source-registry-rejected.json",
    }


def _seed_placed_rows(tmp_path: Path) -> None:
    SourceRegistryRuntimeStore(get_storage_store(tmp_path)).replace_state(
        state={
            "active": [dict(_ACTIVE)],
            "pending": [],
            "rejected": [dict(_REJECTED)],
        },
        tombstones={},
        reason="test_seed",
    )


def _authority_report() -> dict[str, Any]:
    return {
        "runId": "discovery_placed_conflicts",
        "summary": {"queuedCandidateCount": 2},
        "runtime": {},
        "candidates": [],
    }


def _failed_publish_report() -> dict[str, Any]:
    return {
        "runId": "discovery_placed_conflicts",
        "startedAt": "2026-07-01T09:00:00+00:00",
        "finishedAt": _NOW,
        "summary": {"queuedCandidateCount": 2},
        "runtime": {
            "autoApproval": {"enabled": False, "approvedCount": 0, "status": "completed"},
            "registryFinalization": {
                "status": "failed",
                "error": "authority write failed",
                "pendingCount": 2,
                "activeCount": 1,
                "rejectedCount": 1,
                "pendingRows": [dict(row) for row in _RUN_ROWS],
            },
        },
        "candidates": [],
    }


def test_authority_apply_audits_placed_conflicts_without_mutating_state(
    tmp_path: Path,
) -> None:
    try:
        _seed_placed_rows(tmp_path)
        paths = _paths(tmp_path)
        audit: dict[str, int] = {}
        active_conflicts: list[dict[str, Any]] = []

        state, approved, authority = apply_discovery_approval_to_authority(
            active_path=paths["active"],
            pending_path=paths["pending"],
            rejected_path=paths["rejected"],
            pending_rows=[dict(row) for row in _RUN_ROWS],
            report=_authority_report(),
            auto_approve_enabled=False,
            tombstones={},
            approval_state_path=tmp_path / "source-approval-state.json",
            now_iso_fn=lambda: _NOW,
            merge_audit=audit,
            active_merge_conflicts=active_conflicts,
        )

        assert authority == "sqlite"
        assert approved == 0
        assert state["pending"] == []
        [active] = state["active"]
        [rejected] = state["rejected"]
        assert active["name"] == "Active Authority"
        assert active["careersUrl"] == "https://active.example/careers"
        assert rejected["name"] == "Rejected Authority"
        assert rejected["careersUrl"] == "https://rejected.example/careers"
        assert audit == {"rowsDroppedPlaced": 2, "activeFieldConflicts": 4}
        assert [(row["identity"], row["field"]) for row in active_conflicts] == [
            ("active-row", "name"),
            ("active-row", "careersUrl"),
            ("rejected-row", "name"),
            ("rejected-row", "careersUrl"),
        ]
    finally:
        close_storage_stores()


def test_bridge_restage_saves_placed_conflict_audit_without_state_change(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    report_path.write_text(json.dumps(_failed_publish_report()), encoding="utf-8")
    persisted: list[dict[str, list[dict[str, Any]]]] = []
    current_state = {
        "active": [dict(_ACTIVE)],
        "pending": [],
        "rejected": [dict(_REJECTED)],
    }
    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso=_NOW,
        bridge_log=lambda *_args, **_kwargs: None,
        load_state=lambda: json.loads(json.dumps(current_state)),
        persist_state_and_auto_sync=lambda next_state, **_kwargs: append_and_return(
            persisted, json.loads(json.dumps(next_state)), next_state
        ),
    )

    service.reconcile_terminal_discovery_report_from_state()

    assert persisted == []
    assert [source_identity(row) for row in current_state["active"]] == ["active-row"]
    assert [source_identity(row) for row in current_state["rejected"]] == ["rejected-row"]
    finalization = json.loads(report_path.read_text(encoding="utf-8"))["runtime"][
        "registryFinalization"
    ]
    assert finalization["pendingMergeAudit"] == {
        "rowsDroppedPlaced": 2,
        "activeFieldConflicts": 4,
    }
    assert [(row["identity"], row["field"]) for row in finalization["activeMergeConflicts"]] == [
        ("active-row", "name"),
        ("active-row", "careersUrl"),
        ("rejected-row", "name"),
        ("rejected-row", "careersUrl"),
    ]
    assert "pendingRestagedCount" not in finalization
    assert "pendingUpdatedCount" not in finalization
