"""Routing conflicts hold auto-approval; observation-only updates still pass."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.bridge.storage_health import close_storage_stores
from src.source_registry_identity import source_identity
from tests.bridge.test_discovery_service_hardening import _make_service
from tests.bridge.test_source_registry_concurrent_mutations import (
    _B_PENDING,
    _NOW,
    _apply_approval,
    _seed_sqlite_authority,
)
from tests.helpers.mutation import append_and_return

_CURRENT = {
    **_B_PENDING,
    "lastProbedAt": "2026-07-01T09:00:00+00:00",
}


def _bridge_report(*, run_id: str, pending_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "runId": run_id,
        "startedAt": "2026-07-01T09:00:00+00:00",
        "finishedAt": _NOW,
        "summary": {"queuedCandidateCount": len(pending_rows)},
        "runtime": {
            "autoApproval": {"enabled": True, "approvedCount": 0, "status": "completed"},
            "registryFinalization": {
                "status": "failed",
                "error": "authority write failed",
                "pendingCount": len(pending_rows),
                "pendingRows": pending_rows,
            },
        },
        "candidates": [],
    }


def test_authority_apply_holds_a_routing_conflict_for_review(tmp_path: Path) -> None:
    try:
        _seed_sqlite_authority(tmp_path)
        conflicts: list[dict[str, Any]] = []
        run = {
            **_B_PENDING,
            "careersUrl": "https://b.example/migrated",
            "jobsFound": 9,
            "lastProbedAt": _NOW,
        }

        state, approved, authority = _apply_approval(
            tmp_path,
            pending_rows=[run],
            auto_approve_enabled=True,
            merge_conflicts=conflicts,
        )

        assert authority == "sqlite"
        assert approved == 0
        assert [source_identity(row) for row in state["active"]] == ["a"]
        [pending] = state["pending"]
        assert source_identity(pending) == "b"
        assert pending["careersUrl"] == _B_PENDING["careersUrl"]
        assert pending["jobsFound"] == 9
        assert conflicts == [
            {
                "identity": "b",
                "field": "careersUrl",
                "authorityValue": _B_PENDING["careersUrl"],
                "runValue": "https://b.example/migrated",
            }
        ]
    finally:
        close_storage_stores()


def test_authority_apply_allows_an_observation_only_update(tmp_path: Path) -> None:
    try:
        _seed_sqlite_authority(tmp_path)
        conflicts: list[dict[str, Any]] = []
        run = {
            **_B_PENDING,
            "jobsFound": 9,
            "lastProbedAt": _NOW,
        }

        state, approved, authority = _apply_approval(
            tmp_path,
            pending_rows=[run],
            auto_approve_enabled=True,
            merge_conflicts=conflicts,
        )

        assert authority == "sqlite"
        assert approved == 1
        assert sorted(source_identity(row) for row in state["active"]) == ["a", "b"]
        assert state["pending"] == []
        [active] = [row for row in state["active"] if source_identity(row) == "b"]
        assert active["jobsFound"] == 9
        assert conflicts == []
    finally:
        close_storage_stores()


def test_authority_apply_allows_a_display_only_conflict(tmp_path: Path) -> None:
    try:
        _seed_sqlite_authority(tmp_path)
        conflicts: list[dict[str, Any]] = []
        run = {
            **_B_PENDING,
            "name": "Migrated Studio B",
            "jobsFound": 9,
            "lastProbedAt": _NOW,
        }

        state, approved, authority = _apply_approval(
            tmp_path,
            pending_rows=[run],
            auto_approve_enabled=True,
            merge_conflicts=conflicts,
        )

        assert authority == "sqlite"
        assert approved == 1
        [active] = [row for row in state["active"] if source_identity(row) == "b"]
        assert active["name"] == "Studio B"
        assert conflicts == [
            {
                "identity": "b",
                "field": "name",
                "authorityValue": "Studio B",
                "runValue": "Migrated Studio B",
            }
        ]
    finally:
        close_storage_stores()


def test_bridge_restage_holds_a_routing_conflict_for_review(tmp_path: Path) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    run = {
        **_B_PENDING,
        "careersUrl": "https://b.example/migrated",
        "jobsFound": 9,
        "lastProbedAt": _NOW,
    }
    report_path.write_text(
        json.dumps(_bridge_report(run_id="discovery_routing_conflict", pending_rows=[run])),
        encoding="utf-8",
    )
    persisted: list[dict[str, list[dict[str, Any]]]] = []
    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso=_NOW,
        bridge_log=lambda *_args, **_kwargs: None,
        load_state=lambda: {"active": [], "pending": [dict(_CURRENT)], "rejected": []},
        persist_state_and_auto_sync=lambda next_state, **_kwargs: append_and_return(
            persisted, json.loads(json.dumps(next_state)), next_state
        ),
    )

    service.reconcile_terminal_discovery_report_from_state()

    assert len(persisted) == 1
    state = persisted[0]
    assert state["active"] == []
    [pending] = state["pending"]
    assert source_identity(pending) == "b"
    assert pending["careersUrl"] == _B_PENDING["careersUrl"]
    assert pending["jobsFound"] == 9
    saved = json.loads(report_path.read_text(encoding="utf-8"))["runtime"]
    assert saved["autoApproval"]["approvedCount"] == 0
    assert saved["registryFinalization"]["pendingMergeConflicts"] == [
        {
            "identity": "b",
            "field": "careersUrl",
            "authorityValue": _B_PENDING["careersUrl"],
            "runValue": "https://b.example/migrated",
        }
    ]


def test_bridge_restage_allows_an_observation_only_update(tmp_path: Path) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    run = {
        **_B_PENDING,
        "jobsFound": 9,
        "lastProbedAt": _NOW,
    }
    report_path.write_text(
        json.dumps(_bridge_report(run_id="discovery_observation_only", pending_rows=[run])),
        encoding="utf-8",
    )
    persisted: list[dict[str, list[dict[str, Any]]]] = []
    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso=_NOW,
        bridge_log=lambda *_args, **_kwargs: None,
        load_state=lambda: {"active": [], "pending": [dict(_CURRENT)], "rejected": []},
        persist_state_and_auto_sync=lambda next_state, **_kwargs: append_and_return(
            persisted, json.loads(json.dumps(next_state)), next_state
        ),
    )

    service.reconcile_terminal_discovery_report_from_state()

    assert len(persisted) == 1
    state = persisted[0]
    assert [source_identity(row) for row in state["active"]] == ["b"]
    assert state["pending"] == []
    assert state["active"][0]["jobsFound"] == 9
    saved = json.loads(report_path.read_text(encoding="utf-8"))["runtime"]
    assert saved["autoApproval"]["approvedCount"] == 1
