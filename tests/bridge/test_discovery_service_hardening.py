from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.bridge.discovery_service import DiscoveryDeps, DiscoveryPaths, DiscoveryService


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _load_json_object(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if Path(path).exists():
        return json.loads(Path(path).read_text(encoding="utf-8"))
    return dict(default)


def _make_service(
    tmp_path: Path,
    *,
    report_path: Path,
    now_iso: str = "2026-03-20T12:04:00Z",
    **dep_overrides: Any,
) -> DiscoveryService:
    deps: dict[str, Any] = {
        "schema_version": 1,
        "now_iso": lambda: now_iso,
        "now_utc": lambda: None,
        "parse_iso": _parse_iso_utc,
        "pid_is_running": lambda pid: False,
        "bridge_log": lambda *args, **kwargs: None,
        "load_json_object": _load_json_object,
        "save_json_atomic": lambda path, payload: Path(path).write_text(
            json.dumps(payload), encoding="utf-8"
        ),
        "run_background_script": lambda script_name, args=None, **kwargs: 1,
        "append_run_history": lambda payload: payload,
        "upsert_run_history": lambda payload, **_kwargs: payload,
        "prune_started_rows_for_type": lambda *_args, **_kwargs: None,
        "clear_task_state": lambda _task_type: None,
        "normalize_discovery_report_contract": lambda payload: payload,
        "load_state": lambda: {"active": [], "pending": [], "rejected": []},
        "persist_state_and_auto_sync": lambda state, **_kwargs: state,
        "load_sync_runtime_state": lambda: {},
        "maybe_trigger_auto_sync_push": lambda reason: False,
        "mark_discovery_sync_finished": lambda finished_at: None,
    }
    deps.update(dep_overrides)
    return DiscoveryService(
        paths=DiscoveryPaths(
            report=report_path,
            candidates=tmp_path / "source-registry-pending.json",
            pending=tmp_path / "source-registry-pending.json",
            log=tmp_path / "source-discovery.log",
            settings=tmp_path / "source-discovery-config.json",
            approval_state=tmp_path / "source-approval-state.json",
        ),
        deps=DiscoveryDeps(**deps),
    )


def _write_active_report(report_path: Path, *, run_id: str, queued: int) -> None:
    report_path.write_text(
        json.dumps(
            {
                "runId": run_id,
                "startedAt": "2026-03-20T12:00:00Z",
                "finishedAt": "",
                "summary": {"queuedCandidateCount": queued},
                "taskProgress": {
                    "active": True,
                    "phaseKey": "sheet_directory",
                    "counts": {"queuedCandidates": queued},
                },
                "runtime": {"lifecycle": {"owner": "discovery_report"}},
                "candidates": [],
                "failures": [],
            }
        ),
        encoding="utf-8",
    )


def test_reconcile_terminal_discovery_report_from_lifecycle_repairs_active_report(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    _write_active_report(report_path, run_id="discovery_dead_1", queued=2)
    terminal_row = {
        "runId": "discovery_dead_1",
        "taskType": "discovery",
        "lifecycleStatus": "failed",
        "finishedAt": "2026-03-20T12:03:00Z",
        "terminalReason": "owner_inactive_without_terminal_report",
        "summary": {"error": "owner_inactive_without_terminal_report"},
        "taskProgress": {"counts": {"failedProbes": 1}},
    }
    service = _make_service(
        tmp_path,
        report_path=report_path,
        get_lifecycle_row=lambda run_id, task_type: (
            terminal_row if run_id == "discovery_dead_1" and task_type == "discovery" else None
        ),
    )

    repaired = service.reconcile_terminal_discovery_report_from_state()
    saved_report = json.loads(report_path.read_text(encoding="utf-8"))

    assert repaired is not None
    assert saved_report["finishedAt"] == "2026-03-20T12:03:00Z"
    assert saved_report["status"] == "error"
    assert saved_report["summary"]["error"] == "owner_inactive_without_terminal_report"
    assert saved_report["taskProgress"]["active"] is False
    assert saved_report["taskProgress"]["phaseKey"] == "failed"
    assert saved_report["taskProgress"]["counts"]["queuedCandidates"] == 2
    assert saved_report["runtime"]["lifecycle"]["heartbeatAt"] == "2026-03-20T12:03:00Z"


def test_watch_discovery_run_repairs_report_when_child_pid_dead(tmp_path: Path) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    _write_active_report(report_path, run_id="discovery_dead_2", queued=3)
    failed_runs: list[dict[str, object]] = []
    service = _make_service(
        tmp_path,
        report_path=report_path,
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: (
            failed_runs.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
    )

    service.watch_discovery_run_for_auto_sync("discovery_dead_2", 4242, "2026-03-20T12:00:00Z")

    saved_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert failed_runs[-1]["terminal_reason"] == "owner_inactive_without_terminal_report"
    assert saved_report["finishedAt"] == "2026-03-20T12:04:00Z"
    assert saved_report["status"] == "error"
    assert saved_report["taskProgress"]["active"] is False
    assert saved_report["taskProgress"]["counts"]["queuedCandidates"] == 3


def test_watch_discovery_run_waits_for_finalization_before_auto_sync(tmp_path: Path) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    settings_path = tmp_path / "source-discovery-config.json"
    finished_at = "2026-03-20T12:05:00Z"
    settings_path.write_text(
        json.dumps({"autoApproveHealthyPendingOnComplete": True}),
        encoding="utf-8",
    )
    report_path.write_text(
        json.dumps(
            {
                "runId": "discovery_1",
                "startedAt": "2026-03-20T12:00:00Z",
                "finishedAt": finished_at,
                "summary": {
                    "queuedCandidateCount": 0,
                    "failedProbeCount": 0,
                    "probeMissCount": 0,
                },
                "runtime": {
                    "autoApproval": {
                        "enabled": True,
                        "approvedCount": 2,
                        "status": "running",
                    },
                    "registryFinalization": {"status": "running"},
                },
            }
        ),
        encoding="utf-8",
    )

    pid_checks = 0
    marked: list[str] = []

    def pid_is_running(_pid: int) -> bool:
        nonlocal pid_checks
        pid_checks += 1
        if pid_checks == 1:
            payload = json.loads(report_path.read_text(encoding="utf-8"))
            payload["runtime"]["autoApproval"]["status"] = "completed"
            payload["runtime"]["registryFinalization"]["status"] = "completed"
            report_path.write_text(json.dumps(payload), encoding="utf-8")
        return True

    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso="2026-03-20T12:06:00Z",
        pid_is_running=pid_is_running,
        mark_discovery_sync_finished=lambda value: marked.append(value),
    )

    service.watch_discovery_run_for_auto_sync("discovery_1", 123, "2026-03-20T12:00:00Z")

    saved_report = json.loads(report_path.read_text(encoding="utf-8"))
    runtime = saved_report.get("runtime") or {}
    assert ((runtime.get("autoApproval") or {}).get("status")) == "completed"
    assert ((runtime.get("autoApproval") or {}).get("approvedCount")) == 2
    assert ((runtime.get("registryFinalization") or {}).get("status")) == "completed"
    assert pid_checks >= 1
    assert marked == [finished_at]


def test_reconcile_terminal_discovery_registry_uses_report_auto_approval_when_config_stale(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    approval_state_path = tmp_path / "source-approval-state.json"
    approval_state_path.write_text(json.dumps({"approvedSinceLastRun": 12}), encoding="utf-8")
    report_path.write_text(
        json.dumps(
            {
                "runId": "discovery_done_1",
                "startedAt": "2026-03-20T12:00:00Z",
                "finishedAt": "2026-03-20T12:05:00Z",
                "summary": {
                    "queuedCandidateCount": 1,
                    "approvedCandidateCount": 1,
                    "liveCandidateCount": 1,
                },
                "runtime": {
                    "autoApproval": {
                        "enabled": True,
                        "approvedCount": 1,
                        "status": "completed",
                    },
                    "registryFinalization": {
                        "status": "completed",
                        "activeCount": 2,
                        "pendingCount": 0,
                        "rejectedCount": 0,
                    },
                },
                "candidates": [
                    {
                        "id": "pending-ok",
                        "adapter": "static",
                        "name": "Healthy Pending",
                        "jobsFound": 3,
                        "candidateState": "live",
                        "registryState": "active",
                        "approvedBy": "discovery_auto_approve",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    state = {
        "active": [{"id": "active-1", "adapter": "static", "name": "Existing"}],
        "pending": [
            {
                "id": "pending-ok",
                "adapter": "static",
                "name": "Healthy Pending",
                "jobsFound": 3,
            }
        ],
        "rejected": [],
    }
    persisted_states: list[dict[str, list[dict[str, Any]]]] = []
    events: list[str] = []

    def persist_state_and_auto_sync(
        next_state: dict[str, list[dict[str, Any]]], **_kwargs: Any
    ) -> dict[str, list[dict[str, Any]]]:
        persisted = json.loads(json.dumps(next_state))
        persisted_states.append(persisted)
        state["active"] = persisted["active"]
        state["pending"] = persisted["pending"]
        state["rejected"] = persisted["rejected"]
        return persisted

    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso="2026-03-20T12:06:00Z",
        bridge_log=lambda _level, message, **_fields: events.append(str(message)),
        load_state=lambda: json.loads(json.dumps(state)),
        persist_state_and_auto_sync=persist_state_and_auto_sync,
    )

    service.reconcile_terminal_discovery_report_from_state()

    assert len(persisted_states) == 1
    assert [row["id"] for row in state["active"]] == ["active-1", "pending-ok"]
    assert state["pending"] == []
    assert "discovery_registry_reconciled_from_terminal_report" in events
    approval_state = json.loads(approval_state_path.read_text(encoding="utf-8"))
    assert approval_state["approvedSinceLastRun"] == 12


def test_reconcile_terminal_discovery_registry_replays_report_declared_promotions(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    report_path.write_text(
        json.dumps(
            {
                "runId": "discovery_done_report_promoted",
                "startedAt": "2026-03-20T12:00:00Z",
                "finishedAt": "2026-03-20T12:05:00Z",
                "summary": {
                    "queuedCandidateCount": 1,
                    "approvedCandidateCount": 1,
                    "liveCandidateCount": 1,
                },
                "runtime": {
                    "autoApproval": {
                        "enabled": True,
                        "approvedCount": 1,
                        "status": "completed",
                    },
                    "registryFinalization": {
                        "status": "completed",
                        "activeCount": 2,
                        "pendingCount": 0,
                        "rejectedCount": 0,
                    },
                },
                "candidates": [
                    {
                        "id": "static:listing_url:https://example.com/careers",
                        "sourceIdentity": "static:listing_url:https://example.com/careers",
                        "adapter": "static",
                        "name": "Report Promoted",
                        "jobsFound": 0,
                        "candidateState": "validated",
                        "registryState": "active",
                        "stateChangedAt": "2026-03-20T12:05:30Z",
                        "stateChangedBy": "discovery_auto_approve",
                        "approvedBy": "registry_migration_v2",
                        "promotionReason": "manual_review_only",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    state = {
        "active": [{"id": "active-1", "adapter": "static", "name": "Existing"}],
        "pending": [
            {
                "id": "static:listing_url:https://example.com/careers",
                "adapter": "static",
                "name": "Report Promoted",
                "jobsFound": 0,
                "candidateState": "validated",
            }
        ],
        "rejected": [],
    }
    persisted_states: list[dict[str, list[dict[str, Any]]]] = []
    events: list[str] = []

    def persist_state_and_auto_sync(
        next_state: dict[str, list[dict[str, Any]]], **_kwargs: Any
    ) -> dict[str, list[dict[str, Any]]]:
        persisted = json.loads(json.dumps(next_state))
        persisted_states.append(persisted)
        state["active"] = persisted["active"]
        state["pending"] = persisted["pending"]
        state["rejected"] = persisted["rejected"]
        return persisted

    service = _make_service(
        tmp_path,
        report_path=report_path,
        bridge_log=lambda _level, message, **_fields: events.append(str(message)),
        load_state=lambda: json.loads(json.dumps(state)),
        persist_state_and_auto_sync=persist_state_and_auto_sync,
    )

    service.reconcile_terminal_discovery_report_from_state()

    assert len(persisted_states) == 1
    assert [row["id"] for row in state["active"]] == [
        "active-1",
        "static:listing_url:https://example.com/careers",
    ]
    assert state["pending"] == []
    promoted = state["active"][-1]
    assert promoted["registryState"] == "active"
    assert promoted["stateChangedBy"] == "discovery_auto_approve"
    assert promoted["stateChangedAt"] == "2026-03-20T12:05:30Z"
    assert "discovery_registry_reconciled_from_terminal_report" in events


def test_reconcile_terminal_discovery_registry_skips_unsafe_finalization_mismatch(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    report_path.write_text(
        json.dumps(
            {
                "runId": "discovery_done_2",
                "startedAt": "2026-03-20T12:00:00Z",
                "finishedAt": "2026-03-20T12:05:00Z",
                "summary": {"queuedCandidateCount": 1, "approvedCandidateCount": 1},
                "runtime": {
                    "autoApproval": {
                        "enabled": True,
                        "approvedCount": 1,
                        "status": "completed",
                    },
                    "registryFinalization": {
                        "status": "completed",
                        "activeCount": 99,
                        "pendingCount": 0,
                        "rejectedCount": 0,
                    },
                },
                "candidates": [{"id": "pending-ok", "adapter": "static", "jobsFound": 3}],
            }
        ),
        encoding="utf-8",
    )
    state = {
        "active": [{"id": "active-1", "adapter": "static"}],
        "pending": [{"id": "pending-ok", "adapter": "static", "jobsFound": 3}],
        "rejected": [],
    }
    persisted_states: list[dict[str, list[dict[str, Any]]]] = []
    events: list[str] = []
    service = _make_service(
        tmp_path,
        report_path=report_path,
        bridge_log=lambda _level, message, **_fields: events.append(str(message)),
        load_state=lambda: json.loads(json.dumps(state)),
        persist_state_and_auto_sync=lambda next_state, **_kwargs: (
            persisted_states.append(json.loads(json.dumps(next_state))) or next_state
        ),
    )

    service.reconcile_terminal_discovery_report_from_state()

    assert persisted_states == []
    assert [row["id"] for row in state["active"]] == ["active-1"]
    assert [row["id"] for row in state["pending"]] == ["pending-ok"]
    assert "discovery_registry_reconciliation_skipped" in events
