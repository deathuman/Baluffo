from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from src.bridge.discovery_service import DiscoveryDeps, DiscoveryPaths, DiscoveryService
from src.bridge.registry_service import RegistryPaths, RegistryService
from tests.helpers.mutation import append_and_return
from tests.helpers.report_state import parse_iso_utc

_parse_iso_utc = parse_iso_utc


def _load_json_object(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if Path(path).exists():
        loaded = json.loads(Path(path).read_text(encoding="utf-8"))
        return loaded if isinstance(loaded, dict) else dict(default)
    return dict(default)


def _make_service(
    tmp_path: Path,
    *,
    report_path: Path,
    now_iso: str = "2026-03-20T12:04:00Z",
    tombstones_path: Path | None = None,
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
            tombstones=tombstones_path or tmp_path / "source-registry-tombstones.json",
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
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            failed_runs, {"runId": run_id, "taskType": task_type, **kwargs}, {}
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
    state: dict[str, list[dict[str, Any]]] = {
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
        persisted = {bucket: [dict(row) for row in rows] for bucket, rows in next_state.items()}
        persisted_states.append(persisted)
        state["active"] = persisted["active"]
        state["pending"] = persisted["pending"]
        state["rejected"] = persisted["rejected"]
        return persisted

    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso="2026-03-20T12:06:00Z",
        bridge_log=lambda level, message, **fields: events.append(str(message)),
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
    state: dict[str, list[dict[str, Any]]] = {
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
        persisted = {bucket: [dict(row) for row in rows] for bucket, rows in next_state.items()}
        persisted_states.append(persisted)
        state["active"] = persisted["active"]
        state["pending"] = persisted["pending"]
        state["rejected"] = persisted["rejected"]
        return persisted

    service = _make_service(
        tmp_path,
        report_path=report_path,
        bridge_log=lambda level, message, **fields: events.append(str(message)),
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


def test_reconciled_discovery_auto_approval_survives_registry_load_safe_demotions(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    winner_id = "static:listing_url:https://studio.example/careers"
    promoted_id = "static:listing_url:https://www.studio.example/careers"
    active_path.write_text(
        json.dumps(
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
                }
            ]
        ),
        encoding="utf-8",
    )
    pending_path.write_text(
        json.dumps(
            [
                {
                    "id": promoted_id,
                    "adapter": "static",
                    "name": "Static Studio",
                    "studio": "Static Studio",
                    "jobsFound": 3,
                    "rankScore": 20,
                    "score": 10,
                    "candidateState": "validated",
                }
            ]
        ),
        encoding="utf-8",
    )
    rejected_path.write_text("[]", encoding="utf-8")
    report_path.write_text(
        json.dumps(
            {
                "runId": "discovery_done_report_promoted_survives_load",
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
                        "id": promoted_id,
                        "sourceIdentity": promoted_id,
                        "adapter": "static",
                        "name": "Static Studio",
                        "studio": "Static Studio",
                        "jobsFound": 3,
                        "candidateState": "live",
                        "registryState": "active",
                        "stateChangedAt": "2026-03-20T12:05:30Z",
                        "stateChangedBy": "discovery_auto_approve",
                        "approvedBy": "registry_migration_v2",
                        "promotionReason": "manual_review_only",
                        "promotionRecommendation": "duplicate_candidate",
                        "duplicateOfActiveSource": True,
                        "staticSourceState": "active_duplicate",
                        "rankScore": 20,
                        "score": 10,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    registry_service = RegistryService(
        paths=RegistryPaths(
            active=active_path,
            pending=pending_path,
            rejected=rejected_path,
        ),
        default_active=[],
        normalize_manual_static=lambda row: row,
    )
    events: list[str] = []
    service = _make_service(
        tmp_path,
        report_path=report_path,
        bridge_log=lambda level, message, **fields: events.append(str(message)),
        load_state=registry_service.load_state,
        persist_state_and_auto_sync=lambda state, **_kwargs: registry_service.persist_state(state),
    )

    service.reconcile_terminal_discovery_report_from_state()
    loaded = registry_service.load_state()
    auto_heal = registry_service.get_auto_heal_report()

    assert [row["id"] for row in loaded["active"]] == [winner_id, promoted_id]
    assert loaded["pending"] == []
    assert auto_heal["safeAutomation"]["demoted"] == 0
    assert auto_heal["safeAutomation"]["skippedRows"] == [
        {
            "id": promoted_id,
            "reason": "protected_from_load_time_safe_auto_demote",
        }
    ]
    assert "discovery_registry_reconciled_from_terminal_report" in events


def test_discovery_watch_retries_transient_heartbeat_and_terminal_write_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    _write_active_report(report_path, run_id="discovery_1", queued=2)
    heartbeats: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    logs: list[tuple[str, str, dict[str, Any]]] = []
    sleeps: list[float] = []

    def bridge_log(level: str, event: str, **fields: Any) -> None:
        logs.append((level, event, fields))

    heartbeat_calls = 0

    def heartbeat(run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        nonlocal heartbeat_calls
        heartbeat_calls += 1
        if heartbeat_calls == 1:
            raise sqlite3.OperationalError("discovery heartbeat failed")
        heartbeats.append({"runId": run_id, "taskType": task_type, **kwargs})
        return {}

    pid_checks = 0

    def pid_is_running(_pid: int) -> bool:
        nonlocal pid_checks
        pid_checks += 1
        return pid_checks <= 2

    failure_calls = 0

    def fail(run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        nonlocal failure_calls
        failure_calls += 1
        if failure_calls == 1:
            raise sqlite3.OperationalError("discovery closeout failed")
        failures.append({"runId": run_id, "taskType": task_type, **kwargs})
        return {}

    def no_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) > 10:
            raise AssertionError("discovery watcher did not converge")

    service = _make_service(
        tmp_path,
        report_path=report_path,
        bridge_log=bridge_log,
        pid_is_running=pid_is_running,
        heartbeat_lifecycle_run=heartbeat,
        fail_lifecycle_run=fail,
    )
    monkeypatch.setattr("src.bridge.discovery_service_watch.time.sleep", no_sleep)

    service.watch_discovery_run_for_auto_sync("discovery_1", 123, "2026-03-20T12:00:00Z")

    assert heartbeat_calls == 2
    assert len(heartbeats) == 1
    assert failure_calls == 2
    assert len(failures) == 1
    assert failures[0]["terminal_reason"] == "owner_inactive_without_terminal_report"
    retries = [entry for entry in logs if entry[1] == "task_lifecycle_watch_retry"]
    assert [(entry[0], entry[2]["stage"], entry[2]["taskType"]) for entry in retries] == [
        ("warn", "heartbeat", "discovery"),
        ("warn", "terminal_closeout", "discovery"),
    ]
    assert "discovery heartbeat failed" in retries[0][2]["error"]
    assert "discovery closeout failed" in retries[1][2]["error"]
    saved_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert saved_report["finishedAt"] == "2026-03-20T12:04:00Z"
    assert saved_report["taskProgress"]["active"] is False


def test_discovery_watch_retries_transient_terminal_finalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    report_path.write_text(
        json.dumps(
            {
                "runId": "discovery_1",
                "startedAt": "2026-03-20T12:00:00Z",
                "finishedAt": "2026-03-20T12:05:00Z",
                "summary": {"queuedCandidateCount": 0},
                "runtime": {},
                "candidates": [],
                "failures": [],
            }
        ),
        encoding="utf-8",
    )
    finalized: list[dict[str, Any]] = []
    logs: list[tuple[str, str, dict[str, Any]]] = []
    sleeps: list[float] = []
    finalize_calls = 0

    def finish(run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        nonlocal finalize_calls
        finalize_calls += 1
        if finalize_calls == 1:
            raise sqlite3.OperationalError("discovery finalize failed")
        finalized.append({"runId": run_id, "taskType": task_type, **kwargs})
        return {}

    def no_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) > 10:
            raise AssertionError("discovery watcher did not converge")

    service = _make_service(
        tmp_path,
        report_path=report_path,
        bridge_log=lambda level, event, **fields: logs.append((level, event, fields)),
        finish_lifecycle_run=finish,
    )
    monkeypatch.setattr("src.bridge.discovery_service_watch.time.sleep", no_sleep)

    service.watch_discovery_run_for_auto_sync("discovery_1", 123, "2026-03-20T12:00:00Z")

    assert finalize_calls == 2
    assert len(finalized) == 1
    assert finalized[0]["terminal_reason"] == "completed"
    retries = [entry for entry in logs if entry[1] == "task_lifecycle_watch_retry"]
    assert [(entry[0], entry[2]["stage"], entry[2]["taskType"]) for entry in retries] == [
        ("warn", "finalize", "discovery")
    ]
    assert "discovery finalize failed" in retries[0][2]["error"]


def test_reconcile_terminal_discovery_registry_persists_on_finalization_count_mismatch(
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
    state: dict[str, list[dict[str, Any]]] = {
        "active": [{"id": "active-1", "adapter": "static"}],
        "pending": [{"id": "pending-ok", "adapter": "static", "jobsFound": 3}],
        "rejected": [],
    }
    persisted_states: list[dict[str, list[dict[str, Any]]]] = []
    events: list[str] = []
    service = _make_service(
        tmp_path,
        report_path=report_path,
        bridge_log=lambda level, message, **fields: events.append(str(message)),
        load_state=lambda: json.loads(json.dumps(state)),
        persist_state_and_auto_sync=lambda next_state, **_kwargs: append_and_return(
            persisted_states, json.loads(json.dumps(next_state)), next_state
        ),
    )

    service.reconcile_terminal_discovery_report_from_state()

    # The report's counts are stale (it claims 99 active); the merge is still the
    # authority-aware result and must be persisted, not dropped.
    assert len(persisted_states) == 1
    assert [row["id"] for row in persisted_states[0]["active"]] == ["active-1", "pending-ok"]
    assert persisted_states[0]["pending"] == []
    assert "discovery_registry_reconciliation_count_mismatch" in events


def test_reconcile_terminal_discovery_registry_restages_pending_after_failed_publish(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    report_path.write_text(
        json.dumps(
            {
                "runId": "discovery_publish_failed",
                "startedAt": "2026-03-20T12:00:00Z",
                "finishedAt": "2026-03-20T12:05:00Z",
                "summary": {"queuedCandidateCount": 2},
                "runtime": {
                    "autoApproval": {
                        "enabled": False,
                        "approvedCount": 0,
                        "status": "completed",
                    },
                    "registryFinalization": {
                        "status": "failed",
                        "error": "authority write failed",
                        "pendingCount": 2,
                        "pendingRows": [
                            {
                                "id": "pending-new",
                                "adapter": "static",
                                "name": "New Pending",
                                "jobsFound": 2,
                            },
                            {
                                "id": "already-active",
                                "adapter": "static",
                                "name": "Active Duplicate",
                            },
                        ],
                    },
                },
                "candidates": [],
            }
        ),
        encoding="utf-8",
    )
    state: dict[str, list[dict[str, Any]]] = {
        "active": [{"id": "already-active", "adapter": "static", "name": "Active Duplicate"}],
        "pending": [],
        "rejected": [],
    }
    persisted_states: list[dict[str, list[dict[str, Any]]]] = []
    persist_reasons: list[str] = []
    events: list[tuple[str, str]] = []

    def persist_state_and_auto_sync(
        next_state: dict[str, list[dict[str, Any]]], **kwargs: Any
    ) -> dict[str, list[dict[str, Any]]]:
        persist_reasons.append(str(kwargs.get("reason") or ""))
        persisted = {bucket: [dict(row) for row in rows] for bucket, rows in next_state.items()}
        persisted_states.append(persisted)
        return persisted

    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso="2026-03-20T12:06:00Z",
        bridge_log=lambda level, message, **fields: events.append((level, str(message))),
        load_state=lambda: json.loads(json.dumps(state)),
        persist_state_and_auto_sync=persist_state_and_auto_sync,
    )

    service.reconcile_terminal_discovery_report_from_state()

    assert len(persisted_states) == 1
    # A row that was not pending before really was restaged, so this branch keeps
    # the restage reason; only field-only carries get the update reason.
    assert persist_reasons == ["discovery_pending_restage"]
    assert [row["id"] for row in persisted_states[0]["active"]] == ["already-active"]
    assert [row["id"] for row in persisted_states[0]["pending"]] == ["pending-new"]
    saved = json.loads(report_path.read_text(encoding="utf-8"))
    finalization = saved["runtime"]["registryFinalization"]
    assert finalization["status"] == "failed"
    assert finalization["pendingRestagedCount"] == 1
    assert finalization["pendingRestagedAt"] == "2026-03-20T12:06:00Z"
    assert ("info", "discovery_registry_pending_restaged") in events
