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
