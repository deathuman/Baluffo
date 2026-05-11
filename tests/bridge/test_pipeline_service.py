from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.bridge.pipeline_service import PipelineRuntime, PipelineService
from src.bridge.run_history_api import (
    ChildTaskSnapshot,
    LifecycleProjection,
    SyncHistoryDeps,
    project_run_history,
)


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _make_pipeline_service(**overrides: Any) -> PipelineService:
    kwargs: dict[str, Any] = {
        "pipeline_state_lock": __import__("threading").RLock(),
        "pipeline_status": {},
        "runtime": PipelineRuntime(),
        "bridge_log": lambda *args, **kwargs: None,
        "now_iso": lambda: "2026-05-06T19:00:00Z",
        "parse_iso": _parse_iso,
        "append_run_history": lambda row: row,
        "upsert_run_history": lambda entry, **_kwargs: entry,
        "task_running_from_state": lambda _task_type: False,
        "sync_task_running": lambda: False,
        "current_fetch_output_count": lambda: 0,
        "load_json_object": lambda _path, default: default,
        "load_runtime_evidence": lambda path, default=None: default or {},
        "wait_for_sync_completion": lambda _run_id, _timeout_s: {},
        "discovery_report_path": Path("source-discovery-report.json"),
        "fetch_report_path": Path("jobs-fetch-report.json"),
        "trigger_discovery_task": lambda **_kwargs: (200, {}),
        "start_fetcher_task": lambda _payload: {},
        "start_sync_task": lambda _action, **_kwargs: {},
        "get_app_version": lambda: "0.0.0-test",
    }
    kwargs.update(overrides)
    return PipelineService(**kwargs)


def test_wait_for_report_completion_refreshes_pipeline_child_heartbeat(tmp_path: Path) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    refreshed: list[tuple[str, str, str]] = []
    parent_heartbeats: list[tuple[str, str, str]] = []

    service = _make_pipeline_service(
        pipeline_status={
            "runId": "pipeline_1",
            "stage": "discovery",
            "progress": {"currentStep": 1, "totalSteps": 3, "label": "Running discovery..."},
        },
        refresh_child_task_heartbeat=lambda task_type, run_id, started_at: (
            refreshed.append((task_type, run_id, started_at)) or True
        ),
        heartbeat_lifecycle_run=lambda run_id, task_type, **kwargs: (
            parent_heartbeats.append((run_id, task_type, str(kwargs.get("stage") or ""))) or {}
        ),
    )

    report = service.wait_for_report_completion(
        report_path=report_path,
        started_at="2026-05-06T18:00:00Z",
        timeout_s=10.0,
        report_name="discovery report",
        load_json_object=lambda _path, _default: {
            "runId": "discovery_child_1",
            "startedAt": "2026-05-06T18:00:00Z",
            "finishedAt": "2026-05-06T18:10:00Z",
        },
        task_type="discovery",
        task_run_id="discovery_child_1",
    )

    assert report["finishedAt"] == "2026-05-06T18:10:00Z"
    assert refreshed == [
        ("discovery", "discovery_child_1", "2026-05-06T18:00:00Z"),
    ]
    assert parent_heartbeats == [("pipeline_1", "pipeline", "discovery")]


def test_pipeline_waits_for_discovery_auto_approval_after_child_terminal_report() -> None:
    parent_heartbeats: list[tuple[str, str, str]] = []
    service = _make_pipeline_service(
        pipeline_status={"runId": "pipeline_1"},
        load_json_object=lambda _path, _default: {
            "runtime": {"autoApproval": {"enabled": True, "status": "completed"}}
        },
        heartbeat_lifecycle_run=lambda run_id, task_type, **kwargs: (
            parent_heartbeats.append((run_id, task_type, str(kwargs.get("stage") or ""))) or {}
        ),
    )

    service._wait_for_discovery_auto_approval(
        {"runtime": {"autoApproval": {"enabled": True, "status": "running"}}}
    )

    assert parent_heartbeats
    assert all(
        row == ("pipeline_1", "pipeline", "discovery_auto_approval") for row in parent_heartbeats
    )


def test_pipeline_waits_for_discovery_registry_finalization_before_fetch() -> None:
    parent_heartbeats: list[tuple[str, str, str]] = []
    service = _make_pipeline_service(
        pipeline_status={"runId": "pipeline_1"},
        load_json_object=lambda _path, _default: {
            "runtime": {
                "autoApproval": {"enabled": True, "status": "completed"},
                "registryFinalization": {"status": "completed"},
            }
        },
        heartbeat_lifecycle_run=lambda run_id, task_type, **kwargs: (
            parent_heartbeats.append((run_id, task_type, str(kwargs.get("stage") or ""))) or {}
        ),
    )

    service._wait_for_discovery_auto_approval(
        {
            "runtime": {
                "autoApproval": {"enabled": True, "status": "completed"},
                "registryFinalization": {"status": "running"},
            }
        }
    )

    assert parent_heartbeats
    assert all(
        row == ("pipeline_1", "pipeline", "discovery_registry_finalization")
        for row in parent_heartbeats
    )


def test_status_payload_recovers_inactive_pipeline_worker_after_terminal_fetch_report() -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "fetch",
        "progress": {"currentStep": 2, "totalSteps": 3, "percent": 67, "label": "Running fetch..."},
        "startedAt": "2026-05-06T18:00:00Z",
        "finishedAt": "",
        "error": "",
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    finished_fetch_report = {
        "runId": "fetch_1",
        "startedAt": "2026-05-06T18:05:00Z",
        "finishedAt": "2026-05-06T18:35:00Z",
        "taskProgress": {
            "active": True,
            "phaseKey": "executing_sources",
            "phaseLabel": "Executing sources",
            "mode": "determinate",
            "ratio": 0.8,
        },
        "summary": {"outputCount": 42, "failedSources": 3},
    }
    finished_children: list[dict[str, Any]] = []
    failed_runs: list[dict[str, Any]] = []
    cleared: list[str] = []

    service = _make_pipeline_service(
        pipeline_status=status,
        current_fetch_output_count=lambda: 42,
        load_json_object=lambda _path, _default: dict(finished_fetch_report),
        load_runtime_evidence=lambda _path, _default: dict(finished_fetch_report),
        get_projected_run_history=lambda: LifecycleProjection(
            rows=[],
            child_tasks={
                "fetch": ChildTaskSnapshot(
                    task_type="fetch",
                    run_id="fetch_1",
                    started_at="2026-05-06T18:05:00Z",
                    finished_at="",
                    active=True,
                    terminal_status="",
                    summary={},
                    outputs={},
                    task_progress={},
                    explicit_dead=False,
                    diagnostics=(),
                )
            },
            diagnostics=[],
        ),
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: (
            finished_children.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: (
            failed_runs.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
        clear_task_state=lambda task_type: cleared.append(task_type),
    )

    payload = service.get_status_payload()

    assert payload["active"] is False
    assert payload["stage"] == "error"
    assert payload["error"] == "pipeline_worker_inactive_after_fetch_completed"
    assert payload["finalOutputCount"] == 42
    assert finished_children == [
        {
            "runId": "fetch_1",
            "taskType": "fetch",
            "finished_at": "2026-05-06T18:35:00Z",
            "terminal_reason": "completed",
            "summary": {"outputCount": 42, "failedSources": 3},
            "progress": {
                "active": False,
                "phaseKey": "executing_sources",
                "phaseLabel": "Executing sources",
                "mode": "determinate",
                "ratio": 0.8,
            },
        }
    ]
    assert failed_runs[-1]["runId"] == "pipeline_1"
    assert failed_runs[-1]["taskType"] == "pipeline"
    assert failed_runs[-1]["terminal_reason"] == "failed"
    assert failed_runs[-1]["summary"]["error"] == "pipeline_worker_inactive_after_fetch_completed"
    assert cleared == []


def test_status_payload_recovers_inactive_pipeline_worker_after_terminal_sync_failure() -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "sync_push",
        "progress": {
            "currentStep": 3,
            "totalSteps": 3,
            "percent": 100,
            "label": "Running sync push...",
        },
        "startedAt": "2026-05-06T18:00:00Z",
        "finishedAt": "",
        "error": "",
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    failed_runs: list[dict[str, Any]] = []

    service = _make_pipeline_service(
        pipeline_status=status,
        current_fetch_output_count=lambda: 42,
        get_projected_run_history=lambda: LifecycleProjection(
            rows=[],
            child_tasks={
                "sync": ChildTaskSnapshot(
                    task_type="sync",
                    run_id="sync_1",
                    started_at="2026-05-06T18:40:00Z",
                    finished_at="2026-05-06T18:40:08Z",
                    active=False,
                    terminal_status="error",
                    summary={"error": "Snapshot size exceeded"},
                    outputs={},
                    task_progress={},
                    explicit_dead=True,
                    diagnostics=(),
                )
            },
            diagnostics=[],
        ),
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: (
            failed_runs.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
    )

    payload = service.get_status_payload()

    assert payload["active"] is False
    assert payload["stage"] == "error"
    assert payload["error"] == "sync_push: Snapshot size exceeded"
    assert payload["finalOutputCount"] == 42
    assert failed_runs[-1]["runId"] == "pipeline_1"
    assert failed_runs[-1]["summary"]["error"] == "sync_push: Snapshot size exceeded"


def _project_discovery_history(
    *,
    task_state: dict[str, Any],
    now: datetime,
    pipeline_status: dict[str, Any] | None = None,
    discovery_report_overrides: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    started_at = "2026-05-06T18:00:00Z"
    discovery_report = {
        "runId": "discovery_child_1",
        "startedAt": started_at,
        "finishedAt": "",
        "summary": {},
        "runtime": {"lifecycle": {"heartbeatAt": "2026-05-06T18:00:00Z"}},
    }
    discovery_report.update(dict(discovery_report_overrides or {}))
    history = [
        {
            "id": "discovery_child_1",
            "runId": "discovery_child_1",
            "type": "discovery",
            "status": "started",
            "startedAt": started_at,
            "finishedAt": "",
            "durationMs": 0,
            "summary": {},
        }
    ]

    def load_json_sync(path: Any, default: dict[str, Any]) -> dict[str, Any]:
        token = str(path)
        if token == "discovery-report":
            return discovery_report
        if token == "task-state":
            return task_state
        return dict(default)

    projection = project_run_history(
        SyncHistoryDeps(
            ops_state_lock=__import__("threading").RLock(),
            load_run_history=lambda: list(history),
            save_run_history=lambda _rows: None,
            save_json_atomic=lambda _path, _payload: None,
            prune_started_rows_for_type=lambda *_args, **_kwargs: None,
            clear_task_state=lambda _task_type: None,
            clear_task_state_locked=lambda _task_type: None,
            upsert_run_history=lambda entry, **_kwargs: entry,
            task_running_from_state=lambda _task_type: False,
            report_is_stale_in_progress=lambda *_args, **_kwargs: False,
            load_json_object=load_json_sync,
            load_runtime_evidence=load_json_sync,
            normalize_fetch_report_contract=lambda payload: payload,
            normalize_discovery_report_contract=lambda payload: payload,
            summarize_fetch_report=lambda _report: {},
            summarize_discovery_report=lambda _report: ({}, "ok"),
            jobs_fetch_report_path=Path("fetch-report"),
            jobs_fetch_tasks_path=Path("fetch-tasks"),
            discovery_report_path=Path("discovery-report"),
            task_state_path=Path("task-state"),
            get_active_sync_runs=lambda: set(),
            parse_iso=_parse_iso,
            now_iso=lambda: "2026-05-06T19:00:00Z",
            now_utc=lambda: now,
            get_jobs_pipeline_status_payload=lambda: dict(pipeline_status or {}),
        )
    )
    return projection.rows


def test_recent_child_task_heartbeat_keeps_quiet_discovery_projected_running() -> None:
    rows = _project_discovery_history(
        task_state={
            "discovery": {
                "runId": "discovery_child_1",
                "taskType": "discovery",
                "pid": 123,
                "status": "running",
                "startedAt": "2026-05-06T18:00:00Z",
                "heartbeatAt": "2026-05-06T18:59:00Z",
            }
        },
        now=datetime(2026, 5, 6, 19, 0, 0, tzinfo=UTC),
    )

    row = next(row for row in rows if row["runId"] == "discovery_child_1")
    assert row["status"] == "started"
    assert row["finishedAt"] == ""
    assert "error" not in row["summary"]


def test_terminal_discovery_report_wins_over_stale_active_task_state() -> None:
    rows = _project_discovery_history(
        task_state={
            "discovery": {
                "runId": "discovery_child_1",
                "taskType": "discovery",
                "pid": 123,
                "status": "running",
                "startedAt": "2026-05-06T18:00:00Z",
                "heartbeatAt": "2026-05-06T18:59:00Z",
            }
        },
        now=datetime(2026, 5, 6, 19, 0, 0, tzinfo=UTC),
        discovery_report_overrides={
            "finishedAt": "2026-05-06T18:58:00Z",
            "taskProgress": {
                "active": True,
                "phaseKey": "finalizing",
                "updatedAt": "2026-05-06T18:59:00Z",
            },
        },
    )

    row = next(row for row in rows if row["runId"] == "discovery_child_1")
    assert row["status"] == "ok"
    assert row["finishedAt"] == "2026-05-06T18:58:00Z"
    assert "error" not in row["summary"]


def test_orphaned_quiet_discovery_still_projects_terminal_error() -> None:
    rows = _project_discovery_history(
        task_state={},
        now=datetime(2026, 5, 6, 19, 0, 0, tzinfo=UTC),
    )

    row = next(row for row in rows if row["runId"] == "discovery_child_1")
    assert row["status"] == "error"
    assert row["summary"]["error"] == "owner_inactive_without_terminal_report"


def test_active_pipeline_discovery_stage_keeps_quiet_child_discovery_running() -> None:
    rows = _project_discovery_history(
        task_state={},
        now=datetime(2026, 5, 6, 19, 0, 0, tzinfo=UTC),
        pipeline_status={
            "active": True,
            "stage": "discovery",
            "startedAt": "2026-05-06T17:59:00Z",
        },
    )

    row = next(row for row in rows if row["runId"] == "discovery_child_1")
    assert row["status"] == "started"
    assert row["finishedAt"] == ""
    assert "error" not in row["summary"]
