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


def test_wait_for_report_completion_finalizes_terminal_child_before_parent_abort() -> None:
    runtime = PipelineRuntime(abort_requests={"pipeline_1": {"reason": "test_abort"}})
    finished_children: list[dict[str, Any]] = []
    report = {
        "runId": "fetch_1",
        "startedAt": "2026-05-06T18:00:00Z",
        "finishedAt": "2026-05-06T18:05:00Z",
        "summary": {"outputCount": 4},
        "taskProgress": {"active": True, "phaseKey": "done"},
    }
    service = _make_pipeline_service(
        pipeline_status={"active": True, "runId": "pipeline_1", "stage": "fetch"},
        runtime=runtime,
        load_json_object=lambda _path, _default: dict(report),
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: (
            finished_children.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
    )

    result = service.wait_for_report_completion(
        report_path=Path("jobs-fetch-report.json"),
        started_at="2026-05-06T18:00:00Z",
        timeout_s=10.0,
        report_name="fetch report",
        load_json_object=lambda _path, _default: dict(report),
        task_type="fetch",
        task_run_id="fetch_1",
    )

    assert result["runId"] == "fetch_1"
    assert finished_children[-1]["runId"] == "fetch_1"
    assert finished_children[-1]["terminal_reason"] == "completed"


def test_wait_for_report_completion_does_not_finalize_abort_requested_child() -> None:
    finished_children: list[dict[str, Any]] = []
    report = {
        "runId": "fetch_1",
        "startedAt": "2026-05-06T18:00:00Z",
        "finishedAt": "2026-05-06T18:05:00Z",
        "summary": {"outputCount": 4},
        "taskProgress": {"active": True, "phaseKey": "done"},
    }
    service = _make_pipeline_service(
        pipeline_status={"active": True, "runId": "pipeline_1", "stage": "fetch"},
        load_json_object=lambda _path, _default: dict(report),
        get_projected_run_history=lambda: LifecycleProjection(
            rows=[
                {
                    "runId": "fetch_1",
                    "taskType": "fetch",
                    "status": "running",
                    "stage": "aborting",
                    "summary": {"abortRequestedAt": "2026-05-06T18:04:00Z"},
                    "taskProgress": {"phaseKey": "aborting"},
                }
            ],
            child_tasks={},
            diagnostics=[],
        ),
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: (
            finished_children.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
    )

    try:
        service.wait_for_report_completion(
            report_path=Path("jobs-fetch-report.json"),
            started_at="2026-05-06T18:00:00Z",
            timeout_s=10.0,
            report_name="fetch report",
            load_json_object=lambda _path, _default: dict(report),
            task_type="fetch",
            task_run_id="fetch_1",
        )
    except Exception as exc:
        assert exc.__class__.__name__ == "PipelineAbortRequested"
    else:
        raise AssertionError("expected PipelineAbortRequested")

    assert finished_children == []


def test_pipeline_stage_heartbeat_uses_normalized_lifecycle_progress() -> None:
    parent_heartbeats: list[dict[str, Any]] = []
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "baselineOutputCount": 12,
        "jobsPageLoadedCount": 15,
        "finalOutputCount": 0,
    }
    service = _make_pipeline_service(
        pipeline_status=status,
        heartbeat_lifecycle_run=lambda _run_id, _task_type, **kwargs: (
            parent_heartbeats.append(dict(kwargs)) or {}
        ),
    )

    service._mark_stage(stage="fetch", current_step=2, total_steps=3, label="Running fetch...")

    assert status["progress"] == {
        "currentStep": 2,
        "totalSteps": 3,
        "percent": 67,
        "label": "Running fetch...",
    }
    progress = parent_heartbeats[-1]["progress"]
    assert progress["active"] is True
    assert progress["phaseKey"] == "fetch"
    assert progress["phaseLabel"] == "Running fetch..."
    assert progress["mode"] == "determinate"
    assert progress["counts"]["currentStep"] == 2
    assert progress["counts"]["totalSteps"] == 3
    assert progress["counts"]["baselineOutputCount"] == 12
    assert progress["counts"]["jobsPageLoadedCount"] == 15


def test_pipeline_completion_uses_normalized_lifecycle_progress() -> None:
    finished_runs: list[dict[str, Any]] = []
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "fetch",
        "progress": {"currentStep": 2, "totalSteps": 3, "percent": 67, "label": "Running fetch..."},
        "baselineOutputCount": 12,
        "jobsPageLoadedCount": 15,
        "finalOutputCount": 0,
    }
    service = _make_pipeline_service(
        pipeline_status=status,
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: (
            finished_runs.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
    )

    service._set_completed(status="ok", final_output_count=20)

    assert status["progress"] == {
        "currentStep": 3,
        "totalSteps": 3,
        "percent": 100,
        "label": "Pipeline completed",
    }
    progress = finished_runs[-1]["progress"]
    assert progress["active"] is False
    assert progress["phaseKey"] == "completed"
    assert progress["phaseLabel"] == "Pipeline completed"
    assert progress["counts"]["currentStep"] == 3
    assert progress["counts"]["totalSteps"] == 3
    assert progress["counts"]["baselineOutputCount"] == 12
    assert progress["counts"]["jobsPageLoadedCount"] == 15
    assert progress["counts"]["finalOutputCount"] == 20


def test_pipeline_abort_marks_canceled_lifecycle() -> None:
    canceled_runs: list[dict[str, Any]] = []
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "fetch",
        "progress": {"currentStep": 2, "totalSteps": 3, "percent": 67, "label": "Running fetch..."},
        "startedAt": "2026-05-06T18:00:00Z",
        "finishedAt": "",
        "error": "",
        "baselineOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    runtime = PipelineRuntime(active_run_id="pipeline_1")
    service = _make_pipeline_service(
        pipeline_status=status,
        runtime=runtime,
        cancel_lifecycle_run=lambda run_id, task_type, **kwargs: (
            canceled_runs.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
    )

    result = service.request_abort(
        "pipeline_1",
        reason="test_abort",
        requested_at="2026-05-06T18:10:00Z",
    )
    service._run_worker("pipeline_1")

    assert result["state"] == "aborting"
    assert status["active"] is False
    assert status["stage"] == "canceled"
    assert canceled_runs[-1]["runId"] == "pipeline_1"
    assert canceled_runs[-1]["terminal_reason"] == "user_abort_requested"


def test_pipeline_abort_during_sync_is_deferred_until_sync_finishes() -> None:
    canceled_runs: list[dict[str, Any]] = []
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
        "jobsPageLoadedCount": 0,
    }
    service = _make_pipeline_service(
        pipeline_status=status,
        start_sync_task=lambda _action, **_kwargs: {"started": True, "runId": "sync_1"},
        wait_for_sync_completion=lambda _run_id, _timeout_s: {"status": "ok", "summary": {}},
        cancel_lifecycle_run=lambda run_id, task_type, **kwargs: (
            canceled_runs.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
    )

    result = service.request_abort(
        "pipeline_1",
        reason="test_abort",
        requested_at="2026-05-06T18:10:00Z",
    )
    try:
        service._run_sync_push_stage("pipeline_1")
    except Exception as exc:
        assert exc.__class__.__name__ == "PipelineAbortRequested"
    service._set_completed(status="canceled")

    assert result["deferred"] is True
    assert status["stage"] == "canceled"
    assert canceled_runs[-1]["terminal_reason"] == "user_abort_requested"


def test_pipeline_abort_pending_sync_is_idempotent() -> None:
    heartbeats: list[dict[str, Any]] = []
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "abort_pending_sync",
        "progress": {
            "currentStep": 3,
            "totalSteps": 3,
            "percent": 100,
            "label": "Abort after sync...",
        },
        "startedAt": "2026-05-06T18:00:00Z",
    }
    service = _make_pipeline_service(
        pipeline_status=status,
        heartbeat_lifecycle_run=lambda _run_id, _task_type, **kwargs: (
            heartbeats.append(dict(kwargs)) or {}
        ),
    )

    result = service.request_abort(
        "pipeline_1",
        reason="second_click",
        requested_at="2026-05-06T18:11:00Z",
    )
    service._check_abort("pipeline_1", defer_sync=True)

    assert result["deferred"] is True
    assert result["state"] == "abort_pending_sync"
    assert status["stage"] == "abort_pending_sync"
    assert heartbeats[-1]["stage"] == "abort_pending_sync"


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


def test_pipeline_completion_notifier_fires_once_for_long_terminal_run() -> None:
    calls: list[dict[str, Any]] = []
    logs: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    service = _make_pipeline_service(
        now_iso=lambda: "2026-05-06T19:02:00Z",
        bridge_log=lambda *args, **kwargs: logs.append((args, kwargs)),
        pipeline_completion_notifier=lambda payload: (
            calls.append(payload) or {"notified": True, "reason": "notified", "hwnd": 101}
        ),
    )
    service._status.update(
        {
            "runId": "pipeline_1",
            "startedAt": "2026-05-06T19:00:00Z",
            "baselineOutputCount": 0,
            "jobsPageLoadedCount": 0,
        }
    )

    service._set_completed(status="ok", final_output_count=5)
    service._set_completed(status="ok", final_output_count=5)

    assert len(calls) == 1
    assert calls[0]["runId"] == "pipeline_1"
    assert calls[0]["durationSeconds"] == 120.0
    assert calls[0]["status"] == "completed"
    assert calls[0]["updatesFound"] is True
    assert any(args[1] == "jobs_pipeline_completion_attention" for args, _kwargs in logs)


def test_pipeline_completion_notifier_failure_does_not_block_terminal_status() -> None:
    logs: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def _raise(_payload: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("attention failed")

    service = _make_pipeline_service(
        now_iso=lambda: "2026-05-06T19:02:00Z",
        bridge_log=lambda *args, **kwargs: logs.append((args, kwargs)),
        pipeline_completion_notifier=_raise,
    )
    service._status.update(
        {
            "active": True,
            "runId": "pipeline_1",
            "startedAt": "2026-05-06T19:00:00Z",
            "baselineOutputCount": 0,
            "jobsPageLoadedCount": 0,
        }
    )

    service._set_completed(status="ok", final_output_count=5)
    payload = service.get_status_payload()

    assert payload["active"] is False
    assert payload["stage"] == "completed"
    assert any(args[1] == "jobs_pipeline_completion_attention_failed" for args, _kwargs in logs)


def test_pipeline_completion_notifier_skips_short_terminal_run() -> None:
    calls: list[dict[str, Any]] = []
    service = _make_pipeline_service(
        now_iso=lambda: "2026-05-06T19:00:59Z",
        pipeline_completion_notifier=lambda payload: calls.append(payload),
    )
    service._status.update(
        {
            "runId": "pipeline_1",
            "startedAt": "2026-05-06T19:00:00Z",
            "baselineOutputCount": 0,
            "jobsPageLoadedCount": 0,
        }
    )

    service._set_completed(status="ok", final_output_count=5)

    assert calls == []
