from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from src.bridge.pipeline_service import PipelineRuntime, PipelineService
from src.bridge.run_history_api import ChildTaskSnapshot, LifecycleProjection
from tests.helpers.mutation import append_and_return


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
        refresh_child_task_heartbeat=lambda task_type, run_id, started_at: append_and_return(
            refreshed, (task_type, run_id, started_at), True
        ),
        heartbeat_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            parent_heartbeats, (run_id, task_type, str(kwargs.get("stage") or "")), {}
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
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            finished_children, {"runId": run_id, "taskType": task_type, **kwargs}, {}
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
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            finished_children, {"runId": run_id, "taskType": task_type, **kwargs}, {}
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
        heartbeat_lifecycle_run=lambda _run_id, _task_type, **kwargs: append_and_return(
            parent_heartbeats, dict(kwargs), {}
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
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            finished_runs, {"runId": run_id, "taskType": task_type, **kwargs}, {}
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
        cancel_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            canceled_runs, {"runId": run_id, "taskType": task_type, **kwargs}, {}
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
        cancel_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            canceled_runs, {"runId": run_id, "taskType": task_type, **kwargs}, {}
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
        heartbeat_lifecycle_run=lambda _run_id, _task_type, **kwargs: append_and_return(
            heartbeats, dict(kwargs), {}
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
        heartbeat_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            parent_heartbeats, (run_id, task_type, str(kwargs.get("stage") or "")), {}
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
        heartbeat_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            parent_heartbeats, (run_id, task_type, str(kwargs.get("stage") or "")), {}
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
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            finished_children, {"runId": run_id, "taskType": task_type, **kwargs}, {}
        ),
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            failed_runs, {"runId": run_id, "taskType": task_type, **kwargs}, {}
        ),
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


def test_status_payload_warns_after_inactive_pipeline_worker_terminal_sync_failure() -> None:
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
    finished_runs: list[dict[str, Any]] = []

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
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            finished_runs, {"runId": run_id, "taskType": task_type, **kwargs}, {}
        ),
    )

    payload = service.get_status_payload()

    assert payload["active"] is False
    assert payload["stage"] == "completed_with_warnings"
    assert payload["error"] == ""
    assert payload["syncStatus"] == "warning"
    assert payload["syncWarning"]["kind"] == "sync_push_failed"
    assert payload["syncWarning"]["message"] == "Snapshot size exceeded"
    assert payload["finalOutputCount"] == 42
    assert finished_runs[-1]["runId"] == "pipeline_1"
    assert finished_runs[-1]["terminal_reason"] == "completed_with_warnings"


def test_pipeline_completion_notifier_fires_once_for_long_terminal_run() -> None:
    calls: list[dict[str, Any]] = []
    logs: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    service = _make_pipeline_service(
        now_iso=lambda: "2026-05-06T19:02:00Z",
        bridge_log=lambda *args, **kwargs: logs.append((args, kwargs)),
        pipeline_completion_notifier=lambda payload: append_and_return(
            calls, payload, {"notified": True, "reason": "notified", "hwnd": 101}
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
