from __future__ import annotations

from pathlib import Path
from typing import Any

from src.bridge.pipeline_service import PipelineAbortRequested, PipelineRuntime
from tests.bridge.test_pipeline_service import _make_pipeline_service


def test_pipeline_abort_during_discovery_wait_finishes_canceled() -> None:
    canceled_runs: list[dict[str, Any]] = []
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "discovery",
        "progress": {
            "currentStep": 1,
            "totalSteps": 3,
            "percent": 33,
            "label": "Running discovery...",
        },
        "startedAt": "2026-05-06T18:00:00Z",
        "finishedAt": "",
        "error": "",
        "baselineOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    runtime = PipelineRuntime(active_run_id="pipeline_1", abort_requests={})

    def refresh_child(task_type: str, _run_id: str, _started_at: str) -> bool:
        if task_type == "discovery":
            runtime.abort_requests = {"pipeline_1": {"reason": "test_abort"}}
        return True

    service = _make_pipeline_service(
        pipeline_status=status,
        runtime=runtime,
        trigger_discovery_task=lambda **_kwargs: (
            200,
            {"started": True, "runId": "discovery_1", "startedAt": "2026-05-06T18:00:05Z"},
        ),
        refresh_child_task_heartbeat=refresh_child,
        cancel_lifecycle_run=lambda run_id, task_type, **kwargs: (
            canceled_runs.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
    )

    service._run_worker("pipeline_1")

    assert status["active"] is False
    assert status["stage"] == "canceled"
    assert status["error"] == ""
    assert canceled_runs[-1]["runId"] == "pipeline_1"
    assert canceled_runs[-1]["terminal_reason"] == "user_abort_requested"


def test_pipeline_abort_during_fetch_wait_finishes_canceled() -> None:
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
    runtime = PipelineRuntime(active_run_id="pipeline_1", abort_requests={})

    def load_runtime(path: Any, default: Any = None) -> dict[str, Any]:
        if "source-discovery" in str(path):
            return {
                "runId": "discovery_1",
                "startedAt": "2026-05-06T18:00:05Z",
                "finishedAt": "2026-05-06T18:00:06Z",
                "summary": {},
                "taskProgress": {"active": False, "phaseKey": "completed"},
            }
        return dict(default or {})

    def refresh_child(task_type: str, _run_id: str, _started_at: str) -> bool:
        if task_type == "fetch":
            runtime.abort_requests = {"pipeline_1": {"reason": "test_abort"}}
        return True

    service = _make_pipeline_service(
        pipeline_status=status,
        runtime=runtime,
        load_runtime_evidence=load_runtime,
        trigger_discovery_task=lambda **_kwargs: (
            200,
            {"started": True, "runId": "discovery_1", "startedAt": "2026-05-06T18:00:05Z"},
        ),
        start_fetcher_task=lambda _payload: {
            "started": True,
            "runId": "fetch_1",
            "startedAt": "2026-05-06T18:00:07Z",
        },
        refresh_child_task_heartbeat=refresh_child,
        cancel_lifecycle_run=lambda run_id, task_type, **kwargs: (
            canceled_runs.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
    )

    service._run_worker("pipeline_1")

    assert status["active"] is False
    assert status["stage"] == "canceled"
    assert status["error"] == ""
    assert canceled_runs[-1]["runId"] == "pipeline_1"
    assert canceled_runs[-1]["terminal_reason"] == "user_abort_requested"


def test_child_report_wait_preserves_pipeline_abort_exception_type() -> None:
    service = _make_pipeline_service(
        pipeline_status={"active": True, "runId": "pipeline_1", "stage": "discovery"},
        runtime=PipelineRuntime(abort_requests={"pipeline_1": {"reason": "test_abort"}}),
    )

    try:
        service._wait_for_child_report(
            phase="discovery_wait",
            report_path=Path("source-discovery-report.json"),
            started_at="2026-05-06T18:00:00Z",
            timeout_s=10.0,
            report_name="discovery report",
            load_json_object=lambda _path, _default: {},
            task_type="discovery",
            task_run_id="discovery_1",
        )
    except PipelineAbortRequested as exc:
        assert str(exc) == "pipeline abort requested"
    else:
        raise AssertionError("expected PipelineAbortRequested")
