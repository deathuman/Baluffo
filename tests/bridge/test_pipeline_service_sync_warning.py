from __future__ import annotations

from typing import Any

import pytest

from src.bridge.run_history_api import ChildTaskSnapshot, LifecycleProjection
from tests.helpers.mutation import append_and_return
from tests.helpers.pipeline_service_factory import make_pipeline_service


def test_pipeline_completes_with_warning_for_recoverable_sync_conflict() -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "starting",
        "progress": {},
        "startedAt": "2026-05-06T18:00:00Z",
        "finishedAt": "",
        "error": "",
        "baselineOutputCount": 10,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 10,
    }
    finished_runs: list[dict[str, Any]] = []
    conflict = (
        "is at a8f0ae858e0e7c8ecafe671bf9825f6e7328dd97 "
        "but expected db2c4166cf428892f165629d27933ce492d346d1"
    )
    service = make_pipeline_service(
        pipeline_status=status,
        current_fetch_output_count=lambda: 42,
        start_sync_task=lambda _action, **_kwargs: {"started": True, "runId": "sync_1"},
        wait_for_sync_completion=lambda _run_id, _timeout_s: {
            "status": "error",
            "summary": {"error": conflict},
        },
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            finished_runs, {"runId": run_id, "taskType": task_type, **kwargs}, {}
        ),
    )
    service._run_discovery_stage = lambda run_id: None  # type: ignore[method-assign]
    service._run_fetch_stage = lambda run_id: None  # type: ignore[method-assign]
    service._run_registry_conflict_adjudication_stage = lambda run_id: None  # type: ignore[method-assign]

    service._run_worker("pipeline_1")
    payload = service.get_status_payload()

    assert payload["active"] is False
    assert payload["stage"] == "completed_with_warnings"
    assert payload["error"] == ""
    assert payload["completedWithWarnings"] is True
    assert payload["syncStatus"] == "warning"
    assert payload["syncWarning"]["kind"] == "recoverable_remote_conflict"
    assert payload["warnings"][0]["message"] == conflict
    assert payload["updatesFound"] is True
    assert finished_runs[-1]["terminal_reason"] == "completed_with_warnings"
    assert finished_runs[-1]["summary"]["syncWarning"]["kind"] == "recoverable_remote_conflict"


def test_fetch_failure_warning_uses_defined_threshold() -> None:
    service = make_pipeline_service()

    assert (
        service._fetch_failure_warning({"summary": {"failedSources": 0, "sourceCount": 10}}) is None
    )
    warning = service._fetch_failure_warning({"summary": {"failedSources": 1, "sourceCount": 10}})

    assert warning is not None
    assert warning["kind"] == "fetch_sources_failed"
    assert warning["threshold"] == 1
    assert warning["message"] == "1 of 10 sources failed during fetch."


def test_pipeline_completes_with_warning_for_fetch_failures_and_snapshot_size() -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "starting",
        "progress": {},
        "startedAt": "2026-05-06T18:00:00Z",
        "finishedAt": "",
        "error": "",
        "baselineOutputCount": 10,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 10,
    }
    finished_runs: list[dict[str, Any]] = []
    service = make_pipeline_service(
        pipeline_status=status,
        current_fetch_output_count=lambda: 42,
        start_fetcher_task=lambda _payload: {
            "started": True,
            "runId": "fetch_1",
            "startedAt": "2026-05-06T18:00:00Z",
        },
        start_sync_task=lambda _action, **_kwargs: {
            "started": True,
            "runId": "sync_1",
            "startedAt": "2026-05-06T18:40:00Z",
        },
        wait_for_sync_completion=lambda _run_id, _timeout_s: {
            "status": "ok",
            "summary": {
                "sizeWarning": True,
                "sizeBytes": 6_000_000,
                "maxSnapshotSizeBytes": 100_000_000,
            },
        },
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            finished_runs, {"runId": run_id, "taskType": task_type, **kwargs}, {}
        ),
    )
    service._run_discovery_stage = lambda run_id: None  # type: ignore[method-assign]
    service._run_registry_conflict_adjudication_stage = lambda run_id: None  # type: ignore[method-assign]
    service._wait_for_child_report = lambda **_kwargs: {  # type: ignore[method-assign]
        "summary": {"failedSources": 3, "sourceCount": 80, "outputCount": 42}
    }

    service._run_worker("pipeline_1")
    payload = service.get_status_payload()

    assert payload["active"] is False
    assert payload["stage"] == "completed_with_warnings"
    assert payload["completedWithWarnings"] is True
    assert payload["syncStatus"] == "warning"
    assert [warning["kind"] for warning in payload["warnings"]] == [
        "fetch_sources_failed",
        "snapshot_size_warning",
    ]
    assert payload["warnings"][0]["failedSources"] == 3
    assert payload["warnings"][0]["threshold"] == 1
    assert payload["warnings"][1]["sizeBytes"] == 6_000_000
    assert payload["warnings"][1]["thresholdBytes"] == 5 * 1024 * 1024
    assert payload["syncWarning"]["kind"] == "snapshot_size_warning"
    assert payload["updatesFound"] is True
    assert finished_runs[-1]["terminal_reason"] == "completed_with_warnings"


def test_status_payload_recovers_inactive_pipeline_worker_after_snapshot_size_warning() -> None:
    finished_runs: list[dict[str, Any]] = []
    service = make_pipeline_service(
        pipeline_status={
            "active": True,
            "runId": "pipeline_1",
            "stage": "sync_push",
            "progress": {"currentStep": 3, "totalSteps": 3, "label": "Running sync push..."},
            "startedAt": "2026-05-06T18:00:00Z",
            "finishedAt": "",
            "error": "",
        },
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
                    terminal_status="ok",
                    summary={"sizeWarning": True, "sizeBytes": 6_000_000},
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
    assert payload["completedWithWarnings"] is True
    assert payload["syncStatus"] == "warning"
    assert payload["syncWarning"]["kind"] == "snapshot_size_warning"
    assert finished_runs[-1]["terminal_reason"] == "completed_with_warnings"


@pytest.mark.parametrize(
    ("status", "error", "expected_stage", "expected_terminal"),
    [
        ("error", "pipeline failed", "error", "failed"),
        ("canceled", "", "canceled", "user_abort_requested"),
    ],
)
def test_error_and_canceled_completions_never_become_completed_with_warnings(
    status: str,
    error: str,
    expected_stage: str,
    expected_terminal: str,
) -> None:
    finished_runs: list[dict[str, Any]] = []
    failed_runs: list[dict[str, Any]] = []
    canceled_runs: list[dict[str, Any]] = []
    service = make_pipeline_service(
        pipeline_status={
            "active": True,
            "runId": "pipeline_1",
            "stage": "fetch",
            "progress": {},
            "startedAt": "2026-05-06T18:00:00Z",
            "finishedAt": "",
            "error": "",
            "baselineOutputCount": 0,
            "finalOutputCount": 0,
            "jobsPageLoadedCount": 0,
        },
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            finished_runs, {"runId": run_id, "taskType": task_type, **kwargs}, {}
        ),
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            failed_runs, {"runId": run_id, "taskType": task_type, **kwargs}, {}
        ),
        cancel_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            canceled_runs, {"runId": run_id, "taskType": task_type, **kwargs}, {}
        ),
    )

    service._set_completed(
        status=status,
        error=error,
        final_output_count=0,
        warnings=[{"kind": "fetch_sources_failed"}],
        sync_warning={"kind": "sync_push_failed"},
    )
    payload = service.get_status_payload()

    assert payload["stage"] == expected_stage
    assert payload["completedWithWarnings"] is False
    assert payload["warnings"] == [{"kind": "fetch_sources_failed"}]
    assert payload["syncWarning"] == {"kind": "sync_push_failed"}
    assert finished_runs == []
    terminal_runs = failed_runs if status == "error" else canceled_runs
    assert terminal_runs[-1]["terminal_reason"] == expected_terminal


def test_status_payload_recovers_inactive_pipeline_worker_after_recoverable_sync_conflict() -> None:
    finished_runs: list[dict[str, Any]] = []
    conflict = "sha does not match"
    service = make_pipeline_service(
        pipeline_status={
            "active": True,
            "runId": "pipeline_1",
            "stage": "sync_push",
            "progress": {"currentStep": 3, "totalSteps": 3, "label": "Running sync push..."},
            "startedAt": "2026-05-06T18:00:00Z",
            "finishedAt": "",
            "error": "",
        },
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
                    summary={"error": conflict},
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
    assert payload["completedWithWarnings"] is True
    assert payload["syncWarning"]["kind"] == "recoverable_remote_conflict"
    assert finished_runs[-1]["terminal_reason"] == "completed_with_warnings"
