from __future__ import annotations

from typing import Any

from src.bridge.run_history_api import ChildTaskSnapshot, LifecycleProjection
from tests.bridge.test_pipeline_service import _make_pipeline_service


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
    service = _make_pipeline_service(
        pipeline_status=status,
        current_fetch_output_count=lambda: 42,
        start_sync_task=lambda _action, **_kwargs: {"started": True, "runId": "sync_1"},
        wait_for_sync_completion=lambda _run_id, _timeout_s: {
            "status": "error",
            "summary": {"error": conflict},
        },
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: (
            finished_runs.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
    )
    service._run_discovery_stage = lambda _run_id: None  # type: ignore[method-assign]
    service._run_fetch_stage = lambda _run_id: None  # type: ignore[method-assign]
    service._run_registry_conflict_adjudication_stage = lambda _run_id: None  # type: ignore[method-assign]

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


def test_status_payload_recovers_inactive_pipeline_worker_after_recoverable_sync_conflict() -> None:
    finished_runs: list[dict[str, Any]] = []
    conflict = "sha does not match"
    service = _make_pipeline_service(
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
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: (
            finished_runs.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
    )

    payload = service.get_status_payload()

    assert payload["active"] is False
    assert payload["stage"] == "completed_with_warnings"
    assert payload["error"] == ""
    assert payload["completedWithWarnings"] is True
    assert payload["syncWarning"]["kind"] == "recoverable_remote_conflict"
    assert finished_runs[-1]["terminal_reason"] == "completed_with_warnings"
