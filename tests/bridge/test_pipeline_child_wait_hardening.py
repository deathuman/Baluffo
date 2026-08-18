from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from src.bridge.pipeline_service import PipelineAbortRequested
from src.bridge.run_history_api import ChildTaskSnapshot, LifecycleProjection
from tests.bridge.test_pipeline_service import _make_pipeline_service
from tests.helpers.mutation import append_and_return


def test_wait_for_report_completion_fails_promptly_when_fetch_child_terminal_without_report(
    tmp_path: Path,
) -> None:
    failures: list[dict[str, Any]] = []
    events: list[tuple[str, dict[str, Any]]] = []
    service = _make_pipeline_service(
        pipeline_status={"runId": "pipeline_1", "stage": "fetch"},
        bridge_log=lambda level, message, **fields: events.append((str(message), dict(fields))),
        load_json_object=lambda _path, _default: {
            "runId": "fetch_1",
            "startedAt": "2026-05-06T18:00:00Z",
            "finishedAt": "",
        },
        get_projected_run_history=lambda: LifecycleProjection(
            rows=[],
            child_tasks={
                "fetch": ChildTaskSnapshot(
                    task_type="fetch",
                    run_id="fetch_1",
                    started_at="2026-05-06T18:00:00Z",
                    finished_at="2026-05-06T18:10:00Z",
                    active=False,
                    terminal_status="error",
                    summary={"error": "fetch child crashed"},
                    outputs={},
                    task_progress={},
                    explicit_dead=True,
                    diagnostics=(),
                )
            },
            diagnostics=[],
        ),
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            failures, {"runId": run_id, "taskType": task_type, **kwargs}, {}
        ),
    )

    with pytest.raises(TimeoutError, match="fetch child crashed"):
        service.wait_for_report_completion(
            report_path=tmp_path / "jobs-fetch-report.json",
            started_at="2026-05-06T18:00:00Z",
            timeout_s=1200.0,
            report_name="fetch report",
            load_json_object=service._load_json_object,
            task_type="fetch",
            task_run_id="fetch_1",
        )

    assert failures[-1]["runId"] == "fetch_1"
    assert failures[-1]["taskType"] == "fetch"
    assert failures[-1]["summary"]["error"] == "fetch child crashed"
    assert any(
        message == "jobs_pipeline_child_terminal_without_terminal_report" for message, _ in events
    )


def test_pipeline_propagates_terminal_fetch_report_error_code() -> None:
    service = _make_pipeline_service()
    cast(Any, service).wait_for_report_completion = lambda **_kwargs: {
        "status": "error",
        "finishedAt": "2026-07-17T08:03:00+00:00",
        "summary": {
            "error": "availability_identity_preflight_failed",
            "errorCode": "availability_identity_preflight_failed",
        },
    }

    with pytest.raises(
        RuntimeError,
        match="fetch_wait: availability_identity_preflight_failed",
    ):
        service._wait_for_child_report(phase="fetch_wait")


def test_wait_for_report_completion_cancels_when_discovery_child_canceled_without_report(
    tmp_path: Path,
) -> None:
    failures: list[dict[str, Any]] = []
    service = _make_pipeline_service(
        pipeline_status={"runId": "pipeline_1", "stage": "discovery"},
        load_json_object=lambda _path, _default: {
            "runId": "discovery_1",
            "startedAt": "2026-05-06T18:00:00Z",
            "finishedAt": "",
        },
        get_projected_run_history=lambda: LifecycleProjection(
            rows=[],
            child_tasks={
                "discovery": ChildTaskSnapshot(
                    task_type="discovery",
                    run_id="discovery_1",
                    started_at="2026-05-06T18:00:00Z",
                    finished_at="2026-05-06T18:02:00Z",
                    active=False,
                    terminal_status="canceled",
                    summary={"terminalReason": "user_abort_requested"},
                    outputs={},
                    task_progress={},
                    explicit_dead=True,
                    diagnostics=(),
                )
            },
            diagnostics=[],
        ),
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            failures, {"runId": run_id, "taskType": task_type, **kwargs}, {}
        ),
    )

    with pytest.raises(PipelineAbortRequested, match="pipeline child abort requested"):
        service.wait_for_report_completion(
            report_path=tmp_path / "source-discovery-report.json",
            started_at="2026-05-06T18:00:00Z",
            timeout_s=1200.0,
            report_name="discovery report",
            load_json_object=service._load_json_object,
            task_type="discovery",
            task_run_id="discovery_1",
        )

    assert failures == []


def test_live_child_evidence_extends_absolute_fetch_wait_cap(tmp_path: Path) -> None:
    failures: list[dict[str, Any]] = []
    heartbeats: list[tuple[str, str, str]] = []
    current: dict[str, Any] = {"now": datetime(2026, 5, 6, 18, 0, tzinfo=UTC), "calls": 0}

    def load_report(_path: Path, _default: dict[str, Any]) -> dict[str, Any]:
        current["calls"] = int(current["calls"]) + 1
        if int(current["calls"]) <= 5:
            return {
                "runId": "fetch_1",
                "startedAt": "2026-05-06T18:00:00Z",
                "finishedAt": "",
                "taskProgress": {
                    "active": True,
                    "phaseKey": "merging_results",
                    "counts": {
                        "sourceCount": 555,
                        "completedTasks": 555,
                        "runningTasks": 0,
                    },
                },
            }
        return {
            "runId": "fetch_1",
            "startedAt": "2026-05-06T18:00:00Z",
            "finishedAt": "2026-05-06T19:30:00Z",
            "summary": {"outputCount": 78329},
            "taskProgress": {"active": False, "phaseKey": "completed"},
        }

    service = _make_pipeline_service(
        pipeline_status={"runId": "pipeline_1", "stage": "fetch"},
        load_json_object=load_report,
        refresh_child_task_heartbeat=lambda task_type, run_id, started_at: append_and_return(
            heartbeats, (task_type, run_id, started_at), True
        ),
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            failures, {"runId": run_id, "taskType": task_type, **kwargs}, {}
        ),
    )
    service._report_wait_now = lambda: current["now"]  # type: ignore[method-assign]
    service._report_wait_sleep = lambda seconds: current.update(  # type: ignore[method-assign]
        {"now": current["now"] + timedelta(seconds=1000)}
    )

    report = service.wait_for_report_completion(
        report_path=tmp_path / "jobs-fetch-report.json",
        started_at="2026-05-06T18:00:00Z",
        timeout_s=10.0,
        report_name="fetch report",
        load_json_object=service._load_json_object,
        task_type="fetch",
        task_run_id="fetch_1",
    )

    assert report["finishedAt"] == "2026-05-06T19:30:00Z"
    assert int(current["calls"]) == 6
    assert len(heartbeats) >= 5
    assert current["now"] >= datetime(2026, 5, 6, 19, 6, tzinfo=UTC)
    assert failures == []
