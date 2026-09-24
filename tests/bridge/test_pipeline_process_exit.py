from __future__ import annotations

from typing import Any

import pytest

from src.bridge.pipeline_service import PipelineService
from tests._pipeline_execution_shared import _install_fake_wait_clock, _projection_snapshot
from tests.helpers.pipeline_service_factory import make_pipeline_service


def _wait_service(
    *,
    process_observations: list[dict[str, Any]],
    reports: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    clock: dict[str, Any],
) -> PipelineService:
    def process_state(_task_type: str, _run_id: str) -> dict[str, Any]:
        return process_observations.pop(0) if process_observations else {"state": "unknown"}

    def load_report(_path: Any, _default: Any) -> dict[str, Any]:
        return reports.pop(0) if len(reports) > 1 else reports[0]

    def record_failure(*args: Any, **kwargs: Any) -> dict[str, Any]:
        failures.append({"args": args, **kwargs})
        return kwargs

    return make_pipeline_service(
        pipeline_status={
            "active": True,
            "runId": "pipeline_1",
            "stage": "discovery",
        },
        now_iso=lambda: clock["now"].isoformat().replace("+00:00", "Z"),
        load_json_object=load_report,
        get_child_process_state=process_state,
        get_projected_run_history=lambda: _projection_snapshot(
            task_type="discovery",
            run_id="discovery_1",
            active=True,
        ),
        fail_lifecycle_run=record_failure,
    )


def test_wait_fails_fast_for_confirmed_process_exit(monkeypatch, tmp_path) -> None:
    clock, waits = _install_fake_wait_clock(monkeypatch, start_at="2026-09-24T12:00:00Z")
    failures: list[dict[str, Any]] = []
    service = _wait_service(
        process_observations=[
            {
                "state": "exited",
                "pid": 39,
                "returnCode": -9,
                "signal": 9,
                "identitySource": "registered_popen",
            }
        ],
        reports=[
            {
                "runId": "discovery_1",
                "startedAt": "2026-09-24T12:00:01Z",
                "finishedAt": "",
            }
        ],
        failures=failures,
        clock=clock,
    )

    with pytest.raises(TimeoutError, match="child process exited"):
        service.wait_for_report_completion(
            report_path=tmp_path / "discovery-report.json",
            started_at="2026-09-24T12:00:01Z",
            timeout_s=900.0,
            report_name="discovery report",
            load_json_object=service._load_json_object,
            task_type="discovery",
            task_run_id="discovery_1",
        )

    assert waits == []
    assert failures[-1]["terminal_reason"] == "owner_inactive_without_terminal_report"
    assert failures[-1]["summary"] == {
        "error": (
            "owner_inactive_without_terminal_report: "
            "discovery report child process exited before terminal report"
        )
    }


def test_terminal_report_wins_over_exited_process_observation(monkeypatch, tmp_path) -> None:
    clock, waits = _install_fake_wait_clock(monkeypatch, start_at="2026-09-24T12:00:00Z")
    failures: list[dict[str, Any]] = []
    service = _wait_service(
        process_observations=[
            {
                "state": "exited",
                "pid": 39,
                "returnCode": -9,
                "identitySource": "registered_popen",
            }
        ],
        reports=[
            {
                "runId": "discovery_1",
                "startedAt": "2026-09-24T12:00:01Z",
                "finishedAt": "2026-09-24T12:00:02Z",
                "summary": {"status": "ok"},
            }
        ],
        failures=failures,
        clock=clock,
    )

    report = service.wait_for_report_completion(
        report_path=tmp_path / "discovery-report.json",
        started_at="2026-09-24T12:00:01Z",
        timeout_s=900.0,
        report_name="discovery report",
        load_json_object=service._load_json_object,
        task_type="discovery",
        task_run_id="discovery_1",
    )

    assert report["finishedAt"] == "2026-09-24T12:00:02Z"
    assert waits == []
    assert failures == []


def test_running_process_keeps_wait_alive_without_report_liveness(monkeypatch, tmp_path) -> None:
    clock, waits = _install_fake_wait_clock(monkeypatch, start_at="2026-09-24T12:00:00Z")
    failures: list[dict[str, Any]] = []
    service = _wait_service(
        process_observations=[
            {"state": "running", "pid": 39, "identitySource": "registered_popen"},
            {"state": "running", "pid": 39, "identitySource": "registered_popen"},
        ],
        reports=[
            {
                "runId": "discovery_1",
                "startedAt": "2026-09-24T12:00:01Z",
                "finishedAt": "",
            },
            {
                "runId": "discovery_1",
                "startedAt": "2026-09-24T12:00:01Z",
                "finishedAt": "",
            },
            {
                "runId": "discovery_1",
                "startedAt": "2026-09-24T12:00:01Z",
                "finishedAt": "2026-09-24T12:00:03Z",
                "summary": {"status": "ok"},
            },
        ],
        failures=failures,
        clock=clock,
    )

    report = service.wait_for_report_completion(
        report_path=tmp_path / "discovery-report.json",
        started_at="2026-09-24T12:00:01Z",
        timeout_s=900.0,
        report_name="discovery report",
        load_json_object=service._load_json_object,
        task_type="discovery",
        task_run_id="discovery_1",
    )

    assert report["finishedAt"] == "2026-09-24T12:00:03Z"
    assert len(waits) == 2
    assert failures == []
