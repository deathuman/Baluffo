from __future__ import annotations

from pathlib import Path
from typing import Any

from src.bridge.pipeline_control_files import (
    abort_request_path,
    abort_requests_dir,
    read_pipeline_status,
    write_abort_request,
)
from src.bridge.pipeline_service import PipelineAbortRequested, PipelineRuntime, PipelineService
from src.source_registry_io import load_runtime_evidence
from tests.helpers.mutation import append_and_return


def _make_pipeline_service(**overrides: Any) -> PipelineService:
    kwargs: dict[str, Any] = {
        "pipeline_state_lock": __import__("threading").RLock(),
        "pipeline_status": {},
        "runtime": PipelineRuntime(),
        "bridge_log": lambda *args, **kwargs: None,
        "now_iso": lambda: "2026-05-06T19:00:00Z",
        "parse_iso": lambda _value: None,
        "append_run_history": lambda row: row,
        "upsert_run_history": lambda entry, **_kwargs: entry,
        "sync_task_running": lambda: False,
        "current_fetch_output_count": lambda: 0,
        "load_json_object": lambda _path, default: default,
        "load_runtime_evidence": lambda _path, default=None: default or {},
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


def test_pipeline_service_consumes_container_gateway_abort_request(tmp_path: Path) -> None:
    runtime = PipelineRuntime(abort_requests={})
    service = _make_pipeline_service(
        pipeline_status={"active": True, "runId": "pipeline_1", "stage": "fetch"},
        runtime=runtime,
        control_data_dir=tmp_path,
    )
    write_abort_request(
        tmp_path,
        run_id="pipeline_1",
        task_type="pipeline",
        reason="test",
        requested_at="2026-05-06T19:00:01Z",
    )

    try:
        service._check_abort("pipeline_1")  # noqa: SLF001
    except PipelineAbortRequested:
        pass
    else:
        raise AssertionError("expected gateway abort request to cancel pipeline")

    assert runtime.abort_requests == {
        "pipeline_1": {
            "requestedAt": "2026-05-06T19:00:01Z",
            "reason": "test",
        }
    }


def test_abort_request_path_sanitizes_run_id_under_abort_root(tmp_path: Path) -> None:
    run_id = r"..\..\outside/pipeline_1"
    target = abort_request_path(tmp_path, run_id)
    root = abort_requests_dir(tmp_path)

    assert target.parent == root
    assert target.name == "outsidepipeline_1.json"
    assert target.resolve().relative_to(root.resolve())

    write_abort_request(
        tmp_path,
        run_id=run_id,
        task_type="pipeline",
        reason="test",
        requested_at="2026-05-06T19:00:01Z",
    )

    assert target.exists()
    assert not (tmp_path / "outside" / "pipeline_1.json").exists()


def test_pipeline_control_abort_requests_child_and_waits_for_live_child(
    tmp_path: Path,
) -> None:
    runtime = PipelineRuntime(abort_requests={})
    child_abort_requests: list[tuple[str, str, str]] = []
    live_children = {"fetch_1": True}
    status: dict[str, Any] = {"active": True, "runId": "pipeline_1", "stage": "fetch"}
    service = _make_pipeline_service(
        pipeline_status=status,
        runtime=runtime,
        control_data_dir=tmp_path,
        child_run_is_live=lambda _task_type, run_id: bool(live_children.get(run_id)),
        abort_child_run=lambda task_type, run_id, reason: append_and_return(
            child_abort_requests, (task_type, run_id, reason), {"ok": True}
        ),
    )
    service._attach_lifecycle_child_row(  # noqa: SLF001
        run_id="pipeline_1",
        task_type="fetch",
        child_run_id="fetch_1",
        child_started_at="2026-05-06T19:00:02Z",
    )
    write_abort_request(
        tmp_path,
        run_id="pipeline_1",
        task_type="pipeline",
        reason="gateway_abort",
        requested_at="2026-05-06T19:00:03Z",
    )

    service._check_abort("pipeline_1")  # noqa: SLF001
    service._set_completed(status="canceled", final_output_count=10)  # noqa: SLF001

    assert child_abort_requests == [("fetch", "fetch_1", "gateway_abort")]
    assert status["active"] is True
    assert status["stage"] == "aborting"
    assert status["activeChildren"][0]["runId"] == "fetch_1"

    live_children["fetch_1"] = False
    try:
        service._check_abort("pipeline_1")  # noqa: SLF001
    except PipelineAbortRequested:
        pass
    else:
        raise AssertionError("expected gateway abort to finish once child is not live")

    service._set_completed(status="canceled", final_output_count=10)  # noqa: SLF001
    assert status["active"] is False
    assert status["stage"] == "canceled"


def test_pipeline_control_retries_child_abort_while_child_remains_live(
    tmp_path: Path,
) -> None:
    runtime = PipelineRuntime(
        abort_requests={
            "pipeline_1": {
                "requestedAt": "2026-05-06T19:00:03Z",
                "reason": "gateway_abort",
            }
        }
    )
    child_abort_requests: list[tuple[str, str, str]] = []
    status: dict[str, Any] = {"active": True, "runId": "pipeline_1", "stage": "fetch"}
    service = _make_pipeline_service(
        pipeline_status=status,
        runtime=runtime,
        control_data_dir=tmp_path,
        child_run_is_live=lambda _task_type, run_id: run_id == "fetch_1",
        abort_child_run=lambda task_type, run_id, reason: append_and_return(
            child_abort_requests,
            (task_type, run_id, reason),
            {"ok": True, "warnings": ["process_not_registered"]},
        ),
    )
    service._attach_lifecycle_child_row(  # noqa: SLF001
        run_id="pipeline_1",
        task_type="fetch",
        child_run_id="fetch_1",
        child_started_at="2026-05-06T19:00:02Z",
    )

    service._check_abort("pipeline_1")  # noqa: SLF001
    service._check_abort("pipeline_1")  # noqa: SLF001

    assert child_abort_requests == [
        ("fetch", "fetch_1", "gateway_abort"),
        ("fetch", "fetch_1", "gateway_abort"),
    ]
    assert status["active"] is True
    assert status["stage"] == "aborting"
    assert status["warnings"] == ["process_not_registered"]


def test_pipeline_control_records_warning_when_child_abort_unavailable(
    tmp_path: Path,
) -> None:
    runtime = PipelineRuntime(
        abort_requests={
            "pipeline_1": {
                "requestedAt": "2026-05-06T19:00:03Z",
                "reason": "gateway_abort",
            }
        }
    )
    status: dict[str, Any] = {"active": True, "runId": "pipeline_1", "stage": "fetch"}
    service = _make_pipeline_service(
        pipeline_status=status,
        runtime=runtime,
        control_data_dir=tmp_path,
        child_run_is_live=lambda _task_type, run_id: run_id == "fetch_1",
    )
    service._attach_lifecycle_child_row(  # noqa: SLF001
        run_id="pipeline_1",
        task_type="fetch",
        child_run_id="fetch_1",
        child_started_at="2026-05-06T19:00:02Z",
    )

    service._check_abort("pipeline_1")  # noqa: SLF001

    assert status["active"] is True
    assert status["stage"] == "aborting"
    assert status["warnings"] == ["child_abort_unavailable:fetch:fetch_1"]


def test_pipeline_service_writes_active_child_control_snapshot(tmp_path: Path) -> None:
    service = _make_pipeline_service(
        pipeline_status={"active": True, "runId": "pipeline_1", "stage": "fetch"},
        control_data_dir=tmp_path,
    )

    service._attach_lifecycle_child_row(  # noqa: SLF001
        run_id="pipeline_1",
        task_type="fetch",
        child_run_id="fetch_1",
        child_started_at="2026-05-06T19:00:02Z",
    )

    payload = read_pipeline_status(tmp_path)
    children = payload.get("activeChildren")
    assert isinstance(children, list)
    assert children[0]["taskType"] == "fetch"
    assert children[0]["runId"] == "fetch_1"
    assert children[0]["parentRunId"] == "pipeline_1"
    assert children[0]["controlPlaneSource"] == "pipeline-status"
    assert children[0]["displayOnly"] is True
    assert children[0]["taskProgress"]["phaseLabel"] == "Fetch running"
    assert payload["activeChildTaskType"] == "fetch"
    assert payload["activeChildRunId"] == "fetch_1"


def test_pipeline_service_refreshes_control_snapshot_during_child_heartbeat(
    tmp_path: Path,
) -> None:
    timestamps = iter(["2026-05-06T19:00:00Z", "2026-05-06T19:00:15Z"])
    service = _make_pipeline_service(
        pipeline_status={"active": True, "runId": "pipeline_1", "stage": "fetch"},
        control_data_dir=tmp_path,
        now_iso=lambda: next(timestamps),
    )
    service._attach_lifecycle_child_row(  # noqa: SLF001
        run_id="pipeline_1",
        task_type="fetch",
        child_run_id="fetch_1",
        child_started_at="2026-05-06T19:00:02Z",
    )

    service._heartbeat_pipeline_wait()  # noqa: SLF001

    payload = read_pipeline_status(tmp_path)
    assert payload["active"] is True
    assert payload["stage"] == "fetch"
    assert payload["snapshotAt"] == "2026-05-06T19:00:15Z"
    assert payload["activeChildren"][0]["runId"] == "fetch_1"


def test_pipeline_service_clears_active_children_on_terminal_status(tmp_path: Path) -> None:
    service = _make_pipeline_service(
        pipeline_status={"active": True, "runId": "pipeline_1", "stage": "fetch"},
        control_data_dir=tmp_path,
    )
    service._attach_lifecycle_child_row(  # noqa: SLF001
        run_id="pipeline_1",
        task_type="fetch",
        child_run_id="fetch_1",
        child_started_at="2026-05-06T19:00:02Z",
    )

    service._set_completed(status="ok", final_output_count=123)  # noqa: SLF001

    payload = read_pipeline_status(tmp_path)
    assert payload["active"] is False
    assert payload["activeChildren"] == []
    assert payload["activeChildTaskType"] == ""
    assert payload["activeChildRunId"] == ""
    snapshot = load_runtime_evidence(tmp_path / "admin-active-task-snapshot.json", {})
    assert snapshot["count"] == 1
    assert snapshot["tasks"][0]["taskType"] == "pipeline"
    assert snapshot["tasks"][0]["active"] is False
