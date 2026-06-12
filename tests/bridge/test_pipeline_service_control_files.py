from __future__ import annotations

from pathlib import Path
from typing import Any

from src.bridge.pipeline_control_files import write_abort_request
from src.bridge.pipeline_service import PipelineAbortRequested, PipelineRuntime, PipelineService


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
        "task_running_from_state": lambda _task_type: False,
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
