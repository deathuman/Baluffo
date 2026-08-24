from __future__ import annotations

import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from src.bridge.pipeline_service import PipelineRuntime, PipelineService
from tests.helpers.mutation import append_and_return


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _make_service(**overrides: Any) -> PipelineService:
    kwargs: dict[str, Any] = {
        "pipeline_state_lock": threading.RLock(),
        "pipeline_status": {},
        "runtime": PipelineRuntime(),
        "bridge_log": lambda *args, **kwargs: None,
        "now_iso": lambda: "2026-05-06T19:00:00Z",
        "parse_iso": _parse_iso,
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


def test_post_publish_callback_runs_once_for_short_success() -> None:
    calls: list[dict[str, Any]] = []
    service = _make_service(
        now_iso=lambda: "2026-05-06T19:00:01Z",
        pipeline_post_publish_callback=lambda payload: append_and_return(calls, payload, {}),
    )
    service._status.update(
        {
            "runId": "pipeline_short",
            "startedAt": "2026-05-06T19:00:00Z",
            "baselineOutputCount": 0,
            "jobsPageLoadedCount": 0,
        }
    )

    service._set_completed(status="ok", final_output_count=1)
    service._set_completed(status="ok", final_output_count=1)

    assert len(calls) == 1
    assert calls[0]["runId"] == "pipeline_short"
    assert calls[0]["status"] == "completed"


def test_post_publish_callback_skips_failed_and_canceled_runs() -> None:
    calls: list[dict[str, Any]] = []
    for run_id, status in (("pipeline_failed", "error"), ("pipeline_canceled", "canceled")):
        service = _make_service(
            pipeline_post_publish_callback=lambda payload: append_and_return(calls, payload, {}),
        )
        service._status.update(
            {
                "runId": run_id,
                "startedAt": "2026-05-06T18:59:00Z",
                "baselineOutputCount": 0,
                "jobsPageLoadedCount": 0,
            }
        )
        service._set_completed(status=status)

    assert calls == []
