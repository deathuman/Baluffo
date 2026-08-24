from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from src.bridge.pipeline_service import PipelineRuntime, PipelineService


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


def test_pipeline_discovery_stage_fails_on_terminal_error_report() -> None:
    terminal_report = {
        "runId": "discovery_failed_1",
        "startedAt": "2026-05-06T18:00:00Z",
        "finishedAt": "2026-05-06T18:01:00Z",
        "status": "error",
        "summary": {"error": "owner_inactive_without_terminal_report"},
    }
    service = _make_pipeline_service(
        pipeline_status={"active": True, "runId": "pipeline_1"},
        trigger_discovery_task=lambda **_kwargs: (
            200,
            {
                "started": True,
                "runId": "discovery_failed_1",
                "startedAt": "2026-05-06T18:00:00Z",
            },
        ),
        load_runtime_evidence=lambda _path, _default=None: dict(terminal_report),
    )

    with pytest.raises(RuntimeError, match="owner_inactive_without_terminal_report"):
        service._run_discovery_stage("pipeline_1")  # noqa: SLF001
