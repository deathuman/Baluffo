"""Shared ``PipelineService`` builder for bridge pipeline tests.

Callers override any dependency seam by keyword, so a test that needs a
different ``now_iso`` or ``parse_iso`` passes it as an override rather than
re-declaring the whole keyword map.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from src.bridge.pipeline_service import PipelineRuntime, PipelineService
from tests.helpers.report_state import parse_iso

__all__ = ["make_pipeline_service"]


def make_pipeline_service(**overrides: Any) -> PipelineService:
    kwargs: dict[str, Any] = {
        "pipeline_state_lock": threading.RLock(),
        "pipeline_status": {},
        "runtime": PipelineRuntime(),
        "bridge_log": lambda *args, **kwargs: None,
        "now_iso": lambda: "2026-05-06T19:00:00Z",
        "parse_iso": parse_iso,
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
