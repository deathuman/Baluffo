from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from src.bridge.pipeline_service import PipelineRuntime, PipelineService
from tests.helpers.mutation import append_and_return


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


def test_pipeline_fetch_child_uses_bounded_container_profile(monkeypatch) -> None:
    payloads: list[dict[str, Any]] = []
    service = _make_pipeline_service(
        container_mode=True,
        start_fetcher_task=lambda payload: append_and_return(
            payloads, dict(payload), {"runId": "fetch_1"}
        ),
    )

    monkeypatch.delenv("BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS", raising=False)
    service._start_fetch_child()

    assert payloads[-1] == {
        "preset": "default",
        "maxWorkers": 12,
        "maxPerDomain": 3,
        "adapterHttpConcurrency": 32,
        "staticDetailConcurrency": 6,
        "browserFallbackMaxWorkers": 4,
    }


def test_pipeline_fetch_child_clamps_container_profile_env(monkeypatch) -> None:
    payloads: list[dict[str, Any]] = []
    service = _make_pipeline_service(
        container_mode=True,
        start_fetcher_task=lambda payload: append_and_return(
            payloads, dict(payload), {"runId": "fetch_1"}
        ),
    )

    monkeypatch.setenv("BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS", "99")
    monkeypatch.setenv("BALUFFO_CONTAINER_PIPELINE_BROWSER_FALLBACK_MAX_WORKERS", "99")
    service._start_fetch_child()
    monkeypatch.setenv("BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS", "bad")
    monkeypatch.setenv("BALUFFO_CONTAINER_PIPELINE_BROWSER_FALLBACK_MAX_WORKERS", "bad")
    service._start_fetch_child()

    assert payloads[0]["maxWorkers"] == 12
    assert payloads[0]["adapterHttpConcurrency"] == 32
    assert payloads[0]["browserFallbackMaxWorkers"] == 6
    assert payloads[1]["maxWorkers"] == 12
    assert payloads[1]["adapterHttpConcurrency"] == 32
    assert payloads[1]["browserFallbackMaxWorkers"] == 4


def test_pipeline_fetch_child_keeps_desktop_fetch_defaults_unmodified() -> None:
    payloads: list[dict[str, Any]] = []
    service = _make_pipeline_service(
        container_mode=False,
        start_fetcher_task=lambda payload: append_and_return(
            payloads, dict(payload), {"runId": "fetch_1"}
        ),
    )

    service._start_fetch_child()

    assert payloads[-1] == {"preset": "default"}
