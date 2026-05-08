from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.bridge.pipeline_service import PipelineRuntime, PipelineService
from src.jobs.pipeline_runtime_summary import build_fetch_task_progress_payload


class FakeLock:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None


def make_parse_iso():
    def parse_iso(value):
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (AttributeError, ValueError):
            return None

    return parse_iso


def test_fetch_task_progress_ratio_is_count_based() -> None:
    progress = build_fetch_task_progress_payload(
        phase_key="executing_sources",
        phase_label="Executing sources",
        task_rows={
            "a": {"status": "ok"},
            "b": {"status": "error"},
            "c": {"status": "excluded"},
            "d": {"status": "running"},
            "e": {"status": "queued"},
        },
        output_count=12,
    )

    assert progress["ratio"] == pytest.approx(0.6)
    assert progress["counts"]["resolvedSources"] == 3
    assert progress["counts"]["totalTasks"] == 5


def test_run_worker_skips_registry_adjudication_before_sync_by_default(
    tmp_path: Path,
) -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "starting",
        "progress": {},
        "startedAt": "2026-03-22T12:00:00Z",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    discovery_report_path = tmp_path / "discovery-report.json"
    fetch_report_path = tmp_path / "fetch-report.json"
    registry_calls: list[dict[str, Any]] = []
    sync_calls: list[tuple[str, str, bool]] = []

    def load_json_object(path: Path, default: Any) -> Any:
        if path == discovery_report_path:
            return {
                "runId": "discovery_1",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:00:01Z",
            }
        if path == fetch_report_path:
            return {
                "runId": "fetch_1",
                "startedAt": "2026-03-22T12:00:02Z",
                "finishedAt": "2026-03-22T12:00:03Z",
                "summary": {"outputCount": 12},
            }
        return default

    def start_sync_task(action: str, *, reason: str, automatic: bool) -> dict[str, Any]:
        sync_calls.append((action, reason, automatic))
        return {"started": True, "runId": "sync-123"}

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: "2026-03-22T12:00:04Z",
        parse_iso=make_parse_iso(),
        append_run_history=lambda x: x,
        upsert_run_history=lambda x, **kw: x,
        task_running_from_state=lambda x: False,
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 12,
        load_json_object=load_json_object,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=discovery_report_path,
        fetch_report_path=fetch_report_path,
        trigger_discovery_task=lambda **kw: (
            200,
            {
                "started": True,
                "runId": "discovery_1",
                "startedAt": "2026-03-22T12:00:00Z",
            },
        ),
        start_fetcher_task=lambda x: {
            "started": True,
            "runId": "fetch_1",
            "startedAt": "2026-03-22T12:00:02Z",
        },
        start_sync_task=start_sync_task,
        get_app_version=lambda: "1.0.0",
        get_projected_run_history=lambda: SimpleNamespace(child_tasks={}),
        run_registry_conflict_adjudication=lambda payload: (
            registry_calls.append(payload) or {"checkedFamilyCount": 0, "demoted": 0}
        ),
    )

    service._run_worker("pipeline_1")

    assert registry_calls == []
    assert sync_calls == [("push", "jobs_pipeline", False)]
    assert status["active"] is False
    assert status["stage"] == "completed"
