from __future__ import annotations

import datetime as datetime_module
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from src.bridge.pipeline_service import PipelineRuntime, PipelineService


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


def install_fake_wait_clock(
    monkeypatch: pytest.MonkeyPatch, *, start_at: str
) -> tuple[dict[str, datetime], list[float]]:
    clock = {"now": datetime.fromisoformat(start_at.replace("Z", "+00:00"))}
    waits: list[float] = []

    class FakeDateTime(datetime_module.datetime):
        @classmethod
        def now(cls, tz=None):
            current = clock["now"]
            if tz is None:
                return current.replace(tzinfo=None)
            return current.astimezone(tz)

    class FakeEvent:
        def wait(self, delay: float) -> None:
            waits.append(float(delay))
            clock["now"] = clock["now"] + datetime_module.timedelta(seconds=float(delay))

    def fake_sleep(delay: float) -> None:
        waits.append(float(delay))
        clock["now"] = clock["now"] + datetime_module.timedelta(seconds=float(delay))

    monkeypatch.setattr(datetime_module, "datetime", FakeDateTime)
    monkeypatch.setattr(threading, "Event", FakeEvent)
    # Pipeline wait loops now sleep via time.sleep (pipeline_service_children._report_wait_sleep).
    monkeypatch.setattr("src.bridge.pipeline_service_children.time.sleep", fake_sleep)
    return clock, waits


def pipeline_status() -> dict[str, Any]:
    return {
        "active": True,
        "runId": "pipeline_1",
        "stage": "starting",
        "progress": {
            "currentStep": 0,
            "totalSteps": 3,
            "percent": 0,
            "label": "Starting pipeline...",
        },
        "startedAt": "2026-03-22T12:00:00Z",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }


def make_service(
    *,
    status: dict[str, Any],
    tmp_path: Path,
    load_json_object,
    start_fetcher_task,
) -> PipelineService:
    return PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: "2026-03-22T12:00:04Z",
        parse_iso=make_parse_iso(),
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 5,
        load_json_object=load_json_object,
        load_runtime_evidence=load_json_object,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=tmp_path / "discovery-report.json",
        fetch_report_path=tmp_path / "fetch-report.json",
        trigger_discovery_task=lambda **kw: (
            200,
            {"started": True, "runId": "discovery_1", "startedAt": "2026-03-22T12:00:00Z"},
        ),
        start_fetcher_task=start_fetcher_task,
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
    )


def test_wait_for_report_completion_requires_matching_child_run_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    status = pipeline_status()
    status["stage"] = "fetch"
    _clock, waits = install_fake_wait_clock(monkeypatch, start_at="2026-03-22T12:00:00Z")
    reports = [
        {
            "runId": "fetch_old",
            "startedAt": "2026-03-22T12:00:01Z",
            "finishedAt": "2026-03-22T12:00:02Z",
        },
        {
            "runId": "fetch_1",
            "startedAt": "2026-03-22T12:00:01Z",
            "finishedAt": "2026-03-22T12:00:03Z",
        },
    ]

    def load_fetch_report(_path: Path, _default: Any) -> dict[str, Any]:
        if len(reports) > 1:
            return reports.pop(0)
        return reports[0]

    service = make_service(
        status=status,
        tmp_path=tmp_path,
        load_json_object=load_fetch_report,
        start_fetcher_task=lambda x: {"started": True, "runId": "fetch_1"},
    )

    report = service.wait_for_report_completion(
        report_path=tmp_path / "fetch-report.json",
        started_at="2026-03-22T12:00:01Z",
        timeout_s=10.0,
        report_name="fetch report",
        load_json_object=service._load_json_object,
        task_type="fetch",
        task_run_id="fetch_1",
    )

    assert str(report.get("runId") or "") == "fetch_1"
    assert waits == [1.0]


def test_run_worker_proceeds_to_fetch_after_warning_discovery_report(tmp_path: Path) -> None:
    status = pipeline_status()
    fetch_calls: list[dict[str, Any]] = []
    discovery_report_path = tmp_path / "discovery-report.json"
    fetch_report_path = tmp_path / "fetch-report.json"

    def load_json_object(path: Path, default: Any) -> Any:
        if path == discovery_report_path:
            return {
                "runId": "discovery_1",
                "status": "warning",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:00:01Z",
                "summary": {"failedProbeCount": 3, "probeMissCount": 1},
            }
        if path == fetch_report_path:
            return {
                "runId": "fetch_1",
                "startedAt": "2026-03-22T12:00:02Z",
                "finishedAt": "2026-03-22T12:00:03Z",
                "summary": {"outputCount": 5},
            }
        return default

    def start_fetcher_task(payload: dict[str, Any]) -> dict[str, Any]:
        fetch_calls.append(payload)
        return {"started": True, "runId": "fetch_1", "startedAt": "2026-03-22T12:00:02Z"}

    service = make_service(
        status=status,
        tmp_path=tmp_path,
        load_json_object=load_json_object,
        start_fetcher_task=start_fetcher_task,
    )

    service._run_worker("pipeline_1")

    assert fetch_calls == [{"preset": "default"}]
    assert status["stage"] == "completed"
    assert status["error"] == ""
