"""Tests for pipeline execution and Admin panel task display verification.

This module tests:
1. Pipeline execution through all stages (discovery, fetch, sync_push)
2. Pipeline status payload accuracy at each stage
3. Admin panel task information display (metrics, timestamps, status)
4. Run history recording and retrieval
"""

from __future__ import annotations


# Module-level test to satisfy test discovery contract
def test_pipeline_execution_module_loads() -> None:
    """Verify pipeline execution module loads correctly."""
    from src.bridge.pipeline_service import PipelineRuntime, PipelineService

    assert PipelineRuntime is not None
    assert PipelineService is not None


import datetime as datetime_module
import threading
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.bridge.pipeline_service import PipelineRuntime, PipelineService


def load_json_object_stub(_path: Path, default: Any) -> Any:
    return {
        "startedAt": "2026-03-22T12:00:00Z",
        "finishedAt": "2026-03-22T12:00:00Z",
        "summary": {"outputCount": 0},
    }


class FakeLock:
    """Fake lock for testing."""

    def __init__(self):
        self._acquired = False

    def __enter__(self):
        self._acquired = True
        return self

    def __exit__(self, *args):
        self._acquired = False


def make_parse_iso():
    """Create a parse_iso function that returns datetime objects."""

    def parse_iso(value):
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None

    return parse_iso


def _projection_snapshot(
    *, task_type: str, run_id: str, active: bool, finished_at: str = "", explicit_dead: bool = False
):
    return SimpleNamespace(
        child_tasks={
            task_type: SimpleNamespace(
                run_id=run_id,
                active=active,
                finished_at=finished_at,
                explicit_dead=explicit_dead,
            )
        }
    )


def _pipeline_status_payload(
    *,
    active: bool,
    run_id: str,
    stage: str,
    current_step: int,
    total_steps: int,
    percent: int,
    label: str,
    started_at: str,
    finished_at: str,
    updates_found: bool,
    refresh_recommended: bool,
    baseline_output_count: int,
    final_output_count: int,
    jobs_page_loaded_count: int,
) -> dict[str, object]:
    return {
        "active": active,
        "runId": run_id,
        "stage": stage,
        "progress": {
            "currentStep": current_step,
            "totalSteps": total_steps,
            "percent": percent,
            "label": label,
        },
        "startedAt": started_at,
        "finishedAt": finished_at,
        "error": "",
        "updatesFound": updates_found,
        "refreshRecommended": refresh_recommended,
        "baselineOutputCount": baseline_output_count,
        "finalOutputCount": final_output_count,
        "jobsPageLoadedCount": jobs_page_loaded_count,
        "appVersion": "1.0.0",
    }


def _install_fake_wait_clock(
    monkeypatch: pytest.MonkeyPatch, *, start_at: str
) -> tuple[dict[str, datetime], list[float]]:
    clock = {"now": datetime.fromisoformat(start_at.replace("Z", "+00:00"))}
    waits: list[float] = []
    real_datetime = datetime_module.datetime

    class FakeDateTime(real_datetime):
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

    monkeypatch.setattr(datetime_module, "datetime", FakeDateTime)
    monkeypatch.setattr(threading, "Event", FakeEvent)
    return clock, waits


class TestPipelineServiceStages:
    """Tests for pipeline service stage progression."""

    def test_pipeline_status_payload_structure(self, tmp_path: Path) -> None:
        """Test that pipeline status payload has expected structure."""
        status: dict[str, Any] = {
            "active": False,
            "runId": "",
            "stage": "idle",
            "progress": {"currentStep": 0, "totalSteps": 3, "percent": 0, "label": "Idle"},
            "startedAt": "",
            "finishedAt": "",
            "error": "",
            "updatesFound": False,
            "refreshRecommended": False,
            "baselineOutputCount": 0,
            "finalOutputCount": 0,
            "jobsPageLoadedCount": 0,
        }

        runtime = PipelineRuntime()

        service = PipelineService(
            pipeline_state_lock=FakeLock(),
            pipeline_status=status,
            runtime=runtime,
            bridge_log=lambda *a, **kw: None,
            now_iso=lambda: "2026-03-22T12:00:00Z",
            parse_iso=make_parse_iso(),
            append_run_history=lambda x: x,
            upsert_run_history=lambda x, **kw: x,
            task_running_from_state=lambda x: False,
            sync_task_running=lambda: False,
            current_fetch_output_count=lambda: 0,
            load_json_object=load_json_object_stub,
            wait_for_sync_completion=lambda x, y: {"status": "ok"},
            discovery_report_path=tmp_path / "discovery-report.json",
            fetch_report_path=tmp_path / "fetch-report.json",
            trigger_discovery_task=lambda **kw: (200, {"started": True}),
            start_fetcher_task=lambda x: {"started": True, "startedAt": "2026-03-22T12:00:00Z"},
            start_sync_task=lambda action, reason, automatic: {
                "started": True,
                "runId": "sync-123",
            },
            get_app_version=lambda: "1.0.0",
        )

        payload = service.get_status_payload()

        # Verify core status fields
        assert "active" in payload
        assert "runId" in payload
        assert "stage" in payload
        assert "progress" in payload
        assert "startedAt" in payload
        assert "finishedAt" in payload
        assert "error" in payload
        assert "appVersion" in payload
        assert "updatesFound" in payload
        assert "refreshRecommended" in payload

        # Verify progress structure
        progress = payload["progress"]
        assert "currentStep" in progress
        assert "totalSteps" in progress
        assert "percent" in progress
        assert "label" in progress

    def test_pipeline_start_returns_correct_response(self, tmp_path: Path) -> None:
        """Test pipeline start returns correct response structure."""
        status: dict[str, Any] = {
            "active": False,
            "runId": "",
            "stage": "idle",
            "progress": {"currentStep": 0, "totalSteps": 3, "percent": 0, "label": "Idle"},
            "startedAt": "",
            "finishedAt": "",
            "error": "",
            "updatesFound": False,
            "refreshRecommended": False,
            "baselineOutputCount": 0,
            "finalOutputCount": 0,
            "jobsPageLoadedCount": 0,
        }

        runtime = PipelineRuntime()

        service = PipelineService(
            pipeline_state_lock=FakeLock(),
            pipeline_status=status,
            runtime=runtime,
            bridge_log=lambda *a, **kw: None,
            now_iso=lambda: "2026-03-22T12:00:00Z",
            parse_iso=make_parse_iso(),
            append_run_history=lambda x: x,
            upsert_run_history=lambda x, **kw: x,
            task_running_from_state=lambda x: False,
            sync_task_running=lambda: False,
            current_fetch_output_count=lambda: 10,
            load_json_object=load_json_object_stub,
            wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
            discovery_report_path=tmp_path / "discovery-report.json",
            fetch_report_path=tmp_path / "fetch-report.json",
            trigger_discovery_task=lambda **kw: (200, {"started": True}),
            start_fetcher_task=lambda x: {"started": True},
            start_sync_task=lambda action, reason, automatic: {"started": True},
            get_app_version=lambda: "1.0.0",
        )

        result = service.start_task({"jobsPageLoadedCount": 5})

        # Verify correct response structure
        assert result["started"] is True
        assert "runId" in result
        assert result["stage"] == "starting"
        assert "progress" in result
        assert result["runId"].startswith("pipeline_")

    def test_pipeline_blocked_when_already_running(self, tmp_path: Path) -> None:
        """Test pipeline cannot start when already running."""
        status: dict[str, Any] = {
            "active": True,
            "runId": "existing-pipeline-123",
            "stage": "fetch",
            "progress": {
                "currentStep": 2,
                "totalSteps": 3,
                "percent": 66,
                "label": "Running fetch...",
            },
            "startedAt": "2026-03-22T12:00:00Z",
            "finishedAt": "",
            "error": "",
            "updatesFound": False,
            "refreshRecommended": False,
            "baselineOutputCount": 10,
            "finalOutputCount": 0,
            "jobsPageLoadedCount": 5,
        }

        runtime = PipelineRuntime()

        service = PipelineService(
            pipeline_state_lock=FakeLock(),
            pipeline_status=status,
            runtime=runtime,
            bridge_log=lambda *a, **kw: None,
            now_iso=lambda: "2026-03-22T12:00:00Z",
            parse_iso=make_parse_iso(),
            append_run_history=lambda x: x,
            upsert_run_history=lambda x, **kw: x,
            task_running_from_state=lambda x: False,
            sync_task_running=lambda: False,
            current_fetch_output_count=lambda: 0,
            load_json_object=load_json_object_stub,
            wait_for_sync_completion=lambda x, y: {"status": "ok"},
            discovery_report_path=tmp_path / "discovery-report.json",
            fetch_report_path=tmp_path / "fetch-report.json",
            trigger_discovery_task=lambda **kw: (200, {"started": True}),
            start_fetcher_task=lambda x: {"started": True},
            start_sync_task=lambda action, reason, automatic: {"started": True},
            get_app_version=lambda: "1.0.0",
        )

        result = service.start_task({})

        assert result["started"] is False
        assert "already running" in result.get("error", "").lower()
        assert result["runId"] == "existing-pipeline-123"


@pytest.mark.parametrize(
    "payload, expected",
    [
        (
            _pipeline_status_payload(
                active=True,
                run_id="test-pipeline-123",
                stage="fetch",
                current_step=2,
                total_steps=3,
                percent=66,
                label="Running fetch...",
                started_at="2026-03-22T12:00:00Z",
                finished_at="",
                updates_found=False,
                refresh_recommended=False,
                baseline_output_count=10,
                final_output_count=5,
                jobs_page_loaded_count=8,
            ),
            {"active": True, "stage": "fetch", "percent": 66, "finalOutputCount": 5},
        ),
        (
            _pipeline_status_payload(
                active=False,
                run_id="test-pipeline-123",
                stage="completed",
                current_step=3,
                total_steps=3,
                percent=100,
                label="Pipeline completed",
                started_at="2026-03-22T12:00:00Z",
                finished_at="2026-03-22T12:05:00Z",
                updates_found=True,
                refresh_recommended=True,
                baseline_output_count=10,
                final_output_count=15,
                jobs_page_loaded_count=8,
            ),
            {"active": False, "stage": "completed", "percent": 100, "finalOutputCount": 15},
        ),
    ],
)
def test_status_endpoint_returns_current_state(payload, expected, tmp_path: Path) -> None:
    """Test /tasks/run-jobs-pipeline-status returns accurate state."""
    from src.bridge.routes import get_routes

    class FakeHandler:
        def __init__(self):
            self.sent = []

        def send_json(self, payload, status=200):
            self.sent.append({"status": status, "payload": payload})

    class FakeApi:
        def get_jobs_pipeline_status_payload(self):
            return payload

    handler = FakeHandler()
    api = FakeApi()

    result = get_routes.handle_get(
        handler, api=api, path="/tasks/run-jobs-pipeline-status", query={}
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200

    payload = handler.sent[-1]["payload"]
    assert payload["active"] is expected["active"]
    assert payload["runId"] == "test-pipeline-123"
    assert payload["stage"] == expected["stage"]
    assert payload["progress"]["percent"] == expected["percent"]
    assert payload["progress"]["totalSteps"] == 3
    assert payload["baselineOutputCount"] == 10
    assert payload["finalOutputCount"] == expected["finalOutputCount"]
    if payload["active"]:
        assert payload["progress"]["currentStep"] == 2
    else:
        assert payload["updatesFound"] is True
        assert payload["refreshRecommended"] is True


class TestAdminPanelTaskDisplay:
    """Tests for Admin panel task information display accuracy."""

    def test_ops_health_returns_service_info(self, tmp_path: Path) -> None:
        """Test ops health returns service info for Admin display."""
        from datetime import datetime

        from src.bridge import ops_health
        from src.bridge.ops_api import OpsHealthDeps

        history = [
            {
                "id": "pipeline-1",
                "type": "pipeline",
                "status": "ok",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:05:00Z",
                "durationMs": 300000,
                "summary": {
                    "baselineOutputCount": 10,
                    "jobsPageLoadedCount": 8,
                    "finalOutputCount": 15,
                    "updatesFound": True,
                },
            },
        ]
        deps = OpsHealthDeps(
            get_history=lambda: history,
            get_fetch_report=lambda: {"summary": {}},
            get_state=lambda: {"active": False, "pending": []},
            get_tombstones=lambda: {},
            get_sync_status_payload=lambda: {},
            now_iso=lambda: "2026-03-22T12:10:00Z",
            desktop_mode=True,
            desktop_last_activity_at="2026-03-22T12:10:00Z",
            owner_state={},
            load_alert_state_fn=lambda: {},
            save_alert_state_fn=lambda x: None,
            parse_schedule_metadata_fn=lambda: {
                "fetcher": {"intervalHours": 6},
                "discovery": {"intervalHours": 24},
            },
            parse_iso=make_parse_iso(),
            now_utc=lambda: datetime(2026, 3, 22, 12, 10, 0),
        )

        health = ops_health.compute_ops_health(deps)

        # Verify health includes service info for Admin display
        assert "service" in health
        assert health["service"] == "baluffo-bridge"
        assert "status" in health

    def test_ops_history_includes_pipeline_runs(self, tmp_path: Path) -> None:
        """Test /ops/history endpoint returns pipeline runs for Admin display."""
        from src.bridge.routes import get_routes

        history = [
            {
                "id": "pipeline-123",
                "type": "pipeline",
                "status": "ok",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:05:00Z",
                "durationMs": 300000,
                "summary": {
                    "baselineOutputCount": 10,
                    "jobsPageLoadedCount": 8,
                    "finalOutputCount": 15,
                    "updatesFound": True,
                },
            },
            {
                "id": "discovery-123",
                "type": "discovery",
                "status": "ok",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:01:00Z",
                "durationMs": 60000,
                "summary": {"candidatesFound": 25},
            },
        ]

        class FakeHandler:
            def __init__(self):
                self.sent = []

            def send_json(self, payload, status=200):
                self.sent.append({"status": status, "payload": payload})

        class FakeApi:
            def sync_history_from_reports(self):
                return history

        handler = FakeHandler()
        api = FakeApi()

        result = get_routes.handle_get(handler, api=api, path="/ops/history", query={})

        assert result is True
        payload = handler.sent[-1]["payload"]

        assert "runs" in payload
        runs = payload["runs"]

        # Verify pipeline run is included
        pipeline_runs = [r for r in runs if r.get("type") == "pipeline"]
        assert len(pipeline_runs) >= 1

        pipeline = pipeline_runs[0]
        assert pipeline["id"] == "pipeline-123"
        assert pipeline["status"] == "ok"
        assert "summary" in pipeline
        assert pipeline["summary"]["updatesFound"] is True


class TestPipelineMetricsTimestamps:
    """Tests for accurate metrics and timestamps in pipeline."""

    def test_pipeline_records_timing_metadata(self, tmp_path: Path) -> None:
        """Test pipeline records accurate timing metadata."""
        status: dict[str, Any] = {
            "active": False,
            "runId": "",
            "stage": "idle",
            "progress": {"currentStep": 0, "totalSteps": 3, "percent": 0, "label": "Idle"},
            "startedAt": "",
            "finishedAt": "",
            "error": "",
            "updatesFound": False,
            "refreshRecommended": False,
            "baselineOutputCount": 0,
            "finalOutputCount": 0,
            "jobsPageLoadedCount": 0,
        }

        runtime = PipelineRuntime()
        history_records: list[dict[str, Any]] = []

        service = PipelineService(
            pipeline_state_lock=FakeLock(),
            pipeline_status=status,
            runtime=runtime,
            bridge_log=lambda *a, **kw: None,
            now_iso=lambda: "2026-03-22T12:00:00Z",
            parse_iso=make_parse_iso(),
            append_run_history=lambda x: history_records.append(x),
            upsert_run_history=lambda x, **kw: x,
            task_running_from_state=lambda x: False,
            sync_task_running=lambda: False,
            current_fetch_output_count=lambda: 10,
            load_json_object=load_json_object_stub,
            wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
            discovery_report_path=tmp_path / "discovery-report.json",
            fetch_report_path=tmp_path / "fetch-report.json",
            trigger_discovery_task=lambda **kw: (
                200,
                {"started": True, "startedAt": "2026-03-22T12:00:00Z"},
            ),
            start_fetcher_task=lambda x: {"started": True, "startedAt": "2026-03-22T12:00:00Z"},
            start_sync_task=lambda action, reason, automatic: {
                "started": True,
                "runId": "sync-123",
            },
            get_app_version=lambda: "1.0.0",
        )

        service.start_task({})

        # Verify initial history record
        assert len(history_records) >= 1
        initial_record = history_records[0]

        assert "id" in initial_record
        assert initial_record["type"] == "pipeline"
        assert initial_record["status"] == "started"
        assert "startedAt" in initial_record
        assert "summary" in initial_record

        # Verify summary contains baseline info
        summary = initial_record["summary"]
        assert "baselineOutputCount" in summary
        assert "jobsPageLoadedCount" in summary
        assert "stage" in summary

    def test_pipeline_status_shows_baseline_count(self, tmp_path: Path) -> None:
        """Test pipeline status includes baseline output count."""
        status: dict[str, Any] = {
            "active": False,
            "runId": "",
            "stage": "idle",
            "progress": {"currentStep": 0, "totalSteps": 3, "percent": 0, "label": "Idle"},
            "startedAt": "",
            "finishedAt": "",
            "error": "",
            "updatesFound": False,
            "refreshRecommended": False,
            "baselineOutputCount": 0,
            "finalOutputCount": 0,
            "jobsPageLoadedCount": 0,
        }

        runtime = PipelineRuntime()

        service = PipelineService(
            pipeline_state_lock=FakeLock(),
            pipeline_status=status,
            runtime=runtime,
            bridge_log=lambda *a, **kw: None,
            now_iso=lambda: "2026-03-22T12:00:00Z",
            parse_iso=make_parse_iso(),
            append_run_history=lambda x: x,
            upsert_run_history=lambda x, **kw: x,
            task_running_from_state=lambda x: False,
            sync_task_running=lambda: False,
            current_fetch_output_count=lambda: 25,  # Current output count
            load_json_object=load_json_object_stub,
            wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
            discovery_report_path=tmp_path / "discovery-report.json",
            fetch_report_path=tmp_path / "fetch-report.json",
            trigger_discovery_task=lambda **kw: (200, {"started": True}),
            start_fetcher_task=lambda x: {"started": True},
            start_sync_task=lambda action, reason, automatic: {"started": True},
            get_app_version=lambda: "1.0.0",
        )

        # Start pipeline with jobs page count
        result = service.start_task({"jobsPageLoadedCount": 15})

        # Verify the status includes baseline info
        assert status["baselineOutputCount"] == 25
        assert status["jobsPageLoadedCount"] == 15

    def test_pipeline_completion_shows_updates_found(self, tmp_path: Path) -> None:
        """Test pipeline completion correctly identifies updates found."""
        status: dict[str, Any] = {
            "active": False,
            "runId": "",
            "stage": "idle",
            "progress": {"currentStep": 0, "totalSteps": 3, "percent": 0, "label": "Idle"},
            "startedAt": "",
            "finishedAt": "",
            "error": "",
            "updatesFound": False,
            "refreshRecommended": False,
            "baselineOutputCount": 0,
            "finalOutputCount": 0,
            "jobsPageLoadedCount": 0,
        }

        runtime = PipelineRuntime()

        service = PipelineService(
            pipeline_state_lock=FakeLock(),
            pipeline_status=status,
            runtime=runtime,
            bridge_log=lambda *a, **kw: None,
            now_iso=lambda: "2026-03-22T12:00:00Z",
            parse_iso=make_parse_iso(),
            append_run_history=lambda x: x,
            upsert_run_history=lambda x, **kw: x,
            task_running_from_state=lambda x: False,
            sync_task_running=lambda: False,
            current_fetch_output_count=lambda: 30,  # More than baseline of 20
            load_json_object=load_json_object_stub,
            wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
            discovery_report_path=tmp_path / "discovery-report.json",
            fetch_report_path=tmp_path / "fetch-report.json",
            trigger_discovery_task=lambda **kw: (200, {"started": True}),
            start_fetcher_task=lambda x: {"started": True},
            start_sync_task=lambda action, reason, automatic: {"started": True},
            get_app_version=lambda: "1.0.0",
        )

        # Start with baseline of 20, after run output will be 30
        status["baselineOutputCount"] = 20
        status["jobsPageLoadedCount"] = 18

        # Simulate completion by calling _set_completed
        service._set_completed(status="ok", final_output_count=30)

        # Verify updates found is set correctly
        # updatesFound = final > max(baseline, loaded)
        # 30 > max(20, 18) = 20, so True
        assert status["updatesFound"] is True
        assert status["refreshRecommended"] is True
        assert status["finalOutputCount"] == 30


def test_pipeline_status_matches_frontend_contract(tmp_path: Path) -> None:
    """Test pipeline status payload matches frontend expectations."""
    from src.bridge.routes import get_routes

    class FakeHandler:
        def __init__(self):
            self.sent = []

        def send_json(self, payload, status=200):
            self.sent.append({"status": status, "payload": payload})

    class FakeApi:
        def get_jobs_pipeline_status_payload(self):
            return _pipeline_status_payload(
                active=False,
                run_id="pipeline-abc123",
                stage="completed",
                current_step=3,
                total_steps=3,
                percent=100,
                label="Pipeline completed",
                started_at="2026-03-22T12:00:00Z",
                finished_at="2026-03-22T12:05:00Z",
                updates_found=True,
                refresh_recommended=True,
                baseline_output_count=100,
                final_output_count=150,
                jobs_page_loaded_count=95,
            )

    handler = FakeHandler()
    api = FakeApi()

    result = get_routes.handle_get(
        handler, api=api, path="/tasks/run-jobs-pipeline-status", query={}
    )

    assert result is True
    payload = handler.sent[-1]["payload"]

    assert payload["active"] is False
    assert payload["runId"] == "pipeline-abc123"
    assert payload["stage"] == "completed"
    assert payload["progress"]["percent"] == 100
    assert payload["progress"]["currentStep"] == 3
    assert payload["progress"]["totalSteps"] == 3
    assert "appVersion" in payload
    assert payload["baselineOutputCount"] == 100
    assert payload["finalOutputCount"] == 150
    assert payload["updatesFound"] is True
    assert payload["refreshRecommended"] is True


def test_pipeline_worker_completes_without_error_and_uses_report_loader(tmp_path: Path) -> None:
    status: dict[str, Any] = {
        "active": False,
        "runId": "",
        "stage": "idle",
        "progress": {"currentStep": 0, "totalSteps": 3, "percent": 0, "label": "Idle"},
        "startedAt": "",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    runtime = PipelineRuntime()
    discovery_report_path = tmp_path / "discovery-report.json"
    fetch_report_path = tmp_path / "fetch-report.json"
    load_calls: list[Path] = []

    def load_json_object(path: Path, default: Any) -> Any:
        load_calls.append(Path(path))
        if Path(path) == discovery_report_path:
            return {
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:00:00Z",
            }
        if Path(path) == fetch_report_path:
            return {
                "startedAt": "2026-03-22T12:00:01Z",
                "finishedAt": "2026-03-22T12:00:01Z",
                "summary": {"outputCount": 12},
            }
        return default

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=runtime,
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: "2026-03-22T12:00:00Z",
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
            {"started": True, "startedAt": "2026-03-22T12:00:00Z"},
        ),
        start_fetcher_task=lambda x: {"started": True, "startedAt": "2026-03-22T12:00:01Z"},
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
    )

    result = service.start_task({"jobsPageLoadedCount": 5})
    assert result["started"] is True
    assert runtime.active_thread is not None
    runtime.active_thread.join(timeout=2.0)
    assert runtime.active_thread.is_alive() is False
    assert status["stage"] != "error"
    assert status["error"] == ""
    assert discovery_report_path in load_calls
    assert fetch_report_path in load_calls


def test_admin_bridge_pipeline_service_wires_load_json_object(monkeypatch, tmp_path: Path) -> None:
    from src import admin_bridge

    monkeypatch.setattr(admin_bridge, "DISCOVERY_REPORT_PATH", tmp_path / "discovery-report.json")
    monkeypatch.setattr(admin_bridge, "JOBS_FETCH_REPORT_PATH", tmp_path / "fetch-report.json")
    monkeypatch.setattr(admin_bridge, "_PIPELINE_SERVICE", None)

    service = admin_bridge._get_pipeline_service()

    assert service._load_json_object is admin_bridge.load_json_object


def test_wait_for_report_completion_returns_terminal_report_even_while_projected_child_is_active(
    monkeypatch, tmp_path: Path
) -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "fetch",
        "progress": {"currentStep": 2, "totalSteps": 3, "percent": 67, "label": "Running fetch..."},
        "startedAt": "2026-03-22T12:00:00Z",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    waits: list[float] = []

    class FakeEvent:
        def wait(self, delay: float) -> None:
            waits.append(float(delay))

    monkeypatch.setattr(threading, "Event", FakeEvent)

    projection_states = [True, False]

    def get_projected_run_history():
        active = projection_states.pop(0) if projection_states else False
        return _projection_snapshot(task_type="fetch", run_id="fetch_1", active=active)

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: "2026-03-22T12:00:00Z",
        parse_iso=make_parse_iso(),
        append_run_history=lambda x: x,
        upsert_run_history=lambda x, **kw: x,
        task_running_from_state=lambda x: False,
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 0,
        load_json_object=lambda _path, _default: {
            "runId": "fetch_1",
            "startedAt": "2026-03-22T12:00:01Z",
            "finishedAt": "2026-03-22T12:00:02Z",
            "summary": {"outputCount": 12},
        },
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=tmp_path / "discovery-report.json",
        fetch_report_path=tmp_path / "fetch-report.json",
        trigger_discovery_task=lambda **kw: (200, {"started": True}),
        start_fetcher_task=lambda x: {"started": True, "runId": "fetch_1"},
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        get_projected_run_history=get_projected_run_history,
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

    assert str(report.get("finishedAt") or "") == "2026-03-22T12:00:02Z"
    assert waits == []


def test_wait_for_report_completion_survives_timeout_while_projected_child_stays_active(
    monkeypatch, tmp_path: Path
) -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "fetch",
        "progress": {"currentStep": 2, "totalSteps": 3, "percent": 67, "label": "Running fetch..."},
        "startedAt": "2026-03-22T12:00:00Z",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    clock, waits = _install_fake_wait_clock(monkeypatch, start_at="2026-03-22T12:00:00Z")

    projection_states = [True] * 11 + [False]
    fetch_reports = [
        {"runId": "fetch_1", "startedAt": "2026-03-22T12:00:01Z", "finishedAt": ""}
    ] * 11 + [
        {
            "runId": "fetch_1",
            "startedAt": "2026-03-22T12:00:01Z",
            "finishedAt": "2026-03-22T12:00:15Z",
        }
    ]

    def get_projected_run_history():
        active = projection_states.pop(0) if projection_states else False
        return _projection_snapshot(task_type="fetch", run_id="fetch_1", active=active)

    def load_fetch_report(_path: Path, _default: Any) -> dict[str, Any]:
        if len(fetch_reports) > 1:
            return fetch_reports.pop(0)
        return fetch_reports[0]

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: clock["now"].isoformat().replace("+00:00", "Z"),
        parse_iso=make_parse_iso(),
        append_run_history=lambda x: x,
        upsert_run_history=lambda x, **kw: x,
        task_running_from_state=lambda x: False,
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 0,
        load_json_object=load_fetch_report,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=tmp_path / "discovery-report.json",
        fetch_report_path=tmp_path / "fetch-report.json",
        trigger_discovery_task=lambda **kw: (200, {"started": True}),
        start_fetcher_task=lambda x: {"started": True, "runId": "fetch_1"},
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        get_projected_run_history=get_projected_run_history,
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

    assert str(report.get("finishedAt") or "") == "2026-03-22T12:00:15Z"
    assert len(waits) == 11
    assert clock["now"] == datetime.fromisoformat("2026-03-22T12:00:11+00:00")


def test_wait_for_report_completion_survives_timeout_while_child_liveness_callback_stays_true(
    monkeypatch, tmp_path: Path
) -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "fetch",
        "progress": {"currentStep": 2, "totalSteps": 3, "percent": 67, "label": "Running fetch..."},
        "startedAt": "2026-03-22T12:00:00Z",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    clock, waits = _install_fake_wait_clock(monkeypatch, start_at="2026-03-22T12:00:00Z")

    child_live_states = [True] * 11 + [False]
    fetch_reports = [
        {"runId": "fetch_1", "startedAt": "2026-03-22T12:00:01Z", "finishedAt": ""}
    ] * 11 + [
        {
            "runId": "fetch_1",
            "startedAt": "2026-03-22T12:00:01Z",
            "finishedAt": "2026-03-22T12:00:15Z",
        }
    ]

    def load_fetch_report(_path: Path, _default: Any) -> dict[str, Any]:
        if len(fetch_reports) > 1:
            return fetch_reports.pop(0)
        return fetch_reports[0]

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: clock["now"].isoformat().replace("+00:00", "Z"),
        parse_iso=make_parse_iso(),
        append_run_history=lambda x: x,
        upsert_run_history=lambda x, **kw: x,
        task_running_from_state=lambda x: False,
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 0,
        load_json_object=load_fetch_report,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=tmp_path / "discovery-report.json",
        fetch_report_path=tmp_path / "fetch-report.json",
        trigger_discovery_task=lambda **kw: (200, {"started": True}),
        start_fetcher_task=lambda x: {"started": True, "runId": "fetch_1"},
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        child_run_is_live=lambda task_type, run_id: (
            str(task_type) == "fetch"
            and str(run_id) == "fetch_1"
            and bool(child_live_states.pop(0) if child_live_states else False)
        ),
        get_projected_run_history=lambda: _projection_snapshot(
            task_type="fetch", run_id="fetch_1", active=False
        ),
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

    assert str(report.get("finishedAt") or "") == "2026-03-22T12:00:15Z"
    assert len(waits) == 11
    assert clock["now"] == datetime.fromisoformat("2026-03-22T12:00:11+00:00")


def test_run_worker_completes_after_long_active_fetch_wait(monkeypatch, tmp_path: Path) -> None:
    status: dict[str, Any] = {
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
    upserts: list[dict[str, Any]] = []
    bridge_events: list[tuple[str, dict[str, Any]]] = []
    clock, waits = _install_fake_wait_clock(monkeypatch, start_at="2026-03-22T12:00:00Z")
    discovery_report_path = tmp_path / "discovery-report.json"
    fetch_report_path = tmp_path / "fetch-report.json"
    fetch_projection_states = [True] * 1201 + [False]
    fetch_reports = [
        {"runId": "fetch_1", "startedAt": "2026-03-22T12:00:01Z", "finishedAt": ""}
    ] * 1201 + [
        {
            "runId": "fetch_1",
            "startedAt": "2026-03-22T12:00:01Z",
            "finishedAt": "2026-03-22T12:20:05Z",
        }
    ]

    def load_json_object(path: Path, default: Any) -> Any:
        if path == discovery_report_path:
            return {
                "runId": "discovery_1",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:00:01Z",
            }
        if path == fetch_report_path:
            if len(fetch_reports) > 1:
                return fetch_reports.pop(0)
            return fetch_reports[0]
        return default

    def get_projected_run_history():
        stage = str(status.get("stage") or "").strip().lower()
        if stage == "discovery":
            return SimpleNamespace(
                child_tasks={
                    "discovery": SimpleNamespace(
                        run_id="discovery_1",
                        active=False,
                        finished_at="2026-03-22T12:00:01Z",
                        explicit_dead=False,
                    )
                }
            )
        if stage == "fetch":
            active = fetch_projection_states.pop(0) if fetch_projection_states else False
            return _projection_snapshot(task_type="fetch", run_id="fetch_1", active=active)
        return SimpleNamespace(child_tasks={})

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: clock["now"].isoformat().replace("+00:00", "Z"),
        parse_iso=make_parse_iso(),
        append_run_history=lambda x: x,
        upsert_run_history=lambda x, **kw: upserts.append(x),
        task_running_from_state=lambda x: False,
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 12,
        load_json_object=load_json_object,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=discovery_report_path,
        fetch_report_path=fetch_report_path,
        trigger_discovery_task=lambda **kw: (
            200,
            {"started": True, "runId": "discovery_1", "startedAt": "2026-03-22T12:00:00Z"},
        ),
        start_fetcher_task=lambda x: {
            "started": True,
            "runId": "fetch_1",
            "startedAt": "2026-03-22T12:00:01Z",
        },
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        get_projected_run_history=get_projected_run_history,
    )

    service._run_worker("pipeline_1")

    assert status["active"] is False
    assert status["stage"] == "completed"
    assert status["error"] == ""
    assert len(waits) == 1201
    assert upserts[-1]["status"] == "ok"


def test_run_worker_attaches_to_existing_child_tasks_on_conflict(tmp_path: Path) -> None:
    status: dict[str, Any] = {
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
    bridge_events: list[tuple[str, dict[str, Any]]] = []
    discovery_report_path = tmp_path / "discovery-report.json"
    fetch_report_path = tmp_path / "fetch-report.json"

    def bridge_log(level: str, message: str, **fields: Any) -> None:
        bridge_events.append((message, {"level": level, **fields}))

    def load_json_object(path: Path, default: Any) -> Any:
        if path == discovery_report_path:
            return {
                "runId": "discovery_live_1",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:00:05Z",
            }
        if path == fetch_report_path:
            return {
                "runId": "fetch_live_1",
                "startedAt": "2026-03-22T12:00:06Z",
                "finishedAt": "2026-03-22T12:00:10Z",
            }
        return default

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=bridge_log,
        now_iso=lambda: "2026-03-22T12:00:00Z",
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
            409,
            {
                "started": False,
                "alreadyRunning": True,
                "runId": "discovery_live_1",
                "startedAt": "2026-03-22T12:00:00Z",
                "task": "source_discovery",
                "taskType": "discovery",
                "pid": 111,
                "status": "running",
            },
        ),
        start_fetcher_task=lambda x: {
            "started": False,
            "alreadyRunning": True,
            "runId": "fetch_live_1",
            "startedAt": "2026-03-22T12:00:06Z",
            "task": "jobs_fetcher",
            "taskType": "fetch",
            "pid": 222,
            "status": "running",
        },
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        get_projected_run_history=lambda: SimpleNamespace(child_tasks={}),
    )

    service._run_worker("pipeline_1")

    assert status["active"] is False
    assert status["stage"] == "completed"
    assert status["error"] == ""
    assert (
        "jobs_pipeline_attached_existing_child_task",
        {
            "level": "info",
            "runId": "pipeline_1",
            "childTask": "discovery",
            "childRunId": "discovery_live_1",
        },
    ) in bridge_events


def test_run_worker_keeps_waiting_for_attached_fetch_child_while_live_evidence_remains(
    monkeypatch, tmp_path: Path
) -> None:
    status: dict[str, Any] = {
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
    upserts: list[dict[str, Any]] = []
    bridge_events: list[tuple[str, dict[str, Any]]] = []
    clock, waits = _install_fake_wait_clock(monkeypatch, start_at="2026-03-22T12:00:00Z")
    discovery_report_path = tmp_path / "discovery-report.json"
    fetch_report_path = tmp_path / "fetch-report.json"
    fetch_reports = [
        {"runId": "fetch_live_1", "startedAt": "2026-03-22T12:00:01Z", "finishedAt": ""}
    ] * 1201 + [
        {
            "runId": "fetch_live_1",
            "startedAt": "2026-03-22T12:00:01Z",
            "finishedAt": "2026-03-22T12:20:05Z",
        }
    ]
    child_live_states = [True] * 1201 + [False]

    def load_json_object(path: Path, default: Any) -> Any:
        if path == discovery_report_path:
            return {
                "runId": "discovery_1",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:00:01Z",
            }
        if path == fetch_report_path:
            if len(fetch_reports) > 1:
                return fetch_reports.pop(0)
            return fetch_reports[0]
        return default

    def bridge_log(level: str, message: str, **fields: Any) -> None:
        bridge_events.append((message, {"level": level, **fields}))

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=bridge_log,
        now_iso=lambda: clock["now"].isoformat().replace("+00:00", "Z"),
        parse_iso=make_parse_iso(),
        append_run_history=lambda x: x,
        upsert_run_history=lambda x, **kw: upserts.append(x),
        task_running_from_state=lambda x: False,
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 12,
        load_json_object=load_json_object,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=discovery_report_path,
        fetch_report_path=fetch_report_path,
        trigger_discovery_task=lambda **kw: (
            200,
            {"started": True, "runId": "discovery_1", "startedAt": "2026-03-22T12:00:00Z"},
        ),
        start_fetcher_task=lambda x: {
            "started": False,
            "alreadyRunning": True,
            "runId": "fetch_live_1",
            "startedAt": "2026-03-22T12:00:01Z",
            "task": "jobs_fetcher",
            "taskType": "fetch",
            "pid": 222,
            "status": "running",
        },
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        child_run_is_live=lambda task_type, run_id: (
            str(task_type) == "fetch"
            and str(run_id) == "fetch_live_1"
            and bool(child_live_states.pop(0) if child_live_states else False)
        ),
        get_projected_run_history=lambda: SimpleNamespace(child_tasks={}),
    )

    service._run_worker("pipeline_1")

    assert status["active"] is False
    assert status["stage"] == "completed"
    assert status["error"] == ""
    assert len(waits) == 1201
    assert upserts[-1]["status"] == "ok"
    assert (
        "jobs_pipeline_attached_existing_child_task",
        {
            "level": "info",
            "runId": "pipeline_1",
            "childTask": "fetch",
            "childRunId": "fetch_live_1",
        },
    ) in bridge_events


def test_run_worker_errors_when_fetch_owner_goes_inactive_without_terminal_report(
    monkeypatch, tmp_path: Path
) -> None:
    status: dict[str, Any] = {
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
    upserts: list[dict[str, Any]] = []
    clock, waits = _install_fake_wait_clock(monkeypatch, start_at="2026-03-22T12:00:00Z")
    discovery_report_path = tmp_path / "discovery-report.json"
    fetch_report_path = tmp_path / "fetch-report.json"

    def load_json_object(path: Path, default: Any) -> Any:
        if path == discovery_report_path:
            return {
                "runId": "discovery_1",
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "2026-03-22T12:00:01Z",
            }
        if path == fetch_report_path:
            return {"runId": "fetch_1", "startedAt": "2026-03-22T12:00:01Z", "finishedAt": ""}
        return default

    def get_projected_run_history():
        stage = str(status.get("stage") or "").strip().lower()
        if stage == "discovery":
            return SimpleNamespace(
                child_tasks={
                    "discovery": SimpleNamespace(
                        run_id="discovery_1",
                        active=False,
                        finished_at="2026-03-22T12:00:01Z",
                        explicit_dead=False,
                    )
                }
            )
        if stage == "fetch":
            return _projection_snapshot(task_type="fetch", run_id="fetch_1", active=False)
        return SimpleNamespace(child_tasks={})

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: clock["now"].isoformat().replace("+00:00", "Z"),
        parse_iso=make_parse_iso(),
        append_run_history=lambda x: x,
        upsert_run_history=lambda x, **kw: upserts.append(x),
        task_running_from_state=lambda x: False,
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 0,
        load_json_object=load_json_object,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=discovery_report_path,
        fetch_report_path=fetch_report_path,
        trigger_discovery_task=lambda **kw: (
            200,
            {"started": True, "runId": "discovery_1", "startedAt": "2026-03-22T12:00:00Z"},
        ),
        start_fetcher_task=lambda x: {
            "started": True,
            "runId": "fetch_1",
            "startedAt": "2026-03-22T12:00:01Z",
        },
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        get_projected_run_history=get_projected_run_history,
    )

    service._run_worker("pipeline_1")

    assert status["active"] is False
    assert status["stage"] == "error"
    assert "did not finish within timeout" in status["error"]
    assert len(waits) == 1200
    assert upserts[-1]["status"] == "error"


def test_pipeline_start_allows_live_fetch_to_attach_later(tmp_path: Path) -> None:
    status: dict[str, Any] = {
        "active": False,
        "runId": "",
        "stage": "idle",
        "progress": {"currentStep": 0, "totalSteps": 3, "percent": 0, "label": "Idle"},
        "startedAt": "",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }

    runtime = PipelineRuntime()

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=runtime,
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: "2026-03-22T12:00:00Z",
        parse_iso=make_parse_iso(),
        append_run_history=lambda x: x,
        upsert_run_history=lambda x, **kw: x,
        task_running_from_state=lambda x: False,
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 0,
        load_json_object=load_json_object_stub,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=tmp_path / "discovery-report.json",
        fetch_report_path=tmp_path / "fetch-report.json",
        trigger_discovery_task=lambda **kw: (200, {"started": True}),
        start_fetcher_task=lambda x: {"started": True},
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
        get_projected_run_history=lambda: _projection_snapshot(
            task_type="fetch", run_id="fetch_live_1", active=True
        ),
    )

    result = service.start_task({"jobsPageLoadedCount": 5})

    assert result["started"] is True
    assert str(result.get("stage") or "") == "starting"
    assert runtime.active_thread is not None
    runtime.active_thread.join(timeout=2.0)
