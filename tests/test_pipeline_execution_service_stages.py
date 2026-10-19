"""Tests for pipeline execution service stages."""

import time

from src.bridge import pipeline_service_stages
from src.bridge.performance_profile import (
    clear_performance_profile,
    snapshot_performance_profile,
)
from tests._pipeline_execution_shared import (
    Any,
    FakeLock,
    Path,
    PipelineRuntime,
    PipelineService,
    load_json_object_stub,
    make_parse_iso,
)


def _launch_service(
    tmp_path: Path,
    *,
    bridge_log: Any,
    status: dict[str, Any] | None = None,
    current_fetch_output_count: Any = None,
) -> PipelineService:
    return PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status
        if status is not None
        else {
            "active": False,
            "runId": "",
            "stage": "idle",
            "progress": {},
            "startedAt": "",
            "finishedAt": "",
            "error": "",
            "updatesFound": False,
            "refreshRecommended": False,
            "baselineOutputCount": 0,
            "finalOutputCount": 0,
            "jobsPageLoadedCount": 0,
        },
        runtime=PipelineRuntime(),
        bridge_log=bridge_log,
        now_iso=lambda: "2026-03-22T12:00:00Z",
        parse_iso=make_parse_iso(),
        sync_task_running=lambda: False,
        current_fetch_output_count=current_fetch_output_count or (lambda: 10),
        load_json_object=load_json_object_stub,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=tmp_path / "discovery-report.json",
        fetch_report_path=tmp_path / "fetch-report.json",
        trigger_discovery_task=lambda **kw: (200, {"started": True}),
        start_fetcher_task=lambda x: {"started": True},
        start_sync_task=lambda action, reason, automatic: {"started": True},
        get_app_version=lambda: "1.0.0",
    )


def _launch_operations() -> dict[str, dict[str, Any]]:
    return {
        row["label"]: row
        for row in snapshot_performance_profile()["operationTimings"]["operations"]
    }


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


class TestPipelineLaunchTelemetry:
    def test_start_records_launch_duration_in_journal_and_profile(self, tmp_path: Path) -> None:
        clear_performance_profile()
        entries: list[tuple[str, str, dict[str, Any]]] = []

        service = _launch_service(
            tmp_path,
            bridge_log=lambda level, event, **fields: entries.append((level, event, fields)),
        )
        result = service.start_task({"jobsPageLoadedCount": 5})

        started = [entry for entry in entries if entry[1] == "jobs_pipeline_launch_started"]
        finished = [entry for entry in entries if entry[1] == "jobs_pipeline_launch_finished"]
        assert len(started) == 1
        assert started[0][2]["jobsPageLoadedCount"] == 5
        assert len(finished) == 1
        level, event, fields = finished[0]
        assert event == "jobs_pipeline_launch_finished"
        assert level == "info"
        assert fields["started"] is True
        assert fields["completed"] is True
        assert fields["slow"] is False
        assert fields["runId"] == result["runId"]
        assert fields["stage"] == "starting"
        assert fields["durationMs"] >= 0
        assert set(fields["phaseDurationsMs"]) == {
            "lockAcquireMs",
            "admissionMs",
            "baselineCountMs",
            "statusWriteMs",
            "workerStartMs",
        }
        assert fields["slowestPhaseMs"] >= 0
        launch = _launch_operations()["jobs_pipeline_launch"]
        assert launch["count"] == 1
        assert launch["lastMs"] == fields["durationMs"]
        assert launch["errorCount"] == 0

    def test_slow_launch_names_the_expensive_phase(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setattr(pipeline_service_stages, "JOBS_PIPELINE_LAUNCH_SLOW_MS", 0)

        def slow_baseline() -> int:
            time.sleep(0.05)
            return 10

        entries: list[tuple[str, str, dict[str, Any]]] = []
        service = _launch_service(
            tmp_path,
            bridge_log=lambda level, event, **fields: entries.append((level, event, fields)),
            current_fetch_output_count=slow_baseline,
        )
        service.start_task({})

        finished = [entry for entry in entries if entry[1] == "jobs_pipeline_launch_finished"]
        assert len(finished) == 1
        level, _event, fields = finished[0]
        assert level == "warn"
        assert fields["slow"] is True
        assert fields["started"] is True
        assert fields["slowestPhase"] == "baselineCountMs"
        assert fields["slowestPhaseMs"] > 0
        assert fields["phaseDurationsMs"]["baselineCountMs"] == fields["slowestPhaseMs"]

    def test_refused_launch_is_recorded_without_an_error_sample(self, tmp_path: Path) -> None:
        clear_performance_profile()
        entries: list[tuple[str, str, dict[str, Any]]] = []

        service = _launch_service(
            tmp_path,
            bridge_log=lambda level, event, **fields: entries.append((level, event, fields)),
            status={
                "active": True,
                "runId": "existing-pipeline-123",
                "stage": "fetch",
                "progress": {},
                "startedAt": "2026-03-22T12:00:00Z",
                "finishedAt": "",
                "error": "",
            },
        )
        result = service.start_task({})

        assert result["started"] is False
        finished = [entry for entry in entries if entry[1] == "jobs_pipeline_launch_finished"]
        assert len(finished) == 1
        _level, _event, fields = finished[0]
        assert fields["started"] is False
        assert fields["runId"] == "existing-pipeline-123"
        assert "already running" in fields["error"].lower()
        assert set(fields["phaseDurationsMs"]) == {"lockAcquireMs", "admissionMs"}
        launch = _launch_operations()["jobs_pipeline_launch"]
        assert launch["count"] == 1
        assert launch["errorCount"] == 0
