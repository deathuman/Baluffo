"""Tests for pipeline execution service stages."""

from tests._pipeline_execution_shared import (
    Any,
    FakeLock,
    Path,
    PipelineRuntime,
    PipelineService,
    load_json_object_stub,
    make_parse_iso,
)


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
