"""Tests for pipeline execution metrics and timestamps."""

from tests._pipeline_execution_shared import (
    Any,
    FakeLock,
    Path,
    PipelineRuntime,
    PipelineService,
    load_json_object_stub,
    make_parse_iso,
)
from tests.helpers.mutation import append_and_return


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
        lifecycle_starts: list[dict[str, Any]] = []

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
            start_lifecycle_run=lambda **kwargs: append_and_return(
                lifecycle_starts, dict(kwargs), {}
            ),
        )

        service.start_task({})

        # Verify initial lifecycle row
        assert len(lifecycle_starts) >= 1
        initial_record = lifecycle_starts[0]

        assert initial_record["run_id"].startswith("pipeline_")
        assert initial_record["task_type"] == "pipeline"
        assert initial_record["stage"] == "starting"
        assert initial_record["owner_kind"] == "pipeline"
        assert "summary" in initial_record

        # Verify summary contains baseline info
        summary = initial_record["summary"]
        assert "baselineOutputCount" in summary
        assert "jobsPageLoadedCount" in summary
        assert "stage" in summary
        progress = initial_record["progress"]
        assert progress["phaseKey"] == "starting"
        assert progress["phaseLabel"] == "Starting pipeline..."
        assert progress["counts"]["currentStep"] == 0
        assert progress["counts"]["totalSteps"] == 3
        assert progress["counts"]["baselineOutputCount"] == 10

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
