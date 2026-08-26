"""Tests for pipeline execution contract and wiring behavior."""

from tests._pipeline_execution_shared import (
    Any,
    FakeLock,
    Path,
    PipelineRuntime,
    PipelineService,
    _pipeline_status_payload,
    make_parse_iso,
)


def test_pipeline_status_matches_frontend_contract(tmp_path: Path) -> None:
    """Test pipeline status payload matches frontend expectations."""
    from src.bridge.routes.get_pipeline_tasks import handle_pipeline_task_routes

    class FakeHandler:
        def __init__(self):
            self.sent = []

        def send_json(self, payload, status=200):
            self.sent.append({"status": status, "payload": payload})

        def send_bytes(self, body: bytes, *, content_type: str, status: int = 200, **_headers):
            self.sent.append({"status": status, "body": body, "content_type": content_type})

    class FakeApi:
        def get_jobs_pipeline_status_payload(self) -> dict[str, Any]:
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

        def get_jobs_pipeline_schedule_payload(self) -> dict[str, Any]:
            return {}

        def get_job_availability_check_status(self, run_id: str) -> dict[str, Any]:
            return {}

    handler = FakeHandler()
    api = FakeApi()

    result = handle_pipeline_task_routes(
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
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 12,
        load_json_object=load_json_object,
        load_runtime_evidence=load_json_object,
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
    admin_bridge.BRIDGE_SERVICES.reset_pipeline_service()

    service = admin_bridge._get_pipeline_service()

    assert service._load_json_object is admin_bridge.load_json_object
