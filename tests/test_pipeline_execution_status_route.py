"""Tests for pipeline execution status route contract."""

from tests._pipeline_execution_shared import (
    Any,
    Path,
    _pipeline_status_payload,
    pytest,
)


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
    from src.bridge.routes.get_pipeline_tasks import handle_pipeline_task_routes

    class FakeHandler:
        def __init__(self):
            self.sent = []

        def send_json(self, payload, status=200):
            self.sent.append({"status": status, "payload": payload})

        def send_bytes(self, body: bytes, *, content_type: str, status: int = 200, **_headers):
            self.sent.append({"status": status, "body": body, "content_type": content_type})

    class FakeApi:
        def get_jobs_pipeline_status_payload(self):
            return payload

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
