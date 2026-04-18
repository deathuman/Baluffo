from src.jobs.pipeline_runtime import snapshot_task_rows
from src.shared.live_task import (
    build_live_task_contract_fields,
    build_live_task_payload,
    build_live_task_progress_payload,
    normalize_live_task_payload,
)


def test_build_live_task_contract_fields_emits_tasks_alias_without_shared_rows() -> None:
    payload = build_live_task_payload(
        task_type="fetch",
        active=True,
        run_id="fetch_live_1",
        started_at="2026-03-08T10:00:00.000Z",
        heartbeat_at="2026-03-08T10:00:01.000Z",
        task_progress=build_live_task_progress_payload(
            active=True,
            phase_key="execute_sources",
            phase_label="Executing sources",
            counts={"sourceCount": 3, "runningTasks": 1},
        ),
        work_items=[
            {
                "id": "source_1",
                "name": "Source 1",
                "status": "running",
            }
        ],
        recent_events=[
            {
                "timestamp": "2026-03-08T10:00:02.000Z",
                "phaseKey": "execute_sources",
                "workItemId": "source_1",
                "message": "Running source 1",
            }
        ],
    )

    fields = build_live_task_contract_fields(payload)

    assert fields["heartbeatAt"] == "2026-03-08T10:00:01.000Z"
    assert fields["taskProgress"]["counts"] == {"sourceCount": 3, "runningTasks": 1}
    assert "tasks" not in fields
    assert fields["workItems"][0]["status"] == "running"
    assert fields["recentEvents"][0]["message"] == "Running source 1"


def test_normalize_live_task_payload_accepts_legacy_tasks_input() -> None:
    payload = {
        "taskType": "fetch",
        "tasks": [
            {
                "id": "legacy_source_1",
                "name": "Legacy Source 1",
                "status": "running",
            }
        ],
        "recentEvents": [
            {
                "timestamp": "2026-03-08T10:00:02.000Z",
                "message": "Legacy event",
            }
        ],
    }

    normalized = normalize_live_task_payload(payload, task_type="fetch")

    assert "tasks" not in normalized
    assert len(normalized["workItems"]) == 1
    assert normalized["workItems"][0]["id"] == "legacy_source_1"
    assert normalized["recentEvents"][0]["message"] == "Legacy event"


def test_snapshot_task_rows_returns_public_copy_of_runtime_rows() -> None:
    task_rows = {
        "source_1": {
            "id": "source_1",
            "name": "Source 1",
            "status": "running",
            "heartbeatAt": "2026-03-08T10:00:01.000Z",
        }
    }

    snapshot = snapshot_task_rows(task_rows)

    assert snapshot == [task_rows["source_1"]]
    assert snapshot[0] is not task_rows["source_1"]

    task_rows["source_1"]["status"] = "completed"
    assert snapshot[0]["status"] == "running"
