from src.jobs.pipeline_runtime_summary import snapshot_task_rows
from src.shared.live_task import (
    append_live_task_event,
    build_live_task_contract_fields,
    build_live_task_payload,
    build_live_task_progress_payload,
    normalize_live_task_event,
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
    assert fields["recentEvents"][0]["schemaVersion"] == 1
    assert fields["recentEvents"][0]["event"] == "execute_sources"
    assert fields["recentEvents"][0]["message"] == "Running source 1"


def test_build_live_task_progress_payload_preserves_wait_reason() -> None:
    payload = build_live_task_progress_payload(
        active=True,
        phase_key="fetching",
        phase_label="Fetching",
        counts={"runningTasks": 1},
        wait_reason="domain_gate",
    )

    assert payload["waitReason"] == "domain_gate"


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
    assert normalized["recentEvents"][0]["event"] == "live_task_event"


def test_normalize_live_task_event_emits_versioned_event_envelope() -> None:
    event = normalize_live_task_event(
        {
            "timestamp": "2026-03-08T10:00:02.000Z",
            "level": "invalid",
            "event": "source_started",
            "phaseKey": "execute_sources",
            "workItemId": "source_1",
            "message": "Running source 1",
        },
        default_task_type="fetch",
        default_run_id="fetch_live_1",
    )

    assert event == {
        "schemaVersion": 1,
        "timestamp": "2026-03-08T10:00:02.000Z",
        "level": "info",
        "event": "source_started",
        "taskType": "fetch",
        "runId": "fetch_live_1",
        "workItemId": "source_1",
        "phaseKey": "execute_sources",
        "message": "Running source 1",
        "target": "",
        "targetUrl": "",
    }


def test_normalize_live_task_event_derives_stable_event_names() -> None:
    phase_event = normalize_live_task_event(
        {"phaseKey": "probing_candidates", "message": "Probing candidates"},
        default_task_type="discovery",
    )
    default_event = normalize_live_task_event(
        {"message": "Sync progress"},
        default_task_type="sync",
    )

    assert phase_event["event"] == "probing_candidates"
    assert phase_event["schemaVersion"] == 1
    assert default_event["event"] == "live_task_event"
    assert default_event["schemaVersion"] == 1


def test_append_live_task_event_uses_shared_event_envelope() -> None:
    events = append_live_task_event(
        [],
        {
            "timestamp": "2026-03-08T10:00:02.000Z",
            "phaseKey": "sync_pull",
            "message": "Pulling remote registry",
        },
    )

    assert events == [
        {
            "schemaVersion": 1,
            "timestamp": "2026-03-08T10:00:02.000Z",
            "level": "info",
            "event": "sync_pull",
            "taskType": "",
            "runId": "",
            "workItemId": "",
            "phaseKey": "sync_pull",
            "message": "Pulling remote registry",
            "target": "",
            "targetUrl": "",
        }
    ]


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
