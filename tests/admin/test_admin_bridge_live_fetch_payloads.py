from unittest import mock

import pytest

from src import admin_bridge

from ._admin_bridge_live_payloads_shared import (
    _setup_sparse_fetch_task_artifact_keeps_owner_active,
    _setup_stale_finished_report_with_live_fetch_owner,
    active_progress,
    assert_live_task_event_envelope,
    fetch_report,
    start_lifecycle_for_task,
    task_live_payload,
    task_state_entry,
)

pytestmark = pytest.mark.usefixtures("admin_bridge_entrypoint_root")


def test_get_task_live_payload_fetch_preserves_shared_contract() -> None:
    run_id = "fetch_live_contract_1"
    started_at = "2026-03-08T10:00:00.000Z"
    progress = active_progress(
        "execute_sources",
        "Executing sources",
        {"sourceCount": 4, "completedTasks": 2},
    )
    summary = {"outputCount": 7, "failedSources": 1, "sourceCount": 4}
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": task_state_entry("fetch", run_id=run_id, started_at=started_at),
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        fetch_report(
            run_id=run_id,
            started_at=started_at,
            summary=summary,
            task_progress=progress,
        ),
    )
    start_lifecycle_for_task(
        "fetch",
        run_id=run_id,
        started_at=started_at,
        progress=progress,
        summary=summary,
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_TASKS_PATH,
        {
            "taskType": "fetch",
            "runId": run_id,
            "startedAt": started_at,
            "status": "running",
            "summary": {"running": 1},
            "taskProgress": active_progress(
                "execute_sources",
                "Executing sources",
                {"sourceCount": 4, "runningTasks": 1},
            ),
            "workItems": [
                {
                    "id": "source_1",
                    "name": "Source 1",
                    "status": "running",
                    "progress": {
                        "phaseKey": "details",
                        "phaseLabel": "Fetching details",
                        "counts": {"emittedJobs": 3},
                        "updatedAt": started_at,
                    },
                }
            ],
            "recentEvents": [
                {
                    "timestamp": started_at,
                    "phaseKey": "details",
                    "workItemId": "source_1",
                    "message": "Fetching details",
                }
            ],
        },
    )

    payload = task_live_payload("fetch")

    assert str(payload.get("taskType") or "") == "fetch"
    assert bool(payload.get("active")) is True
    assert str(payload.get("status") or "") == "running"
    assert str(payload.get("runId") or "") == run_id
    assert str(payload.get("startedAt") or "") == started_at
    assert (payload.get("summary") or {}).get("outputCount") == 7
    assert (payload.get("summary") or {}).get("running") == 1
    progress = payload.get("taskProgress") or {}
    counts = progress.get("counts") or {}
    assert progress.get("phaseKey") == "execute_sources"
    assert counts.get("sourceCount") == 4
    assert counts.get("completedTasks") == 2
    assert counts.get("runningTasks") == 1
    work_items = payload.get("workItems") or []
    assert len(work_items) == 1
    assert "tasks" not in payload
    assert work_items[0].get("id") == "source_1"
    assert ((work_items[0].get("progress") or {}).get("counts") or {}).get("emittedJobs") == 3
    recent_events = payload.get("recentEvents") or []
    assert len(recent_events) == 1
    assert_live_task_event_envelope(
        recent_events[0],
        task_type="fetch",
        run_id=run_id,
        event_name="details",
        phase_key="details",
    )
    assert recent_events[0].get("message") == "Fetching details"


def test_get_task_live_payload_fetch_ignores_stale_task_artifact_and_uses_current_report_detail() -> (
    None
):
    run_id = "fetch_live_current_1"
    started_at = "2026-03-08T10:00:00.000Z"
    heartbeat_at = "2026-03-08T10:03:00.000Z"
    progress = active_progress(
        "execute_sources",
        "Executing sources",
        {
            "resolvedSources": 10,
            "sourceCount": 551,
            "runningTasks": 541,
            "queuedTasks": 0,
            "outputCount": 34081,
            "failedSources": 0,
            "excludedSources": 0,
            "completedTasks": 10,
        },
    )
    summary = {
        "successfulSources": 10,
        "failedSources": 0,
        "excludedSources": 0,
        "outputCount": 34081,
        "sourceCount": 551,
    }
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": task_state_entry("fetch", run_id=run_id, started_at=started_at),
        },
    )
    current_report = fetch_report(
        run_id=run_id,
        started_at=started_at,
        summary=summary,
        runtime={"selectedSourceCount": 551, "heartbeatAt": heartbeat_at},
        task_progress=progress,
    )
    current_report["sources"] = [
        {
            "name": "Studio A",
            "status": "running",
            "adapter": "static",
            "studio": "Studio A",
            "keptCount": 17,
            "durationMs": 26000,
        }
    ]
    admin_bridge.save_json_atomic(admin_bridge.JOBS_FETCH_REPORT_PATH, current_report)
    start_lifecycle_for_task(
        "fetch",
        run_id=run_id,
        started_at=started_at,
        progress=progress,
        summary=summary,
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_TASKS_PATH,
        {
            "taskType": "fetch",
            "runId": "fetch_live_stale_1",
            "startedAt": "2026-03-08T09:50:00.000Z",
            "status": "running",
            "taskProgress": active_progress(
                "execute_sources",
                "Executing sources",
                {
                    "resolvedSources": 9,
                    "sourceCount": 551,
                    "runningTasks": 542,
                    "queuedTasks": 0,
                    "outputCount": 29957,
                    "completedTasks": 9,
                },
            ),
        },
    )

    payload = task_live_payload("fetch")

    assert str(payload.get("runId") or "") == run_id
    progress = payload.get("taskProgress") or {}
    counts = progress.get("counts") or {}
    assert counts.get("resolvedSources") == 10
    assert counts.get("sourceCount") == 551
    assert counts.get("outputCount") == 34081
    work_items = payload.get("workItems") or []
    assert len(work_items) == 1
    assert work_items[0].get("name") == "Studio A"
    recent_events = payload.get("recentEvents") or []
    assert len(recent_events) == 1
    assert "10/551 sources resolved" in str(recent_events[0].get("message") or "")


def test_get_task_live_payload_fetch_supplements_current_run_detail_when_task_artifact_lacks_work_items() -> (
    None
):
    run_id = "fetch_live_partial_1"
    started_at = "2026-03-08T10:00:00.000Z"
    heartbeat_at = "2026-03-08T10:04:00.000Z"
    progress = active_progress(
        "execute_sources",
        "Executing sources",
        {
            "resolvedSources": 10,
            "sourceCount": 551,
            "runningTasks": 541,
            "queuedTasks": 0,
            "outputCount": 34081,
            "completedTasks": 10,
        },
    )
    summary = {
        "successfulSources": 10,
        "failedSources": 0,
        "excludedSources": 0,
        "outputCount": 34081,
        "sourceCount": 551,
    }
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": task_state_entry("fetch", run_id=run_id, started_at=started_at),
        },
    )
    report = fetch_report(
        run_id=run_id,
        started_at=started_at,
        summary=summary,
        runtime={"selectedSourceCount": 551, "heartbeatAt": heartbeat_at},
        task_progress=progress,
    )
    report["sources"] = [
        {
            "name": "Studio A",
            "status": "running",
            "adapter": "static",
            "studio": "Studio A",
            "keptCount": 17,
            "durationMs": 26000,
        }
    ]
    admin_bridge.save_json_atomic(admin_bridge.JOBS_FETCH_REPORT_PATH, report)
    start_lifecycle_for_task(
        "fetch",
        run_id=run_id,
        started_at=started_at,
        progress=progress,
        summary=summary,
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_TASKS_PATH,
        {
            "taskType": "fetch",
            "runId": run_id,
            "startedAt": started_at,
            "status": "running",
            "taskProgress": active_progress(
                "execute_sources",
                "Executing sources",
                {
                    "resolvedSources": 9,
                    "sourceCount": 551,
                    "runningTasks": 541,
                    "queuedTasks": 0,
                    "outputCount": 29957,
                    "completedTasks": 9,
                },
            ),
            "summary": {"running": 541},
        },
    )

    payload = task_live_payload("fetch")

    progress = payload.get("taskProgress") or {}
    counts = progress.get("counts") or {}
    assert counts.get("resolvedSources") == 10
    assert counts.get("runningTasks") == 541
    assert counts.get("sourceCount") == 551
    work_items = payload.get("workItems") or []
    assert len(work_items) == 1
    assert work_items[0].get("name") == "Studio A"
    recent_events = payload.get("recentEvents") or []
    assert len(recent_events) == 1
    assert "output 34081" in str(recent_events[0].get("message") or "")


def test_get_task_live_payload_fetch_prefers_live_owner_state_over_stale_finished_report() -> None:
    _setup_stale_finished_report_with_live_fetch_owner()

    with mock.patch.object(admin_bridge, "pid_is_running", return_value=True):
        payload = task_live_payload("fetch")

    assert bool(payload.get("active")) is True
    assert str(payload.get("status") or "") == "running"
    assert str(payload.get("runId") or "") == "fetch_live_owner_1"
    counts = (payload.get("taskProgress") or {}).get("counts") or {}
    assert counts.get("resolvedSources") == 2
    assert counts.get("runningTasks") == 1
    work_items = payload.get("workItems") or []
    assert len(work_items) == 1
    assert work_items[0].get("id") == "source_live_1"
    task_state = admin_bridge.load_json_object(admin_bridge.TASK_STATE_PATH, {})
    assert str((task_state.get("fetch") or {}).get("runId") or "") == "fetch_live_owner_1"


def test_get_task_live_payload_fetch_keeps_sparse_active_artifact_meaningful() -> None:
    _setup_sparse_fetch_task_artifact_keeps_owner_active()

    with mock.patch.object(admin_bridge, "pid_is_running", return_value=False):
        payload = task_live_payload("fetch")

    assert bool(payload.get("active")) is True
    assert str(payload.get("status") or "") == "running"
    progress = payload.get("taskProgress") or {}
    counts = progress.get("counts") or {}
    assert bool(progress.get("active")) is True
    assert str(progress.get("phaseLabel") or "") == "Executing sources"
    assert counts.get("sourceCount") == 10
    assert counts.get("outputCount") == 4
    assert counts.get("runningTasks") == 1
    work_items = payload.get("workItems") or []
    assert len(work_items) == 1
    assert work_items[0].get("name") == "Sparse Source"
    recent_events = payload.get("recentEvents") or []
    assert len(recent_events) == 1
    assert "Fetch still active" in str(recent_events[0].get("message") or "")
