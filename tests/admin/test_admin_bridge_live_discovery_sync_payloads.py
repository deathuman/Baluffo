from unittest import mock

import pytest

from src import admin_bridge

from ._admin_bridge_live_payloads_shared import (
    active_progress,
    assert_live_task_event_envelope,
    discovery_report,
    start_lifecycle_for_task,
    task_live_payload,
    task_state_entry,
)

pytestmark = pytest.mark.usefixtures("admin_bridge_entrypoint_root")


def test_get_task_live_payload_discovery_preserves_shared_contract() -> None:
    run_id = "discovery_live_contract_1"
    started_at = "2026-03-08T10:00:00.000Z"
    heartbeat_at = "2026-03-08T10:00:05.000Z"
    progress = active_progress(
        "probing_candidates",
        "Probing candidates",
        {"foundEndpoints": 4, "probedCandidates": 9, "queuedCandidates": 3},
    )
    summary = {
        "foundEndpointCount": 4,
        "probedCandidateCount": 9,
        "queuedCandidateCount": 3,
        "failedProbeCount": 1,
    }
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "discovery": task_state_entry("discovery", run_id=run_id, started_at=started_at),
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.DISCOVERY_REPORT_PATH,
        {
            **discovery_report(
                run_id=run_id,
                started_at=started_at,
                summary=summary,
                task_progress=progress,
            ),
            "runtime": {
                "lifecycle": {"heartbeatAt": heartbeat_at},
                "adapterTimings": [
                    {
                        "adapter": "games_jobs_direct",
                        "generatedCount": 4,
                        "failureCount": 1,
                        "probedCount": 9,
                        "healthyCount": 3,
                        "queuedCount": 3,
                        "durationMs": 9000,
                    }
                ],
            },
            "recentEvents": [
                {
                    "timestamp": heartbeat_at,
                    "phaseKey": "probing_candidates",
                    "workItemId": "games_jobs_direct",
                    "message": "Probing games_jobs_direct",
                }
            ],
        },
    )
    start_lifecycle_for_task(
        "discovery",
        run_id=run_id,
        started_at=started_at,
        progress=progress,
        summary=summary,
    )

    payload = task_live_payload("discovery")

    assert str(payload.get("taskType") or "") == "discovery"
    assert bool(payload.get("active")) is True
    assert str(payload.get("status") or "") == "running"
    assert str(payload.get("runId") or "") == run_id
    progress = payload.get("taskProgress") or {}
    counts = progress.get("counts") or {}
    assert progress.get("phaseKey") == "probing_candidates"
    assert counts.get("foundEndpoints") == 4
    assert counts.get("probedCandidates") == 9
    assert counts.get("queuedCandidates") == 3
    work_items = payload.get("workItems") or []
    assert len(work_items) == 1
    assert "tasks" not in payload
    assert work_items[0].get("id") == "games_jobs_direct"
    assert work_items[0].get("status") == "running"
    assert ((work_items[0].get("progress") or {}).get("counts") or {}).get("healthyCount") == 3
    recent_events = payload.get("recentEvents") or []
    assert len(recent_events) == 1
    assert_live_task_event_envelope(
        recent_events[0],
        task_type="discovery",
        run_id=run_id,
        event_name="probing_candidates",
        phase_key="probing_candidates",
    )
    assert recent_events[0].get("phaseKey") == "probing_candidates"
    assert "endpoints 4" in str(recent_events[0].get("message") or "")
    assert "queued 3" in str(recent_events[0].get("message") or "")


def test_get_task_live_payload_sync_preserves_active_live_contract() -> None:
    run_id = "sync_live_contract_1"
    started_at = "2026-03-08T10:00:00.000Z"
    admin_bridge.save_json_atomic(
        admin_bridge.SYNC_LIVE_TASK_PATH,
        {
            "taskType": "sync",
            "runId": run_id,
            "startedAt": started_at,
            "status": "running",
            "taskProgress": {
                "active": True,
                "phaseKey": "sync_pull",
                "phaseLabel": "Sync pull",
                "mode": "determinate",
                "ratio": 0.5,
                "counts": {
                    "activeCount": 12,
                    "pendingCount": 4,
                    "rejectedCount": 1,
                    "changed": True,
                },
            },
            "summary": {"action": "pull", "activeCount": 12, "pendingCount": 4, "rejectedCount": 1},
            "workItems": [
                {
                    "id": "registry_pull",
                    "name": "Registry pull",
                    "status": "running",
                    "progress": {
                        "phaseKey": "sync_pull",
                        "phaseLabel": "Sync pull",
                        "counts": {"changed": True},
                        "updatedAt": started_at,
                    },
                }
            ],
            "recentEvents": [
                {
                    "timestamp": started_at,
                    "phaseKey": "sync_pull",
                    "workItemId": "registry_pull",
                    "message": "Pulling remote registry",
                }
            ],
        },
    )

    with mock.patch.object(admin_bridge.SyncState, "get_active_sync_runs", return_value={run_id}):
        payload = task_live_payload("sync")

    assert str(payload.get("taskType") or "") == "sync"
    assert bool(payload.get("active")) is True
    assert str(payload.get("status") or "") == "running"
    assert str(payload.get("runId") or "") == run_id
    progress = payload.get("taskProgress") or {}
    counts = progress.get("counts") or {}
    assert progress.get("phaseKey") == "sync_pull"
    assert counts.get("activeCount") == 12
    assert counts.get("pendingCount") == 4
    assert counts.get("rejectedCount") == 1
    assert counts.get("changed") is True
    work_items = payload.get("workItems") or []
    assert len(work_items) == 1
    assert "tasks" not in payload
    assert work_items[0].get("id") == "registry_pull"
    recent_events = payload.get("recentEvents") or []
    assert len(recent_events) == 1
    assert_live_task_event_envelope(
        recent_events[0],
        task_type="sync",
        run_id=run_id,
        event_name="sync_pull",
        phase_key="sync_pull",
    )
    assert recent_events[0].get("message") == "Pulling remote registry"


def test_get_task_live_payload_sync_normalizes_lifecycle_backed_started_run() -> None:
    run_id = "sync_history_contract_1"
    started_at = "2026-03-08T10:00:00.000Z"
    summary = {
        "action": "push",
        "activeCount": 8,
        "pendingCount": 2,
        "rejectedCount": 1,
    }
    admin_bridge.save_json_atomic(admin_bridge.SYNC_LIVE_TASK_PATH, {})
    start_lifecycle_for_task("sync", run_id=run_id, started_at=started_at, summary=summary)

    with mock.patch.object(admin_bridge.SyncState, "get_active_sync_runs", return_value=set()):
        payload = task_live_payload("sync")

    assert str(payload.get("taskType") or "") == "sync"
    assert bool(payload.get("active")) is False
    assert str(payload.get("status") or "") == "running"
    assert str(payload.get("runId") or "") == run_id
    progress = payload.get("taskProgress") or {}
    counts = progress.get("counts") or {}
    assert progress.get("phaseKey") == "sync_push"
    assert progress.get("phaseLabel") == "Sync push"
    assert counts.get("lastAction") == "push"
    assert (payload.get("summary") or {}).get("pendingCount") == 2
