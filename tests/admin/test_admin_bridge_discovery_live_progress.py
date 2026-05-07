from unittest import mock

import pytest

from src import admin_bridge
from tests.admin._runtime_helpers import (
    active_progress,
    current_task_payload,
    discovery_report,
    task_live_payload,
    task_row,
    task_state_entry,
)

pytestmark = pytest.mark.usefixtures("admin_bridge_entrypoint_root")


def test_quiet_discovery_task_state_heartbeat_keeps_owner_active() -> None:
    started_at = admin_bridge.now_iso()
    run_id = "discovery_quiet_live_1"
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "discovery": {
                **task_state_entry("discovery", run_id=run_id, started_at=started_at, pid=222),
                "heartbeatAt": started_at,
            },
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.DISCOVERY_REPORT_PATH,
        {
            **discovery_report(
                run_id=run_id,
                started_at=started_at,
                task_progress={
                    **active_progress(
                        "scanning_sources",
                        "Scanning Gameprog directory",
                        {"generatedCandidates": 24, "stageIndex": 6, "stageTotal": 11},
                    ),
                    "updatedAt": "",
                    "targetLabel": "Scanning Gameprog directory",
                },
                summary={"generatedCandidateCount": 24, "stageIndex": 6, "stageTotal": 11},
            ),
            "runtime": {"lifecycle": {"owner": "discovery_report", "heartbeatAt": ""}},
        },
    )

    with mock.patch.object(admin_bridge, "pid_is_running", return_value=False):
        payload = current_task_payload()

    discovery_row = task_row(payload, "discovery")
    assert payload.get("count") == 1
    assert discovery_row.get("active") is True
    assert str(discovery_row.get("status") or "") == "running"
    assert str(discovery_row.get("runId") or "") == run_id


def test_discovery_live_payload_uses_stage_work_items_and_events() -> None:
    run_id = "discovery_stage_live_1"
    started_at = "2026-03-08T10:00:00.000Z"
    heartbeat_at = "2026-03-08T10:00:05.000Z"
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "discovery": {
                **task_state_entry("discovery", run_id=run_id, started_at=started_at),
                "heartbeatAt": heartbeat_at,
            },
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.DISCOVERY_REPORT_PATH,
        {
            **discovery_report(
                run_id=run_id,
                started_at=started_at,
                summary={
                    "foundEndpointCount": 4,
                    "generatedCandidateCount": 12,
                    "survivedDedupeCandidateCount": 8,
                    "probedCandidateCount": 9,
                    "queuedCandidateCount": 3,
                    "stageIndex": 10,
                    "stageTotal": 11,
                    "completedStageCount": 9,
                },
                task_progress=active_progress(
                    "probing_candidates",
                    "Probing candidates",
                    {
                        "foundEndpoints": 4,
                        "generatedCandidates": 12,
                        "survivedDedupeCandidates": 8,
                        "probedCandidates": 9,
                        "queuedCandidates": 3,
                        "stageIndex": 10,
                        "stageTotal": 11,
                        "completedStages": 9,
                        "currentStageKey": "probe",
                        "currentStageLabel": "Candidate probes",
                    },
                ),
            ),
            "failures": [
                {
                    "adapter": "gameprog",
                    "stage": "website_fetch",
                    "name": "https://codemount.studio",
                    "error": "[Errno 11001] getaddrinfo failed",
                }
            ],
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
        },
    )

    payload = task_live_payload("discovery")

    progress = payload.get("taskProgress") or {}
    counts = progress.get("counts") or {}
    assert counts.get("generatedCandidates") == 12
    assert counts.get("stageIndex") == 10
    work_items = payload.get("workItems") or []
    assert work_items[0].get("id") == "probe"
    assert work_items[0].get("status") == "running"
    adapter_item = next(row for row in work_items if row.get("id") == "games_jobs_direct")
    assert adapter_item.get("status") == "ok"
    recent_events = payload.get("recentEvents") or []
    assert "generated 12" in str(recent_events[0].get("message") or "")
    assert "10/11 stages" in str(recent_events[0].get("message") or "")
    failure_event = next(
        row for row in recent_events if str(row.get("phaseKey") or "") == "website_fetch"
    )
    expected_message = (
        "Gameprog studio website fetch failed for https://codemount.studio: "
        "[Errno 11001] getaddrinfo failed"
    )
    assert str(failure_event.get("message") or "") == expected_message
    assert str(failure_event.get("target") or "") == "https://codemount.studio"
