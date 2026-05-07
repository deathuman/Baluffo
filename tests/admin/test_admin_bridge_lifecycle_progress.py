from unittest import mock

import pytest

from src import admin_bridge
from tests.admin._runtime_helpers import (
    active_progress,
    current_task_payload,
    discovery_report,
    task_row,
    task_state_entry,
)

pytestmark = pytest.mark.usefixtures("admin_bridge_entrypoint_root")


def test_task_state_lifecycle_preserves_richer_discovery_progress() -> None:
    started_at = admin_bridge.now_iso()
    report_heartbeat_at = "2026-05-06T19:05:00Z"
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "discovery": task_state_entry(
                "discovery",
                run_id="discovery_progress_1",
                started_at=started_at,
                pid=222,
            ),
        },
    )
    report = discovery_report(
        run_id="discovery_progress_1",
        started_at=started_at,
        task_progress={
            **active_progress(
                "scanning_sources",
                "Scanning known careers pages",
                {"queuedCandidates": 3, "stageIndex": 7, "stageTotal": 11},
            ),
            "mode": "indeterminate",
            "ratio": 0,
        },
        summary={"queuedCandidateCount": 3},
    )
    report["runtime"] = {
        "lifecycle": {"owner": "discovery_report", "heartbeatAt": report_heartbeat_at}
    }
    admin_bridge.save_json_atomic(admin_bridge.DISCOVERY_REPORT_PATH, report)
    admin_bridge.start_lifecycle_run(
        run_id="discovery_progress_1",
        task_type="discovery",
        started_at=started_at,
        stage="starting",
        owner_kind="process",
        owner_pid=222,
        progress={
            "active": True,
            "phaseKey": "starting",
            "phaseLabel": "Spawning discovery worker",
            "mode": "indeterminate",
            "ratio": 0,
            "counts": {},
            "updatedAt": started_at,
        },
        summary={},
    )
    admin_bridge.heartbeat_lifecycle_run(
        "discovery_progress_1",
        "discovery",
        heartbeat_at=report_heartbeat_at,
        stage="running",
        progress={
            **active_progress(
                "scanning_sources",
                "Scanning known careers pages",
                {"queuedCandidates": 3, "stageIndex": 7, "stageTotal": 11},
            ),
            "mode": "indeterminate",
            "ratio": 0,
        },
        summary={"queuedCandidateCount": 3},
    )

    with mock.patch.object(admin_bridge, "pid_is_running", return_value=True):
        payload = current_task_payload()

    discovery_row = task_row(payload, "discovery")
    progress = discovery_row.get("taskProgress") or {}
    assert progress.get("phaseLabel") == "Scanning known careers pages"
    assert (progress.get("counts") or {}).get("stageIndex") == 7
    assert discovery_row.get("lifecycleStatus") == "running"
    assert discovery_row.get("heartbeatAt") == report_heartbeat_at
    assert discovery_row.get("finishedAt") == ""


def test_task_state_pipeline_uses_active_child_progress_label() -> None:
    started_at = admin_bridge.now_iso()
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "discovery": task_state_entry(
                "discovery",
                run_id="discovery_child_1",
                started_at=started_at,
                pid=222,
            ),
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.DISCOVERY_REPORT_PATH,
        discovery_report(
            run_id="discovery_child_1",
            started_at=started_at,
            task_progress={
                **active_progress(
                    "scanning_sources",
                    "Scanning GameDevMap directory",
                    {"stageIndex": 7, "stageTotal": 11},
                ),
                "mode": "indeterminate",
                "ratio": 0,
            },
            summary={"queuedCandidateCount": 0},
        ),
    )
    admin_bridge.bridge_runtime_state.PIPELINE_STATUS.update(
        {
            "active": True,
            "runId": "pipeline_child_progress_1",
            "stage": "discovery",
            "startedAt": started_at,
            "finishedAt": "",
            "progress": {
                "currentStep": 1,
                "totalSteps": 3,
                "percent": 33,
                "label": "Running discovery...",
            },
        }
    )
    try:
        admin_bridge.start_lifecycle_run(
            run_id="pipeline_child_progress_1",
            task_type="pipeline",
            started_at=started_at,
            stage="discovery",
            owner_kind="pipeline",
            progress={
                "active": True,
                "phaseKey": "discovery",
                "phaseLabel": "Running discovery...",
                "mode": "determinate",
                "ratio": 1 / 3,
                "counts": {"currentStep": 1, "totalSteps": 3},
                "updatedAt": started_at,
            },
            summary={"stage": "discovery"},
        )
        admin_bridge.start_lifecycle_run(
            run_id="discovery_child_1",
            task_type="discovery",
            started_at=started_at,
            stage="starting",
            owner_kind="process",
            owner_pid=222,
            progress={
                "active": True,
                "phaseKey": "starting",
                "phaseLabel": "Spawning discovery worker",
                "mode": "indeterminate",
                "ratio": 0,
                "counts": {},
                "updatedAt": started_at,
            },
            summary={},
        )
        admin_bridge.attach_lifecycle_child(
            run_id="discovery_child_1",
            task_type="discovery",
            parent_run_id="pipeline_child_progress_1",
            parent_task_type="pipeline",
            owner_kind="pipeline",
        )
        admin_bridge.heartbeat_lifecycle_run(
            "discovery_child_1",
            "discovery",
            heartbeat_at=started_at,
            stage="running",
            progress={
                **active_progress(
                    "scanning_sources",
                    "Scanning GameDevMap directory",
                    {"stageIndex": 7, "stageTotal": 11},
                ),
                "mode": "indeterminate",
                "ratio": 0,
            },
            summary={"queuedCandidateCount": 0},
        )

        with mock.patch.object(admin_bridge, "pid_is_running", return_value=True):
            payload = current_task_payload()
    finally:
        admin_bridge.bridge_runtime_state.PIPELINE_STATUS.update(
            {
                "active": False,
                "runId": "",
                "stage": "idle",
                "startedAt": "",
                "finishedAt": "",
                "progress": {"currentStep": 0, "totalSteps": 3, "percent": 0, "label": "Idle"},
            }
        )

    pipeline_row = task_row(payload, "pipeline")
    pipeline_progress = pipeline_row.get("taskProgress") or {}
    pipeline_summary = pipeline_row.get("summary") or {}
    assert pipeline_progress.get("phaseLabel") == "Discovery: Scanning GameDevMap directory"
    assert pipeline_summary.get("activeChildTaskType") == "discovery"
    assert pipeline_summary.get("activeChildRunId") == "discovery_child_1"


def test_task_state_ignores_stale_discovery_report_after_pipeline_advances_to_fetch() -> None:
    started_at = admin_bridge.now_iso()
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_LIFECYCLE_PATH,
        {"schemaVersion": 1, "updatedAt": "", "rows": []},
    )
    admin_bridge.save_json_atomic(
        admin_bridge.DISCOVERY_REPORT_PATH,
        discovery_report(
            run_id="discovery_stale_1",
            started_at=started_at,
            task_progress=active_progress(
                "probing_candidates",
                "Probing 872 candidate(s)",
                {"stageIndex": 10, "stageTotal": 11},
            ),
        ),
    )
    admin_bridge.bridge_runtime_state.PIPELINE_STATUS.update(
        {
            "active": True,
            "runId": "pipeline_fetch_1",
            "stage": "fetch",
            "startedAt": started_at,
            "finishedAt": "",
            "progress": {
                "currentStep": 2,
                "totalSteps": 3,
                "percent": 67,
                "label": "Running fetch...",
            },
        }
    )
    try:
        admin_bridge.start_lifecycle_run(
            run_id="pipeline_fetch_1",
            task_type="pipeline",
            started_at=started_at,
            stage="fetch",
            owner_kind="pipeline",
            progress=active_progress(
                "fetch",
                "Running fetch...",
                {"currentStep": 2, "totalSteps": 3},
            ),
            summary={"stage": "fetch"},
        )
        admin_bridge.start_lifecycle_run(
            run_id="discovery_stale_1",
            task_type="discovery",
            started_at=started_at,
            stage="discovery",
            owner_kind="pipeline",
            parent_run_id="pipeline_fetch_1",
            parent_task_type="pipeline",
        )
        admin_bridge.finish_lifecycle_run(
            "discovery_stale_1",
            "discovery",
            finished_at=started_at,
            summary={"probedCandidateCount": 872},
        )
        admin_bridge.start_lifecycle_run(
            run_id="fetch_active_1",
            task_type="fetch",
            started_at=started_at,
            stage="fetch",
            owner_kind="pipeline",
            parent_run_id="pipeline_fetch_1",
            parent_task_type="pipeline",
            progress=active_progress("fetch", "Fetching jobs", {"completedTasks": 10}),
            summary={"sourceCount": 100},
        )

        payload = current_task_payload()
    finally:
        admin_bridge.bridge_runtime_state.PIPELINE_STATUS.update(
            {
                "active": False,
                "runId": "",
                "stage": "idle",
                "startedAt": "",
                "finishedAt": "",
                "progress": {"currentStep": 0, "totalSteps": 3, "percent": 0, "label": "Idle"},
            }
        )

    task_types = {str(row.get("taskType") or "") for row in payload.get("tasks") or []}
    assert task_types == {"pipeline", "fetch"}


def test_task_state_filters_pipeline_owned_child_when_parent_stage_mismatches() -> None:
    started_at = admin_bridge.now_iso()
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_LIFECYCLE_PATH,
        {"schemaVersion": 1, "updatedAt": "", "rows": []},
    )
    admin_bridge.bridge_runtime_state.PIPELINE_STATUS.update(
        {
            "active": True,
            "runId": "pipeline_stage_mismatch_1",
            "stage": "fetch",
            "startedAt": started_at,
            "finishedAt": "",
            "progress": {
                "currentStep": 2,
                "totalSteps": 3,
                "percent": 67,
                "label": "Running fetch...",
            },
        }
    )
    try:
        admin_bridge.start_lifecycle_run(
            run_id="pipeline_stage_mismatch_1",
            task_type="pipeline",
            started_at=started_at,
            stage="fetch",
            owner_kind="pipeline",
            summary={"stage": "fetch"},
        )
        admin_bridge.start_lifecycle_run(
            run_id="discovery_wrong_stage_1",
            task_type="discovery",
            started_at=started_at,
            stage="discovery",
            owner_kind="pipeline",
            parent_run_id="pipeline_stage_mismatch_1",
            parent_task_type="pipeline",
            progress=active_progress(
                "probing_candidates",
                "Probing stale candidates",
                {"stageIndex": 10, "stageTotal": 11},
            ),
        )

        payload = current_task_payload()
    finally:
        admin_bridge.bridge_runtime_state.PIPELINE_STATUS.update(
            {
                "active": False,
                "runId": "",
                "stage": "idle",
                "startedAt": "",
                "finishedAt": "",
                "progress": {"currentStep": 0, "totalSteps": 3, "percent": 0, "label": "Idle"},
            }
        )

    task_types = {str(row.get("taskType") or "") for row in payload.get("tasks") or []}
    assert task_types == {"pipeline"}
    assert any(
        str(item.get("code") or "") == "pipeline_child_stage_mismatch"
        for item in payload.get("diagnostics") or []
    )
