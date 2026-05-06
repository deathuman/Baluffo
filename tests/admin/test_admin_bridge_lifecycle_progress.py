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
    admin_bridge.save_json_atomic(
        admin_bridge.DISCOVERY_REPORT_PATH,
        discovery_report(
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
        ),
    )
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

    with mock.patch.object(admin_bridge, "pid_is_running", return_value=True):
        payload = current_task_payload()

    discovery_row = task_row(payload, "discovery")
    progress = discovery_row.get("taskProgress") or {}
    assert progress.get("phaseLabel") == "Scanning known careers pages"
    assert (progress.get("counts") or {}).get("stageIndex") == 7
    assert discovery_row.get("lifecycleStatus") == "running"
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
