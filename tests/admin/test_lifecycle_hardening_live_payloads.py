from __future__ import annotations

import pytest

from src import admin_bridge
from tests.admin._runtime_helpers import (
    active_progress,
    current_task_payload,
    fetch_report,
    task_live_payload,
    task_row,
    task_state_entry,
)

pytestmark = pytest.mark.usefixtures("admin_bridge_entrypoint_root")


def test_current_task_state_fetch_row_uses_live_output_progress_ratio() -> None:
    run_id = "fetch_live_ratio_authority_1"
    started_at = "2026-03-08T10:00:00.000Z"
    stale_progress = active_progress(
        "executing_sources",
        "Executing sources",
        {"resolvedSources": 2, "completedTasks": 2, "sourceCount": 4},
    )
    stale_progress["ratio"] = 0.95
    live_progress = active_progress(
        "executing_sources",
        "Executing sources",
        {
            "resolvedSources": 2,
            "completedTasks": 2,
            "sourceCount": 4,
            "runningTasks": 1,
            "queuedTasks": 1,
            "outputCount": 12,
            "failedSources": 1,
        },
    )
    live_progress["ratio"] = 0.5
    summary = {
        "successfulSources": 1,
        "failedSources": 1,
        "excludedSources": 0,
        "outputCount": 12,
        "sourceCount": 4,
    }
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {"fetch": task_state_entry("fetch", run_id=run_id, started_at=started_at)},
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        fetch_report(
            run_id=run_id,
            started_at=started_at,
            summary=summary,
            task_progress=live_progress,
        ),
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_TASKS_PATH,
        {
            "taskType": "fetch",
            "runId": run_id,
            "startedAt": started_at,
            "status": "running",
            "summary": {"running": 1, "queued": 1, "outputCount": 12},
            "taskProgress": live_progress,
        },
    )
    admin_bridge.start_lifecycle_run(
        run_id=run_id,
        task_type="fetch",
        started_at=started_at,
        owner_kind="process",
        owner_pid=111,
        progress=stale_progress,
        summary=summary,
    )

    current_row = task_row(current_task_payload(), "fetch")
    live_payload = task_live_payload("fetch")

    assert current_row["runId"] == run_id
    assert current_row["taskProgress"]["ratio"] == live_payload["taskProgress"]["ratio"]
    assert current_row["taskProgress"]["ratio"] == 0.5
    assert current_row["taskProgress"]["counts"]["resolvedSources"] == 2
    assert current_row["parentRunId"] == ""
