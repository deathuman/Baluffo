import json
from unittest import mock

import pytest

from src import admin_bridge
from src.bridge.ops_api import _compact_task_state_payload

from ._admin_bridge_live_payloads_shared import (
    CURRENT_TASK_STATE_CASES,
    _CurrentTaskStateCase,
    current_task_payload,
)

pytestmark = pytest.mark.usefixtures("admin_bridge_entrypoint_root")


@pytest.mark.parametrize("case", CURRENT_TASK_STATE_CASES, ids=lambda case: case.name)
def test_get_current_task_state_payload_cases(case: _CurrentTaskStateCase) -> None:
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_LIFECYCLE_PATH,
        {"schemaVersion": 1, "updatedAt": "", "rows": []},
    )
    cleanup = case.setup()
    try:
        if case.pid_is_running is None:
            payload = current_task_payload()
        else:
            with mock.patch.object(
                admin_bridge, "pid_is_running", return_value=case.pid_is_running
            ):
                payload = current_task_payload()
        case.assert_payload(payload)
    finally:
        if cleanup is not None:
            cleanup()


def test_current_task_state_summary_payload_stays_bounded_for_large_work_items() -> None:
    payload = {
        "tasks": [
            {
                "taskType": "fetch",
                "runId": "fetch_large_1",
                "active": True,
                "workItems": [
                    {
                        "source": f"source-{index}",
                        "status": "pending",
                        "details": "x" * 1000,
                    }
                    for index in range(5000)
                ],
                "recentEvents": [
                    {
                        "event": "source_progress",
                        "message": "x" * 1000,
                        "index": index,
                    }
                    for index in range(200)
                ],
            }
        ],
        "count": 1,
    }

    summary = _compact_task_state_payload(payload)
    row = summary["tasks"][0]

    assert summary["summary"] is True
    assert "workItems" not in row
    assert row["workItemCount"] == 5000
    assert row["recentEventCount"] == 200
    assert len(row["recentEvents"]) == 5
    assert len(json.dumps(summary).encode("utf-8")) < 256 * 1024
