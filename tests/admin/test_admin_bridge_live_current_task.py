import json
from typing import Any
from unittest import mock

import pytest

from src import admin_bridge
from src.bridge import storage_task_rows
from src.bridge.ops_api_task_state import OpsApiTaskStateMixin, _compact_task_state_payload

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


class _LifecycleRowsStub:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def _current_lifecycle_rows(self) -> list[dict]:
        return list(self._rows)


def test_task_state_summary_merges_active_storage_rows(monkeypatch) -> None:
    monkeypatch.setattr(storage_task_rows, "has_active_watchers", lambda: True)
    fetch: dict[str, Any] = {
        "taskType": "fetch",
        "runId": "fetch_1",
        "active": True,
        "startedAt": "2026-03-01T00:00:01+00:00",
    }
    storage: dict[str, Any] = {
        "taskType": "storage_reclaim",
        "runId": "storage_reclaim_1",
        "active": True,
        "startedAt": "2026-03-01T00:00:05+00:00",
        "status": "running",
    }
    finished: dict[str, Any] = {
        "taskType": "storage_rebuild",
        "runId": "rebuild_old",
        "active": False,
        "startedAt": "2026-03-01T00:00:00+00:00",
        "finishedAt": "2026-03-01T00:00:02+00:00",
    }

    stub: Any = _LifecycleRowsStub([fetch, storage, finished])
    payload: Any = {"tasks": [fetch], "count": 1, "summary": True}

    result = OpsApiTaskStateMixin._with_active_storage_rows(stub, payload)

    assert [row["runId"] for row in result["tasks"]] == ["storage_reclaim_1", "fetch_1"]
    assert result["count"] == 2
    assert result["summary"] is True


def test_task_state_summary_skips_the_ledger_without_a_storage_watcher(monkeypatch) -> None:
    monkeypatch.setattr(storage_task_rows, "has_active_watchers", lambda: False)
    stub: Any = object()
    payload: Any = {"tasks": [{"taskType": "fetch", "runId": "fetch_1"}], "count": 1}

    assert OpsApiTaskStateMixin._with_active_storage_rows(stub, payload) is payload


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
