from collections.abc import Callable
from dataclasses import dataclass
from unittest import mock

import pytest

from src import admin_bridge
from tests.admin._runtime_helpers import (
    active_progress,
    completed_progress,
    current_task_payload,
    discovery_report,
    fetch_report,
    history_row,
    matching_history_rows,
    task_live_payload,
    task_row,
    task_state_entry,
)

pytestmark = pytest.mark.usefixtures("admin_bridge_entrypoint_root")


@dataclass(frozen=True)
class _CurrentTaskStateCase:
    name: str
    setup: Callable[[], Callable[[], None] | None]
    assert_payload: Callable[[dict[str, object]], None]
    pid_is_running: bool | None = None


def _setup_active_tasks_projection() -> Callable[[], None]:
    started_at = admin_bridge.now_iso()
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": task_state_entry("fetch", run_id="fetch_1", started_at=started_at),
            "discovery": task_state_entry(
                "discovery",
                run_id="discovery_1",
                started_at=started_at,
                pid=222,
            ),
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        fetch_report(
            run_id="fetch_1",
            started_at=started_at,
            task_progress=active_progress(
                "executing_sources",
                "Executing sources",
                {"resolvedSources": 5, "sourceCount": 10},
            ),
            summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
        ),
    )
    admin_bridge.save_json_atomic(
        admin_bridge.DISCOVERY_REPORT_PATH,
        discovery_report(
            run_id="discovery_1",
            started_at=started_at,
            task_progress={
                **active_progress(
                    "scanning_sources",
                    "Scanning known careers pages",
                    {"queuedCandidates": 3},
                ),
                "mode": "indeterminate",
                "ratio": 0,
            },
            summary={"queuedCandidateCount": 3},
        ),
    )
    admin_bridge.bridge_runtime_state.PIPELINE_STATUS.update(
        {
            "active": True,
            "runId": "pipeline_1",
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
    admin_bridge.SyncState.add_active_sync_run("sync_1")
    admin_bridge.append_run_history(
        {
            "id": "sync_1",
            "type": "sync",
            "status": "started",
            "startedAt": started_at,
            "finishedAt": "",
            "durationMs": 0,
            "summary": {"action": "push"},
        }
    )

    def cleanup() -> None:
        admin_bridge.SyncState.remove_active_sync_run("sync_1")
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

    return cleanup


def _assert_active_tasks_projection(payload: dict[str, object]) -> None:
    tasks = payload.get("tasks") or []
    task_types = {str(row.get("taskType") or "") for row in tasks}
    assert payload.get("count") == 4
    assert {"fetch", "discovery", "pipeline", "sync"} <= task_types
    fetch_row = task_row(payload, "fetch")
    assert str((fetch_row.get("taskProgress") or {}).get("phaseKey") or "") == "executing_sources"
    pipeline_row = task_row(payload, "pipeline")
    assert str((pipeline_row.get("taskProgress") or {}).get("phaseLabel") or "") == "Running fetch..."


def _setup_finished_reports_clear_stale_state() -> None:
    started_at = "2026-03-08T10:00:30.000Z"
    finished_at = "2026-03-08T10:05:30.000Z"
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": task_state_entry("fetch", run_id="fetch_1", started_at=started_at),
            "discovery": task_state_entry(
                "discovery",
                run_id="discovery_1",
                started_at=started_at,
                pid=222,
            ),
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        fetch_report(
            run_id="fetch_1",
            started_at=started_at,
            finished_at=finished_at,
            task_progress=completed_progress("Fetcher completed"),
            summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
        ),
    )
    admin_bridge.save_json_atomic(
        admin_bridge.DISCOVERY_REPORT_PATH,
        discovery_report(
            run_id="discovery_1",
            started_at=started_at,
            finished_at=finished_at,
            task_progress=completed_progress("Discovery completed"),
            summary={"queuedCandidateCount": 3},
        ),
    )


def _assert_finished_reports_clear_stale_state(payload: dict[str, object]) -> None:
    assert payload.get("count") == 0
    assert payload.get("tasks") == []
    assert admin_bridge.load_json_object(admin_bridge.TASK_STATE_PATH, {}) == {}


def _setup_heartbeat_gap_fetch() -> None:
    started_at = admin_bridge.now_iso()
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": task_state_entry("fetch", run_id="fetch_1", started_at=started_at),
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        fetch_report(
            run_id="fetch_1",
            started_at=started_at,
            runtime={"lifecycle": {"owner": "fetch_report", "heartbeatAt": ""}},
            task_progress={
                **active_progress(
                    "executing_sources",
                    "Executing sources",
                    {"resolvedSources": 5, "sourceCount": 10},
                ),
                "active": False,
            },
            summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
        ),
    )


def _assert_heartbeat_gap_fetch(payload: dict[str, object]) -> None:
    fetch_row = task_row(payload, "fetch")
    assert payload.get("count") == 1
    assert fetch_row.get("active") is True
    assert str(fetch_row.get("status") or "") == "running"


def _setup_active_owner_over_finished_history() -> None:
    started_at = admin_bridge.now_iso()
    run_id = "fetch_live_1"
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
            runtime={"lifecycle": {"owner": "fetch_report", "heartbeatAt": started_at}},
            task_progress=active_progress(
                "executing_sources",
                "Executing sources",
                {"resolvedSources": 5, "sourceCount": 10},
            ),
            summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
        ),
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_TASKS_PATH,
        {
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": "",
            "heartbeatAt": started_at,
            "taskProgress": {"active": True},
            "summary": {"queued": 0, "running": 1, "ok": 0, "error": 0},
            "tasks": [],
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [
            history_row(
                row_id=run_id,
                run_id=run_id,
                status="warning",
                started_at=started_at,
                finished_at=admin_bridge.now_iso(),
                duration_ms=123,
                summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
            )
        ],
    )


def _assert_active_owner_over_finished_history(payload: dict[str, object]) -> None:
    fetch_row = task_row(payload, "fetch")
    assert fetch_row["active"] is True
    assert str(fetch_row.get("runId") or "") == "fetch_live_1"
    diagnostics = payload.get("diagnostics") or []
    assert any(
        str(item.get("code") or "") == "history_finished_while_owner_active" for item in diagnostics
    )


def _setup_report_finished_while_owner_active() -> None:
    started_at = admin_bridge.now_iso()
    finished_at = admin_bridge.now_iso()
    run_id = "fetch_report_finished_1"
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
            finished_at=finished_at,
            runtime={"lifecycle": {"owner": "fetch_report", "heartbeatAt": started_at}},
            task_progress=active_progress(
                "executing_sources",
                "Executing sources",
                {"resolvedSources": 5, "sourceCount": 10},
            ),
            summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
        ),
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_TASKS_PATH,
        {
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": "",
            "heartbeatAt": started_at,
            "taskProgress": {"active": True},
            "summary": {"queued": 0, "running": 1, "ok": 0, "error": 0},
            "tasks": [],
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [
            history_row(
                row_id=run_id,
                run_id=run_id,
                status="warning",
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=123,
                summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
            )
        ],
    )


def _assert_report_finished_while_owner_active(payload: dict[str, object]) -> None:
    fetch_row = task_row(payload, "fetch")
    assert fetch_row["active"] is True
    assert str(fetch_row.get("runId") or "") == "fetch_report_finished_1"
    diagnostics = payload.get("diagnostics") or []
    assert any(
        str(item.get("code") or "") == "report_finished_while_owner_active" for item in diagnostics
    )


def _setup_stale_finished_report_with_live_fetch_owner() -> None:
    live_started_at = admin_bridge.now_iso()
    live_run_id = "fetch_live_owner_1"
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": task_state_entry("fetch", run_id=live_run_id, started_at=live_started_at),
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        fetch_report(
            run_id="fetch_finished_old_1",
            started_at="2026-03-01T00:00:00+00:00",
            finished_at="2026-03-01T00:05:00+00:00",
            task_progress=completed_progress("Fetcher completed"),
            summary={"outputCount": 7, "failedSources": 0, "sourceCount": 10},
        ),
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_TASKS_PATH,
        {
            "taskType": "fetch",
            "runId": live_run_id,
            "startedAt": live_started_at,
            "heartbeatAt": live_started_at,
            "status": "running",
            "summary": {"running": 1},
            "taskProgress": active_progress(
                "execute_sources",
                "Executing sources",
                {"sourceCount": 10, "runningTasks": 1, "resolvedSources": 2},
            ),
            "workItems": [
                {
                    "id": "source_live_1",
                    "name": "Live Source",
                    "status": "running",
                }
            ],
        },
    )


def _assert_stale_finished_report_with_live_fetch_owner(payload: dict[str, object]) -> None:
    fetch_row = task_row(payload, "fetch")
    assert payload.get("count") == 1
    assert fetch_row.get("active") is True
    assert str(fetch_row.get("status") or "") == "running"
    assert str(fetch_row.get("runId") or "") == "fetch_live_owner_1"
    counts = (fetch_row.get("taskProgress") or {}).get("counts") or {}
    assert counts.get("sourceCount") == 10
    assert counts.get("runningTasks") == 1
    task_state = admin_bridge.load_json_object(admin_bridge.TASK_STATE_PATH, {})
    assert str((task_state.get("fetch") or {}).get("runId") or "") == "fetch_live_owner_1"


def _setup_sparse_fetch_task_artifact_keeps_owner_active() -> None:
    started_at = admin_bridge.now_iso()
    run_id = "fetch_sparse_live_1"
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
            finished_at="",
            runtime={"lifecycle": {"owner": "fetch_sparse", "heartbeatAt": started_at}},
            task_progress={},
            summary={"outputCount": 4, "failedSources": 0, "sourceCount": 10},
        ),
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_TASKS_PATH,
        {
            "taskType": "fetch",
            "active": True,
            "runId": run_id,
            "startedAt": started_at,
            "heartbeatAt": started_at,
            "status": "running",
            "taskProgress": {},
            "workItems": [
                {
                    "id": "source_sparse_1",
                    "name": "Sparse Source",
                    "status": "running",
                }
            ],
            "recentEvents": [
                {
                    "timestamp": started_at,
                    "message": "Fetch still active",
                }
            ],
            "summary": {"running": 1},
        },
    )


def _assert_sparse_fetch_task_artifact_keeps_owner_active(payload: dict[str, object]) -> None:
    fetch_row = task_row(payload, "fetch")
    assert fetch_row.get("active") is True
    assert str(fetch_row.get("status") or "") == "running"
    assert str(fetch_row.get("runId") or "") == "fetch_sparse_live_1"


CURRENT_TASK_STATE_CASES = [
    pytest.param(
        _CurrentTaskStateCase(
            name="active-task-projection",
            setup=_setup_active_tasks_projection,
            assert_payload=_assert_active_tasks_projection,
        ),
        id="active-task-projection",
    ),
    pytest.param(
        _CurrentTaskStateCase(
            name="finished-reports-clear-stale-state",
            setup=_setup_finished_reports_clear_stale_state,
            assert_payload=_assert_finished_reports_clear_stale_state,
        ),
        id="finished-reports-clear-stale-state",
    ),
    pytest.param(
        _CurrentTaskStateCase(
            name="heartbeat-gap-keeps-fetch-visible",
            setup=_setup_heartbeat_gap_fetch,
            assert_payload=_assert_heartbeat_gap_fetch,
            pid_is_running=True,
        ),
        id="heartbeat-gap-keeps-fetch-visible",
    ),
    pytest.param(
        _CurrentTaskStateCase(
            name="active-owner-over-finished-history",
            setup=_setup_active_owner_over_finished_history,
            assert_payload=_assert_active_owner_over_finished_history,
        ),
        id="active-owner-over-finished-history",
    ),
    pytest.param(
        _CurrentTaskStateCase(
            name="report-finished-while-owner-active",
            setup=_setup_report_finished_while_owner_active,
            assert_payload=_assert_report_finished_while_owner_active,
        ),
        id="report-finished-while-owner-active",
    ),
    pytest.param(
        _CurrentTaskStateCase(
            name="stale-finished-report-with-live-fetch-owner",
            setup=_setup_stale_finished_report_with_live_fetch_owner,
            assert_payload=_assert_stale_finished_report_with_live_fetch_owner,
            pid_is_running=True,
        ),
        id="stale-finished-report-with-live-fetch-owner",
    ),
    pytest.param(
        _CurrentTaskStateCase(
            name="sparse-fetch-task-artifact-keeps-owner-active",
            setup=_setup_sparse_fetch_task_artifact_keeps_owner_active,
            assert_payload=_assert_sparse_fetch_task_artifact_keeps_owner_active,
            pid_is_running=False,
        ),
        id="sparse-fetch-task-artifact-keeps-owner-active",
    ),
]


@pytest.mark.parametrize("case", CURRENT_TASK_STATE_CASES, ids=lambda case: case.name)
def test_get_current_task_state_payload_cases(case: _CurrentTaskStateCase) -> None:
    cleanup = case.setup()
    try:
        if case.pid_is_running is None:
            payload = current_task_payload()
        else:
            with mock.patch.object(admin_bridge, "pid_is_running", return_value=case.pid_is_running):
                payload = current_task_payload()
        case.assert_payload(payload)
    finally:
        if cleanup is not None:
            cleanup()


def test_get_task_live_payload_fetch_preserves_shared_contract() -> None:
    run_id = "fetch_live_contract_1"
    started_at = "2026-03-08T10:00:00.000Z"
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
            summary={"outputCount": 7, "failedSources": 1, "sourceCount": 4},
            task_progress=active_progress(
                "execute_sources",
                "Executing sources",
                {"sourceCount": 4, "completedTasks": 2},
            ),
        ),
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
    assert recent_events[0].get("message") == "Fetching details"


def test_get_task_live_payload_fetch_ignores_stale_task_artifact_and_uses_current_report_detail() -> None:
    run_id = "fetch_live_current_1"
    started_at = "2026-03-08T10:00:00.000Z"
    heartbeat_at = "2026-03-08T10:03:00.000Z"
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": task_state_entry("fetch", run_id=run_id, started_at=started_at),
        },
    )
    current_report = fetch_report(
        run_id=run_id,
        started_at=started_at,
        summary={
            "successfulSources": 10,
            "failedSources": 0,
            "excludedSources": 0,
            "outputCount": 34081,
            "sourceCount": 551,
        },
        runtime={"selectedSourceCount": 551, "heartbeatAt": heartbeat_at},
        task_progress=active_progress(
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
        ),
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


def test_get_task_live_payload_fetch_supplements_current_run_detail_when_task_artifact_lacks_work_items() -> None:
    run_id = "fetch_live_partial_1"
    started_at = "2026-03-08T10:00:00.000Z"
    heartbeat_at = "2026-03-08T10:04:00.000Z"
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": task_state_entry("fetch", run_id=run_id, started_at=started_at),
        },
    )
    report = fetch_report(
        run_id=run_id,
        started_at=started_at,
        summary={
            "successfulSources": 10,
            "failedSources": 0,
            "excludedSources": 0,
            "outputCount": 34081,
            "sourceCount": 551,
        },
        runtime={"selectedSourceCount": 551, "heartbeatAt": heartbeat_at},
        task_progress=active_progress(
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
        ),
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


def test_get_task_live_payload_discovery_preserves_shared_contract() -> None:
    run_id = "discovery_live_contract_1"
    started_at = "2026-03-08T10:00:00.000Z"
    heartbeat_at = "2026-03-08T10:00:05.000Z"
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
                summary={
                    "foundEndpointCount": 4,
                    "probedCandidateCount": 9,
                    "queuedCandidateCount": 3,
                    "failedProbeCount": 1,
                },
                task_progress=active_progress(
                    "probing_candidates",
                    "Probing candidates",
                    {"foundEndpoints": 4, "probedCandidates": 9, "queuedCandidates": 3},
                ),
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
    assert recent_events[0].get("message") == "Pulling remote registry"


def test_get_task_live_payload_sync_normalizes_history_backed_started_run() -> None:
    run_id = "sync_history_contract_1"
    started_at = "2026-03-08T10:00:00.000Z"
    admin_bridge.save_json_atomic(admin_bridge.SYNC_LIVE_TASK_PATH, {})
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [
            {
                "id": run_id,
                "runId": run_id,
                "type": "sync",
                "status": "started",
                "startedAt": started_at,
                "finishedAt": "",
                "durationMs": 0,
                "summary": {
                    "action": "push",
                    "activeCount": 8,
                    "pendingCount": 2,
                    "rejectedCount": 1,
                },
            }
        ],
    )

    with mock.patch.object(admin_bridge.SyncState, "get_active_sync_runs", return_value=set()):
        payload = task_live_payload("sync")

    assert str(payload.get("taskType") or "") == "sync"
    assert bool(payload.get("active")) is False
    assert str(payload.get("status") or "") == "started"
    assert str(payload.get("runId") or "") == run_id
    progress = payload.get("taskProgress") or {}
    counts = progress.get("counts") or {}
    assert progress.get("phaseKey") == "sync_push"
    assert progress.get("phaseLabel") == "Sync push"
    assert counts.get("lastAction") == "push"
    assert (payload.get("summary") or {}).get("pendingCount") == 2
