from collections.abc import Callable
from dataclasses import dataclass

import pytest

from src import admin_bridge
from tests.admin._runtime_helpers import (
    active_progress,
    completed_progress,
    current_task_payload,
    discovery_report,
    fetch_report,
    history_row,
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


def assert_live_task_event_envelope(
    event: dict[str, object],
    *,
    task_type: str,
    run_id: str,
    event_name: str,
    phase_key: str,
) -> None:
    assert event.get("schemaVersion") == 1
    assert event.get("event") == event_name
    assert event.get("taskType") == task_type
    assert event.get("runId") == run_id
    assert event.get("phaseKey") == phase_key
    assert "timestamp" in event
    assert "level" in event
    assert "workItemId" in event
    assert "message" in event


def start_lifecycle_for_task(
    task_type: str,
    *,
    run_id: str,
    started_at: str,
    progress: dict[str, object] | None = None,
    summary: dict[str, object] | None = None,
) -> None:
    admin_bridge.start_lifecycle_run(
        run_id=run_id,
        task_type=task_type,
        started_at=started_at,
        owner_kind="process",
        owner_pid=111,
        progress=progress or {},
        summary=summary or {},
    )


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
    admin_bridge.start_lifecycle_run(
        run_id="fetch_1",
        task_type="fetch",
        started_at=started_at,
        owner_kind="process",
        owner_pid=111,
        progress=active_progress(
            "executing_sources",
            "Executing sources",
            {"resolvedSources": 5, "sourceCount": 10},
        ),
        summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
    )
    admin_bridge.start_lifecycle_run(
        run_id="discovery_1",
        task_type="discovery",
        started_at=started_at,
        owner_kind="process",
        owner_pid=222,
        progress={
            **active_progress(
                "scanning_sources",
                "Scanning known careers pages",
                {"queuedCandidates": 3},
            ),
            "mode": "indeterminate",
            "ratio": 0,
        },
        summary={"queuedCandidateCount": 3},
    )
    admin_bridge.start_lifecycle_run(
        run_id="sync_1",
        task_type="sync",
        started_at=started_at,
        owner_kind="process",
        summary={"action": "push"},
    )
    admin_bridge.start_lifecycle_run(
        run_id="pipeline_1",
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
    assert (
        str((pipeline_row.get("taskProgress") or {}).get("phaseLabel") or "") == "Running fetch..."
    )


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
    admin_bridge.start_lifecycle_run(
        run_id="fetch_1",
        task_type="fetch",
        started_at=started_at,
        owner_kind="process",
        owner_pid=111,
        progress=active_progress(
            "executing_sources",
            "Executing sources",
            {"resolvedSources": 5, "sourceCount": 10},
        ),
        summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
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
    admin_bridge.start_lifecycle_run(
        run_id=run_id,
        task_type="fetch",
        started_at=started_at,
        owner_kind="process",
        owner_pid=111,
        progress=active_progress(
            "executing_sources",
            "Executing sources",
            {"resolvedSources": 5, "sourceCount": 10},
        ),
        summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
    )


def _assert_active_owner_over_finished_history(payload: dict[str, object]) -> None:
    fetch_row = task_row(payload, "fetch")
    assert fetch_row["active"] is True
    assert str(fetch_row.get("runId") or "") == "fetch_live_1"


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
    admin_bridge.start_lifecycle_run(
        run_id=run_id,
        task_type="fetch",
        started_at=started_at,
        owner_kind="process",
        owner_pid=111,
        progress=active_progress(
            "executing_sources",
            "Executing sources",
            {"resolvedSources": 5, "sourceCount": 10},
        ),
        summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
    )


def _assert_report_finished_while_owner_active(payload: dict[str, object]) -> None:
    fetch_row = task_row(payload, "fetch")
    assert fetch_row["active"] is True
    assert str(fetch_row.get("runId") or "") == "fetch_report_finished_1"


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
    admin_bridge.start_lifecycle_run(
        run_id=live_run_id,
        task_type="fetch",
        started_at=live_started_at,
        owner_kind="process",
        owner_pid=111,
        progress=active_progress(
            "execute_sources",
            "Executing sources",
            {"sourceCount": 10, "runningTasks": 1, "resolvedSources": 2},
        ),
        summary={"running": 1},
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
    admin_bridge.start_lifecycle_run(
        run_id=run_id,
        task_type="fetch",
        started_at=started_at,
        owner_kind="process",
        owner_pid=111,
        summary={"running": 1},
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


__all__ = [
    "CURRENT_TASK_STATE_CASES",
    "_CurrentTaskStateCase",
    "active_progress",
    "assert_live_task_event_envelope",
    "completed_progress",
    "current_task_payload",
    "discovery_report",
    "fetch_report",
    "history_row",
    "_setup_sparse_fetch_task_artifact_keeps_owner_active",
    "_setup_stale_finished_report_with_live_fetch_owner",
    "start_lifecycle_for_task",
    "task_live_payload",
    "task_row",
    "task_state_entry",
]
