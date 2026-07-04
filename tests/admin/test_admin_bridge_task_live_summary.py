import json
from unittest import mock

import pytest

from src import admin_bridge
from src.bridge import ops_task_fetch_live as ops_task_fetch_live_mod
from tests.admin._runtime_helpers import active_progress, fetch_report, task_live_payload

pytestmark = pytest.mark.usefixtures("admin_bridge_entrypoint_root")


def _start_fetch_lifecycle(
    *,
    run_id: str,
    started_at: str,
    progress: dict[str, object],
    summary: dict[str, object],
) -> None:
    admin_bridge.start_lifecycle_run(
        run_id=run_id,
        task_type="fetch",
        started_at=started_at,
        owner_kind="process",
        owner_pid=111,
        progress=progress,
        summary=summary,
    )


def test_get_task_live_payload_fetch_summary_stays_bounded_without_report_detail() -> None:
    run_id = "fetch_live_summary_1"
    started_at = "2026-03-08T10:00:00.000Z"
    progress = active_progress(
        "execute_sources",
        "Executing sources",
        {
            "resolvedSources": 120,
            "sourceCount": 5000,
            "runningTasks": 4,
            "queuedTasks": 4876,
            "outputCount": 12345,
            "failedSources": 7,
            "completedTasks": 120,
        },
    )
    summary = {
        "successfulSources": 113,
        "failedSources": 7,
        "excludedSources": 0,
        "outputCount": 12345,
        "sourceCount": 5000,
    }
    large_report = fetch_report(
        run_id=run_id,
        started_at=started_at,
        summary=summary,
        task_progress=progress,
    )
    large_report["sources"] = [
        {
            "name": f"source_{index}",
            "status": "ok",
            "keptCount": index,
            "durationMs": index,
        }
        for index in range(5000)
    ]
    admin_bridge.save_json_atomic(admin_bridge.JOBS_FETCH_REPORT_PATH, large_report)
    _start_fetch_lifecycle(
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
            "active": True,
            "taskProgress": progress,
            "workItems": [{"id": f"live_{index}", "status": "running"} for index in range(200)],
            "recentEvents": [
                {
                    "timestamp": started_at,
                    "phaseKey": "execute_sources",
                    "message": f"event {index}",
                }
                for index in range(12)
            ],
        },
    )
    original_load_runtime_evidence = ops_task_fetch_live_mod.load_runtime_evidence

    def guarded_load_runtime_evidence(path, default):
        assert path != admin_bridge.JOBS_FETCH_REPORT_PATH
        return original_load_runtime_evidence(path, default)

    with mock.patch.object(
        ops_task_fetch_live_mod,
        "load_runtime_evidence",
        side_effect=guarded_load_runtime_evidence,
    ):
        payload = task_live_payload("fetch", summary=True)

    assert payload.get("summaryView") is True
    assert payload.get("detailLevel") == "summary"
    assert str(payload.get("runId") or "") == run_id
    assert bool(payload.get("active")) is True
    counts = (payload.get("taskProgress") or {}).get("counts") or {}
    assert counts.get("resolvedSources") == 120
    assert counts.get("sourceCount") == 5000
    assert counts.get("outputCount") == 12345
    assert payload.get("workItems") == []
    assert payload.get("workItemCount") == 0
    assert payload.get("recentEventCount") == 12
    assert len(payload.get("recentEvents") or []) == 5
    assert len(json.dumps(payload).encode("utf-8")) < 64 * 1024


def test_get_task_live_payload_prefers_writing_outputs_summary_sidecar() -> None:
    run_id = "fetch_live_summary_writing_outputs"
    started_at = "2026-03-08T10:00:00.000Z"
    stale_progress = active_progress(
        "execute_sources",
        "Executing sources",
        {
            "resolvedSources": 10,
            "sourceCount": 10,
            "runningTasks": 0,
            "queuedTasks": 0,
            "outputCount": 100,
            "failedSources": 0,
            "completedTasks": 10,
        },
    )
    summary = {
        "successfulSources": 10,
        "failedSources": 0,
        "excludedSources": 0,
        "outputCount": 100,
        "sourceCount": 10,
    }
    _start_fetch_lifecycle(
        run_id=run_id,
        started_at=started_at,
        progress=stale_progress,
        summary=summary,
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_TASKS_PATH,
        {
            "taskType": "fetch",
            "runId": run_id,
            "startedAt": started_at,
            "status": "running",
            "active": True,
            "taskProgress": stale_progress,
            "workItems": [],
            "recentEvents": [],
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH.with_name("jobs-fetch-report-summary.json"),
        {
            "ok": True,
            "summaryView": True,
            "detailLevel": "summary",
            "runId": run_id,
            "status": "running",
            "startedAt": started_at,
            "summary": summary,
            "taskProgress": {
                "active": True,
                "phaseKey": "writing_outputs",
                "phaseLabel": "Writing outputs",
                "mode": "indeterminate",
                "counts": {"outputCount": 100, "sourceCount": 10, "completedTasks": 10},
            },
        },
    )

    payload = task_live_payload("fetch", summary=True)

    progress = payload.get("taskProgress") or {}
    assert progress.get("phaseKey") == "writing_outputs"
    assert progress.get("phaseLabel") == "Writing outputs"
    assert (progress.get("counts") or {}).get("outputCount") == 100


def test_get_task_live_payload_prefers_finalizing_sources_summary_sidecar() -> None:
    run_id = "fetch_live_summary_finalizing_sources"
    started_at = "2026-03-08T10:00:00.000Z"
    stale_progress = active_progress(
        "executing_sources",
        "Executing sources",
        {
            "resolvedSources": 10,
            "sourceCount": 10,
            "runningTasks": 0,
            "queuedTasks": 0,
            "outputCount": 100,
            "failedSources": 0,
            "completedTasks": 10,
        },
    )
    summary = {
        "successfulSources": 10,
        "failedSources": 0,
        "excludedSources": 0,
        "outputCount": 100,
        "sourceCount": 10,
    }
    _start_fetch_lifecycle(
        run_id=run_id,
        started_at=started_at,
        progress=stale_progress,
        summary=summary,
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_TASKS_PATH,
        {
            "taskType": "fetch",
            "runId": run_id,
            "startedAt": started_at,
            "status": "running",
            "active": True,
            "taskProgress": stale_progress,
            "workItems": [],
            "recentEvents": [],
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH.with_name("jobs-fetch-report-summary.json"),
        {
            "ok": True,
            "summaryView": True,
            "detailLevel": "summary",
            "runId": run_id,
            "status": "running",
            "startedAt": started_at,
            "summary": summary,
            "taskProgress": {
                "active": True,
                "phaseKey": "finalizing_sources",
                "phaseLabel": "Finalizing source results",
                "mode": "determinate",
                "ratio": 1,
                "counts": {
                    "sourceCount": 10,
                    "completedTasks": 10,
                    "resolvedSources": 10,
                    "runningTasks": 0,
                    "queuedTasks": 0,
                    "outputCount": 100,
                },
            },
        },
    )

    payload = task_live_payload("fetch", summary=True)

    progress = payload.get("taskProgress") or {}
    counts = progress.get("counts") or {}
    assert progress.get("phaseKey") == "finalizing_sources"
    assert progress.get("phaseLabel") == "Finalizing source results"
    assert counts.get("completedTasks") == 10
    assert counts.get("runningTasks") == 0
    assert counts.get("queuedTasks") == 0
