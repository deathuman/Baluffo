from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.bridge import active_task_snapshot


def test_hot_snapshot_compacts_rows_and_rejects_stale(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "admin-active-task-snapshot.json"
    active_task_snapshot.write_snapshot(
        snapshot_path,
        [
            {
                "taskType": "fetch",
                "runId": "fetch_hot",
                "active": True,
                "status": "running",
                "startedAt": "2026-06-16T10:00:00+00:00",
                "heartbeatAt": "2026-06-16T10:00:05+00:00",
                "taskProgress": {
                    "active": True,
                    "phaseKey": "executing_sources",
                    "phaseLabel": "Fetching job listings",
                    "counts": {"completedTasks": 1, "totalTasks": 2},
                },
                "summary": {"running": 1},
                "outputs": {"report": "jobs-fetch-report.json"},
                "workItems": [{"id": str(index)} for index in range(25)],
                "recentEvents": [{"message": f"event {index}"} for index in range(8)],
                "sources": [{"name": "large"}],
            }
        ],
        snapshot_at="2026-06-16T10:00:05+00:00",
    )

    snapshot = active_task_snapshot.load_fresh_snapshot(
        snapshot_path,
        now=datetime(2026, 6, 16, 10, 0, 10, tzinfo=UTC),
    )
    assert snapshot is not None
    row = snapshot["tasks"][0]
    assert row["runId"] == "fetch_hot"
    assert "workItems" not in row
    assert "sources" not in row
    assert row["workItemCount"] == 25
    assert row["workItemsTruncated"] is True
    assert len(row["recentEvents"]) == 5
    assert row["recentEventCount"] == 8
    assert row["recentEventsTruncated"] is True

    stale = active_task_snapshot.load_fresh_snapshot(
        snapshot_path,
        now=datetime(2026, 6, 16, 10, 2, 0, tzinfo=UTC),
    )
    assert stale is None


def test_hot_snapshot_state_and_live_payloads_use_pipeline_fallback() -> None:
    stale_snapshot = active_task_snapshot.empty_snapshot(
        snapshot_at=(datetime.now(UTC) - timedelta(minutes=5)).isoformat()
    )
    pipeline_status = {
        "active": True,
        "runId": "pipeline_hot",
        "stage": "fetch",
        "startedAt": "2026-06-16T10:00:00+00:00",
        "snapshotAt": "2026-06-16T10:00:05+00:00",
        "progress": {"currentStep": 2, "totalSteps": 3, "percent": 66, "label": "Fetch"},
    }

    state_payload = active_task_snapshot.task_state_summary_from_snapshot(
        None,
        pipeline_status=pipeline_status,
    )
    assert state_payload is not None
    assert state_payload["source"] == "hot-active-snapshot"
    assert state_payload["tasks"][0]["taskType"] == "pipeline"
    assert state_payload["tasks"][0]["runId"] == "pipeline_hot"
    assert state_payload["diagnostics"][0]["code"] == "hot_snapshot_pipeline_child_missing"

    live_payload = active_task_snapshot.live_summary_from_snapshot(
        stale_snapshot,
        "fetch",
        pipeline_status=pipeline_status,
    )
    assert live_payload is not None
    assert live_payload["taskType"] == "fetch"
    assert live_payload["active"] is True
    assert live_payload["summaryView"] is True
    assert live_payload["workItems"] == []
    assert (
        live_payload["diagnostics"][0]["code"]
        == "hot_snapshot_child_synthetic_from_pipeline_status"
    )
