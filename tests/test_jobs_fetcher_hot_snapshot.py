from __future__ import annotations

import threading
from pathlib import Path

from src.jobs.pipeline_runtime_summary import PipelineTaskRuntime
from src.jobs.pipeline_runtime_writers import make_task_state_writer
from src.source_registry_io import load_runtime_evidence


def test_task_state_writer_updates_hot_active_snapshot(tmp_path: Path) -> None:
    runtime = PipelineTaskRuntime(
        run_id="fetch_hot",
        started_at="2026-06-16T10:00:00+00:00",
        task_rows={
            "Studio": {
                "name": "Studio",
                "status": "running",
                "startedAt": "2026-06-16T10:00:00+00:00",
                "heartbeatAt": "2026-06-16T10:00:02+00:00",
                "progress": {
                    "active": True,
                    "phaseKey": "listing_fetch",
                    "phaseLabel": "Fetching Studio",
                    "counts": {"completedTasks": 0, "totalTasks": 1},
                },
            }
        },
        recent_events=[{"message": f"event {index}"} for index in range(7)],
        task_lock=threading.Lock(),
    )
    task_state_path = tmp_path / "jobs-fetch-tasks.json"
    active_snapshot_path = tmp_path / "admin-active-task-snapshot.json"

    def normalize_task_state_payload(payload, **_kwargs):
        return payload

    def fake_write_text_if_changed(path, text):
        Path(path).write_text(text, encoding="utf-8")
        return True

    write_task_state = make_task_state_writer(
        runtime=runtime,
        run_id="fetch_hot",
        started_at="2026-06-16T10:00:00+00:00",
        report_path=str(tmp_path / "jobs-fetch-report.json"),
        task_state_path=task_state_path,
        active_snapshot_path=active_snapshot_path,
        normalize_task_state_payload=normalize_task_state_payload,
        write_text_if_changed=fake_write_text_if_changed,
    )

    write_task_state(force=True)

    snapshot = load_runtime_evidence(active_snapshot_path, {})
    assert snapshot["source"] == "hot-active-snapshot"
    assert snapshot["count"] == 1
    row = snapshot["tasks"][0]
    assert row["taskType"] == "fetch"
    assert row["runId"] == "fetch_hot"
    assert "workItems" not in row
    assert row["workItemCount"] == 1
    assert row["recentEventCount"] == 7
    assert len(row["recentEvents"]) == 5
