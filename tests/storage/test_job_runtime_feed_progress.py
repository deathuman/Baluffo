from __future__ import annotations

from typing import Any

from src.storage import BaluffoStore, JobRuntimeStore
from tests.helpers.temp_paths import workspace_tmpdir


def _row(index: int) -> dict[str, object]:
    return {
        "id": index,
        "title": f"Job {index}",
        "company": "Studio",
        "city": "Amsterdam",
        "country": "Netherlands",
        "workType": "Hybrid",
        "contractType": "Full-time",
        "jobLink": f"https://example.test/jobs/{index}",
        "source": "Example",
        "sourceJobId": f"job-{index}",
        "status": "active",
        "firstSeenAt": "2026-05-12T10:00:00Z",
        "lastSeenAt": "2026-05-12T10:00:00Z",
        "sourceBundle": [{"sourceName": "Example", "sourceJobId": f"job-{index}"}],
    }


def test_stage_and_publish_report_phase_and_batch_progress() -> None:
    with workspace_tmpdir("job-runtime-feed-progress") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T10:00:00Z",
                batch_size=1,
            )
            stage_progress: list[dict[str, Any]] = []

            staged = runtime.stage_feed(
                run_id="fetch_progress",
                rows=[_row(1), _row(2)],
                on_progress=stage_progress.append,
            )

            assert all(event["phase"] == "stage" for event in stage_progress)
            stage_details = [str(event.get("detail") or "") for event in stage_progress]
            assert "preparing" in stage_details
            assert "write_jobs" in stage_details
            assert "write_sources" in stage_details
            assert stage_details[-1] == "complete"
            job_events = [event for event in stage_progress if event.get("detail") == "write_jobs"]
            assert [event["processedRowCount"] for event in job_events] == [1, 2]
            assert [event["batchIndex"] for event in job_events] == [1, 2]
            assert [event["batchCount"] for event in job_events] == [2, 2]

            publish_progress: list[dict[str, Any]] = []
            runtime.publish_generation(
                staged.generation,
                expected_row_count=staged.row_count,
                expected_row_hash=staged.row_hash,
                on_progress=publish_progress.append,
            )

            assert all(event["phase"] == "publish" for event in publish_progress)
            publish_details = [str(event.get("detail") or "") for event in publish_progress]
            assert publish_details[0] == "read_generation"
            assert "hash_generation" in publish_details
            assert "write_state" in publish_details
            assert publish_details[-1] == "complete"
            assert publish_progress[-1]["rowCount"] == 2
            assert publish_progress[-1]["sourceCount"] == 2
