from __future__ import annotations

from src.storage import BaluffoStore, JobRuntimeStore
from tests.helpers.temp_paths import workspace_tmpdir


def _row(index: int, source_count: int) -> dict[str, object]:
    return {
        "id": index,
        "title": f"Job {index}",
        "company": "Studio",
        "city": "Amsterdam",
        "country": "Netherlands",
        "jobLink": f"https://example.test/jobs/{index}",
        "source": "Example",
        "sourceJobId": f"job-{index}",
        "status": "active",
        "firstSeenAt": "2026-05-12T10:00:00Z",
        "lastSeenAt": "2026-05-12T10:00:00Z",
        "sourceBundle": [
            {"sourceName": "Source", "sourceJobId": f"id-{index}-{source_index}"}
            for source_index in range(source_count)
        ],
    }


def test_cleanup_old_generations_caps_source_rows_not_job_keys() -> None:
    rows = [_row(index, source_count) for index, source_count in enumerate((3, 2, 1), start=1)]
    with workspace_tmpdir("job-runtime-cleanup-row-cap") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T10:00:00Z",
                batch_size=2,
                delete_batch_size=2,
            )
            old = runtime.replace_feed(run_id="fetch_old", rows=rows)
            runtime.replace_feed(run_id="fetch_current", rows=[_row(99, 0)])
            progress: list[dict[str, object]] = []

            deleted = runtime.cleanup_old_generations(
                delete_cap=10,
                on_progress=progress.append,
            )

            increments: list[int] = []
            previous = 0
            for event in progress:
                raw_total = event.get("deletedRowCount")
                total = int(raw_total) if isinstance(raw_total, (int, str)) else 0
                if total > previous:
                    increments.append(total - previous)
                    previous = total

            assert deleted == 1
            assert increments
            assert max(increments) <= 2, increments
            assert sum(increments) == 9
            assert runtime.rows_for_generation(old.generation) == []
