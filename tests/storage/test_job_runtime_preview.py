from __future__ import annotations

import time

from src.storage import BaluffoStore, JobRuntimeStore
from tests.helpers.temp_paths import workspace_tmpdir


def _row(index: int) -> dict[str, object]:
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
        "firstSeenAt": "2026-11-11T10:00:00Z",
        "lastSeenAt": "2026-11-11T10:00:00Z",
        "sourceBundle": [],
    }


def _replace_generations(store: BaluffoStore, count: int) -> list[str]:
    runtime = JobRuntimeStore(store, now_iso=lambda: "2026-11-11T10:00:00Z")
    return [
        runtime.replace_feed(run_id=f"fetch_{index}", rows=[_row(index)]).generation
        for index in range(count)
    ]


def test_preview_counts_legacy_generations_within_its_budget() -> None:
    with workspace_tmpdir("preview-bounded-count") as data_dir:
        with BaluffoStore(data_dir) as store:
            _replace_generations(store, 3)
            store.write(lambda conn: conn.execute("DELETE FROM job_feed_generation_stats"))

            preview = JobRuntimeStore(store).cleanup_preview()

    assert preview["countsKnown"] is True
    assert preview["countsSource"] == "bounded-count"
    assert preview["candidateGenerationCount"] == 2
    assert preview["candidateRowCount"] == 2
    assert {row["jobRowCount"] for row in preview["candidates"]} == {1}
    assert all(row["countsKnown"] is True for row in preview["candidates"])


def test_bounded_read_interrupts_sql_and_leaves_the_read_connection_usable() -> None:
    with workspace_tmpdir("preview-bounded-read") as data_dir:
        with BaluffoStore(data_dir) as store:
            result = store.execute_read_with_deadline(
                """
                WITH RECURSIVE counter(value) AS (
                    SELECT 1
                    UNION ALL
                    SELECT value + 1 FROM counter WHERE value < 1000000000
                )
                SELECT COUNT(*) AS value FROM counter
                """,
                deadline=time.monotonic() + 0.02,
            )
            usable = store.execute_read("SELECT 1 AS ok")

    assert result is None
    assert usable == [{"ok": 1}]
