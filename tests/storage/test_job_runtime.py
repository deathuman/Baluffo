from __future__ import annotations

import pytest

from src.storage import BaluffoStore, JobRuntimeStore
from src.storage.job_runtime import jobs_feed_rows_hash
from tests.helpers.temp_paths import workspace_tmpdir


def _row(index: int, *, source_bundle: list[dict[str, object]] | None = None) -> dict[str, object]:
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
        "sourceBundle": list(source_bundle or []),
    }


def test_replace_feed_publishes_generation_and_preserves_order_and_source_bundle() -> None:
    rows = [
        _row(
            2,
            source_bundle=[
                {"sourceName": "A", "sourceJobId": "same"},
                {"sourceName": "A", "sourceJobId": "same"},
            ],
        ),
        _row(1),
    ]
    with workspace_tmpdir("job-runtime") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")

            summary = runtime.replace_feed(run_id="fetch_abc", rows=rows)

            assert summary.published is True
            assert summary.row_count == 2
            assert summary.row_hash == jobs_feed_rows_hash(rows)
            assert runtime.current_rows() == rows
            assert runtime.current_summary() == {
                "generation": summary.generation,
                "runId": "fetch_abc",
                "rowCount": 2,
                "rowHash": jobs_feed_rows_hash(rows),
                "sourceCount": 2,
                "sourceHash": summary.source_hash,
                "publishedAt": "2026-05-12T10:00:00Z",
                "updatedAt": "2026-05-12T10:00:00Z",
            }


def test_staged_generation_is_invisible_until_published() -> None:
    first_rows = [_row(1)]
    staged_rows = [_row(2)]
    with workspace_tmpdir("job-runtime-stage") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")
            first = runtime.replace_feed(run_id="fetch_first", rows=first_rows)

            staged = runtime.stage_feed(run_id="fetch_second", rows=staged_rows)

            assert staged.published is False
            assert runtime.current_summary()["generation"] == first.generation
            assert runtime.current_rows() == first_rows

            runtime.publish_generation(
                staged.generation,
                expected_row_count=staged.row_count,
                expected_row_hash=staged.row_hash,
            )

            assert runtime.current_rows() == staged_rows


def test_publish_rejects_generation_hash_mismatch() -> None:
    with workspace_tmpdir("job-runtime-hash-mismatch") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")
            staged = runtime.stage_feed(run_id="fetch_second", rows=[_row(2)])

            with pytest.raises(ValueError, match="hash mismatch"):
                runtime.publish_generation(staged.generation, expected_row_hash="wrong")


def test_cleanup_old_generations_keeps_current_generation() -> None:
    with workspace_tmpdir("job-runtime-cleanup") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")
            old = runtime.replace_feed(run_id="fetch_old", rows=[_row(1)])
            current = runtime.replace_feed(run_id="fetch_current", rows=[_row(2)])

            deleted = runtime.cleanup_old_generations(delete_cap=10)

            assert deleted == 1
            assert runtime.current_summary()["generation"] == current.generation
            assert runtime.rows_for_generation(current.generation) == [_row(2)]
            assert runtime.rows_for_generation(old.generation) == []
