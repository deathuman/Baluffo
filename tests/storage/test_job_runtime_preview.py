from __future__ import annotations

import time
from typing import Any

import pytest

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
            generations = _replace_generations(store, 3)
            store.write(lambda conn: conn.execute("DELETE FROM job_feed_generation_stats"))

            preview = JobRuntimeStore(store).cleanup_preview()
            actual_history_bytes = store.execute_read(
                """
                SELECT COALESCE(SUM(length(CAST(payload_json AS BLOB))), 0) AS total
                FROM jobs
                WHERE feed_generation != ?
                """,
                (generations[-1],),
            )[0]["total"]

    assert preview["countsKnown"] is True
    assert preview["countsSource"] == "bounded-count"
    assert preview["candidateGenerationCount"] == 2
    assert preview["candidateRowCount"] == 2
    assert {row["jobRowCount"] for row in preview["candidates"]} == {1}
    assert all(row["countsKnown"] is True for row in preview["candidates"])
    # A fully sampled generation is measured, not extrapolated, so the estimate
    # equals the payload it claims and cannot exceed it.
    assert preview["historicalGenerationEstimatedBytes"] == actual_history_bytes


def test_preview_reports_current_and_historical_payload_estimates() -> None:
    with workspace_tmpdir("preview-reclaim-fields") as data_dir:
        with BaluffoStore(data_dir) as store:
            generations = _replace_generations(store, 3)
            preview = JobRuntimeStore(store).cleanup_preview()
            freelist_bytes = store.file_sizes()["freelistBytes"]

    assert preview["currentGeneration"] == generations[-1]
    assert preview["currentGenerationEstimatedBytes"] > 0
    assert preview["historicalGenerationEstimatedBytes"] > 0
    assert preview["historicalGenerationCount"] == 2
    assert preview["estimatedReclaimableBytes"] == (
        preview["historicalGenerationEstimatedBytes"] + freelist_bytes
    )


def test_preview_history_sample_ignores_the_delete_cap() -> None:
    with workspace_tmpdir("preview-reclaim-beyond-cap") as data_dir:
        with BaluffoStore(data_dir) as store:
            _replace_generations(store, 6)
            preview = JobRuntimeStore(store).cleanup_preview(delete_cap=1)

    # The cap bounds the candidate list; it must not bound the history estimate,
    # because the cap is exactly what made the count misleading on real backlogs.
    assert preview["deleteCap"] == 1
    assert preview["candidateGenerationCount"] == 1
    assert preview["capReached"] is True
    assert preview["historicalGenerationCount"] == 5
    assert preview["historicalGenerationEstimatedBytes"] > 0


def test_preview_history_sample_is_query_bounded_and_reuses_candidate_samples(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("preview-reclaim-bounded-sample") as data_dir:
        with BaluffoStore(data_dir) as store:
            _replace_generations(store, 7)
            statements: list[str] = []
            execute_read = store.execute_read

            def recording_read(sql: str, parameters: Any = None) -> Any:
                statements.append(sql)
                return execute_read(sql) if parameters is None else execute_read(sql, parameters)

            monkeypatch.setattr(store, "execute_read", recording_read)
            preview = JobRuntimeStore(store).cleanup_preview()

    sample_queries = [sql for sql in statements if "AS sample_count" in sql]
    generation_queries = [
        sql
        for sql in statements
        if "GROUP BY feed_generation" in sql and "ORDER BY feed_generation ASC" in sql
    ]

    assert preview["candidateGenerationCount"] == 4
    assert preview["historicalGenerationCount"] == 6
    # Two tables per generation: four candidates, the two generations outside the
    # cap, and the current one. The candidate generations are not sampled twice.
    assert len(sample_queries) == 14
    # Candidates and history are prefixes of one listing, so a single bounded
    # GROUP BY serves both and no query covers a whole backlog.
    assert len(generation_queries) == 1
    assert "LIMIT ?" in generation_queries[0]


def test_preview_history_stays_outside_the_keep_window() -> None:
    with workspace_tmpdir("preview-reclaim-keep-window") as data_dir:
        with BaluffoStore(data_dir) as store:
            _replace_generations(store, 6)
            preview = JobRuntimeStore(store).cleanup_preview(keep_generations=2)

    # Keep-two retention protects the current generation and one more; history
    # must describe only what retention would actually delete.
    assert preview["candidateGenerationCount"] == 4
    assert preview["historicalGenerationCount"] == 4
    assert preview["historicalGenerationEstimatedBytes"] > 0


def test_preview_reports_no_history_when_only_the_current_feed_exists() -> None:
    with workspace_tmpdir("preview-reclaim-current-only") as data_dir:
        with BaluffoStore(data_dir) as store:
            _replace_generations(store, 1)
            preview = JobRuntimeStore(store).cleanup_preview()
            freelist_bytes = store.file_sizes()["freelistBytes"]

    assert preview["currentGenerationEstimatedBytes"] > 0
    assert preview["historicalGenerationEstimatedBytes"] == 0
    assert preview["historicalGenerationCount"] == 0
    assert preview["estimatedReclaimableBytes"] == freelist_bytes


def test_preview_without_a_current_generation_reports_no_reclaim_estimate() -> None:
    with workspace_tmpdir("preview-reclaim-empty") as data_dir:
        with BaluffoStore(data_dir) as store:
            preview = JobRuntimeStore(store).cleanup_preview()

    assert preview["currentGenerationEstimatedBytes"] == 0
    assert preview["historicalGenerationEstimatedBytes"] == 0
    assert preview["historicalGenerationCount"] == 0
    assert preview["estimatedReclaimableBytes"] == 0


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
