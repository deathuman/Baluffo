from __future__ import annotations

import pytest

from src.storage import BaluffoStore, JobRuntimeStore
from src.storage.job_runtime import (
    _JOB_SOURCES_BATCH_DELETE_SQL,
    _JOBS_BATCH_DELETE_SQL,
    _base_job_key,
    _source_identity,
    jobs_feed_rows_hash,
)
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


def test_source_identity_rows_needs_a_published_generation() -> None:
    with workspace_tmpdir("job-runtime-source-rows-empty") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")

            assert runtime.source_identity_rows({"greenhouse:siei"}) is None


def test_source_identity_rows_filters_to_the_requested_sources() -> None:
    rows = [
        _row(
            1,
            source_bundle=[
                {"source": "greenhouse:siei", "sourceJobId": "greenhouse:siei:100001"},
            ],
        ),
        _row(
            2,
            source_bundle=[
                {"source": "greenhouse:pdi", "sourceJobId": "greenhouse:pdi:200001"},
                {"source": "greenhouse:haven", "sourceJobId": "greenhouse:haven:400001"},
            ],
        ),
        _row(
            3,
            source_bundle=[
                {"source": "greenhouse:naughtydog", "sourceJobId": "gh:300001"},
            ],
        ),
    ]
    with workspace_tmpdir("job-runtime-source-rows") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")
            runtime.replace_feed(run_id="fetch_sources", rows=rows)

            selected = runtime.source_identity_rows({"GREENHOUSE:SIEI", "greenhouse:pdi"})

            assert selected is not None
            assert {row["sourceJobId"] for row in selected} == {
                "greenhouse:siei:100001",
                "greenhouse:pdi:200001",
            }
            assert runtime.source_identity_rows(set()) == []


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


def test_cleanup_old_generations_batches_rows_and_reports_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("job-runtime-cleanup-batches") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T10:00:00Z",
                batch_size=1,
                delete_batch_size=1,
            )
            bundle: list[dict[str, object]] = [{"sourceName": "Source", "sourceJobId": "id-1"}]
            old = runtime.replace_feed(
                run_id="fetch_old",
                rows=[
                    _row(1, source_bundle=bundle),
                    _row(2, source_bundle=bundle),
                ],
            )
            current = runtime.replace_feed(run_id="fetch_current", rows=[_row(3)])
            write_calls = 0
            original_write = store.write

            def counting_write(callback):
                nonlocal write_calls
                write_calls += 1
                return original_write(callback)

            monkeypatch.setattr(store, "write", counting_write)
            progress: list[dict[str, object]] = []

            deleted = runtime.cleanup_old_generations(
                delete_cap=10,
                on_progress=progress.append,
            )

            assert deleted == 1
            assert write_calls >= 6
            assert progress[-1] == {
                "deletedGenerationCount": 1,
                "deletedRowCount": 4,
                "generation": old.generation,
            }
            assert runtime.rows_for_generation(old.generation) == []
            assert runtime.rows_for_generation(current.generation) == [_row(3)]


def test_generation_cleanup_batch_plan_uses_feed_generation_indexes() -> None:
    with workspace_tmpdir("job-runtime-cleanup-plan") as data_dir:
        with BaluffoStore(data_dir) as store:

            def seed(conn) -> None:
                conn.executemany(
                    "INSERT INTO jobs(feed_generation, job_key, row_ordinal) VALUES (?, ?, ?)",
                    [
                        (generation, f"job_{generation}_{index}", index)
                        for generation in ("old", "current")
                        for index in range(5000)
                    ],
                )
                conn.executemany(
                    "INSERT INTO job_sources(feed_generation, job_key, source_ordinal) "
                    "VALUES (?, ?, 0)",
                    [
                        (generation, f"job_{generation}_{index}")
                        for generation in ("old", "current")
                        for index in range(5000)
                    ],
                )

            store.write(seed)
            store.write(lambda conn: conn.execute("ANALYZE"))

            cases = (
                (
                    _JOB_SOURCES_BATCH_DELETE_SQL,
                    ("old", 500),
                    (
                        "INTEGER PRIMARY KEY (rowid=?)",
                        "idx_job_sources_generation_job (feed_generation=?)",
                    ),
                ),
                (
                    _JOBS_BATCH_DELETE_SQL,
                    ("old", "old", 500),
                    (
                        "idx_jobs_generation_ordinal (feed_generation=? AND row_ordinal=?)",
                        "idx_jobs_generation_ordinal (feed_generation=?)",
                    ),
                ),
            )
            for statement, parameters, expected_fragments in cases:
                plan = store.execute_read(
                    "EXPLAIN QUERY PLAN " + statement,
                    parameters,
                )
                details = [str(row.get("detail") or "") for row in plan]
                plan_text = "\n".join(details)

                assert all(fragment in plan_text for fragment in expected_fragments), plan_text
                assert not any(
                    detail.startswith("SCAN") or " SCAN " in detail for detail in details
                ), plan_text


def test_cleanup_old_generations_keep_generations_and_unconstrained() -> None:
    bundle: list[dict[str, object]] = [{"sourceName": "Source", "sourceJobId": "id-1"}]
    with workspace_tmpdir("job-runtime-cleanup-multi") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")
            g1 = runtime.replace_feed(run_id="run_1", rows=[_row(1, source_bundle=bundle)])
            g2 = runtime.replace_feed(run_id="run_2", rows=[_row(2, source_bundle=bundle)])
            g3 = runtime.replace_feed(run_id="run_3", rows=[_row(3, source_bundle=bundle)])
            g4 = runtime.replace_feed(run_id="run_4", rows=[_row(4, source_bundle=bundle)])
            g5 = runtime.replace_feed(run_id="run_5", rows=[_row(5, source_bundle=bundle)])

            # Keep 2 generations unconstrained
            deleted = runtime.cleanup_old_generations(keep_generations=2, delete_cap=None)
            assert deleted == 3

            remaining_jobs = store.execute_read(
                "SELECT DISTINCT feed_generation FROM jobs ORDER BY feed_generation"
            )
            assert len(remaining_jobs) == 2
            assert g5.generation in [r["feed_generation"] for r in remaining_jobs]
            assert g4.generation in [r["feed_generation"] for r in remaining_jobs]

            # Sources for g1, g2, g3 should be completely gone
            remaining_sources = store.execute_read(
                "SELECT DISTINCT feed_generation FROM job_sources ORDER BY feed_generation"
            )
            assert len(remaining_sources) == 2

            # Now clean down to keep_generations=1
            deleted_last = runtime.cleanup_old_generations(keep_generations=1, delete_cap=None)
            assert deleted_last == 1
            final_jobs = store.execute_read("SELECT DISTINCT feed_generation FROM jobs")
            assert len(final_jobs) == 1
            assert final_jobs[0]["feed_generation"] == g5.generation


def test_job_identity_fallbacks_are_stable_for_sparse_rows() -> None:
    assert _base_job_key({"dedupKey": "dedup-a", "jobLink": "https://example.test/1"}, 0) == (
        _base_job_key({"dedupKey": "dedup-a"}, 99)
    )
    assert _base_job_key({"jobLink": "https://example.test/1"}, 0) == _base_job_key(
        {"jobLink": "https://example.test/1", "id": "ignored"},
        99,
    )
    assert _base_job_key({"source": "Source", "sourceJobId": "job-1"}, 0) == _base_job_key(
        {"source": "Source", "sourceJobId": "job-1"},
        99,
    )
    assert _base_job_key({}, 0) != _base_job_key({}, 1)
    assert _source_identity({}, 4) == "source:4"


def test_stage_feed_normalizes_source_bundle_json_and_empty_inputs() -> None:
    with workspace_tmpdir("job-runtime-source-bundle-json") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")

            empty = runtime.stage_feed(run_id="fetch_empty", rows=[])
            assert empty.row_count == 0
            assert empty.source_count == 0
            assert runtime.current_rows() == []

            summary = runtime.stage_feed(
                run_id="fetch_json_bundle",
                rows=[
                    {
                        **_row(3),
                        "sourceBundle": '[{"source": "Fallback", "sourceJobId": "fallback-1"}]',
                    }
                ],
            )

            rows = runtime.rows_for_generation(summary.generation)
            assert summary.source_count == 1
            assert rows[0]["sourceBundle"] == [{"source": "Fallback", "sourceJobId": "fallback-1"}]


def test_job_runtime_empty_summary_and_publish_guards() -> None:
    with workspace_tmpdir("job-runtime-empty-summary") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")

            assert runtime.current_generation() == ""
            assert runtime.current_rows() == []
            assert runtime.current_summary() == {
                "generation": "",
                "runId": "",
                "rowCount": 0,
                "rowHash": "",
                "sourceCount": 0,
                "sourceHash": "",
                "publishedAt": "",
                "updatedAt": "",
            }
            assert runtime.rows_for_generation("") == []
            assert runtime.parity_hash() == jobs_feed_rows_hash([])

            with pytest.raises(ValueError, match="requires a generation"):
                runtime.publish_generation("")


def test_publish_rejects_row_count_mismatch_and_cleanup_respects_delete_cap() -> None:
    with workspace_tmpdir("job-runtime-count-mismatch") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")
            old = runtime.replace_feed(run_id="fetch_old", rows=[_row(1)])
            staged = runtime.stage_feed(run_id="fetch_second", rows=[_row(2)])

            with pytest.raises(ValueError, match="row count mismatch"):
                runtime.publish_generation(staged.generation, expected_row_count=2)

            assert runtime.cleanup_old_generations(delete_cap=0) == 0
            assert runtime.rows_for_generation(old.generation) == [_row(1)]
            assert runtime.parity_hash([_row(2)]) == jobs_feed_rows_hash([_row(2)])
