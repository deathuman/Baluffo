from __future__ import annotations

import threading
from typing import cast

import pytest

from src.storage import BaluffoStore, JobRuntimeStore
from src.storage.retention_cancel import RetentionCancelled
from src.storage.runtime_retention import preview_runtime_retention, run_runtime_retention
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


def test_runtime_retention_drains_old_jobs_generations_and_keeps_current() -> None:
    with workspace_tmpdir("runtime-retention-jobs-drain") as data_dir:
        with BaluffoStore(data_dir) as store:
            generations = _replace_generations(store, 5)

            result = run_runtime_retention(store, reason="post-serve")

            assert result["deletedGenerationCount"] == 4
            assert result["jobGenerations"]["deletedGenerationCount"] == 4
            assert result["jobGenerations"]["processedGenerationCount"] == 4
            assert result["jobGenerations"]["totalGenerationCount"] == 4
            assert result["jobGenerations"]["totalGenerationRowCount"] == 4
            assert result["jobGenerations"]["deletedGenerationRowCount"] == 4
            assert result["jobGenerations"]["deletedRowCount"] == 4
            assert result["jobGenerations"]["generationRowCountKnown"] is True
            assert result["jobGenerations"]["truncated"] is True
            assert result["truncated"] is True
            remaining = store.execute_read(
                "SELECT DISTINCT feed_generation FROM jobs ORDER BY feed_generation"
            )
            assert [row["feed_generation"] for row in remaining] == [generations[-1]]
            assert JobRuntimeStore(store).current_generation() == generations[-1]


def test_runtime_retention_real_pass_replaces_stale_nested_generation_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("runtime-retention-real-stale-jobs") as data_dir:
        with BaluffoStore(data_dir) as store:
            generations = _replace_generations(store, 3)
            monkeypatch.setattr(store, "schedule_compaction_if_needed", lambda **_: False)
            store._last_retention = {
                **store._last_retention,
                "deletedGenerationCount": 0,
                "jobGenerations": {
                    "status": "ok",
                    "deletedGenerationCount": 9,
                    "deletedRowCount": 18,
                    "truncated": True,
                },
                "truncated": True,
            }

            assert store._run_runtime_retention(reason="post-serve") is True
            if store._retention_timer is not None:
                store._retention_timer.cancel()

            final = store.runtime_retention_status()
            remaining = store.execute_read("SELECT DISTINCT feed_generation FROM jobs")

    assert final["status"] == "ok"
    assert final["phase"] == "complete"
    assert final["deletedGenerationCount"] == 2
    assert final["jobGenerations"]["deletedGenerationCount"] == 2
    assert final["jobGenerations"]["deletedRowCount"] == 2
    assert final["jobGenerations"]["truncated"] is False
    assert final["truncated"] is False
    assert [row["feed_generation"] for row in remaining] == [generations[-1]]


def test_runtime_retention_reports_live_generation_rows_while_deleting() -> None:
    with workspace_tmpdir("runtime-retention-jobs-live-progress") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-11-11T10:00:00Z")
            runtime.replace_feed(
                run_id="fetch_old",
                rows=[_row(index) for index in range(1001)],
            )
            runtime.replace_feed(run_id="fetch_current", rows=[_row(2000)])
            progress: list[dict[str, object]] = []
            run_runtime_retention(store, reason="post-serve", on_progress=progress.append)

            assert any(
                event.get("phase") == "prune-job-generations"
                and 0
                < int(cast(int, event.get("deletedGenerationRowCount") or 0))
                < int(cast(int, event.get("totalGenerationRowCount") or 0))
                for event in progress
            )
            generation_events = [
                event
                for event in progress
                if event.get("phase") == "prune-job-generations"
                and event.get("stage") == "deleting"
            ]
            assert generation_events
            assert all(
                int(cast(int, event.get("deletedGenerationRowCount") or 0))
                == int(cast(int, event.get("deletedRowCount") or 0))
                for event in generation_events
            )
            assert all(
                int(cast(int, event.get("deletedGenerationCount") or 0)) == 0
                or int(cast(int, event.get("deletedGenerationRowCount") or 0)) > 0
                for event in generation_events
            )
            assert any(
                int(cast(int, event.get("deletedGenerationCount") or 0)) > 0
                and int(cast(int, event.get("deletedGenerationRowCount") or 0)) > 0
                for event in generation_events
            )


def test_generation_cleanup_reports_live_generation_row_progress() -> None:
    with workspace_tmpdir("job-runtime-cleanup-live-progress") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T10:00:00Z",
                delete_batch_size=1,
            )
            runtime.replace_feed(run_id="fetch_old", rows=[_row(1), _row(2)])
            runtime.replace_feed(run_id="fetch_current", rows=[_row(3)])
            progress: list[dict[str, object]] = []

            runtime.cleanup_old_generations(delete_cap=10, on_progress=progress.append)

            assert any(
                event.get("stage") == "deleting"
                and 0 < int(cast(int, event.get("deletedGenerationRowCount") or 0)) < 2
                for event in progress
            )


def test_runtime_retention_reports_retry_as_scheduled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failed_retention(store, *, reason="", on_progress=None, cancel_requested=None):
        raise ValueError("simulated retention failure")

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", failed_retention)

    with workspace_tmpdir("runtime-retention-retry-state") as data_dir:
        with BaluffoStore(data_dir) as store:
            monkeypatch.setattr(store, "schedule_compaction_if_needed", lambda **_: False)
            assert store._run_runtime_retention(reason="post-serve") is True
            status = store.runtime_retention_status()
            if store._retention_timer is not None:
                store._retention_timer.cancel()

    assert status["status"] == "failed"
    assert status["retryScheduled"] is True
    assert status["followUpScheduled"] is False
    assert status["completionState"] == "retry-scheduled"
    assert status["fullyComplete"] is False


def test_runtime_retention_reports_phase_progress() -> None:
    with workspace_tmpdir("runtime-retention-jobs-progress") as data_dir:
        with BaluffoStore(data_dir) as store:
            _replace_generations(store, 2)
            progress: list[dict[str, object]] = []

            result = run_runtime_retention(
                store,
                reason="post-serve",
                on_progress=progress.append,
            )

            phases = [str(item.get("phase") or "") for item in progress]
            assert "prune-source-runs" in phases
            assert "compact-archived-payloads" in phases
            assert "prune-job-generations" in phases
            assert any(
                item.get("phase") == "prune-source-runs" and item.get("stage") == "deleting"
                for item in progress
            )
            assert any(
                item.get("phase") == "prune-job-generations"
                and item.get("stage") == "deleting"
                and item.get("deletedGenerationCount") == 1
                and item.get("totalGenerationCount") == 1
                and item.get("totalGenerationRowCount") == 1
                and item.get("generationRowCountKnown") is True
                for item in progress
            )
            assert result["deletedGenerationCount"] == 1
            assert result["jobGenerations"]["deletedGenerationCount"] == 1


def test_runtime_retention_counts_legacy_generation_rows_and_clears_stats() -> None:
    with workspace_tmpdir("runtime-retention-legacy-generation-stats") as data_dir:
        with BaluffoStore(data_dir) as store:
            generations = _replace_generations(store, 2)
            old = generations[0]
            store.write(
                lambda conn: conn.execute(
                    "DELETE FROM job_feed_generation_stats WHERE feed_generation = ?",
                    (old,),
                )
            )
            progress: list[dict[str, object]] = []

            result = run_runtime_retention(
                store,
                reason="post-serve",
                on_progress=progress.append,
            )

            assert any(
                item.get("stage") == "counting" and item.get("totalGenerationCount") == 1
                for item in progress
            )
            assert any(
                item.get("generationRowCountKnown") is True
                and item.get("totalGenerationRowCount") == 1
                for item in progress
            )
            assert result["jobGenerations"]["generationRowCountKnown"] is True
            assert result["jobGenerations"]["totalGenerationRowCount"] == 1
            assert (
                store.execute_scalar(
                    "SELECT COUNT(*) FROM job_feed_generation_stats WHERE feed_generation = ?",
                    (old,),
                )
                == 0
            )


def test_runtime_retention_marks_a_short_jobs_cleanup_as_drained() -> None:
    with workspace_tmpdir("runtime-retention-jobs-short") as data_dir:
        with BaluffoStore(data_dir) as store:
            generations = _replace_generations(store, 2)

            result = run_runtime_retention(store, reason="post-serve")

            assert result["deletedGenerationCount"] == 1
            assert result["jobGenerations"]["deletedGenerationCount"] == 1
            assert result["jobGenerations"]["totalGenerationCount"] == 1
            assert result["jobGenerations"]["truncated"] is False
            assert result["truncated"] is False
            remaining = store.execute_read("SELECT DISTINCT feed_generation FROM jobs")
            assert [row["feed_generation"] for row in remaining] == [generations[-1]]


def test_cleanup_preview_reports_candidates_without_writing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("runtime-retention-preview") as data_dir:
        with BaluffoStore(data_dir) as store:
            generations = _replace_generations(store, 4)
            runtime = JobRuntimeStore(store)

            def refuse_write(*_args, **_kwargs):
                raise AssertionError("cleanup preview must be read-only")

            monkeypatch.setattr(store, "write", refuse_write)
            preview = runtime.cleanup_preview()
            capped = runtime.cleanup_preview(delete_cap=2)

    assert preview["currentGeneration"] == generations[-1]
    assert preview["currentFeedProtected"] is True
    assert preview["currentJobRowCount"] == 1
    assert preview["currentSourceRowCount"] == 0
    assert preview["candidateGenerationCount"] == 3
    assert {row["generation"] for row in preview["candidates"]} == set(generations[:3])
    assert preview["candidateJobRowCount"] == 3
    assert preview["candidateSourceRowCount"] == 0
    assert preview["candidateRowCount"] == 3
    assert preview["estimatedBytes"] > 0
    assert preview["countsKnown"] is True
    assert preview["countsSource"] == "generation-stats"
    assert capped["candidateGenerationCount"] == 2
    assert capped["capReached"] is True


def test_cleanup_preview_uses_sample_bounds_when_count_budget_is_zero() -> None:
    with workspace_tmpdir("runtime-retention-preview-legacy") as data_dir:
        with BaluffoStore(data_dir) as store:
            _replace_generations(store, 3)
            store.write(lambda conn: conn.execute("DELETE FROM job_feed_generation_stats"))
            runtime = JobRuntimeStore(store)
            preview = runtime.cleanup_preview(count_budget_s=0)

    assert preview["countsKnown"] is False
    assert preview["countsSource"] == "generation-stats+sample-bounds"
    assert preview["candidateGenerationCount"] == 2
    assert preview["candidateRowCount"] == 2
    assert all(row["countsKnown"] is False for row in preview["candidates"])
    assert {row["jobRowCount"] for row in preview["candidates"]} == {1}


def test_preview_runtime_retention_reports_the_same_candidates_read_only() -> None:
    with workspace_tmpdir("runtime-retention-preview-wrapper") as data_dir:
        with BaluffoStore(data_dir) as store:
            generations = _replace_generations(store, 3)

            report = preview_runtime_retention(store)

            assert report["readOnly"] is True
            assert report["status"] == "ok"
            assert report["scope"] == "jobs-feed-generations"
            assert report["currentGeneration"] == generations[-1]
            assert report["currentFeedProtected"] is True
            assert report["candidateGenerationCount"] == 2
            assert {row["generation"] for row in report["candidates"]} == set(generations[:2])
            assert JobRuntimeStore(store).current_generation() == generations[-1]


def test_stale_run_token_does_not_cancel_a_newer_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = [threading.Event(), threading.Event()]
    release = [threading.Event(), threading.Event()]
    calls = 0

    def fake_retention(store, *, reason="", on_progress=None, cancel_requested=None):
        nonlocal calls
        index = calls
        calls += 1
        started[index].set()
        assert release[index].wait(5)
        if index == 1:
            assert cancel_requested() is True
            raise RetentionCancelled("newer pass cancelled")
        return {"status": "ok"}

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", fake_retention)

    with workspace_tmpdir("runtime-retention-stale-token") as data_dir:
        with BaluffoStore(data_dir) as store:
            monkeypatch.setattr(store, "schedule_compaction_if_needed", lambda **_: False)
            assert store.schedule_runtime_retention(reason="first", force=True)
            assert started[0].wait(2)
            first_worker = store._retention_thread
            assert first_worker is not None
            first_run_id = store.runtime_retention_status()["runId"]
            release[0].set()
            first_worker.join(5)

            assert store.schedule_runtime_retention(reason="second", force=True)
            assert started[1].wait(2)
            second_worker = store._retention_thread
            assert second_worker is not None
            second_run_id = store.runtime_retention_status()["runId"]
            assert second_run_id != first_run_id

            stale = store.cancel_runtime_retention(first_run_id)
            assert stale["requested"] is False
            assert stale["cancelling"] is False
            assert stale["stale"] is True
            assert stale["reason"] == "run-id-mismatch"
            assert stale["runtimeRetention"]["runId"] == second_run_id
            assert stale["runtimeRetention"]["status"] == "running"
            assert stale["runtimeRetention"]["cancelRequested"] is False

            missing = store.cancel_runtime_retention()
            assert missing["requested"] is False
            assert missing["stale"] is True
            assert missing["reason"] == "run-id-required"

            cancelled = store.cancel_runtime_retention(second_run_id)
            assert cancelled["requested"] is True
            assert cancelled["runtimeRetention"]["runId"] == second_run_id
            release[1].set()
            second_worker.join(5)
            final = store.runtime_retention_status()
            if store._retention_timer is not None:
                store._retention_timer.cancel()

    assert final["status"] == "cancelled"
    assert calls == 2
