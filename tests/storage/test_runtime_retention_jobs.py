from __future__ import annotations

import pytest

from src.storage import BaluffoStore, JobRuntimeStore
from src.storage.runtime_retention import run_runtime_retention
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
            assert result["jobGenerations"]["deletedRowCount"] == 4
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
                for item in progress
            )
            assert result["deletedGenerationCount"] == 1
            assert result["jobGenerations"]["deletedGenerationCount"] == 1


def test_runtime_retention_marks_a_short_jobs_cleanup_as_drained() -> None:
    with workspace_tmpdir("runtime-retention-jobs-short") as data_dir:
        with BaluffoStore(data_dir) as store:
            generations = _replace_generations(store, 2)

            result = run_runtime_retention(store, reason="post-serve")

            assert result["deletedGenerationCount"] == 1
            assert result["jobGenerations"]["deletedGenerationCount"] == 1
            assert result["jobGenerations"]["truncated"] is False
            assert result["truncated"] is False
            remaining = store.execute_read("SELECT DISTINCT feed_generation FROM jobs")
            assert [row["feed_generation"] for row in remaining] == [generations[-1]]
