from __future__ import annotations

import json
from pathlib import Path

from tests.helpers.temp_paths import workspace_tmpdir
from tools.measurements.pipeline import generation_cleanup_profile as profile


def test_fixed_fixture_rows_are_deterministic_and_self_contained() -> None:
    rows = profile.build_fixture_rows(job_count=2, sources_per_job=2)
    same_rows = profile.build_fixture_rows(job_count=2, sources_per_job=2)

    assert rows == same_rows
    assert profile.fixture_rows_sha256(rows) == profile.fixture_rows_sha256(same_rows)
    assert rows[0]["source"] == "benchmark_fixture"
    assert rows[0]["company"] == "Benchmark Studio"
    assert rows[0]["jobLink"] == "https://bench.invalid/jobs/0"
    assert len(rows[0]["sourceBundle"]) == 2
    assert rows[0]["sourceBundleCount"] == 2
    assert rows[1]["id"] == 2
    assert rows[1]["sourceBundle"][0]["sourceId"] == "benchmark_source_0"
    assert "Paradox Interactive" not in json.dumps(rows)


def test_fixture_rows_extra_payload_is_explicit_and_deterministic() -> None:
    baseline = profile.build_fixture_rows(job_count=1, sources_per_job=1)
    large = profile.build_fixture_rows(
        job_count=1,
        sources_per_job=1,
        extra_payload_bytes=4096,
    )

    assert len(large[0]["description"]) == 4096
    assert len(large[0]["description"]) > len(baseline[0]["description"])
    assert large[0]["sourceBundle"] == baseline[0]["sourceBundle"]


def test_build_database_seeds_old_and_current_generations() -> None:
    with workspace_tmpdir("generation-cleanup-fixture") as data_dir:
        metadata = profile.build_database(
            data_dir / "fixture",
            job_count=3,
            sources_per_job=2,
            extra_payload_bytes=0,
            log=lambda _message: None,
        )

        assert metadata["fixtureVersion"] == profile.FIXTURE_VERSION
        assert metadata["oldGeneration"] == profile.FIXTURE_OLD_GENERATION
        assert metadata["currentGeneration"] == profile.FIXTURE_CURRENT_GENERATION
        assert metadata["expectedOldRows"] == 9
        assert metadata["tableCounts"] == {
            "jobs": 4,
            "job_sources": 7,
            "job_feed_generation_stats": 2,
        }
        assert metadata["fixtureRowsSha256"]


def test_run_arm_measures_the_real_cleanup_path() -> None:
    with workspace_tmpdir("generation-cleanup-arm") as data_dir:
        metadata = profile.build_database(
            data_dir / "fixture",
            job_count=3,
            sources_per_job=2,
            extra_payload_bytes=0,
            log=lambda _message: None,
        )
        arm = profile.run_arm(
            arm_name="baseline",
            base_db=Path(metadata["databasePath"]),
            work_dir=data_dir / "arm",
            disable_auto_checkpoint=False,
            delete_batch_size=2,
            expected_deleted_rows=9,
            log=lambda _message: None,
        )

        assert arm["deletedGenerations"] == 1
        assert arm["deletedRows"] == 9
        assert arm["deletedRowsMatch"] is True
        assert arm["remainingCounts"] == {"jobs": 1, "job_sources": 1}
        assert arm["batchCount"] >= 5
        assert all(
            {
                "deleteSeconds",
                "commitSeconds",
                "walBytesBefore",
                "walBytesAfter",
                "walGrowthBytes",
            }
            <= set(row)
            for row in arm["batches"]
        )
        assert arm["checkpoints"][-1]["mode"] == "TRUNCATE"
        assert arm["checkpoints"][-1]["walBytesAfter"] == 0

        summary = profile.summarize_arm(arm)
        assert summary["deletedRows"] == 9
        assert summary["byTable"]["job_sources"]["deletedRows"] == 6
        assert summary["byTable"]["jobs"]["deletedRows"] == 3
        assert summary["batchSeconds"]["max"] >= 0


def test_main_writes_artifacts_and_removes_fixture_databases() -> None:
    with workspace_tmpdir("generation-cleanup-main") as data_dir:
        output_root = data_dir / "runs"
        exit_code = profile.main(
            [
                "--output-root",
                str(output_root),
                "--job-count",
                "3",
                "--sources-per-job",
                "2",
                "--delete-batch-size",
                "2",
                "--arms",
                "baseline",
            ]
        )

        assert exit_code == 0
        run_dirs = [path for path in output_root.iterdir() if path.is_dir()]
        assert len(run_dirs) == 1
        run_dir = run_dirs[0]
        results = json.loads((run_dir / "results.json").read_text(encoding="utf-8"))

        assert results["fixtureVersion"] == profile.FIXTURE_VERSION
        assert results["config"]["jobCount"] == 3
        assert results["summaries"]["baseline"]["deletedRows"] == 9
        assert results["fixturesRemoved"] is True
        assert not (run_dir / "fixtures").exists()
        assert not (run_dir / "arms").exists()
        assert (run_dir / "SUMMARY.md").is_file()
