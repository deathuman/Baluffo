"""The Jobs feed recovery lane must seed a completed pipeline whose feed is gone."""

import json
from pathlib import Path

import pytest

from src import packaged_desktop_smoke as smoke
from src.ship.jobs_first_run_state import (
    has_loadable_runtime_feed_artifacts,
    has_successful_runtime_jobs_report,
)
from src.storage.baluffo_store import BaluffoStore
from src.storage.task_runtime import TaskRuntimeStore

pytestmark = [pytest.mark.packaging]

FINISHED_AT = "2026-11-18T12:00:00+00:00"


def _pipeline_rows(data_dir: Path) -> list[dict[str, object]]:
    with BaluffoStore(data_dir) as store:
        rows = TaskRuntimeStore(store).recent_task_runs()
    return [row for row in rows if str(row.get("taskType") or "") == "pipeline"]


def test_jobs_feed_recovery_seed_leaves_a_successful_report_and_no_feed(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "runtime-data"

    smoke.packaged_smoke_orchestrator_mod._seed_jobs_feed_recovery_smoke_state(  # noqa: SLF001
        data_dir, finished_at=FINISHED_AT
    )

    assert has_loadable_runtime_feed_artifacts(data_dir) is False
    # The report alone is not servable, which is exactly what makes the page ask
    # for a rebuild instead of trusting the last successful run.
    assert has_successful_runtime_jobs_report(data_dir) is False
    report = json.loads((data_dir / "jobs-fetch-report.json").read_text(encoding="utf-8"))
    assert int(report["summary"]["outputCount"]) > 0
    assert report["finishedAt"] == FINISHED_AT
    sidecar = json.loads((data_dir / "jobs-fetch-report-summary.json").read_text(encoding="utf-8"))
    assert sidecar["runId"] == report["runId"]
    assert sidecar["finishedAt"] == report["finishedAt"]


def test_jobs_feed_recovery_seed_registers_a_completed_pipeline_row(tmp_path: Path) -> None:
    data_dir = tmp_path / "runtime-data"

    smoke.packaged_smoke_orchestrator_mod._seed_jobs_feed_recovery_smoke_state(  # noqa: SLF001
        data_dir, finished_at=FINISHED_AT
    )

    rows = _pipeline_rows(data_dir)
    assert len(rows) == 1
    assert rows[0]["runId"] == "packaged-smoke-pipeline-feed-lost"
    assert rows[0]["lifecycleStatus"] == "succeeded"
    assert rows[0]["finishedAt"] == FINISHED_AT


def test_jobs_feed_recovery_seed_removes_a_previously_written_feed(tmp_path: Path) -> None:
    data_dir = tmp_path / "runtime-data"
    data_dir.mkdir(parents=True)
    feed = json.dumps([{"id": "stale", "title": "Stale Seed Job"}])
    for name in ("jobs-unified-light.json", "jobs-unified.json"):
        (data_dir / name).write_text(feed, encoding="utf-8")

    smoke.packaged_smoke_orchestrator_mod._seed_jobs_feed_recovery_smoke_state(  # noqa: SLF001
        data_dir, finished_at=FINISHED_AT
    )

    assert not (data_dir / "jobs-unified-light.json").exists()
    assert not (data_dir / "jobs-unified.json").exists()
    assert has_loadable_runtime_feed_artifacts(data_dir) is False


def test_jobs_feed_recovery_lane_seeds_only_for_its_script(tmp_path: Path) -> None:
    orchestrator = smoke.packaged_smoke_orchestrator_mod
    seeded = tmp_path / "seeded"
    unseeded = tmp_path / "unseeded"

    assert orchestrator._seed_jobs_feed_recovery_smoke_state_if_needed(  # noqa: SLF001
        node_smoke_script=smoke.JOBS_FEED_RECOVERY_NODE_SMOKE_SCRIPT,
        jobs_feed_recovery_script=smoke.JOBS_FEED_RECOVERY_NODE_SMOKE_SCRIPT,
        data_dir=seeded,
        finished_at=FINISHED_AT,
    )
    assert (seeded / "jobs-fetch-report.json").is_file()
    assert (
        orchestrator._seed_jobs_feed_recovery_smoke_state_if_needed(  # noqa: SLF001
            node_smoke_script=smoke.DEFAULT_NODE_SMOKE_SCRIPT,
            jobs_feed_recovery_script=smoke.JOBS_FEED_RECOVERY_NODE_SMOKE_SCRIPT,
            data_dir=unseeded,
            finished_at=FINISHED_AT,
        )
        is False
    )
    assert not unseeded.exists()


def test_jobs_feed_recovery_lane_runs_the_controlled_bootstrap() -> None:
    # Without the smoke bootstrap mode the lane would queue a real Google Sheets
    # fetch and never reach the deterministic one-row promotion it asserts.
    assert (
        smoke.packaged_bootstrap_smoke_mode(smoke.JOBS_FEED_RECOVERY_NODE_SMOKE_SCRIPT)
        == "controlled-heartbeat-success"
    )
