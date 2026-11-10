"""Commit-phase concurrency coverage for the pipeline finalize lock scope.

Lives outside the terminal harness so that harness stays within the test line
budget; these tests are about lock scope, not report shape.
"""

from __future__ import annotations

import json
import threading
import traceback
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from src.jobs import pipeline_finalize
from src.jobs.pipeline_bootstrap import build_pipeline_paths
from src.jobs.state_lifecycle import write_job_lifecycle_state
from src.pipeline_io import write_atomic_if_changed, write_pipeline_rows_sidecar
from src.shared.json_io import read_json
from tests.helpers.jobs_rows import canonical_feed_row
from tests.test_pipeline_finalize_terminal_harness import _run_finalize


def test_finalize_pipeline_run_acquires_lock_for_authoritative_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen_paths: list[Path] = []
    real_lock = pipeline_finalize.jobs_feed_reconciliation_lock

    @contextmanager
    def commit_lock_spy(path: Path, **kwargs):
        seen_paths.append(Path(path))
        with real_lock(path, **kwargs):
            yield

    monkeypatch.setattr(pipeline_finalize, "jobs_feed_reconciliation_lock", commit_lock_spy)

    _report, _calls, paths, _progress_phase = _run_finalize(tmp_path, monkeypatch)

    assert seen_paths == [paths.output_dir]


def test_concurrent_direct_live_update_survives_long_finalize_and_reaches_committed_feed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A direct availability update during finalize compute must reach the feed.

    The pre-compute refresh runs without the reconciliation lock, so a long
    finalize can overlap a bridge availability check. The commit must reread
    lifecycle/feed state under the lock and publish the newer direct-live row.
    """
    paths = build_pipeline_paths(tmp_path)
    initial_lifecycle = {
        "job-1": {
            "status": "active",
            "availabilityId": "availability_1",
            "availabilityStatus": "unavailable",
            "availabilityCheckedAt": "2026-06-21T09:59:00+00:00",
            "jobLink": "https://example.com/jobs/1",
            "firstSeenAt": "2026-01-01T00:00:00+00:00",
            "lastSeenAt": "2026-06-21T09:00:00+00:00",
        }
    }
    write_job_lifecycle_state(paths.lifecycle_state_path, initial_lifecycle)

    compute_started = threading.Event()
    release_compute = threading.Event()

    def blocking_quality_reports(_rows: list[dict[str, Any]]) -> tuple[Any, ...]:
        compute_started.set()
        assert release_compute.wait(5.0)
        return ({"location": "ok"}, {"sector": "ok"}, {"contamination": "ok"}, {"city": "ok"})

    result: dict[str, Any] = {}
    errors: list[str] = []

    def run_finalize() -> None:
        try:
            result["finalize"] = _run_finalize(
                tmp_path,
                monkeypatch,
                deduped_rows=[],
                quality_reports=blocking_quality_reports,
                applied_lifecycle=initial_lifecycle,
            )
        except BaseException:
            errors.append(traceback.format_exc())

    worker = threading.Thread(target=run_finalize)
    worker.start()
    try:
        assert compute_started.wait(2.0)
        updated_lifecycle = {
            "job-1": {
                **initial_lifecycle["job-1"],
                "availabilityStatus": "available",
                "availabilityCheckedAt": "2026-06-21T10:00:01+00:00",
                "availabilityVerifiedAt": "2026-06-21T10:00:01+00:00",
                "availabilityEvidence": {
                    "kind": "direct_live",
                    "confidence": "definitive",
                    "checkedAt": "2026-06-21T10:00:01+00:00",
                    "source": "example.com",
                },
            }
        }
        write_job_lifecycle_state(paths.lifecycle_state_path, updated_lifecycle)
        concurrent_row = canonical_feed_row("availability_1")
        write_atomic_if_changed(paths.json_path, json.dumps([concurrent_row], ensure_ascii=False))
        write_pipeline_rows_sidecar(paths.json_path, [concurrent_row])
    finally:
        release_compute.set()
        worker.join(timeout=5.0)

    assert errors == [], errors[0]
    assert "finalize" in result
    _report, calls, committed_paths, _progress_phase = result["finalize"]
    assert committed_paths.json_path == paths.json_path

    committed_feed = read_json(paths.json_path, [])
    assert [row.get("availabilityId") for row in committed_feed] == ["availability_1"]
    committed_lifecycle = calls["lifecycle_state"][-1][1]
    assert committed_lifecycle["job-1"]["availabilityStatus"] == "available"
    assert committed_lifecycle["job-1"]["availabilityEvidence"]["kind"] == "direct_live"
