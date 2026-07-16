import errno
import json
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

import src.bridge.job_availability_service as availability_service_module
import src.jobs.feed_reconciliation_lock as feed_lock_module
from src.bridge.job_availability_service import JobAvailabilityService
from src.bridge.task_launch_jobs_feed import (
    JobsFeedContext,
    jobs_feed_reconciliation_transaction,
    mirror_jobs_feed_rows,
    reconcile_jobs_feed_availability,
    rollback_jobs_feed_reconciliation,
)
from src.jobs.models import CanonicalJob
from src.jobs.pipeline_finalize import _merge_concurrent_direct_live_rows
from src.jobs.state_lifecycle import read_job_lifecycle_state, write_job_lifecycle_state
from src.pipeline_io import write_atomic_if_changed
from src.shared.json_io import read_json
from src.storage import BaluffoStore, JobRuntimeStore


def _canonical_feed_row(availability_id: str = "availability_1") -> dict:
    return {
        "id": "job-1",
        "title": "Engine Programmer",
        "company": "Studio",
        "city": "Rome",
        "country": "Italy",
        "workType": "Hybrid",
        "contractType": "Full-time",
        "jobLink": "https://example.com/jobs/1",
        "sector": "Games",
        "profession": "engine-programmer",
        "source": "fixture",
        "sourceJobId": "1",
        "availabilityId": availability_id,
        "availabilityStatus": "available",
        "sourceBundle": [],
    }


def _wait_for_terminal_status(service: JobAvailabilityService, run_id: str) -> dict:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        status = service.status(run_id)
        if status.get("status") != "running":
            return status
        time.sleep(0.001)
    raise AssertionError(f"availability task {run_id} did not finish")


def test_pipeline_finalization_merges_only_concurrent_definitive_direct_reopens() -> None:
    reopened = _canonical_feed_row("availability_reopened")
    unrelated = {**_canonical_feed_row("availability_unrelated"), "id": "job-2"}
    merged = _merge_concurrent_direct_live_rows(
        [],
        [reopened, unrelated],
        {
            "reopened": {
                "availabilityId": "availability_reopened",
                "availabilityStatus": "available",
                "availabilityEvidence": {
                    "kind": "direct_live",
                    "confidence": "definitive",
                },
            },
            "unrelated": {
                "availabilityId": "availability_unrelated",
                "availabilityStatus": "available",
                "availabilityEvidence": {
                    "kind": "source_present",
                    "confidence": "definitive",
                },
            },
        },
    )

    assert all(isinstance(row, CanonicalJob) for row in merged)
    assert [row.availabilityId for row in merged] == ["availability_reopened"]


def test_direct_rotation_checkpoint_does_not_regress_to_stale_evidence(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    service = JobAvailabilityService(data_dir=data_dir, local_store_factory=lambda: None)
    service._record_direct_checkpoint(
        "availability_1",
        {"checkedAt": "2026-07-14T12:00:00+00:00", "kind": "direct_live"},
        "2026-07-14T12:00:00+00:00",
    )
    service._record_direct_checkpoint(
        "availability_1",
        {"checkedAt": "2026-07-14T11:00:00+00:00", "kind": "direct_closed"},
        "2026-07-14T12:01:00+00:00",
    )

    checkpoint = read_json(data_dir / "jobs-availability-direct-checkpoints.json", {})["rows"][0]
    assert checkpoint["checkedAt"] == "2026-07-14T12:00:00+00:00"
    assert checkpoint["kind"] == "direct_live"


def test_pipeline_mirror_waits_for_complete_direct_transition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    write_job_lifecycle_state(
        data_dir / "jobs-lifecycle-state.json",
        {
            "job-1": {
                "status": "active",
                "availabilityId": "availability_1",
                "availabilityStatus": "available",
                "jobLink": "https://example.com/jobs/1",
            }
        },
    )
    (data_dir / "jobs-unified.json").write_text(
        json.dumps([_canonical_feed_row()]), encoding="utf-8"
    )
    with BaluffoStore(data_dir) as storage:
        storage.set_authority_mode("jobsFeed", "sqlite", reason="test-cutover")
        JobRuntimeStore(storage).replace_feed(run_id="seed", rows=[_canonical_feed_row()])

    entered_lifecycle_write = threading.Event()
    release_lifecycle_write = threading.Event()
    real_write = write_job_lifecycle_state
    blocked = False

    def block_transition_write(path: Path, rows: dict) -> None:
        nonlocal blocked
        if not blocked and any(
            str(entry.get("availabilityStatus") or "") == "unavailable" for entry in rows.values()
        ):
            blocked = True
            entered_lifecycle_write.set()
            assert release_lifecycle_write.wait(2.0)
        real_write(path, rows)

    monkeypatch.setattr(
        availability_service_module, "write_job_lifecycle_state", block_transition_write
    )

    class Validator:
        def check(self, _url: str):
            return {
                "kind": "direct_closed",
                "confidence": "definitive",
                "checkedAt": "2026-07-10T10:00:00+00:00",
                "source": "example.com",
                "httpStatus": 410,
            }

    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=lambda: None,
        validator=Validator(),
        enforce_direct=True,
    )
    started = service.start({"availabilityId": "availability_1"})
    assert entered_lifecycle_write.wait(1.0)

    pipeline_row = _canonical_feed_row("availability_pipeline")
    pipeline_row["id"] = "job-pipeline"
    pipeline_row["jobLink"] = "https://example.com/jobs/pipeline"
    write_atomic_if_changed(data_dir / "jobs-unified.json", json.dumps([pipeline_row]))
    context = JobsFeedContext(
        data_dir=data_dir,
        jobs_fetch_report=data_dir / "jobs-fetch-report.json",
        now_iso=lambda: "2026-07-10T10:01:00+00:00",
        bridge_log=lambda *_args, **_kwargs: None,
        save_json_atomic=lambda _path, _payload: None,
    )
    mirror_result: list[bool] = []
    mirror_thread = threading.Thread(
        target=lambda: mirror_result.append(
            mirror_jobs_feed_rows(context, {"runId": "pipeline_new"})
        )
    )
    mirror_thread.start()
    time.sleep(0.05)
    assert mirror_result == []

    release_lifecycle_write.set()
    mirror_thread.join(timeout=2.0)
    assert not mirror_thread.is_alive()
    assert _wait_for_terminal_status(service, started["runId"])["status"] == "succeeded"
    assert mirror_result == [True]
    assert read_json(data_dir / "jobs-unified.json", [])[0]["availabilityId"] == (
        "availability_pipeline"
    )
    with BaluffoStore(data_dir) as storage:
        assert (
            JobRuntimeStore(storage).current_rows()[0]["availabilityId"] == "availability_pipeline"
        )


def test_direct_transition_reloads_lifecycle_after_waiting_for_pipeline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    state_path = data_dir / "jobs-lifecycle-state.json"
    write_job_lifecycle_state(
        state_path,
        {
            "job-1": {
                "status": "active",
                "availabilityId": "availability_1",
                "availabilityStatus": "available",
                "jobLink": "https://example.com/jobs/1",
            }
        },
    )

    class Validator:
        def check(self, _url: str):
            return {
                "kind": "direct_closed",
                "confidence": "definitive",
                "checkedAt": "2026-07-10T10:00:00+00:00",
                "source": "example.com",
                "httpStatus": 410,
            }

    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=lambda: None,
        validator=Validator(),
        enforce_direct=True,
    )
    monkeypatch.setattr(service, "_rewrite_feeds", lambda *_args, **_kwargs: (None, None))
    reached_pre_transaction_read = threading.Event()
    real_apply = service._apply_checked_evidence

    def signal_pre_transaction_read(**kwargs):
        reached_pre_transaction_read.set()
        return real_apply(**kwargs)

    monkeypatch.setattr(service, "_apply_checked_evidence", signal_pre_transaction_read)

    with jobs_feed_reconciliation_transaction(data_dir):
        started = service.start({"availabilityId": "availability_1"})
        assert reached_pre_transaction_read.wait(1.0)
        pipeline_lifecycle = read_job_lifecycle_state(state_path)
        pipeline_lifecycle["job-from-pipeline"] = {
            "status": "active",
            "availabilityId": "availability_pipeline",
            "availabilityStatus": "available",
            "jobLink": "https://example.com/jobs/pipeline",
        }
        write_job_lifecycle_state(state_path, pipeline_lifecycle)

    assert _wait_for_terminal_status(service, started["runId"])["status"] == "succeeded"
    final_lifecycle = read_job_lifecycle_state(state_path)
    assert "job-from-pipeline" in final_lifecycle
    assert final_lifecycle["job-1"]["availabilityStatus"] == "unavailable"


def test_gzip_feed_snapshot_restores_previous_projection(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    initial_row = _canonical_feed_row()
    write_atomic_if_changed(data_dir / "jobs-unified.json", json.dumps([initial_row]))
    context = JobsFeedContext(
        data_dir=data_dir,
        jobs_fetch_report=data_dir / "jobs-fetch-report.json",
        now_iso=lambda: "2026-07-10T10:00:00+00:00",
        bridge_log=lambda *_args, **_kwargs: None,
        save_json_atomic=lambda _path, _payload: None,
        job_runtime_store_factory=lambda: None,
    )
    snapshot = reconcile_jobs_feed_availability(
        context,
        availability_id="availability_1",
        entry={
            **initial_row,
            "availabilityStatus": "unavailable",
            "availabilityUnavailableAt": "2026-07-10T10:00:00+00:00",
        },
    )
    assert snapshot is not None
    assert read_json(data_dir / "jobs-unified.json", None) == []

    rollback_jobs_feed_reconciliation(context, snapshot)

    assert read_json(data_dir / "jobs-unified.json", None) == [initial_row]
    assert (data_dir / "jobs-unified.json.gz").exists()
    assert not (data_dir / "jobs-unified.json").exists()


def test_sqlite_authority_without_generation_fails_closed(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    initial_row = _canonical_feed_row()
    private_feed = json.dumps([initial_row])
    (data_dir / "jobs-unified.json").write_text(private_feed, encoding="utf-8")

    with BaluffoStore(data_dir) as storage:
        storage.set_authority_mode("jobsFeed", "sqlite", reason="test-empty-authority")
        runtime = JobRuntimeStore(storage)
        context = JobsFeedContext(
            data_dir=data_dir,
            jobs_fetch_report=data_dir / "jobs-fetch-report.json",
            now_iso=lambda: "2026-07-10T10:00:00+00:00",
            bridge_log=lambda *_args, **_kwargs: None,
            save_json_atomic=lambda _path, _payload: None,
            job_runtime_store_factory=lambda: runtime,
        )

        snapshot = reconcile_jobs_feed_availability(
            context,
            availability_id="availability_1",
            entry={**initial_row, "availabilityStatus": "unavailable"},
        )

        assert snapshot is None
        assert runtime.current_generation() == ""
        assert runtime.current_rows() == []
        assert storage.get_authority_modes()["jobsFeed"] == "sqlite"
    assert (data_dir / "jobs-unified.json").read_text(encoding="utf-8") == private_feed
    assert not (data_dir / "jobs-unified-light.json").exists()
    assert not (data_dir / "jobs-availability-tombstones.json").exists()


def test_reconciliation_transaction_blocks_another_process(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    marker = tmp_path / "child-acquired.txt"
    script = (
        "import pathlib, sys\n"
        "from src.jobs.feed_reconciliation_lock import jobs_feed_reconciliation_lock\n"
        "data_dir = pathlib.Path(sys.argv[1])\n"
        "marker = pathlib.Path(sys.argv[2])\n"
        "with jobs_feed_reconciliation_lock(data_dir):\n"
        "    marker.write_text('acquired', encoding='utf-8')\n"
    )
    with jobs_feed_reconciliation_transaction(data_dir):
        with jobs_feed_reconciliation_transaction(data_dir):
            process = subprocess.Popen(
                [sys.executable, "-c", script, str(data_dir), str(marker)],
                cwd=Path(__file__).resolve().parents[2],
            )
            time.sleep(0.2)
            assert not marker.exists()

    assert process.wait(timeout=5.0) == 0
    assert marker.read_text(encoding="utf-8") == "acquired"


def test_contended_lock_retry_is_not_limited_to_ten_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    def acquire() -> None:
        nonlocal attempts
        attempts += 1
        if attempts <= 12:
            raise OSError(errno.EDEADLK, "contended")

    monkeypatch.setattr(feed_lock_module.time, "sleep", lambda _delay: None)
    feed_lock_module._retry_contended_lock(acquire)  # noqa: SLF001

    assert attempts == 13
