import threading
import time
from pathlib import Path

import pytest

from src.bridge.job_availability_service import JobAvailabilityService
from src.jobs.state_lifecycle import write_job_lifecycle_state


def _wait_for_terminal_status(service: JobAvailabilityService, run_id: str) -> dict:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        status = service.status(run_id)
        if status.get("status") != "running":
            return status
        time.sleep(0.001)
    raise AssertionError(f"availability task {run_id} did not finish")


def test_invalid_availability_target_fails_in_worker_and_clears_active_state(
    tmp_path: Path,
) -> None:
    service = JobAvailabilityService(data_dir=tmp_path / "data", local_store_factory=lambda: None)

    started = service.start({"availabilityId": "availability_missing"})

    assert started["started"] is True
    status = _wait_for_terminal_status(service, started["runId"])
    assert status["status"] == "failed"
    assert service._active_by_availability_id == {}


def test_duplicate_start_reuses_run_while_target_preparation_is_blocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    entry = {
        "availabilityId": "availability_blocked",
        "jobLink": "https://example.com/jobs/blocked",
        "availabilityStatus": "available",
    }
    write_job_lifecycle_state(data_dir / "jobs-lifecycle-state.json", {"job-1": entry})
    prep_entered = threading.Event()
    release_prep = threading.Event()

    class Validator:
        def check(self, _url: str) -> dict:
            return {
                "kind": "direct_live",
                "confidence": "unknown",
                "checkedAt": "2026-07-18T10:00:00+00:00",
                "source": "example.com",
            }

    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=lambda: None,
        validator=Validator(),
        enforce_direct=False,
    )

    def blocked_prepare(availability_id: str):
        assert availability_id == "availability_blocked"
        prep_entered.set()
        assert release_prep.wait(2.0)
        return "job-1", "canonical", service.lifecycle_path, dict(entry)

    monkeypatch.setattr(service, "_prepare_target", blocked_prepare)
    first = service.start({"availabilityId": "availability_blocked"})
    assert prep_entered.wait(1.0)

    started_at = time.monotonic()
    second = service.start({"availabilityId": "availability_blocked"})
    elapsed = time.monotonic() - started_at

    assert second["started"] is True
    assert second["reused"] is True
    assert second["runId"] == first["runId"]
    assert elapsed < 0.25

    release_prep.set()
    assert _wait_for_terminal_status(service, first["runId"])["status"] == "succeeded"


def test_start_and_status_stay_responsive_while_apply_is_blocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    entry = {
        "availabilityId": "availability_apply_blocked",
        "jobLink": "https://example.com/jobs/apply-blocked",
        "availabilityStatus": "available",
    }
    write_job_lifecycle_state(data_dir / "jobs-lifecycle-state.json", {"job-1": entry})
    apply_entered = threading.Event()
    release_apply = threading.Event()

    class Validator:
        def check(self, _url: str) -> dict:
            return {
                "kind": "direct_live",
                "confidence": "unknown",
                "checkedAt": "2026-07-18T10:00:00+00:00",
                "source": "example.com",
            }

    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=lambda: None,
        validator=Validator(),
        enforce_direct=True,
    )

    def blocked_apply(**kwargs):
        apply_entered.set()
        assert release_apply.wait(2.0)
        return dict(kwargs["current_entry"]), False

    monkeypatch.setattr(service, "_apply_checked_evidence", blocked_apply)
    first = service.start({"availabilityId": "availability_apply_blocked"})
    assert apply_entered.wait(1.0)

    started_at = time.monotonic()
    second = service.start({"availabilityId": "availability_apply_blocked"})
    status = service.status(first["runId"])
    elapsed = time.monotonic() - started_at

    assert second["reused"] is True
    assert second["runId"] == first["runId"]
    assert status["status"] == "running"
    assert elapsed < 0.25

    release_apply.set()
    assert _wait_for_terminal_status(service, first["runId"])["status"] == "succeeded"
