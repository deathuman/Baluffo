"""Tests for the availability service's reused lifecycle-state parse."""

from pathlib import Path

import pytest

import src.bridge.job_availability_service as availability_service_module
from src.bridge.job_availability_service import JobAvailabilityService
from src.jobs.state_lifecycle import write_job_lifecycle_state


def _state_rows(count: int = 3, *, marker: str = "available") -> dict[str, dict[str, object]]:
    return {
        f"job-{index}": {
            "status": "active",
            "availabilityId": f"availability_{index}",
            "availabilityStatus": marker,
            "jobLink": f"https://example.com/jobs/{index}",
        }
        for index in range(count)
    }


def _counting_reads(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    calls: list[Path] = []
    real_read = availability_service_module.read_job_lifecycle_state

    def counting(state_path: Path) -> dict[str, dict[str, object]]:
        calls.append(Path(state_path))
        return real_read(state_path)

    monkeypatch.setattr(availability_service_module, "read_job_lifecycle_state", counting)
    return calls


def test_prepared_targets_share_one_parse_of_the_lifecycle_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    write_job_lifecycle_state(data_dir / "jobs-lifecycle-state.json", _state_rows())
    service = JobAvailabilityService(data_dir=data_dir, local_store_factory=lambda: None)
    calls = _counting_reads(monkeypatch)

    first = service._prepare_target("availability_0")
    second = service._prepare_target("availability_1")
    third = service._prepare_target("availability_2")

    assert first[3]["availabilityId"] == "availability_0"
    assert second[3]["availabilityId"] == "availability_1"
    assert third[3]["availabilityId"] == "availability_2"
    assert len(calls) == 1


def test_lifecycle_state_write_invalidates_the_shared_parse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    state_path = data_dir / "jobs-lifecycle-state.json"
    write_job_lifecycle_state(state_path, _state_rows())
    service = JobAvailabilityService(data_dir=data_dir, local_store_factory=lambda: None)
    calls = _counting_reads(monkeypatch)
    service._prepare_target("availability_0")

    write_job_lifecycle_state(state_path, _state_rows(count=6, marker="verification_overdue"))
    refreshed = service._prepare_target("availability_5")

    assert len(calls) == 2
    assert refreshed[3]["availabilityStatus"] == "verification_overdue"


def test_a_mutating_reader_cannot_poison_the_shared_parse(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    state_path = data_dir / "jobs-lifecycle-state.json"
    write_job_lifecycle_state(state_path, _state_rows())
    service = JobAvailabilityService(data_dir=data_dir, local_store_factory=lambda: None)

    reader = service._lifecycle_rows(state_path)
    reader["job-0"] = {"status": "archived", "availabilityId": "availability_0"}

    assert service._lifecycle_rows(state_path)["job-0"]["status"] == "active"
