import pytest

from src.jobs.availability_tombstones import (
    capture_availability_tombstone,
    reconcile_availability_tombstones,
    restore_availability_tombstone,
)


def _canonical_row(availability_id: str = "availability_1") -> dict:
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
        "description": "Complete canonical description",
        "availabilityId": availability_id,
        "availabilityStatus": "available",
    }


def test_tombstone_restores_complete_canonical_row_with_lifecycle_overlay() -> None:
    tombstones: dict[str, dict] = {}
    capture_availability_tombstone(
        tombstones,
        _canonical_row(),
        {
            "availabilityId": "availability_1",
            "availabilityStatus": "unavailable",
            "availabilityUnavailableAt": "2026-07-01T10:00:00+00:00",
            "availabilityEvidence": {"kind": "source_absent"},
        },
    )

    restored = restore_availability_tombstone(
        tombstones,
        "availability_1",
        {
            "availabilityId": "availability_1",
            "availabilityStatus": "available",
            "availabilityCheckedAt": "2026-07-10T10:00:00+00:00",
            "status": "active",
        },
    )

    assert restored["description"] == "Complete canonical description"
    assert restored["workType"] == "Hybrid"
    assert restored["availabilityStatus"] == "available"
    assert tombstones == {}


def test_missing_or_conflicting_tombstones_fail_closed() -> None:
    with pytest.raises(ValueError, match="canonical row unavailable"):
        restore_availability_tombstone(
            {},
            "availability_1",
            {"availabilityId": "availability_1", "availabilityStatus": "available"},
        )
    with pytest.raises(ValueError, match="identity mismatch"):
        capture_availability_tombstone(
            {},
            _canonical_row("availability_other"),
            {"availabilityId": "availability_1", "availabilityStatus": "unavailable"},
        )


def test_source_reappearance_prunes_tombstone_and_lifecycle_pruning_bounds_it() -> None:
    canonical = _canonical_row()
    existing: dict[str, dict] = {}
    capture_availability_tombstone(
        existing,
        canonical,
        {"availabilityId": "availability_1", "availabilityStatus": "unavailable"},
    )
    available_lifecycle = {
        "job-1": {"availabilityId": "availability_1", "availabilityStatus": "available"}
    }

    assert (
        reconcile_availability_tombstones(
            existing,
            before_rows=[],
            after_rows=[canonical],
            lifecycle_rows=available_lifecycle,
        )
        == {}
    )
    assert (
        reconcile_availability_tombstones(
            existing,
            before_rows=[],
            after_rows=[],
            lifecycle_rows={},
        )
        == {}
    )


def test_source_closure_captures_complete_canonical_tombstone() -> None:
    canonical = _canonical_row()
    lifecycle = {
        "job-1": {
            "availabilityId": "availability_1",
            "availabilityStatus": "unavailable",
            "availabilityUnavailableAt": "2026-07-10T10:00:00+00:00",
            "availabilityEvidence": {"kind": "source_absent"},
        }
    }

    tombstones = reconcile_availability_tombstones(
        {}, before_rows=[canonical], after_rows=[], lifecycle_rows=lifecycle
    )

    assert tombstones["availability_1"]["canonicalRow"]["profession"] == "engine-programmer"
    assert tombstones["availability_1"]["reason"] == "source_absent"
