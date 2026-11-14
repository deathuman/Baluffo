from __future__ import annotations

import pytest

from src.storage.job_runtime import (
    canonical_jobs_feed_row,
    jobs_feed_content_hash,
    jobs_feed_rows_hash,
)


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": 1,
        "title": "Tools Programmer",
        "company": "Studio A",
        "city": "Amsterdam",
        "country": "Netherlands",
        "jobLink": "https://example.test/jobs/1",
        "source": "Studio A",
        "sourceJobId": "job-1",
        "status": "active",
        "firstSeenAt": "2026-05-12T10:00:00Z",
        "lastSeenAt": "2026-05-12T10:00:00Z",
        "fetchedAt": "2026-05-12T10:00:00Z",
        "availabilityId": "availability_1",
        "availabilityStatus": "available",
        "availabilityCheckedAt": "2026-05-12T10:00:00Z",
        "availabilityVerifiedAt": "2026-05-12T10:00:00Z",
        "availabilityUnavailableAt": "",
        "availabilityEvidence": {
            "kind": "source_present",
            "confidence": "definitive",
            "checkedAt": "2026-05-12T10:00:00Z",
            "source": "Studio A",
        },
        "sourceBundle": [{"sourceName": "Studio A", "sourceJobId": "job-1"}],
    }
    row.update(overrides)
    return row


def test_content_hash_ignores_volatile_observation_timestamps() -> None:
    observed = _row()
    reobserved = _row(
        lastSeenAt="2026-05-13T10:00:00Z",
        fetchedAt="2026-05-13T10:00:00Z",
        availabilityCheckedAt="2026-05-13T10:00:00Z",
        availabilityVerifiedAt="2026-05-13T10:00:00Z",
        availabilityUnavailableAt="2026-05-13T10:00:00Z",
        availabilityEvidence={
            "kind": "source_present",
            "confidence": "definitive",
            "checkedAt": "2026-05-13T10:00:00Z",
            "source": "Studio A",
        },
    )

    assert jobs_feed_content_hash([observed]) == jobs_feed_content_hash([reobserved])
    assert jobs_feed_rows_hash([observed]) != jobs_feed_rows_hash([reobserved])


def test_content_hash_ignores_volatile_fields_inside_source_bundles() -> None:
    observed = _row()
    reobserved = _row(
        sourceBundle=[
            {"sourceName": "Studio A", "sourceJobId": "job-1", "lastSeenAt": "2026-05-13"}
        ]
    )

    assert jobs_feed_content_hash([observed]) == jobs_feed_content_hash([reobserved])


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("title", "Senior Tools Programmer"),
        ("status", "likely_removed"),
        ("availabilityStatus", "verification_overdue"),
        ("sourceBundle", [{"sourceName": "Studio A", "sourceJobId": "job-2"}]),
    ],
)
def test_content_hash_changes_for_published_content(field: str, value: object) -> None:
    observed = _row()
    changed = _row(**{field: value})

    assert jobs_feed_content_hash([observed]) != jobs_feed_content_hash([changed])


def test_content_hash_changes_for_availability_evidence() -> None:
    observed = _row()
    changed = _row(
        availabilityEvidence={
            "kind": "direct_live",
            "confidence": "definitive",
            "checkedAt": "2026-05-12T10:00:00Z",
            "source": "Studio A",
        }
    )

    assert jobs_feed_content_hash([observed]) != jobs_feed_content_hash([changed])


def test_canonical_row_keeps_stable_fields_and_drops_only_volatile_ones() -> None:
    projected = canonical_jobs_feed_row(_row())

    assert projected["title"] == "Tools Programmer"
    assert projected["availabilityStatus"] == "available"
    assert projected["availabilityEvidence"] == {
        "kind": "source_present",
        "confidence": "definitive",
        "source": "Studio A",
    }
    assert "lastSeenAt" not in projected
    assert "fetchedAt" not in projected
    assert "availabilityCheckedAt" not in projected
    assert "availabilityVerifiedAt" not in projected
    assert "availabilityUnavailableAt" not in projected
