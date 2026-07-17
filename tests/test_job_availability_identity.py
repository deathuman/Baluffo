import json
import time

import pytest

from src.jobs.availability_identity import (
    AvailabilityIdentityPreflightError,
    prepare_availability_identities,
    validate_published_availability_rows,
)
from src.jobs.models import CanonicalJob
from src.jobs.state_lifecycle import apply_job_lifecycle_state


def test_lifecycle_identity_does_not_join_matching_secondary_dedup_keys() -> None:
    shared_dedup = "secondary:matching-title-company-location"
    previous = {
        "status": "active",
        "title": "Designer",
        "company": "Studio",
        "jobLink": "https://example.com/jobs/opening-a",
        "source": "source_a",
        "sourceJobId": "opening-a",
        "dedupKey": shared_dedup,
        "availabilityId": "availability_opening_a",
        "availabilityAliases": [
            "source:source_a:opening-a",
            shared_dedup,
            f"dedup:{shared_dedup}",
        ],
    }
    current = CanonicalJob.from_mapping(
        {
            "title": "Designer",
            "company": "Studio",
            "jobLink": "https://example.com/jobs/opening-b",
            "source": "source_a",
            "sourceJobId": "opening-b",
            "dedupKey": shared_dedup,
        }
    )

    rows, lifecycle, _archive, _summary = apply_job_lifecycle_state(
        deduped_rows=[current],
        observed_rows=[current],
        lifecycle_rows={"source:source_a:opening-a": previous},
        finished_at="2026-07-14T10:00:00+00:00",
        allow_mark_missing=False,
    )

    assert set(lifecycle) == {
        "source:source_a:opening-a",
        "source:source_a:opening-b",
    }
    assert rows[0].availabilityId != "availability_opening_a"


def test_conflicting_source_aliases_are_repaired_with_distinct_url_identities() -> None:
    rows = [
        CanonicalJob.from_mapping(
            {
                "title": title,
                "company": company,
                "source": "sheet",
                "sourceJobId": "reused-row-id",
                "jobLink": url,
                "availabilityId": "availability_contaminated",
            }
        )
        for title, company, url in (
            ("Engineer", "Studio A", "https://a.example/jobs/1"),
            ("Artist", "Studio B", "https://b.example/jobs/2"),
        )
    ]
    prepared = prepare_availability_identities(
        rows=rows,
        observed_rows=rows,
        lifecycle_rows={
            "legacy": {
                "availabilityId": "availability_contaminated",
                "availabilityStatus": "unavailable",
                "availabilityAliases": ["source:sheet:reused-row-id"],
            }
        },
        detected_at="2026-07-16T12:00:00+00:00",
    )

    assert len({row.availabilityId for row in prepared.rows}) == 2
    assert "legacy" not in prepared.lifecycle_rows
    quarantined = prepared.quarantine_additions["availability_contaminated"]
    assert quarantined["reason"] == "cross_url_identity_collision"
    assert len(quarantined["replacementAvailabilityIds"]) == 2
    assert prepared.summary["unresolvedIdentityConflictCount"] == 0


def test_identical_canonical_urls_may_share_one_availability_identity() -> None:
    rows = [
        CanonicalJob.from_mapping(
            {
                "title": f"Engineer {index}",
                "company": "Studio",
                "source": "sheet",
                "sourceJobId": f"row-{index}",
                "jobLink": "https://jobs.example/opening/1?utm_source=test",
            }
        )
        for index in range(2)
    ]
    prepared = prepare_availability_identities(
        rows=rows,
        observed_rows=rows,
        lifecycle_rows={},
        detected_at="2026-07-16T12:00:00+00:00",
    )

    assert len({row.availabilityId for row in prepared.rows}) == 1
    assert prepared.summary["monitorableRowCount"] == 2


def test_one_to_one_source_alias_keeps_identity_when_url_changes() -> None:
    row = CanonicalJob.from_mapping(
        {
            "title": "Engineer",
            "company": "Studio",
            "source": "provider",
            "sourceJobId": "job-42",
            "jobLink": "https://jobs.example/opening/42-new",
        }
    )
    prepared = prepare_availability_identities(
        rows=[row],
        observed_rows=[row],
        lifecycle_rows={
            "source:provider:job-42": {
                "source": "provider",
                "sourceJobId": "job-42",
                "jobLink": "https://jobs.example/opening/42-old",
                "availabilityId": "availability_stable",
            }
        },
        detected_at="2026-07-16T12:00:00+00:00",
    )

    assert prepared.rows[0].availabilityId == "availability_stable"
    assert prepared.quarantine_additions == {}


def test_new_unambiguous_source_alias_keeps_identity_across_url_change() -> None:
    def prepare(url: str):
        row = CanonicalJob.from_mapping(
            {
                "title": "Engineer",
                "company": "Studio",
                "source": "provider",
                "sourceJobId": "job-42",
                "jobLink": url,
            }
        )
        return (
            prepare_availability_identities(
                rows=[row],
                observed_rows=[row],
                lifecycle_rows={},
                detected_at="2026-07-16T12:00:00+00:00",
            )
            .rows[0]
            .availabilityId
        )

    old_identity = prepare("https://jobs.example/opening/42-old")
    new_identity = prepare("https://jobs.example/opening/42-new")

    assert old_identity
    assert new_identity == old_identity


def test_large_exact_identity_preflight_has_no_cross_url_collisions() -> None:
    rows = [
        CanonicalJob.from_mapping(
            {
                "title": f"Role {index}",
                "company": "Synthetic Studio",
                "source": "synthetic",
                "sourceJobId": f"job-{index}",
                "jobLink": f"https://jobs.example/opening/{index}",
            }
        )
        for index in range(5_000)
    ]
    prepared = prepare_availability_identities(
        rows=rows,
        observed_rows=rows,
        lifecycle_rows={},
        detected_at="2026-07-16T12:00:00+00:00",
    )

    identities = [row.availabilityId for row in prepared.rows]
    assert all(identities)
    assert len(set(identities)) == len(rows)
    assert prepared.summary["monitorableRowCount"] == len(rows)
    assert prepared.summary["acceptedMonitorableRowCount"] == len(rows)
    assert prepared.summary["rejectedRowCount"] == 0
    assert prepared.summary["rejectionReasonCounts"] == {}
    assert prepared.summary["unresolvedMissingIdentityCount"] == 0
    assert prepared.summary["unresolvedIdentityConflictCount"] == 0


@pytest.mark.slow
def test_live_scale_identity_preflight_partitions_one_unresolvable_candidate() -> None:
    unique_count = 79_525
    rows = [
        CanonicalJob.from_mapping(
            {
                "title": f"Role {index}",
                "company": "Synthetic Studio",
                "source": "synthetic",
                "sourceJobId": f"job-{index}",
                "jobLink": f"https://jobs.example/opening/{index}",
            }
        )
        for index in range(unique_count)
    ]
    rows.extend(
        CanonicalJob.from_mapping(
            {
                "title": f"Conflicted role {index}",
                "company": "Synthetic Studio",
                "source": "synthetic",
                "sourceJobId": "reused-live-scale-id",
                "jobLink": url,
            }
        )
        for index, url in enumerate(
            (
                "https://jobs.example/conflict/1",
                "https://jobs.example/conflict/2",
                "",
            )
        )
    )

    started = time.perf_counter()
    prepared = prepare_availability_identities(
        rows=rows,
        observed_rows=rows,
        lifecycle_rows={},
        detected_at="2026-07-17T12:00:00+00:00",
    )
    elapsed = time.perf_counter() - started

    assert len(rows) == 79_528
    assert len(prepared.rows) == 79_527
    assert len(prepared.observed_rows) == 79_527
    assert prepared.summary["rejectedRowCount"] == 1
    assert prepared.summary["postFilterUnresolvedMissingIdentityCount"] == 0
    assert prepared.summary["postFilterUnresolvedIdentityConflictCount"] == 0
    assert len(json.dumps(prepared.quarantine_additions)) < 16_384
    assert elapsed < 120


def test_conflicting_url_less_candidate_is_quarantined_and_excluded() -> None:
    rows = [
        CanonicalJob.from_mapping(
            {
                "title": title,
                "company": "Studio",
                "source": "sheet",
                "sourceJobId": "reused-row-id",
                "jobLink": url,
            }
        )
        for title, url in (
            ("Engineer", "https://jobs.example/opening/1"),
            ("Artist", "https://jobs.example/opening/2"),
            ("Unidentified", ""),
        )
    ]

    prepared = prepare_availability_identities(
        rows=rows,
        observed_rows=rows,
        lifecycle_rows={},
        detected_at="2026-07-17T12:00:00+00:00",
    )

    assert len(prepared.rows) == 2
    assert len(prepared.observed_rows) == 2
    assert all(row.availabilityId for row in prepared.rows)
    assert prepared.summary["candidateMonitorableRowCount"] == 3
    assert prepared.summary["acceptedMonitorableRowCount"] == 2
    assert prepared.summary["rejectedRowCount"] == 1
    assert prepared.summary["rejectionReasonCounts"] == {
        "conflicting_source_alias_without_public_url": 1
    }
    unresolved = [
        row
        for row in prepared.quarantine_additions.values()
        if row.get("kind") == "unresolved_candidate_group"
    ]
    assert len(unresolved) == 1
    assert unresolved[0]["candidateCount"] == 1
    assert unresolved[0]["relatedCandidateCount"] == 3
    assert "reused-row-id" not in str(unresolved[0])


def test_publication_invariant_rejects_one_identity_across_distinct_urls() -> None:
    rows = [
        {
            "jobLink": f"https://jobs.example/opening/{index}",
            "source": "provider",
            "sourceJobId": f"job-{index}",
            "availabilityId": "availability_shared",
            "availabilityStatus": "available",
            "availabilityEvidence": {
                "kind": "source_present",
                "confidence": "definitive",
                "checkedAt": "2026-07-16T12:00:00+00:00",
            },
        }
        for index in range(2)
    ]

    with pytest.raises(AvailabilityIdentityPreflightError) as exc_info:
        validate_published_availability_rows(rows)
    assert exc_info.value.reason == "availability_publication_identity_collision"
