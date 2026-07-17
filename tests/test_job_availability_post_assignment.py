from src.jobs.availability_identity import prepare_availability_identities
from src.jobs.models import CanonicalJob


def _source_identity(source_only: CanonicalJob) -> str:
    return (
        prepare_availability_identities(
            rows=[source_only],
            observed_rows=[source_only],
            lifecycle_rows={},
            detected_at="2026-07-17T12:00:00+00:00",
        )
        .rows[0]
        .availabilityId
    )


def test_post_assignment_collision_repairs_url_row_and_rejects_url_less_row() -> None:
    source_only = CanonicalJob.from_mapping(
        {
            "title": "Source-only role",
            "company": "Studio",
            "source": "sheet",
            "sourceJobId": "source-only-id",
            "jobLink": "",
        }
    )
    source_identity = _source_identity(source_only)
    url_row = CanonicalJob.from_mapping(
        {
            "title": "URL role",
            "company": "Other Studio",
            "source": "provider",
            "sourceJobId": "url-role-id",
            "jobLink": "https://jobs.example/opening/url-role",
            "availabilityId": source_identity,
        }
    )

    prepared = prepare_availability_identities(
        rows=[url_row, source_only],
        observed_rows=[url_row, source_only],
        lifecycle_rows={
            source_identity: {
                "availabilityId": source_identity,
                "availabilityStatus": "available",
            }
        },
        detected_at="2026-07-17T12:00:00+00:00",
    )

    assert len(prepared.rows) == len(prepared.observed_rows) == 1
    assert prepared.rows[0].jobLink == "https://jobs.example/opening/url-role"
    assert prepared.rows[0].availabilityId != source_identity
    assert prepared.summary["rejectionReasonCounts"] == {
        "post_assignment_identity_conflict_without_public_url": 1
    }
    assert prepared.summary["postFilterUnresolvedIdentityConflictCount"] == 0
    assert source_identity not in {
        entry.get("availabilityId") for entry in prepared.lifecycle_rows.values()
    }
    assert prepared.quarantine_additions[source_identity]["reason"] == (
        "cross_url_identity_collision"
    )


def test_post_assignment_collision_repair_handles_multiple_production_groups() -> None:
    rows: list[CanonicalJob] = []
    for index in range(8):
        source_only = CanonicalJob.from_mapping(
            {
                "title": f"Source-only role {index}",
                "company": "Studio",
                "source": "sheet",
                "sourceJobId": f"source-only-{index}",
                "jobLink": "",
            }
        )
        rows.extend(
            [
                CanonicalJob.from_mapping(
                    {
                        "title": f"URL role {index}",
                        "company": "Other Studio",
                        "source": "provider",
                        "sourceJobId": f"url-role-{index}",
                        "jobLink": f"https://jobs.example/opening/{index}",
                        "availabilityId": _source_identity(source_only),
                    }
                ),
                source_only,
            ]
        )

    prepared = prepare_availability_identities(
        rows=rows,
        observed_rows=rows,
        lifecycle_rows={},
        detected_at="2026-07-17T12:00:00+00:00",
    )

    assert len(prepared.rows) == len(prepared.observed_rows) == 8
    assert prepared.summary["repairedIdentityCount"] == 8
    assert prepared.summary["contaminatedIdentityCount"] == 8
    assert prepared.summary["rejectedRowCount"] == 8
    assert prepared.summary["postFilterUnresolvedIdentityConflictCount"] == 0
    assert len({row.availabilityId for row in prepared.rows}) == 8
