from src.jobs.models import CanonicalJob
from src.jobs.state_lifecycle import (
    apply_job_lifecycle_state,
    build_lifecycle_source_evidence,
)


def test_carried_exact_identity_is_initialized_without_becoming_observed() -> None:
    carried = CanonicalJob.from_mapping(
        {
            "title": "Lifecycle Engineer",
            "company": "Evidence Studio",
            "jobLink": "https://example.com/jobs/lifecycle",
            "source": "failed_source",
            "sourceJobId": "life-1",
            "availabilityId": "availability_carried",
            "firstSeenAt": "2026-04-01T09:00:00+00:00",
            "lastSeenAt": "2026-04-20T09:00:00+00:00",
        }
    )
    evidence = build_lifecycle_source_evidence(
        [{"name": "failed_source", "status": "error", "error": "timeout"}],
        selected_source_names={"failed_source"},
        allow_missing=True,
    )

    rows, lifecycle, _archive, summary = apply_job_lifecycle_state(
        deduped_rows=[carried],
        observed_rows=[],
        lifecycle_rows={},
        finished_at="2026-04-30T12:00:00+00:00",
        allow_mark_missing=False,
        source_evidence=evidence,
    )

    entry = next(iter(lifecycle.values()))
    assert rows[0].availabilityId == "availability_carried"
    assert rows[0].availabilityStatus == "available"
    assert entry["lastSeenAt"] == "2026-04-20T09:00:00+00:00"
    assert entry["consecutiveAvailabilityFailures"] == 1
    assert entry["availabilityEvidence"]["kind"] == "source_failed"
    assert summary["carriedInitialized"] == 1
