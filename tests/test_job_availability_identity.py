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
