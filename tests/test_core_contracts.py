from src.core.schemas import CanonicalJobSchema


def test_canonical_job_schema_preserves_lifecycle_and_location_fields_on_dump() -> None:
    payload = {
        "id": "job-1",
        "title": "Gameplay Programmer",
        "company": "Example Studio",
        "jobLink": "https://example.invalid/jobs/1",
        "lifecycleEvent": "preserved",
        "lifecycleReason": "source_failed",
        "locations": [{"city": "Milan", "country": "Italy", "workType": "Hybrid"}],
        "locationSummary": "Milan, Italy (Hybrid)",
    }

    dumped = CanonicalJobSchema.model_validate(payload).model_dump()

    assert dumped["lifecycleEvent"] == "preserved"
    assert dumped["lifecycleReason"] == "source_failed"
    assert dumped["locations"] == [{"city": "Milan", "country": "Italy", "workType": "Hybrid"}]
    assert dumped["locationSummary"] == "Milan, Italy (Hybrid)"
