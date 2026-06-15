from unittest import mock

from src import jobs_fetcher


def test_pipeline_output_contract_preserves_camelcase_schema() -> None:
    """
    Guard test to ensure that output JSON preserves the camelCase keys
    expected by the frontend domain (frontend/jobs/domain.js).
    """
    payload = jobs_fetcher.canonicalize_job(
        {
            "title": "Technical Artist",
            "company": "Giant Enemy Crab",
            "city": "Amsterdam",
            "country": "NL",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://example.com/jobs/123",
            "sector": "Game",
        },
        source="guard-test",
        fetched_at="2026-03-15T12:00:00+00:00",
    )
    assert payload is not None

    merged, _stats = jobs_fetcher.deduplicate_jobs([payload])
    assert len(merged) == 1
    row = merged[0].to_dict()

    expected_keys = {
        "title",
        "company",
        "city",
        "country",
        "workType",
        "contractType",
        "jobLink",
        "sector",
        "profession",
        "sourceJobId",
        "postedAt",
        "qualityScore",
        "focusScore",
    }

    for key in expected_keys:
        assert key in row, (
            f"Contract violation: missing camelCase key '{key}' in pipeline output row."
        )


@mock.patch("sys.argv", ["jobs_fetcher.py", "--only-sources", "missing-dummy-source", "--quiet"])
def test_pipeline_rejects_unknown_only_sources_without_silent_success() -> None:
    """
    Guard test to ensure unknown targeted selectors fail clearly instead of
    silently running an empty fetch.
    """
    fake_report = {
        "summary": {"outputCount": 1, "failedSources": 0},
        "outputs": {"report": "unit-test-report.json"},
    }
    with mock.patch("src.jobs.pipeline.run_pipeline", return_value=fake_report) as run_pipeline:
        assert jobs_fetcher.main() == 2

    run_pipeline.assert_not_called()
