"""Tests for jobs fetcher quality deduplication."""

import pytest

from src import jobs_fetcher as jf


@pytest.mark.parametrize(
    ("sparse_title", "sparse_job_link", "sparse_sector", "rich_title"),
    [
        (
            "Rendering Engineer Engineering Guildford, UK | Utrecht, NL United Kingdom",
            "https://jobs.ashbyhq.com/stellarentertainment/5e067256-96d1-4923-9d48-a920639c9fbe",
            "Tech",
            "Rendering Engineer",
        ),
        (
            "Technical Artist Art Guildford, UK | Utrecht, NL United Kingdom",
            "https://jobs.ashbyhq.com/stellarentertainment/4526ffd2-860e-4e2d-8743-4e637ca0ced6",
            "Game",
            "Technical Artist",
        ),
    ],
    ids=["rendering-engineer", "technical-artist"],
)
def test_deduplicate_jobs_merges_sparse_stellar_variants_into_richer_rows(
    sparse_title: str,
    sparse_job_link: str,
    sparse_sector: str,
    rich_title: str,
) -> None:
    sparse = jf.canonicalize_job(
        {
            "title": sparse_title,
            "company": "Stellar Entertainment Software",
            "city": "",
            "country": "Unknown",
            "locations": [{"city": "", "country": "Unknown"}],
            "jobLink": sparse_job_link,
            "sector": sparse_sector,
        },
        source="static_source::static:listing_url:https://stellarentertainment.software/join-us/",
        fetched_at="2026-03-20T00:00:00Z",
    )
    rich = jf.canonicalize_job(
        {
            "title": rich_title,
            "company": "Stellar Entertainment",
            "city": "Guildford",
            "country": "England",
            "locations": [
                {"city": "", "country": "Unknown"},
                {"city": "Guildford", "country": "England"},
                {"city": "Utrecht", "country": "NL"},
            ],
            "jobLink": "https://jobs.ashbyhq.com/stellarentertainment/8615ea53-9992-489f-b2cd-38ede3434679",
            "sector": "Game",
        },
        source="google_sheets_1er2oaxo",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert sparse is not None
    assert rich is not None
    rows, stats = jf.deduplicate_jobs([sparse, rich])
    assert int(stats["outputCount"]) == 1
    assert len(rows) == 1
    payload = rows[0].to_dict()
    assert payload["title"] == rich_title
    assert payload["company"] == "Stellar Entertainment"
    assert payload["city"] == "Guildford"
    assert payload["country"] == "England"
    assert payload["locations"] == [
        {"city": "Guildford", "country": "England"},
        {"city": "Utrecht", "country": "NL"},
    ]
    assert payload["locationSummary"] == "Guildford, England | Utrecht, NL"
