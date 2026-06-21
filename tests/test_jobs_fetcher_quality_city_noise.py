"""Tests for jobs fetcher quality city-noise contracts."""

from src import jobs_fetcher as jf
from src.jobs.text_utils import load_city_noise_contract


def test_canonicalize_job_with_reason_blanks_shared_city_noise_contract_fragments() -> None:
    contract = load_city_noise_contract()
    assert "bachelor's degree" in contract["proseFragments"]
    assert "learn" in contract["sentencePrefixes"]
    assert "%label_" in contract["placeholderFragments"]
    assert "????" in contract["knownJunkTokens"]
    assert "ai solutions pm" in contract["knownJunkTokens"]
    for token in [
        "Any",
        "Apps for kids",
        "CET +- 4",
        "CET +- 2",
        "COME FLY WITH US",
        "Chief Human Resource Officer (CHRO)",
        "Come work with us!",
        "Chronos: Before the Ashes",
        "Community",
        "Contact",
        "Create amazing characters that are efficient",
        "Create",
        "Cybersecurity",
        "Culture & Values",
        "Data & Engineering",
        "Data & Research",
        "Department",
        "Departments",
        "Do Not Sell My Information",
        "Do Not Share My Personal",
        "EU & NA",
        "Endless Legend is a 4X turn",
        "Ensure brand message is consistent",
        "Entertain the world",
        "Filter by",
        "Filter roles by",
        "Filters",
        "Finance",
        "Finance & Accounting",
        "Find us on Facebook",
        "From Concept to Console: Meet Winslow",
        "Full",
        "Full or part",
        "Games FQA Warsaw",
        "HUMANKIND is a turn",
        "Head of IP Licensing BD",
        "Head of Recruiting",
        "Help create video scripts",
        "In this role",
        "Imprint",
        "Internal Tools & Player Insights",
        "Interviews",
        "Junior",
        "Join our crew",
        "Join the community",
        "Join us",
        "Legal",
        "Ltd. )",
        "Mastery social platforms: Facebook",
        "Office",
        "Organization",
        "People & Culture",
        "Senior Production Accountant (Feature) : 2026",
        "Sega of America",
        "Sign in",
        "Spontaneous application",
        "Startup Directory Founder Directory Launch YC",
        "Student",
        "Student & Recent Graduates",
        "Studio",
        "Studios",
        "Titan Quest II Announced",
        "To be clear",
        "To be considered",
        "UNAVAILABLE",
        "UK",
        "Web Build Purple Imp",
        "Work & Innovation",
    ]:
        assert token.lower() in contract["knownJunkTokens"]

    cases = [
        "A bachelor's degree in digital communications",
        "If you are looking for Tokyo",
        "%LABEL_POSITION_TYPE_REMOTE_ANY%",
        "????",
        "Any",
        "Come work with us!",
    ]
    for city in cases:
        row, reason = jf.canonicalize_job_with_reason(
            {
                "title": "Artist",
                "company": "Studio",
                "city": city,
                "country": "Japan",
                "jobLink": "https://example.com/city-contract",
                "sector": "Game",
            },
            source="static_source::noise",
            fetched_at="2026-03-20T00:00:00Z",
        )
        assert reason == ""
        assert row is not None
        payload = row if isinstance(row, dict) else row.to_dict()
        assert payload["city"] == ""
        assert payload["country"] == "Japan"

    for city, expected_country in [
        ("EU & NA", "EU & NA"),
        ("UK", "UK"),
    ]:
        row, reason = jf.canonicalize_job_with_reason(
            {
                "title": "Artist",
                "company": "Studio",
                "city": city,
                "country": "Unknown",
                "jobLink": "https://example.com/city-contract-country",
                "sector": "Game",
            },
            source="static_source::noise",
            fetched_at="2026-03-20T00:00:00Z",
        )
        assert reason == ""
        assert row is not None
        payload = row if isinstance(row, dict) else row.to_dict()
        assert payload["city"] == ""
        assert payload["country"] == expected_country
        assert payload["locationSummary"] == expected_country
        assert payload["locations"] == [{"city": "", "country": expected_country}]


def test_canonicalize_job_with_reason_blanks_structural_city_noise_values() -> None:
    for city in ["2026", "3"]:
        row, reason = jf.canonicalize_job_with_reason(
            {
                "title": "Artist",
                "company": "Studio",
                "city": city,
                "country": "Japan",
                "jobLink": "https://example.com/city-structural-noise",
                "sector": "Game",
            },
            source="static_source::noise",
            fetched_at="2026-03-20T00:00:00Z",
        )
        assert reason == ""
        assert row is not None
        payload = row if isinstance(row, dict) else row.to_dict()
        assert payload["city"] == ""
        assert payload["country"] == "Japan"


def test_canonicalize_job_with_reason_blanks_metric_and_css_location_noise() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Artist",
            "company": "Studio",
            "city": "6,559 followers",
            "country": "--grid-gutter: calc(var(--sqs-mobile-site-gutter, 6vw) - 0.0px);",
            "jobLink": "https://example.com/metric-noise",
            "sector": "Game",
        },
        source="static_source::noise",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == ""
    assert payload["country"] == ""


def test_canonicalize_job_with_reason_rejects_country_work_type_noise() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Artist",
            "company": "Studio",
            "city": "Tokyo",
            "country": "Hybrid",
            "jobLink": "https://example.com/country-noise",
            "sector": "Game",
        },
        source="static_source::noise",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == "Tokyo"
    assert payload["country"] == "Japan"


def test_canonicalize_job_with_reason_preserves_region_country_names() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Artist",
            "company": "Studio",
            "city": "Skopje",
            "country": "North Macedonia",
            "jobLink": "https://example.com/region-country",
            "sector": "Game",
        },
        source="static_source::region-country",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == "Skopje"
    assert payload["country"] == "North Macedonia"


def test_canonicalize_job_with_reason_promotes_first_meaningful_multi_location_entry() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Rendering Engineer",
            "company": "Stellar Entertainment",
            "locations": [
                {"city": "", "country": "Unknown"},
                {"city": "Guildford", "country": "England"},
                {"city": "Utrecht", "country": "NL"},
            ],
            "jobLink": "https://jobs.ashbyhq.com/stellarentertainment/8615ea53-96d1-4923-9d48-a920639c9fbe",
            "sector": "Game",
        },
        source="ashby_sources",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == "Guildford"
    assert payload["country"] == "England"
    assert payload["locationSummary"] == "Guildford, England | Utrecht, NL"
    assert payload["locations"][0] == {"city": "Guildford", "country": "England"}


def test_canonicalize_job_with_reason_rebuilds_location_summary_from_surviving_entries() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Rendering Engineer",
            "company": "Stellar Entertainment",
            "city": "AI Solutions PM",
            "country": "Unknown",
            "locations": [
                {"city": "AI Solutions PM", "country": "Unknown"},
                {"city": "Guildford", "country": "UK"},
            ],
            "jobLink": "https://jobs.ashbyhq.com/stellarentertainment/5e067256-96d1-4923-9d48-a920639c9fbe",
            "sector": "Tech",
        },
        source="static_source::listing_url:https://stellarentertainment.software/join-us/",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == "Guildford"
    assert payload["country"] == "UK"
    assert payload["locationSummary"] == "Guildford, UK"
    assert payload["locations"] == [{"city": "Guildford", "country": "UK"}]
