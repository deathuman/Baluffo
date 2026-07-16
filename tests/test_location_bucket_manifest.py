from src.jobs.location_bucket_manifest import (
    build_unknown_country_bucket_manifest,
    check_manifest_against_rows,
)
from src.jobs.text_utils import sanitize_location_text


def test_build_unknown_country_bucket_manifest_groups_and_classifies_rows() -> None:
    manifest = build_unknown_country_bucket_manifest(
        [
            {
                "Title": "Sales Executive",
                "Company": "SailPoint",
                "City": "Hong Kong",
                "Country": "",
                "Source": "google_sheets",
                "JobLink": "https://example.com/hk",
            },
            {
                "Title": "Senior Artist",
                "Company": "Riot Games",
                "City": "Los Angeles, USA",
                "Country": "Unknown",
                "Source": "static_source::riot",
                "JobLink": "https://example.com/la",
            },
            {
                "Title": "Lead Environment Artist",
                "Company": "Area 35 East",
                "City": "grid",
                "Country": "Unknown",
                "Source": "static_source::area35",
                "JobLink": "https://example.com/grid",
            },
        ]
    )

    assert [item["city"] for item in manifest] == ["grid", "Hong Kong", "Los Angeles, USA"]
    assert manifest[0]["family"] == "garbage"
    assert manifest[1]["family"] == "city_only"
    assert manifest[1]["representative"]["jobHost"] == "example.com"
    assert manifest[2]["family"] == "city_blob"


def test_check_manifest_against_rows_reports_resolution_status() -> None:
    manifest = [
        {
            "city": "Hong Kong",
            "family": "city_only",
            "count": 1,
            "representative": {
                "title": "Sales Executive",
                "company": "SailPoint",
                "source": "google_sheets",
                "jobLink": "https://example.com/hk",
                "jobHost": "example.com",
            },
        },
        {
            "city": "grid",
            "family": "garbage",
            "count": 1,
            "representative": {
                "title": "Lead Environment Artist",
                "company": "Area 35 East",
                "source": "static_source::area35",
                "jobLink": "https://example.com/grid",
                "jobHost": "example.com",
            },
        },
    ]

    results = check_manifest_against_rows(
        manifest,
        [
            {
                "Title": "Sales Executive",
                "Company": "SailPoint",
                "City": "Hong Kong",
                "Country": "HK",
                "Source": "google_sheets",
                "JobLink": "https://example.com/hk",
            },
            {
                "Title": "Lead Environment Artist",
                "Company": "Area 35 East",
                "City": "",
                "Country": "",
                "Source": "static_source::area35",
                "JobLink": "https://example.com/grid",
            },
        ],
    )

    assert results == [
        {
            "city": "Hong Kong",
            "family": "city_only",
            "count": 1,
            "status": "resolved",
            "candidate": {
                "title": "Sales Executive",
                "company": "SailPoint",
                "city": "Hong Kong",
                "country": "HK",
                "source": "google_sheets",
                "jobLink": "https://example.com/hk",
            },
        },
        {
            "city": "grid",
            "family": "garbage",
            "count": 1,
            "status": "cleared",
            "candidate": {
                "title": "Lead Environment Artist",
                "company": "Area 35 East",
                "city": "",
                "country": "",
                "source": "static_source::area35",
                "jobLink": "https://example.com/grid",
            },
        },
    ]


def test_sanitize_location_text_accepts_iso_country_codes() -> None:
    assert sanitize_location_text("HK", field_name="country") == ("HK", "")
    assert sanitize_location_text("SE", field_name="country") == ("SE", "")
