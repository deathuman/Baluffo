"""Tests for jobs fetcher quality sanitizer behavior."""

from src import jobs_fetcher as jf
from src.jobs.contamination_audit import build_city_garbage_report, build_contamination_report


def test_public_text_sanitizer_cleans_html_contaminated_fields() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": '<div class="title">Technical Artist</div>',
            "company": "Kojimaproductions",
            "city": '<div class="location">Tokyo',
            "country": "Japan</div>",
            "contractType": "<span>Full-time</span>",
            "jobLink": "https://www.kojimaproductions.jp/en/technical-artist",
            "sector": "<div>Game</div>",
        },
        source="static_source::kojima",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["title"] == "Technical Artist"
    assert payload["city"] == "Tokyo"
    assert payload["country"] == "Japan"
    assert payload["contractType"] == "Full-time"
    assert payload["sector"] == "Game"


def test_contamination_audit_reports_public_field_examples() -> None:
    report = build_contamination_report(
        [
            {
                "title": "Clean",
                "company": "Studio",
                "city": "Paris",
                "country": "France",
                "jobLink": "https://example.com/1",
            },
            {
                "title": '<div class="title">Artist</div>',
                "company": "Studio",
                "city": '<div class="location">Tokyo',
                "country": "Japan</div>",
                "source": "static",
                "jobLink": "https://example.com/2",
            },
        ]
    )
    assert int(report["contaminatedRows"]) == 1
    assert int(report["fieldCounts"]["title"]) == 1
    assert int(report["fieldCounts"]["city"]) == 1
    assert int(report["fieldCounts"]["country"]) == 1
    assert str(report["examples"][0]["fields"]["city"]) == '<div class="location">Tokyo'


def test_city_garbage_audit_reports_obvious_garbage_examples() -> None:
    report = build_city_garbage_report(
        [
            {
                "title": "Gameplay Engineer",
                "company": "Studio",
                "city": "We're sorry",
                "country": "US",
                "locationSummary": "Winston-Salem, US | Clear search results",
                "locations": [
                    {"city": "We're sorry", "country": "US"},
                    {"city": "Berlin", "country": "DE"},
                ],
                "jobLink": "https://example.com/1",
            },
            {
                "title": "Gameplay Engineer",
                "company": "Studio",
                "city": "AI Enablement",
                "country": "US",
                "locationSummary": "AI Enablement | Regensburg, DE",
                "locations": [{"city": "AI Enablement", "country": "US"}],
                "jobLink": "https://example.com/2",
            },
            {
                "title": "Gameplay Engineer",
                "company": "Studio",
                "city": "Tokyo",
                "country": "JP",
                "locationSummary": "Tokyo, JP",
                "locations": [{"city": "Tokyo", "country": "JP"}],
                "jobLink": "https://example.com/3",
            },
        ]
    )
    assert int(report["totalRows"]) == 3
    assert int(report["garbageRows"]) == 2
    assert int(report["fieldCounts"]["city"]) == 2
    assert int(report["fieldCounts"]["locationSummary"]) == 2
    assert int(report["fieldCounts"]["locations.city"]) == 2
    assert int(report["categoryCounts"]["site_chrome"]) == 3
    assert int(report["categoryCounts"]["role_category"]) == 3
    assert str(report["examples"][0]["fields"]["city"]["category"]) == "site_chrome"
    assert str(report["examples"][1]["fields"]["city"]["category"]) == "role_category"
