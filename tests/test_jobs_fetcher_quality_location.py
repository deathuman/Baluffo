"""Tests for jobs fetcher quality location guardrails."""

from src import jobs_fetcher as jf
from src.jobs.contamination_audit import build_public_text_quality_report
from src.jobs.pipeline_finalize import _apply_final_location_quality_guardrail
from tests.helpers import jobs_reporting


def test_canonicalize_job_with_reason_blanks_semantic_location_noise() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Growth Marketing Intern",
            "company": "Sleeper",
            "city": "Remote, United States; San Francisco Area, United States Remote; New York City; Los Angeles",
            "country": "Unknown",
            "jobLink": "https://jobs.ashbyhq.com/sleeper/example",
            "sector": "Game",
        },
        source="ashby_sources",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == ""
    assert payload["country"] == ""


def test_canonicalize_job_with_reason_normalizes_raw_city_blob_without_locations() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Technical Artist",
            "company": "Riot Games",
            "city": "Los Angeles, USA",
            "country": "Unknown",
            "jobLink": "https://example.com/riot",
            "sector": "Game",
        },
        source="static_source::static:listing_url:https://www.riotgames.com/en/work-with-us/jobs",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == "Los Angeles"
    assert payload["country"] == "US"
    assert payload["locations"] == [{"city": "Los Angeles", "country": "US"}]


def test_canonicalize_job_with_reason_preserves_city_only_unknown_country_summary() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Tools Programmer",
            "company": "Example Studio",
            "city": "Cambridge",
            "country": "Unknown",
            "jobLink": "https://example.com/cambridge",
            "sector": "Game",
        },
        source="static_source::example",
        fetched_at="2026-03-20T00:00:00Z",
    )

    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == "Cambridge"
    assert payload["country"] == ""
    assert payload["locations"] == [{"city": "Cambridge", "country": ""}]
    assert payload["locationSummary"] == "Cambridge"


def test_canonicalize_job_with_reason_promotes_country_only_raw_city_value() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Localization Producer",
            "company": "PlayStation Global",
            "city": "Japan",
            "country": "Unknown",
            "jobLink": "https://example.com/japan",
            "sector": "Game",
        },
        source="greenhouse_boards",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == ""
    assert payload["country"] == "Japan"
    assert payload["locations"] == [{"city": "", "country": "Japan"}]


def test_final_location_quality_guardrail_keeps_unknown_country_placeholders() -> None:
    rows = [
        {
            "title": "Tools Programmer",
            "company": "Example Studio",
            "city": "Cambridge",
            "country": "Unknown",
            "source": "static_source::example",
            "jobLink": "https://example.com/cambridge",
        },
        {
            "title": "Artist",
            "company": "Example Studio",
            "city": "Tokyo",
            "country": "N/A",
            "source": "static_source::example",
            "jobLink": "https://example.com/tokyo",
        },
    ]

    report = _apply_final_location_quality_guardrail(rows)

    assert int(report["invalidLocationFieldCount"]) == 0
    assert rows[0]["country"] == "Unknown"
    assert rows[1]["country"] == "N/A"


def test_final_location_quality_guardrail_still_blanks_invalid_country_noise() -> None:
    rows = [
        {
            "title": "Producer",
            "company": "Example Studio",
            "city": "Berlin",
            "country": "Hybrid",
            "source": "static_source::example",
            "jobLink": "https://example.com/producer",
        },
        {
            "title": "Engineer",
            "company": "Example Studio",
            "city": "Paris",
            "country": 'document.addEventListener("DOMContentLoaded", function () {',
            "source": "static_source::example",
            "jobLink": "https://example.com/engineer",
        },
    ]

    report = _apply_final_location_quality_guardrail(rows)

    assert int(report["invalidLocationFieldCount"]) == 2
    assert int(report["fieldCounts"]["country"]) == 2
    assert rows[0]["country"] == ""
    assert rows[1]["country"] == ""


def test_public_text_quality_report_includes_city_garbage_audit() -> None:
    report = build_public_text_quality_report(
        [
            {
                "title": "Gameplay Engineer",
                "company": "Studio",
                "city": "We're sorry",
                "country": "US",
                "locationSummary": "Winston-Salem, US | Clear search results",
                "locations": [{"city": "We're sorry", "country": "US"}],
                "jobLink": "https://example.com/report",
            }
        ]
    )
    assert "contaminatedRows" in report
    assert "locationQualityAudit" in report
    assert "cityGarbageAudit" in report
    assert int(report["cityGarbageAudit"]["garbageRows"]) == 1


def test_normalize_fetch_report_payload_preserves_city_garbage_audit() -> None:
    normalized_report = jobs_reporting.normalize_fetch_report_payload(
        {
            "schemaVersion": 1,
            "runId": "run-1",
            "startedAt": "2026-03-30T00:00:00Z",
            "finishedAt": "2026-03-30T00:05:00Z",
            "runtime": {},
            "contaminationAudit": {"totalRows": 1, "contaminatedRows": 0},
            "cityGarbageAudit": {
                "totalRows": 1,
                "garbageRows": 1,
                "fieldCounts": {"city": 1},
                "categoryCounts": {"site_chrome": 1},
                "examples": [],
            },
            "locationQualityAudit": {"totalRows": 1, "invalidLocationFieldCount": 0},
            "sectorQualityAudit": {"totalRows": 1, "downgradedGameSectorCount": 0},
        }
    )
    assert int(normalized_report["cityGarbageAudit"]["garbageRows"]) == 1
    assert int(normalized_report["cityGarbageAudit"]["fieldCounts"]["city"]) == 1
    assert int(normalized_report["cityGarbageAudit"]["categoryCounts"]["site_chrome"]) == 1
