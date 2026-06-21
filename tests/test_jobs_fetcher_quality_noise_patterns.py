"""Tests for jobs fetcher quality noise-pattern handling."""

from src import jobs_fetcher as jf
from src.jobs import canonicalize as jobs_canonicalize
from src.jobs.contamination_audit import build_location_quality_report
from tests.helpers import jobs_reporting


def test_canonicalize_job_with_reason_blanks_label_placeholder_location_noise() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Associate QA Coordinator United States",
            "company": "IllFonic",
            "city": "%LABEL_POSITION_TYPE_REMOTE_ANY%",
            "country": "Unknown",
            "locations": [
                {"city": "%LABEL_POSITION_TYPE_REMOTE_ANY%", "country": "Unknown"},
                {"city": "", "country": "US"},
            ],
            "jobLink": "https://illfonic.breezy.hr/p/06c96306a484-associate-qa-coordinator",
            "sector": "Tech",
        },
        source="static_source::static",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == ""
    assert payload["country"] == "US"
    assert payload["locationSummary"] == "US"
    assert payload["locations"] == [{"city": "", "country": "US"}]


def test_canonicalize_job_with_reason_blanks_role_blob_location_noise() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Lead Level Scripter Montréal CDI",
            "company": "Don't Nod",
            "city": "Administratif, Assistant, Gestion, RH...",
            "country": "Unknown",
            "locations": [
                {"city": "Administratif, Assistant, Gestion, RH...", "country": "Unknown"},
                {"city": "Paris", "country": "FR"},
            ],
            "jobLink": "https://jobs.smartrecruiters.com/DONTNOD/744000104833006",
            "sector": "Game",
        },
        source="static_source::static",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == "Paris"
    assert payload["country"] == "FR"
    assert payload["locationSummary"] == "Paris, FR"
    assert payload["locations"] == [{"city": "Paris", "country": "FR"}]


def test_canonicalize_job_with_reason_normalizes_sector_from_game_evidence() -> None:
    jobs_canonicalize.reset_sector_quality_audit()
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Assembler/Bike Builder",
            "company": "Trek",
            "city": "Bennetts Green",
            "country": "Australia",
            "jobLink": "https://trekbikes.wd1.myworkdayjobs.com/en-US/TREK/job/Bennetts-Green-NSW-Australia/Assembler-Bike-Builder_Trek113973-1",
            "sector": "Game",
        },
        source="google_sheets",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["sector"] == "Tech"
    audit = jobs_canonicalize.snapshot_sector_quality_audit(total_rows=1)
    assert int(audit["downgradedGameSectorCount"]) == 1
    assert audit["examples"][0]["rawSector"] == "Game"
    normalized_report = jobs_reporting.normalize_fetch_report_payload(
        {
            "runId": "sector-audit",
            "sectorQualityAudit": audit,
        }
    )
    assert int(normalized_report["sectorQualityAudit"]["downgradedGameSectorCount"]) == 1

    game_row, game_reason = jf.canonicalize_job_with_reason(
        {
            "title": "Gameplay Programmer",
            "company": "Studio Other",
            "city": "Amsterdam",
            "country": "Netherlands",
            "jobLink": "https://example.com/gameplay",
            "sector": "Tech",
        },
        source="google_sheets",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert game_reason == ""
    assert game_row is not None
    game_payload = game_row if isinstance(game_row, dict) else game_row.to_dict()
    assert game_payload["sector"] == "Game"
    assert (
        int(
            jobs_canonicalize.snapshot_sector_quality_audit(total_rows=2)[
                "downgradedGameSectorCount"
            ]
        )
        == 1
    )

    zynga_row, zynga_reason = jf.canonicalize_job_with_reason(
        {
            "title": "Marketing Artist",
            "company": "Zynga",
            "city": "San Francisco",
            "country": "US",
            "jobLink": "https://job-boards.greenhouse.io/zyngacareers/jobs/5835998004",
            "sector": "Tech",
            "sourceBundle": [
                {
                    "source": "greenhouse_boards",
                    "sourceJobId": "greenhouse:zyngacareers:5835998004",
                    "jobLink": "https://job-boards.greenhouse.io/zyngacareers/jobs/5835998004",
                    "postedAt": "2026-03-27T11:09:42+00:00",
                    "adapter": "greenhouse",
                    "studio": "Zynga",
                }
            ],
        },
        source="greenhouse_boards",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert zynga_reason == ""
    assert zynga_row is not None
    zynga_payload = zynga_row if isinstance(zynga_row, dict) else zynga_row.to_dict()
    assert zynga_payload["sector"] == "Game"
    assert zynga_payload["companyType"] == "Game"

    gameloft_row, gameloft_reason = jf.canonicalize_job_with_reason(
        {
            "title": "[Dungeons & Dragons PC-Console] Artiste d'éclairage de niveaux - Lighter level artist",
            "company": "Gameloft",
            "city": "Montreal",
            "country": "Canada",
            "jobLink": "https://jobs.smartrecruiters.com/Gameloft/744000115751281",
            "sector": "Tech",
        },
        source="smartrecruiters_sources",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert gameloft_reason == ""
    assert gameloft_row is not None
    gameloft_payload = gameloft_row if isinstance(gameloft_row, dict) else gameloft_row.to_dict()
    assert gameloft_payload["sector"] == "Game"
    assert gameloft_payload["companyType"] == "Game"

    cloud_row, cloud_reason = jf.canonicalize_job_with_reason(
        {
            "title": "Senior Gameplay Programmer",
            "company": "Cloud Chamber",
            "city": "Montreal",
            "country": "Canada",
            "jobLink": "https://example.com/cloud-chamber/senior-gameplay-programmer",
            "sector": "Tech",
            "sourceBundle": [
                {
                    "source": "greenhouse_boards",
                    "sourceJobId": "greenhouse:cloudchamberen:7655929003",
                    "jobLink": "https://job-boards.greenhouse.io/cloudchamberen/jobs/7655929003",
                    "postedAt": "2026-03-16T15:18:09+00:00",
                    "adapter": "greenhouse",
                    "studio": "Cloud Chamber",
                }
            ],
        },
        source="google_sheets",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert cloud_reason == ""
    assert cloud_row is not None
    cloud_payload = cloud_row if isinstance(cloud_row, dict) else cloud_row.to_dict()
    assert cloud_payload["sector"] == "Game"
    assert cloud_payload["companyType"] == "Game"

    sega_me_row, sega_me_reason = jf.canonicalize_job_with_reason(
        {
            "title": "Projects /After sales services Engineer",
            "company": "SEGA",
            "city": "10th of Ramadan",
            "country": "Unknown",
            "jobLink": "https://eg.linkedin.com/jobs/view/projects-after-sales-services-engineer-at-%E2%80%8F%E2%80%8Esega-m-electrical-products%E2%80%8E-4399033334",
            "sector": "Tech",
            "sourceBundle": [
                {
                    "source": "static_source::static:listing_url:https://www.linkedin.com/jobs/search/?currentjobid=4148163061&geoid=92000000&keywords=sega",
                    "sourceJobId": "static:static:listing_url:https://www.linkedin.com/jobs/search/?currentjobid=4148163061&geoid=92000000&keywords=sega:55d919920a",
                    "jobLink": "https://eg.linkedin.com/jobs/view/projects-after-sales-services-engineer-at-%E2%80%8F%E2%80%8Esega-m-electrical-products%E2%80%8E-4399033334",
                    "postedAt": "",
                    "adapter": "static",
                    "studio": "SEGA",
                }
            ],
        },
        source="static_source::static:listing_url:https://www.linkedin.com/jobs/search/?currentjobid=4148163061&geoid=92000000&keywords=sega",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert sega_me_reason == ""
    assert sega_me_row is not None
    sega_me_payload = sega_me_row if isinstance(sega_me_row, dict) else sega_me_row.to_dict()
    assert sega_me_payload["sector"] == "Tech"
    assert sega_me_payload["companyType"] == "Tech"


def test_canonicalize_job_with_reason_blanks_title_like_city_noise() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Artist",
            "company": "Studio",
            "city": "2D Artist, Bombergrounds",
            "country": "31-621 Kraków, Poland",
            "jobLink": "https://example.com/title-noise",
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


def test_canonicalize_job_with_reason_blanks_composite_and_script_city_noise() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Artist",
            "company": "Studio",
            "city": "Berlin / Hamburg",
            "country": 'document.addEventListener("DOMContentLoaded", function () {',
            "jobLink": "https://example.com/composite-noise",
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


def test_location_quality_audit_reports_semantic_location_examples() -> None:
    report = build_location_quality_report(
        [
            {
                "title": "Clean",
                "company": "Studio",
                "city": "Paris",
                "country": "France",
                "jobLink": "https://example.com/1",
            },
            {
                "title": "Growth Marketing Intern",
                "company": "Sleeper",
                "city": "Remote, United States; San Francisco Area, United States Remote; New York City; Los Angeles",
                "country": "Unknown",
                "source": "ashby_sources",
                "jobLink": "https://example.com/2",
            },
            {
                "title": "Artist",
                "company": "Studio",
                "city": "6,559 followers",
                "country": "--grid-gutter: calc(var(--sqs-mobile-site-gutter, 6vw) - 0.0px);",
                "source": "static_source::noise",
                "jobLink": "https://example.com/3",
            },
            {
                "title": "Artist",
                "company": "Studio",
                "city": "2D Artist, Bombergrounds",
                "country": "31-621 Kraków, Poland",
                "source": "static_source::noise",
                "jobLink": "https://example.com/4",
            },
            {
                "title": "Artist",
                "company": "Studio",
                "city": "Berlin / Hamburg",
                "country": 'document.addEventListener("DOMContentLoaded", function () {',
                "source": "static_source::noise",
                "jobLink": "https://example.com/5",
            },
        ]
    )
    assert int(report["invalidLocationFieldCount"]) == 7
    assert int(report["fieldCounts"]["city"]) == 4
    assert int(report["fieldCounts"]["country"]) == 3
    assert (
        str(report["examples"][0]["fields"]["city"]["reason"])
        == "invalid_city_semantic_multi_location_blob"
    )
    assert str(report["examples"][1]["fields"]["city"]["reason"]) == "invalid_city_semantic_noise"
    assert (
        str(report["examples"][1]["fields"]["country"]["reason"])
        == "invalid_country_semantic_noise"
    )
    assert str(report["examples"][2]["fields"]["city"]["reason"]) == "invalid_city_semantic_noise"
    assert (
        str(report["examples"][2]["fields"]["country"]["reason"])
        == "invalid_country_semantic_noise"
    )
    assert str(report["examples"][3]["fields"]["city"]["reason"]) == "invalid_city_semantic_noise"
    assert (
        str(report["examples"][3]["fields"]["country"]["reason"])
        == "invalid_country_semantic_noise"
    )
