"""Tests for jobs fetcher providers individual parser fixtures."""

import json

from src import jobs_fetcher as jf
from tests.helpers.job_fixtures import _fixture


def test_parse_teamtailor_listing_links_fixture() -> None:
    rows = jf.parse_teamtailor_listing_links(
        _fixture("teamtailor_listing.html"),
        base_url="https://career.paradoxplaza.com",
    )
    assert len(rows) == 2
    assert all("/jobs/" in row for row in rows)
    assert all("show_more" not in row for row in rows)


def test_parse_jobpostings_from_html_teamtailor_fixture() -> None:
    rows = jf.parse_jobpostings_from_html(
        _fixture("teamtailor_job.html"),
        base_url="https://career.paradoxplaza.com/jobs/6926996-game-programmer",
        fallback_company="Paradox Interactive",
        fallback_source_id_prefix="teamtailor:test",
    )
    assert len(rows) == 1
    assert rows[0]["title"] == "Game Programmer"
    assert rows[0]["city"] == ""
    assert rows[0]["country"] == "Unknown"


def test_parse_wellfound_html_fixture() -> None:
    rows = jf.parse_wellfound_html(_fixture("wellfound.html"))
    assert len(rows) == 1
    assert rows[0]["sourceJobId"] == "wf-1"
    assert rows[0]["workType"] == "Remote"


def test_parse_smartrecruiters_jobs_payload_fixture() -> None:
    payload = json.loads(_fixture("smartrecruiters_jobs.json"))
    rows = jf.parse_smartrecruiters_jobs_payload(
        payload, "CDPROJEKTRED", fallback_company="CD PROJEKT RED"
    )
    assert len(rows) == 1
    assert rows[0]["title"] == "Environment Artist"
    assert rows[0]["company"] == "CD PROJEKT RED"
    assert rows[0]["jobLink"] == "https://jobs.smartrecruiters.com/CDPROJEKTRED/environment-artist"


def test_parse_smartrecruiters_jobs_payload_rewrites_api_ref_to_public_job_url() -> None:
    payload = {
        "content": [
            {
                "id": "744000115751281",
                "name": "[Dungeons & Dragons PC-Console] Artiste d'éclairage de niveaux - Lighter level artist",
                "ref": "https://api.smartrecruiters.com/v1/companies/Gameloft/postings/744000115751281",
                "releasedDate": "2026-02-20T10:00:00Z",
                "location": {"city": "Montreal", "country": "CA"},
                "department": "Art",
            }
        ]
    }
    rows = jf.parse_smartrecruiters_jobs_payload(payload, "Gameloft", fallback_company="Gameloft")
    assert len(rows) == 1
    assert rows[0]["jobLink"] == "https://jobs.smartrecruiters.com/Gameloft/744000115751281"


def test_parse_smartrecruiters_jobs_payload_normalizes_location_variants() -> None:
    payload = {
        "content": [
            {
                "id": "744000115751282",
                "name": "Gameplay Programmer",
                "ref": "744000115751282",
                "releasedDate": "2026-02-20T10:00:00Z",
                "location": {"city": "Vancouver, CA | CA", "country": "CA"},
                "department": "Engineering",
            }
        ]
    }
    rows = jf.parse_smartrecruiters_jobs_payload(payload, "Studio", fallback_company="Studio")
    assert len(rows) == 1
    assert rows[0]["city"] == "Vancouver"
    assert rows[0]["country"] == "CA"
    assert rows[0]["locations"] == [{"city": "Vancouver", "country": "CA"}]
    assert rows[0]["locationSummary"] == "Vancouver, CA"


def test_parse_lever_jobs_payload_normalizes_multi_location_strings() -> None:
    payload = [
        {
            "id": "lever-1",
            "text": "Gameplay Programmer",
            "hostedUrl": "https://example.com/jobs/lever-1",
            "categories": {
                "location": "Munich, DE | München, DE",
            },
            "descriptionPlain": "Hiring a gameplay programmer for the game team.",
        }
    ]
    rows = jf.parse_lever_jobs_payload(payload, "studio", fallback_company="Studio")
    assert len(rows) == 1
    assert rows[0]["city"] == "Munich"
    assert rows[0]["country"] == "DE"
    assert rows[0]["locations"] == [{"city": "Munich", "country": "DE"}]
    assert rows[0]["locationSummary"] == "Munich, DE"


def test_parse_workable_jobs_payload_fixture() -> None:
    payload = json.loads(_fixture("workable_jobs.json"))
    rows = jf.parse_workable_jobs_payload(payload, "hutch", fallback_company="Hutch")
    assert len(rows) == 1
    assert rows[0]["workType"] == "Remote"


def test_parse_workable_jobs_payload_uses_top_level_location_fields() -> None:
    payload = {
        "name": "Vertigo",
        "jobs": [
            {
                "shortcode": "3F55933E05",
                "title": "3D Environment Artist",
                "url": "https://apply.workable.com/j/3F55933E05",
                "published": "2026-02-02",
                "created_at": "2026-02-02",
                "city": "Istanbul",
                "country": "Turkey",
                "locations": [
                    {
                        "city": "Istanbul",
                        "country": "Turkey",
                        "countryCode": "TR",
                        "region": "Istanbul",
                        "hidden": False,
                    }
                ],
                "description": "We're now looking for a passionate 3D Environment Artist to join our dynamic team in Istanbul. This is an on-site role.",
                "telecommuting": False,
            }
        ],
    }

    rows = jf.parse_workable_jobs_payload(payload, "vertigogames", fallback_company="Vertigo")
    assert len(rows) == 1
    row = rows[0]
    assert row["city"] == "Istanbul"
    assert row["country"] == "TR"
    assert row["locations"] == [{"city": "Istanbul", "country": "TR"}]
    assert row["locationSummary"] == "Istanbul, TR"
    assert row["workType"] == ""


def test_parse_workable_jobs_payload_falls_back_to_description_location() -> None:
    payload = {
        "name": "Goliath Games",
        "jobs": [
            {
                "shortcode": "goliath-1",
                "title": "Package Designer",
                "url": "https://apply.workable.com/goliath/j/goliath-1/",
                "published": "2026-02-02",
                "created_at": "2026-02-02",
                "department": "Tech",
                "description": "We are hiring a Package Designer for our team in Norristown, Pennsylvania, United States. This is an onsite role.",
                "telecommuting": False,
            }
        ],
    }

    rows = jf.parse_workable_jobs_payload(payload, "goliath", fallback_company="Goliath Games")
    assert len(rows) == 1
    row = rows[0]
    assert row["city"] == "Norristown"
    assert row["country"] == "US"
    assert row["locations"] == [{"city": "Norristown", "country": "US"}]
    assert row["locationSummary"] == "Norristown, US"


def test_parse_workable_jobs_payload_maps_state_abbreviations_in_description_location() -> None:
    payload = {
        "name": "Example Games",
        "jobs": [
            {
                "shortcode": "example-1",
                "title": "Gameplay Engineer",
                "url": "https://apply.workable.com/example/j/example-1/",
                "department": "Engineering",
                "description": "Join our game team in San Francisco, CA. This is an onsite role.",
                "telecommuting": False,
            }
        ],
    }

    rows = jf.parse_workable_jobs_payload(payload, "example", fallback_company="Example Games")
    assert len(rows) == 1
    row = rows[0]
    assert row["city"] == "San Francisco"
    assert row["country"] == "US"
    assert row["locations"] == [{"city": "San Francisco", "country": "US"}]
    assert row["locationSummary"] == "San Francisco, US"


def test_parse_greenhouse_jobs_payload_falls_back_to_description_location() -> None:
    payload = {
        "jobs": [
            {
                "id": 12345,
                "title": "Senior Quest Designer - Varsapura",
                "company_name": "HoYoverse",
                "absolute_url": "https://boards.greenhouse.io/hoyoverse/jobs/12345",
                "location": {"name": ""},
                "content": "<p>Location: Tokyo, Japan</p><p>Onsite role.</p>",
                "first_published": "2026-03-01T10:00:00Z",
            }
        ]
    }

    rows = jf.parse_greenhouse_jobs_payload(payload, "hoyoverse", fallback_company="HoYoverse")
    assert len(rows) == 1
    row = rows[0]
    assert row["city"] == "Tokyo"
    assert row["country"] == "Japan"
    assert row["locations"] == [{"city": "Tokyo", "country": "Japan"}]
    assert row["locationSummary"] == "Tokyo, Japan"


def test_parse_pinpoint_jobs_payload_falls_back_to_description_location() -> None:
    payload = {
        "data": [
            {
                "id": "ho-1",
                "title": "Senior Gameplay Engineer",
                "url": "https://gameplaygalaxy.pinpointhq.com/postings/ho-1",
                "location": {"name": ""},
                "workplace_type_text": "Onsite",
                "employment_type_text": "Full Time",
                "job": {"department": {"name": "Design"}},
                "description": "<p>Location: Tokyo, Japan</p><p>Onsite role.</p>",
            }
        ]
    }

    rows = jf.parse_pinpoint_jobs_payload(payload, "hoyoverse", fallback_company="HoYoverse")
    assert len(rows) == 1
    row = rows[0]
    assert row["city"] == "Tokyo"
    assert row["country"] == "Japan"
    assert row["locations"] == [{"city": "Tokyo", "country": "Japan"}]
    assert row["locationSummary"] == "Tokyo, Japan"


def test_parse_ashby_jobs_from_html_fixture() -> None:
    rows = jf.parse_ashby_jobs_from_html(
        _fixture("ashby_jobs.html"), "https://jobs.ashbyhq.com/jagex/jobs", "Jagex"
    )
    assert len(rows) == 2
    assert {row["jobLink"].split("/", 3)[2] for row in rows} == {"jobs.ashbyhq.com"}


def test_parse_ashby_jobs_from_embedded_careers_links() -> None:
    html = """
        <div>
          <a href="https://thatgamecompany.com/careers/?ashby_jid=7ea5dd25-3fcb-4d42-8217-89dd9b6f5083#/">
            <h3>Senior 3D Environment Artist</h3>
          </a>
          <a href="https://thatgamecompany.com/careers/?ashby_jid=b1a491f9-fb8f-44fa-a511-818525dee8a9#/">
            Gameplay Engineer
          </a>
        </div>
        """
    rows = jf.parse_ashby_jobs_from_html(
        html, "https://jobs.ashbyhq.com/thatgamecompany/jobs", "thatgamecompany"
    )
    assert len(rows) == 2
    assert any(str(row.get("title") or "") == "Senior 3D Environment Artist" for row in rows)
    assert any("ashby_jid=" in str(row.get("jobLink") or "") for row in rows)


def test_parse_ashby_jobs_from_hosted_board_root_links() -> None:
    html = """
        <div>
          <a href="/thatgamecompany/7ea5dd25-3fcb-4d42-8217-89dd9b6f5083">
            <h3>Senior 3D Environment Artist</h3>
          </a>
          <a href="/thatgamecompany/b1a491f9-fb8f-44fa-a511-818525dee8a9">
            Gameplay Engineer
          </a>
        </div>
        """
    rows = jf.parse_ashby_jobs_from_html(
        html, "https://jobs.ashbyhq.com/thatgamecompany", "thatgamecompany"
    )
    assert len(rows) == 2
    assert any(
        str(row.get("jobLink") or "").endswith(
            "/thatgamecompany/7ea5dd25-3fcb-4d42-8217-89dd9b6f5083"
        )
        for row in rows
    )


def test_parse_ashby_jobs_from_embedded_app_data() -> None:
    html = """
        <script>
        window.__appData = {
          "organization": {"name": "thatgamecompany"},
          "jobBoard": {
            "jobPostings": [
              {
                "id": "7ea5dd25-3fcb-4d42-8217-89dd9b6f5083",
                "title": "Senior Frontend Engineer",
                "locationName": "Remote - US",
                "workplaceType": "Remote",
                "employmentType": "FullTime",
                "publishedDate": "2026-03-20"
              }
            ]
          }
        };
        </script>
        """
    rows = jf.parse_ashby_jobs_from_html(
        html, "https://jobs.ashbyhq.com/thatgamecompany/jobs", "thatgamecompany"
    )
    assert len(rows) == 1
    assert (
        rows[0]["jobLink"]
        == "https://jobs.ashbyhq.com/thatgamecompany/7ea5dd25-3fcb-4d42-8217-89dd9b6f5083"
    )
    assert rows[0]["contractType"] == "Full Time"
    assert rows[0]["workType"] == "Remote"
    assert rows[0]["title"] == "Senior Frontend Engineer"


def test_non_ashby_provider_keeps_game_keyword_filter_strict() -> None:
    payload = [
        {
            "id": "job-1",
            "text": "Senior Frontend Engineer",
            "hostedUrl": "https://jobs.lever.co/example/job-1",
            "categories": {"location": "Remote", "team": "Engineering", "commitment": "Full-time"},
        }
    ]
    rows = jf.parse_lever_jobs_payload(payload, "example", fallback_company="Example Tech")
    assert rows == []


def test_parse_breezy_jobs_html_fixture() -> None:
    rows = jf.parse_breezy_jobs_html(
        _fixture("breezy_jobs.html"), "https://yallaplay.breezy.hr/", "YallaPlay"
    )
    assert len(rows) == 2
    assert all(row["company"] == "YallaPlay" for row in rows)
    assert any(row["workType"] == "Remote" for row in rows)


def test_parse_jazzhr_jobs_html_fixture() -> None:
    rows = jf.parse_jazzhr_jobs_html(
        _fixture("jazzhr_jobs.html"),
        "https://lostboysinteractive.applytojob.com/apply",
        "Lost Boys Interactive",
    )
    assert len(rows) == 2
    assert all(row["company"] == "Lost Boys Interactive" for row in rows)
    assert any(row["contractType"] == "Full Time" for row in rows)


def test_parse_recruitee_jobs_payload_fixture() -> None:
    payload = json.loads(_fixture("recruitee_jobs.json"))
    rows = jf.parse_recruitee_jobs_payload(
        payload,
        "jobs.crazygames.com",
        fallback_company="CrazyGames",
    )
    assert len(rows) == 2
    assert all(row["company"] == "CrazyGames" for row in rows)
    assert any(row["workType"] == "Remote" for row in rows)


def test_parse_pinpoint_jobs_payload_fixture() -> None:
    payload = json.loads(_fixture("pinpoint_jobs.json"))
    rows = jf.parse_pinpoint_jobs_payload(
        payload,
        "gameplaygalaxy",
        fallback_company="Gameplay Galaxy",
    )
    assert len(rows) == 2
    assert all(row["company"] == "Gameplay Galaxy" for row in rows)
    assert any(row["workType"] == "Remote" for row in rows)


def test_parse_personio_feed_xml_fixture() -> None:
    rows = jf.parse_personio_feed_xml(_fixture("personio_feed.xml"), source_name="InnoGames")
    assert len(rows) >= 1
    assert any(row["title"] == "Environment Artist" for row in rows)
