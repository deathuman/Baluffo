import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from unittest import mock

import pytest

from tests.jobs_fetcher_helpers import (
    AdapterValidationError,
    _fixture,
    jf,
    jobs_registry,
)


def test_parse_remote_ok_payload_filters_game_roles() -> None:
    payload = json.loads(_fixture("remoteok.json"))
    rows = jf.parse_remote_ok_payload(payload)
    assert len(rows) == 1
    assert rows[0]["sourceJobId"] == "101"
    assert rows[0]["company"] == "Nebula Games"


def test_parse_reddit_json_payload_filters_and_normalizes() -> None:
    payload = {
        "data": {
            "children": [
                {
                    "data": {
                        "id": "abc123",
                        "title": "We're hiring a Unity Technical Artist at Nebula Games",
                        "selftext": "Remote role. Apply https://jobs.nebula.dev/ta",
                        "link_flair_text": "Hiring",
                        "permalink": "/r/gamedev/comments/abc123/test/",
                        "url": "https://www.reddit.com/r/gamedev/comments/abc123/test/",
                        "created_utc": 1700000000,
                        "author": "nebula_hr",
                    }
                },
                {
                    "data": {
                        "id": "zzz999",
                        "title": "For hire - Unity dev available",
                        "selftext": "Open to work",
                        "link_flair_text": "For Hire",
                        "permalink": "/r/gamedev/comments/zzz999/test/",
                        "url": "https://www.reddit.com/r/gamedev/comments/zzz999/test/",
                        "created_utc": 1700000000,
                        "author": "someone",
                    }
                },
            ]
        }
    }
    rows, dropped = jf.parse_reddit_json_payload(
        payload,
        subreddit="gamedev",
        min_confidence=20,
        reject_for_hire_posts=True,
    )
    assert len(rows) == 1
    assert dropped >= 1
    assert rows[0]["company"] == "Nebula Games"
    assert "jobs.nebula.dev" in rows[0]["jobLink"]


def test_parse_x_payload_and_mastodon_payload() -> None:
    x_rows, x_dropped = jf.parse_x_payload(
        {
            "data": [
                {
                    "id": "987",
                    "text": "We're hiring an Unreal Programmer at Pixel Forge. Apply https://jobs.pixelforge.dev/u",
                    "created_at": "2026-03-09T11:00:00Z",
                }
            ]
        },
        query_label="#gamedevjobs",
        min_confidence=20,
        reject_for_hire_posts=True,
    )
    assert len(x_rows) == 1
    assert x_dropped == 0
    assert "pixelforge" in x_rows[0]["jobLink"].lower()

    mastodon_rows, mastodon_dropped = jf.parse_mastodon_payload(
        [
            {
                "id": "m1",
                "content": "<p>We are hiring technical artists at Aurora Games. Apply https://careers.aurora.dev/ta</p>",
                "created_at": "2026-03-09T11:05:00Z",
                "url": "https://mastodon.gamedev.place/@aurora/111",
                "account": {"display_name": "Aurora Games"},
            }
        ],
        instance="https://mastodon.gamedev.place",
        tag="gamedevjobs",
        min_confidence=20,
        reject_for_hire_posts=True,
    )
    assert len(mastodon_rows) == 1
    assert mastodon_dropped == 0
    assert "aurora.dev" in mastodon_rows[0]["jobLink"]


def test_parse_x_rss_payload() -> None:
    rss = """<?xml version="1.0" encoding="UTF-8"?>
<rss><channel>
  <item>
    <title>We're hiring a Unity Engineer at Orbit Games</title>
    <link>https://nitter.net/orbit/status/123</link>
    <description>Apply here https://jobs.orbit.dev/unity</description>
    <pubDate>Mon, 09 Mar 2026 11:00:00 GMT</pubDate>
  </item>
</channel></rss>"""
    rows, dropped = jf.parse_x_rss_payload(
        rss,
        query_label="#gamedevjobs",
        min_confidence=20,
        reject_for_hire_posts=True,
    )
    assert len(rows) == 1
    assert dropped == 0
    assert "jobs.orbit.dev" in rows[0]["jobLink"]


def test_fingerprint_url_keeps_language_query_significant_for_non_personio_urls() -> None:
    from src.jobs.common.url import fingerprint_url
    from src.jobs.text_utils import normalize_url

    base = "https://example.com/jobs/1317878"
    with_lang = "https://example.com/jobs/1317878?language=en"
    assert fingerprint_url(with_lang) != fingerprint_url(base)
    assert normalize_url(with_lang) == with_lang


def test_fingerprint_url_matches_smartrecruiters_short_and_slugged_urls() -> None:
    short = "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145"
    slugged = "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-technical-director-level-design-m-f-nb-projet-non-annonce"
    api = "https://api.smartrecruiters.com/v1/companies/Ubisoft2/postings/744000108777145"
    assert jf.fingerprint_url(short) == jf.fingerprint_url(slugged)
    assert jf.fingerprint_url(short) == jf.fingerprint_url(api)


@dataclass(frozen=True)
class _FixtureParseCase:
    name: str
    parser: Callable[..., list[dict[str, Any]]]
    fixture_name: str
    loader: Callable[[str], Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    expected_len: int
    at_least: bool = False
    extra_check: Callable[[list[dict[str, Any]]], None] = lambda rows: None


def _assert_gamesindustry(rows: list[dict[str, Any]]) -> None:
    assert rows[0]["title"] == "Senior Quality Analyst"
    assert rows[0]["company"] == "Sharkmob"
    assert rows[0]["sourceJobId"] == "43821"
    assert rows[0]["jobLink"].startswith("https://jobs.gamesindustry.biz/job/")
    titles = {row["title"] for row in rows}
    assert "Read more" not in titles
    assert "Programming (6)" not in titles


def test_parse_gamesindustry_html_extracts_company_and_city_from_real_43784_listing() -> None:
    html = _fixture("gamesindustry_job_43784_listing.html")
    rows = jf.parse_gamesindustry_html(html, "https://jobs.gamesindustry.biz")

    assert len(rows) == 1
    assert rows[0]["title"] == "Senior Level Designer [Fixed Term]"
    assert rows[0]["company"] == "Avalanche Studios"
    assert rows[0]["city"] == "Stockholm"
    assert rows[0]["country"] == "SE"
    assert rows[0]["sourceJobId"] == "43784"


def _assert_greenhouse(rows: list[dict[str, Any]]) -> None:
    assert all(row["sourceJobId"].startswith("greenhouse:guerrilla-games:") for row in rows)
    assert rows[0]["company"] == "Guerrilla Games"
    assert rows[0]["country"] == "NL"


def _assert_lever(rows: list[dict[str, Any]]) -> None:
    assert rows[0]["title"] == "Technical Artist"
    assert rows[0]["country"] == "NL"


def _assert_workable(rows: list[dict[str, Any]]) -> None:
    assert rows[0]["workType"] == "Remote"


def _assert_breezy(rows: list[dict[str, Any]]) -> None:
    assert all(row["company"] == "YallaPlay" for row in rows)
    assert any(row["workType"] == "Remote" for row in rows)


def _assert_jazzhr(rows: list[dict[str, Any]]) -> None:
    assert all(row["company"] == "Lost Boys Interactive" for row in rows)
    assert any(row["contractType"] == "Full Time" for row in rows)


def _assert_recruitee(rows: list[dict[str, Any]]) -> None:
    assert all(row["company"] == "CrazyGames" for row in rows)
    assert any(row["workType"] == "Remote" for row in rows)


def _assert_pinpoint(rows: list[dict[str, Any]]) -> None:
    assert all(row["company"] == "Gameplay Galaxy" for row in rows)
    assert any(row["workType"] == "Remote" for row in rows)


def _assert_personio(rows: list[dict[str, Any]]) -> None:
    assert any(row["title"] == "Environment Artist" for row in rows)


FIXTURE_PARSE_CASES = [
    pytest.param(
        _FixtureParseCase(
            name="gamesindustry",
            parser=jf.parse_gamesindustry_html,
            fixture_name="gamesindustry_jobs.html",
            loader=lambda text: text,
            args=("https://jobs.gamesindustry.biz",),
            kwargs={},
            expected_len=2,
            extra_check=_assert_gamesindustry,
        ),
        id="gamesindustry",
    ),
    pytest.param(
        _FixtureParseCase(
            name="greenhouse",
            parser=jf.parse_greenhouse_jobs_payload,
            fixture_name="greenhouse_guerrilla_jobs.json",
            loader=json.loads,
            args=("guerrilla-games",),
            kwargs={},
            expected_len=2,
            extra_check=_assert_greenhouse,
        ),
        id="greenhouse",
    ),
    pytest.param(
        _FixtureParseCase(
            name="lever",
            parser=jf.parse_lever_jobs_payload,
            fixture_name="lever_jobs.json",
            loader=json.loads,
            args=("sandboxvr",),
            kwargs={"fallback_company": "Sandbox VR"},
            expected_len=1,
            extra_check=_assert_lever,
        ),
        id="lever",
    ),
    pytest.param(
        _FixtureParseCase(
            name="workable",
            parser=jf.parse_workable_jobs_payload,
            fixture_name="workable_jobs.json",
            loader=json.loads,
            args=("hutch",),
            kwargs={"fallback_company": "Hutch"},
            expected_len=1,
            extra_check=_assert_workable,
        ),
        id="workable",
    ),
    pytest.param(
        _FixtureParseCase(
            name="breezy",
            parser=jf.parse_breezy_jobs_html,
            fixture_name="breezy_jobs.html",
            loader=lambda text: text,
            args=("https://yallaplay.breezy.hr/", "YallaPlay"),
            kwargs={},
            expected_len=2,
            extra_check=_assert_breezy,
        ),
        id="breezy",
    ),
    pytest.param(
        _FixtureParseCase(
            name="jazzhr",
            parser=jf.parse_jazzhr_jobs_html,
            fixture_name="jazzhr_jobs.html",
            loader=lambda text: text,
            args=("https://lostboysinteractive.applytojob.com/apply", "Lost Boys Interactive"),
            kwargs={},
            expected_len=2,
            extra_check=_assert_jazzhr,
        ),
        id="jazzhr",
    ),
    pytest.param(
        _FixtureParseCase(
            name="recruitee",
            parser=jf.parse_recruitee_jobs_payload,
            fixture_name="recruitee_jobs.json",
            loader=json.loads,
            args=("jobs.crazygames.com",),
            kwargs={"fallback_company": "CrazyGames"},
            expected_len=2,
            extra_check=_assert_recruitee,
        ),
        id="recruitee",
    ),
    pytest.param(
        _FixtureParseCase(
            name="pinpoint",
            parser=jf.parse_pinpoint_jobs_payload,
            fixture_name="pinpoint_jobs.json",
            loader=json.loads,
            args=("gameplaygalaxy",),
            kwargs={"fallback_company": "Gameplay Galaxy"},
            expected_len=2,
            extra_check=_assert_pinpoint,
        ),
        id="pinpoint",
    ),
    pytest.param(
        _FixtureParseCase(
            name="personio",
            parser=jf.parse_personio_feed_xml,
            fixture_name="personio_feed.xml",
            loader=lambda text: text,
            args=(),
            kwargs={"source_name": "InnoGames"},
            expected_len=1,
            at_least=True,
            extra_check=_assert_personio,
        ),
        id="personio",
    ),
]


@pytest.mark.parametrize("case", FIXTURE_PARSE_CASES, ids=lambda case: case.name)
def test_parse_fixture_provider_payloads(case: _FixtureParseCase) -> None:
    loaded = case.loader(_fixture(case.fixture_name))
    rows = case.parser(loaded, *case.args, **case.kwargs)
    if case.at_least:
        assert len(rows) >= case.expected_len
    else:
        assert len(rows) == case.expected_len
    case.extra_check(rows)


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
    assert all("jobs.ashbyhq.com" in row["jobLink"] for row in rows)


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


def test_run_ashby_sources_source_falls_back_to_careers_page_when_board_is_stale() -> None:
    from src.jobs.adapters.plugins.provider_api import html_board as html_board_module

    source_rows = [
        {
            "name": "thatgamecompany (Ashby)",
            "studio": "thatgamecompany",
            "adapter": "ashby",
            "board_url": "https://jobs.ashbyhq.com/thatgamecompany/jobs",
            "careersUrl": "https://thatgamecompany.com/careers/",
            "enabledByDefault": True,
        }
    ]

    class _Deps:
        def registry_entries(self, adapter: str):
            assert adapter == "ashby"
            return source_rows

        def fetch_with_retries(
            self, url: str, fetch_text, timeout_s: int, retries: int, backoff_s: float
        ) -> str:
            return fetch_text(url, timeout_s)

        def set_source_diagnostics(self, source_name: str, **kwargs) -> None:
            return None

    deps = _Deps()
    with (
        mock.patch.object(html_board_module, "registry_entries", deps.registry_entries),
        mock.patch.object(html_board_module, "fetch_with_retries", deps.fetch_with_retries),
        mock.patch.object(html_board_module, "set_source_diagnostics", deps.set_source_diagnostics),
    ):

        def fake_fetch(url: str, _: int) -> str:
            if url == "https://jobs.ashbyhq.com/thatgamecompany/jobs":
                return "<html><body><h1>Job not found</h1><a href='/'>View all open positions</a></body></html>"
            if url == "https://jobs.ashbyhq.com/thatgamecompany":
                return "<html><body><h1>Page not found</h1></body></html>"
            if url == "https://thatgamecompany.com/careers/":
                return """
                    <a href="https://thatgamecompany.com/careers/?ashby_jid=7ea5dd25-3fcb-4d42-8217-89dd9b6f5083#/">
                      Senior 3D Environment Artist
                    </a>
                    """
                raise AssertionError(f"unexpected url {url}")

        rows = jf.run_ashby_sources_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        assert len(rows) == 1
        assert str(rows[0].get("title") or "") == "Senior 3D Environment Artist"


def test_run_ashby_sources_source_normalizes_stale_jobs_url_to_board_root() -> None:
    from src.jobs.adapters.plugins.provider_api import html_board as html_board_module

    source_rows = [
        {
            "name": "thatgamecompany (Ashby)",
            "studio": "thatgamecompany",
            "adapter": "ashby",
            "board_url": "https://jobs.ashbyhq.com/thatgamecompany/jobs",
            "enabledByDefault": True,
        }
    ]

    class _Deps:
        def registry_entries(self, adapter: str):
            assert adapter == "ashby"
            return source_rows

        def fetch_with_retries(
            self, url: str, fetch_text, timeout_s: int, retries: int, backoff_s: float
        ) -> str:
            return fetch_text(url, timeout_s)

        def set_source_diagnostics(self, source_name: str, **kwargs) -> None:
            return None

    deps = _Deps()
    with (
        mock.patch.object(html_board_module, "registry_entries", deps.registry_entries),
        mock.patch.object(html_board_module, "fetch_with_retries", deps.fetch_with_retries),
        mock.patch.object(html_board_module, "set_source_diagnostics", deps.set_source_diagnostics),
    ):

        def fake_fetch(url: str, _: int) -> str:
            # The code tries multiple candidate URLs - first the original, then normalized
            if url == "https://jobs.ashbyhq.com/thatgamecompany/jobs":
                # Original URL returns "Job not found" - triggers fallback to next candidate
                return "<html><body><h1>Job not found</h1></body></html>"
            if url == "https://jobs.ashbyhq.com/thatgamecompany":
                # Normalized URL returns actual job
                return """
                    <a href="/thatgamecompany/7ea5dd25-3fcb-4d42-8217-89dd9b6f5083">
                      Senior 3D Environment Artist
                    </a>
                    """
            raise AssertionError(f"unexpected url {url}")

        rows = jf.run_ashby_sources_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        assert len(rows) == 1
        assert str(rows[0].get("jobLink") or "").endswith(
            "/thatgamecompany/7ea5dd25-3fcb-4d42-8217-89dd9b6f5083"
        )


def test_run_personio_sources_source_classifies_dead_marketing_redirect() -> None:
    source_rows = [
        {
            "name": "InnoGames (Personio)",
            "studio": "InnoGames",
            "adapter": "personio",
            "feed_url": "https://innogames.jobs.personio.de/xml",
            "enabledByDefault": True,
        }
    ]
    with mock.patch("src.jobs.adapters.provider_api.registry_entries", return_value=source_rows):
        jf.SOURCE_DIAGNOSTICS.clear()
        rows = jf.run_personio_sources_source(
            fetch_text=lambda _url, _timeout: (
                "<html><body><h1>HR und Lohnbuchhaltung endlich vereint</h1></body></html>"
            ),
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )
        assert rows == []
        detail = ((jf.SOURCE_DIAGNOSTICS.get("personio_sources") or {}).get("details") or [{}])[0]
        assert str(detail.get("classification") or "") == "dead_listing_page"


def test_run_personio_sources_source_classifies_rate_limited_errors() -> None:
    source_rows = [
        {
            "name": "InnoGames (Personio)",
            "studio": "InnoGames",
            "adapter": "personio",
            "feed_url": "https://innogames.jobs.personio.de/xml",
            "enabledByDefault": True,
        }
    ]
    with mock.patch("src.jobs.adapters.provider_api.registry_entries", return_value=source_rows):
        jf.SOURCE_DIAGNOSTICS.clear()
        with pytest.raises(AdapterValidationError):
            jf.run_personio_sources_source(
                fetch_text=lambda _url, _timeout: (_ for _ in ()).throw(
                    RuntimeError("HTTP 429 for https://innogames.jobs.personio.de/xml")
                ),
                timeout_s=5,
                retries=0,
                backoff_s=0,
            )
        detail = ((jf.SOURCE_DIAGNOSTICS.get("personio_sources") or {}).get("details") or [{}])[0]
        assert str(detail.get("classification") or "") == "rate_limited"


def test_parse_gamejobs_html_fixture() -> None:
    rows = jf.parse_gamejobs_html(_fixture("gamejobs.html"), base_url="https://gamejobs.co/")
    assert len(rows) == 2
    assert rows[0]["company"] == "Pixel Forge"
    assert any(row["workType"] == "Remote" for row in rows)


def test_run_gamejobs_source_paginates_search_pages() -> None:
    page_one = """
        <html><body>
          <a href="/jobs/lead-gameplay-programmer">Lead Gameplay Programmer</a>
          <a href="/companies/pixel-forge">Pixel Forge</a>
          <a href="/locations/amsterdam-netherlands">Amsterdam, Netherlands</a>
          <a href="/jobs/technical-artist">Technical Artist</a>
          <a href="/companies/nebula-games">Nebula Games</a>
          <a href="/locations/worldwide-remote">Worldwide Remote</a>
        </body></html>
        """
    page_two = """
        <html><body>
          <a href="/jobs/economy-designer">Economy Designer</a>
          <a href="/companies/rainfall-interactive">Rainfall Interactive</a>
          <a href="/locations/london-united-kingdom">London, United Kingdom</a>
          <a href="/jobs/lead-gameplay-programmer">Lead Gameplay Programmer</a>
          <a href="/companies/pixel-forge">Pixel Forge</a>
          <a href="/locations/amsterdam-netherlands">Amsterdam, Netherlands</a>
        </body></html>
        """
    seen_urls: list[str] = []

    def fake_fetch_text(url: str, timeout: int) -> str:
        _ = timeout
        seen_urls.append(url)
        if url == "https://gamejobs.co/":
            return page_one
        if url == "https://gamejobs.co/search?page=2":
            return page_two
        if url == "https://gamejobs.co/search?page=3":
            return "<html><body>No jobs</body></html>"
        raise AssertionError(f"unexpected url {url}")

    rows = jf.run_gamejobs_source(fetch_text=fake_fetch_text, timeout_s=5, retries=0, backoff_s=0)
    assert len(rows) == 3
    assert any(row["title"] == "Economy Designer" for row in rows)
    assert seen_urls[:3] == [
        "https://gamejobs.co/",
        "https://gamejobs.co/search?page=2",
        "https://gamejobs.co/search?page=3",
    ]


def test_parse_workwithindies_html_fixture() -> None:
    rows = jf.parse_workwithindies_html(
        _fixture("workwithindies.html"),
        base_url="https://www.workwithindies.com/",
    )
    assert len(rows) == 2
    assert rows[0]["company"] == "Moonshot Games"
    assert any(row["workType"] == "Remote" for row in rows)
    assert any(row["country"] == "CA" for row in rows)


def test_parse_8bitplay_html_fixture() -> None:
    rows = jf.parse_8bitplay_html(
        _fixture("8bitplay_jobs.html"),
        base_url="https://8bitplay.com/jobs/",
    )
    assert len(rows) == 2
    assert rows[0]["company"] == "Pixel Dominion"
    assert any(row["workType"] == "Remote" for row in rows)


def test_run_8bitplay_source_paginates_job_board_pages() -> None:
    page_one = _fixture("8bitplay_jobs.html")
    page_two = """
        <html><body>
          <a href="https://8bitplay.com/job/rendering-engineer/" class="post__similar-job">
            <div class="acf-job-board__top">
              <div class="acf-job-board__logo"><p class="acf-job-board__img-text">Nebula Forge</p></div>
              <h2 class="acf-job-board__props"><span>PC/Console</span><span>Europe</span></h2>
            </div>
            <h3 class="post__similar-job-title acf-jtw__title">Rendering Engineer</h3>
          </a>
        </body></html>
        """
    seen_urls: list[str] = []

    def fake_fetch_text(url: str, timeout: int) -> str:
        _ = timeout
        seen_urls.append(url)
        if url == "https://8bitplay.com/jobs/":
            return page_one
        if url == "https://8bitplay.com/jobs/?job-board-paged=2":
            return page_two
        if url == "https://8bitplay.com/jobs/?job-board-paged=3":
            return "<html><body>No more jobs</body></html>"
        raise AssertionError(f"unexpected url {url}")

    rows = jf.run_8bitplay_source(fetch_text=fake_fetch_text, timeout_s=5, retries=0, backoff_s=0)
    assert len(rows) == 3
    assert any(row["title"] == "Rendering Engineer" for row in rows)
    assert seen_urls[:3] == [
        "https://8bitplay.com/jobs/",
        "https://8bitplay.com/jobs/?job-board-paged=2",
        "https://8bitplay.com/jobs/?job-board-paged=3",
    ]


def test_parse_gracklehq_html_fixture() -> None:
    rows = jf.parse_gracklehq_html(
        _fixture("gracklehq_jobs.html"),
        base_url="https://gracklehq.com/jobs",
    )
    assert len(rows) == 2
    assert rows[0]["company"] == "Ubisoft"
    assert any(row["workType"] == "Remote" for row in rows)


def test_run_gracklehq_source_follows_next_pages() -> None:
    page_one = (
        _fixture("gracklehq_jobs.html")
        + '<a href="./jobs?pageidx=2" class="btn btn-default ">Next</a>'
    )
    page_two = """
        <html><body>
          <div class="joblisting">
            <a href="/rd/372395" target="_blank">Gameplay Programmer</a>
            <div>Robot Eclipse - Remote</div>
            <div class="bottomright">&lt;1d</div>
          </div>
        </body></html>
        """
    seen_urls: list[str] = []

    def fake_fetch_text(url: str, timeout: int) -> str:
        _ = timeout
        seen_urls.append(url)
        if url == "https://gracklehq.com/jobs":
            return page_one
        if url == "https://gracklehq.com/jobs?pageidx=2":
            return page_two
        raise AssertionError(f"unexpected url {url}")

    rows = jf.run_gracklehq_source(fetch_text=fake_fetch_text, timeout_s=5, retries=0, backoff_s=0)
    assert len(rows) == 3
    assert any(row["title"] == "Gameplay Programmer" for row in rows)
    assert seen_urls == [
        "https://gracklehq.com/jobs",
        "https://gracklehq.com/jobs?pageidx=2",
    ]


def test_run_gracklehq_source_stops_on_repeated_next_page() -> None:
    page_one = (
        _fixture("gracklehq_jobs.html")
        + '<a href="./jobs?pageidx=2" class="btn btn-default ">Next</a>'
    )
    page_two = """
        <html><body>
          <div class="joblisting">
            <a href="/rd/372395" target="_blank">Gameplay Programmer</a>
            <div>Robot Eclipse - Remote</div>
          </div>
          <a href="./jobs?pageidx=2" class="btn btn-default ">Next</a>
        </body></html>
        """
    seen_urls: list[str] = []

    def fake_fetch_text(url: str, timeout: int) -> str:
        _ = timeout
        seen_urls.append(url)
        if url == "https://gracklehq.com/jobs":
            return page_one
        if url == "https://gracklehq.com/jobs?pageidx=2":
            return page_two
        raise AssertionError(f"unexpected url {url}")

    rows = jf.run_gracklehq_source(fetch_text=fake_fetch_text, timeout_s=5, retries=0, backoff_s=0)
    assert len(rows) == 3
    assert seen_urls == [
        "https://gracklehq.com/jobs",
        "https://gracklehq.com/jobs?pageidx=2",
    ]


def test_normalize_source_report_row_preserves_structured_details() -> None:
    row = jf.normalize_source_report_row(
        {
            "name": "lever_sources",
            "status": "ok",
            "browserEscalationEligible": True,
            "browserEscalationEligibilityReason": "js_required",
            "browserEscalationEnabled": True,
            "details": [
                {
                    "adapter": "lever",
                    "studio": "Jagex",
                    "name": "Jagex (Lever)",
                    "status": "ok",
                    "fetchedCount": 3,
                    "keptCount": 2,
                    "error": "",
                    "browserEscalationEligible": False,
                    "browserEscalationEnabled": True,
                }
            ],
        }
    )
    assert row["browserEscalationEligible"] is True
    assert row["browserEscalationEligibilityReason"] == "js_required"
    assert row["browserEscalationEnabled"] is True
    details = row.get("details")
    assert isinstance(details, list)
    assert isinstance(details[0], dict)
    assert details[0]["name"] == "Jagex (Lever)"
    assert int(details[0]["keptCount"]) == 2
    assert details[0]["browserEscalationEligible"] is False
    assert details[0]["browserEscalationEnabled"] is True


def test_normalize_source_report_row_preserves_aggregate_provider_site_changed_url_surface() -> (
    None
):
    row = jf.normalize_source_report_row(
        {
            "name": "greenhouse_boards",
            "status": "ok",
            "adapter": "greenhouse",
            "failureBucket": "site_changed",
            "providerUrl": "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true",
        }
    )
    assert (
        str(row.get("providerUrl") or "")
        == "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true"
    )

    non_site_changed = jf.normalize_source_report_row(
        {
            "name": "greenhouse_boards",
            "status": "ok",
            "adapter": "greenhouse",
            "failureBucket": "needs_review",
            "providerUrl": "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true",
        }
    )
    assert "providerUrl" not in non_site_changed


def test_run_greenhouse_boards_source_with_fixture() -> None:
    payload = _fixture("greenhouse_guerrilla_jobs.json")
    previous = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Guerrilla Games",
            "studio": "Guerrilla Games",
            "adapter": "greenhouse",
            "slug": "guerrilla-games",
            "enabledByDefault": True,
        }
    ]

    try:
        with mock.patch.object(
            jobs_registry, "STUDIO_SOURCE_REGISTRY", list(jf.STUDIO_SOURCE_REGISTRY)
        ):

            def fake_fetch(url: str, _: int) -> str:
                assert "boards-api.greenhouse.io" in url
                assert "guerrilla-games" in url
                return payload

            rows = jf.run_greenhouse_boards_source(
                fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
            )
            assert len(rows) == 2
            assert any("guerrilla-games/jobs/" in row["jobLink"] for row in rows)
    finally:
        jf.STUDIO_SOURCE_REGISTRY = previous


def test_run_teamtailor_source_with_fixture() -> None:
    listing = _fixture("teamtailor_listing.html")
    detail = _fixture("teamtailor_job.html")

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://career.paradoxplaza.com/jobs":
            return listing
        if "/jobs/" in url:
            return detail
        raise RuntimeError(f"Unexpected URL: {url}")

        rows = jf.run_teamtailor_sources_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        assert len(rows) >= 1
        assert any("career.paradoxplaza.com/jobs/" in row["jobLink"] for row in rows)


def test_default_registry_no_longer_seeds_stale_ashby_personio_or_placeholder_greenhouse_rows() -> (
    None
):
    names = {str(row.get("name") or "") for row in jf.STUDIO_SOURCE_REGISTRY}
    # Verify placeholder was removed from registry
    assert "Example Studio GmbH (Greenhouse)" not in names
    # Verify valid studios still exist (any Bandai Namco entry)
    assert any("Bandai Namco" in n for n in names)


def test_hrmos_plugin_extracts_listing_rows_without_detail_fetch() -> None:
    from src.jobs.adapters.plugins.static import hrmos

    html = """
        <div>
          <a href="/pages/cygames/jobs/0001">
            <h2>Gameplay Programmer</h2>
            <span>Tokyo, Japan</span>
            <span>Full-time</span>
          </a>
          <a href="/pages/cygames/jobs/0002">
            <h2>Technical Artist</h2>
            <span>Osaka, Japan</span>
            <span>Contract</span>
          </a>
        </div>
        """

    rows = hrmos.run(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=10,
        retries=0,
        backoff_s=0.0,
        pages=["https://hrmos.co/pages/cygames/jobs"],
        source_row={"id": "cygames", "name": "Cygames"},
    )

    assert len(rows) == 2
    assert rows[0]["jobLink"] == "https://hrmos.co/pages/cygames/jobs/0001"
    assert rows[0]["title"] == "Gameplay Programmer"
    assert rows[0]["city"] == "Tokyo"
    assert rows[0]["country"] == "Japan"


def test_hrmos_plugin_does_not_emit_full_prose_blob_as_location() -> None:
    from src.jobs.adapters.plugins.static import hrmos

    html = """
        <div>
          <a href="/pages/gamefreak/jobs/10-4">
            <h2>キャリア登録</h2>
            <span>キャリア登録 「キャリア登録」とは？ 当社に興味・関心を持たれた方にご自身のキャリア（職務経歴）を簡易登録いただくことで、適したポジションがある場合、人事担当者から個別にご案内させていただく仕組みです。</span>
            <span>正社員</span>
          </a>
        </div>
        """

    rows = hrmos.run(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=10,
        retries=0,
        backoff_s=0.0,
        pages=["https://hrmos.co/pages/gamefreak/jobs?jobtype=full"],
        source_row={"id": "gamefreak", "name": "GAME FREAK inc."},
    )

    assert len(rows) == 1
    assert rows[0]["title"] == "キャリア登録"
    assert rows[0]["city"] == ""


def test_riot_plugin_extracts_listing_rows_without_detail_fetch() -> None:
    from src.jobs.adapters.plugins.static import riot

    html = """
        <div>
          <a href="/en/j/7449593">
            <span>Senior Software Engineer</span>
            <span>Engineering</span>
            <span>Dublin, Ireland</span>
          </a>
        </div>
        """

    rows = riot.run(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=10,
        retries=0,
        backoff_s=0.0,
        pages=["https://www.riotgames.com/en/work-with-us/jobs"],
        source_row={"id": "riot", "name": "Riot Games"},
    )

    assert len(rows) == 1
    assert rows[0]["jobLink"] == "https://www.riotgames.com/en/j/7449593"
    assert rows[0]["title"] == "Senior Software Engineer"
    assert rows[0]["city"] == "Dublin"
    assert rows[0]["country"] == "Ireland"


def test_lionbridge_plugin_splits_city_region_country_listing_rows() -> None:
    from src.jobs.adapters.plugins.static import lionbridge

    html = """
        <table>
          <tr>
            <td><a href="/jobs/test-lead">Test Lead</a></td>
            <td>Mexico City, CMDX, Mexico</td>
            <td>Onsite</td>
          </tr>
        </table>
        """

    rows = lionbridge.run(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=10,
        retries=0,
        backoff_s=0.0,
        pages=["https://careers.lionbridge.com/jobs/search"],
        source_row={"id": "lionbridge", "name": "Lionbridge Games"},
    )

    assert len(rows) == 1
    assert rows[0]["title"] == "Test Lead"
    assert rows[0]["city"] == "Mexico City"
    assert rows[0]["country"] == "Mexico"


def test_choose_detail_traversal_mode_prefers_listing_only_for_verified_hosts() -> None:
    from src.jobs.adapters.static_helpers import (
        build_static_source_runtime_config,
        choose_detail_traversal_mode,
    )

    runtime = build_static_source_runtime_config(4)
    mode = choose_detail_traversal_mode(
        "https://hrmos.co/pages/cygames/jobs",
        runtime_config=runtime,
        profile={"detail_fetch_required": False},
        plugin_meta={"detailFetchRequired": False},
        listing_jobs_found=10,
        discovered_links=10,
        source_key="static_source::cygames",
        source_state_rows={},
    )
    assert mode == "listing_only"


def test_choose_detail_traversal_mode_uncapped_deep_static_overrides_listing_only_with_probable_detail_links() -> (
    None
):
    from src.jobs.adapters.static_helpers import (
        build_static_source_runtime_config,
        choose_detail_traversal_mode,
    )

    with mock.patch.dict("os.environ", {"BALUFFO_UNCAPPED_DEEP_STATIC": "1"}, clear=False):
        runtime = build_static_source_runtime_config(4)
    mode = choose_detail_traversal_mode(
        "https://hrmos.co/pages/cygames/jobs",
        runtime_config=runtime,
        profile={"detail_fetch_required": False},
        plugin_meta={"detailFetchRequired": False},
        listing_jobs_found=10,
        discovered_links=10,
        source_key="static_source::cygames",
        source_state_rows={},
        probable_detail_candidates=3,
    )
    assert mode == "full_detail"


def test_choose_detail_traversal_mode_uncapped_deep_static_keeps_listing_only_without_probable_detail_links() -> (
    None
):
    from src.jobs.adapters.static_helpers import (
        build_static_source_runtime_config,
        choose_detail_traversal_mode,
    )

    with mock.patch.dict("os.environ", {"BALUFFO_UNCAPPED_DEEP_STATIC": "1"}, clear=False):
        runtime = build_static_source_runtime_config(4)
    mode = choose_detail_traversal_mode(
        "https://hrmos.co/pages/cygames/jobs",
        runtime_config=runtime,
        profile={"detail_fetch_required": False},
        plugin_meta={"detailFetchRequired": False},
        listing_jobs_found=10,
        discovered_links=10,
        source_key="static_source::cygames",
        source_state_rows={},
        probable_detail_candidates=0,
    )
    assert mode == "listing_only"


def test_choose_detail_traversal_mode_uncapped_zero_caps_promotes_capped_detail_to_full_detail() -> (
    None
):
    from src.jobs.adapters.static_helpers import (
        build_static_source_runtime_config,
        choose_detail_traversal_mode,
    )

    source_state_rows = {
        "static_source::climax": {
            "lastDetailPagesVisited": 42,
            "lastKeptCount": 1,
            "lastDurationMs": 52000,
            "lastDetailYieldPct": 2,
        }
    }
    regular_runtime = build_static_source_runtime_config(4)
    regular_mode = choose_detail_traversal_mode(
        "https://careers.climaxstudios.com/jobs",
        runtime_config=regular_runtime,
        profile={},
        plugin_meta={},
        listing_jobs_found=0,
        discovered_links=28,
        source_key="static_source::climax",
        source_state_rows=source_state_rows,
    )
    with mock.patch.dict(
        "os.environ",
        {
            "BALUFFO_UNCAPPED_DEEP_STATIC": "1",
            "BALUFFO_STATIC_LOW_YIELD_DETAIL_CAP": "0",
            "BALUFFO_STATIC_VERY_LOW_YIELD_DETAIL_CAP": "0",
        },
        clear=False,
    ):
        uncapped_runtime = build_static_source_runtime_config(4)
    uncapped_mode = choose_detail_traversal_mode(
        "https://careers.climaxstudios.com/jobs",
        runtime_config=uncapped_runtime,
        profile={},
        plugin_meta={},
        listing_jobs_found=0,
        discovered_links=28,
        source_key="static_source::climax",
        source_state_rows=source_state_rows,
    )
    assert regular_mode == "capped_detail"
    assert uncapped_mode == "full_detail"


def test_personio_adapter_skips_recent_rate_limited_source_only() -> None:
    from src.jobs.adapters import provider_api

    now = jf.datetime.now(jf.timezone.utc).isoformat()
    registry_rows = [
        {
            "name": "Rate Limited Studio",
            "studio": "Rate Limited Studio",
            "feed_url": "https://example.com/rate.xml",
        },
        {
            "name": "Healthy Studio",
            "studio": "Healthy Studio",
            "feed_url": "https://example.com/ok.xml",
        },
    ]

    def fake_fetch(url: str, _timeout: int) -> str:
        if url.endswith("/ok.xml"):
            return """<?xml version="1.0"?><workzag-jobs><position><id>1</id><name>Engine Programmer</name><office>Remote</office><employmentType>Full-time</employmentType><url>https://example.com/jobs/1</url></position></workzag-jobs>"""
        raise AssertionError(f"unexpected fetch for {url}")

    with mock.patch.object(provider_api, "registry_entries", return_value=registry_rows):
        rows = provider_api.run_personio_sources_source(
            fetch_text=fake_fetch,
            timeout_s=10,
            retries=0,
            backoff_s=0.0,
            source_state_rows={
                "Rate Limited Studio": {
                    "lastError": "HTTP 429 Too Many Requests",
                    "lastFailureAt": now,
                }
            },
        )

    assert len(rows) == 1
    assert rows[0]["title"] == "Engine Programmer"


def test_personio_rate_limit_cooldown_can_be_configured() -> None:
    from src.jobs.adapters import provider_api

    with mock.patch.dict(
        "os.environ", {"BALUFFO_PERSONIO_RATE_LIMIT_COOLDOWN_MINUTES": "15"}, clear=False
    ):
        cutoff = provider_api._personio_rate_limit_cutoff()
    delta_minutes = (jf.datetime.now(jf.timezone.utc) - cutoff).total_seconds() / 60
    assert 14 <= delta_minutes <= 16
