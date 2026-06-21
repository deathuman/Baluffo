"""Tests for jobs fetcher providers payload parsing."""

import json
from typing import Any

from src import jobs_fetcher as jf
from tests.helpers.job_fixtures import _fixture


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
