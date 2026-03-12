import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

from scripts import jobs_fetcher as jf
from scripts import jobs_fetcher_registry as jfr
from scripts.scrapers import runner as scrapy_runner
from tests.temp_paths import workspace_tmpdir


class JobsFetcherTests(unittest.TestCase):
    def fixture(self, name: str) -> str:
        path = Path(__file__).parent / "fixtures" / name
        return path.read_text(encoding="utf-8")

    def fixture_json(self, name: str):
        return json.loads(self.fixture(name))

    def test_parse_google_sheets_csv_fixture(self) -> None:
        rows = jf.parse_google_sheets_csv(self.fixture("google_sheets.csv"))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["title"], "Gameplay Programmer")
        self.assertEqual(rows[0]["company"], "Pixel Forge")

    def test_parse_google_sheets_csv_supports_job_type_link_headers(self) -> None:
        csv_text = (
            "Intro row,,,,,,,,,\n"
            "Company,Company Category,Job Category,Job,Job Type,Postal Code,City,Fully Remote?,Link,Added\n"
            "Studio A,Developer,Programming,Gameplay Programmer,Full-Time,10115,Berlin,Yes,https://example.com/jobs/1,2026-03-10\n"
        )
        rows = jf.parse_google_sheets_csv(csv_text)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Gameplay Programmer")
        self.assertEqual(rows[0]["company"], "Studio A")
        self.assertEqual(rows[0]["contractType"], "Full-Time")
        self.assertEqual(rows[0]["jobLink"], "https://example.com/jobs/1")
        self.assertEqual(rows[0]["sector"], "Developer")

    def test_parse_google_sheets_csv_supports_studio_header_alias(self) -> None:
        csv_text = (
            "Studio,Country,Job Title,Experience Level,Link\n"
            "Acme Games,Germany,Senior Gameplay Engineer,Senior,https://example.com/jobs/42\n"
        )
        rows = jf.parse_google_sheets_csv(csv_text)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["company"], "Acme Games")
        self.assertEqual(rows[0]["country"], "Germany")
        self.assertEqual(rows[0]["title"], "Senior Gameplay Engineer")

    def test_parse_args_uses_config_backed_output_and_social_defaults(self) -> None:
        prev_argv = list(sys.argv)
        try:
            sys.argv = ["jobs_fetcher.py"]
            args = jf.parse_args()
        finally:
            sys.argv = prev_argv
        self.assertEqual(Path(args.output_dir), jf.DEFAULT_OUTPUT_DIR)
        self.assertEqual(Path(args.social_config_path), jf.DEFAULT_SOCIAL_CONFIG_PATH)

    def test_parse_remote_ok_payload_filters_game_roles(self) -> None:
        payload = json.loads(self.fixture("remoteok.json"))
        rows = jf.parse_remote_ok_payload(payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["sourceJobId"], "101")
        self.assertEqual(rows[0]["company"], "Nebula Games")

    def test_run_remote_ok_source_falls_back_to_secondary_endpoint(self) -> None:
        payload = self.fixture("remoteok.json")
        calls = []

        def fake_fetch(url: str, _: int) -> str:
            calls.append(url)
            if "remoteok.com/api" in url:
                raise RuntimeError("primary endpoint failed")
            if "remoteok.io/api" in url:
                return payload
            raise RuntimeError(f"Unhandled URL: {url}")

        rows = jf.run_remote_ok_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(calls), 2)
        self.assertIn("remoteok.com/api", calls[0])
        self.assertIn("remoteok.io/api", calls[1])

    def test_parse_reddit_json_payload_filters_and_normalizes(self) -> None:
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
        self.assertEqual(len(rows), 1)
        self.assertGreaterEqual(dropped, 1)
        self.assertEqual(rows[0]["company"], "Nebula Games")
        self.assertIn("jobs.nebula.dev", rows[0]["jobLink"])

    def test_parse_x_payload_and_mastodon_payload(self) -> None:
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
        self.assertEqual(len(x_rows), 1)
        self.assertEqual(x_dropped, 0)
        self.assertIn("pixelforge", x_rows[0]["jobLink"].lower())

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
        self.assertEqual(len(mastodon_rows), 1)
        self.assertEqual(mastodon_dropped, 0)
        self.assertIn("aurora.dev", mastodon_rows[0]["jobLink"])

    def test_parse_x_rss_payload(self) -> None:
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
        self.assertEqual(len(rows), 1)
        self.assertEqual(dropped, 0)
        self.assertIn("jobs.orbit.dev", rows[0]["jobLink"])

    def test_run_social_x_source_uses_rss_fallback_without_credentials(self) -> None:
        social_cfg = {
            "enabled": True,
            "minConfidence": 20,
            "rejectForHirePosts": True,
            "x": {
                "enabled": True,
                "queries": ["#gamedevjobs"],
                "maxPostsPerQuery": 5,
                "api": {"enabled": True, "endpoint": "https://api.x.com/2/tweets/search/recent", "bearerTokenEnv": "BALUFFO_X_BEARER_TOKEN"},
                "scraperFallback": {"enabled": False, "endpoint": ""},
                "rssFallback": {"enabled": True, "instances": ["https://nitter.net"]},
            },
        }
        rss = """<?xml version="1.0" encoding="UTF-8"?>
<rss><channel>
  <item>
    <title>Hiring Technical Artist at Nova Studio</title>
    <link>https://nitter.net/nova/status/42</link>
    <description>Apply https://careers.nova.dev/ta</description>
    <pubDate>Mon, 09 Mar 2026 11:05:00 GMT</pubDate>
  </item>
</channel></rss>"""

        def fake_fetch(url: str, _: int) -> str:
            if "nitter.net/search/rss" in url:
                return rss
            raise RuntimeError(f"Unhandled URL: {url}")

        rows = jf.run_social_x_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            social_config=social_cfg,
        )
        self.assertEqual(len(rows), 1)
        self.assertIn("careers.nova.dev", rows[0]["jobLink"])

    def test_deduplicate_jobs_uses_social_source_id_fallback(self) -> None:
        row_a = {
            "id": "",
            "title": "Technical Artist",
            "company": "Nebula Games",
            "city": "",
            "country": "Unknown",
            "workType": "",
            "contractType": "Unknown",
            "jobLink": "",
            "sector": "Game",
            "profession": "technical-artist",
            "companyType": "Game",
            "description": "Technical Artist at Nebula Games",
            "source": "social_reddit",
            "sourceJobId": "reddit:gamedev:abc",
            "fetchedAt": "2026-03-09T11:00:00Z",
            "postedAt": "2026-03-09T10:00:00Z",
            "status": "active",
            "sourceBundleCount": 1,
            "sourceBundle": [{"source": "social_reddit", "sourceJobId": "reddit:gamedev:abc", "jobLink": "", "postedAt": "", "adapter": "social", "studio": "gamedev"}],
            "adapter": "social",
            "studio": "reddit/gamedev",
        }
        row_b = dict(row_a)
        row_b["jobLink"] = "https://www.reddit.com/r/gamedev/comments/abc"
        deduped, stats = jf.deduplicate_jobs([row_a, row_b])
        self.assertEqual(len(deduped), 1)
        self.assertEqual(stats["mergedCount"], 1)

    def test_run_pipeline_social_sources_report_and_output(self) -> None:
        social_cfg = {
            "enabled": True,
            "minConfidence": 20,
            "rejectForHirePosts": True,
            "reddit": {"enabled": True, "subreddits": ["gamedev"], "maxPostsPerSubreddit": 5, "rssFallback": True, "htmlFallback": False},
            "x": {
                "enabled": True,
                "queries": ["#gamedevjobs"],
                "maxPostsPerQuery": 5,
                "api": {"enabled": False, "endpoint": "", "bearerTokenEnv": "BALUFFO_X_BEARER_TOKEN"},
                "scraperFallback": {"enabled": True, "endpoint": "https://example.local/x-search"},
            },
            "mastodon": {"enabled": True, "instances": ["https://mastodon.gamedev.place"], "hashtags": ["gamedevjobs"], "maxPostsPerTag": 5},
        }

        def social_reddit_loader(**kwargs):
            return jf.run_social_reddit_source(**kwargs, social_config=social_cfg)

        def social_x_loader(**kwargs):
            return jf.run_social_x_source(**kwargs, social_config=social_cfg)

        def social_mastodon_loader(**kwargs):
            return jf.run_social_mastodon_source(**kwargs, social_config=social_cfg)

        reddit_payload = {
            "data": {
                "children": [
                    {
                        "data": {
                            "id": "abc123",
                            "title": "We're hiring a Technical Artist at Nebula Games",
                            "selftext": "Apply https://jobs.nebula.dev/ta",
                            "link_flair_text": "Hiring",
                            "permalink": "/r/gamedev/comments/abc123/test/",
                            "url": "https://www.reddit.com/r/gamedev/comments/abc123/test/",
                            "created_utc": 1700000000,
                            "author": "nebula_hr",
                        }
                    }
                ]
            }
        }
        x_payload = {
            "data": [
                {
                    "id": "x1",
                    "text": "We are hiring an Environment Artist at Pixel Forge. Apply https://jobs.pixelforge.dev/ea",
                    "created_at": "2026-03-09T11:00:00Z",
                }
            ]
        }
        mastodon_payload = [
            {
                "id": "m1",
                "content": "<p>Hiring gameplay programmer at Aurora Games https://careers.aurora.dev/gp</p>",
                "created_at": "2026-03-09T11:05:00Z",
                "url": "https://mastodon.gamedev.place/@aurora/111",
                "account": {"display_name": "Aurora Games"},
            }
        ]

        def fake_fetch(url: str, _: int) -> str:
            if "reddit.com/r/gamedev/new.json" in url:
                return json.dumps(reddit_payload)
            if "example.local/x-search" in url:
                return json.dumps(x_payload)
            if "mastodon.gamedev.place/api/v1/timelines/tag/gamedevjobs" in url:
                return json.dumps(mastodon_payload)
            raise RuntimeError(f"Unhandled URL in fake fetch: {url}")

        with workspace_tmpdir("jobs-fetcher-social") as tmp:
            report = jf.run_pipeline(
                output_dir=Path(tmp),
                fetch_text=fake_fetch,
                source_loaders=[
                    ("social_reddit", social_reddit_loader),
                    ("social_x", social_x_loader),
                    ("social_mastodon", social_mastodon_loader),
                ],
                timeout_s=5,
                retries=0,
                backoff_s=0,
            )
            sources = {row["name"]: row for row in report["sources"]}
            self.assertEqual(sources["social_reddit"]["status"], "ok")
            self.assertEqual(sources["social_x"]["status"], "ok")
            self.assertEqual(sources["social_mastodon"]["status"], "ok")
            self.assertGreaterEqual(sources["social_reddit"]["keptCount"], 1)
            rows = json.loads((Path(tmp) / "jobs-unified.json").read_text(encoding="utf-8"))
            self.assertTrue(any(str(row.get("source") or "").startswith("social_") for row in rows))

    def test_parse_gamesindustry_html_fixture(self) -> None:
        rows = jf.parse_gamesindustry_html(self.fixture("gamesindustry_jobs.html"), base_url="https://jobs.gamesindustry.biz")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["title"], "Senior Quality Analyst")
        self.assertEqual(rows[0]["company"], "Sharkmob")
        self.assertEqual(rows[0]["sourceJobId"], "43821")
        self.assertTrue(rows[0]["jobLink"].startswith("https://jobs.gamesindustry.biz/job/"))
        titles = {row["title"] for row in rows}
        self.assertNotIn("Read more", titles)
        self.assertNotIn("Programming (6)", titles)

    def test_parse_greenhouse_jobs_payload_fixture(self) -> None:
        payload = json.loads(self.fixture("greenhouse_guerrilla_jobs.json"))
        rows = jf.parse_greenhouse_jobs_payload(payload, "guerrilla-games")
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["sourceJobId"].startswith("greenhouse:guerrilla-games:") for row in rows))
        self.assertEqual(rows[0]["company"], "Guerrilla Games")
        self.assertEqual(rows[0]["country"], "Netherlands")

    def test_parse_teamtailor_listing_links_fixture(self) -> None:
        rows = jf.parse_teamtailor_listing_links(
            self.fixture("teamtailor_listing.html"),
            base_url="https://career.paradoxplaza.com",
        )
        self.assertEqual(len(rows), 2)
        self.assertTrue(all("/jobs/" in row for row in rows))
        self.assertTrue(all("show_more" not in row for row in rows))

    def test_parse_jobpostings_from_html_teamtailor_fixture(self) -> None:
        rows = jf.parse_jobpostings_from_html(
            self.fixture("teamtailor_job.html"),
            base_url="https://career.paradoxplaza.com/jobs/6926996-game-programmer",
            fallback_company="Paradox Interactive",
            fallback_source_id_prefix="teamtailor:test",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Game Programmer")
        self.assertEqual(rows[0]["city"], "Delft")
        self.assertEqual(rows[0]["country"], "NL")

    def test_parse_wellfound_html_fixture(self) -> None:
        rows = jf.parse_wellfound_html(self.fixture("wellfound.html"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["sourceJobId"], "wf-1")
        self.assertEqual(rows[0]["workType"], "Remote")

    def test_parse_lever_jobs_payload_fixture(self) -> None:
        payload = json.loads(self.fixture("lever_jobs.json"))
        rows = jf.parse_lever_jobs_payload(payload, "sandboxvr", fallback_company="Sandbox VR")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Technical Artist")
        self.assertEqual(rows[0]["country"], "NL")

    def test_parse_smartrecruiters_jobs_payload_fixture(self) -> None:
        payload = json.loads(self.fixture("smartrecruiters_jobs.json"))
        rows = jf.parse_smartrecruiters_jobs_payload(payload, "CDPROJEKTRED", fallback_company="CD PROJEKT RED")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Environment Artist")
        self.assertEqual(rows[0]["company"], "CD PROJEKT RED")

    def test_parse_workable_jobs_payload_fixture(self) -> None:
        payload = json.loads(self.fixture("workable_jobs.json"))
        rows = jf.parse_workable_jobs_payload(payload, "hutch", fallback_company="Hutch")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["workType"], "Remote")

    def test_parse_ashby_jobs_from_html_fixture(self) -> None:
        rows = jf.parse_ashby_jobs_from_html(self.fixture("ashby_jobs.html"), "https://jobs.ashbyhq.com/jagex/jobs", "Jagex")
        self.assertEqual(len(rows), 2)
        self.assertTrue(all("jobs.ashbyhq.com" in row["jobLink"] for row in rows))

    def test_parse_personio_feed_xml_fixture(self) -> None:
        rows = jf.parse_personio_feed_xml(self.fixture("personio_feed.xml"), source_name="InnoGames")
        self.assertGreaterEqual(len(rows), 1)
        self.assertTrue(any(row["title"] == "Environment Artist" for row in rows))

    def test_normalize_source_report_row_preserves_structured_details(self) -> None:
        row = jf.normalize_source_report_row({
            "name": "lever_sources",
            "status": "ok",
            "details": [
                {
                    "adapter": "lever",
                    "studio": "Jagex",
                    "name": "Jagex (Lever)",
                    "status": "ok",
                    "fetchedCount": 3,
                    "keptCount": 2,
                    "error": "",
                }
            ],
        })
        details = row.get("details")
        self.assertIsInstance(details, list)
        self.assertIsInstance(details[0], dict)
        self.assertEqual(details[0]["name"], "Jagex (Lever)")
        self.assertEqual(int(details[0]["keptCount"]), 2)

    def test_run_greenhouse_boards_source_with_fixture(self) -> None:
        payload = self.fixture("greenhouse_guerrilla_jobs.json")
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
            def fake_fetch(url: str, _: int) -> str:
                self.assertIn("boards-api.greenhouse.io", url)
                self.assertIn("guerrilla-games", url)
                return payload

            rows = jf.run_greenhouse_boards_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            self.assertEqual(len(rows), 2)
            self.assertTrue(any("guerrilla-games/jobs/" in row["jobLink"] for row in rows))
        finally:
            jf.STUDIO_SOURCE_REGISTRY = previous

    def test_run_teamtailor_source_with_fixture(self) -> None:
        listing = self.fixture("teamtailor_listing.html")
        detail = self.fixture("teamtailor_job.html")

        def fake_fetch(url: str, _: int) -> str:
            if url == "https://career.paradoxplaza.com/jobs":
                return listing
            if "/jobs/" in url:
                return detail
            raise RuntimeError(f"Unexpected URL: {url}")

        rows = jf.run_teamtailor_sources_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
        self.assertGreaterEqual(len(rows), 1)
        self.assertTrue(any("career.paradoxplaza.com/jobs/" in row["jobLink"] for row in rows))

    def test_run_static_studio_pages_source_with_fixture(self) -> None:
        listing = self.fixture("littlechicken_jobs_page.html")
        detail = self.fixture("littlechicken_job_detail.html")
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Little Chicken",
                "studio": "Little Chicken",
                "adapter": "static",
                "company": "Little Chicken",
                "pages": ["https://www.littlechicken.nl/about-us/jobs/"],
                "enabledByDefault": True,
            }
        ]

        try:
            def fake_fetch(url: str, _: int) -> str:
                if url == "https://www.littlechicken.nl/about-us/jobs/":
                    return listing
                if "/job/" in url:
                    return detail
                raise RuntimeError(f"Unexpected URL: {url}")

            rows = jf.run_static_studio_pages_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            self.assertGreaterEqual(len(rows), 2)
            self.assertTrue(any("littlechicken.nl/job/" in row["jobLink"] for row in rows))
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

    def test_run_static_studio_pages_source_loads_kojima_dynamic_listing(self) -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Kojima Productions (Manual Website)",
                "studio": "Kojima Productions",
                "adapter": "static",
                "company": "Kojima Productions",
                "pages": ["https://www.kojimaproductions.jp/en/careers"],
                "enabledByDefault": True,
            }
        ]

        base_html = '<section class="job-listings"><div class="views-container" data-viewref="kjp_job_listing"></div></section>'
        dynamic_html = """
        <table>
          <tr class="job-listing-item"><td><a href="/en/game-programmer">Game Programmer</a></td></tr>
          <tr class="job-listing-item"><td><a href="/en/ai-programmer">AI Programmer</a></td></tr>
        </table>
        """

        class _FakeResponse:
            def __init__(self, body: str) -> None:
                self._body = body

            def read(self) -> bytes:
                return self._body.encode("utf-8")

            def __enter__(self):  # noqa: ANN204
                return self

            def __exit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN204
                return False

        def fake_urlopen(request_obj, timeout=0):  # noqa: ANN001
            full_url = getattr(request_obj, "full_url", str(request_obj))
            if str(full_url).endswith("/kjpviewloader/load"):
                return _FakeResponse(dynamic_html)
            raise RuntimeError(f"Unexpected urlopen URL: {full_url}")

        def fake_fetch(url: str, _: int) -> str:
            if url == "https://www.kojimaproductions.jp/en/careers":
                return base_html
            if url in {
                "https://www.kojimaproductions.jp/en/game-programmer",
                "https://www.kojimaproductions.jp/en/ai-programmer",
            }:
                return "<html><body><h1>job</h1></body></html>"
            raise RuntimeError(f"Unexpected URL: {url}")

        try:
            with mock.patch.object(jf, "urlopen", side_effect=fake_urlopen):
                rows = jf.run_static_studio_pages_source(
                    fetch_text=fake_fetch,
                    timeout_s=5,
                    retries=0,
                    backoff_s=0,
                )
            titles = {str(row.get("title") or "") for row in rows}
            self.assertIn("Game Programmer", titles)
            self.assertIn("Ai Programmer", titles)
            self.assertGreaterEqual(len(rows), 2)
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

    def test_run_static_studio_pages_source_accepts_larian_uuid_paths_and_rejects_location_pages(self) -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Larian Studios (Manual Website)",
                "studio": "Larian Studios",
                "adapter": "static",
                "company": "Larian Studios",
                "pages": ["https://larian.com/careers"],
                "enabledByDefault": True,
            }
        ]
        listing = (
            '<html><body>'
            '<a href="https://larian.com/careers/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee">Senior Engineer</a>'
            '<a href="https://larian.com/careers/location/gent?location=Gent">Gent</a>'
            "</body></html>"
        )
        detail = "<html><body><h1>Senior Engineer</h1></body></html>"
        try:
            def fake_fetch(url: str, _: int) -> str:
                if url == "https://larian.com/careers":
                    return listing
                if url == "https://larian.com/careers/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee":
                    return detail
                raise RuntimeError(f"Unexpected URL: {url}")

            rows = jf.run_static_studio_pages_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["jobLink"], "https://larian.com/careers/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

    def test_run_static_studio_pages_source_accepts_cdpr_query_key_override(self) -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Cdprojektred (Manual Website)",
                "studio": "Cdprojektred",
                "adapter": "static",
                "company": "Cdprojektred",
                "pages": ["https://cdprojektred.com/en/jobs"],
                "detailQueryKeys": ["gh_jid"],
                "enabledByDefault": True,
            }
        ]
        listing = '<html><body><a href="https://cdprojektred.com/en/jobs?gh_jid=1234">Gameplay Engineer</a></body></html>'
        detail = "<html><body><h1>Gameplay Engineer</h1></body></html>"
        try:
            def fake_fetch(url: str, _: int) -> str:
                if url == "https://cdprojektred.com/en/jobs":
                    return listing
                if url == "https://cdprojektred.com/en/jobs?gh_jid=1234":
                    return detail
                raise RuntimeError(f"Unexpected URL: {url}")

            rows = jf.run_static_studio_pages_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["jobLink"], "https://cdprojektred.com/en/jobs?gh_jid=1234")
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

    def test_run_static_studio_pages_source_accepts_remedy_query_key_override(self) -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Remedy Entertainment (Manual Website)",
                "studio": "Remedy Entertainment",
                "adapter": "static",
                "company": "Remedy Entertainment",
                "pages": ["https://www.remedygames.com/careers"],
                "detailQueryKeys": ["jobid"],
                "enabledByDefault": True,
            }
        ]
        listing = '<html><body><a href="https://www.remedygames.com/careers/open?jobid=42">Rendering Programmer</a></body></html>'
        detail = "<html><body><h1>Rendering Programmer</h1></body></html>"
        try:
            def fake_fetch(url: str, _: int) -> str:
                if url == "https://www.remedygames.com/careers":
                    return listing
                if url == "https://www.remedygames.com/careers/open?jobid=42":
                    return detail
                raise RuntimeError(f"Unexpected URL: {url}")

            rows = jf.run_static_studio_pages_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["jobLink"], "https://www.remedygames.com/careers/open?jobid=42")
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

    def test_run_static_studio_pages_source_accepts_ubisoft_query_key_override(self) -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Ubisoft (Manual Website)",
                "studio": "Ubisoft",
                "adapter": "static",
                "company": "Ubisoft",
                "pages": ["https://www.ubisoft.com/en-us/company/careers/locations/milan"],
                "detailQueryKeys": ["jobid"],
                "enabledByDefault": True,
            }
        ]
        listing = '<html><body><a href="https://www.ubisoft.com/en-us/company/careers/search?jobid=99">Engine Programmer</a></body></html>'
        detail = "<html><body><h1>Engine Programmer</h1></body></html>"
        try:
            def fake_fetch(url: str, _: int) -> str:
                if url == "https://www.ubisoft.com/en-us/company/careers/locations/milan":
                    return listing
                if url == "https://www.ubisoft.com/en-us/company/careers/search?jobid=99":
                    return detail
                raise RuntimeError(f"Unexpected URL: {url}")

            rows = jf.run_static_studio_pages_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["jobLink"], "https://www.ubisoft.com/en-us/company/careers/search?jobid=99")
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

    def test_scrapy_runner_emits_valid_envelope_selftest(self) -> None:
        runner_path = Path(jf.__file__).resolve().parent / "scrapers" / "runner.py"
        config = {
            "source": {
                "name": "Scrapy Test Studio",
                "studio": "Scrapy Test Studio",
                "pages": ["https://example.com/jobs"],
                "nlPriority": False,
                "remoteFriendly": True,
            },
            "runtime": {
                "timeout_s": 5,
                "retries": 1,
                "backoff_s": 1.0,
                "download_delay": 0.1,
            },
        }
        env = dict(os.environ)
        env["BALUFFO_SCRAPY_RUNNER_SELFTEST"] = "1"
        result = subprocess.run(  # noqa: S603
            [sys.executable, str(runner_path)],
            input=json.dumps(config),
            text=True,
            capture_output=True,
            check=False,
            env=env,
        )
        envelope = json.loads(result.stdout)
        self.assertIn("ok", envelope)
        self.assertIsInstance(envelope.get("jobs"), list)
        self.assertIsInstance(envelope.get("details"), list)
        self.assertIsInstance(envelope.get("partialErrors"), list)
        self.assertIsInstance(envelope.get("stats"), dict)
        detail = (envelope.get("details") or [{}])[0]
        self.assertIn("classification", detail)
        self.assertIn("browserFallbackRecommended", detail)
        self.assertIn("sourceId", detail)
        self.assertIn("candidate_links_found", envelope.get("stats") or {})

    def test_scrapy_runner_invalid_schema_emits_error_envelope(self) -> None:
        runner_path = Path(jf.__file__).resolve().parent / "scrapers" / "runner.py"
        result = subprocess.run(  # noqa: S603
            [sys.executable, str(runner_path)],
            input=json.dumps({"source": {"name": "Only Name"}}),
            text=True,
            capture_output=True,
            check=False,
        )
        envelope = json.loads(result.stdout)
        self.assertFalse(bool(envelope.get("ok")))
        self.assertIsInstance(envelope.get("partialErrors"), list)
        self.assertNotEqual(result.returncode, 0)

    def test_scrapy_runner_jobylon_v1_extracts_jobs(self) -> None:
        source_html = "<html><script>window.jbl_company_id = 2986;</script></html>"
        embed_html = """
        <div id="jobylon-job-329202">
            <div class="jobylon-job-title">Senior Support Engineer</div>
            <ul><li class="jobylon-location"><strong>Location</strong> Helsinki</li></ul>
            <a class="jobylon-apply-btn" href="https://emp.jobylon.com/jobs/329202-remedy-entertainment-senior-support-engineer/"></a>
        </div>
        <div id="jobylon-job-322343">
            <div class="jobylon-job-title">Development Director</div>
            <ul><li class="jobylon-location"><strong>Location</strong> Stockholm</li></ul>
            <a class="jobylon-apply-btn" href="https://emp.jobylon.com/jobs/322343-remedy-entertainment-development-director/"></a>
        </div>
        <div id="jobylon-job-000001">
            <div class="jobylon-job-title">Open Application</div>
            <a class="jobylon-apply-btn" href="https://emp.jobylon.com/jobylon-open-application/"></a>
        </div>
        """

        with mock.patch.object(scrapy_runner, "_http_text", side_effect=[source_html, embed_html]):
            jobs, stats, errors, reject_reasons = scrapy_runner._extract_jobylon_v1_jobs(
                source_name="Remedy",
                studio="Remedy Entertainment",
                page_url="https://www.remedygames.com/careers",
                timeout_s=20,
            )

        self.assertEqual(len(jobs), 2)
        self.assertEqual(stats.get("jobs_emitted"), 2)
        self.assertEqual(stats.get("candidate_links_found"), 2)
        self.assertEqual(stats.get("detail_pages_visited"), 2)
        self.assertEqual(errors, [])
        self.assertEqual(int(reject_reasons.get("open_application", 0)), 1)
        self.assertTrue(all(isinstance(row.get("sourceBundle"), list) and row.get("sourceBundle") for row in jobs))
        links = {str(row.get("jobLink") or "") for row in jobs}
        self.assertIn("https://emp.jobylon.com/jobs/329202-remedy-entertainment-senior-support-engineer/", links)
        self.assertIn("https://emp.jobylon.com/jobs/322343-remedy-entertainment-development-director/", links)

    def test_run_scrapy_static_source_handles_malformed_json(self) -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Scrapy Test Studio",
                "studio": "Scrapy Test Studio",
                "adapter": "scrapy_static",
                "pages": ["https://example.com/jobs"],
                "enabledByDefault": True,
            }
        ]
        fake_result = mock.Mock()
        fake_result.stdout = b"not json"
        fake_result.stderr = b"runner stderr"
        fake_result.returncode = 1
        try:
            with mock.patch("subprocess.run", return_value=fake_result):
                with mock.patch.object(jf, "set_source_diagnostics") as diag:
                    rows = jf.run_scrapy_static_source(
                        fetch_text=lambda _url, _timeout: "",
                        timeout_s=5,
                        retries=1,
                        backoff_s=1.0,
                    )
                    self.assertEqual(rows, [])
                    diag.assert_called_once()
                    args, kwargs = diag.call_args
                    self.assertEqual(args[0], "scrapy_static_sources")
                    self.assertEqual(kwargs.get("adapter"), "scrapy_static")
                    details = kwargs.get("details") or []
                    self.assertTrue(details)
                    self.assertEqual(str(details[0].get("classification") or ""), "parse_error")
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

    def test_scrapy_static_registration_in_default_loaders(self) -> None:
        self.assertIn("scrapy_static_sources", jfr.DEFAULT_SOURCE_LOADER_NAMES)
        self.assertIn("google_sheets_1er2oaxo", jfr.DEFAULT_SOURCE_LOADER_NAMES)
        self.assertIn("google_sheets_1mvqhxat", jfr.DEFAULT_SOURCE_LOADER_NAMES)
        self.assertEqual(jfr.SOURCE_REPORT_META["scrapy_static_sources"]["adapter"], "scrapy_static")
        self.assertEqual(jfr.SOURCE_REPORT_META["google_sheets_1er2oaxo"]["adapter"], "csv")
        self.assertEqual(jfr.SOURCE_REPORT_META["google_sheets_1mvqhxat"]["adapter"], "csv")
        names = [name for name, _ in jf.default_source_loaders()]
        self.assertIn("scrapy_static_sources", names)
        self.assertIn("google_sheets_1er2oaxo", names)
        self.assertIn("google_sheets_1mvqhxat", names)

    def test_run_pipeline_writes_browser_fallback_queue(self) -> None:
        def scraper_loader(**_: object):
            jf.set_source_diagnostics(
                "scrapy_static_sources",
                adapter="scrapy_static",
                studio="multiple",
                details=[
                    {
                        "adapter": "scrapy_static",
                        "studio": "Valve",
                        "name": "Valve Careers Scrapy",
                        "status": "ok",
                        "fetchedCount": 10,
                        "keptCount": 0,
                        "error": "",
                        "classification": "fetch_ok_extract_zero",
                        "browserFallbackRecommended": True,
                        "top_reject_reasons": ["missing_title:4"],
                        "sourceId": "valve-source-id",
                        "pages": ["https://www.valvesoftware.com/en/jobs"],
                        "stats": {
                            "downloader/request_count": 10,
                            "downloader/response_count": 10,
                            "downloader/response_status_count/200": 10,
                            "retry/count": 0,
                            "item_scraped_count": 0,
                            "candidate_links_found": 8,
                            "detail_pages_visited": 8,
                            "jobs_emitted": 0,
                            "jobs_rejected_validation": 8,
                            "finish_reason": "finished",
                        },
                    }
                ],
                partial_errors=[],
            )
            return []

        with workspace_tmpdir("jobs-fetcher-scrapy-fallback") as tmp:
            out = Path(tmp)
            report = jf.run_pipeline(
                output_dir=out,
                source_loaders=[("scrapy_static_sources", scraper_loader)],
                show_progress=False,
            )
            queue_path = out / "jobs-browser-fallback-queue.json"
            self.assertTrue(queue_path.exists())
            queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
            self.assertEqual(len(queue_rows), 1)
            self.assertEqual(str(queue_rows[0].get("adapter") or ""), "scrapy_static")
            self.assertEqual(str(queue_rows[0].get("classification") or ""), "fetch_ok_extract_zero")
            self.assertEqual(str((report.get("outputs") or {}).get("browserFallbackQueue") or ""), str(queue_path))
            details = ((report.get("sources") or [{}])[0].get("details") or [{}])[0]
            self.assertEqual(str(details.get("classification") or ""), "fetch_ok_extract_zero")
            self.assertTrue(bool(details.get("browserFallbackRecommended")))

    def test_map_profession_recognizes_focus_synonyms(self) -> None:
        self.assertEqual(jf.map_profession("Senior Tech Artist"), "technical-artist")
        self.assertEqual(jf.map_profession("Material Artist"), "technical-artist")
        self.assertEqual(jf.map_profession("World Artist"), "environment-artist")
        self.assertEqual(jf.map_profession("Terrain Artist"), "environment-artist")

    def test_compute_focus_score_prioritizes_target_nl_and_remote(self) -> None:
        ta_nl = jf.canonicalize_job(
            {
                "title": "Technical Artist",
                "company": "Studio NL",
                "city": "Amsterdam",
                "country": "NL",
                "workType": "Hybrid",
                "contractType": "Full-time",
                "jobLink": "https://example.com/ta-nl",
                "sector": "Game",
                "postedAt": "2026-03-01",
            },
            source="x",
            fetched_at=jf.now_iso(),
        )
        ta_remote = jf.canonicalize_job(
            {
                "title": "Technical Artist",
                "company": "Studio Remote",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/ta-remote",
                "sector": "Game",
                "postedAt": "2026-03-01",
            },
            source="x",
            fetched_at=jf.now_iso(),
        )
        non_target = jf.canonicalize_job(
            {
                "title": "Gameplay Programmer",
                "company": "Studio Other",
                "city": "Amsterdam",
                "country": "NL",
                "workType": "Hybrid",
                "contractType": "Full-time",
                "jobLink": "https://example.com/gameplay",
                "sector": "Game",
                "postedAt": "2026-03-01",
            },
            source="x",
            fetched_at=jf.now_iso(),
        )
        self.assertIsNotNone(ta_nl)
        self.assertIsNotNone(ta_remote)
        self.assertIsNotNone(non_target)
        self.assertGreater(ta_nl["focusScore"], ta_remote["focusScore"])
        self.assertGreater(ta_remote["focusScore"], non_target["focusScore"])

    def test_dedup_primary_key_prefers_richer_latest_record(self) -> None:
        first = jf.canonicalize_job(
            {
                "title": "Gameplay Programmer",
                "company": "Pixel Forge",
                "city": "Amsterdam",
                "country": "NL",
                "workType": "Hybrid",
                "contractType": "Full-time",
                "jobLink": "https://pixelforge.dev/jobs/123?utm_source=x",
                "sector": "Game",
                "postedAt": "2026-01-01",
            },
            source="a",
            fetched_at=jf.now_iso(),
        )
        second = jf.canonicalize_job(
            {
                "title": "Gameplay Programmer",
                "company": "Pixel Forge",
                "city": "Amsterdam",
                "country": "Netherlands",
                "workType": "Hybrid",
                "contractType": "Permanent",
                "jobLink": "https://pixelforge.dev/jobs/123",
                "sector": "Gaming",
                "postedAt": "2026-02-10",
                "sourceJobId": "r-2",
            },
            source="b",
            fetched_at=jf.now_iso(),
        )
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        rows, stats = jf.deduplicate_jobs([first, second])
        self.assertEqual(stats["outputCount"], 1)
        self.assertEqual(int(stats.get("mergedByPrimaryUrl") or 0), 1)
        self.assertEqual(int(stats.get("mergedBySecondaryKey") or 0), 0)
        self.assertEqual(int(stats.get("mergedBySocialKey") or 0), 0)
        self.assertEqual(rows[0]["sourceJobId"], "r-2")
        self.assertTrue(rows[0]["dedupKey"].startswith("url:"))

    def test_dedup_secondary_key_merges_without_link(self) -> None:
        first = jf.canonicalize_job(
            {
                "title": "Technical Artist",
                "company": "Orion Labs",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Contract",
                "jobLink": "",
                "sector": "Game",
                "postedAt": "2026-02-01",
            },
            source="a",
            fetched_at=jf.now_iso(),
        )
        second = jf.canonicalize_job(
            {
                "title": "Technical Artist",
                "company": "Orion Labs",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Contract",
                "jobLink": "",
                "sector": "Game",
                "postedAt": "2026-02-05",
            },
            source="b",
            fetched_at=jf.now_iso(),
        )
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        rows, stats = jf.deduplicate_jobs([first, second])
        self.assertEqual(stats["outputCount"], 1)
        self.assertEqual(int(stats.get("mergedBySecondaryKey") or 0), 1)
        self.assertTrue(rows[0]["dedupKey"].startswith("secondary:"))
        self.assertGreaterEqual(int(rows[0].get("sourceBundleCount") or 0), 2)
        self.assertIsInstance(rows[0].get("sourceBundle"), list)

    def test_canonicalize_job_with_reason_accounts_drop_reasons(self) -> None:
        dropped_title, reason_title = jf.canonicalize_job_with_reason(
            {"company": "Studio A", "jobLink": "https://example.com/jobs/1"},
            source="x",
            fetched_at=jf.now_iso(),
        )
        dropped_company, reason_company = jf.canonicalize_job_with_reason(
            {"title": "Gameplay Engineer", "jobLink": "https://example.com/jobs/2"},
            source="x",
            fetched_at=jf.now_iso(),
        )
        dropped_payload, reason_payload = jf.canonicalize_job_with_reason(
            "not-a-dict",
            source="x",
            fetched_at=jf.now_iso(),
        )
        self.assertIsNone(dropped_title)
        self.assertIsNone(dropped_company)
        self.assertIsNone(dropped_payload)
        self.assertEqual(reason_title, "missing_title")
        self.assertEqual(reason_company, "missing_company")
        self.assertEqual(reason_payload, "invalid_payload")

    def test_pipeline_partial_success_when_one_source_fails(self) -> None:
        def failing_loader(**_: object):
            raise RuntimeError("timeout")

        def ok_loader(**_: object):
            return [
                {
                    "title": "Gameplay Programmer",
                    "company": "Nebula Games",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "Remote",
                    "contractType": "Full-time",
                    "jobLink": "https://example.com/jobs/1",
                    "sector": "Game",
                    "sourceJobId": "ok-1",
                    "postedAt": "2026-02-10",
                }
            ]

        with workspace_tmpdir("jobs-fetcher") as tmp:
            report = jf.run_pipeline(
                output_dir=Path(tmp),
                source_loaders=[("failing", failing_loader), ("ok", ok_loader)],
            )

            self.assertEqual(report["summary"]["failedSources"], 1)
            self.assertEqual(report["summary"]["outputCount"], 1)

            output = json.loads((Path(tmp) / "jobs-unified.json").read_text(encoding="utf-8"))
            self.assertEqual(len(output), 1)
            self.assertEqual(output[0]["source"], "ok")

    def test_pipeline_preserves_previous_output_when_current_is_empty(self) -> None:
        existing = [
            {
                "id": 1,
                "title": "Engine Programmer",
                "company": "Archive Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://archive.example/jobs/1",
                "sector": "Game",
                "profession": "engine",
                "companyType": "Game",
                "description": "Engine Programmer at Archive Studio",
                "source": "archive",
                "sourceJobId": "archive-1",
                "fetchedAt": "2026-02-01T00:00:00+00:00",
                "postedAt": "2026-01-30T00:00:00+00:00",
                "dedupKey": "url:archive",
                "qualityScore": 100,
            }
        ]

        def empty_loader(**_: object):
            return []

        with workspace_tmpdir("jobs-fetcher") as tmp:
            out = Path(tmp)
            (out / "jobs-unified.json").write_text(json.dumps(existing), encoding="utf-8")
            report = jf.run_pipeline(output_dir=out, source_loaders=[("empty", empty_loader)])

            output = json.loads((out / "jobs-unified.json").read_text(encoding="utf-8"))
            self.assertEqual(len(output), 1)
            self.assertTrue(report["summary"]["preservedPreviousOutput"])

    def test_pipeline_tracks_likely_removed_jobs_in_lifecycle_state(self) -> None:
        def one_job_loader(**_: object):
            return [
                {
                    "title": "Engine Programmer",
                    "company": "Lifecycle Studio",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "Remote",
                    "contractType": "Full-time",
                    "jobLink": "https://example.com/lifecycle/engine-programmer",
                    "sector": "Game",
                    "sourceJobId": "life-1",
                    "postedAt": "2026-03-01",
                }
            ]

        def empty_loader(**_: object):
            return []

        previous_default_loaders = jf.default_source_loaders
        try:
            with workspace_tmpdir("jobs-fetcher") as tmp:
                out = Path(tmp)
                jf.default_source_loaders = lambda: [("only_source", one_job_loader)]
                first = jf.run_pipeline(output_dir=out, preserve_previous_on_empty=False)
                self.assertEqual(int(first["summary"].get("outputCount") or 0), 1)
                self.assertEqual(int(first["summary"].get("lifecycleActiveCount") or 0), 1)

                jf.default_source_loaders = lambda: [("only_source", empty_loader)]
                second = jf.run_pipeline(output_dir=out, preserve_previous_on_empty=False)
                self.assertEqual(int(second["summary"].get("outputCount") or 0), 0)
                self.assertEqual(int(second["summary"].get("lifecycleLikelyRemovedCount") or 0), 1)

                lifecycle_payload = json.loads((out / "jobs-lifecycle-state.json").read_text(encoding="utf-8"))
                jobs_map = lifecycle_payload.get("jobs") or {}
                self.assertEqual(len(jobs_map), 1)
                entry = list(jobs_map.values())[0]
                self.assertEqual(str(entry.get("status") or ""), "likely_removed")
                self.assertTrue(str(entry.get("removedAt") or ""))
        finally:
            jf.default_source_loaders = previous_default_loaders

    def test_pipeline_output_contract_matches_frontend(self) -> None:
        def ok_loader(**_: object):
            return [
                {
                    "title": "Technical Artist",
                    "company": "Orion Labs",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "remote",
                    "contractType": "contract",
                    "jobLink": "https://example.com/jobs/ta",
                    "sector": "gaming",
                    "sourceJobId": "ta-1",
                    "postedAt": "2026-02-01",
                }
            ]

        with workspace_tmpdir("jobs-fetcher") as tmp:
            jf.run_pipeline(output_dir=Path(tmp), source_loaders=[("ok", ok_loader)])
            rows = json.loads((Path(tmp) / "jobs-unified.json").read_text(encoding="utf-8"))
            self.assertEqual(len(rows), 1)
            row = rows[0]
            for field in jf.REQUIRED_FIELDS:
                self.assertIn(field, row)
            for field in jf.OPTIONAL_FIELDS:
                self.assertIn(field, row)
            self.assertEqual(row["workType"], "Remote")
            self.assertIsInstance(row["focusScore"], int)

    def test_pipeline_default_sources_exclude_wellfound_and_include_guerrilla(self) -> None:
        google_csv = self.fixture("google_sheets.csv")
        remote_json = self.fixture("remoteok.json")
        gamesindustry_html = self.fixture("gamesindustry_jobs.html")
        greenhouse_json = self.fixture("greenhouse_guerrilla_jobs.json")
        greenhouse_playstation_json = self.fixture("greenhouse_playstation_jobs.json")
        teamtailor_listing = self.fixture("teamtailor_listing.html")
        teamtailor_job = self.fixture("teamtailor_job.html")
        littlechicken_listing = self.fixture("littlechicken_jobs_page.html")
        littlechicken_detail = self.fixture("littlechicken_job_detail.html")
        lever_json = self.fixture("lever_jobs.json")
        smart_json = self.fixture("smartrecruiters_jobs.json")
        workable_json = self.fixture("workable_jobs.json")
        ashby_html = self.fixture("ashby_jobs.html")
        personio_xml = self.fixture("personio_feed.xml")

        def fake_fetch(url: str, _: int) -> str:
            if "docs.google.com/spreadsheets" in url or "api.allorigins.win/raw" in url:
                return google_csv
            if "remoteok.com/api" in url:
                return remote_json
            if "jobs.gamesindustry.biz" in url:
                return gamesindustry_html
            if "boards-api.greenhouse.io" in url and "guerrilla-games" in url:
                return greenhouse_json
            if "boards-api.greenhouse.io" in url and "sonyinteractiveentertainmentglobal" in url:
                return greenhouse_playstation_json
            if url == "https://career.paradoxplaza.com/jobs":
                return teamtailor_listing
            if "career.paradoxplaza.com/jobs/" in url:
                return teamtailor_job
            if "api.lever.co" in url:
                return lever_json
            if "api.smartrecruiters.com" in url:
                return smart_json
            if "apply.workable.com/api/v1/widget/accounts" in url:
                return workable_json
            if "jobs.ashbyhq.com" in url:
                return ashby_html
            if "jobs.personio.de/xml" in url:
                return personio_xml
            if url == "https://www.littlechicken.nl/about-us/jobs/" or url == "https://www.littlechicken.nl/job/":
                return littlechicken_listing
            if "littlechicken.nl/job/" in url:
                return littlechicken_detail
            raise RuntimeError(f"Unhandled URL in fake fetch: {url}")

        with workspace_tmpdir("jobs-fetcher") as tmp:
            report = jf.run_pipeline(
                output_dir=Path(tmp),
                fetch_text=fake_fetch,
                timeout_s=5,
                retries=0,
                backoff_s=0,
            )

            sources = {row["name"]: row for row in report["sources"]}
            self.assertEqual(sources["google_sheets"]["status"], "ok")
            self.assertEqual(sources["google_sheets_1er2oaxo"]["status"], "ok")
            self.assertEqual(sources["google_sheets_1mvqhxat"]["status"], "ok")
            self.assertEqual(sources["remote_ok"]["status"], "ok")
            self.assertEqual(sources["gamesindustry"]["status"], "ok")
            self.assertEqual(sources["greenhouse_boards"]["status"], "ok")
            self.assertEqual(sources["teamtailor_sources"]["status"], "ok")
            self.assertEqual(sources["lever_sources"]["status"], "ok")
            self.assertEqual(sources["smartrecruiters_sources"]["status"], "ok")
            self.assertEqual(sources["workable_sources"]["status"], "ok")
            self.assertEqual(sources["ashby_sources"]["status"], "ok")
            self.assertEqual(sources["personio_sources"]["status"], "ok")
            static_rows = [row for row in report["sources"] if str(row.get("adapter") or "").lower() == "static"]
            self.assertTrue(static_rows)
            self.assertTrue(any(str(row.get("status") or "").lower() == "ok" for row in static_rows))
            self.assertEqual(sources["wellfound"]["status"], "excluded")
            self.assertIn("disabled_by_default", sources["wellfound"]["error"])
            self.assertEqual(sources["greenhouse_boards"]["adapter"], "greenhouse")
            self.assertEqual(sources["teamtailor_sources"]["adapter"], "teamtailor")
            self.assertEqual(sources["lever_sources"]["adapter"], "lever")
            self.assertEqual(sources["smartrecruiters_sources"]["adapter"], "smartrecruiters")
            self.assertEqual(sources["workable_sources"]["adapter"], "workable")
            self.assertEqual(sources["ashby_sources"]["adapter"], "ashby")
            self.assertEqual(sources["personio_sources"]["adapter"], "personio")
            self.assertIn("failedSources", report["summary"])
            self.assertGreaterEqual(report["summary"]["excludedSources"], 1)
            self.assertIn("targetRoleCount", report["summary"])
            self.assertIn("netherlandsCount", report["summary"])
            self.assertIn("remoteCount", report["summary"])
            self.assertIn("rawFetchedCount", report["summary"])
            self.assertIn("uniqueOutputCount", report["summary"])
            self.assertIn("sourceBundleCollisions", report["summary"])

            rows = json.loads((Path(tmp) / "jobs-unified.json").read_text(encoding="utf-8"))
            self.assertTrue(any("guerrilla" in row.get("company", "").lower() for row in rows))
            self.assertTrue(any("playstation" in row.get("company", "").lower() for row in rows))
            self.assertTrue(any("paradox" in row.get("company", "").lower() for row in rows))
            self.assertTrue(any("little chicken" in row.get("company", "").lower() for row in rows))
            self.assertTrue(all("focusScore" in row for row in rows))
            self.assertTrue(all("sourceBundleCount" in row for row in rows))
            self.assertTrue(all("sourceBundle" in row for row in rows))
            all_errors = " ".join(row.get("error", "") for row in report["sources"])
            self.assertNotIn("403", all_errors)

    def test_run_pipeline_writes_normalized_report_task_and_source_state_contracts(self) -> None:
        def ok_loader(**_: object):
            return [
                {
                    "title": "Engine Programmer",
                    "company": "Contract Studio",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "Remote",
                    "contractType": "Full-time",
                    "jobLink": "https://example.com/contract/engine-programmer",
                    "sector": "Game",
                    "sourceJobId": "contract-1",
                    "postedAt": "2026-03-01",
                }
            ]

        with workspace_tmpdir("jobs-fetcher") as tmp:
            out = Path(tmp)
            report = jf.run_pipeline(
                output_dir=out,
                source_loaders=[("ok_source", ok_loader)],
                max_workers=2,
                max_per_domain=2,
            )
            self.assertEqual(str(report.get("schemaVersion") or ""), str(jf.SCHEMA_VERSION))
            runtime = report.get("runtime") or {}
            self.assertEqual(int(runtime.get("maxWorkers") or 0), 2)
            self.assertEqual(int(runtime.get("maxPerDomain") or 0), 2)
            self.assertEqual(str(runtime.get("fetchStrategy") or ""), "auto")
            self.assertIn(str(runtime.get("fetchClient") or ""), {"urllib", "httpx_async"})
            self.assertEqual(int(runtime.get("adapterHttpConcurrency") or 0), jf.DEFAULT_ADAPTER_HTTP_CONCURRENCY)
            self.assertEqual(int(runtime.get("selectedSourceCount") or 0), 1)
            self.assertIn("summary", report)
            self.assertIn("sources", report)
            self.assertEqual(str(report["sources"][0].get("fetchStrategy") or ""), "auto")
            self.assertIn("loss", report["sources"][0])
            self.assertIn("canonicalDropReasons", (report["sources"][0].get("loss") or {}))

            task_payload = json.loads((out / "jobs-fetch-tasks.json").read_text(encoding="utf-8"))
            self.assertEqual(str(task_payload.get("schemaVersion") or ""), str(jf.SCHEMA_VERSION))
            self.assertIn("summary", task_payload)
            self.assertIn("tasks", task_payload)
            self.assertIn("outputs", task_payload)
            self.assertEqual(str((task_payload.get("outputs") or {}).get("report") or ""), str(out / "jobs-fetch-report.json"))

            state_payload = json.loads((out / "jobs-source-state.json").read_text(encoding="utf-8"))
            self.assertEqual(str(state_payload.get("schemaVersion") or ""), str(jf.SCHEMA_VERSION))
            sources_state = state_payload.get("sources") or {}
            self.assertIn("ok_source", sources_state)
            self.assertEqual(int((sources_state["ok_source"]).get("consecutiveFailures") or 0), 0)

    def test_run_pipeline_includes_selection_exclusions(self) -> None:
        def ok_loader(**_: object):
            return [
                {
                    "title": "Technical Artist",
                    "company": "Incl Studio",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "Remote",
                    "contractType": "Full-time",
                    "jobLink": "https://example.com/included",
                    "sector": "Game",
                    "sourceJobId": "incl-1",
                    "postedAt": "2026-03-01",
                }
            ]

        with workspace_tmpdir("jobs-fetcher") as tmp:
            out = Path(tmp)
            report = jf.run_pipeline(
                output_dir=out,
                source_loaders=[("included_source", ok_loader)],
                selection_exclusions=[
                    {
                        "name": "excluded_source",
                        "status": "excluded",
                        "adapter": "custom",
                        "fetchStrategy": "auto",
                        "studio": "",
                        "fetchedCount": 0,
                        "keptCount": 0,
                        "error": "only_sources_filter",
                        "exclusionReason": "only_sources_filter",
                        "durationMs": 0,
                    }
                ],
            )
            excluded_rows = [row for row in (report.get("sources") or []) if row.get("name") == "excluded_source"]
            self.assertEqual(len(excluded_rows), 1)
            self.assertEqual(str(excluded_rows[0].get("status") or ""), "excluded")
            self.assertEqual(str(excluded_rows[0].get("exclusionReason") or ""), "only_sources_filter")

    def test_should_skip_source_by_ttl_honors_recent_success_and_failure_state(self) -> None:
        now = jf.now_iso()
        rows = {"source_a": {"lastSuccessAt": now, "consecutiveFailures": 0}}
        self.assertTrue(jf.should_skip_source_by_ttl("source_a", rows, ttl_minutes=360))

        rows["source_a"]["consecutiveFailures"] = 2
        self.assertFalse(jf.should_skip_source_by_ttl("source_a", rows, ttl_minutes=360))

    def test_should_skip_source_by_cadence_uses_hot_and_cold_windows(self) -> None:
        now = jf.datetime.now(jf.timezone.utc)
        rows = {
            "hot_source": {
                "lastSuccessAt": (now - jf.timedelta(minutes=10)).isoformat(),
                "lastChangedAt": (now - jf.timedelta(minutes=30)).isoformat(),
                "consecutiveFailures": 0,
            },
            "cold_source": {
                "lastSuccessAt": (now - jf.timedelta(minutes=20)).isoformat(),
                "lastChangedAt": (now - jf.timedelta(days=2)).isoformat(),
                "consecutiveFailures": 0,
            },
        }
        self.assertTrue(jf.should_skip_source_by_cadence("hot_source", rows, hot_minutes=15, cold_minutes=60))
        self.assertTrue(jf.should_skip_source_by_cadence("cold_source", rows, hot_minutes=15, cold_minutes=60))

        rows["hot_source"]["lastSuccessAt"] = (now - jf.timedelta(minutes=20)).isoformat()
        rows["cold_source"]["lastSuccessAt"] = (now - jf.timedelta(minutes=70)).isoformat()
        self.assertFalse(jf.should_skip_source_by_cadence("hot_source", rows, hot_minutes=15, cold_minutes=60))
        self.assertFalse(jf.should_skip_source_by_cadence("cold_source", rows, hot_minutes=15, cold_minutes=60))

    def test_run_pipeline_excludes_quarantined_source_unless_ignored(self) -> None:
        calls = {"count": 0}

        def ok_loader(**_: object):
            calls["count"] += 1
            return [
                {
                    "title": "Gameplay Engineer",
                    "company": "Circuit Studio",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "Remote",
                    "contractType": "Full-time",
                    "jobLink": "https://example.com/circuit/gameplay-engineer",
                    "sector": "Game",
                    "sourceJobId": "circuit-1",
                    "postedAt": "2026-03-01",
                }
            ]

        with workspace_tmpdir("jobs-fetcher") as tmp:
            out = Path(tmp)
            blocked_until = (jf.datetime.now(jf.timezone.utc) + jf.timedelta(hours=2)).isoformat()
            state_payload = {
                "updatedAt": jf.now_iso(),
                "sources": {
                    "blocked_source": {
                        "consecutiveFailures": 3,
                        "quarantinedUntilAt": blocked_until,
                    }
                },
            }
            (out / "jobs-source-state.json").write_text(json.dumps(state_payload), encoding="utf-8")

            blocked_report = jf.run_pipeline(
                output_dir=out,
                source_loaders=[("blocked_source", ok_loader)],
                circuit_breaker_failures=3,
                circuit_breaker_cooldown_minutes=180,
                ignore_circuit_breaker=False,
            )
            blocked_rows = [row for row in blocked_report.get("sources", []) if row.get("name") == "blocked_source"]
            self.assertEqual(calls["count"], 0)
            self.assertEqual(len(blocked_rows), 1)
            self.assertEqual(str(blocked_rows[0].get("status") or ""), "excluded")
            self.assertIn("circuit_breaker_active_until", str(blocked_rows[0].get("error") or ""))

            unblocked_report = jf.run_pipeline(
                output_dir=out,
                source_loaders=[("blocked_source", ok_loader)],
                circuit_breaker_failures=3,
                circuit_breaker_cooldown_minutes=180,
                ignore_circuit_breaker=True,
            )
            unblocked_rows = [row for row in unblocked_report.get("sources", []) if row.get("name") == "blocked_source"]
            self.assertGreaterEqual(calls["count"], 1)
            self.assertEqual(len(unblocked_rows), 1)
            self.assertEqual(str(unblocked_rows[0].get("status") or ""), "ok")

    def test_pipeline_report_snapshot_contract(self) -> None:
        def ok_loader(**_: object):
            return [
                {
                    "title": "Technical Artist",
                    "company": "Snapshot Studio",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "Remote",
                    "contractType": "Full-time",
                    "jobLink": "https://example.com/snapshot/ta",
                    "sector": "Game",
                    "sourceJobId": "snap-1",
                    "postedAt": "2026-03-01",
                }
            ]

        with workspace_tmpdir("jobs-fetcher") as tmp:
            report = jf.run_pipeline(output_dir=Path(tmp), source_loaders=[("ok", ok_loader)])
            snapshot = {
                "schemaVersion": report.get("schemaVersion"),
                "summary": {
                    "inputCount": int(report["summary"].get("inputCount") or 0),
                    "mergedCount": int(report["summary"].get("mergedCount") or 0),
                    "outputCount": int(report["summary"].get("outputCount") or 0),
                    "rawFetchedCount": int(report["summary"].get("rawFetchedCount") or 0),
                    "uniqueOutputCount": int(report["summary"].get("uniqueOutputCount") or 0),
                    "sourceCount": int(report["summary"].get("sourceCount") or 0),
                    "successfulSources": int(report["summary"].get("successfulSources") or 0),
                    "failedSources": int(report["summary"].get("failedSources") or 0),
                    "excludedSources": int(report["summary"].get("excludedSources") or 0),
                },
                "outputs": {
                    "hasJson": bool(report.get("outputs", {}).get("json")),
                    "hasCsv": bool(report.get("outputs", {}).get("csv")),
                    "hasLightJson": bool(report.get("outputs", {}).get("lightJson")),
                    "hasChangedFlags": isinstance(report.get("outputs", {}).get("changed"), dict),
                },
                "sources": [
                    {
                        "name": str(report["sources"][0].get("name")),
                        "status": str(report["sources"][0].get("status")),
                        "fetchedCount": int(report["sources"][0].get("fetchedCount") or 0),
                        "keptCount": int(report["sources"][0].get("keptCount") or 0),
                    }
                ],
            }
            self.assertEqual(snapshot, self.fixture_json("jobs_fetch_report_snapshot.json"))


if __name__ == "__main__":
    unittest.main()
