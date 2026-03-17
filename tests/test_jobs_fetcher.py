import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from unittest import mock

from src import jobs_fetcher as jf
from src import jobs_fetcher_registry as jfr
from src.jobs import common as jobs_common
from src.scrapers import runner as scrapy_runner
from tests.helpers.temp_paths import workspace_tmpdir
from src.jobs.adapters import _runtime as runtime_resolver
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.types import AdapterPluginContext


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def _fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def _fixture_json(name: str):
    return json.loads(_fixture(name))


def test_parse_google_sheets_csv_fixture() -> None:
    rows = jf.parse_google_sheets_csv(_fixture("google_sheets.csv"))
    assert len(rows) == 2
    assert rows[0]["title"] == "Gameplay Programmer"
    assert rows[0]["company"] == "Pixel Forge"


def test_runtime_facade_falls_back_to_main_module_for_jobs_fetcher_runs() -> None:
    prev_jf = runtime_resolver.sys.modules.get("src.jobs_fetcher")
    prev_main = runtime_resolver.sys.modules.get("__main__")
    try:
        runtime_resolver.sys.modules.pop("src.jobs_fetcher", None)
        # When run as `python -m src.jobs_fetcher`, __main__ is the jobs_fetcher module.
        # Simulate that so facade() returns a module that has the parser attributes.
        runtime_resolver.sys.modules["__main__"] = jf
        main_mod = runtime_resolver.sys.modules.get("__main__")
        class _Spec:
            name = "src.jobs_fetcher"
        if main_mod is None:
            raise RuntimeError("__main__ module missing")
        prev_spec = getattr(main_mod, "__spec__", None)
        main_mod.__spec__ = _Spec()  # type: ignore[attr-defined]
        resolved = runtime_resolver.facade()
        assert resolved is main_mod
        assert callable(getattr(resolved, "parse_ashby_jobs_from_html", None))
        assert callable(getattr(resolved, "parse_personio_feed_xml", None))
        assert callable(getattr(resolved, "parse_epic_games_jobs_payload", None))
        main_mod.__spec__ = prev_spec  # type: ignore[attr-defined]
    finally:
        if prev_main is not None:
            runtime_resolver.sys.modules["__main__"] = prev_main
        if prev_jf is not None:
            runtime_resolver.sys.modules["src.jobs_fetcher"] = prev_jf


def test_static_plugin_registry_selects_supercell_plugin() -> None:
    ctx = AdapterPluginContext(family="static", adapter_key="static", source_identity="supercell.com")
    plugin, selection = default_registry.select(ctx)
    assert selection.plugin_name in {"supercell"}


def test_registry_entries_static_filters_redundant_when_provider_present() -> None:
    """When the registry has both a provider source (e.g. SmartRecruiters CD PROJEKT RED) and a static source for the same careers host, static entries for that host are excluded."""
    provider_entry = {
        "name": "CD PROJEKT RED (SmartRecruiters)",
        "studio": "CD PROJEKT RED",
        "adapter": "smartrecruiters",
        "company_id": "CDPROJEKTRED",
        "api_url": "https://api.smartrecruiters.com/v1/companies/CDPROJEKTRED/postings",
        "enabledByDefault": True,
    }
    static_cdprojekt = {
        "name": "Cdprojektred (Manual Website)",
        "studio": "Cdprojektred",
        "adapter": "static",
        "pages": ["https://www.cdprojektred.com/en/jobs"],
        "enabledByDefault": True,
    }
    static_other = {
        "name": "Other Studio (Manual Website)",
        "studio": "Other",
        "adapter": "static",
        "pages": ["https://other.com/careers"],
        "enabledByDefault": True,
    }
    patched_registry = [provider_entry, static_cdprojekt, static_other]
    with mock.patch.object(jobs_common, "STUDIO_SOURCE_REGISTRY", patched_registry):
        static_entries = jobs_common.registry_entries("static")
    # Redundant static (cdprojektred) must be filtered out; other static must remain.
    names = [e.get("name") for e in static_entries]
    assert "Other Studio (Manual Website)" in names
    assert "Cdprojektred (Manual Website)" not in names


def test_parse_google_sheets_csv_supports_job_type_link_headers() -> None:
        csv_text = (
            "Intro row,,,,,,,,,\n"
            "Company,Company Category,Job Category,Job,Job Type,Postal Code,City,Fully Remote?,Link,Added\n"
            "Studio A,Developer,Programming,Gameplay Programmer,Full-Time,10115,Berlin,Yes,https://example.com/jobs/1,2026-03-10\n"
        )
        rows = jf.parse_google_sheets_csv(csv_text)
        assert len(rows) == 1
        assert rows[0]["title"] == "Gameplay Programmer"
        assert rows[0]["company"] == "Studio A"
        assert rows[0]["contractType"] == "Full-Time"
        assert rows[0]["jobLink"] == "https://example.com/jobs/1"
        assert rows[0]["sector"] == "Developer"

def test_parse_google_sheets_csv_supports_studio_header_alias() -> None:
        csv_text = (
            "Studio,Country,Job Title,Experience Level,Link\n"
            "Acme Games,Germany,Senior Gameplay Engineer,Senior,https://example.com/jobs/42\n"
        )
        rows = jf.parse_google_sheets_csv(csv_text)
        assert len(rows) == 1
        assert rows[0]["company"] == "Acme Games"
        assert rows[0]["country"] == "Germany"
        assert rows[0]["title"] == "Senior Gameplay Engineer"

def test_parse_google_sheets_csv_skips_known_bad_company_labels() -> None:
        csv_text = (
            "Company,Company Name,City,Country,Job Title,Link\n"
            "giant enemy crab,Actual Studio,Amsterdam,NL,Gameplay Engineer,https://example.com/jobs/77\n"
        )
        rows = jf.parse_google_sheets_csv(csv_text)
        assert len(rows) == 1
        assert rows[0]["company"] == "Actual Studio"

def test_parse_google_sheets_csv_preserves_untrustworthy_company_as_unknown() -> None:
        csv_text = (
            "Company,City,Country,Job Title,Link\n"
            "FarBridge,Amsterdam,NL,Gameplay Engineer,https://example.com/jobs/88\n"
        )
        rows = jf.parse_google_sheets_csv(csv_text)
        assert len(rows) == 1
        assert rows[0]["company"] == jf.UNKNOWN_COMPANY_LABEL

def test_parse_google_sheets_csv_recovers_job_link_from_source_contact() -> None:
        csv_text = (
            "Company,City,Country,Job Title,Job Link,Source/Contact\n"
            "Insomniac Games,Burbank,US,Character TD,,https://insomniac.games/careers/character-td\n"
        )
        rows = jf.parse_google_sheets_csv(csv_text)
        assert len(rows) == 1
        assert rows[0]["jobLink"] == "https://insomniac.games/careers/character-td"

def test_parse_google_sheets_csv_ignores_email_only_source_contact() -> None:
        csv_text = (
            "Company,City,Country,Job Title,Source/Contact\n"
            "Studio A,Amsterdam,NL,Gameplay Engineer,jobs@example.com\n"
        )
        rows = jf.parse_google_sheets_csv(csv_text)
        assert len(rows) == 1
        assert rows[0]["jobLink"] == ""

def test_canonicalize_job_with_reason_preserves_known_bad_company_labels_as_unknown() -> None:
        normalized, reason = jf.canonicalize_job_with_reason(
            {
                "title": "Gameplay Engineer",
                "company": "giant enemy crab",
                "jobLink": "https://example.com/jobs/99",
            },
            source="google_sheets",
            fetched_at="2026-03-13T10:00:00Z",
        )
        assert normalized is not None
        assert normalized["company"] == jf.UNKNOWN_COMPANY_LABEL
        assert reason == ""

def test_parse_args_uses_config_backed_output_and_social_defaults() -> None:
        prev_argv = list(sys.argv)
        try:
            sys.argv = ["jobs_fetcher.py"]
            args = jf.parse_args()
        finally:
            sys.argv = prev_argv
        assert Path(args.output_dir) == jf.DEFAULT_OUTPUT_DIR
        assert Path(args.social_config_path) == jf.DEFAULT_SOCIAL_CONFIG_PATH


def test_default_source_loaders_includes_all_registry_sources() -> None:
    """All DEFAULT_SOURCE_LOADER_NAMES (except static_studio_pages*) are attempted via loaders or static shards."""
    base_expected = {
        n for n in jfr.DEFAULT_SOURCE_LOADER_NAMES
        if n not in {"static_studio_pages", "static_studio_pages_a_i", "static_studio_pages_j_r", "static_studio_pages_s_z"}
    }
    loaders_with_social = jf.default_source_loaders(social_enabled=True)
    loader_names = {name for name, _ in loaders_with_social}
    for name in base_expected:
        assert name in loader_names, f"Registry source {name} should be in default loaders when social_enabled=True"
    assert len(loaders_with_social) >= len(base_expected), "Loaders should include all base sources plus static shards"


def test_parse_remote_ok_payload_filters_game_roles() -> None:
        payload = json.loads(_fixture("remoteok.json"))
        rows = jf.parse_remote_ok_payload(payload)
        assert len(rows) == 1
        assert rows[0]["sourceJobId"] == "101"
        assert rows[0]["company"] == "Nebula Games"

def test_run_remote_ok_source_falls_back_to_secondary_endpoint() -> None:
        payload = _fixture("remoteok.json")
        calls = []

        def fake_fetch(url: str, _: int) -> str:
            calls.append(url)
            if "remoteok.com/api" in url:
                raise RuntimeError("primary endpoint failed")
            if "remoteok.io/api" in url:
                return payload
            raise RuntimeError(f"Unhandled URL: {url}")

        rows = jf.run_remote_ok_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
        assert len(rows) == 1
        assert len(calls) == 2
        assert "remoteok.com/api" in calls[0]
        assert "remoteok.io/api" in calls[1]

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

def test_run_social_x_source_uses_rss_fallback_without_credentials() -> None:
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
        assert len(rows) == 1
        assert "careers.nova.dev" in rows[0]["jobLink"]

def test_deduplicate_jobs_uses_social_source_id_fallback() -> None:
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
        assert len(deduped) == 1
        assert stats["mergedCount"] == 1

def test_deduplicate_jobs_merges_resolved_redirect_with_direct_job() -> None:
        redirect_target = "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-technical-director-level-design-m-f-nb-projet-non-annonce"
        redirect_resolver = lambda url: redirect_target if "gracklehq.com/rd/372393" in str(url) else str(url)
        redirect_row = jf.canonicalize_job(
            {
                "title": "Technical Director Level Design - M/F/NB - unannounced project",
                "company": jf.UNKNOWN_COMPANY_LABEL,
                "city": "Montpellier",
                "country": "France",
                "workType": "Onsite",
                "contractType": "Unknown",
                "jobLink": "https://gracklehq.com/rd/372393",
                "sector": "Game",
                "sourceJobId": "sheet-12396",
            },
            source="google_sheets",
            fetched_at=jf.now_iso(),
            resolve_redirect_url=redirect_resolver,
        )
        direct_row = jf.canonicalize_job(
            {
                "title": "Technical Director Level Design - M/F/NB - unannounced project",
                "company": "Ubisoft",
                "city": "Montpellier",
                "country": "France",
                "workType": "Onsite",
                "contractType": "Unknown",
                "jobLink": redirect_target,
                "sector": "Game",
                "sourceJobId": "sheet-12551",
            },
            source="google_sheets",
            fetched_at=jf.now_iso(),
        )
        assert redirect_row is not None
        assert direct_row is not None
        rows, stats = jf.deduplicate_jobs([redirect_row, direct_row])
        assert stats["outputCount"] == 1
        assert int(stats.get("mergedByPrimaryUrl") or 0) == 1
        assert rows[0]["company"] == "Ubisoft"
        assert rows[0]["jobLink"] == redirect_target
        assert int(rows[0].get("sourceBundleCount") or 0) == 2

def test_deduplicate_jobs_keeps_unresolved_redirect_separate() -> None:
        redirect_row = jf.canonicalize_job(
            {
                "title": "Technical Director Level Design - M/F/NB - unannounced project",
                "company": jf.UNKNOWN_COMPANY_LABEL,
                "city": "Montpellier",
                "country": "France",
                "workType": "Onsite",
                "contractType": "Unknown",
                "jobLink": "https://gracklehq.com/rd/372393",
                "sector": "Game",
                "sourceJobId": "sheet-12396",
            },
            source="google_sheets",
            fetched_at=jf.now_iso(),
            resolve_redirect_url=lambda url: str(url),
        )
        direct_row = jf.canonicalize_job(
            {
                "title": "Technical Director Level Design - M/F/NB - unannounced project",
                "company": "Ubisoft",
                "city": "Montpellier",
                "country": "France",
                "workType": "Onsite",
                "contractType": "Unknown",
                "jobLink": "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-technical-director-level-design-m-f-nb-projet-non-annonce",
                "sector": "Game",
                "sourceJobId": "sheet-12551",
            },
            source="google_sheets",
            fetched_at=jf.now_iso(),
        )
        assert redirect_row is not None
        assert direct_row is not None
        rows, stats = jf.deduplicate_jobs([redirect_row, direct_row])
        assert stats["outputCount"] == 2
        assert int(stats.get("mergedByPrimaryUrl") or 0) == 0

def test_pooled_redirect_resolver_reuses_cached_resolution_and_headers() -> None:
        calls = []
        fake_httpx = type(
            "_FakeHttpxModule",
            (),
            {
                "Client": None,
                "Timeout": staticmethod(lambda value: value),
                "Limits": staticmethod(lambda **kwargs: kwargs),
            },
        )()

        class _FakeClient:
            def request(self, method: str, url: str):  # noqa: ANN001
                calls.append((method, url))
                return type("_Resp", (), {"url": "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role"})()

            def close() -> None:
                return None

        with mock.patch.object(fake_httpx, "Client", return_value=_FakeClient()) as client_ctor:
            with mock.patch.object(jf, "httpx", fake_httpx):
                resolver = jf.build_redirect_resolver(timeout_s=5, max_connections=4)
                try:
                    first = resolver.resolve("https://gracklehq.com/rd/372393")
                    second = resolver.resolve("https://gracklehq.com/rd/372393")
                finally:
                    resolver.close()

        assert first == "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role"
        assert second == first
        assert len(calls) == 1
        assert client_ctor.call_args.kwargs.get("headers") == jf.DEFAULT_REDIRECT_HEADERS

def test_canonicalize_google_sheets_rows_uses_redirect_cache_once_for_duplicates() -> None:
        rows = [
            {
                "sourceJobId": "sheet-1",
                "title": "Technical Director",
                "company": jf.UNKNOWN_COMPANY_LABEL,
                "city": "Paris",
                "country": "France",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://gracklehq.com/rd/372393",
                "sector": "Game",
            },
            {
                "sourceJobId": "sheet-2",
                "title": "Technical Director",
                "company": jf.UNKNOWN_COMPANY_LABEL,
                "city": "Paris",
                "country": "France",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://gracklehq.com/rd/372393",
                "sector": "Game",
            },
        ]

        class _FakeResolver:
            def __init__(self) -> None:
                self.cache = {}
                self.calls = 0
                self.cache_hits = 0

            def resolve(self, url: str) -> str:
                if url in self.cache:
                    self.cache_hits += 1
                    return self.cache[url]
                self.calls += 1
                resolved = "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role"
                self.cache[url] = resolved
                return resolved

            def snapshot_stats(self) -> dict:
                return {"cacheHits": self.cache_hits, "resolvedCount": self.calls}

        canonical_rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
            rows,
            source="google_sheets",
            fetched_at="2026-03-13T00:00:00+00:00",
            redirect_resolver=_FakeResolver(),
            redirect_concurrency=4,
        )
        assert len(canonical_rows) == 2
        assert not drop_reasons
        assert stats["redirect_candidates"] == 2
        assert stats["redirect_resolved"] == 2
        assert stats["redirect_cache_hits"] == 1
        assert all("smartrecruiters.com" in row["jobLink"] for row in canonical_rows)

def test_canonicalize_google_sheets_rows_falls_back_when_redirect_resolution_fails() -> None:
        rows = [
            {
                "sourceJobId": "sheet-1",
                "title": "Character TD",
                "company": jf.UNKNOWN_COMPANY_LABEL,
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://gracklehq.com/rd/999999",
                "sector": "Game",
            }
        ]

        class _FakeResolver:
            def resolve(self, url: str) -> str:
                return url

            def snapshot_stats(self) -> dict:
                return {"cacheHits": 0, "resolvedCount": 0}

        canonical_rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
            rows,
            source="google_sheets",
            fetched_at="2026-03-13T00:00:00+00:00",
            redirect_resolver=_FakeResolver(),
            redirect_concurrency=2,
        )
        assert len(canonical_rows) == 1
        assert not drop_reasons
        assert canonical_rows[0]["jobLink"] == "https://gracklehq.com/rd/999999"
        assert stats["redirect_resolved"] == 0

def test_fingerprint_url_matches_smartrecruiters_short_and_slugged_urls() -> None:
        short = "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145"
        slugged = "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-technical-director-level-design-m-f-nb-projet-non-annonce"
        api = "https://api.smartrecruiters.com/v1/companies/Ubisoft2/postings/744000108777145"
        assert jf.fingerprint_url(short) == jf.fingerprint_url(slugged)
        assert jf.fingerprint_url(short) == jf.fingerprint_url(api)

def test_run_pipeline_social_sources_report_and_output() -> None:
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
            assert sources["social_reddit"]["status"] == "ok"
            assert sources["social_x"]["status"] == "ok"
            assert sources["social_mastodon"]["status"] == "ok"
            assert sources["social_reddit"]["keptCount"] == 1
            rows = json.loads((Path(tmp) / "jobs-unified.json").read_text(encoding="utf-8"))
            assert any(str(row.get("source") or "").startswith("social_") for row in rows)

def test_parse_gamesindustry_html_fixture() -> None:
        rows = jf.parse_gamesindustry_html(_fixture("gamesindustry_jobs.html"), base_url="https://jobs.gamesindustry.biz")
        assert len(rows) == 2
        assert rows[0]["title"] == "Senior Quality Analyst"
        assert rows[0]["company"] == "Sharkmob"
        assert rows[0]["sourceJobId"] == "43821"
        assert rows[0]["jobLink"].startswith("https://jobs.gamesindustry.biz/job/")
        titles = {row["title"] for row in rows}
        assert "Read more" not in titles
        assert "Programming (6)" not in titles

def test_parse_greenhouse_jobs_payload_fixture() -> None:
        payload = json.loads(_fixture("greenhouse_guerrilla_jobs.json"))
        rows = jf.parse_greenhouse_jobs_payload(payload, "guerrilla-games")
        assert len(rows) == 2
        assert all(row["sourceJobId"].startswith("greenhouse:guerrilla-games:") for row in rows)
        assert rows[0]["company"] == "Guerrilla Games"
        assert rows[0]["country"] == "Netherlands"

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
        assert rows[0]["city"] == "Delft"
        assert rows[0]["country"] == "NL"

def test_parse_wellfound_html_fixture() -> None:
        rows = jf.parse_wellfound_html(_fixture("wellfound.html"))
        assert len(rows) == 1
        assert rows[0]["sourceJobId"] == "wf-1"
        assert rows[0]["workType"] == "Remote"

def test_parse_lever_jobs_payload_fixture() -> None:
        payload = json.loads(_fixture("lever_jobs.json"))
        rows = jf.parse_lever_jobs_payload(payload, "sandboxvr", fallback_company="Sandbox VR")
        assert len(rows) == 1
        assert rows[0]["title"] == "Technical Artist"
        assert rows[0]["country"] == "NL"

def test_parse_smartrecruiters_jobs_payload_fixture() -> None:
        payload = json.loads(_fixture("smartrecruiters_jobs.json"))
        rows = jf.parse_smartrecruiters_jobs_payload(payload, "CDPROJEKTRED", fallback_company="CD PROJEKT RED")
        assert len(rows) == 1
        assert rows[0]["title"] == "Environment Artist"
        assert rows[0]["company"] == "CD PROJEKT RED"

def test_parse_workable_jobs_payload_fixture() -> None:
        payload = json.loads(_fixture("workable_jobs.json"))
        rows = jf.parse_workable_jobs_payload(payload, "hutch", fallback_company="Hutch")
        assert len(rows) == 1
        assert rows[0]["workType"] == "Remote"

def test_parse_ashby_jobs_from_html_fixture() -> None:
        rows = jf.parse_ashby_jobs_from_html(_fixture("ashby_jobs.html"), "https://jobs.ashbyhq.com/jagex/jobs", "Jagex")
        assert len(rows) == 2
        assert all("jobs.ashbyhq.com" in row["jobLink"] for row in rows)

def test_parse_personio_feed_xml_fixture() -> None:
        rows = jf.parse_personio_feed_xml(_fixture("personio_feed.xml"), source_name="InnoGames")
        assert len(rows) >= 1
        assert any(row["title"] == "Environment Artist" for row in rows)

def test_normalize_source_report_row_preserves_structured_details() -> None:
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
        assert isinstance(details, list)
        assert isinstance(details[0], dict)
        assert details[0]["name"] == "Jagex (Lever)"
        assert int(details[0]["keptCount"]) == 2

def test_normalize_source_report_row_preserves_static_stage_timings() -> None:
        row = jf.normalize_source_report_row({
            "name": "static_source::test",
            "status": "ok",
            "adapter": "static",
            "stageTimingsMs": {
                "listingFetch": 120,
                "candidateExtraction": 45,
                "detailFetch": 310,
                "canonicalization": 12,
            },
            "details": [
                {
                    "adapter": "static",
                    "studio": "Test Studio",
                    "name": "Test Studio",
                    "status": "ok",
                    "stats": {
                        "candidate_links_found": 8,
                        "detail_pages_visited": 4,
                        "jobs_emitted": 3,
                        "fetch_cache_hits": 2,
                        "detail_yield_percent": 75,
                        "listing_fetch_ms": 120,
                        "candidate_extraction_ms": 45,
                        "detail_fetch_ms": 310,
                    },
                }
            ],
        })
        assert (row.get("stageTimingsMs") or {}).get("detailFetch") == 310
        detail_stats = ((row.get("details") or [{}])[0].get("stats") or {})
        assert int(detail_stats.get("fetch_cache_hits") or 0) == 2
        assert int(detail_stats.get("detail_yield_percent") or 0) == 75

def test_normalize_source_report_row_preserves_google_sheets_redirect_stats() -> None:
        row = jf.normalize_source_report_row({
            "name": "google_sheets",
            "status": "ok",
            "adapter": "csv",
            "stageTimingsMs": {
                "parseCsv": 55,
                "redirectResolve": 91,
                "canonicalization": 120,
            },
            "details": [
                {
                    "adapter": "csv",
                    "studio": "community_sheet",
                    "name": "google_sheets",
                    "status": "ok",
                    "stats": {
                        "parse_csv_ms": 55,
                        "redirect_candidates": 7,
                        "redirect_resolved": 6,
                        "redirect_cache_hits": 2,
                        "redirect_resolve_ms": 91,
                    },
                }
            ],
        })
        assert (row.get("stageTimingsMs") or {}).get("redirectResolve") == 91
        detail_stats = ((row.get("details") or [{}])[0].get("stats") or {})
        assert int(detail_stats.get("redirect_candidates") or 0) == 7
        assert int(detail_stats.get("redirect_cache_hits") or 0) == 2

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
            def fake_fetch(url: str, _: int) -> str:
                assert "boards-api.greenhouse.io" in url
                assert "guerrilla-games" in url
                return payload

            rows = jf.run_greenhouse_boards_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
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

            rows = jf.run_teamtailor_sources_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            assert len(rows) >= 1
            assert any("career.paradoxplaza.com/jobs/" in row["jobLink"] for row in rows)

def test_run_static_studio_pages_source_with_fixture() -> None:
        listing = _fixture("littlechicken_jobs_page.html")
        detail = _fixture("littlechicken_job_detail.html")
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        # Use example.net so the generic fallback runs (no static plugin handles it)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Fallback Test Studio",
                "studio": "Fallback Test Studio",
                "adapter": "static",
                "company": "Fallback Test Studio",
                "pages": ["https://example.net/about-us/jobs/"],
                "enabledByDefault": True,
            }
        ]

        try:
            def fake_fetch(url: str, _: int) -> str:
                if url == "https://example.net/about-us/jobs/":
                    return listing
                if "/job/" in url:
                    return detail
                raise RuntimeError(f"Unexpected URL: {url}")

            rows = jf.run_static_studio_pages_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            assert len(rows) == 2
            assert any("/job/" in (row.get("jobLink") or "") for row in rows)
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

def test_run_static_studio_pages_source_loads_kojima_dynamic_listing() -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        # Use a host that has no static plugin so the generic flow runs (no Kojima plugin).
        # Listing HTML already contains job links so we don't need the dynamic POST.
        base_url = "https://kojima-careers-test.example.com/en/careers"
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Kojima Productions (Manual Website)",
                "studio": "Kojima Productions",
                "adapter": "static",
                "company": "Kojima Productions",
                "pages": [base_url],
                "enabledByDefault": True,
            }
        ]
        listing_html = """
        <table>
          <tr class="job-listing-item"><td><a href="/en/game-programmer">Game Programmer</a></td></tr>
          <tr class="job-listing-item"><td><a href="/en/ai-programmer">AI Programmer</a></td></tr>
        </table>
        """

        def fake_fetch(url: str, _: int) -> str:
            if url == base_url:
                return listing_html
            if url in {
                "https://kojima-careers-test.example.com/en/game-programmer",
                "https://kojima-careers-test.example.com/en/ai-programmer",
            }:
                return "<html><body><h1>job</h1></body></html>"
            raise RuntimeError(f"Unexpected URL: {url}")

        try:
            rows = jf.run_static_studio_pages_source(
                fetch_text=fake_fetch,
                timeout_s=5,
                retries=0,
                backoff_s=0,
            )
            titles = {str(row.get("title") or "") for row in rows}
            assert "Game Programmer" in titles
            assert "Ai Programmer" in titles
            assert len(rows) == 2
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

def test_run_static_studio_pages_source_accepts_larian_uuid_paths_and_rejects_location_pages() -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        # Use larian.com so fallback runs and applies /careers/location/ exclusion heuristic (no plugin for larian)
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
            assert len(rows) == 1
            assert rows[0]["jobLink"] == "https://larian.com/careers/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

def test_run_static_studio_pages_source_accepts_cdpr_query_key_override() -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        # Use a host that has no static plugin so the generic flow runs with detailQueryKeys
        base = "https://cdpr-careers-test.example.com/en/jobs"
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Cdprojektred (Manual Website)",
                "studio": "Cdprojektred",
                "adapter": "static",
                "company": "Cdprojektred",
                "pages": [base],
                "detailQueryKeys": ["gh_jid"],
                "enabledByDefault": True,
            }
        ]
        listing = f'<html><body><a href="{base}?gh_jid=1234">Gameplay Engineer</a></body></html>'
        detail = "<html><body><h1>Gameplay Engineer</h1></body></html>"
        try:
            def fake_fetch(url: str, _: int) -> str:
                if url == base:
                    return listing
                if url == f"{base}?gh_jid=1234":
                    return detail
                raise RuntimeError(f"Unexpected URL: {url}")

            rows = jf.run_static_studio_pages_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            assert len(rows) == 1
            assert rows[0]["jobLink"] == f"{base}?gh_jid=1234"
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

def test_run_static_studio_pages_source_accepts_remedy_query_key_override() -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        # Use a host that has no static plugin so the generic flow runs with detailQueryKeys
        base = "https://remedy-careers-test.example.com/careers"
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Remedy Entertainment (Manual Website)",
                "studio": "Remedy Entertainment",
                "adapter": "static",
                "company": "Remedy Entertainment",
                "pages": [base],
                "detailQueryKeys": ["jobid"],
                "enabledByDefault": True,
            }
        ]
        listing = f'<html><body><a href="{base}/open?jobid=42">Rendering Programmer</a></body></html>'
        detail = "<html><body><h1>Rendering Programmer</h1></body></html>"
        try:
            def fake_fetch(url: str, _: int) -> str:
                if url == base:
                    return listing
                if url == f"{base}/open?jobid=42":
                    return detail
                raise RuntimeError(f"Unexpected URL: {url}")

            rows = jf.run_static_studio_pages_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            assert len(rows) == 1
            assert rows[0]["jobLink"] == f"{base}/open?jobid=42"
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

def test_run_static_studio_pages_source_accepts_ubisoft_query_key_override() -> None:
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
            assert len(rows) == 1
            assert rows[0]["jobLink"] == "https://www.ubisoft.com/en-us/company/careers/search?jobid=99"
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

def test_run_static_studio_pages_source_dedupes_candidate_links_before_fetch() -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        # Use example.net so the generic fallback runs (no static plugin)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Dedup Test Studio",
                "studio": "Dedup Test Studio",
                "adapter": "static",
                "company": "Dedup Test Studio",
                "pages": ["https://example.net/careers"],
                "enabledByDefault": True,
            }
        ]
        listing = (
            '<html><body>'
            '<div class="job-listing-item"><a href="/job/engine-programmer">Engine Programmer</a></div>'
            '<a href="/job/engine-programmer">Engine Programmer</a>'
            '<script>var detail = "https://example.net/job/engine-programmer";</script>'
            '</body></html>'
        )
        detail = "<html><body><h1>Engine Programmer</h1></body></html>"
        fetch_counts = {"detail": 0}

        try:
            def fake_fetch(url: str, _: int) -> str:
                if url == "https://example.net/careers":
                    return listing
                if url == "https://example.net/job/engine-programmer":
                    fetch_counts["detail"] += 1
                    return detail
                raise RuntimeError(f"Unexpected URL: {url}")

            rows = jf.run_static_studio_pages_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            assert len(rows) == 1
            assert fetch_counts["detail"] == 1
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

def test_run_static_studio_pages_source_parallelizes_detail_fetches() -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        # Use example.net so the generic fallback runs (no static plugin)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Parallel Static Studio",
                "studio": "Parallel Static Studio",
                "adapter": "static",
                "company": "Parallel Static Studio",
                "pages": ["https://example.net/careers"],
                "enabledByDefault": True,
            }
        ]
        listing = (
            '<html><body>'
            '<a href="/job/a">Role A</a>'
            '<a href="/job/b">Role B</a>'
            '<a href="/job/c">Role C</a>'
            '</body></html>'
        )
        active = 0
        peak = 0
        active_lock = threading.Lock()

        try:
            def fake_fetch(url: str, _: int) -> str:
                nonlocal active, peak
                if url == "https://example.net/careers":
                    return listing
                if url in {
                    "https://example.net/job/a",
                    "https://example.net/job/b",
                    "https://example.net/job/c",
                }:
                    with active_lock:
                        active += 1
                        peak = max(peak, active)
                    time.sleep(0.05)
                    with active_lock:
                        active -= 1
                    title = url.rsplit("/", 1)[-1].upper()
                    return f"<html><body><h1>{title}</h1></body></html>"
                raise RuntimeError(f"Unexpected URL: {url}")

            rows = jf.run_static_studio_pages_source(
                fetch_text=fake_fetch,
                timeout_s=5,
                retries=0,
                backoff_s=0,
                static_detail_concurrency=3,
            )
            assert len(rows) == 3
            assert peak >= 2
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

def test_scrapy_runner_emits_valid_envelope_selftest() -> None:
        runner_path = Path(jf.__file__).resolve().parent / "scrapers" / "runner.py"
        config = {
            "source": {
                "name": "Scrapy Test Studio",
                "studio": "Scrapy Test Studio",
                "pages": ["https://example.com/jobs"],
                "nlPriority": False,
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
        assert "ok" in envelope
        assert isinstance(envelope.get("jobs"), list)
        assert isinstance(envelope.get("details"), list)
        assert isinstance(envelope.get("partialErrors"), list)
        assert isinstance(envelope.get("stats"), dict)
        detail = (envelope.get("details") or [{}])[0]
        assert "classification" in detail
        assert "browserFallbackRecommended" in detail
        assert "sourceId" in detail
        assert "candidate_links_found" in (envelope.get("stats") or {})

def test_scrapy_runner_invalid_schema_emits_error_envelope() -> None:
        runner_path = Path(jf.__file__).resolve().parent / "scrapers" / "runner.py"
        result = subprocess.run(  # noqa: S603
            [sys.executable, str(runner_path)],
            input=json.dumps({"source": {"name": "Only Name"}}),
            text=True,
            capture_output=True,
            check=False,
        )
        envelope = json.loads(result.stdout)
        assert not bool(envelope.get("ok"))
        assert isinstance(envelope.get("partialErrors"), list)
        assert result.returncode != 0

def test_scrapy_runner_jobylon_v1_extracts_jobs() -> None:
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

        from src.scrapers.providers import jobylon_v1

        with mock.patch.object(jobylon_v1, "_http_text", side_effect=[source_html, embed_html]):
            jobs, stats, errors, reject_reasons = jobylon_v1.extract_jobylon_v1_jobs(
                source_name="Remedy",
                studio="Remedy Entertainment",
                page_url="https://www.remedygames.com/careers",
                timeout_s=20,
            )

        assert len(jobs) == 2
        assert stats.get("jobs_emitted") == 2
        assert stats.get("candidate_links_found") == 2
        assert stats.get("detail_pages_visited") == 2
        assert errors == []
        assert int(reject_reasons.get("open_application", 0)) == 1
        assert all(isinstance(row.get("sourceBundle"), list) and row.get("sourceBundle") for row in jobs)
        links = {str(row.get("jobLink") or "") for row in jobs}
        assert "https://emp.jobylon.com/jobs/329202-remedy-entertainment-senior-support-engineer/" in links
        assert "https://emp.jobylon.com/jobs/322343-remedy-entertainment-development-director/" in links

def test_run_scrapy_static_source_handles_malformed_json() -> None:
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
                    assert rows == []
                    diag.assert_called_once()
                    args, kwargs = diag.call_args
                    assert args[0] == "scrapy_static_sources"
                    assert kwargs.get("adapter") == "scrapy_static"
                    details = kwargs.get("details") or []
                    assert details
                    assert str(details[0].get("classification") or "") == "parse_error"
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

def test_scrapy_static_registration_in_default_loaders() -> None:
        assert "scrapy_static_sources" in jfr.DEFAULT_SOURCE_LOADER_NAMES
        assert "google_sheets_1er2oaxo" in jfr.DEFAULT_SOURCE_LOADER_NAMES
        assert "google_sheets_1mvqhxat" in jfr.DEFAULT_SOURCE_LOADER_NAMES
        assert jfr.SOURCE_REPORT_META["scrapy_static_sources"]["adapter"] == "scrapy_static"
        assert jfr.SOURCE_REPORT_META["google_sheets_1er2oaxo"]["adapter"] == "csv"
        assert jfr.SOURCE_REPORT_META["google_sheets_1mvqhxat"]["adapter"] == "csv"
        names = [name for name, _ in jf.default_source_loaders()]
        assert "scrapy_static_sources" in names
        assert "google_sheets_1er2oaxo" in names
        assert "google_sheets_1mvqhxat" in names

def test_run_pipeline_writes_browser_fallback_queue() -> None:
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
            assert queue_path.exists()
            queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
            assert len(queue_rows) == 1
            assert str(queue_rows[0].get("adapter") or "") == "scrapy_static"
            assert str(queue_rows[0].get("classification") or "") == "fetch_ok_extract_zero"
            assert str((report.get("outputs") or {}).get("browserFallbackQueue") or "") == str(queue_path)
            details = ((report.get("sources") or [{}])[0].get("details") or [{}])[0]
            assert str(details.get("classification") or "") == "fetch_ok_extract_zero"
            assert bool(details.get("browserFallbackRecommended"))


def test_browser_fallback_queue_one_canonical_per_source() -> None:
    """When a source has multiple pages (main + sub-pages), queue gets one row with canonical listing URL."""
    main_url = "https://supercell.com/en/careers/"
    sub_urls = [
        "https://supercell.com/en/careers/joining-supercell/",
        "https://supercell.com/en/careers/our-offices/",
    ]

    def scraper_loader(**_: object):
        jf.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[
                {
                    "adapter": "scrapy_static",
                    "studio": "Supercell",
                    "name": "Supercell Careers",
                    "status": "ok",
                    "fetchedCount": 3,
                    "keptCount": 0,
                    "error": "",
                    "classification": "fetch_ok_extract_zero",
                    "browserFallbackRecommended": True,
                    "sourceId": "static:listing_url:https://supercell.com/en/careers/",
                    "pages": [main_url, *sub_urls],
                    "stats": {},
                }
            ],
            partial_errors=[],
        )
        return []

    with workspace_tmpdir("jobs-fetcher-browser-queue-canonical") as tmp:
        out = Path(tmp)
        jf.run_pipeline(
            output_dir=out,
            source_loaders=[("scrapy_static_sources", scraper_loader)],
            show_progress=False,
        )
        queue_path = out / "jobs-browser-fallback-queue.json"
        assert queue_path.exists()
        queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
        assert len(queue_rows) == 1
        assert queue_rows[0].get("page") == main_url
        assert str(queue_rows[0].get("studio") or "") == "Supercell"


def test_browser_fallback_queue_excludes_job_provider_domains() -> None:
    """Sources whose domain has job_provider (e.g. Remedy/Jobylon) are not added to the queue."""
    remedy_url = "https://www.remedygames.com/careers"

    def scraper_loader(**_: object):
        jf.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[
                {
                    "adapter": "scrapy_static",
                    "studio": "Remedy",
                    "name": "Remedy Careers",
                    "status": "ok",
                    "fetchedCount": 1,
                    "keptCount": 0,
                    "error": "",
                    "classification": "fetch_ok_extract_zero",
                    "browserFallbackRecommended": True,
                    "sourceId": "static:remedy",
                    "pages": [remedy_url],
                    "stats": {},
                }
            ],
            partial_errors=[],
        )
        return []

    with workspace_tmpdir("jobs-fetcher-browser-queue-no-job-provider") as tmp:
        out = Path(tmp)
        jf.run_pipeline(
            output_dir=out,
            source_loaders=[("scrapy_static_sources", scraper_loader)],
            show_progress=False,
        )
        queue_path = out / "jobs-browser-fallback-queue.json"
        assert queue_path.exists()
        queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
        remedy_rows = [r for r in queue_rows if "remedygames" in str(r.get("page") or "")]
        assert len(remedy_rows) == 0


def test_scrapy_static_registry_from_browser_queue_collapses_by_source_id() -> None:
    """When the browser queue has multiple rows for the same sourceId, registry has one row per source with best URL."""
    with workspace_tmpdir("jobs-fetcher-registry-collapse") as tmp:
        queue_path = Path(tmp) / "jobs-browser-fallback-queue.json"
        # Same sourceId, two pages (main has shorter path)
        queue_path.write_text(
            json.dumps([
                {
                    "adapter": "scrapy_static",
                    "sourceId": "static:supercell",
                    "name": "Supercell",
                    "studio": "Supercell",
                    "page": "https://supercell.com/en/careers/joining-supercell/",
                },
                {
                    "adapter": "scrapy_static",
                    "sourceId": "static:supercell",
                    "name": "Supercell",
                    "studio": "Supercell",
                    "page": "https://supercell.com/en/careers/",
                },
            ], indent=2),
            encoding="utf-8",
        )
        with mock.patch.object(jobs_common, "SCRAPY_BROWSER_QUEUE_PATH", queue_path):
            rows = jobs_common.registry_entries("scrapy_static", enabled_only=True)
        assert len(rows) == 1
        assert rows[0].get("pages") == ["https://supercell.com/en/careers/"]
        assert rows[0].get("id") == "static:supercell"


def test_map_profession_recognizes_focus_synonyms() -> None:
        assert jf.map_profession("Senior Tech Artist") == "technical-artist"
        assert jf.map_profession("Material Artist") == "technical-artist"
        assert jf.map_profession("World Artist") == "environment-artist"
        assert jf.map_profession("Terrain Artist") == "environment-artist"
        assert jf.map_profession("Technical Director") == "technical-director"
        assert jf.map_profession("Associate Technical Director") == "technical-director"
        assert jf.map_profession("Senior Animation TD") == "technical-director"
        assert jf.map_profession("Pipeline TD") == "technical-director"
        assert jf.map_profession("TDengine Programmer") == "engine"

def test_compute_focus_score_prioritizes_target_nl_and_remote() -> None:
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
        assert ta_nl
        assert ta_remote
        assert non_target
        assert ta_nl["focusScore"] > ta_remote["focusScore"]
        assert ta_remote["focusScore"] > non_target["focusScore"]

def test_dedup_primary_key_prefers_richer_latest_record() -> None:
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
        assert first is not None
        assert second is not None
        rows, stats = jf.deduplicate_jobs([first, second])
        assert stats["outputCount"] == 1
        assert int(stats.get("mergedByPrimaryUrl") or 0) == 1
        assert int(stats.get("mergedBySecondaryKey") or 0) == 0
        assert int(stats.get("mergedBySocialKey") or 0) == 0
        assert rows[0]["sourceJobId"] == "r-2"
        assert rows[0]["dedupKey"].startswith("url:")

def test_canonicalize_job_rejects_linkless_rows_before_dedup() -> None:
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
        assert first is None
        assert second is None


def test_canonicalize_job_with_reason_accounts_drop_reasons() -> None:
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
        assert dropped_title is None
        assert dropped_company is None
        assert dropped_payload is None
        assert reason_title == "missing_title"
        assert reason_company == "missing_company"
        assert reason_payload == "invalid_payload"

def test_canonicalize_job_with_reason_requires_job_link() -> None:
        dropped_link, reason_link = jf.canonicalize_job_with_reason(
            {"title": "Gameplay Engineer", "company": "Studio A", "jobLink": ""},
            source="x",
            fetched_at=jf.now_iso(),
        )
        assert dropped_link is None
        assert reason_link == "missing_job_link"

def test_pipeline_partial_success_when_one_source_fails() -> None:
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

            assert report["summary"]["failedSources"] == 1
            assert report["summary"]["outputCount"] == 1

            output = json.loads((Path(tmp) / "jobs-unified.json").read_text(encoding="utf-8"))
            assert len(output) == 1
            assert output[0]["source"] == "ok"

def test_pipeline_preserves_previous_output_when_current_is_empty() -> None:
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
            assert len(output) == 1
            assert report["summary"]["preservedPreviousOutput"]

def test_pipeline_tracks_likely_removed_jobs_in_lifecycle_state() -> None:
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
                assert int(first["summary"].get("outputCount") or 0) == 1
                assert int(first["summary"].get("lifecycleActiveCount") or 0) == 1

                jf.default_source_loaders = lambda: [("only_source", empty_loader)]
                second = jf.run_pipeline(output_dir=out, preserve_previous_on_empty=False)
                assert int(second["summary"].get("outputCount") or 0) == 0
                assert int(second["summary"].get("lifecycleLikelyRemovedCount") or 0) == 1

                lifecycle_payload = json.loads((out / "jobs-lifecycle-state.json").read_text(encoding="utf-8"))
                jobs_map = lifecycle_payload.get("jobs") or {}
                assert len(jobs_map) == 1
                entry = list(jobs_map.values())[0]
                assert str(entry.get("status") or "") == "likely_removed"
                assert str(entry.get("removedAt") or "")
        finally:
            jf.default_source_loaders = previous_default_loaders

def test_pipeline_marks_missing_for_successful_sources_even_when_other_sources_fail() -> None:
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

        def failing_loader(**_: object):
            raise RuntimeError("timeout")

        previous_default_loaders = jf.default_source_loaders
        try:
            with workspace_tmpdir("jobs-fetcher") as tmp:
                out = Path(tmp)
                jf.default_source_loaders = lambda: [("ok_source", one_job_loader), ("failing_source", failing_loader)]
                first = jf.run_pipeline(output_dir=out, preserve_previous_on_empty=False)
                assert int(first["summary"].get("outputCount") or 0) == 1
                assert int(first["summary"].get("failedSources") or 0) == 1

                jf.default_source_loaders = lambda: [("ok_source", empty_loader), ("failing_source", failing_loader)]
                second = jf.run_pipeline(output_dir=out, preserve_previous_on_empty=False)
                assert int(second["summary"].get("failedSources") or 0) == 1
                assert int(second["summary"].get("lifecycleLikelyRemovedCount") or 0) == 1

                lifecycle_payload = json.loads((out / "jobs-lifecycle-state.json").read_text(encoding="utf-8"))
                jobs_map = lifecycle_payload.get("jobs") or {}
                assert len(jobs_map) == 1
                entry = list(jobs_map.values())[0]
                assert str(entry.get("status") or "") == "likely_removed"
                assert str(entry.get("removedAt") or "")
        finally:
            jf.default_source_loaders = previous_default_loaders

def test_pipeline_output_contract_matches_frontend() -> None:
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
            assert len(rows) == 1
            row = rows[0]
            for field in jf.REQUIRED_FIELDS:
                assert field in row
            for field in jf.OPTIONAL_FIELDS:
                assert field in row
            assert row["workType"] == "Remote"
            assert isinstance(row["focusScore"], int)

def test_pipeline_default_sources_exclude_wellfound_and_include_guerrilla() -> None:
        google_csv = _fixture("google_sheets.csv")
        remote_json = _fixture("remoteok.json")
        gamesindustry_html = _fixture("gamesindustry_jobs.html")
        greenhouse_json = _fixture("greenhouse_guerrilla_jobs.json")
        greenhouse_playstation_json = _fixture("greenhouse_playstation_jobs.json")
        teamtailor_listing = _fixture("teamtailor_listing.html")
        teamtailor_job = _fixture("teamtailor_job.html")
        littlechicken_listing = _fixture("littlechicken_jobs_page.html")
        littlechicken_detail = _fixture("littlechicken_job_detail.html")
        lever_json = _fixture("lever_jobs.json")
        smart_json = _fixture("smartrecruiters_jobs.json")
        workable_json = _fixture("workable_jobs.json")
        ashby_html = _fixture("ashby_jobs.html")
        personio_xml = _fixture("personio_feed.xml")

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
            assert sources["google_sheets"]["status"] == "ok"
            assert sources["google_sheets_1er2oaxo"]["status"] == "ok"
            assert sources["google_sheets_1mvqhxat"]["status"] == "ok"
            assert sources["remote_ok"]["status"] == "ok"
            assert sources["gamesindustry"]["status"] == "ok"
            assert sources["greenhouse_boards"]["status"] == "ok"
            assert sources["teamtailor_sources"]["status"] == "ok"
            assert sources["lever_sources"]["status"] == "ok"
            assert sources["smartrecruiters_sources"]["status"] == "ok"
            assert sources["workable_sources"]["status"] == "ok"
            assert sources["ashby_sources"]["status"] == "ok"
            assert sources["personio_sources"]["status"] == "ok"
            static_rows = [row for row in report["sources"] if str(row.get("adapter") or "").lower() == "static"]
            assert static_rows
            assert any(str(row.get("status") or "").lower() == "ok" for row in static_rows)
            assert sources["wellfound"]["status"] == "excluded"
            assert "disabled_by_default" in sources["wellfound"]["error"]
            assert sources["greenhouse_boards"]["adapter"] == "greenhouse"
            assert sources["teamtailor_sources"]["adapter"] == "teamtailor"
            assert sources["lever_sources"]["adapter"] == "lever"
            assert sources["smartrecruiters_sources"]["adapter"] == "smartrecruiters"
            assert sources["workable_sources"]["adapter"] == "workable"
            assert sources["ashby_sources"]["adapter"] == "ashby"
            assert sources["personio_sources"]["adapter"] == "personio"
            assert "failedSources" in report["summary"]
            assert report["summary"]["excludedSources"] == 1
            assert "targetRoleCount" in report["summary"]
            assert "netherlandsCount" in report["summary"]
            assert "remoteCount" in report["summary"]
            assert "rawFetchedCount" in report["summary"]
            assert "uniqueOutputCount" in report["summary"]
            assert "sourceBundleCollisions" in report["summary"]

            rows = json.loads((Path(tmp) / "jobs-unified.json").read_text(encoding="utf-8"))
            assert any("guerrilla" in row.get("company", "").lower() for row in rows)
            assert any("playstation" in row.get("company", "").lower() for row in rows)
            assert any("paradox" in row.get("company", "").lower() for row in rows)
            assert any("little chicken" in row.get("company", "").lower() for row in rows)
            assert all("focusScore" in row for row in rows)
            assert all("sourceBundleCount" in row for row in rows)
            assert all("sourceBundle" in row for row in rows)
            all_errors = " ".join(row.get("error", "") for row in report["sources"])
            assert "403" not in all_errors

def test_run_pipeline_writes_normalized_report_task_and_source_state_contracts() -> None:
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
            assert str(report.get("schemaVersion") or "") == str(jf.SCHEMA_VERSION)
            runtime = report.get("runtime") or {}
            assert int(runtime.get("maxWorkers") or 0) == 2
            assert int(runtime.get("maxPerDomain") or 0) == 2
            assert str(runtime.get("fetchStrategy") or "") == "auto"
            assert str(runtime.get("fetchClient") or "") in {"urllib", "httpx_async"}
            assert int(runtime.get("adapterHttpConcurrency") or 0) == jf.DEFAULT_ADAPTER_HTTP_CONCURRENCY
            assert int(runtime.get("staticDetailConcurrency") or 0) == jf.DEFAULT_STATIC_DETAIL_CONCURRENCY
            assert int(runtime.get("googleSheetsRedirectConcurrency") or 0) == jf.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
            assert int(runtime.get("selectedSourceCount") or 0) == 1
            assert isinstance(runtime.get("slowestSources"), list)
            assert "summary" in report
            assert "sources" in report
            assert str(report["sources"][0].get("fetchStrategy") or "") == "auto"
            assert "loss" in report["sources"][0]
            assert "canonicalDropReasons" in (report["sources"][0].get("loss") or {})

            task_payload = json.loads((out / "jobs-fetch-tasks.json").read_text(encoding="utf-8"))
            assert str(task_payload.get("schemaVersion") or "") == str(jf.SCHEMA_VERSION)
            assert "summary" in task_payload
            assert "tasks" in task_payload
            assert "outputs" in task_payload
            assert str((task_payload.get("outputs") or {}).get("report") or "") == str(out / "jobs-fetch-report.json")

            state_payload = json.loads((out / "jobs-source-state.json").read_text(encoding="utf-8"))
            assert str(state_payload.get("schemaVersion") or "") == str(jf.SCHEMA_VERSION)
            sources_state = state_payload.get("sources") or {}
            assert "ok_source" in sources_state
            assert int((sources_state["ok_source"]).get("consecutiveFailures") or 0) == 0

def test_run_pipeline_tracks_google_sheets_redirect_stats_in_report_and_state() -> None:
        csv_text = (
            "Company,City,Country,Fully Remote?,Job Type,Job,Link\n"
            f"{jf.UNKNOWN_COMPANY_LABEL},Montpellier,France,No,Full-time,Technical Director,https://gracklehq.com/rd/372393\n"
            f"{jf.UNKNOWN_COMPANY_LABEL},Burbank,United States,Yes,Internship,Character TD,https://example.com/jobs/character-td\n"
        )

        def google_loader(**kwargs):
            return jf.run_google_sheets_source(
                **kwargs,
                sheet_id="test-sheet",
                gid="0",
                diagnostics_name="google_sheets",
            )

        class _FakeResolver:
            def __init__(self) -> None:
                self.cache_hits = 0
                self.resolved_count = 0

            def resolve(self, url: str) -> str:
                self.resolved_count += 1
                return "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role"

            def snapshot_stats(self) -> dict:
                return {"cacheHits": self.cache_hits, "resolvedCount": self.resolved_count}

            def close(self) -> None:
                """Match real resolver's close(self) signature."""
                return None

        with workspace_tmpdir("jobs-fetcher-google") as tmp:
            out = Path(tmp)
            with mock.patch.object(jf, "build_redirect_resolver", return_value=_FakeResolver()):
                def fake_fetch(url: str, _: int) -> str:
                    if "docs.google.com" in url or "allorigins.win" in url:
                        return csv_text
                    raise RuntimeError(f"Unexpected URL: {url}")

                report = jf.run_pipeline(
                    output_dir=out,
                    fetch_text=fake_fetch,
                    source_loaders=[("google_sheets", google_loader)],
                    google_sheets_redirect_concurrency=3,
                )

            runtime = report.get("runtime") or {}
            assert int(runtime.get("googleSheetsRedirectConcurrency") or 0) == 3
            source_row = report["sources"][0]
            assert source_row.get("adapter") == "csv"
            detail_stats = ((source_row.get("details") or [{}])[0].get("stats") or {})
            assert int(detail_stats.get("redirect_candidates") or 0) == 1
            assert int(detail_stats.get("redirect_resolved") or 0) == 1
            assert "redirect_resolve_ms" in detail_stats

            state_payload = json.loads((out / "jobs-source-state.json").read_text(encoding="utf-8"))
            source_state = (state_payload.get("sources") or {}).get("google_sheets") or {}
            assert int(source_state.get("lastRedirectCandidates") or 0) == 1
            assert int(source_state.get("lastRedirectResolved") or 0) == 1

def test_run_pipeline_includes_selection_exclusions() -> None:
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
            assert len(excluded_rows) == 1
            assert str(excluded_rows[0].get("status") or "") == "excluded"
            assert str(excluded_rows[0].get("exclusionReason") or "") == "only_sources_filter"

def test_should_skip_source_by_ttl_honors_recent_success_and_failure_state() -> None:
        now = jf.now_iso()
        rows = {"source_a": {"lastSuccessAt": now, "consecutiveFailures": 0}}
        assert jf.should_skip_source_by_ttl("source_a", rows, ttl_minutes=360)

        rows["source_a"]["consecutiveFailures"] = 2
        assert not jf.should_skip_source_by_ttl("source_a", rows, ttl_minutes=360)

def test_should_skip_source_by_cadence_uses_hot_and_cold_windows() -> None:
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
        assert jf.should_skip_source_by_cadence("hot_source", rows, hot_minutes=15, cold_minutes=60)
        assert jf.should_skip_source_by_cadence("cold_source", rows, hot_minutes=15, cold_minutes=60)

        rows["hot_source"]["lastSuccessAt"] = (now - jf.timedelta(minutes=20)).isoformat()
        rows["cold_source"]["lastSuccessAt"] = (now - jf.timedelta(minutes=70)).isoformat()
        assert not jf.should_skip_source_by_cadence("hot_source", rows, hot_minutes=15, cold_minutes=60)
        assert not jf.should_skip_source_by_cadence("cold_source", rows, hot_minutes=15, cold_minutes=60)

def test_run_pipeline_excludes_quarantined_source_unless_ignored() -> None:
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
            assert calls["count"] == 0
            assert len(blocked_rows) == 1
            assert str(blocked_rows[0].get("status") or "") == "excluded"
            assert "circuit_breaker_active_until" in str(blocked_rows[0].get("error") or "")

            unblocked_report = jf.run_pipeline(
                output_dir=out,
                source_loaders=[("blocked_source", ok_loader)],
                circuit_breaker_failures=3,
                circuit_breaker_cooldown_minutes=180,
                ignore_circuit_breaker=True,
            )
            unblocked_rows = [row for row in unblocked_report.get("sources", []) if row.get("name") == "blocked_source"]
            assert calls["count"] == 1
            assert len(unblocked_rows) == 1
            assert str(unblocked_rows[0].get("status") or "") == "ok"

def test_pipeline_report_snapshot_contract() -> None:
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
            assert snapshot == _fixture_json("jobs_fetch_report_snapshot.json")


