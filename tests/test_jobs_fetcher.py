import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from unittest import mock

import pytest

from src import jobs_fetcher as jf
from src import jobs_fetcher_registry as jfr
from src.exceptions import AdapterValidationError
from src.jobs.contamination_audit import build_contamination_report, build_location_quality_report
from src.jobs import common as jobs_common
from src.scrapers import runner as scrapy_runner
from tests.helpers.temp_paths import workspace_tmpdir
from src.jobs.adapters import _runtime as runtime_resolver
from src.jobs.adapters.plugins.provider_api import ensure_registered as ensure_provider_plugins
from src.jobs.adapters.static_helpers import source_detail_limit_for
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
        assert callable(getattr(resolved, "parse_breezy_jobs_html", None))
        assert callable(getattr(resolved, "parse_jazzhr_jobs_html", None))
        assert callable(getattr(resolved, "parse_recruitee_jobs_payload", None))
        assert callable(getattr(resolved, "parse_pinpoint_jobs_payload", None))
        assert callable(getattr(resolved, "parse_8bitplay_html", None))
        assert callable(getattr(resolved, "parse_gracklehq_html", None))
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


def test_provider_plugin_registry_selects_ashby_sources_plugin() -> None:
    ensure_provider_plugins()
    ctx = AdapterPluginContext(family="provider_api", adapter_key="ashby_sources")
    plugin, selection = default_registry.select(ctx)
    assert plugin.name == "ashby_sources"
    assert selection.plugin_name == "ashby_sources"


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


def test_parse_args_uses_updated_pipeline_concurrency_defaults() -> None:
        prev_argv = list(sys.argv)
        try:
            sys.argv = ["jobs_fetcher.py"]
            args = jf.parse_args()
        finally:
            sys.argv = prev_argv
        assert int(args.max_workers or 0) == 12
        assert int(args.max_per_domain or 0) == 3
        assert int(args.timeout or 0) == 15
        assert float(args.backoff or 0) == 1.2
        assert int(args.adapter_http_concurrency or 0) == 48
        assert int(args.static_detail_concurrency or 0) == 10


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


def test_source_detail_limit_for_caps_chronic_low_yield_sources() -> None:
    limit = source_detail_limit_for(
        "Climax (Manual Website)",
        source_state_rows={
            "Climax (Manual Website)": {
                "lastDetailPagesVisited": 42,
                "lastKeptCount": 1,
                "lastDurationMs": 52000,
                "lastDetailYieldPct": 2,
            }
        },
        discovered_links=28,
        listing_jobs_found=0,
        low_yield_detail_cap=12,
        very_low_yield_detail_cap=6,
    )
    assert limit == 12


def test_source_detail_limit_for_uses_tighter_cap_when_listing_jobs_already_found() -> None:
    limit = source_detail_limit_for(
        "Nintendo (Manual Website)",
        source_state_rows={
            "Nintendo (Manual Website)": {
                "lastDetailPagesVisited": 18,
                "lastKeptCount": 2,
                "lastDurationMs": 26000,
                "lastDetailYieldPct": 4,
            }
        },
        discovered_links=20,
        listing_jobs_found=5,
        low_yield_detail_cap=12,
        very_low_yield_detail_cap=6,
    )
    assert limit == 6


def test_scrapy_runner_emit_envelope_tolerates_non_json_safe_values(capsys: pytest.CaptureFixture[str]) -> None:
    scrapy_runner._emit_envelope(
        {
            "ok": True,
            "jobs": [],
            "details": [
                {
                    "name": "GAME FREAK inc. (Manual Website)",
                    "studio": "GAME FREAK",
                    "status": "ok",
                    "meta": {"bad": object()},
                }
            ],
        }
    )
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["ok"] is True
    assert payload["details"][0]["name"] == "GAME FREAK inc. (Manual Website)"


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

def test_canonicalize_job_skips_redirect_resolution_for_gracklehq_source() -> None:
        calls: list[str] = []

        row = jf.canonicalize_job(
            {
                "title": "Technical Director Level Design - M/F/NB - unannounced project",
                "company": jf.UNKNOWN_COMPANY_LABEL,
                "city": "Montpellier",
                "country": "France",
                "workType": "Onsite",
                "contractType": "Unknown",
                "jobLink": "https://gracklehq.com/rd/372393",
                "sector": "Game",
                "sourceJobId": "gracklehq:https://gracklehq.com/rd/372393",
            },
            source="gracklehq",
            fetched_at=jf.now_iso(),
            resolve_redirect_url=lambda url: calls.append(str(url)) or "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role",
        )

        assert row is not None
        assert row["jobLink"] == "https://gracklehq.com/rd/372393"
        assert calls == []

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
        rows = jf.parse_ashby_jobs_from_html(html, "https://jobs.ashbyhq.com/thatgamecompany/jobs", "thatgamecompany")
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
        rows = jf.parse_ashby_jobs_from_html(html, "https://jobs.ashbyhq.com/thatgamecompany", "thatgamecompany")
        assert len(rows) == 2
        assert any(str(row.get("jobLink") or "").endswith("/thatgamecompany/7ea5dd25-3fcb-4d42-8217-89dd9b6f5083") for row in rows)

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
        rows = jf.parse_ashby_jobs_from_html(html, "https://jobs.ashbyhq.com/thatgamecompany/jobs", "thatgamecompany")
        assert len(rows) == 1
        assert rows[0]["jobLink"] == "https://jobs.ashbyhq.com/thatgamecompany/7ea5dd25-3fcb-4d42-8217-89dd9b6f5083"
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
        rows = jf.parse_breezy_jobs_html(_fixture("breezy_jobs.html"), "https://yallaplay.breezy.hr/", "YallaPlay")
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
        from src.jobs.adapters.plugins.provider_api import register as provider_register
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

            def fetch_with_retries(self, url: str, fetch_text, timeout_s: int, retries: int, backoff_s: float) -> str:
                return fetch_text(url, timeout_s)

            def set_source_diagnostics(self, source_name: str, **kwargs) -> None:
                return None

        with mock.patch.object(provider_register.runtime_deps, "facade", return_value=_Deps()):
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

            rows = jf.run_ashby_sources_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            assert len(rows) == 1
            assert str(rows[0].get("title") or "") == "Senior 3D Environment Artist"

def test_run_ashby_sources_source_normalizes_stale_jobs_url_to_board_root() -> None:
        source_rows = [
            {
                "name": "thatgamecompany (Ashby)",
                "studio": "thatgamecompany",
                "adapter": "ashby",
                "board_url": "https://jobs.ashbyhq.com/thatgamecompany/jobs",
                "enabledByDefault": True,
            }
        ]
        with mock.patch("src.jobs.adapters.provider_api.registry_entries", return_value=source_rows):
            def fake_fetch(url: str, _: int) -> str:
                if url == "https://jobs.ashbyhq.com/thatgamecompany/jobs":
                    return "<html><body><h1>Job not found</h1></body></html>"
                if url == "https://jobs.ashbyhq.com/thatgamecompany":
                    return """
                    <a href="/thatgamecompany/7ea5dd25-3fcb-4d42-8217-89dd9b6f5083">
                      Senior 3D Environment Artist
                    </a>
                    """
                raise AssertionError(f"unexpected url {url}")

            rows = jf.run_ashby_sources_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            assert len(rows) == 1
            assert str(rows[0].get("jobLink") or "").endswith("/thatgamecompany/7ea5dd25-3fcb-4d42-8217-89dd9b6f5083")

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
                fetch_text=lambda _url, _timeout: "<html><body><h1>HR und Lohnbuchhaltung endlich vereint</h1></body></html>",
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
                    fetch_text=lambda _url, _timeout: (_ for _ in ()).throw(RuntimeError("HTTP 429 for https://innogames.jobs.personio.de/xml")),
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
        assert any(row["country"] == "Canada" for row in rows)

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
        page_one = _fixture("gracklehq_jobs.html") + '<a href="./jobs?pageidx=2" class="btn btn-default ">Next</a>'
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
        page_one = _fixture("gracklehq_jobs.html") + '<a href="./jobs?pageidx=2" class="btn btn-default ">Next</a>'
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

def test_run_static_studio_pages_source_littlechicken_plugin_extracts_listing_cards() -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Little Chicken (Manual Website)",
                "studio": "Little Chicken",
                "adapter": "static",
                "company": "Little Chicken",
                "pages": ["https://www.littlechicken.nl/jobs/"],
                "enabledByDefault": True,
                "id": "static:listing_url:https://www.littlechicken.nl/jobs/",
            }
        ]
        listing_html = """
        <article><h2>3D Artist Internship</h2><a href="/job/3d-artist-internship/">Read more</a></article>
        <article><h2>2D Artist Internship</h2><a href="/job/2d-artist-internship/">Read more</a></article>
        <article><h2>QA Tester Internship</h2><a href="/job/qa-tester-internship/">Read more</a></article>
        """
        detail_html = """
        <script type="application/ld+json">
        {"@context":"https://schema.org","@type":"JobPosting","title":"3D Artist Internship","url":"https://www.littlechicken.nl/job/3d-artist-internship/","hiringOrganization":{"@type":"Organization","name":"Little Chicken"}}
        </script>
        """

        def fake_fetch(url: str, _: int) -> str:
            if url == "https://www.littlechicken.nl/jobs/":
                return listing_html
            if "littlechicken.nl/job/" in url:
                return detail_html.replace("3d-artist-internship", url.rstrip("/").split("/")[-1]).replace("3D Artist Internship", url.rstrip("/").split("/")[-1].replace("-", " ").title())
            raise RuntimeError(f"Unexpected URL: {url}")

        try:
            rows = jf.run_static_studio_pages_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            titles = {str(row.get("title") or "") for row in rows}
            assert "3D Artist Internship" in titles
            assert "2D Artist Internship" in titles
            assert "Qa Tester Internship" in titles
            assert len(rows) == 3
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

def test_run_static_studio_pages_source_milestone_plugin_extracts_intervieweb_iframe() -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Milestone (Manual Website)",
                "studio": "Milestone",
                "adapter": "static",
                "company": "Milestone",
                "pages": ["https://milestone.it/careers"],
                "enabledByDefault": True,
                "id": "static:listing_url:https://milestone.it/careers",
            }
        ]
        listing_html = """
        <script src="https://cezanneondemand.intervieweb.it/integration/announces_js.php?lang=en&utype=0&k=abc123&LAC=milestone&d=milestone.it&annType=published&view=list&defgroup=name&gnavenable=1&desc=1&typeView=large"></script>
        """
        iframe_html = """
        <a href="https://cezanneondemand.intervieweb.it/app.php?opmode=guest&module=iframeAnnunci&act1=1&IdAnnuncio=60982&lang=en">Game Designer_tech</a>
        <div>Milano, Italia Design</div>
        <a href="https://cezanneondemand.intervieweb.it/app.php?opmode=guest&module=iframeAnnunci&act1=1&IdAnnuncio=61104&lang=en">JUNIOR IT SERVICE DESK</a>
        <div>Milano, Italia ICT and Information Systems</div>
        """

        def fake_fetch(url: str, _: int) -> str:
            if url == "https://milestone.it/careers":
                return listing_html
            if "module=iframeAnnunci" in url and "act1=23" in url:
                return iframe_html
            raise RuntimeError(f"Unexpected URL: {url}")

        try:
            rows = jf.run_static_studio_pages_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            titles = {str(row.get("title") or "") for row in rows}
            assert "Game Designer_tech" in titles
            assert "JUNIOR IT SERVICE DESK" in titles
            assert len(rows) == 2
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

def test_run_static_studio_pages_source_activision_plugin_extracts_job_links() -> None:
        source_rows = [
            {
                "name": "Activision (Manual Website)",
                "studio": "Activision",
                "adapter": "static",
                "company": "Activision",
                "pages": ["https://careers.activision.com"],
                "enabledByDefault": True,
                "id": "static:listing_url:https://careers.activision.com",
            }
        ]
        listing_html = """
        <a href="https://careers.activision.com/search-results">Search Jobs</a>
        <a href="https://careers.activision.com/job/R025845/Programmeur-senior-Productivite">Programmeur senior, Productivite</a>
        <a href="https://careers.activision.com/apply?jobSeqNo=ACPUUSR025845EXTERNAL">Apply Now</a>
        """
        rows = jf.run_static_studio_pages_source(
            fetch_text=lambda _url, _timeout: listing_html,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=source_rows,
        )
        assert len(rows) == 1
        assert str(rows[0].get("jobLink") or "").endswith("/job/R025845/Programmeur-senior-Productivite")

def test_run_static_studio_pages_source_nacon_plugin_extracts_listing_cards() -> None:
        from src.jobs.adapters.plugins.static.register import register_static_plugins

        register_static_plugins()
        source_rows = [
            {
                "name": "Nacon Studio Milan (Manual Website)",
                "studio": "Nacon Studio Milan",
                "adapter": "static",
                "company": "Nacon Studio Milan",
                "pages": ["https://www.naconstudiomilan.com/careers/"],
                "enabledByDefault": True,
                "id": "static:listing_url:https://www.naconstudiomilan.com/careers",
            }
        ]
        listing_html = """
        <article>
          <h4>Gameplay Designer</h4>
          <p>We are looking for a Gameplay Designer.</p>
          <a href="/careers/gameplay-designer/">Learn more</a>
        </article>
        <article>
          <h4>AI Programmer</h4>
          <p>We are looking for an experienced AI Programmer.</p>
          <a href="/careers/ai-programmer/">Learn more</a>
        </article>
        """
        rows = jf.run_static_studio_pages_source(
            fetch_text=lambda _url, _timeout: listing_html,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=source_rows,
        )
        titles = {str(row.get("title") or "") for row in rows}
        assert titles == {"Gameplay Designer", "AI Programmer"}

def test_run_static_studio_pages_source_kojima_plugin_uses_browser_listing() -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Kojimaproductions (Manual Website)",
                "studio": "Kojimaproductions",
                "adapter": "static",
                "company": "Kojimaproductions",
                "pages": ["https://www.kojimaproductions.jp/en/careers"],
                "enabledByDefault": True,
                "id": "static:listing_url:https://www.kojimaproductions.jp/en/careers",
            }
        ]
        listing_html = "<html><body><p>Open Positions</p></body></html>"
        browser_html = """
        <a href="/en/game-programmer">Game Programmer<br>Programming<br>Tokyo, Japan</a>
        <a href="/en/technical-artist">Technical Artist<br>Programming<br>Tokyo, Japan</a>
        """

        def fake_fetch(url: str, _: int) -> str:
            if url == "https://www.kojimaproductions.jp/en/careers":
                return listing_html
            raise RuntimeError(f"Unexpected URL: {url}")

        try:
            rows = jf.run_static_studio_pages_source(
                fetch_text=fake_fetch,
                timeout_s=5,
                retries=0,
                backoff_s=0,
                try_playwright=lambda _url, _timeout: (browser_html, ""),
            )
            titles = {str(row.get("title") or "") for row in rows}
            assert "Game Programmer" in titles
            assert "Technical Artist" in titles
            assert len(rows) == 2
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

def test_run_static_studio_pages_source_blizzard_plugin_follows_role_pages_to_search_results() -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Blizzard Entertainment (Sheet)",
                "studio": "Blizzard Entertainment",
                "adapter": "static",
                "company": "Blizzard Entertainment",
                "pages": ["https://careers.blizzard.com/global/en"],
                "enabledByDefault": True,
                "id": "static:listing_url:https://careers.blizzard.com/global/en",
            }
        ]
        home_html = '<a href="/global/en/engineering-technology">ENGINEERING & TECHNOLOGY</a>'
        role_html = '<a href="https://careers.blizzard.com/global/en/search-results?rk=l-engineering-technology&sortBy=Most%20relevant">View Open Jobs</a>'
        results_html = """
        <a href="https://careers.blizzard.com/global/en/job/R026699/Software-Engineer-Server-World-of-Warcraft-Irvine-CA">Software Engineer, Server - World of Warcraft | Irvine, CA</a>
        <div>Location Irvine, California, United States of America Posted Date January 30 2026 Category Engineering Job Id R026699</div>
        <a href="https://careers.blizzard.com/global/en/job/R026419/Lead-Systems-Engineer-Unreal-Engine-5">Lead Systems Engineer, Unreal Engine 5</a>
        <div>Location Irvine, California, United States of America Posted Date February 03 2026 Category Engineering Job Id R026419</div>
        """

        def fake_fetch(url: str, _: int) -> str:
            if url == "https://careers.blizzard.com/global/en":
                return home_html
            if url == "https://careers.blizzard.com/global/en/engineering-technology":
                return role_html
            if "search-results?rk=l-engineering-technology" in url:
                return results_html
            raise RuntimeError(f"Unexpected URL: {url}")

        try:
            rows = jf.run_static_studio_pages_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            titles = {str(row.get("title") or "") for row in rows}
            assert "Software Engineer, Server - World of Warcraft | Irvine, CA" in titles
            assert "Lead Systems Engineer, Unreal Engine 5" in titles
            assert len(rows) == 2
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

def test_default_registry_no_longer_seeds_stale_ashby_personio_or_placeholder_greenhouse_rows() -> None:
        names = {str(row.get("name") or "") for row in jf.STUDIO_SOURCE_REGISTRY}
        assert "Example Studio GmbH (Greenhouse)" in names

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
                        "classification": "blocked_or_challenge",
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
            assert str(queue_rows[0].get("classification") or "") == "blocked_or_challenge"
            assert str((report.get("outputs") or {}).get("browserFallbackQueue") or "") == str(queue_path)
            details = ((report.get("sources") or [{}])[0].get("details") or [{}])[0]
            assert str(details.get("classification") or "") == "blocked_or_challenge"
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
                        "classification": "blocked_or_challenge",
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

def test_browser_fallback_queue_skips_fetch_ok_extract_zero_sources() -> None:
    def scraper_loader(**_: object):
        jf.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[
                {
                    "adapter": "scrapy_static",
                    "studio": "Nacon Studio Milan",
                    "name": "Nacon Studio Milan",
                    "status": "ok",
                    "fetchedCount": 1,
                    "keptCount": 0,
                    "error": "no jobs extracted from source pages",
                    "classification": "fetch_ok_extract_zero",
                    "browserFallbackRecommended": True,
                    "sourceId": "static:nacon",
                    "pages": ["https://www.naconstudiomilan.com/careers/"],
                    "stats": {},
                }
            ],
            partial_errors=[],
        )
        return []

    with workspace_tmpdir("jobs-fetcher-browser-queue-skip-parse-zero") as tmp:
        out = Path(tmp)
        jf.run_pipeline(
            output_dir=out,
            source_loaders=[("scrapy_static_sources", scraper_loader)],
            show_progress=False,
        )
        queue_path = out / "jobs-browser-fallback-queue.json"
        queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
        assert queue_rows == []

def test_run_scrapy_static_source_timeout_is_not_requeued() -> None:
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Tequilaworks (Manual Website)",
                "studio": "Tequilaworks",
                "adapter": "scrapy_static",
                "pages": ["https://tequilaworks.com/en/careers"],
                "enabledByDefault": True,
            }
        ]
        try:
            with mock.patch("subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="runner", timeout=20)):
                jf.SOURCE_DIAGNOSTICS.clear()
                rows = jf.run_scrapy_static_source(
                    fetch_text=lambda _url, _timeout: "",
                    timeout_s=5,
                    retries=0,
                    backoff_s=0,
                )
                assert rows == []
                detail = ((jf.SOURCE_DIAGNOSTICS.get("scrapy_static_sources") or {}).get("details") or [{}])[0]
                assert str(detail.get("classification") or "") == "browser_timeout"
                assert not bool(detail.get("browserFallbackRecommended"))
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

def test_public_text_sanitizer_cleans_html_contaminated_fields() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": '<div class="title">Technical Artist</div>',
            "company": "Kojimaproductions",
            "city": '<div class="location">Tokyo',
            "country": "Japan</div>",
            "contractType": '<span>Full-time</span>',
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
            {"title": "Clean", "company": "Studio", "city": "Paris", "country": "France", "jobLink": "https://example.com/1"},
            {"title": '<div class="title">Artist</div>', "company": "Studio", "city": '<div class="location">Tokyo', "country": "Japan</div>", "source": "static", "jobLink": "https://example.com/2"},
        ]
    )
    assert int(report["contaminatedRows"]) == 1
    assert int(report["fieldCounts"]["title"]) == 1
    assert int(report["fieldCounts"]["city"]) == 1
    assert int(report["fieldCounts"]["country"]) == 1
    assert str(report["examples"][0]["fields"]["city"]) == '<div class="location">Tokyo'


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
    assert payload["country"] == "Unknown"


def test_location_quality_audit_reports_semantic_location_examples() -> None:
    report = build_location_quality_report(
        [
            {"title": "Clean", "company": "Studio", "city": "Paris", "country": "France", "jobLink": "https://example.com/1"},
            {
                "title": "Growth Marketing Intern",
                "company": "Sleeper",
                "city": "Remote, United States; San Francisco Area, United States Remote; New York City; Los Angeles",
                "country": "Unknown",
                "source": "ashby_sources",
                "jobLink": "https://example.com/2",
            },
        ]
    )
    assert int(report["invalidLocationFieldCount"]) == 1
    assert int(report["fieldCounts"]["city"]) == 1
    assert str(report["examples"][0]["fields"]["city"]["reason"]) == "invalid_city_semantic_multi_location_blob"


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
            assert int(report["summary"].get("outputCount") or 0) == 1

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
                first = jf.run_pipeline(output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True)
                assert int(first["summary"].get("outputCount") or 0) == 1
                assert int(first["summary"].get("lifecycleActiveCount") or 0) == 1

                jf.default_source_loaders = lambda: [("only_source", empty_loader)]
                second = jf.run_pipeline(output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True)
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
                first = jf.run_pipeline(output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True)
                assert int(first["summary"].get("outputCount") or 0) == 1
                assert int(first["summary"].get("failedSources") or 0) == 1

                jf.default_source_loaders = lambda: [("ok_source", empty_loader), ("failing_source", failing_loader)]
                second = jf.run_pipeline(output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True)
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
        gamejobs_html = _fixture("gamejobs.html")
        workwithindies_html = _fixture("workwithindies.html")
        eightbitplay_html = _fixture("8bitplay_jobs.html")
        gracklehq_html = _fixture("gracklehq_jobs.html")
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
        recruitee_json = _fixture("recruitee_jobs.json")
        pinpoint_json = _fixture("pinpoint_jobs.json")
        breezy_html = _fixture("breezy_jobs.html")
        jazzhr_html = _fixture("jazzhr_jobs.html")
        personio_xml = _fixture("personio_feed.xml")

        def fake_fetch(url: str, _: int) -> str:
            if "docs.google.com/spreadsheets" in url or "api.allorigins.win/raw" in url:
                return google_csv
            if "remoteok.com/api" in url:
                return remote_json
            if "jobs.gamesindustry.biz" in url:
                return gamesindustry_html
            if url == "https://gamejobs.co/":
                return gamejobs_html
            if url == "https://www.workwithindies.com/":
                return workwithindies_html
            if url == "https://8bitplay.com/jobs/":
                return eightbitplay_html
            if url == "https://gracklehq.com/jobs":
                return gracklehq_html
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
            if "jobs.crazygames.com/api/offers" in url:
                return recruitee_json
            if "gameplaygalaxy.pinpointhq.com/postings.json" in url:
                return pinpoint_json
            if "breezy.hr" in url:
                return breezy_html
            if "applytojob.com/apply" in url:
                return jazzhr_html
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
            assert sources["gamejobs"]["status"] == "ok"
            assert sources["workwithindies"]["status"] == "ok"
            assert sources["8bitplay"]["status"] == "ok"
            assert sources["gracklehq"]["status"] == "ok"
            assert sources["greenhouse_boards"]["status"] == "ok"
            assert sources["teamtailor_sources"]["status"] == "ok"
            assert sources["lever_sources"]["status"] == "ok"
            assert sources["smartrecruiters_sources"]["status"] == "ok"
            assert sources["workable_sources"]["status"] == "ok"
            assert sources["recruitee_sources"]["status"] == "ok"
            assert sources["pinpoint_sources"]["status"] == "ok"
            assert sources["ashby_sources"]["status"] == "ok"
            assert sources["breezy_sources"]["status"] == "ok"
            assert sources["jazzhr_sources"]["status"] == "ok"
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
            assert sources["recruitee_sources"]["adapter"] == "recruitee"
            assert sources["pinpoint_sources"]["adapter"] == "pinpoint"
            assert sources["8bitplay"]["adapter"] == "html"
            assert sources["gracklehq"]["adapter"] == "html"
            assert sources["ashby_sources"]["adapter"] == "ashby"
            assert sources["breezy_sources"]["adapter"] == "breezy"
            assert sources["jazzhr_sources"]["adapter"] == "jazzhr"
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
            assert any("pixel forge" in row.get("company", "").lower() for row in rows)
            assert any("moonshot games" in row.get("company", "").lower() for row in rows)
            assert any("pixel dominion" in row.get("company", "").lower() for row in rows)
            assert any("ubisoft" in row.get("company", "").lower() for row in rows)
            assert any("crazygames" in row.get("company", "").lower() for row in rows)
            assert any("gameplay galaxy" in row.get("company", "").lower() for row in rows)
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
            timing = runtime.get("timingSummary") or {}
            assert "medianSourceDurationMs" in timing
            assert "p95SourceDurationMs" in timing
            assert "stageTotalsMs" in timing
            assert "adapterTimings" in timing
            assert "summary" in report
            assert "sources" in report
            assert str(report["sources"][0].get("fetchStrategy") or "") == "auto"
            assert "loss" in report["sources"][0]
            assert "canonicalDropReasons" in (report["sources"][0].get("loss") or {})
            stage_timings = report["sources"][0].get("stageTimingsMs") or {}
            if stage_timings:
                assert "fetchAndParse" in stage_timings
            adapter_timings = timing.get("adapterTimings") or []
            if adapter_timings:
                assert str(adapter_timings[0].get("adapter") or "") == "custom"

            task_payload = json.loads((out / "jobs-fetch-tasks.json").read_text(encoding="utf-8"))
            assert str(task_payload.get("schemaVersion") or "") == str(jf.SCHEMA_VERSION)
            assert "summary" in task_payload
            assert "tasks" in task_payload
            assert "outputs" in task_payload
            assert str((task_payload.get("outputs") or {}).get("report") or "") == str(out / "jobs-fetch-report.json")
            assert str((task_payload.get("tasks") or [])[0].get("status") or "") == "ok"

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
                fetch_text=kwargs["fetch_text"],
                timeout_s=kwargs["timeout_s"],
                retries=kwargs["retries"],
                backoff_s=kwargs["backoff_s"],
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
            assert str(source_state.get("lastAdapter") or "") == "csv"

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


def test_get_incremental_cache_decision_prefers_skip_and_listing_modes() -> None:
        from src.jobs import state as state_pkg

        now = jf.datetime.now(jf.timezone.utc)
        rows = {
            "provider_source": {
                "lastAdapter": "greenhouse",
                "lastStatus": "ok",
                "lastSuccessAt": (now - jf.timedelta(minutes=10)).isoformat(),
                "lastChangedAt": (now - jf.timedelta(minutes=20)).isoformat(),
                "lastKeptCount": 3,
            },
            "static_source::example": {
                "lastAdapter": "static",
                "lastStatus": "ok",
                "lastSuccessAt": (now - jf.timedelta(minutes=20)).isoformat(),
                "lastKeptCount": 2,
                "lastListingFingerprint": "abc123",
            },
        }
        provider_decision = state_pkg.get_incremental_cache_decision("provider_source", rows, adapter="greenhouse")
        static_decision = state_pkg.get_incremental_cache_decision("static_source::example", rows, adapter="static")
        assert provider_decision["cacheDecision"] == "skip_fresh"
        assert static_decision["cacheDecision"] == "listing_only"


def test_get_incremental_cache_decision_treats_future_next_eligible_after_run_as_skip_fresh() -> None:
        from src.jobs import state as state_pkg

        now = jf.datetime.now(jf.timezone.utc)
        rows = {
            "provider_board": {
                "lastAdapter": "lever",
                "lastStatus": "ok",
                "lastSuccessAt": (now - jf.timedelta(minutes=1)).isoformat(),
                "lastKeptCount": 5,
                "nextEligibleCheckAt": (now + jf.timedelta(minutes=30)).isoformat(),
                "cacheDecision": "run_now",
                "cacheDecisionReason": "no_cache_state",
            }
        }
        decision = state_pkg.get_incremental_cache_decision("provider_board", rows, adapter="lever")
        assert decision["cacheDecision"] == "skip_fresh"
        assert decision["cacheDecisionReason"] == "within_freshness_window"


def test_run_pipeline_incremental_second_run_skips_fresh_source_and_preserves_output() -> None:
        calls = {"count": 0}

        def ok_loader(**_: object):
            calls["count"] += 1
            return [
                {
                    "title": "Gameplay Engineer",
                    "company": "Incremental Studio",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "Remote",
                    "contractType": "Full-time",
                    "jobLink": "https://example.com/incremental/gameplay-engineer",
                    "sector": "Game",
                    "sourceJobId": "incremental-1",
                    "postedAt": "2026-03-01",
                }
            ]

        with workspace_tmpdir("jobs-fetcher-incremental") as tmp:
            out = Path(tmp)
            first = jf.run_pipeline(output_dir=out, source_loaders=[("incremental_source", ok_loader)], show_progress=False)
            second = jf.run_pipeline(output_dir=out, source_loaders=[("incremental_source", ok_loader)], show_progress=False)
            assert calls["count"] == 1
            assert int(first["summary"].get("outputCount") or 0) == 1
            assert int(second["summary"].get("outputCount") or 0) == 1
            excluded = [row for row in (second.get("sources") or []) if row.get("name") == "incremental_source"]
            assert len(excluded) == 1
            assert str(excluded[0].get("status") or "") == "excluded"
            assert str(excluded[0].get("cacheDecision") or "") == "skip_fresh"
            assert "cache_" in str(excluded[0].get("exclusionReason") or "")


def test_run_pipeline_force_refresh_all_bypasses_incremental_skip() -> None:
        calls = {"count": 0}

        def ok_loader(**_: object):
            calls["count"] += 1
            return [
                {
                    "title": "Engine Programmer",
                    "company": "Refresh Studio",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "Remote",
                    "contractType": "Full-time",
                    "jobLink": "https://example.com/refresh/engine-programmer",
                    "sector": "Game",
                    "sourceJobId": "refresh-1",
                    "postedAt": "2026-03-01",
                }
            ]

        with workspace_tmpdir("jobs-fetcher-force-refresh") as tmp:
            out = Path(tmp)
            jf.run_pipeline(output_dir=out, source_loaders=[("refresh_source", ok_loader)], show_progress=False)
            jf.run_pipeline(
                output_dir=out,
                source_loaders=[("refresh_source", ok_loader)],
                show_progress=False,
                force_refresh_all=True,
            )
            assert calls["count"] == 2


def test_apply_incremental_cache_exclusions_keeps_provider_family_loader_for_board_level_refresh() -> None:
        from src.jobs import pipeline_loader_selection as selection_pkg
        from src.jobs import state as state_pkg

        now = jf.datetime.now(jf.timezone.utc)
        future = (now + jf.timedelta(minutes=10)).isoformat()
        selected = [
            ("greenhouse_boards", lambda **_: []),
            ("incremental_source", lambda **_: []),
        ]
        source_state_rows = {
            "greenhouse_boards": {
                "lastAdapter": "greenhouse",
                "lastStatus": "ok",
                "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
                "lastKeptCount": 2,
                "nextEligibleCheckAt": future,
                "cacheDecision": "skip_fresh",
                "cacheDecisionReason": "within_freshness_window",
            },
            "incremental_source": {
                "lastAdapter": "custom",
                "lastStatus": "ok",
                "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
                "lastKeptCount": 1,
                "nextEligibleCheckAt": future,
                "cacheDecision": "skip_fresh",
                "cacheDecisionReason": "within_freshness_window",
            },
        }
        filtered, skipped = selection_pkg.apply_incremental_cache_exclusions(
            selected,
            incremental_cache_enabled=True,
            force_refresh_all=False,
            source_state_rows=source_state_rows,
            get_incremental_cache_decision=state_pkg.get_incremental_cache_decision,
            build_excluded_source_report=lambda name, reason: {"name": name, "exclusionReason": reason},
            source_report_meta={
                "greenhouse_boards": {"adapter": "greenhouse"},
                "incremental_source": {"adapter": "custom"},
            },
        )
        assert [name for name, _ in filtered] == ["greenhouse_boards"]
        assert [row["name"] for row in skipped] == ["incremental_source"]


def test_provider_family_json_sources_refresh_only_stale_boards() -> None:
        from src.jobs.adapters.plugins.provider_api import register as provider_register

        calls = []
        captured = {}

        class _Deps:
            def registry_entries(self, adapter: str):
                assert adapter == "greenhouse"
                return [
                    {"name": "Fresh Board", "studio": "Fresh Board", "endpoint": "https://example.com/fresh.json"},
                    {"name": "Stale Board", "studio": "Stale Board", "endpoint": "https://example.com/stale.json"},
                ]

            def fetch_with_retries(self, url: str, fetch_text, timeout_s: int, retries: int, backoff_s: float) -> str:
                calls.append(url)
                return json.dumps({"jobs": [{"id": url}]})

            def set_source_diagnostics(self, source_name: str, **kwargs) -> None:
                captured["source_name"] = source_name
                captured["kwargs"] = kwargs

        now = jf.datetime.now(jf.timezone.utc)
        state_rows = {
            "Fresh Board": {
                "lastAdapter": "greenhouse",
                "lastStatus": "ok",
                "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
                "lastKeptCount": 2,
                "nextEligibleCheckAt": (now + jf.timedelta(minutes=10)).isoformat(),
                "cacheDecision": "skip_fresh",
                "cacheDecisionReason": "within_freshness_window",
            },
            "Stale Board": {
                "lastAdapter": "greenhouse",
                "lastStatus": "ok",
                "lastSuccessAt": (now - jf.timedelta(hours=3)).isoformat(),
                "lastChangedAt": (now - jf.timedelta(days=2)).isoformat(),
                "lastKeptCount": 2,
            },
        }

        with mock.patch.object(provider_register.runtime_deps, "facade", return_value=_Deps()):
            rows = provider_register._run_json_feed_sources(
                adapter_name="greenhouse",
                registry_adapter="greenhouse",
                default_error="missing endpoint",
                parse_payload=lambda source, payload, studio: [{
                    "title": f"{studio} Engineer",
                    "company": studio,
                    "city": "",
                    "country": "Unknown",
                    "workType": "",
                    "contractType": "",
                    "jobLink": f"https://example.com/{str(source.get('name') or '').lower().replace(' ', '-')}",
                    "sector": "Game",
                    "postedAt": "",
                    "sourceJobId": f"greenhouse:{str(source.get('name') or '')}",
                }],
                build_url=lambda source: str(source.get("endpoint") or ""),
                payload_count=lambda payload, parsed: len(parsed),
                fetch_text=lambda url, timeout: "",
                timeout_s=5,
                retries=0,
                backoff_s=0,
                source_state_rows=state_rows,
                force_refresh_all=False,
            )
        assert calls == ["https://example.com/stale.json"]
        assert len(rows) == 1
        details = captured["kwargs"]["details"]
        fresh_detail = next(row for row in details if row["name"] == "Fresh Board")
        stale_detail = next(row for row in details if row["name"] == "Stale Board")
        assert fresh_detail["status"] == "excluded"
        assert fresh_detail["cacheDecision"] == "skip_fresh"
        assert stale_detail["status"] == "ok"
        assert stale_detail["cacheDecision"] == "run_now"


def test_provider_family_revalidate_only_board_skips_fetch_on_not_modified() -> None:
        from src.jobs.adapters.plugins.provider_api import register as provider_register

        calls = []
        captured = {}

        class _Deps:
            def registry_entries(self, adapter: str):
                assert adapter == "lever"
                return [{"name": "Revalidate Board", "studio": "Revalidate Board", "endpoint": "https://example.com/revalidate.json"}]

            def fetch_with_retries(self, url: str, fetch_text, timeout_s: int, retries: int, backoff_s: float) -> str:
                calls.append(url)
                return "[]"

            def set_source_diagnostics(self, source_name: str, **kwargs) -> None:
                captured["kwargs"] = kwargs

        now = jf.datetime.now(jf.timezone.utc)
        state_rows = {
            "Revalidate Board": {
                "lastAdapter": "lever",
                "lastStatus": "ok",
                "lastSuccessAt": (now - jf.timedelta(minutes=30)).isoformat(),
                "lastChangedAt": (now - jf.timedelta(days=2)).isoformat(),
                "lastKeptCount": 1,
                "lastHttpEtag": "etag-1",
            }
        }

        with mock.patch.object(provider_register.runtime_deps, "facade", return_value=_Deps()), \
             mock.patch.object(
                 provider_register,
                 "conditional_revalidate_url",
                 return_value={"supported": True, "notModified": True, "statusCode": 304, "etag": "etag-1", "lastModified": ""},
             ):
            rows = provider_register._run_json_feed_sources(
                adapter_name="lever",
                registry_adapter="lever",
                default_error="missing endpoint",
                parse_payload=lambda source, payload, studio: [],
                build_url=lambda source: str(source.get("endpoint") or ""),
                payload_count=lambda payload, parsed: len(parsed),
                fetch_text=lambda url, timeout: "",
                timeout_s=5,
                retries=0,
                backoff_s=0,
                source_state_rows=state_rows,
                force_refresh_all=False,
            )
        assert rows == []
        assert calls == []
        details = captured["kwargs"]["details"]
        assert len(details) == 1
        assert details[0]["status"] == "excluded"
        assert details[0]["cacheDecision"] == "revalidate_only"
        assert details[0]["cacheDecisionReason"] == "not_modified_304"
        assert details[0]["httpStatus"] == 304


def test_teamtailor_sources_skip_fresh_listing_without_fetching() -> None:
        from src.jobs.adapters.plugins.provider_api import register as provider_register

        calls = []
        captured = {}

        class _Deps:
            def registry_entries(self, adapter: str):
                assert adapter == "teamtailor"
                return [{
                    "name": "Paradox Teamtailor",
                    "studio": "Paradox Interactive",
                    "listing_url": "https://career.paradoxplaza.com/jobs",
                    "base_url": "https://career.paradoxplaza.com",
                    "company": "Paradox Interactive",
                }]

            def fetch_with_retries(self, url: str, fetch_text, timeout_s: int, retries: int, backoff_s: float) -> str:
                calls.append(url)
                return ""

            def set_source_diagnostics(self, source_name: str, **kwargs) -> None:
                captured["kwargs"] = kwargs

        now = jf.datetime.now(jf.timezone.utc)
        state_rows = {
            "Paradox Teamtailor": {
                "lastAdapter": "teamtailor",
                "lastStatus": "ok",
                "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
                "lastKeptCount": 3,
                "nextEligibleCheckAt": (now + jf.timedelta(minutes=20)).isoformat(),
                "cacheDecision": "skip_fresh",
                "cacheDecisionReason": "within_freshness_window",
            }
        }

        with mock.patch.object(provider_register.runtime_deps, "facade", return_value=_Deps()):
            rows = provider_register._run_teamtailor_sources(
                fetch_text=lambda url, timeout: "",
                timeout_s=5,
                retries=0,
                backoff_s=0,
                source_state_rows=state_rows,
                force_refresh_all=False,
            )
        assert rows == []
        assert calls == []
        details = captured["kwargs"]["details"]
        assert len(details) == 1
        assert details[0]["status"] == "excluded"
        assert details[0]["cacheDecision"] == "skip_fresh"
        assert details[0]["cacheDecisionReason"] == "within_freshness_window"


def test_apply_incremental_cache_exclusions_keeps_social_multi_feed_loaders_for_detail_level_refresh() -> None:
        from src.jobs import pipeline_loader_selection as selection_pkg
        from src.jobs import state as state_pkg

        now = jf.datetime.now(jf.timezone.utc)
        future = (now + jf.timedelta(minutes=10)).isoformat()
        selected = [
            ("social_x", lambda **_: []),
            ("social_mastodon", lambda **_: []),
            ("social_reddit", lambda **_: []),
        ]
        source_state_rows = {
            "social_x": {"lastAdapter": "social", "nextEligibleCheckAt": future, "cacheDecision": "skip_fresh", "cacheDecisionReason": "within_freshness_window"},
            "social_mastodon": {"lastAdapter": "social", "nextEligibleCheckAt": future, "cacheDecision": "skip_fresh", "cacheDecisionReason": "within_freshness_window"},
            "social_reddit": {"lastAdapter": "social", "nextEligibleCheckAt": future, "cacheDecision": "skip_fresh", "cacheDecisionReason": "within_freshness_window"},
        }
        filtered, skipped = selection_pkg.apply_incremental_cache_exclusions(
            selected,
            incremental_cache_enabled=True,
            force_refresh_all=False,
            source_state_rows=source_state_rows,
            get_incremental_cache_decision=state_pkg.get_incremental_cache_decision,
            build_excluded_source_report=lambda name, reason: {"name": name, "exclusionReason": reason},
            source_report_meta={
                "social_x": {"adapter": "social"},
                "social_mastodon": {"adapter": "social"},
                "social_reddit": {"adapter": "social"},
            },
        )
        assert [name for name, _ in filtered] == ["social_x", "social_mastodon"]
        assert [row["name"] for row in skipped] == ["social_reddit"]


def test_social_x_skips_fresh_query_without_fetching() -> None:
        cfg = {
            "enabled": True,
            "minConfidence": 40,
            "rejectForHirePosts": True,
            "x": {
                "enabled": True,
                "queries": ["game jobs"],
                "rssFallback": {"enabled": True, "instances": ["https://nitter.example"]},
            },
        }
        now = jf.datetime.now(jf.timezone.utc)
        state_rows = {
            "x:game jobs": {
                "lastAdapter": "social",
                "lastStatus": "ok",
                "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
                "lastKeptCount": 2,
                "nextEligibleCheckAt": (now + jf.timedelta(minutes=20)).isoformat(),
                "cacheDecision": "skip_fresh",
                "cacheDecisionReason": "within_freshness_window",
            }
        }

        def failing_fetch(url: str, timeout: int) -> str:
            raise AssertionError("social_x fetch should be skipped for fresh query")

        rows = jf.run_social_x_source(
            fetch_text=failing_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            social_config=cfg,
            source_state_rows=state_rows,
            force_refresh_all=False,
        )
        assert rows == []
        diag = jf.SOURCE_DIAGNOSTICS.get("social_x") or {}
        details = diag.get("details") or []
        assert len(details) == 1
        assert details[0]["status"] == "excluded"
        assert details[0]["cacheDecision"] == "skip_fresh"


def test_social_mastodon_skips_fresh_instance_tag_without_fetching() -> None:
        cfg = {
            "enabled": True,
            "minConfidence": 40,
            "rejectForHirePosts": True,
            "mastodon": {
                "enabled": True,
                "instances": ["https://mastodon.example"],
                "hashtags": ["gamedevjobs"],
            },
        }
        now = jf.datetime.now(jf.timezone.utc)
        state_rows = {
            "mastodon:mastodon.example:#gamedevjobs": {
                "lastAdapter": "social",
                "lastStatus": "ok",
                "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
                "lastKeptCount": 2,
                "nextEligibleCheckAt": (now + jf.timedelta(minutes=20)).isoformat(),
                "cacheDecision": "skip_fresh",
                "cacheDecisionReason": "within_freshness_window",
            }
        }

        def failing_fetch(url: str, timeout: int) -> str:
            raise AssertionError("social_mastodon fetch should be skipped for fresh instance/tag")

        rows = jf.run_social_mastodon_source(
            fetch_text=failing_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            social_config=cfg,
            source_state_rows=state_rows,
            force_refresh_all=False,
        )
        assert rows == []
        diag = jf.SOURCE_DIAGNOSTICS.get("social_mastodon") or {}
        details = diag.get("details") or []
        assert len(details) == 1
        assert details[0]["status"] == "excluded"
        assert details[0]["cacheDecision"] == "skip_fresh"


def test_social_reddit_skips_fresh_subreddit_without_fetching() -> None:
        cfg = {
            "enabled": True,
            "minConfidence": 40,
            "rejectForHirePosts": True,
            "reddit": {
                "enabled": True,
                "subreddits": ["gamedev"],
                "maxPostsPerSubreddit": 5,
                "rssFallback": True,
                "htmlFallback": True,
                "rateLimitDelay": 0,
            },
        }
        future = "2099-01-01T00:00:00+00:00"
        state_rows = {
            "reddit:r/gamedev": {
                "lastAdapter": "social",
                "nextEligibleCheckAt": future,
                "cacheDecision": "skip_fresh",
                "cacheDecisionReason": "within_freshness_window",
            }
        }

        def failing_fetch(_: str, __: int) -> str:
            raise AssertionError("social_reddit fetch should be skipped for fresh subreddit")

        rows = jf.run_social_reddit_source(
            fetch_text=failing_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            social_config=cfg,
            source_state_rows=state_rows,
            force_refresh_all=False,
        )
        assert rows == []
        diag = jf.SOURCE_DIAGNOSTICS.get("social_reddit") or {}
        details = diag.get("details") or []
        assert len(details) == 1
        assert details[0]["status"] == "excluded"
        assert details[0]["cacheDecision"] == "skip_fresh"


def test_run_social_reddit_source_keeps_successful_rss_fallback_out_of_error_state() -> None:
        cfg = {
            "enabled": True,
            "minConfidence": 20,
            "rejectForHirePosts": True,
            "reddit": {
                "enabled": True,
                "subreddits": ["gamedev"],
                "maxPostsPerSubreddit": 5,
                "rssFallback": True,
                "htmlFallback": False,
                "rateLimitDelay": 0,
            },
        }
        calls = []

        def fake_fetch(url: str, _: int) -> str:
            calls.append(url)
            if url.endswith("/new.json?limit=5"):
                raise RuntimeError("json api blocked")
            if url.endswith("/new.rss"):
                return "<feed />"
            raise AssertionError(f"unexpected reddit url: {url}")

        with mock.patch(
            "src.jobs.adapters.plugins.social.register._social_parsers.parse_reddit_rss_payload",
            return_value=(
                [
                    {
                        "title": "Technical Artist",
                        "company": "Nebula Games",
                        "jobLink": "https://jobs.nebula.dev/ta",
                        "sourceJobId": "reddit:gamedev:abc123",
                        "source": "social_reddit",
                    }
                ],
                0,
            ),
        ):
            rows = jf.run_social_reddit_source(
                fetch_text=fake_fetch,
                timeout_s=5,
                retries=0,
                backoff_s=0,
                social_config=cfg,
            )
        assert len(rows) >= 1
        diag = jf.SOURCE_DIAGNOSTICS.get("social_reddit") or {}
        details = diag.get("details") or []
        assert len(details) == 1
        assert details[0]["status"] == "ok"
        assert details[0]["error"] == ""


def test_run_pipeline_reports_social_subsource_cache_rollup() -> None:
        def social_loader(**_: object):
            jf.SOURCE_DIAGNOSTICS["social_x"] = {
                "adapter": "social",
                "studio": "x",
                "details": [
                    {"name": "x:game jobs", "studio": "x", "status": "excluded", "cacheDecision": "skip_fresh", "cacheDecisionReason": "within_freshness_window"},
                    {"name": "x:unity jobs", "studio": "x", "status": "ok", "cacheDecision": "run_now", "cacheDecisionReason": "provider_refresh_due", "fetchedCount": 1, "keptCount": 1},
                ],
            }
            return [{
                "title": "Gameplay Engineer",
                "company": "Studio Social",
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": "https://example.com/social/gameplay-engineer",
                "sector": "Game",
                "sourceJobId": "social-x-1",
                "postedAt": "2026-03-01",
            }]

        with workspace_tmpdir("jobs-fetcher-social-subsource-rollup") as tmp:
            report = jf.run_pipeline(
                output_dir=Path(tmp),
                source_loaders=[("social_x", social_loader)],
                show_progress=False,
                force_refresh_all=True,
            )
            row = next(item for item in report["sources"] if item["name"] == "social_x")
            assert row["subsourceCount"] == 2
            assert row["subsourceCacheDecisionCounts"] == {"skip_fresh": 1, "run_now": 1}
            assert row["subsourceSkippedCount"] == 1
            assert row["subsourceRefreshedCount"] == 1


def test_run_pipeline_reports_board_level_provider_cache_rollup() -> None:
        def provider_family_loader(**_: object):
            jf.SOURCE_DIAGNOSTICS["greenhouse_boards"] = {
                "adapter": "greenhouse",
                "studio": "multiple",
                "details": [
                    {"name": "Board A", "studio": "Board A", "status": "excluded", "cacheDecision": "skip_fresh", "cacheDecisionReason": "within_freshness_window"},
                    {"name": "Board B", "studio": "Board B", "status": "excluded", "cacheDecision": "revalidate_only", "cacheDecisionReason": "not_modified_304", "httpStatus": 304},
                    {"name": "Board C", "studio": "Board C", "status": "ok", "cacheDecision": "run_now", "cacheDecisionReason": "provider_refresh_due", "fetchedCount": 1, "keptCount": 1},
                ],
            }
            return [
                {
                    "title": "Gameplay Engineer",
                    "company": "Board C",
                    "city": "",
                    "country": "Unknown",
                    "workType": "",
                    "contractType": "",
                    "jobLink": "https://example.com/board-c/gameplay-engineer",
                    "sector": "Game",
                    "sourceJobId": "greenhouse:board-c:1",
                    "postedAt": "2026-03-01",
                }
            ]

        with workspace_tmpdir("jobs-fetcher-provider-board-rollup") as tmp:
            report = jf.run_pipeline(
                output_dir=Path(tmp),
                source_loaders=[("greenhouse_boards", provider_family_loader)],
                show_progress=False,
                force_refresh_all=True,
            )
            row = next(item for item in report["sources"] if item["name"] == "greenhouse_boards")
            assert row["boardCount"] == 3
            assert row["boardCacheDecisionCounts"] == {"skip_fresh": 1, "revalidate_only": 1, "run_now": 1}
            assert row["boardSkippedCount"] == 1
            assert row["boardRevalidatedCount"] == 1
            assert row["boardNotModifiedCount"] == 1
            assert row["boardRefreshedCount"] == 1

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


def test_choose_detail_traversal_mode_prefers_listing_only_for_verified_hosts() -> None:
        from src.jobs.adapters.static_helpers import build_static_source_runtime_config, choose_detail_traversal_mode

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


def test_personio_adapter_skips_recent_rate_limited_source_only() -> None:
        from src.jobs.adapters import provider_api

        now = jf.datetime.now(jf.timezone.utc).isoformat()
        registry_rows = [
            {"name": "Rate Limited Studio", "studio": "Rate Limited Studio", "feed_url": "https://example.com/rate.xml"},
            {"name": "Healthy Studio", "studio": "Healthy Studio", "feed_url": "https://example.com/ok.xml"},
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


def test_run_static_studio_pages_source_globalstep_listing_only() -> None:
        html = """
        <a href="https://globalstep.com/jobs/unity-game-developer/">
          <h2>Unity Game Developer</h2>
          <span>Bucharest - Romania</span>
          <span>3+ Years</span>
          <span>More Details</span>
        </a>
        """
        rows = jf.run_static_studio_pages_source(
            fetch_text=lambda _url, _timeout: html,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[{
                "name": "GlobalStep (Sheet)",
                "studio": "GlobalStep",
                "company": "GlobalStep",
                "pages": ["https://globalstep.com/careers/"],
                "id": "static:listing_url:https://globalstep.com/careers/",
            }],
        )
        assert len(rows) == 1
        assert rows[0]["jobLink"] == "https://globalstep.com/jobs/unity-game-developer/"


def test_run_static_studio_pages_source_climax_listing_only() -> None:
        html = """
        <a href="https://www.climaxstudios.com/join-our-team/jobs/eD-experienced-games-producer/">
          <h3>Experienced Games Producer</h3>
          <p>Exciting role</p>
          <div>Location London England United Kingdom</div>
          <div>Create, Permanent</div>
        </a>
        """
        rows = jf.run_static_studio_pages_source(
            fetch_text=lambda _url, _timeout: html,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[{
                "name": "Climax Studios (Sheet)",
                "studio": "Climax Studios",
                "company": "Climax Studios",
                "pages": ["https://www.climaxstudios.com/join-our-team/jobs/"],
                "id": "static:listing_url:https://www.climaxstudios.com/join-our-team/jobs/",
            }],
        )
        assert len(rows) == 1
        assert rows[0]["title"] == "Experienced Games Producer"


def test_run_static_studio_pages_source_amber_jobvite_listing_only() -> None:
        html = """
        <a href="/amberstudiocareers/job/oSIbufwZ">
          <div>Senior Unity Game Engineer (Project Based)</div>
          <div>Remote, Brazil</div>
        </a>
        """
        rows = jf.run_static_studio_pages_source(
            fetch_text=lambda _url, _timeout: html,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[{
                "name": "Amber (Sheet)",
                "studio": "Amber",
                "company": "Amber",
                "pages": ["https://jobs.jobvite.com/amberstudiocareers/search?l=Worldwide"],
                "id": "static:listing_url:https://jobs.jobvite.com/amberstudiocareers/search?l=Worldwide",
            }],
        )
        assert len(rows) == 1
        assert rows[0]["jobLink"] == "https://jobs.jobvite.com/amberstudiocareers/job/oSIbufwZ"


def test_run_static_studio_pages_source_amanotes_plugin_extracts_next_data_positions() -> None:
        html = """
        <script id="__NEXT_DATA__" type="application/json">
        {
          "props": {
            "pageProps": {
              "positions": [
                {
                  "title": "Senior Backend Developer (NodeJS)",
                  "location": "HCMC",
                  "type": "Full-time",
                  "team": "Tech",
                  "leverId": "43fa1ef6-a45e-4718-9b8f-022c673632c6",
                  "slug": {"current": "senior-backend-developer"}
                },
                {
                  "title": "[New Games] Game Unit Manager",
                  "location": "HCMC",
                  "type": "Full-time",
                  "team": "Games",
                  "leverId": "cb73238c-a74d-4d0f-9dfd-a4c32e0f1c41",
                  "slug": {"current": "new-games-game-unit-manager"}
                }
              ]
            }
          }
        }
        </script>
        """
        rows = jf.run_static_studio_pages_source(
            fetch_text=lambda _url, _timeout: html,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[{
                "name": "Amanotes (Sheet)",
                "studio": "Amanotes",
                "company": "Amanotes",
                "pages": ["https://www.careers.amanotes.com/jobs"],
                "id": "static:listing_url:https://www.careers.amanotes.com/jobs",
            }],
        )
        assert [row["title"] for row in rows] == [
            "Senior Backend Developer (NodeJS)",
            "[New Games] Game Unit Manager",
        ]
        assert rows[0]["jobLink"] == (
            "https://www.careers.amanotes.com/jobs/"
            "senior-backend-developer/43fa1ef6-a45e-4718-9b8f-022c673632c6"
        )


def test_run_pipeline_records_wall_clock_timing_summary() -> None:
        def ok_loader(**_: object):
            return [
                {
                    "title": "Gameplay Engineer",
                    "company": "Timing Studio",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "Remote",
                    "contractType": "Full-time",
                    "jobLink": "https://example.com/timing/gameplay-engineer",
                    "sector": "Game",
                    "sourceJobId": "timing-1",
                    "postedAt": "2026-03-01",
                }
            ]

        with workspace_tmpdir("jobs-fetcher-wall-clock") as tmp:
            report = jf.run_pipeline(output_dir=Path(tmp), source_loaders=[("timing_source", ok_loader)], show_progress=False)
            timing = (((report.get("runtime") or {}).get("timingSummary")) or {})
            assert int(timing.get("wallClockDurationMs") or 0) >= 0


def test_personio_rate_limit_cooldown_can_be_configured() -> None:
        from src.jobs.adapters import provider_api

        with mock.patch.dict("os.environ", {"BALUFFO_PERSONIO_RATE_LIMIT_COOLDOWN_MINUTES": "15"}, clear=False):
            cutoff = provider_api._personio_rate_limit_cutoff()
        delta_minutes = (jf.datetime.now(jf.timezone.utc) - cutoff).total_seconds() / 60
        assert 14 <= delta_minutes <= 16


def test_normalize_work_type_derives_remote_from_title_when_field_empty() -> None:
    from src.jobs.normalizers import normalize_work_type

    assert normalize_work_type("", "Technical Artist (Malta/Remote)") == "Remote"
    assert normalize_work_type("", "Gameplay Programmer (Malta/Remote)") == "Remote"
    assert normalize_work_type("", "Senior Engineer - Remote") == "Remote"
    assert normalize_work_type("", "Ui Programmer (Remote)") == "Remote"
    assert normalize_work_type("", "Ai Programmer (Malta/Remote)") == "Remote"

    assert normalize_work_type("", "Senior Engineer - Onsite") == "Onsite"
    assert normalize_work_type("", "Office Assistant (Malta)") == "Onsite"
    assert normalize_work_type("", "Project Manager (Malta)") == "Onsite"

    assert normalize_work_type("Remote", "Some Onsite Job") == "Remote"
    assert normalize_work_type("Hybrid", "Onsite Engineer") == "Hybrid"
    assert normalize_work_type("", "Engineer - Hybrid") == "Hybrid"
    assert normalize_work_type("", "Mixed Mode Artist") == "Hybrid"



