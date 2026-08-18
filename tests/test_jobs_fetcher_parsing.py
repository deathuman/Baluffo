import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from src import jobs_fetcher as jf
from src.jobs.adapters.html_parsers import parse_jobposting_location_details
from src.scrapers import runner as scrapy_runner
from tests.helpers.job_fixtures import _fixture
from tests.helpers.mutation import append_and_return


def test_scrapy_runner_emit_envelope_tolerates_non_json_safe_values(
    capsys: pytest.CaptureFixture[str],
) -> None:
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


def test_social_parsers_drop_discussion_and_not_hiring_posts() -> None:
    reddit_payload = {
        "data": {
            "children": [
                {
                    "data": {
                        "id": "bad001",
                        "title": "Why is nobody hiring gameplay programmers anymore?",
                        "selftext": "This industry is rough. https://studio.example/blog/hiring-rant",
                        "link_flair_text": "Discussion",
                        "permalink": "/r/gamedev/comments/bad001/test/",
                        "url": "https://www.reddit.com/r/gamedev/comments/bad001/test/",
                        "created_utc": 1700000000,
                        "author": "someone",
                    }
                },
                {
                    "data": {
                        "id": "bad002",
                        "title": "We are not hiring right now at Nebula Games",
                        "selftext": "Please stop asking.",
                        "link_flair_text": "Meta",
                        "permalink": "/r/gamedev/comments/bad002/test/",
                        "url": "https://www.reddit.com/r/gamedev/comments/bad002/test/",
                        "created_utc": 1700000000,
                        "author": "nebula_hr",
                    }
                },
            ]
        }
    }
    rows, dropped = jf.parse_reddit_json_payload(
        reddit_payload,
        subreddit="gamedev",
        min_confidence=20,
        reject_for_hire_posts=True,
    )
    assert rows == []
    assert dropped == 2


def test_social_parsers_reject_reddit_article_links_with_author_fallback_company() -> None:
    reject_reasons: dict[str, Any] = {}
    reddit_payload = {
        "data": {
            "children": [
                {
                    "data": {
                        "id": "bad003",
                        "title": "A study on why games raise shadow levels, occasionally making OLEDs look like LCDs!",
                        "selftext": "We're hiring curious rendering engineers. https://gammastudios.tech/technical-color-grading-1-raised-blacks",
                        "link_flair_text": "Hiring",
                        "permalink": "/r/gamedev/comments/bad003/test/",
                        "url": "https://gammastudios.tech/technical-color-grading-1-raised-blacks",
                        "created_utc": 1700000000,
                        "author": "filoppi",
                    }
                }
            ]
        }
    }
    rows, dropped = jf.parse_reddit_json_payload(
        reddit_payload,
        subreddit="gamedev",
        min_confidence=20,
        reject_for_hire_posts=True,
        reject_reasons=reject_reasons,
    )
    assert rows == []
    assert dropped == 1
    assert reject_reasons["non_job_destination_url"] == 1


def test_social_parsers_reject_reddit_rss_article_links() -> None:
    reject_reasons: dict[str, Any] = {}
    rss = """<?xml version="1.0" encoding="UTF-8"?>
<rss><channel>
  <item>
    <title>A study on why games raise shadow levels, occasionally making OLEDs look like LCDs!</title>
    <link>https://gammastudios.tech/technical-color-grading-1-raised-blacks</link>
    <description>We're hiring curious rendering engineers.</description>
    <pubDate>Mon, 09 Mar 2026 11:00:00 GMT</pubDate>
  </item>
</channel></rss>"""
    rows, dropped = jf.parse_reddit_rss_payload(
        rss,
        subreddit="gamedev",
        min_confidence=20,
        reject_for_hire_posts=True,
        reject_reasons=reject_reasons,
    )
    assert rows == []
    assert dropped == 1
    assert reject_reasons["non_job_destination_url"] == 1


def test_social_parsers_drop_reposts_and_generic_job_chatter() -> None:
    rss = """<?xml version="1.0" encoding="UTF-8"?>
<rss><channel>
  <item>
    <title>Orbit Games is hiring a Unity Engineer</title>
    <link>https://nitter.net/orbit/status/123</link>
    <description>Posting this here because people should see it.</description>
    <pubDate>Mon, 09 Mar 2026 11:00:00 GMT</pubDate>
  </item>
</channel></rss>"""
    rows, dropped = jf.parse_x_rss_payload(
        rss,
        query_label="#gamedevjobs",
        min_confidence=20,
        reject_for_hire_posts=True,
    )
    assert rows == []
    assert dropped == 1

    mastodon_rows, mastodon_dropped = jf.parse_mastodon_payload(
        [
            {
                "id": "m2",
                "content": "<p>Anyone hiring Unity devs? My portfolio is at https://portfolio.example</p>",
                "created_at": "2026-03-09T11:05:00Z",
                "url": "https://mastodon.gamedev.place/@artist/112",
                "account": {"display_name": "Artist Person"},
            }
        ],
        instance="https://mastodon.gamedev.place",
        tag="gamedevjobs",
        min_confidence=20,
        reject_for_hire_posts=True,
    )
    assert mastodon_rows == []
    assert mastodon_dropped == 1


def test_social_parsers_allow_explicit_hiring_with_company_root_apply_link() -> None:
    x_rows, x_dropped = jf.parse_x_payload(
        {
            "data": [
                {
                    "id": "988",
                    "text": "Moonshot Games is hiring gameplay engineers. Apply at https://moonshotgames.com",
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
    assert x_rows[0]["jobLink"] == "https://moonshotgames.com/"


def test_run_social_x_source_uses_rss_fallback_without_credentials() -> None:
    social_cfg = {
        "enabled": True,
        "minConfidence": 20,
        "rejectForHirePosts": True,
        "x": {
            "enabled": True,
            "queries": ["#gamedevjobs"],
            "maxPostsPerQuery": 5,
            "api": {
                "enabled": True,
                "endpoint": "https://api.x.com/2/tweets/search/recent",
                "bearerTokenEnv": "BALUFFO_X_BEARER_TOKEN",
            },
            "scraperFallback": {"enabled": False, "endpoint": ""},
            "rssFallback": {"enabled": True, "instances": ["https://nitter.net"]},
        },
    }
    rss = """<?xml version="1.0" encoding="UTF-8"?>
<rss><channel>
  <item>
    <title>We're hiring a Technical Artist at Nova Studio</title>
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


def test_normalize_url_strips_language_query_param() -> None:
    from src.jobs.common.url import fingerprint_url
    from src.jobs.text_utils import normalize_url

    base = "https://www.personio.de/job/1317878"
    with_lang = "https://www.personio.de/job/1317878?language=en"
    with_lang_short = "https://www.personio.de/job/1317878?lang"
    with_lang_other = "https://www.personio.de/job/1317878?other=val"
    assert fingerprint_url(with_lang) == fingerprint_url(base)
    assert fingerprint_url(with_lang_short) == fingerprint_url(base)
    assert fingerprint_url(with_lang_other) != fingerprint_url(base)
    assert normalize_url(with_lang) == with_lang
    assert normalize_url(with_lang_short) == "https://www.personio.de/job/1317878?lang="


def test_deduplicate_jobs_covers_redirect_and_identity_rules() -> None:
    now_iso = jf.now_iso()
    redirect_target = "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-technical-director-level-design-m-f-nb-projet-non-annonce"
    cases: list[dict[str, Any]] = [
        {
            "name": "unknown company enrichment",
            "rows": [
                {
                    "id": "",
                    "title": "Senior Environment Artist",
                    "company": "Unknown company",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "",
                    "contractType": "",
                    "jobLink": "https://www.smartrecruiters.com/CDPROJEKTRED/744000112115839",
                    "sector": "Game",
                    "source": "google_sheets",
                    "sourceJobId": "sheet-5",
                    "fetchedAt": now_iso,
                    "postedAt": "2026-03-09T10:00:00Z",
                    "status": "active",
                    "sourceBundle": [
                        {
                            "source": "google_sheets",
                            "jobLink": "https://gracklehq.com/rd/373481",
                            "postedAt": "2026-03-09T10:00:00Z",
                        }
                    ],
                },
                {
                    "id": "",
                    "title": "Senior Environment Artist",
                    "company": "CD PROJEKT RED",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "",
                    "contractType": "",
                    "jobLink": "https://jobs.smartrecruiters.com/CDPROJEKTRED/744000098332693",
                    "sector": "Game",
                    "source": "smartrecruiters_sources",
                    "sourceJobId": "smartrecruiters:CDPROJEKTRED:744000098332693",
                    "fetchedAt": now_iso,
                    "postedAt": "2026-03-09T10:00:00Z",
                    "status": "active",
                    "sourceBundle": [
                        {
                            "source": "google_sheets",
                            "jobLink": "https://gracklehq.com/rd/373481",
                            "postedAt": "2026-03-09T10:00:00Z",
                        },
                        {
                            "source": "smartrecruiters_sources",
                            "jobLink": "https://jobs.smartrecruiters.com/CDPROJEKTRED/744000098332693",
                            "postedAt": "2026-03-09T10:00:00Z",
                        },
                    ],
                },
            ],
            "outputCount": 2,
            "companies": {"CD PROJEKT RED"},
        },
        {
            "name": "known company preservation",
            "rows": [
                {
                    "id": "",
                    "title": "Senior Environment Artist",
                    "company": "Known Studio A",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "",
                    "contractType": "",
                    "jobLink": "https://jobs.smartrecruiters.com/CDPROJEKTRED/744000112115839",
                    "sector": "Game",
                    "source": "google_sheets",
                    "sourceJobId": "sheet-5",
                    "fetchedAt": now_iso,
                    "postedAt": "2026-03-09T10:00:00Z",
                    "status": "active",
                    "sourceBundle": [
                        {
                            "source": "google_sheets",
                            "jobLink": "https://gracklehq.com/rd/373481",
                            "postedAt": "2026-03-09T10:00:00Z",
                        }
                    ],
                },
                {
                    "id": "",
                    "title": "Senior Environment Artist",
                    "company": "Known Studio B",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "",
                    "contractType": "",
                    "jobLink": "https://jobs.smartrecruiters.com/CDPROJEKTRED/744000098332693",
                    "sector": "Game",
                    "source": "smartrecruiters_sources",
                    "sourceJobId": "smartrecruiters:CDPROJEKTRED:744000098332693",
                    "fetchedAt": now_iso,
                    "postedAt": "2026-03-09T10:00:00Z",
                    "status": "active",
                    "sourceBundle": [
                        {
                            "source": "google_sheets",
                            "jobLink": "https://gracklehq.com/rd/373481",
                            "postedAt": "2026-03-09T10:00:00Z",
                        },
                        {
                            "source": "smartrecruiters_sources",
                            "jobLink": "https://jobs.smartrecruiters.com/CDPROJEKTRED/744000098332693",
                            "postedAt": "2026-03-09T10:00:00Z",
                        },
                    ],
                },
            ],
            "outputCount": 2,
            "companies": {"Known Studio A", "Known Studio B"},
        },
        {
            "name": "social source id fallback",
            "rows": [
                {
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
                    "sourceBundle": [
                        {
                            "source": "social_reddit",
                            "sourceJobId": "reddit:gamedev:abc",
                            "jobLink": "",
                            "postedAt": "",
                            "adapter": "social",
                            "studio": "gamedev",
                        }
                    ],
                    "adapter": "social",
                    "studio": "reddit/gamedev",
                },
                {
                    "id": "",
                    "title": "Technical Artist",
                    "company": "Nebula Games",
                    "city": "",
                    "country": "Unknown",
                    "workType": "",
                    "contractType": "Unknown",
                    "jobLink": "https://www.reddit.com/r/gamedev/comments/abc",
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
                    "sourceBundle": [
                        {
                            "source": "social_reddit",
                            "sourceJobId": "reddit:gamedev:abc",
                            "jobLink": "",
                            "postedAt": "",
                            "adapter": "social",
                            "studio": "gamedev",
                        }
                    ],
                    "adapter": "social",
                    "studio": "reddit/gamedev",
                },
            ],
            "outputCount": 1,
            "mergedCount": 1,
        },
        {
            "name": "resolved redirect merge",
            "rows": [
                jf.canonicalize_job(
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
                    fetched_at=now_iso,
                    resolve_redirect_url=lambda url: (
                        redirect_target if "gracklehq.com/rd/372393" in str(url) else str(url)
                    ),
                ),
                jf.canonicalize_job(
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
                    fetched_at=now_iso,
                ),
            ],
            "outputCount": 1,
            "mergedByPrimaryUrl": 1,
            "company": "Ubisoft",
            "jobLink": redirect_target,
            "sourceBundleCount": 2,
        },
        {
            "name": "unresolved redirect separation",
            "rows": [
                jf.canonicalize_job(
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
                    fetched_at=now_iso,
                    resolve_redirect_url=lambda url: str(url),
                ),
                jf.canonicalize_job(
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
                    fetched_at=now_iso,
                ),
            ],
            "outputCount": 2,
            "mergedByPrimaryUrl": 0,
        },
    ]

    for case in cases:
        rows, stats = jf.deduplicate_jobs([row for row in case["rows"] if row is not None])
        assert int(stats.get("outputCount") or 0) == case["outputCount"], case["name"]
        if "companies" in case:
            assert {r.company for r in rows} == case["companies"], case["name"]
        if "mergedCount" in case:
            assert int(stats.get("mergedCount") or 0) == case["mergedCount"], case["name"]
        if "mergedByPrimaryUrl" in case:
            assert int(stats.get("mergedByPrimaryUrl") or 0) == case["mergedByPrimaryUrl"], case[
                "name"
            ]
        if "company" in case:
            assert rows[0].company == case["company"], case["name"]
        if "jobLink" in case:
            assert rows[0].jobLink == case["jobLink"], case["name"]
        if "sourceBundleCount" in case:
            assert rows[0].sourceBundleCount == case["sourceBundleCount"], case["name"]


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
        resolve_redirect_url=lambda url: append_and_return(
            calls,
            str(url),
            "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role",
        ),
    )

    assert row is not None
    assert row.jobLink == "https://gracklehq.com/rd/372393"
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
            return type(
                "_Resp",
                (),
                {"url": "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role"},
            )()

        def close(self) -> None:
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
    assert all(
        isinstance(row.get("sourceBundle"), list) and row.get("sourceBundle") for row in jobs
    )
    links = {str(row.get("jobLink") or "") for row in jobs}
    assert (
        "https://emp.jobylon.com/jobs/329202-remedy-entertainment-senior-support-engineer/" in links
    )
    assert "https://emp.jobylon.com/jobs/322343-remedy-entertainment-development-director/" in links


@pytest.mark.parametrize(
    ("location_entry", "expected_city", "expected_country", "expected_summary"),
    [
        ({"addressLocality": "Apr. 06", "addressCountry": "US"}, "", "US", ""),
        ({"addressLocality": "AI Solutions PM", "addressCountry": "US"}, "", "US", ""),
        (
            {"addressLocality": "Administrative & Support Services", "addressCountry": "CA"},
            "",
            "CA",
            "",
        ),
        (
            {"addressLocality": "Guildford", "addressCountry": "UK"},
            "Guildford",
            "UK",
            "Guildford, UK",
        ),
    ],
)
def test_parse_jobposting_location_details_rejects_noise_locality(
    location_entry: dict[str, str],
    expected_city: str,
    expected_country: str,
    expected_summary: str,
) -> None:
    details = parse_jobposting_location_details(location_entry)
    assert details["city"] == expected_city
    assert details["country"] == expected_country
    assert details["locationSummary"] == expected_summary


def test_parse_jobposting_location_details_rebuilds_summary_from_surviving_entries() -> None:
    details = parse_jobposting_location_details(
        [
            {"addressLocality": "Apr. 06", "addressCountry": "Unknown"},
            {"addressLocality": "Guildford", "addressCountry": "UK"},
        ]
    )
    assert details["city"] == "Guildford"
    assert details["country"] == "UK"
    assert details["locations"] == [{"city": "Guildford", "country": "UK"}]
    assert details["locationSummary"] == "Guildford, UK"


def test_parse_jobposting_location_details_deduplicates_variants_and_drops_role_bleed() -> None:
    details = parse_jobposting_location_details(
        {
            "addressLocality": "Artiste technique | Montréal, CA",
            "addressCountry": "CA",
        }
    )
    assert details["city"] == "Montréal"
    assert details["country"] == "CA"
    assert details["locations"] == [{"city": "Montréal", "country": "CA"}]
    assert details["locationSummary"] == "Montréal, CA"
