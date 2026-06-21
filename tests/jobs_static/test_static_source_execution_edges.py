"""Tests for static source execution edge behavior."""

# ruff: noqa: F401
import json
import subprocess
import time
from unittest import mock

import pytest

from src.exceptions import AdapterValidationError
from src.jobs.browser_fallback import BrowserFallbackCircuitBreaker
from src.jobs.common.http import HttpStatusError
from tests.helpers.concurrency import BlockingActiveCounter

from ._helpers import (
    FIXTURES_DIR,
    AdapterPluginContext,
    Counter,
    GenericCareersSpider,
    HtmlResponse,
    Path,
    Request,
    _fixture,
    _looks_like_location_cell,
    _parse_structured_locations,
    _read_fixture,
    ats_wrappers,
    build_city_garbage_report,
    build_contamination_report,
    build_location_quality_report,
    build_public_text_quality_report,
    classify_job_page,
    default_registry,
    ensure_provider_plugins,
    extract_rendered_card_jobs,
    frontier,
    hashlib,
    jf,
    jfr,
    jobs_canonicalize,
    jobs_common_config,
    jobs_common_registry,
    jobs_dedup,
    jobs_registry,
    jobs_reporting,
    kojima,
    process_detail_link,
    rendered_cards,
    scrapy_runner,
    sheet_studios,
    source_detail_limit_for,
    source_detail_retries_for,
    static_helpers,
    static_scrapy,
    workspace_tmpdir,
)


def test_run_static_studio_pages_source_follows_safe_listing_redirect() -> None:
    source_row = {
        "name": "Redirect Listing Studio",
        "studio": "Redirect Listing Studio",
        "adapter": "static",
        "company": "Redirect Listing Studio",
        "pages": ["https://example.net/careers"],
        "enabledByDefault": True,
    }
    fetched_urls: list[str] = []

    def fake_fetch(url: str, _timeout: int) -> str:
        fetched_urls.append(url)
        if url == "https://example.net/careers":
            raise HttpStatusError(301, url, location="/jobs")
        if url == "https://example.net/jobs":
            return """
                <html><head><script type="application/ld+json">
                {"@context":"https://schema.org","@type":"JobPosting","title":"Redirect Role",
                "hiringOrganization":{"name":"Redirect Listing Studio"},
                "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
                "url":"https://example.net/jobs/redirect-role"}
                </script></head><body></body></html>
            """
        raise RuntimeError(f"Unexpected URL: {url}")

    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[source_row],
    )

    assert len(rows) == 1
    assert fetched_urls == ["https://example.net/careers", "https://example.net/jobs"]


def test_run_static_studio_pages_source_rejects_obvious_off_target_detail_links() -> None:
    source_row = {
        "name": "Off Target Studio",
        "studio": "Off Target Studio",
        "adapter": "static",
        "company": "Off Target Studio",
        "pages": ["https://example.net/careers"],
        "enabledByDefault": True,
    }
    listing_html = """
        <html>
          <head>
            <script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Platform Engineer",
            "hiringOrganization":{"name":"Off Target Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/platform-engineer"}
            </script>
          </head>
          <body>
            <a href="https://www.youtube.com/watch?v=abc">Video</a>
            <a href="https://example.net/legal/privacy-policy">Privacy</a>
            <a href="https://forms.gle/example">Form</a>
          </body>
        </html>
    """
    fetched_urls: list[str] = []

    def fake_fetch(url: str, _timeout: int) -> str:
        fetched_urls.append(url)
        if url == "https://example.net/careers":
            return listing_html
        raise RuntimeError(f"Unexpected URL: {url}")

    jf.SOURCE_DIAGNOSTICS.clear()
    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[source_row],
    )

    assert len(rows) == 1
    assert fetched_urls == ["https://example.net/careers"]
    details = jf.SOURCE_DIAGNOSTICS.get("static_studio_pages", {}).get("details") or []
    assert details
    assert int((details[0].get("loss") or {}).get("staticNonJobUrlRejected") or 0) >= 3


def test_run_static_studio_pages_source_caps_multi_host_external_detail_fanout() -> None:
    source_row = {
        "name": "External Fanout Studio",
        "studio": "External Fanout Studio",
        "adapter": "static",
        "company": "External Fanout Studio",
        "pages": ["https://example.net/careers"],
        "enabledByDefault": True,
    }
    anchors = "\n".join(
        f'<a href="https://jobs{index}.example.com/jobs/role-{index}">Role {index}</a>'
        for index in range(12)
    )
    listing_html = f"<html><body>{anchors}</body></html>"
    fetched_urls: list[str] = []

    def fake_fetch(url: str, _timeout: int) -> str:
        fetched_urls.append(url)
        if url == "https://example.net/careers":
            return listing_html
        role = url.rstrip("/").rsplit("/", 1)[-1].replace("-", " ").title()
        return f"""
            <html><head><script type="application/ld+json">
            {{"@context":"https://schema.org","@type":"JobPosting","title":"{role}",
            "hiringOrganization":{{"name":"External Fanout Studio"}},
            "jobLocation":{{"address":{{"addressLocality":"Remote","addressCountry":"US"}}}},
            "url":"{url}"}}
            </script></head><body></body></html>
        """

    jf.SOURCE_DIAGNOSTICS.clear()
    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[source_row],
    )

    assert len(rows) == 8
    assert len(fetched_urls) == 9
    details = jf.SOURCE_DIAGNOSTICS.get("static_studio_pages", {}).get("details") or []
    assert int(((details[0].get("stats") or {}).get("external_detail_links_capped")) or 0) == 4


def test_run_static_studio_pages_source_zero_yield_listing_falls_through_to_needs_review() -> None:
    source_row = {
        "name": "Provider Zero Yield Studio",
        "studio": "Provider Zero Yield Studio",
        "adapter": "static",
        "company": "Provider Zero Yield Studio",
        "pages": ["https://jobs.workdayjobs.com/provider-zero-yield"],
        "enabledByDefault": True,
    }
    fetch_calls: list[str] = []
    jf.SOURCE_DIAGNOSTICS.clear()

    def fake_fetch(url: str, _timeout: int) -> str:
        fetch_calls.append(url)
        return "<html><body><h1>Join our team</h1><p>No openings right now.</p></body></html>"

    with pytest.raises(AdapterValidationError):
        jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=2,
            backoff_s=0,
            sources=[source_row],
        )

    assert fetch_calls == ["https://jobs.workdayjobs.com/provider-zero-yield"]
    details = jf.SOURCE_DIAGNOSTICS.get("static_studio_pages", {}).get("details") or []
    assert details
    assert str((details[0].get("stats") or {}).get("listing_terminal_reason") or "") == ""
    assert details[0].get("classification") == "site_changed"


def test_run_static_studio_pages_source_keeps_post_listing_detail_tail() -> None:
    source_row = {
        "name": "Post Listing Tail Studio",
        "studio": "Post Listing Tail Studio",
        "adapter": "static",
        "company": "Post Listing Tail Studio",
        "pages": ["https://example.net/careers"],
        "enabledByDefault": True,
    }
    listing_html = """
        <html>
          <head><script type="application/ld+json">
          {"@context":"https://schema.org","@type":"JobPosting","title":"Listed Role",
          "hiringOrganization":{"name":"Post Listing Tail Studio"},
          "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
          "url":"https://example.net/jobs/listed-role"}
          </script></head>
          <body>
            <a href="/job/a">Role A</a>
            <a href="/job/b">Role B</a>
            <a href="/job/c">Role C</a>
          </body>
        </html>
    """
    fetched_detail_urls: list[str] = []

    def fake_fetch(url: str, _timeout: int) -> str:
        if url == "https://example.net/careers":
            return listing_html
        if url.startswith("https://example.net/job/"):
            fetched_detail_urls.append(url)
            title = url.rsplit("/", 1)[-1].upper()
            return f"<html><body><h1>{title} Engineer</h1></body></html>"
        raise RuntimeError(f"Unexpected URL: {url}")

    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[source_row],
        static_detail_concurrency=1,
    )

    assert len(rows) >= 1
    assert fetched_detail_urls == [
        "https://example.net/job/a",
        "https://example.net/job/b",
        "https://example.net/job/c",
    ]


def test_run_static_studio_pages_source_records_listing_browser_fallback_terminal_reason() -> None:
    source_row = {
        "name": "Fallback Empty Listing Studio",
        "studio": "Fallback Empty Listing Studio",
        "adapter": "static",
        "company": "Fallback Empty Listing Studio",
        "pages": ["https://example.net/careers"],
        "enabledByDefault": True,
    }
    fetch_calls: list[str] = []
    playwright_calls: list[tuple[str, int]] = []
    jf.SOURCE_DIAGNOSTICS.clear()

    def fake_fetch(url: str, _timeout: int) -> str:
        fetch_calls.append(url)
        raise TimeoutError("timed out")

    def fake_try_playwright(url: str, timeout_s: int) -> tuple[str, str]:
        playwright_calls.append((url, timeout_s))
        return "", ""

    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=2,
        backoff_s=0,
        sources=[source_row],
        try_playwright=fake_try_playwright,
    )

    assert rows == []
    assert fetch_calls == [
        "https://example.net/careers",
        "https://example.net/careers",
        "https://example.net/careers",
    ]
    assert len(playwright_calls) == 1
    details = jf.SOURCE_DIAGNOSTICS.get("static_studio_pages", {}).get("details") or []
    assert details
    stats = details[0].get("stats") or {}
    assert int(stats.get("listing_browser_fallbacks") or 0) == 1
    assert stats.get("listing_terminal_reason") == "browser_fallback_empty"


def test_run_static_studio_pages_source_emits_incremental_listing_batch_progress() -> None:
    source_row = {
        "name": "Listing Progress Studio",
        "studio": "Listing Progress Studio",
        "adapter": "static",
        "company": "Listing Progress Studio",
        "pages": [
            "https://example.net/jobs/page-a",
            "https://example.net/jobs/page-b",
            "https://example.net/jobs/page-c",
        ],
        "enabledByDefault": True,
    }
    page_html = {
        "https://example.net/jobs/page-a": """
            <html><head><script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Role A",
            "hiringOrganization":{"name":"Listing Progress Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/role-a"}
            </script></head><body></body></html>
        """,
        "https://example.net/jobs/page-b": """
            <html><head><script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Role B",
            "hiringOrganization":{"name":"Listing Progress Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/role-b"}
            </script></head><body></body></html>
        """,
        "https://example.net/jobs/page-c": """
            <html><head><script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Role C",
            "hiringOrganization":{"name":"Listing Progress Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/role-c"}
            </script></head><body></body></html>
        """,
    }
    progress_events: list[dict[str, object]] = []

    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda url, _timeout: page_html[url],
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[source_row],
        progress_callback=lambda **kwargs: progress_events.append(dict(kwargs)),
    )

    assert len(rows) == 3
    assert any(
        str(event.get("phase_key") or "") == "static_listing_fetch"
        and int((event.get("counts") or {}).get("listingPagesFetched") or 0) == 1
        and str(event.get("target_label") or "") == "Listing fetch 1/3"
        for event in progress_events
    )
