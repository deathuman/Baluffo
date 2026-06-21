"""Tests for static source execution fixture and zero-extract behavior."""

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

        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        assert len(rows) == 2
        assert any("/job/" in (row.get("jobLink") or "") for row in rows)
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_static_source_rejects_regular_pages_as_dead_listing_pages() -> None:
    listing_html = """
        <html>
          <body>
            <a href="/jobs/about">Senior Engineer</a>
          </body>
        </html>
    """
    detail_html = """
        <html>
          <head><title>About</title></head>
          <body><h1>About</h1><p>About us</p></body>
        </html>
    """
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Example Studio (Manual Website)",
            "studio": "Example Studio",
            "adapter": "static",
            "company": "Example Studio",
            "pages": ["https://example.com/careers"],
            "enabledByDefault": True,
        }
    ]

    try:

        def fake_fetch(url: str, _timeout: int) -> str:
            if url == "https://example.com/careers":
                return listing_html
            if url == "https://example.com/jobs/about":
                return detail_html
            raise RuntimeError(f"Unexpected URL: {url}")

        jf.SOURCE_DIAGNOSTICS.clear()
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )
        assert rows == []
        detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[
            0
        ]
        assert str(detail.get("classification") or "") == "dead_listing_page"
        assert int(detail.get("deadListingPageCount") or 0) == 1
        examples = detail.get("deadListingPageExamples")
        assert isinstance(examples, list) and examples
        normalized = jf.normalize_source_report_row(detail)
        assert str(normalized.get("classification") or "") == "dead_listing_page"
        assert int(normalized.get("deadListingPageCount") or 0) == 1
        assert isinstance(normalized.get("deadListingPageExamples"), list)
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_keeps_scanning_after_repeated_dead_detail_pages() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Dead Detail Studio",
            "studio": "Dead Detail Studio",
            "adapter": "static",
            "company": "Dead Detail Studio",
            "pages": ["https://example.net/careers"],
            "enabledByDefault": True,
        }
    ]
    listing_html = (
        "<html><body>"
        '<a href="/job/1"><span></span></a>'
        '<a href="/job/2"><span></span></a>'
        '<a href="/job/3"><span></span></a>'
        '<a href="/job/4"><span></span></a>'
        "</body></html>"
    )
    detail_fetches: list[str] = []

    try:

        def fake_fetch(url: str, _timeout: int) -> str:
            if url == "https://example.net/careers":
                return listing_html
            if url.startswith("https://example.net/job/"):
                detail_fetches.append(url)
                return "<html><head><title>About</title></head><body><p>About us</p></body></html>"
            raise RuntimeError(f"Unexpected URL: {url}")

        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            static_detail_concurrency=1,
        )
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev

    assert rows == []
    assert sorted(detail_fetches) == [
        "https://example.net/job/1",
        "https://example.net/job/2",
        "https://example.net/job/3",
        "https://example.net/job/4",
    ]


def test_run_static_studio_pages_source_empty_detail_batches_do_not_stop_remaining_candidates() -> (
    None
):
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Adaptive Detail Studio",
            "studio": "Adaptive Detail Studio",
            "adapter": "static",
            "company": "Adaptive Detail Studio",
            "pages": ["https://example.net/careers"],
            "enabledByDefault": True,
        }
    ]
    listing_html = (
        "<html><body>"
        '<a href="/job/1"><span></span></a>'
        '<a href="/job/2"><span></span></a>'
        '<a href="/job/3"><span></span></a>'
        '<a href="/job/4"><span></span></a>'
        '<a href="/job/5"><span></span></a>'
        '<a href="/job/6"><span></span></a>'
        "</body></html>"
    )
    detail_fetches: list[str] = []

    try:

        def fake_fetch(url: str, _timeout: int) -> str:
            if url == "https://example.net/careers":
                return listing_html
            if url.startswith("https://example.net/job/"):
                detail_fetches.append(url)
                return "<html><head><title>About</title></head><body><p>About us</p></body></html>"
            raise RuntimeError(f"Unexpected URL: {url}")

        jf.SOURCE_DIAGNOSTICS.clear()
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            static_detail_concurrency=3,
        )
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev

    assert rows == []
    assert sorted(detail_fetches) == [
        "https://example.net/job/1",
        "https://example.net/job/2",
        "https://example.net/job/3",
        "https://example.net/job/4",
        "https://example.net/job/5",
        "https://example.net/job/6",
    ]
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    stats = detail.get("stats") if isinstance(detail.get("stats"), dict) else {}
    assert int(stats.get("detail_batch_count") or 0) == 2
    assert int(stats.get("detail_pages_skipped_by_adaptive_stop") or 0) == 0


def test_run_static_studio_pages_source_listing_rows_do_not_cap_residual_detail_batches() -> None:
    source_row = {
        "name": "Listing Wins Studio",
        "studio": "Listing Wins Studio",
        "adapter": "static",
        "company": "Listing Wins Studio",
        "pages": ["https://example.net/careers"],
        "enabledByDefault": True,
    }
    listing_html = """
        <html>
          <head>
            <script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Platform Engineer",
            "hiringOrganization":{"name":"Listing Wins Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/platform-engineer"}
            </script>
          </head>
          <body>
            <a href="/job/a">Role A</a>
            <a href="/job/b">Role B</a>
            <a href="/job/c">Role C</a>
            <a href="/job/d">Role D</a>
          </body>
        </html>
    """
    detail_fetches: list[str] = []

    def fake_fetch(url: str, _timeout: int) -> str:
        if url == "https://example.net/careers":
            return listing_html
        if url.startswith("https://example.net/job/"):
            detail_fetches.append(url)
            title = url.rsplit("/", 1)[-1].upper()
            return f"<html><body><h1>{title} Engineer</h1></body></html>"
        raise RuntimeError(f"Unexpected URL: {url}")

    jf.SOURCE_DIAGNOSTICS.clear()
    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[source_row],
        static_detail_concurrency=6,
    )

    assert len(rows) == 5
    assert sorted(detail_fetches) == [
        "https://example.net/job/a",
        "https://example.net/job/b",
        "https://example.net/job/c",
        "https://example.net/job/d",
    ]
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    stats = detail.get("stats") if isinstance(detail.get("stats"), dict) else {}
    assert int(stats.get("detail_batch_count") or 0) == 1


def test_static_zero_extract_generic_path_falls_back_to_needs_review() -> None:
    detail = {
        "adapter": "static",
        "studio": "Capcom",
        "name": "Capcom Careers",
        "status": "error",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": "no jobs extracted from source pages",
        "classification": "",
        "browserFallbackRecommended": False,
        "signalQuality": "strong",
        "stats": {
            "candidate_links_found": 0,
            "detail_pages_visited": 0,
            "jobs_emitted": 0,
            "jobs_rejected_validation": 0,
        },
    }

    updated = static_helpers.update_source_detail_taxonomy(detail)

    assert str(updated.get("classification") or "") == "needs_review"
    assert str(updated.get("failureBucket") or "") == "needs_review"
    assert str(updated.get("zeroKeptClassification") or "") == "needs_review"


def test_static_zero_extract_linkedin_429_promotes_to_anti_bot_or_challenge() -> None:
    detail = {
        "adapter": "static",
        "studio": "Nexus Studios",
        "name": "Nexus Studios Careers",
        "status": "error",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": "HTTP 429 Too Many Requests for https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search",
        "classification": "rate_limited",
        "browserFallbackRecommended": False,
        "signalQuality": "strong",
        "stats": {
            "candidate_links_found": 0,
            "detail_pages_visited": 0,
            "jobs_emitted": 0,
            "jobs_rejected_validation": 0,
        },
    }

    updated = static_helpers.update_source_detail_taxonomy(detail)

    assert str(updated.get("classification") or "") == "anti_bot_or_challenge"
    assert str(updated.get("failureBucket") or "") == "anti_bot_or_challenge"
    assert str(updated.get("zeroKeptClassification") or "") == "broken_extraction"
    assert bool(updated.get("browserFallbackRecommended"))
