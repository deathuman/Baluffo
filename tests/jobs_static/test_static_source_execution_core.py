"""Tests for static source execution core behavior."""

# ruff: noqa: F401
import json
import subprocess
import time
from typing import Any
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


def test_run_static_studio_pages_source_dedupes_candidate_links_before_fetch() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    # Use example.net so the generic listing-only fallback runs (no static plugin)
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
        "<html><body>"
        '<div class="job-listing-item"><a href="/job/engine-programmer">Engine Programmer</a></div>'
        '<a href="/job/engine-programmer">Engine Programmer</a>'
        '<script>var detail = "https://example.net/job/engine-programmer";</script>'
        "</body></html>"
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

        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        assert len(rows) == 1
        assert fetch_counts["detail"] == 0
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_emits_heartbeat_callbacks() -> None:
    listing = _fixture("littlechicken_jobs_page.html")
    detail = _fixture("littlechicken_job_detail.html")
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    heartbeat_calls: list[str] = []
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Fallback Heartbeat Studio",
            "studio": "Fallback Heartbeat Studio",
            "adapter": "static",
            "company": "Fallback Heartbeat Studio",
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
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            heartbeat_callback=lambda: heartbeat_calls.append("beat"),
        )
        assert len(rows) == 2
        assert len(heartbeat_calls) >= 4
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_emits_incremental_detail_batch_progress() -> None:
    listing = _fixture("littlechicken_jobs_page.html")
    detail = _fixture("littlechicken_job_detail.html")
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    progress_events: list[dict[str, Any]] = []
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Fallback Progress Studio",
            "studio": "Fallback Progress Studio",
            "adapter": "static",
            "company": "Fallback Progress Studio",
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
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            progress_callback=lambda **kwargs: progress_events.append(dict(kwargs)),
        )
        assert len(rows) == 2
        assert any(
            str(event.get("phase_key") or "") == "static_detail_traversal"
            and int((event.get("counts") or {}).get("detailPagesFetched") or 0) == 1
            and str(event.get("target_label") or "") == "Detail fetch 1/2"
            for event in progress_events
        )
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_flattens_slow_tail_with_history() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    source = {
        "name": "Tail Test Studio",
        "studio": "Tail Test Studio",
        "adapter": "static",
        "company": "Tail Test Studio",
        "pages": ["https://example.net/careers"],
        "enabledByDefault": True,
    }
    jf.STUDIO_SOURCE_REGISTRY = [source]
    listing_html = (
        "<html><body>"
        + "".join(
            f'<article><h2>Role {i}</h2><a href="/job/{i}">More Details</a></article>'
            for i in range(20)
        )
        + "</body></html>"
    )
    detail_html = "<html><body><h1>Role</h1></body></html>"
    detail_calls = {"count": 0}
    tail_state = {
        "Tail Test Studio": {
            "lastDetailPagesVisited": 42,
            "lastKeptCount": 1,
            "lastDurationMs": 145137,
            "lastDetailYieldPct": 2,
            "lastStageTimingsMs": {"detailFetch": 217029},
        }
    }

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://example.net/careers":
            return listing_html
        if url.startswith("https://example.net/job/"):
            detail_calls["count"] += 1
            return detail_html
        raise RuntimeError(f"Unexpected URL: {url}")

    def run_once(source_state_rows: dict[str, dict[str, Any]]) -> tuple[int, int]:
        jf.SOURCE_DIAGNOSTICS.clear()
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[source],
            diagnostics_name="Tail Test Studio",
            source_state_rows=source_state_rows,
        )
        diag = (jf.SOURCE_DIAGNOSTICS.get("Tail Test Studio") or {}).get("details") or []
        stats = (diag[0] if diag else {}).get("stats") or {}
        return int(stats.get("detail_pages_visited") or 0), len(rows)

    try:
        control_detail_pages, control_rows = run_once({})
        tail_detail_pages, tail_rows = run_once(tail_state)

        assert control_rows >= 1
        assert tail_rows >= 1
        assert control_detail_pages == 0
        assert tail_detail_pages == 0
        assert detail_calls["count"] == 0
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_force_refresh_all_reprocesses_detail_links() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Force Refresh Studio",
            "studio": "Force Refresh Studio",
            "adapter": "static",
            "company": "Force Refresh Studio",
            "pages": ["https://target.example/careers"],
            "enabledByDefault": True,
        },
        {
            "name": "Control Studio",
            "studio": "Control Studio",
            "adapter": "static",
            "company": "Control Studio",
            "pages": ["https://control.example/careers"],
            "enabledByDefault": True,
        },
    ]
    target_listing = (
        '<html><body><a href="/job/software-engineer">Software Engineer</a></body></html>'
    )
    control_listing = (
        "<html><head>"
        '<script type="application/ld+json">'
        '{"@context":"https://schema.org","@type":"JobPosting","title":"Control Role",'
        '"hiringOrganization":{"name":"Control Studio"},'
        '"jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},'
        '"url":"https://control.example/job/control-role"}'
        "</script>"
        "</head><body></body></html>"
    )
    target_fingerprint = hashlib.sha1(target_listing.encode("utf-8")).hexdigest()
    source_state_rows = {"Force Refresh Studio": {"lastListingFingerprint": target_fingerprint}}
    detail_calls = {"count": 0}

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://target.example/careers":
            return target_listing
        if url == "https://control.example/careers":
            return control_listing
        if url == "https://target.example/job/software-engineer":
            return "<html><body><h1>Software Engineer</h1></body></html>"
        raise RuntimeError(f"Unexpected URL: {url}")

    def fake_process_detail_html(**kwargs: Any) -> dict[str, Any]:
        detail_calls["count"] += 1
        return {
            "rows": [
                {
                    "sourceJobId": "static:Force Refresh Studio:target",
                    "title": "Software Engineer",
                    "company": "Force Refresh Studio",
                    "city": "",
                    "country": "Unknown",
                    "workType": "",
                    "contractType": "",
                    "jobLink": "https://target.example/job/software-engineer",
                    "sector": "Game",
                    "postedAt": "",
                    "adapter": "static",
                    "studio": "Force Refresh Studio",
                }
            ],
            "parseEmpty": False,
            "fetchMs": 0,
            "parseMs": 0,
            "cacheHit": False,
            "rejectedClassification": "",
            "rejectedExample": "",
        }

    try:
        with mock.patch(
            "src.jobs.adapters.static_listing.extract_rendered_card_jobs", return_value=[]
        ):
            with mock.patch(
                # traversal resolves detail parsing directly from the heuristics leaf,
                # so the fake must replace the traversal leaf's own binding.
                "src.jobs.adapters.static_listing_traversal.process_detail_html",
                side_effect=fake_process_detail_html,
            ):
                rows_no_refresh = jf.run_static_studio_pages_source(
                    fetch_text=fake_fetch,
                    timeout_s=5,
                    retries=0,
                    backoff_s=0,
                    sources=list(jf.STUDIO_SOURCE_REGISTRY),
                    source_state_rows=source_state_rows,
                    force_refresh_all=False,
                )
                rows_force_refresh = jf.run_static_studio_pages_source(
                    fetch_text=fake_fetch,
                    timeout_s=5,
                    retries=0,
                    backoff_s=0,
                    sources=list(jf.STUDIO_SOURCE_REGISTRY),
                    source_state_rows=source_state_rows,
                    force_refresh_all=True,
                )
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev

    assert detail_calls["count"] == 1
    assert len(rows_no_refresh) == 1
    assert len(rows_force_refresh) == 2
    assert any(
        str(row.get("jobLink") or "") == "https://target.example/job/software-engineer"
        for row in rows_force_refresh
    )


def test_run_static_studio_pages_source_parallelizes_detail_fetches() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    # Use example.net so the generic listing-only fallback runs (no static plugin)
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
        "<html><body>"
        '<a href="/job/a">Role A</a>'
        '<a href="/job/b">Role B</a>'
        '<a href="/job/c">Role C</a>'
        "</body></html>"
    )
    fetches = BlockingActiveCounter(auto_release_at=2)

    try:

        def fake_fetch(url: str, _: int) -> str:
            if url == "https://example.net/careers":
                return listing
            if url in {
                "https://example.net/job/a",
                "https://example.net/job/b",
                "https://example.net/job/c",
            }:
                fetches.enter()
                try:
                    fetches.wait_released()
                finally:
                    fetches.exit()
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
        assert fetches.peak >= 2
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_enforces_hard_budget_and_preserves_partial_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Budget Studio",
            "studio": "Budget Studio",
            "adapter": "static",
            "company": "Budget Studio",
            "pages": ["https://example.net/careers"],
            "enabledByDefault": True,
        }
    ]
    listing_html = (
        "<html><body>"
        '<a href="/job/a">Role A</a>'
        '<a href="/job/b">Role B</a>'
        '<a href="/job/c">Role C</a>'
        "</body></html>"
    )
    monkeypatch.setenv("BALUFFO_STATIC_SOURCE_TIME_BUDGET_S", "5")
    fetched_detail_urls: list[str] = []
    clock = {"now": time.perf_counter()}

    def fake_perf_counter() -> float:
        return clock["now"]

    monkeypatch.setattr(time, "perf_counter", fake_perf_counter)

    try:

        def fake_fetch(url: str, _timeout: int) -> str:
            if url == "https://example.net/careers":
                return listing_html
            if url.startswith("https://example.net/job/"):
                fetched_detail_urls.append(url)
                clock["now"] += 2.2
                title = url.rsplit("/", 1)[-1].upper()
                return f"<html><body><h1>{title} Engineer</h1></body></html>"
            raise RuntimeError(f"Unexpected URL: {url}")

        started = time.perf_counter()
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            static_detail_concurrency=1,
        )
        elapsed = time.perf_counter() - started
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev

    assert len(rows) == 2
    assert fetched_detail_urls == [
        "https://example.net/job/a",
        "https://example.net/job/b",
    ]
    assert elapsed < 5.6


def test_run_static_studio_pages_source_parallelizes_listing_fetches() -> None:
    source_row = {
        "name": "Parallel Listing Studio",
        "studio": "Parallel Listing Studio",
        "adapter": "static",
        "company": "Parallel Listing Studio",
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
            "hiringOrganization":{"name":"Parallel Listing Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/role-a"}
            </script></head><body></body></html>
        """,
        "https://example.net/jobs/page-b": """
            <html><head><script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Role B",
            "hiringOrganization":{"name":"Parallel Listing Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/role-b"}
            </script></head><body></body></html>
        """,
        "https://example.net/jobs/page-c": """
            <html><head><script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Role C",
            "hiringOrganization":{"name":"Parallel Listing Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/role-c"}
            </script></head><body></body></html>
        """,
    }
    fetches = BlockingActiveCounter(auto_release_at=2)

    def fake_fetch(url: str, _: int) -> str:
        fetches.enter()
        try:
            fetches.wait_released()
            return page_html[url]
        finally:
            fetches.exit()

    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[source_row],
    )

    assert len(rows) == 3
    assert fetches.peak >= 2


def test_run_static_studio_pages_source_uses_async_listing_fetch_when_provided() -> None:
    source_row = {
        "name": "Async Listing Studio",
        "studio": "Async Listing Studio",
        "adapter": "static",
        "company": "Async Listing Studio",
        "pages": ["https://example.net/jobs"],
        "enabledByDefault": True,
    }
    async_calls: list[tuple[str, int]] = []

    async def fake_listing_async_fetch(
        _client: object, _job: dict[str, Any], url: str, timeout_s: int
    ) -> str:
        async_calls.append((url, timeout_s))
        return """
            <html><head><script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Async Role",
            "hiringOrganization":{"name":"Async Listing Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/async-role"}
            </script></head><body></body></html>
        """

    def failing_sync_fetch(url: str, _timeout: int) -> str:
        raise RuntimeError(f"unexpected sync fetch: {url}")

    rows = jf.run_static_studio_pages_source(
        fetch_text=failing_sync_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[source_row],
        listing_async_fetch=fake_listing_async_fetch,
    )

    assert len(rows) == 1
    assert async_calls == [("https://example.net/jobs", 5)]
