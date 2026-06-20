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
    progress_events: list[dict[str, object]] = []
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
            time.sleep(0.01)
            return detail_html
        raise RuntimeError(f"Unexpected URL: {url}")

    def run_once(source_state_rows: dict[str, dict[str, object]]) -> tuple[float, int, int]:
        jf.SOURCE_DIAGNOSTICS.clear()
        start = time.perf_counter()
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[source],
            diagnostics_name="Tail Test Studio",
            source_state_rows=source_state_rows,
        )
        elapsed = time.perf_counter() - start
        diag = (jf.SOURCE_DIAGNOSTICS.get("Tail Test Studio") or {}).get("details") or []
        stats = (diag[0] if diag else {}).get("stats") or {}
        return elapsed, int(stats.get("detail_pages_visited") or 0), len(rows)

    try:
        control_elapsed, control_detail_pages, control_rows = run_once({})
        tail_elapsed, tail_detail_pages, tail_rows = run_once(tail_state)

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

    def fake_process_detail_html(**kwargs: object) -> dict[str, object]:
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
                "src.jobs.adapters.static_listing.process_detail_html",
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

    try:

        def fake_fetch(url: str, _timeout: int) -> str:
            if url == "https://example.net/careers":
                return listing_html
            if url.startswith("https://example.net/job/"):
                fetched_detail_urls.append(url)
                time.sleep(2.2)
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
        _client: object, _job: dict[str, object], url: str, timeout_s: int
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
            time.sleep(1.2)
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
