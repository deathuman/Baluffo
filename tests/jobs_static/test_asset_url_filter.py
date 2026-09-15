"""Asset-URL filter (2026-09-15): documents must never become listing pages
or detail candidates.

The sms.playstation.com seed row carried five webpack bundle URLs in its
``pages`` list — each was fetched every pass, detected as a JS shell, and
escalated through the browser fallback (52 dead escalations in the
2026-09-15 full pass). The raw-URL detail-candidate scan can likewise
harvest ``<script src>``/``<link href>`` links straight out of a document.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

import pytest

from src.jobs.adapters.static_detail_heuristics_filter import add_detail_link
from src.jobs.adapters.static_listing_runner import StaticFetchRunner
from src.jobs.adapters.static_listing_state import _append_detail_candidate
from src.jobs.adapters.static_runtime import StaticRunDeps, StaticSourceContext
from src.jobs.adapters.static_runtime_support import (
    StaticHtmlFetcher,
    StaticSourceRuntimeConfig,
    build_static_entry_report,
)
from src.jobs.page_gating import filter_listable_pages, looks_like_asset_url


@pytest.mark.parametrize(
    "url",
    [
        "https://sms.playstation.com/js/careers-category.3186a499.js",
        "https://example.com/assets/site.css",
        "https://example.com/bundle.min.mjs",
        "https://example.com/a/b/app.js.map",
        "https://example.com/fonts/Inter.woff2",
        "https://example.com/logo.svg",
        "https://example.com/media/trailer.mp4",
        "https://example.com/pack.tar.gz",
    ],
)
def test_asset_urls_detected(url: str) -> None:
    assert looks_like_asset_url(url) is True


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/careers",
        "https://example.com/en/j/123",
        "https://example.com/jobs/senior-engineer",
        "https://example.com/jobs?page=2",
        "https://example.com/feed.json",
        "https://example.com/sitemap.xml",
        "https://example.com/posting.pdf",
        "https://example.com/robots.txt",
        "https://example.com/",
        "",
        "not a url at all",
    ],
)
def test_document_urls_not_detected(url: str) -> None:
    assert looks_like_asset_url(url) is False


def test_filter_listable_pages_preserves_order_and_documents() -> None:
    pages = [
        "https://example.com/careers",
        "https://example.com/js/app.js",
        "",
        "https://example.com/jobs?page=2",
        "https://example.com/site.css",
    ]

    assert filter_listable_pages(pages) == [
        "https://example.com/careers",
        "",
        "https://example.com/jobs?page=2",
    ]


def _make_static_context(*, pages: list[str]) -> StaticSourceContext:
    source_name = "Static Asset Studio"
    source: dict[str, Any] = {"name": source_name, "company": source_name, "pages": pages}
    run_deps = StaticRunDeps(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=0,
        backoff_s=0,
    )
    runtime_config = StaticSourceRuntimeConfig(
        static_profile="standard",
        static_detail_concurrency=1,
        static_source_time_budget_s=30,
        low_yield_detail_cap=10,
        very_low_yield_detail_cap=3,
        uncapped_deep_static=False,
        listing_only_hosts=[],
        default_path_tokens=[],
        default_query_keys=[],
    )
    return StaticSourceContext(
        run_deps=run_deps,
        runtime_config=runtime_config,
        html_fetcher=StaticHtmlFetcher(
            fetch_text=run_deps.fetch_text,
            timeout_s=run_deps.timeout_s,
            retries=run_deps.retries,
            backoff_s=run_deps.backoff_s,
        ),
        source=source,
        source_name=source_name,
        company=source_name,
        pages=list(source["pages"]),
        entry_report=build_static_entry_report(
            source=source,
            source_name=source_name,
            pages=list(source["pages"]),
            company=source_name,
        ),
        state_entry={},
        selected_source_count=1,
        jobs=[],
        warnings=[],
        errors=[],
        details=[],
    )


def test_runner_listing_intake_filters_asset_pages() -> None:
    ctx = _make_static_context(
        pages=[
            "https://sms.playstation.com",
            "https://sms.playstation.com/js/careers-category.3186a499.js",
            "https://sms.playstation.com/js/careers-detail.4af210d9.js",
        ]
    )

    runner = StaticFetchRunner(ctx)

    assert runner.cleaned_pages == ["https://sms.playstation.com"]
    assert ctx.stats["asset_listing_pages_filtered"] == 2
    assert any("filtered 2 static-asset URL" in warning for warning in ctx.warnings)


def test_runner_listing_intake_kill_switch_restores_fetch_as_registered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.jobs.adapters import static_listing_runner

    monkeypatch.setattr(static_listing_runner, "STATIC_ASSET_URL_FILTER_ENABLED", False)
    pages = [
        "https://sms.playstation.com",
        "https://sms.playstation.com/js/careers.b0cdbeb1.js",
    ]
    ctx = _make_static_context(pages=pages)

    runner = StaticFetchRunner(ctx)

    assert runner.cleaned_pages == pages
    assert "asset_listing_pages_filtered" not in ctx.stats


def test_pagination_discovered_queue_skips_asset_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.jobs.adapters import static_listing_runner

    ctx = _make_static_context(pages=["https://example.com/careers"])
    runner = StaticFetchRunner(ctx)

    def fake_pagination_anchors_for_html(_html: str, page_url: str, *, max_pages: int) -> list[str]:
        return [
            "https://example.com/js/chunk.abc123.js",
            "https://example.com/careers?page=2",
        ][:max_pages]

    monkeypatch.setattr(
        static_listing_runner,
        "pagination_anchors_for_html",
        fake_pagination_anchors_for_html,
    )

    runner._queue_discovered_pagination_pages("https://example.com/careers", ["<html></html>"])

    assert runner.pending_listing_pages == ["https://example.com/careers?page=2"]


def test_add_detail_link_rejects_asset_url_but_accepts_real_job_url() -> None:
    detail_links: list[tuple[str, str]] = []
    detail_seen: set[str] = set()
    seen_links: set[str] = set()
    rejections: Counter[str] = Counter()

    add_detail_link(
        detail_links,
        detail_seen,
        seen_links,
        rejections,
        candidate_url="https://sms.playstation.com/js/careers-category.3186a499.js",
        anchor_text="",
        enforce_heuristics=True,
        page_url="https://sms.playstation.com/careers",
        source={},
        default_path_tokens=[],
        default_query_keys=[],
    )
    assert detail_links == []
    assert rejections["dead_listing_page"] == 1

    add_detail_link(
        detail_links,
        detail_seen,
        seen_links,
        rejections,
        candidate_url="https://example.com/en/j/123",
        anchor_text="Engineer, Tools",
        enforce_heuristics=True,
        page_url="https://example.com/en/",
        source={},
        default_path_tokens=[],
        default_query_keys=[],
    )
    assert len(detail_links) == 1


def test_add_detail_link_asset_gate_beats_probable_detail_tokens() -> None:
    """An asset URL under a registered detail path must still be rejected:
    source path tokens would otherwise classify /jobs/app.js as probable
    detail and queue a fetch that can never parse."""
    detail_links: list[tuple[str, str]] = []
    rejections: Counter[str] = Counter()

    add_detail_link(
        detail_links,
        set(),
        set(),
        rejections,
        candidate_url="https://example.com/jobs/app.js",
        anchor_text="",
        enforce_heuristics=True,
        page_url="https://example.com/careers",
        source={"detailPathTokens": ["/jobs/"]},
        default_path_tokens=[],
        default_query_keys=[],
    )
    assert detail_links == []
    assert rejections["dead_listing_page"] == 1


def test_add_detail_link_kill_switch_restores_asset_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.jobs.adapters import static_detail_heuristics_filter

    monkeypatch.setattr(static_detail_heuristics_filter, "STATIC_ASSET_URL_FILTER_ENABLED", False)
    detail_links: list[tuple[str, str]] = []

    add_detail_link(
        detail_links,
        set(),
        set(),
        Counter(),
        candidate_url="https://example.com/jobs/app.js",
        anchor_text="",
        enforce_heuristics=True,
        page_url="https://example.com/careers",
        source={"detailPathTokens": ["/jobs/"]},
        default_path_tokens=[],
        default_query_keys=[],
    )
    assert len(detail_links) == 1


def test_append_detail_candidate_rejects_asset_url() -> None:
    detail_links: list[Any] = []

    added = _append_detail_candidate(
        detail_links,
        set(),
        set(),
        candidate_url="https://sms.playstation.com/js/careers-landing.c0995026.js",
        anchor_text="",
        depth=0,
        parent_url="https://sms.playstation.com",
    )

    assert added is False
    assert detail_links == []

    added_real = _append_detail_candidate(
        detail_links,
        set(),
        set(),
        candidate_url="https://sms.playstation.com/careers",
        anchor_text="Careers",
        depth=0,
        parent_url="https://sms.playstation.com",
    )
    assert added_real is True
    assert len(detail_links) == 1
