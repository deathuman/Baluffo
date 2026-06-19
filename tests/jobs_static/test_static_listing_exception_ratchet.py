from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.jobs.adapters import static_listing
from src.jobs.adapters.static_runtime import StaticRunDeps, StaticSourceContext
from src.jobs.adapters.static_runtime_support import (
    StaticHtmlFetcher,
    StaticSourceRuntimeConfig,
    build_static_entry_report,
)


def _make_static_context(
    *,
    pages: list[str] | None = None,
    listing_async_fetch: Any = None,
    try_playwright: Any = None,
) -> StaticSourceContext:
    source_name = "Static Ratchet Studio"
    source = {
        "name": source_name,
        "company": source_name,
        "pages": pages if pages is not None else ["https://example.com/careers"],
    }
    run_deps = StaticRunDeps(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=0,
        backoff_s=0,
        listing_async_fetch=listing_async_fetch,
        try_playwright=try_playwright,
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


def test_static_plugin_context_ignores_invalid_url_parse_input() -> None:
    ctx = _make_static_context(pages=["http://[broken"])

    plugin_context = static_listing._static_plugin_context(ctx)

    assert plugin_context is not None
    assert plugin_context.source_identity == "Static Ratchet Studio"


def test_empty_plugin_probe_swallows_expected_fetch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _make_static_context()

    def fetch_html_cached(*_args: Any, **_kwargs: Any) -> tuple[str, bool]:
        raise RuntimeError("HTTP 500")

    monkeypatch.setattr(ctx.html_fetcher, "fetch_html_cached", fetch_html_cached)

    assert static_listing._probe_empty_plugin_listing(ctx, "needs_probe") == "needs_probe"


def test_empty_plugin_probe_does_not_swallow_unexpected_runtime_bug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _make_static_context()

    def fetch_html_cached(*_args: Any, **_kwargs: Any) -> tuple[str, bool]:
        raise RuntimeError("unexpected probe bug")

    monkeypatch.setattr(ctx.html_fetcher, "fetch_html_cached", fetch_html_cached)

    with pytest.raises(RuntimeError, match="unexpected probe bug"):
        static_listing._probe_empty_plugin_listing(ctx, "needs_probe")


def test_plugin_artifact_repair_swallows_expected_fetch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _make_static_context()

    def process_detail_link(**_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("HTTP 500")

    monkeypatch.setattr(static_listing, "process_detail_link", process_detail_link)

    result = static_listing._plugin_static_artifact_detail_result(
        ctx,
        detail="https://example.com/jobs/1",
        title="Producer",
        source_budget_s=30,
    )

    assert result == {}
    assert ctx.warnings == [
        "static:Static Ratchet Studio:https://example.com/jobs/1: artifact repair failed: HTTP 500"
    ]


def test_plugin_artifact_repair_does_not_swallow_unexpected_runtime_bug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _make_static_context()

    def process_detail_link(**_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("unexpected artifact bug")

    monkeypatch.setattr(static_listing, "process_detail_link", process_detail_link)

    with pytest.raises(RuntimeError, match="unexpected artifact bug"):
        static_listing._plugin_static_artifact_detail_result(
            ctx,
            detail="https://example.com/jobs/1",
            title="Producer",
            source_budget_s=30,
        )


def test_dynamic_listing_fetch_swallows_expected_fetch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _make_static_context()
    runner = static_listing.StaticFetchRunner(ctx)

    def maybe_fetch_kojima_job_listing_html(**_kwargs: Any) -> str:
        raise RuntimeError("HTTP 500")

    monkeypatch.setattr(
        static_listing,
        "maybe_fetch_kojima_job_listing_html",
        maybe_fetch_kojima_job_listing_html,
    )

    result = runner._prepare_listing_htmls(
        "https://example.com/careers",
        {"text": "<html><body></body></html>"},
    )

    assert result == ["<html><body></body></html>"]
    assert ctx.errors == [
        "static:Static Ratchet Studio:https://example.com/careers: "
        "dynamic-listing-fetch failed: HTTP 500"
    ]


def test_dynamic_listing_fetch_does_not_swallow_unexpected_runtime_bug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _make_static_context()
    runner = static_listing.StaticFetchRunner(ctx)

    def maybe_fetch_kojima_job_listing_html(**_kwargs: Any) -> str:
        raise RuntimeError("unexpected dynamic listing bug")

    monkeypatch.setattr(
        static_listing,
        "maybe_fetch_kojima_job_listing_html",
        maybe_fetch_kojima_job_listing_html,
    )

    with pytest.raises(RuntimeError, match="unexpected dynamic listing bug"):
        runner._prepare_listing_htmls(
            "https://example.com/careers",
            {"text": "<html><body></body></html>"},
        )


def test_sync_listing_fetch_uses_browser_fallback_for_expected_fetch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _make_static_context(
        try_playwright=lambda _url, _timeout: ("<html>browser listing</html>", "")
    )
    runner = static_listing.StaticFetchRunner(ctx)

    def fetch_listing_html_sync(*_args: Any, **_kwargs: Any) -> str:
        raise TimeoutError("timed out")

    monkeypatch.setattr(runner, "_fetch_listing_html_sync", fetch_listing_html_sync)

    html = runner._fetch_listing_job({}, "https://example.com/careers", 5)

    assert html == "<html>browser listing</html>"
    meta = runner.stage_state.batch_meta["https://example.com/careers"]
    assert meta["browserFallbackUsed"] is True
    assert meta["browserFallbackError"] == ""


def test_sync_listing_fetch_does_not_swallow_unexpected_runtime_bug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _make_static_context()
    runner = static_listing.StaticFetchRunner(ctx)

    def fetch_listing_html_sync(*_args: Any, **_kwargs: Any) -> str:
        raise RuntimeError("unexpected listing fetch bug")

    monkeypatch.setattr(runner, "_fetch_listing_html_sync", fetch_listing_html_sync)

    with pytest.raises(RuntimeError, match="unexpected listing fetch bug"):
        runner._fetch_listing_job({}, "https://example.com/careers", 5)


def test_async_listing_fetch_uses_browser_fallback_for_expected_fetch_failure() -> None:
    async def listing_async_fetch(*_args: Any, **_kwargs: Any) -> str:
        raise TimeoutError("timed out")

    ctx = _make_static_context(
        listing_async_fetch=listing_async_fetch,
        try_playwright=lambda _url, _timeout: ("<html>async browser listing</html>", ""),
    )
    runner = static_listing.StaticFetchRunner(ctx)

    html = asyncio.run(
        runner._fetch_listing_job_async(object(), {}, "https://example.com/careers", 5)
    )

    assert html == "<html>async browser listing</html>"
    meta = runner.stage_state.batch_meta["https://example.com/careers"]
    assert meta["browserFallbackUsed"] is True
    assert meta["browserFallbackError"] == ""


def test_async_listing_fetch_does_not_swallow_unexpected_runtime_bug() -> None:
    async def listing_async_fetch(*_args: Any, **_kwargs: Any) -> str:
        raise RuntimeError("unexpected async listing bug")

    ctx = _make_static_context(listing_async_fetch=listing_async_fetch)
    runner = static_listing.StaticFetchRunner(ctx)

    with pytest.raises(RuntimeError, match="unexpected async listing bug"):
        asyncio.run(runner._fetch_listing_job_async(object(), {}, "https://example.com/careers", 5))


def test_listing_result_records_expected_processing_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _make_static_context()
    runner = static_listing.StaticFetchRunner(ctx)

    def prepare_listing_htmls(*_args: Any, **_kwargs: Any) -> list[str]:
        raise RuntimeError("HTTP 500")

    monkeypatch.setattr(runner, "_prepare_listing_htmls", prepare_listing_htmls)

    runner._process_listing_result(
        {
            "ok": True,
            "url": "https://example.com/careers",
            "payload": {
                "domainProfile": {},
                "sourceBudgetS": 30,
            },
            "text": "<html></html>",
        }
    )

    assert ctx.errors == ["static:Static Ratchet Studio:https://example.com/careers: HTTP 500"]


def test_listing_result_does_not_swallow_unexpected_processing_bug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _make_static_context()
    runner = static_listing.StaticFetchRunner(ctx)

    def prepare_listing_htmls(*_args: Any, **_kwargs: Any) -> list[str]:
        raise RuntimeError("unexpected listing processing bug")

    monkeypatch.setattr(runner, "_prepare_listing_htmls", prepare_listing_htmls)

    with pytest.raises(RuntimeError, match="unexpected listing processing bug"):
        runner._process_listing_result(
            {
                "ok": True,
                "url": "https://example.com/careers",
                "payload": {
                    "domainProfile": {},
                    "sourceBudgetS": 30,
                },
                "text": "<html></html>",
            }
        )
