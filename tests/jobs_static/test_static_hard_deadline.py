"""The static source deadline must reach the actual fetch, not just its loop guards."""

from __future__ import annotations

from typing import Any

from src.jobs.adapters import static_runtime_support
from src.jobs.adapters.static_runtime import StaticRunDeps, build_static_source_context
from src.jobs.adapters.static_runtime_support import (
    StaticHtmlFetcher,
    StaticSourceRuntimeConfig,
)


def _runtime_config() -> StaticSourceRuntimeConfig:
    return StaticSourceRuntimeConfig(
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


def test_source_context_attaches_and_refreshes_the_fetcher_deadline() -> None:
    fetcher = StaticHtmlFetcher(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=0,
        backoff_s=0,
    )
    run_deps = StaticRunDeps(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=0,
        backoff_s=0,
    )
    ctx = build_static_source_context(
        run_deps=run_deps,
        runtime_config=_runtime_config(),
        html_fetcher=fetcher,
        source={
            "name": "Deadline Studio",
            "company": "Deadline Studio",
            "pages": ["https://example.com/careers"],
        },
        selected_source_count=1,
        jobs=[],
        warnings=[],
        errors=[],
        details=[],
    )

    assert fetcher._source_deadline_monotonic == ctx.source_deadline
    before = fetcher._source_deadline_monotonic
    ctx.extend_source_deadline_for_pagination(pages_discovered=1)
    assert fetcher._source_deadline_monotonic == ctx.source_deadline
    assert fetcher._source_deadline_monotonic != before


def test_fetch_html_cached_passes_the_source_deadline_into_retries(monkeypatch) -> None:
    fetcher = StaticHtmlFetcher(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=0,
        backoff_s=0,
    )
    deadline = 1_000.0
    fetcher.set_source_deadline(deadline)
    captured: dict[str, Any] = {}

    def fake_fetch_with_retries(*_args: Any, **kwargs: Any) -> str:
        captured.update(kwargs)
        return "<html><body>listing</body></html>"

    monkeypatch.setattr(static_runtime_support, "fetch_with_retries", fake_fetch_with_retries)

    text, was_cached = fetcher.fetch_html_cached("https://example.com/careers")

    assert text == "<html><body>listing</body></html>"
    assert was_cached is False
    assert captured["deadline_monotonic"] == deadline
