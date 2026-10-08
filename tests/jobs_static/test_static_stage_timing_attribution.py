"""Stage-timing attribution for the static listing lane.

2026-10-07: the terminal report showed candidateExtraction 24.8M ms against
listingFetch 3.1M ms, which reads as "parsing costs 8x the network". Parsing is
sub-millisecond in practice; the 24.8M ms was the inline per-card detail fetches
that `_extract_listing_candidates` reaches through `_append_rendered_card_rows`
-> `_fetch_rendered_detail_rows`, timed inside the extraction window while also
being booked to `detail_fetch_ms`.

These tests pin the corrected attribution: extraction excludes the inline detail
fetch time, the two buckets stay disjoint, and no wall-clock or fetch behaviour
changes.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from src.jobs.adapters import static_listing, static_listing_plugin, static_zero_kept_guard
from src.jobs.adapters.static_runtime import StaticRunDeps, StaticSourceContext
from src.jobs.adapters.static_runtime_support import (
    StaticHtmlFetcher,
    StaticSourceRuntimeConfig,
    build_static_entry_report,
)


def _make_ctx(
    *,
    pages: list[str] | None = None,
    fetch_text: Any = None,
    fetch_sleep_ms: int = 0,
) -> StaticSourceContext:
    source_name = "Stage Timing Studio"
    source = {
        "name": source_name,
        "company": source_name,
        "pages": pages if pages is not None else ["https://example.com/careers"],
    }
    if fetch_text is None:

        def fetch_text(url: str, timeout: int) -> str:
            if fetch_sleep_ms:
                time.sleep(fetch_sleep_ms / 1000.0)
            return "<html><body>listing</body></html>"

    run_deps = StaticRunDeps(
        fetch_text=fetch_text,
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


class _FakePlugin:
    """Minimal plugin stand-in that spends wall clock on fetch and on parse.

    Mirrors the real shape the instrumentation targets: the plugin owns its network
    calls (through `fetch_text` / `fetch_html_cached`) and its parsing, and books
    neither on its own.
    """

    def __init__(self, *, fetch_ms: int, parse_ms: int, raises: bool = False) -> None:
        self._fetch_ms = fetch_ms
        self._parse_ms = parse_ms
        self._raises = raises

    def run(self, *, fetch_text, pages, **_kwargs) -> list[dict[str, Any]]:
        html = fetch_text(pages[0], 15)
        if self._raises:
            raise RuntimeError("plugin network failure")
        time.sleep(self._parse_ms / 1000.0)
        return [{"title": f"row from {html}", "jobLink": f"{pages[0]}/job/1"}]


def _patch_rendered_card_lane(
    monkeypatch: pytest.MonkeyPatch,
    *,
    detail_ms: int,
    job_count: int,
) -> None:
    """Replace the rendered-card lane so it books a known detail_fetch_ms cost.

    This is the shape that produced the misattribution: the lane performs a
    detail fetch (booking detail_fetch_ms) and returns inside the extraction
    window that the runner times.

    The simulated fetch burns wall clock proportional to `detail_ms`, because the
    defect is precisely that detail-fetch wall time lands inside the extraction
    timer. A counter-only stub would let the unfixed code pass by accident: the
    counters alone cannot detect the misattribution, only the clock can.

    Callers pick `detail_ms` small enough to keep the suite fast while still
    exceeding the extraction budget asserted alongside it.
    """
    runner_module = static_listing.StaticFetchRunner.__module__

    def fake_extract_listing_candidates(ctx: StaticSourceContext, **_kwargs: Any) -> tuple:
        ctx.stats["detail_fetch_ms"] = int(ctx.stats.get("detail_fetch_ms") or 0) + detail_ms
        ctx.stats["candidate_links_found"] = (
            int(ctx.stats.get("candidate_links_found") or 0) + job_count
        )
        time.sleep(detail_ms / 1000.0)
        return job_count, [], 0

    import importlib

    module = importlib.import_module(runner_module)
    monkeypatch.setattr(module, "_extract_listing_candidates", fake_extract_listing_candidates)


def test_candidate_extraction_excludes_inline_detail_fetch_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _make_ctx()
    runner = static_listing.StaticFetchRunner(ctx)
    _patch_rendered_card_lane(monkeypatch, detail_ms=300, job_count=3)

    detail_links, listing_jobs_found, _provisional = runner._extract_listing_page(
        "https://example.com/careers",
        30,
        ["<html><body>listing</body></html>"],
    )

    assert detail_links == []
    assert listing_jobs_found == 3
    # The inline detail fetch is booked to detail_fetch_ms ...
    assert int(ctx.stats.get("detail_fetch_ms") or 0) == 300
    # ... and is NOT also counted as extraction. The lane's own work is
    # sub-millisecond, so a correct attribution lands far below the 300ms the
    # simulated detail fetch burned.
    extraction_ms = int(ctx.stats.get("candidate_extraction_ms") or 0)
    assert extraction_ms < 150, (
        "inline detail fetch time leaked into candidate_extraction_ms; "
        f"got {extraction_ms}ms against a 300ms inline detail fetch"
    )


def test_candidate_extraction_floors_at_zero_when_detail_exceeds_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cache-served detail reports 0 fetchMs while the wall clock still moved."""
    ctx = _make_ctx()
    runner = static_listing.StaticFetchRunner(ctx)
    # The reported fetchMs exceeds the wall window (cache-served details report
    # 0, and the subtraction can outrun a coarse clock). Extraction must floor
    # at zero rather than go negative and corrupt the stage totals.
    _patch_rendered_card_lane(monkeypatch, detail_ms=250, job_count=1)

    runner._extract_listing_page(
        "https://example.com/careers",
        30,
        ["<html><body>listing</body></html>"],
    )

    assert int(ctx.stats.get("candidate_extraction_ms") or 0) == 0
    assert int(ctx.stats.get("detail_fetch_ms") or 0) == 250


def test_listing_prepare_is_booked_for_pre_extraction_work() -> None:
    """`_prepare_listing_htmls` reaches three playwright-fallback passes; book them.

    Those passes re-parse the listing and may drive a render, and none of it
    reached listingFetch / candidateExtraction / detailFetch — which is how 46% of
    static source time went unbooked in the 2026-10-07 replay.
    """
    ctx = _make_ctx()
    runner = static_listing.StaticFetchRunner(ctx)

    def slow_prepare(*_args: Any, **_kwargs: Any) -> list[str]:
        time.sleep(0.05)
        return ["<html><body>listing</body></html>"]

    runner._prepare_listing_htmls_timed = slow_prepare  # type: ignore[method-assign]

    runner._prepare_listing_htmls(
        "https://example.com/careers",
        {"text": "<html><body>listing</body></html>", "url": "https://example.com/careers"},
    )

    assert int(ctx.stats.get("listing_prepare_ms") or 0) > 0


def test_listing_prepare_is_booked_even_when_prepare_raises() -> None:
    ctx = _make_ctx()
    runner = static_listing.StaticFetchRunner(ctx)

    def boom(*_args: Any, **_kwargs: Any) -> list[str]:
        time.sleep(0.05)
        raise RuntimeError("prepare exploded")

    runner._prepare_listing_htmls_timed = boom  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="prepare exploded"):
        runner._prepare_listing_htmls(
            "https://example.com/careers",
            {"text": "<html></html>", "url": "https://example.com/careers"},
        )

    assert int(ctx.stats.get("listing_prepare_ms") or 0) > 0


def test_zero_kept_guard_reread_is_booked(monkeypatch: pytest.MonkeyPatch) -> None:
    """The guard's listing re-read is network work; it must reach a stage bucket."""
    ctx = _make_ctx()

    def fetch_html_cached(_url: str) -> tuple[str, bool]:
        time.sleep(0.05)
        return "<html><body>no openings here</body></html>", False

    monkeypatch.setattr(ctx.html_fetcher, "fetch_html_cached", fetch_html_cached)

    bodies = static_zero_kept_guard._fetch_listing_bodies(ctx)

    assert bodies, "expected the stub listing body to be returned"
    assert int(ctx.stats.get("listing_prepare_ms") or 0) > 0


def test_plugin_lane_books_listing_fetch_and_extraction() -> None:
    """The plugin fast path must attribute its time like the generic funnel does.

    603 of 2,316 static sources took this lane and were 85% unattributed, because
    `plugin.run()` fetched and parsed internally without booking either.
    """
    ctx = _make_ctx(fetch_sleep_ms=120)
    plugin = _FakePlugin(fetch_ms=120, parse_ms=80)

    jobs = static_listing_plugin._invoke_static_plugin(ctx, plugin, network_bookings={"ms": 0})

    assert len(jobs) == 1
    assert int(ctx.stats.get("listing_fetch_ms") or 0) >= 100, (
        f"plugin network fetch must reach listing_fetch_ms; got {ctx.stats.get('listing_fetch_ms')}"
    )


def test_plugin_lane_books_parse_remainder_as_extraction() -> None:
    ctx = _make_ctx(fetch_sleep_ms=100)
    plugin = _FakePlugin(fetch_ms=100, parse_ms=60)

    bookings: dict[str, int] = {"ms": 0}
    started = time.perf_counter()
    static_listing_plugin._invoke_static_plugin(ctx, plugin, network_bookings=bookings)
    total_ms = int((time.perf_counter() - started) * 1000)

    parse_ms = max(0, total_ms - int(bookings.get("ms") or 0))
    ctx.stats["candidate_extraction_ms"] = (
        int(ctx.stats.get("candidate_extraction_ms") or 0) + parse_ms
    )

    assert int(bookings.get("ms") or 0) >= 100
    assert int(ctx.stats.get("candidate_extraction_ms") or 0) >= 60, (
        "plugin parse time must reach candidate_extraction_ms"
    )
    # Network and parse buckets must not double-count the same milliseconds.
    assert int(ctx.stats.get("listing_fetch_ms") or 0) >= 100


def test_plugin_lane_books_network_even_when_fetch_raises() -> None:
    ctx = _make_ctx(fetch_sleep_ms=60)
    plugin = _FakePlugin(fetch_ms=0, parse_ms=0, raises=True)

    bookings: dict[str, int] = {"ms": 0}
    with pytest.raises(RuntimeError):
        static_listing_plugin._invoke_static_plugin(ctx, plugin, network_bookings=bookings)

    # A failed network call still consumed time and must be attributed.
    assert int(bookings.get("ms") or 0) > 0
    assert int(ctx.stats.get("listing_fetch_ms") or 0) > 0


def test_candidate_extraction_unchanged_when_no_detail_fetch_happens() -> None:
    ctx = _make_ctx()
    runner = static_listing.StaticFetchRunner(ctx)

    detail_links, listing_jobs_found, _provisional = runner._extract_listing_page(
        "https://example.com/careers",
        30,
        ["<html><body></body></html>"],
    )

    assert detail_links == []
    assert listing_jobs_found == 0
    assert int(ctx.stats.get("detail_fetch_ms") or 0) == 0
    assert int(ctx.stats.get("candidate_extraction_ms") or 0) >= 0


def test_extraction_and_detail_buckets_are_disjoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _make_ctx()
    runner = static_listing.StaticFetchRunner(ctx)
    _patch_rendered_card_lane(monkeypatch, detail_ms=300, job_count=2)

    started = time.perf_counter()
    runner._extract_listing_page(
        "https://example.com/careers",
        30,
        ["<html><body>listing</body></html>"],
    )
    wall_ms = int((time.perf_counter() - started) * 1000)

    extraction_ms = int(ctx.stats.get("candidate_extraction_ms") or 0)
    detail_ms = int(ctx.stats.get("detail_fetch_ms") or 0)

    # detailFetch accounts for the simulated 300ms of detail work, and the
    # extraction bucket does not re-count any of it.
    assert detail_ms == 300
    assert wall_ms >= 300
    assert extraction_ms < 150
    assert extraction_ms < detail_ms
