"""Inline detail fan-out: a concurrent prefetch that cannot change a row.

2026-10-30: the rendered-card verification lane fetched every card's detail page on the
source thread, one card at a time, so a board paid the sum of its page latencies -- 70.2%
of the whole detail cost on a full-population arm
(docs/snapshots/fetch-pipeline-detail-request-audit-2026-10-30.md). The prefetch is not a
second code path for the loop: it only pays pages the shared plan names, and the loop still
plans, emits, counts and aborts in card order. These tests pin that the rows, links,
counters, request set and failure accounting are the serial lane's.
"""

from __future__ import annotations

import urllib.request
from typing import Any

import pytest

from src.jobs.adapters import static_listing, static_listing_rows
from src.jobs.adapters.static_runtime import StaticRunDeps, StaticSourceContext
from src.jobs.adapters.static_runtime_support import (
    StaticHtmlFetcher,
    StaticSourceRuntimeConfig,
    build_static_entry_report,
)
from src.jobs.common.http import HttpStatusError
from tests.helpers.local_http_server import local_http_server

LISTING_URL = "https://studio.example/careers"
LINKS = [f"https://studio.example/jobs/{index}" for index in range(1, 5)]


def _make_static_context(*, lanes: int, fetch_text: Any = None) -> StaticSourceContext:
    source_name = "Studio (Rendered)"
    source: dict[str, Any] = {
        "name": source_name,
        "company": source_name,
        "studio": "Studio",
        "pages": [LISTING_URL],
        "id": f"static:listing_url:{LISTING_URL}",
    }
    run_deps = StaticRunDeps(
        fetch_text=fetch_text or (lambda _url, _timeout: ""),
        timeout_s=5,
        retries=0,
        backoff_s=0,
        static_detail_concurrency=lanes,
    )
    runtime_config = StaticSourceRuntimeConfig(
        static_profile="standard",
        static_detail_concurrency=lanes,
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


def _rendered_rows() -> list[dict[str, Any]]:
    # `_locationHint` without a city is the production shape that forces inline
    # verification (Mundfish), so every card plans a detail fetch.
    return [
        {"title": f"Engine Programmer {index}", "jobLink": link, "_locationHint": "Tokyo"}
        for index, link in enumerate(LINKS, start=1)
    ]


def _html(link: str) -> str:
    return f"<html><body><h1>detail for {link}</h1></body></html>"


def _run_board(
    monkeypatch: pytest.MonkeyPatch,
    *,
    fanout: bool,
    lanes: int,
    fail_url: str = "",
) -> tuple[StaticSourceContext, list[str], list[bool], BaseException | None]:
    """Run the card lane with a detail double that fetches like the real seam.

    Without a prefetched document the double goes through ``fetch_html_cached`` -- the real
    call the serial lane makes -- so both arms share one request path and the failure shapes
    stay comparable.
    """

    monkeypatch.setattr(static_listing_rows, "STATIC_INLINE_DETAIL_FANOUT_ENABLED", fanout)
    requests: list[str] = []
    separations: list[bool] = []

    def fetch_text(url: str, _timeout: int) -> str:
        requests.append(url)
        if url == fail_url:
            raise HttpStatusError(500, url)
        return _html(url)

    ctx = _make_static_context(lanes=lanes, fetch_text=fetch_text)

    def process_detail_link(**kwargs: Any) -> dict[str, Any]:
        detail = kwargs["detail"]
        detail_html = kwargs.get("prefetched_html")
        separations.append(detail_html is not None)
        if detail_html is None:
            detail_html, _cache_hit = kwargs["fetch_html_cached"](
                detail, remaining_budget_s=25.0, retries_override=0
            )
        rows = (
            [{"title": f"Role {detail}", "jobLink": detail}]
            if "detail for" in str(detail_html) and detail.rstrip("/").endswith("2")
            else []
        )
        return {
            "rows": rows,
            "junkProvenanceRowsDropped": 0,
            "nestedDetailLinks": [],
            "parseEmpty": False,
            "fetchMs": int(kwargs.get("prefetched_fetch_ms") or 11),
            "parseMs": 1,
            "cacheHit": bool(kwargs.get("prefetched_cache_hit")),
            "rejectedClassification": "needs_review",
            "rejectedExample": detail,
        }

    monkeypatch.setattr(
        static_listing, "extract_rendered_card_jobs", lambda *_a, **_k: _rendered_rows()
    )
    monkeypatch.setattr(static_listing, "process_detail_link", process_detail_link)

    failure: BaseException | None = None
    try:
        static_listing_rows._append_rendered_card_rows(
            ctx, "<html></html>", LISTING_URL, 30, [], set()
        )
    except Exception as exc:  # the caller's per-page catch turns this into the source error
        failure = exc
    return ctx, requests, separations, failure


def _shape(ctx: StaticSourceContext) -> dict[str, Any]:
    stats = dict(ctx.stats)
    for timing_key in ("detail_fetch_ms", "detail_shell_strikes"):
        stats.pop(timing_key, None)
    return {
        "jobs": [
            (row.get("title"), row.get("jobLink"), row.get("provisionalCardFallback"))
            for row in ctx.jobs
        ],
        "seen": sorted(ctx.seen_links),
        "stats": stats,
        "errors": list(ctx.errors),
    }


def test_fanout_rows_requests_and_counters_match_the_serial_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serial_ctx, serial_requests, serial_separations, serial_failure = _run_board(
        monkeypatch, fanout=False, lanes=3
    )
    fan_ctx, fan_requests, fan_separations, fan_failure = _run_board(
        monkeypatch, fanout=True, lanes=3
    )

    assert serial_failure is None and fan_failure is None
    assert _shape(fan_ctx) == _shape(serial_ctx)
    # Same pages asked for; only the order inside a chunk may differ.
    assert sorted(fan_requests) == sorted(serial_requests) == sorted(LINKS)
    # Every card found its HTML already paid for; the serial lane had none.
    assert fan_separations == [True, True, True, True]
    assert serial_separations == [False, False, False, False]


def test_fanout_failure_keeps_serial_accounting_and_abort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serial_ctx, _serial_requests, _serial_sep, serial_failure = _run_board(
        monkeypatch, fanout=False, lanes=3, fail_url=LINKS[1]
    )
    fan_ctx, _fan_requests, _fan_sep, fan_failure = _run_board(
        monkeypatch, fanout=True, lanes=3, fail_url=LINKS[1]
    )

    assert isinstance(serial_failure, HttpStatusError)
    assert isinstance(fan_failure, HttpStatusError)
    assert str(fan_failure) == str(serial_failure) == f"HTTP 500 for {LINKS[1]}"
    assert fan_ctx.stats["detail_pages_visited"] == serial_ctx.stats["detail_pages_visited"] == 2
    assert fan_ctx.stats["detail_fetch_failed"] == serial_ctx.stats["detail_fetch_failed"] == 1
    assert fan_ctx.errors == serial_ctx.errors


def test_prefetch_is_inert_when_disabled_or_single_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for fanout, lanes in ((False, 3), (True, 1)):
        monkeypatch.setattr(static_listing_rows, "STATIC_INLINE_DETAIL_FANOUT_ENABLED", fanout)
        ctx = _make_static_context(lanes=lanes)
        assert static_listing_rows._inline_detail_prefetcher(ctx, _rendered_rows()) is None


def test_lookahead_is_bounded_when_the_budget_dies_mid_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A page-wide prefetch paid for every remaining card; the window pays <= lanes - 1.

    The 2026-10-30 smoke found the page-wide shape fetching 54% more requests than the serial
    lane on a budget-bound board while visiting fewer pages. This pins the bound: after the
    card whose consumption exhausts the budget, no card beyond the next window is requested.
    """
    lanes = 3
    requests: list[str] = []
    consumed: list[str] = []

    def fetch_text(url: str, _timeout: int) -> str:
        requests.append(url)
        return _html(url)

    ctx = _make_static_context(lanes=lanes, fetch_text=fetch_text)
    cards = [
        {"title": f"Engine Programmer {index}", "jobLink": link, "_locationHint": "Tokyo"}
        for index, link in enumerate(f"https://studio.example/jobs/{i}" for i in range(1, 13))
    ]
    monkeypatch.setattr(static_listing, "extract_rendered_card_jobs", lambda *_a, **_k: cards)

    def process_detail_link(**kwargs: Any) -> dict[str, Any]:
        consumed.append(kwargs["detail"])
        if len(consumed) == 4:
            ctx.source_deadline = 0.0  # the source budget dies right after this card
        return {
            "rows": [],
            "junkProvenanceRowsDropped": 0,
            "nestedDetailLinks": [],
            "parseEmpty": False,
            "fetchMs": 1,
            "parseMs": 1,
            "cacheHit": False,
            "rejectedClassification": "needs_review",
            "rejectedExample": kwargs["detail"],
        }

    monkeypatch.setattr(static_listing, "process_detail_link", process_detail_link)
    monkeypatch.setattr(static_listing_rows, "STATIC_INLINE_DETAIL_FANOUT_ENABLED", True)

    static_listing_rows._append_rendered_card_rows(ctx, "<html></html>", LISTING_URL, 30, [], set())

    assert len(consumed) == 4
    assert len(requests) <= len(consumed) + lanes - 1
    assert len(requests) < len(cards)


DETAIL_HTML = """
    <html>
      <body>
        <h1>Lead Environment Artist</h1>
        <p>Office location:</p>
        <p>7F NTF Takebashi Building, 3-15 Kanda Nishiki-cho, Chiyoda-ku, Tokyo</p>
      </body>
    </html>
"""
EMPTY_DETAIL_HTML = "<html><body><h1>Lead Environment Artist</h1></body></html>"


def _run_real_board(
    monkeypatch: pytest.MonkeyPatch, *, fanout: bool, lanes: int, base: str
) -> tuple[StaticSourceContext, tuple[int, bool, int], list[str]]:
    """The whole lane through real sockets, the real detail parser and the real fetcher."""

    monkeypatch.setattr(static_listing_rows, "STATIC_INLINE_DETAIL_FANOUT_ENABLED", fanout)
    requests: list[str] = []

    def fetch_text(url: str, timeout: int) -> str:
        requests.append(url)
        with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310 - loopback
            body: str = response.read().decode("utf-8")
        return body

    ctx = _make_static_context(lanes=lanes, fetch_text=fetch_text)
    cards = [
        {
            "title": "Lead Environment Artist",
            "jobLink": f"{base}/jobs/{index}",
            "_locationHint": "Tokyo",
        }
        for index in range(1, 5)
    ]
    monkeypatch.setattr(static_listing, "extract_rendered_card_jobs", lambda *_a, **_k: cards)
    counts = static_listing_rows._append_rendered_card_rows(
        ctx, "<html></html>", f"{base}/careers", 30, [], set()
    )
    return ctx, counts, requests


def test_the_real_lane_emits_the_same_rows_through_real_sockets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    routes = {
        f"/jobs/{index}": (
            200,
            {"Content-Type": "text/html; charset=utf-8"},
            (DETAIL_HTML if index <= 2 else EMPTY_DETAIL_HTML).encode(),
        )
        for index in range(1, 5)
    }
    with local_http_server(routes) as base:
        serial_ctx, serial_counts, serial_requests = _run_real_board(
            monkeypatch, fanout=False, lanes=3, base=base
        )
        fan_ctx, fan_counts, fan_requests = _run_real_board(
            monkeypatch, fanout=True, lanes=3, base=base
        )

    assert _shape(fan_ctx) == _shape(serial_ctx)
    assert fan_counts == serial_counts
    assert sorted(fan_requests) == sorted(serial_requests)
    # The parsed detail rows (not the card fallbacks) are what the two arms must agree on;
    # a board whose pages all failed would prove nothing about the parser path.
    assert [row for row in serial_ctx.jobs if row.get("city") == "Tokyo"], "no parsed rows"


def test_prefetched_html_is_used_without_a_second_fetch() -> None:
    from src.jobs.adapters.static_detail_heuristics_entry import process_detail_link

    def fetch_html_cached(*_args: Any, **_kwargs: Any) -> tuple[str, bool]:
        raise AssertionError("prefetched pages must not be fetched again")

    result = process_detail_link(
        detail=LINKS[0],
        detail_title="Engine Programmer 1",
        source_started=0.0,
        static_source_time_budget_s=30,
        fetch_html_cached=fetch_html_cached,
        timeout_s=5,
        detail_retries=0,
        company="Studio",
        source_name="Studio (Rendered)",
        source={"name": "Studio (Rendered)"},
        ignored_link_titles=set(),
        prefetched_html="<html><head><title>Engine Programmer 1</title></head></html>",
        prefetched_fetch_ms=37,
    )

    assert result["fetchMs"] == 37
    assert result["cacheHit"] is False
