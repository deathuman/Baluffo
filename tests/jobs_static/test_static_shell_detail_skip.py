"""Rendered-card verification lane skips JS-shell detail hosts after 2 strikes.

Bandai trace follow-up (2026-09-14): on hrmos-CDN tenants, every rendered card's
detail verification fetched a JS shell the parser can only reject (~0.65s each,
38 verifications ≈ the whole source budget), and the TimeoutError aborted the
listing batch before the queued-candidate plan could convert the 5 demoted
rows. The skip records a strike only when the fetched HTML is shell-shaped AND
parses zero rows — the hrmos *listing* page itself carries Next.js/`__NEXT_DATA__`
tokens yet is fully server-rendered, so shell shape alone must never skip.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.jobs.adapters import static_listing, static_listing_rows
from src.jobs.adapters.plugins.static._heuristics import detect_js_shell
from src.jobs.adapters.static_runtime import StaticRunDeps, StaticSourceContext
from src.jobs.adapters.static_runtime_support import (
    StaticHtmlFetcher,
    StaticSourceRuntimeConfig,
    build_static_entry_report,
)

SHELL_HTML = (
    "<html><head><script>window.__NEXT_DATA__={};</script></head>"
    '<body><div id="__next"></div></body></html>'
)
SERVER_HTML = "<html><body><h1>Engine Programmer</h1></body></html>"


def _make_static_context() -> StaticSourceContext:
    source_name = "Bandai (hrmos)"
    source: dict[str, Any] = {
        "name": source_name,
        "company": source_name,
        "studio": "Bandai Namco Studios",
        "pages": ["https://www.bandainamcostudios.com/recruit/career"],
        "id": "static:listing_url:https://www.bandainamcostudios.com/recruit/career",
    }
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
            backoff_s=0,
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


@pytest.fixture()
def shell_skip_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(static_listing_rows, "STATIC_SHELL_DETAIL_SKIP_ENABLED", True)


def _patch_process_detail_link(
    monkeypatch: pytest.MonkeyPatch,
    ctx: StaticSourceContext,
    *,
    html_by_url: dict[str, str],
    cache_hit: bool = False,
) -> list[str]:
    """Model fresh network fetches (cacheHit=False) while priming the fetch
    cache under the fetcher's normalized key so the strike classifier reads the
    same document the fetch returned — the real path's exact sequence."""
    fetched: list[str] = []

    def process_detail_link(**kwargs: Any) -> dict[str, Any]:
        detail = kwargs["detail"]
        fetched.append(detail)
        return {
            "rows": [],
            "junkProvenanceRowsDropped": 0,
            "nestedDetailLinks": [],
            "parseEmpty": False,
            "fetchMs": 12,
            "parseMs": 1,
            "cacheHit": cache_hit,
            "rejectedClassification": "needs_review",
            "rejectedExample": detail,
        }

    monkeypatch.setattr(static_listing, "parse_jobpostings_from_html", lambda *a, **k: [])
    monkeypatch.setattr(static_listing, "process_detail_link", process_detail_link)
    # Prime the fetch cache with the URL-keyed responses (the real fetch path
    # stores by normalized URL; the strike classifier reads that cache).
    for url, html in html_by_url.items():
        request = ctx.html_fetcher.build_request(url)
        assert request is not None
        with ctx.html_fetcher._fetch_cache_lock:
            ctx.html_fetcher._fetch_cache[request.normalized_url] = html
    return fetched


def test_two_shell_strikes_then_skip(monkeypatch: pytest.MonkeyPatch, shell_skip_on: None) -> None:
    """Two shell+zero verifications arm the host skip; later verifications from
    the same host return without fetching (card-row fallback still fires)."""
    ctx = _make_static_context()
    links = [f"https://cdn.example-hrmos.co/pages/t/jobs/{i}" for i in range(4)]
    html_by_url = {link: SHELL_HTML for link in links}
    fetched = _patch_process_detail_link(monkeypatch, ctx, html_by_url=html_by_url)

    for index, link in enumerate(links):
        rows = static_listing_rows._fetch_rendered_detail_rows(ctx, link, f"Role {index}", 30)
        assert rows == []
        if index < 2:
            assert fetched[-1] == link  # fetched
        else:
            assert fetched[-1] != link  # skipped, no fetch

    assert ctx.detail_shell_strikes == {"cdn.example-hrmos.co": 2}
    assert ctx.stats["detail_shell_strikes"] == 2
    assert ctx.stats["detail_shell_skipped"] == 2
    assert ctx.stats["detail_pages_visited"] == 2


def test_server_rendered_zero_rows_never_strikes(
    monkeypatch: pytest.MonkeyPatch, shell_skip_on: None
) -> None:
    """Zero rows on a server-rendered detail page is a legit-empty read, not a
    shell strike — the skip must not arm on it."""
    ctx = _make_static_context()
    link = "https://plain.example.com/careers/role-a"
    fetched = _patch_process_detail_link(monkeypatch, ctx, html_by_url={link: SERVER_HTML})

    for _ in range(3):
        rows = static_listing_rows._fetch_rendered_detail_rows(ctx, link, "Role A", 30)
        assert rows == []
        assert fetched[-1] == link

    assert ctx.detail_shell_strikes == {}
    assert "detail_shell_strikes" not in ctx.stats
    assert "detail_shell_skipped" not in ctx.stats


def test_cache_hit_does_not_strike(monkeypatch: pytest.MonkeyPatch) -> None:
    """A cache-hit detail (e.g. same URL re-verified after a listing page
    redirect chain) records no strike: the HTML is not fresh evidence of what
    the network returns, and double-counting would arm skips on thin grounds."""
    ctx = _make_static_context()
    link = "https://cdn.example-hrmos.co/pages/t/jobs/one"
    _patch_process_detail_link(monkeypatch, ctx, html_by_url={link: SHELL_HTML})
    monkeypatch.setattr(static_listing_rows, "STATIC_SHELL_DETAIL_SKIP_ENABLED", True)

    # Pre-arm the cache AND have process_detail_link report a cache hit —
    # mirroring a re-verified URL whose HTML was already fetched this run.
    with ctx.html_fetcher._fetch_cache_lock:
        request = ctx.html_fetcher.build_request(link)
        assert request is not None
        ctx.html_fetcher._fetch_cache[request.normalized_url] = SHELL_HTML
    fetched = _patch_process_detail_link(
        monkeypatch, ctx, html_by_url={link: SHELL_HTML}, cache_hit=True
    )

    rows = static_listing_rows._fetch_rendered_detail_rows(ctx, link, "Role", 30)
    assert rows == []
    assert fetched == [link]
    assert ctx.detail_shell_strikes == {}
    assert "detail_shell_strikes" not in ctx.stats


def test_host_scoping_two_hosts_independent(
    monkeypatch: pytest.MonkeyPatch, shell_skip_on: None
) -> None:
    """Strikes are tallied per host: exhausting one host's budget never skips
    another host's verifications."""
    ctx = _make_static_context()
    host_a = "cdn-a.example.com"
    host_b = "cdn-b.example.com"
    links = [f"https://{host_a}/j/1", f"https://{host_b}/j/1"]
    html_by_url = {links[0]: SHELL_HTML, links[1]: SERVER_HTML}
    fetched = _patch_process_detail_link(monkeypatch, ctx, html_by_url=html_by_url)

    for _ in range(2):
        static_listing_rows._fetch_rendered_detail_rows(ctx, links[0], "A", 30)
    assert ctx.detail_shell_strikes == {host_a: 2}
    static_listing_rows._fetch_rendered_detail_rows(ctx, links[1], "B", 30)
    assert fetched[-1] == links[1]
    assert ctx.detail_shell_strikes == {host_a: 2}


def test_kill_switch_restores_fetch_always(
    monkeypatch: pytest.MonkeyPatch, shell_skip_on: None
) -> None:
    """BALUFFO_STATIC_SHELL_DETAIL_SKIP=0 restores fetch-always semantics —
    no skip gate, no strikes recorded."""
    ctx = _make_static_context()
    monkeypatch.setattr(static_listing_rows, "STATIC_SHELL_DETAIL_SKIP_ENABLED", False)
    links = [f"https://cdn.example-hrmos.co/pages/t/jobs/{i}" for i in range(3)]
    html_by_url = {link: SHELL_HTML for link in links}
    fetched = _patch_process_detail_link(monkeypatch, ctx, html_by_url=html_by_url)

    for index, link in enumerate(links):
        static_listing_rows._fetch_rendered_detail_rows(ctx, link, f"Role {index}", 30)
        assert fetched[-1] == link

    assert ctx.detail_shell_strikes == {}
    assert "detail_shell_skipped" not in ctx.stats


def test_card_row_fallback_preserved_after_skip(
    monkeypatch: pytest.MonkeyPatch, shell_skip_on: None
) -> None:
    """The skip must not change the caller's fallback: a skipped verification
    yields no detail rows, so _append_rendered_row emits the card row."""
    ctx = _make_static_context()
    links = [f"https://cdn.example-hrmos.co/pages/t/jobs/{i}" for i in range(3)]
    html_by_url = {link: SHELL_HTML for link in links}
    _patch_process_detail_link(monkeypatch, ctx, html_by_url=html_by_url)

    rendered_rows = [
        {
            "title": "Senior Game Designer",
            "jobLink": links[0],
            "_locationHint": "Tokyo",
        },
        {
            "title": "Engine Programmer",
            "jobLink": links[1],
            "_locationHint": "Tokyo",
        },
    ]

    def extract_rendered_card_jobs(*_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        return rendered_rows

    monkeypatch.setattr(static_listing, "extract_rendered_card_jobs", extract_rendered_card_jobs)
    emitted, _, _ = static_listing_rows._append_rendered_card_rows(
        ctx,
        "<html></html>",
        "https://www.bandainamcostudios.com/recruit/career",
        30,
        [],
        set(),
    )
    assert emitted == 2
    assert [row["jobLink"] for row in ctx.jobs] == [links[0], links[1]]


def test_detect_js_shell_predicate_guards() -> None:
    """The strike predicate leans on the shared shell detector; pin the shapes
    (Next.js shell token, `__NEXT_DATA__`) so a detector regression cannot
    silently flip the skip semantics."""
    assert detect_js_shell(SHELL_HTML)
    assert not detect_js_shell(SERVER_HTML)
