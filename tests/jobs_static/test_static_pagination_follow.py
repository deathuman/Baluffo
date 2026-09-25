"""Static listing pagination follow (hrmos 2026-09-14).

The generic static lane reads only the registry window; boards that server-
paginate (Cygames: 338 postings over pages 1-4, page 1 rendering 100) lose
the rest. The runner now discovers same-listing ``?page=N`` anchors from
fetched listing HTML and follows them within the same run, capped and behind
a kill switch.
"""

from __future__ import annotations

from typing import Any

from src.jobs.adapters.plugins.static._runner import run_simple_static_plugin
from src.jobs.adapters.plugins.static.hrmos import _SPEC as _HRMOS_SPEC
from src.jobs.adapters.static_listing import StaticFetchRunner
from src.jobs.adapters.static_listing_pagination import (
    STATIC_PAGINATION_MAX_FOLLOWED_PAGES,
    pagination_anchors_for_html,
    static_pagination_follow_enabled,
)
from src.jobs.adapters.static_runtime import StaticRunDeps, StaticSourceContext
from src.jobs.adapters.static_runtime_support import (
    StaticHtmlFetcher,
    StaticSourceRuntimeConfig,
    build_static_entry_report,
)

_BASE = "https://hrmos.example/pages/cygames/jobs"


def _listing_html(page: int, *, job_ids: list[str], extra_pages: list[int]) -> str:
    anchors = "".join(
        f'<a href="{_BASE}/0000{job_id}"><h3>Engineer {job_id}</h3><p>Tokyo</p></a>'
        for job_id in job_ids
    )
    page_anchors = "".join(
        f'<li><a class="sg-button" href="{_BASE}?page={n}"> {n} </a></li>' for n in extra_pages
    )
    return f"<html><body><ul class='pg-pagenation'>{page_anchors}</ul>{anchors}</body></html>"


def _pagination_pages_html(*extra_pages: int) -> str:
    page_anchors = "".join(
        f'<li><a class="sg-button" href="{_BASE}?page={n}"> {n} </a></li>' for n in extra_pages
    )
    return f"<html><body><ul class='pg-pagenation'>{page_anchors}</ul></body></html>"


def _make_runner(
    pages_by_url: dict[str, str],
    *,
    state_entry: dict[str, Any] | None = None,
) -> tuple[StaticFetchRunner, StaticSourceContext, list[str]]:
    fetch_log: list[str] = []

    def fetch_text(url: str, _timeout: int) -> str:
        fetch_log.append(url)
        return pages_by_url.get(url, "")

    source_name = "Cygames Static Pagination"
    source = {"name": source_name, "company": source_name, "pages": [_BASE]}
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
        default_path_tokens=["/jobs/"],
        default_query_keys=[],
    )
    ctx = StaticSourceContext(
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
        state_entry=state_entry or {},
        selected_source_count=1,
        jobs=[],
        warnings=[],
        errors=[],
        details=[],
    )
    return StaticFetchRunner(ctx), ctx, fetch_log


# --- parser units -----------------------------------------------------------


def test_pagination_anchors_from_real_shape() -> None:
    html = _pagination_pages_html(1, 2, 3)
    assert pagination_anchors_for_html(html, _BASE) == [
        f"{_BASE}?page=2",
        f"{_BASE}?page=3",
    ]


def test_pagination_anchors_relative_href_and_dedup() -> None:
    html = '<a href="?page=2">2</a><a href="jobs?page=2">dup</a>'
    assert pagination_anchors_for_html(html, _BASE) == [f"{_BASE}?page=2"]


def test_pagination_anchors_reject_foreign_path_host_and_params() -> None:
    html = (
        f'<a href="https://hrmos.example/pages/other/jobs?page=2">other board</a>'
        f'<a href="https://elsewhere.example/pages/cygames/jobs?page=2">other host</a>'
        f'<a href="{_BASE}?page=2&filter=eng">extra param</a>'
        f'<a href="{_BASE}?page=abc">not digits</a>'
        f'<a href="{_BASE}?page=2&page=3">dup param</a>'
        f'<a href="{_BASE}">self</a>'
    )
    assert pagination_anchors_for_html(html, _BASE) == []


def test_pagination_anchors_ignores_query_param_order() -> None:
    html = f'<a href="{_BASE}?filter=eng&page=2">2</a>'
    assert pagination_anchors_for_html(html, f"{_BASE}?filter=eng") == [
        f"{_BASE}?filter=eng&page=2"
    ]


def test_pagination_anchors_never_follow_backwards_to_page_one() -> None:
    html = f'<a href="{_BASE}">1</a><a href="{_BASE}?page=1">1</a>'
    assert pagination_anchors_for_html(html, f"{_BASE}?page=2") == []
    assert pagination_anchors_for_html(html, _BASE) == []


def test_pagination_anchors_cap() -> None:
    html = _pagination_pages_html(2, 3, 4, 5, 6, 7, 8)
    assert STATIC_PAGINATION_MAX_FOLLOWED_PAGES == 6  # documented bound
    assert pagination_anchors_for_html(html, _BASE) == [
        f"{_BASE}?page={n}" for n in (2, 3, 4, 5, 6, 7)
    ]


def test_pagination_anchors_malformed_html_never_raises() -> None:
    assert pagination_anchors_for_html("<a href='?page=2", _BASE) == []
    assert pagination_anchors_for_html(None, _BASE) == []  # type: ignore[arg-type]


def test_pagination_anchors_skip_malformed_bracket_host_href() -> None:
    html = '<a href="http://[cdn_template_directory]/jobs?page=2">template</a>'
    assert pagination_anchors_for_html(html, _BASE) == []


def test_pagination_kill_switch(monkeypatch) -> None:
    monkeypatch.setenv("BALUFFO_STATIC_PAGINATION_FOLLOW", "0")
    assert static_pagination_follow_enabled() is False
    assert pagination_anchors_for_html(_pagination_pages_html(2), _BASE) == []


# --- runner funnels ---------------------------------------------------------


def test_runner_follows_discovered_pagination_pages(monkeypatch) -> None:
    page_one = _listing_html(1, job_ids=["1001", "1002"], extra_pages=[1, 2])
    page_two = _listing_html(2, job_ids=["2001"], extra_pages=[1, 2])
    runner, _ctx, fetch_log = _make_runner({_BASE: page_one, f"{_BASE}?page=2": page_two})

    runner.run()

    assert f"{_BASE}?page=2" in fetch_log
    # Registry window stays first; discovered pages queue behind it.
    assert fetch_log[0] == _BASE
    assert fetch_log.index(f"{_BASE}?page=2") > 0


def test_runner_pagination_respects_kill_switch(monkeypatch) -> None:
    monkeypatch.setenv("BALUFFO_STATIC_PAGINATION_FOLLOW", "0")
    page_one = _listing_html(1, job_ids=["1001"], extra_pages=[2, 3])
    runner, _ctx, fetch_log = _make_runner({_BASE: page_one})

    runner.run()

    assert fetch_log[0] == _BASE
    assert not any("page=" in url for url in fetch_log)


def test_runner_pagination_cap_limits_followed_pages() -> None:
    extra_pages = [2, 3, 4, 5, 6, 7, 8]
    page_one = _listing_html(1, job_ids=["1001"], extra_pages=[1, *extra_pages])
    pages_by_url = {_BASE: page_one}
    for n in extra_pages:
        pages_by_url[f"{_BASE}?page={n}"] = _listing_html(
            n, job_ids=[f"{n}001"], extra_pages=[1, *extra_pages]
        )
    runner, _ctx, fetch_log = _make_runner(pages_by_url)

    runner.run()

    followed = {url for url in fetch_log if "page=" in url}
    assert followed == {f"{_BASE}?page={n}" for n in (2, 3, 4, 5, 6, 7)}
    assert f"{_BASE}?page=8" not in fetch_log


def test_runner_pagination_self_loop_never_fetched() -> None:
    # Page 2's HTML only advertises pages 1-2: nothing new may be re-fetched.
    page_one = _listing_html(1, job_ids=["1001"], extra_pages=[1, 2])
    page_two = _listing_html(2, job_ids=["2001"], extra_pages=[1, 2])
    runner, _ctx, fetch_log = _make_runner({_BASE: page_one, f"{_BASE}?page=2": page_two})

    runner.run()

    assert fetch_log.count(_BASE) == 1
    assert fetch_log.count(f"{_BASE}?page=2") == 1


# --- plugin lane (the live hrmos path) --------------------------------------


def _plugin_parse_html(ctx):
    import re

    from src.jobs.adapters.plugins.static._runner import static_job_row

    rows = []
    seen = set()
    for href, body in re.findall(r'<a href="([^"]*)">(.*?)</a>', ctx.html or ""):
        match = re.search(r"Engineer (\d+)", body)
        if not match:
            continue
        link = (
            href
            if href.startswith("http")
            else f"{_BASE.rsplit('/', 1)[0]}/jobs/{href.lstrip('/')}"
        )
        if link in seen:
            continue
        seen.add(link)
        rows.append(static_job_row(ctx, link=link, title=f"Engineer {match.group(1)}"))
    return rows


def _plugin_source_row() -> dict[str, Any]:
    return {
        "id": "static:listing_url:https://hrmos.example/pages/cygames/jobs",
        "name": "Cygames Group (Sheet)",
        "company": "Cygames Group",
        "pages": [_BASE],
    }


def test_plugin_runner_follows_pagination_pages() -> None:
    page_one = _listing_html(1, job_ids=["1001", "1002"], extra_pages=[1, 2])
    page_two = _listing_html(2, job_ids=["2001"], extra_pages=[1, 2])
    fetch_log: list[str] = []

    def fetch_text(url: str, _timeout: int) -> str:
        fetch_log.append(url)
        return {_BASE: page_one, f"{_BASE}?page=2": page_two}.get(url, "")

    rows = run_simple_static_plugin(
        fetch_text=fetch_text,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=[_BASE],
        source_row=_plugin_source_row(),
        spec=_HRMOS_SPEC,
        parse_html=_plugin_parse_html,
    )

    assert fetch_log == [_BASE, f"{_BASE}?page=2"]
    assert sorted(row["title"] for row in rows) == [
        "Engineer 1001",
        "Engineer 1002",
        "Engineer 2001",
    ]


def test_plugin_runner_pagination_best_effort_on_continuation_failure() -> None:
    page_one = _listing_html(1, job_ids=["1001"], extra_pages=[2])

    def fetch_text(url: str, _timeout: int) -> str:
        if "page=2" in url:
            raise OSError("continuation fetch failed")
        return page_one

    rows = run_simple_static_plugin(
        fetch_text=fetch_text,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=[_BASE],
        source_row=_plugin_source_row(),
        spec=_HRMOS_SPEC,
        parse_html=_plugin_parse_html,
    )

    # Page-1 rows survive; the failed continuation never zeroes the harvest.
    assert [row["title"] for row in rows] == ["Engineer 1001"]


def test_plugin_runner_honors_multi_page_registry_window() -> None:
    page_one = _listing_html(1, job_ids=["1001"], extra_pages=[])
    page_two = _listing_html(2, job_ids=["2001"], extra_pages=[])
    fetch_log: list[str] = []

    def fetch_text(url: str, _timeout: int) -> str:
        fetch_log.append(url)
        return {_BASE: page_one, f"{_BASE}?page=2": page_two}.get(url, "")

    source_row = _plugin_source_row()
    source_row["pages"] = [_BASE, f"{_BASE}?page=2"]
    rows = run_simple_static_plugin(
        fetch_text=fetch_text,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=source_row["pages"],
        source_row=source_row,
        spec=_HRMOS_SPEC,
        parse_html=_plugin_parse_html,
    )

    assert fetch_log == [_BASE, f"{_BASE}?page=2"]
    assert sorted(row["title"] for row in rows) == ["Engineer 1001", "Engineer 2001"]


def test_plugin_runner_later_seed_failure_is_best_effort() -> None:
    page_one = _listing_html(1, job_ids=["1001"], extra_pages=[])

    def fetch_text(url: str, _timeout: int) -> str:
        if "page=2" in url:
            raise OSError("second registry window page failed")
        return page_one

    source_row = _plugin_source_row()
    source_row["pages"] = [_BASE, f"{_BASE}?page=2"]
    rows = run_simple_static_plugin(
        fetch_text=fetch_text,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=source_row["pages"],
        source_row=source_row,
        spec=_HRMOS_SPEC,
        parse_html=_plugin_parse_html,
    )

    # The dead second seed skips; the first seed's rows survive.
    assert [row["title"] for row in rows] == ["Engineer 1001"]


def test_plugin_runner_kill_switch_keeps_registry_windows(monkeypatch) -> None:
    monkeypatch.setenv("BALUFFO_STATIC_PAGINATION_FOLLOW", "0")
    page_one = _listing_html(1, job_ids=["1001"], extra_pages=[])
    page_two = _listing_html(2, job_ids=["2001"], extra_pages=[])
    page_three = _listing_html(3, job_ids=["3001"], extra_pages=[])
    fetch_log: list[str] = []

    def fetch_text(url: str, _timeout: int) -> str:
        fetch_log.append(url)
        return {
            _BASE: page_one,
            f"{_BASE}?page=2": page_two,
            f"{_BASE}?page=3": page_three,
        }.get(url, "")

    source_row = _plugin_source_row()
    source_row["pages"] = [_BASE, f"{_BASE}?page=2"]
    rows = run_simple_static_plugin(
        fetch_text=fetch_text,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=source_row["pages"],
        source_row=source_row,
        spec=_HRMOS_SPEC,
        parse_html=_plugin_parse_html,
    )

    # Configured windows always run; only anchor discovery is gated — so page 2
    # (registry) is fetched while page 3 (would-be discovery) is not.
    assert fetch_log == [_BASE, f"{_BASE}?page=2"]
    assert sorted(row["title"] for row in rows) == ["Engineer 1001", "Engineer 2001"]


def test_plugin_runner_first_seed_failure_still_aborts() -> None:
    def fetch_text(url: str, _timeout: int) -> str:
        raise OSError("primary window down")

    source_row = _plugin_source_row()
    source_row["pages"] = [_BASE, f"{_BASE}?page=2"]
    rows = run_simple_static_plugin(
        fetch_text=fetch_text,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=source_row["pages"],
        source_row=source_row,
        spec=_HRMOS_SPEC,
        parse_html=_plugin_parse_html,
    )

    assert rows == []


def test_plugin_runner_pagination_kill_switch(monkeypatch) -> None:
    monkeypatch.setenv("BALUFFO_STATIC_PAGINATION_FOLLOW", "0")
    page_one = _listing_html(1, job_ids=["1001"], extra_pages=[2])
    fetch_log: list[str] = []

    def fetch_text(url: str, _timeout: int) -> str:
        fetch_log.append(url)
        return page_one

    rows = run_simple_static_plugin(
        fetch_text=fetch_text,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=[_BASE],
        source_row=_plugin_source_row(),
        spec=_HRMOS_SPEC,
        parse_html=_plugin_parse_html,
    )

    assert fetch_log == [_BASE]
    assert [row["title"] for row in rows] == ["Engineer 1001"]


def test_runner_pagination_queues_even_when_fingerprint_skips_details() -> None:
    import hashlib

    page_one = _listing_html(1, job_ids=["1001"], extra_pages=[2])
    pages_by_url = {
        _BASE: page_one,
        f"{_BASE}?page=2": _listing_html(2, job_ids=["2001"], extra_pages=[2]),
    }
    runner, _ctx, fetch_log = _make_runner(
        pages_by_url,
        state_entry={"lastListingFingerprint": hashlib.sha1(page_one.encode()).hexdigest()},
    )

    runner.run()

    # The fingerprint skip only bypasses details; pagination anchors queue
    # before it so the full board window still syncs.
    assert f"{_BASE}?page=2" in fetch_log


# --- path-pager dialect (coverage sweep 2026-09-14) --------------------------


def _path_pages_html(*extra_pages: int) -> str:
    page_anchors = "".join(
        f'<a href="/page/{n}" aria-label="Page {n}">{n}</a>' for n in extra_pages
    )
    details = "".join(f'<a href="/slug/job/{n}001">Job {n}001</a>' for n in extra_pages)
    return f"<html><body>{details}{page_anchors}</body></html>"


_PS_BASE = "https://www.playstation.com/en-us/corporate/playstation-careers/#listings"
_PS_BOARD = "https://careers.playstation.com/"
_PS_CANONICAL = f'<link rel="canonical" href="{_PS_BOARD}">'


def test_path_dialect_anchors_follow_trailing_page_segment() -> None:
    # Nexon shape: path-pager anchors carry the listing path (/job/page/N).
    html = (
        '<a href="/job/page/2">2</a><a href="/job/page/3">3</a>'
        '<a href="/job/some-slug/job/12">detail</a><a href="/jobs/2">wp-comment-noise</a>'
    )
    assert pagination_anchors_for_html(html, "https://x.example/job/") == [
        "https://x.example/job/page/2",
        "https://x.example/job/page/3",
    ]


def test_path_dialect_requires_trailing_page_segment() -> None:
    # Intermediate /page/ segments are a different URL family; the WP noise
    # class (/jobs/2, /jobs/2?replytocom=1) has no /page/ segment at all.
    html = (
        '<a href="/page/2/edit">admin</a>'
        '<a href="/jobs/2">comment</a>'
        '<a href="/page/abc">not digits</a>'
        '<a href="/job/page/2?page=3">piggyback-polluted query</a>'
    )
    assert pagination_anchors_for_html(html, "https://x.example/job/?jobtype=full") == []


def test_path_dialect_ignores_page_param_piggyback_in_query_equality() -> None:
    # Nexon's historic anchors carry a redundant ?page=2 on /page/2 — the
    # piggyback page param is pagination noise, but real filters still gate.
    base = "https://x.example/job/"
    html = '<a href="/job/page/2?page=2">2</a>'
    assert pagination_anchors_for_html(html, base) == ["https://x.example/job/page/2?page=2"]
    html_filtered = '<a href="/job/page/2?jobtype=contract">2</a>'
    assert pagination_anchors_for_html(html_filtered, base) == []


def test_path_dialect_base_page_number_gates() -> None:
    # Matching the query dialect's contract: same page and page < 2 are
    # refused; other pages (backward included — dedupe prevents refetch)
    # are accepted by the URL check.
    base = "https://x.example/job/page/3"
    html = '<a href="/job/page/2">2</a><a href="/job/page/3">3</a><a href="/job/page/4">4</a>'
    assert pagination_anchors_for_html(html, base) == [
        "https://x.example/job/page/2",
        "https://x.example/job/page/4",
    ]
    assert pagination_anchors_for_html('<a href="/job/page/1">1</a>', base) == []


def test_playstation_redirect_window_resolves_via_document_evidence() -> None:
    html = _PS_CANONICAL + _path_pages_html(2, 3, 4, 5, 6, 7, 8)
    anchors = pagination_anchors_for_html(html, _PS_BASE)
    assert anchors == [f"https://careers.playstation.com/page/{n}" for n in (2, 3, 4, 5, 6, 7)]


def test_document_base_evidence_priority_and_fallbacks() -> None:
    # base href wins over canonical and og:url (same registrable domain)
    html = (
        '<link rel="canonical" href="https://b.win.example/x">'
        '<meta property="og:url" content="https://c.win.example/x">'
        '<base href="https://a.win.example/x">'
        '<a href="/x/page/2">2</a>'
    )
    assert pagination_anchors_for_html(html, "https://www.win.example/y") == [
        "https://a.win.example/x/page/2"
    ]
    # og:url alone works (attr order swapped)
    html_og = '<meta content="https://board.example/x" property="og:url"><a href="/x/page/2">2</a>'
    assert pagination_anchors_for_html(html_og, "https://www.board.example/y") == [
        "https://board.example/x/page/2"
    ]
    # malformed/cross-domain/downgrade evidence ignored -> anchors resolve
    # against page_url, so plain query-dialect behavior is unchanged
    for evidence in (
        '<base href="not-a-url">',
        '<link rel="canonical" href="https://evil.org/x">',
        '<link rel="canonical" href="http://www.win.example/x">',
    ):
        assert pagination_anchors_for_html(
            evidence + '<a href="?page=2">2</a>', "https://www.win.example/x"
        ) == ["https://www.win.example/x?page=2"]


def test_document_evidence_same_host_path_never_relocates() -> None:
    # Query-only and scheme-only variants must not change the anchor base:
    # the result equals the no-evidence case exactly.
    page = "https://x.example/jobs"
    for evidence_html in (
        "",
        '<link rel="canonical" href="https://x.example/jobs?render=1">',
    ):
        html = evidence_html + '<a href="/jobs/page/2">2</a>'
        assert pagination_anchors_for_html(html, page) == ["https://x.example/jobs/page/2"]


def test_registrable_domain_guard_units() -> None:
    from src.jobs.adapters.static_listing_pagination import _registrable_domain

    assert _registrable_domain("careers.playstation.com") == "playstation.com"
    assert _registrable_domain("www.playstation.com") == "playstation.com"
    assert _registrable_domain("recruit.nexon.co.jp") == "nexon.co.jp"
    assert _registrable_domain("a.b.co.uk") == "b.co.uk"
    assert _registrable_domain("playstation.com") == "playstation.com"
    assert _registrable_domain("localhost") == "localhost"
    # Sibling-subdomain relocation (the PlayStation shape) is in-site; a
    # foreign registrable domain is never trusted.
    from src.jobs.adapters.static_listing_pagination import _anchor_base_url

    assert (
        _anchor_base_url(
            "https://www.playstation.com/x",
            '<link rel="canonical" href="https://careers.playstation.com/">',
        )
        == "https://careers.playstation.com/"
    )
    assert (
        _anchor_base_url(
            "https://www.a.com/x", '<link rel="canonical" href="https://careers.a.org/">'
        )
        == "https://www.a.com/x"
    )
