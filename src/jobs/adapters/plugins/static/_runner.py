"""Static plugin runner infrastructure.

AI boundary owns: shared static plugin run orchestration, detail fetching, card parsing, and diagnostics.
AI boundary implement in: this file for generic static plugin execution; individual studio extraction stays in plugin modules.
AI boundary search before contracts: static rendered-card helpers, plugin types, static runtime support, and jobs_static tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused static plugin runner tests.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from dataclasses import dataclass
from html import unescape
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.static_runtime_support import is_static_fetch_fallback_exception
from src.jobs.models import RawJob
from src.jobs.page_gating import (
    classify_job_page,
    dead_listing_page_meta,
    looks_like_job_title_candidate,
    looks_like_static_parser_noise_title,
)
from src.jobs.text_utils import clean_text

_EXPECTED_STATIC_PLUGIN_FETCH_EXCEPTIONS = (OSError, RuntimeError, ValueError)


@dataclass(frozen=True)
class SimpleStaticPlugin:
    source_id: str
    default_company: str
    fallback_source: str = ""
    parser_stale_hint: str = ""
    use_page_gate: bool = False
    playwright_on_fetch_error: bool = False
    playwright_on_js_shell: bool = False
    require_generic_parser: bool = False
    empty_detail_fetch_required: bool | None = False
    empty_detail_traversal_mode: str = "listing_only"
    # Feed mode: when set, the plugin recovers rows from a server-rendered RSS feed
    # instead of parsing the page HTML (see ``_feed_postings``); ``filter_feed_keywords``
    # applies the conservative role-keyword gate for mixed news feeds, and should be
    # disabled for dedicated jobs-only feeds.
    feed_url_builder: Callable[[str], str] | None = None
    filter_feed_keywords: bool = True


@dataclass(frozen=True)
class SimpleStaticContext:
    page_url: str
    html: str
    source_row: dict[str, Any]
    company: str
    source_id: str
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]] | None = None


def static_identity_handler(*identities: str) -> Callable[[AdapterPluginContext], bool]:
    normalized = {clean_text(identity).lower() for identity in identities if clean_text(identity)}

    def can_handle(ctx: AdapterPluginContext) -> bool:
        return clean_text(ctx.source_identity).lower() in normalized

    return can_handle


# Section-header phrases that carry role-hint tokens ("Open Roles", "We're Hiring") but
# are not postings; list-only extraction must not publish them as rows.
_LIST_ONLY_SECTION_HEADER_PHRASES = (
    "open role",
    "open position",
    "current opening",
    "job opening",
    "we are hiring",
    "we're hiring",
    "we re hiring",
    "now hiring",
    "join our team",
    "join the team",
    "join us",
    "our team",
    "work with us",
    "work at",
    "life at",
    "careers at",
    "vacancies",
    "recruiting",
    "apply now",
    "available positions",
    "see all roles",
    "all roles",
    "browse roles",
    "explore roles",
    "career opportunities",
)


def looks_like_listing_role_title(title: str) -> bool:
    """Job-title-looking listing heading that is not noise or a section header.

    Shared by the generic WP10 list-only fallback and leaf plugins that post-filter
    extracted titles (e.g. pages where the hero heading shares the role-title markup).
    """
    if looks_like_static_parser_noise_title(title):
        return False
    if not looks_like_job_title_candidate(title):
        return False
    key = clean_text(title).casefold()
    return not any(phrase in key for phrase in _LIST_ONLY_SECTION_HEADER_PHRASES)


def static_job_row(
    ctx: SimpleStaticContext,
    *,
    link: str,
    title: str,
    city: str = "",
    country: str = "Unknown",
    work_type: str = "",
    contract_type: str = "",
    **extra: Any,
) -> RawJob:
    return {
        "sourceJobId": f"static:{ctx.source_id}:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
        "title": title,
        "company": ctx.company,
        "city": city,
        "country": country,
        "workType": work_type,
        "contractType": contract_type,
        "jobLink": link,
        "sector": "Game",
        "postedAt": "",
        **extra,
    }


def static_listing_anchor_link(page_url: str, title: str) -> str:
    """On-page anchor link for a list-only role that has no per-role detail URL.

    Anchors the row to the careers page with a title-derived ``?static-role=<slug>``
    query parameter so ``sourceJobId`` stays distinct per role and the link stays
    on-domain. A query parameter is used (not a ``#`` fragment) deliberately: URL
    normalization strips fragments at several pipeline stages (plugin-row dedup,
    canonicalization, dedup fingerprinting), which would otherwise collapse every
    role on the page into a single surviving row. Query parameters survive
    normalization, so list-only rows stay distinct end-to-end.
    """
    slug = clean_text(title).lower().replace("&", "and").replace(" ", "-")
    return clean_text(urljoin(page_url, "?static-role=" + slug))


def static_list_only_job_rows(
    ctx: SimpleStaticContext,
    *,
    block_sep: re.Pattern[str],
    title_re: re.Pattern[str],
) -> list[RawJob]:
    """Extract list-only roles from a server-rendered page with no per-role links.

    ``block_sep`` must be a lookahead pattern that splits the HTML just before each role
    block (e.g. ``(?=class="role")``); ``title_re`` must capture the role title in
    group 1. Each title becomes a ``static_job_row`` anchored via
    :func:`static_listing_anchor_link` so rows are distinct and on-domain.
    """
    jobs: list[RawJob] = []
    seen: set[str] = set()
    blocks = block_sep.split(ctx.html or "")
    for block in blocks[1:]:
        title_match = title_re.search(block)
        if not title_match:
            continue
        title = clean_text(unescape(strip_html_text(title_match.group(1))))
        if not title:
            continue
        link = static_listing_anchor_link(ctx.page_url, title)
        if link in seen:
            continue
        seen.add(link)
        jobs.append(static_job_row(ctx, link=link, title=title))
    return jobs


def static_listing_job_row(
    *,
    source_id: str,
    link: str,
    title: str,
    company: str,
    city: str = "",
    country: str = "Unknown",
    work_type: str = "",
    contract_type: str = "",
    locations: list[dict[str, str]] | None = None,
    location_summary: str | None = None,
    **extra: Any,
) -> RawJob:
    row: RawJob = {
        "sourceJobId": f"static:{source_id}:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
        "title": title,
        "company": company,
        "city": city,
        "country": country,
        "workType": work_type,
        "contractType": contract_type,
        "jobLink": link,
        "sector": "Game",
        "postedAt": "",
        "adapter": "static",
        "studio": company,
        "source": "",
    }
    if locations is not None:
        row["locations"] = locations
    if location_summary is not None:
        row["locationSummary"] = location_summary
    row.update(extra)
    return row


def first_static_page(pages: list[str]) -> str:
    return clean_text(pages[0]) if pages else ""


def static_plugin_context_values(
    *,
    source_row: dict[str, Any],
    default_company: str,
    default_source_id: str,
    default_source_name: str,
) -> tuple[str, str, str]:
    company = (
        clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name"))
        or default_company
    )
    source_id = clean_text(source_row.get("id")) or default_source_id
    source_name = clean_text(source_row.get("name")) or default_source_name
    return company, source_id, source_name


def _meta(source_row: dict[str, Any], classification: str, **values: Any) -> None:
    source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(classification, **values)


def fetch_static_plugin_html(
    *,
    fetch_text: Callable[[str, int], str],
    page_url: str,
    timeout_s: int,
    source_row: dict[str, Any],
) -> str:
    try:
        return fetch_text(page_url, timeout_s)
    except _EXPECTED_STATIC_PLUGIN_FETCH_EXCEPTIONS as exc:
        if not is_static_fetch_fallback_exception(exc):
            raise
        classification, recommend = _heuristics.classify_fetch_exception(exc)
        _meta(
            source_row,
            classification,
            browser_fallback_recommended=bool(recommend),
            extractor_hint="fetch_failed",
            error=str(exc),
        )
        return ""


def static_plugin_blocked_by_js_shell(
    *,
    html: str,
    page_url: str,
    source_row: dict[str, Any],
) -> bool:
    if not _heuristics.detect_js_shell(html):
        return False
    _meta(
        source_row,
        _heuristics.CLASSIFICATION_BLOCKED_OR_CHALLENGE,
        browser_fallback_recommended=True,
        extractor_hint="js_shell_detected",
        ats_links=_heuristics.detect_outbound_ats_links(html, base_url=page_url),
    )
    return True


def stamp_static_plugin_rows(
    *,
    rows: list[dict[str, Any]],
    company: str,
    source_name: str,
) -> list[RawJob]:
    for row in rows:
        if isinstance(row, dict):
            row["adapter"] = "static"
            row["studio"] = company
            row["source"] = source_name
    return [row for row in rows if isinstance(row, dict)]


def record_static_plugin_empty_parse(
    *,
    html: str,
    page_url: str,
    source_row: dict[str, Any],
) -> None:
    ats_links = _heuristics.detect_outbound_ats_links(html, base_url=page_url)
    if _heuristics.detect_no_openings(html):
        _meta(
            source_row,
            _heuristics.CLASSIFICATION_EMPTY_CONFIRMED,
            browser_fallback_recommended=False,
            empty_confirmed=True,
            extractor_hint="explicit_no_openings_marker",
            ats_links=ats_links,
        )
        return
    likely_js = _heuristics.detect_js_shell(html) or _heuristics.visible_text_len(html) < 400
    _meta(
        source_row,
        _heuristics.CLASSIFICATION_BLOCKED_OR_CHALLENGE
        if likely_js
        else _heuristics.CLASSIFICATION_FETCH_OK_EXTRACT_ZERO,
        browser_fallback_recommended=True,
        extractor_hint="parse_empty_js_shell_suspected" if likely_js else "parse_empty",
        ats_links=ats_links,
    )


def static_detail_link_rows(
    *,
    html: str,
    page_url: str,
    company: str,
    source_id: str,
    is_probable_detail_url: Callable[[str], bool],
) -> list[RawJob]:
    base_host = (urlparse(page_url).netloc or "").lower()
    seen_links: set[str] = set()
    rows: list[RawJob] = []
    for match in re.finditer(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html or ""):
        href = clean_text(match.group(1))
        if not href:
            continue
        absolute = urljoin(page_url, href)
        if (urlparse(absolute).netloc or "").lower() != base_host:
            continue
        if not is_probable_detail_url(absolute):
            continue
        if absolute in seen_links:
            continue
        seen_links.add(absolute)
        anchor_inner = match.group(2) or ""
        anchor_text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", anchor_inner)).strip() or "Job"
        rows.append(
            {
                "title": anchor_text[:200],
                "company": company,
                "jobLink": absolute,
                "sourceJobId": f"{source_id}:{absolute}",
                "city": "",
                "country": "",
                "workType": "",
                "contractType": "",
                "sector": "Game",
                "postedAt": "",
            }
        )
    return rows


def generic_parser_then_detail_links(
    ctx: SimpleStaticContext,
    *,
    extra_anchor_filter: Callable[[str], bool] | None = None,
) -> list[dict[str, Any]]:
    parser = ctx.parse_jobpostings_from_html
    if not callable(parser):
        return []
    rows = parser(
        ctx.html,
        base_url=ctx.page_url,
        fallback_company=ctx.company,
        fallback_source_id_prefix=f"static:{ctx.source_id}",
    )
    if rows:
        return rows
    return static_detail_link_rows(
        html=ctx.html,
        page_url=ctx.page_url,
        company=ctx.company,
        source_id=ctx.source_id,
        is_probable_detail_url=extra_anchor_filter or (lambda _href: True),
    )


def _fetch_html(
    fetch_text: Callable[[str, int], str],
    page_url: str,
    timeout_s: int,
    source_row: dict[str, Any],
    spec: SimpleStaticPlugin,
    try_playwright: Callable[[str, int], tuple[str, str]] | None,
) -> str:
    try:
        html = fetch_text(page_url, timeout_s)
    except _EXPECTED_STATIC_PLUGIN_FETCH_EXCEPTIONS as exc:
        if not is_static_fetch_fallback_exception(exc):
            raise
        if spec.playwright_on_fetch_error and try_playwright:
            html, _ = try_playwright(page_url, max(3, min(timeout_s, 30)))
            if html:
                return html
        if spec.parser_stale_hint:
            classification, recommend = _heuristics.classify_fetch_exception(exc)
            _meta(
                source_row,
                classification,
                browser_fallback_recommended=bool(recommend),
                extractor_hint="fetch_failed",
                error=str(exc),
            )
        return ""
    if spec.playwright_on_js_shell and try_playwright and _heuristics.detect_js_shell(html):
        rendered_html, _ = try_playwright(page_url, max(3, min(timeout_s, 30)))
        return rendered_html or html
    return html


def _blocked_by_page_gate(
    html: str,
    page_url: str,
    source_row: dict[str, Any],
    company: str,
    spec: SimpleStaticPlugin,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    timeout_s: int = 10,
) -> bool:
    if not spec.use_page_gate:
        return False
    job_like, gate_reason = classify_job_page(
        html, page_url, profile=source_row if isinstance(source_row, dict) else None
    )
    if job_like:
        return False
    if gate_reason == "dead_listing_page":
        if try_playwright:
            rendered_html, _ = try_playwright(page_url, max(3, min(timeout_s, 30)))
            if rendered_html and rendered_html != html:
                job_like2, _ = classify_job_page(
                    rendered_html,
                    page_url,
                    profile=source_row if isinstance(source_row, dict) else None,
                )
                if job_like2:
                    return False
        source_row["_staticPluginMeta"] = dead_listing_page_meta(page_url=page_url, company=company)
    return True


def run_simple_static_plugin(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    spec: SimpleStaticPlugin,
    parse_html: Callable[[SimpleStaticContext], list[RawJob]] | None = None,
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    company_override: str = "",
    source_id_override: str = "",
    **kwargs: Any,
) -> list[RawJob]:
    _ = (retries, backoff_s, kwargs)
    if not pages or (spec.require_generic_parser and not callable(parse_jobpostings_from_html)):
        return []
    if spec.feed_url_builder is not None:
        from ._feed_postings import run_website_feed_postings

        return run_website_feed_postings(
            fetch_text=fetch_text,
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
            pages=pages,
            source_row=source_row,
            source_id=spec.source_id,
            default_company=spec.default_company,
            feed_url_builder=spec.feed_url_builder,
            filter_role_keywords=spec.filter_feed_keywords,
        )
    assert parse_html is not None  # non-feed specs always supply a parser
    page_url = clean_text(pages[0])
    if not page_url:
        return []
    company = company_override or (
        clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name"))
        or spec.default_company
    )
    source_id = source_id_override or clean_text(source_row.get("id")) or spec.source_id
    html = _fetch_html(fetch_text, page_url, timeout_s, source_row, spec, try_playwright)
    if not html:
        return []
    if _blocked_by_page_gate(html, page_url, source_row, company, spec, try_playwright, timeout_s):
        return []
    rows = [
        row
        for row in parse_html(
            SimpleStaticContext(
                page_url, html, source_row, company, source_id, parse_jobpostings_from_html
            )
        )
        if isinstance(row, dict)
    ]
    if rows:
        if spec.parser_stale_hint and spec.empty_detail_fetch_required is not None:
            _meta(
                source_row,
                _heuristics.CLASSIFICATION_OK_WITH_JOBS,
                detail_fetch_required=False,
                detail_traversal_mode=spec.empty_detail_traversal_mode,
            )
        source_name = clean_text(source_row.get("name")) or spec.fallback_source or company
        for row in rows:
            row["adapter"] = "static"
            row["studio"] = company
            row["source"] = source_name
        return rows
    if spec.parser_stale_hint:
        _meta(
            source_row,
            _heuristics.CLASSIFICATION_PARSER_STALE,
            browser_fallback_recommended=False,
            extractor_hint=spec.parser_stale_hint,
            detail_fetch_required=spec.empty_detail_fetch_required,
            detail_traversal_mode=spec.empty_detail_traversal_mode,
        )
    return []


def simple_static_run(
    spec: SimpleStaticPlugin,
    parse_html: Callable[[SimpleStaticContext], list[RawJob]] | None = None,
) -> Callable[..., list[RawJob]]:
    def run(
        *,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
        pages: list[str],
        source_row: dict[str, Any],
        **kwargs: Any,
    ) -> list[RawJob]:
        return run_simple_static_plugin(
            fetch_text=fetch_text,
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
            pages=pages,
            source_row=source_row,
            spec=spec,
            parse_html=parse_html,
            **kwargs,
        )

    return run
