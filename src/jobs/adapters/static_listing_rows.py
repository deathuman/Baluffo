"""Static listing parsed and rendered row assembly.

AI boundary owns: converting listing HTML into parsed rows and rendered card rows, adding detail
links, collecting listing candidates, and emitting listing-only meta.
AI boundary implement in: this leaf for row assembly; state helpers come from
``static_listing_state.py``. Seam: parser/detail helpers are resolved through the coordinator at
call time so tests can patch them.
"""

from __future__ import annotations

import re
from html import unescape
from typing import Any

from src.jobs.adapters.html_parsers import (
    strip_html_text,
)
from src.jobs.adapters.plugins.static._runner import (
    looks_like_listing_role_title,
    static_listing_anchor_link,
    static_listing_job_row,
)
from src.jobs.adapters.static_detail_heuristics import (
    add_detail_link,
)
from src.jobs.adapters.static_listing_common import StaticDetailCandidate
from src.jobs.adapters.static_listing_state import (
    _append_detail_candidate,
    _is_provisional_static_artifact_row,
    _needs_detail_location_resolution,
)
from src.jobs.page_gating import (
    looks_like_job_title_candidate,
    looks_like_static_parser_noise_title,
)
from src.jobs.text_utils import clean_text, normalize_url
from src.shared.regex import find_urls_in_text

from .static_runtime import StaticSourceContext


def _source_studio(ctx: StaticSourceContext) -> str:
    return clean_text(ctx.source.get("studio")) or ctx.company or ctx.source_name


# pure — reads ctx field
def _source_label(ctx: StaticSourceContext) -> str:
    return clean_text(ctx.source.get("name")) or ctx.company or ctx.source_name


_LIST_ONLY_HEADING_TAG_RE = re.compile(r"(?is)<(h[2-4])[^>]*>(.*?)</\1>")
_LIST_ONLY_SCRIPT_STYLE_RE = re.compile(r"(?is)<(?:script|style)[^>]*>.*?</(?:script|style)>")
_LIST_ONLY_MIN_JOB_LIKE_HEADINGS = 2
_LIST_ONLY_MAX_ANCHORED_ROWS = 50


# pure — heading scan
def _job_like_heading_titles(listing_html: str) -> list[str]:
    """Distinct job-title-looking headings from a block-structured listing."""
    titles: list[str] = []
    seen: set[str] = set()
    page_html = _LIST_ONLY_SCRIPT_STYLE_RE.sub(" ", listing_html or "")
    for match in _LIST_ONLY_HEADING_TAG_RE.finditer(page_html):
        title = clean_text(unescape(strip_html_text(match.group(2) or "")))
        key = title.casefold()
        if not title or key in seen:
            continue
        if not looks_like_listing_role_title(title):
            continue
        seen.add(key)
        titles.append(title)
    return titles


# mutation — appends generic list-only rows
def _append_block_title_fallback_rows(
    ctx: StaticSourceContext,
    listing_htmls: list[str],
    page_url: str,
) -> int:
    """Generic list-only recovery for block-structured listings with no detail links.

    When the listing page parses job-title-looking headings but exposes no per-role
    links (and the generic JSON-LD / rendered-card / detail-link paths found nothing),
    emit one query-anchored row per distinct job-like heading. This recovers boards
    like Upsurge's without a per-host plugin; ``static_listing_anchor_link`` keeps the
    rows distinct through URL normalization (see WP9 for why query anchors, not
    fragments).
    """
    titles: list[str] = []
    seen_titles: set[str] = set()
    for listing_html in listing_htmls:
        for title in _job_like_heading_titles(listing_html):
            key = title.casefold()
            if key in seen_titles:
                continue
            seen_titles.add(key)
            titles.append(title)
    if len(titles) < _LIST_ONLY_MIN_JOB_LIKE_HEADINGS:
        return 0
    source_id = clean_text(ctx.source.get("id")) or ctx.source_name
    studio = _source_studio(ctx)
    appended = 0
    for title in titles[:_LIST_ONLY_MAX_ANCHORED_ROWS]:
        link = static_listing_anchor_link(page_url, title)
        if not link or link in ctx.seen_links:
            continue
        ctx.seen_links.add(link)
        row = static_listing_job_row(
            source_id=source_id,
            link=link,
            title=title,
            company=studio,
        )
        row["source"] = _source_label(ctx)
        ctx.jobs.append(row)
        appended += 1
    return appended


# mutation — modifies in-place state
def _append_parsed_listing_rows(
    ctx: StaticSourceContext,
    listing_html: str,
    page_url: str,
    detail_links: list[StaticDetailCandidate],
    detail_seen: set[str],
) -> tuple[int, int]:
    emitted_count = 0
    provisional_count = 0
    from src.jobs.adapters import static_listing as _sl

    parsed = _sl.parse_jobpostings_from_html(
        listing_html,
        base_url=page_url,
        fallback_company=ctx.company,
        fallback_source_id_prefix=f"static:{ctx.source_name}",
    )
    for row in parsed:
        link = normalize_url(row.get("jobLink"))
        if _is_provisional_static_artifact_row(row):
            provisional_count += 1
            if link:
                _append_detail_candidate(
                    detail_links,
                    detail_seen,
                    ctx.seen_links,
                    candidate_url=link,
                    anchor_text=clean_text(row.get("title")),
                    depth=0,
                    parent_url=page_url,
                )
            continue
        if looks_like_static_parser_noise_title(clean_text(row.get("title"))):
            continue
        if not link or link in ctx.seen_links:
            continue
        ctx.seen_links.add(link)
        row["adapter"] = "static"
        row["studio"] = _source_studio(ctx)
        ctx.jobs.append(row)
        emitted_count += 1
    return emitted_count, provisional_count


# mutation — modifies in-place state
def _fetch_rendered_detail_rows(
    ctx: StaticSourceContext, link: str, title: str, source_budget_s: int
) -> list[Any]:
    from src.jobs.adapters import static_listing as _sl

    detail_result = _sl.process_detail_link(
        detail=link,
        detail_title=title,
        source_started=ctx.source_started,
        static_source_time_budget_s=source_budget_s,
        fetch_html_cached=ctx.html_fetcher.fetch_html_cached,
        timeout_s=ctx.run_deps.timeout_s,
        detail_retries=ctx.run_deps.retries,
        company=ctx.company,
        source_name=ctx.source_name,
        source=ctx.source,
        ignored_link_titles=ctx.ignored_link_titles,
        default_path_tokens=ctx.runtime_config.default_path_tokens,
        default_query_keys=ctx.runtime_config.default_query_keys,
    )
    ctx.stats["detail_pages_visited"] += 1
    ctx.emit_source_progress(
        phase_key="static_detail_traversal",
        phase_label="Traversing detail pages",
        target_label=title or link or ctx.source_name,
        target_url=link,
    )
    ctx.stats["detail_fetch_ms"] += int(detail_result.get("fetchMs") or 0)
    return detail_result.get("rows") or []


# mutation — modifies in-place state
def _append_rendered_detail_rows(ctx: StaticSourceContext, rows: list[Any]) -> int:
    emitted_count = 0
    for emitted_row in rows:
        if not isinstance(emitted_row, dict):
            continue
        emitted_row["source"] = _source_label(ctx)
        emitted_row["studio"] = _source_studio(ctx)
        ctx.jobs.append(emitted_row)
        emitted_count += 1
    return emitted_count


# mutation — modifies in-place state
def _append_rendered_row(
    ctx: StaticSourceContext,
    raw_row: dict[str, Any],
    source_budget_s: int,
    page_url: str,
    detail_links: list[StaticDetailCandidate],
    detail_seen: set[str],
) -> tuple[int, bool, int]:
    row = dict(raw_row)
    if clean_text(row.pop("_renderedCardMode", "")) == "fallback":
        return 0, False, 0
    row["adapter"] = "static"
    row["studio"] = _source_studio(ctx)
    row["source"] = _source_label(ctx)
    title = clean_text(row.get("title"))
    link = normalize_url(row.get("jobLink"))
    location_hint = clean_text(row.pop("_locationHint", ""))
    if looks_like_static_parser_noise_title(title):
        return 0, False, 0
    if _is_provisional_static_artifact_row(row):
        if link:
            _append_detail_candidate(
                detail_links,
                detail_seen,
                ctx.seen_links,
                candidate_url=link,
                anchor_text=title,
                depth=0,
                parent_url=page_url,
            )
        return 0, False, 1
    if not link or link in ctx.seen_links:
        return 0, False, 0
    job_like = looks_like_job_title_candidate(title)
    needs_lookup = (not job_like) or _needs_detail_location_resolution(row, link, location_hint)
    if not needs_lookup:
        ctx.seen_links.add(link)
        ctx.jobs.append(row)
        return 1, True, 0
    emitted_rows = _fetch_rendered_detail_rows(ctx, link, title, source_budget_s)
    if emitted_rows:
        ctx.seen_links.add(link)
        return _append_rendered_detail_rows(ctx, emitted_rows), job_like, 0
    if row:
        ctx.jobs.append(row)
        return 1, False, 0
    return 0, False, 0


# mutation — modifies in-place state
def _append_rendered_card_rows(
    ctx: StaticSourceContext,
    listing_html: str,
    page_url: str,
    source_budget_s: int,
    detail_links: list[StaticDetailCandidate],
    detail_seen: set[str],
) -> tuple[int, bool, int]:
    from src.jobs.adapters import static_listing as _sl

    rendered_rows = _sl.extract_rendered_card_jobs(
        listing_html,
        page_url=page_url,
        company=ctx.company,
        source_id=clean_text(ctx.source.get("id")) or ctx.source_name,
        allow_any_anchor=True,
    )
    emitted_count = 0
    has_job_like_title = False
    provisional_count = 0
    for raw_row in rendered_rows:
        count, job_like, provisional = _append_rendered_row(
            ctx,
            raw_row,
            source_budget_s,
            page_url,
            detail_links,
            detail_seen,
        )
        emitted_count += count
        has_job_like_title = has_job_like_title or (job_like and count > 0)
        provisional_count += provisional
    return emitted_count, has_job_like_title, provisional_count


# mutation — modifies in-place state
def _record_listing_only_rendered_meta(ctx: StaticSourceContext) -> None:
    ctx.source["_staticPluginMeta"] = {
        "detailFetchRequired": False,
        "detailTraversalMode": "listing_only",
    }
    ctx.emit_heartbeat()


# mutation — modifies in-place state
def _add_listing_detail_link(
    ctx: StaticSourceContext,
    detail_links: list[StaticDetailCandidate],
    detail_seen: set[str],
    candidate_url: str,
    anchor_text: str,
    enforce_heuristics: bool,
    page_url: str,
) -> None:
    dead_listing_count_before = int(ctx.link_rejections.get("dead_listing_page", 0))
    new_links: list[tuple[str, str]] = []
    add_detail_link(
        new_links,
        detail_seen,
        ctx.seen_links,
        ctx.link_rejections,
        candidate_url=candidate_url,
        anchor_text=anchor_text,
        enforce_heuristics=enforce_heuristics,
        page_url=page_url,
        source=ctx.source,
        default_path_tokens=ctx.runtime_config.default_path_tokens,
        default_query_keys=ctx.runtime_config.default_query_keys,
    )
    for detail_url, detail_title in new_links:
        detail_links.append(
            StaticDetailCandidate(
                url=detail_url,
                title=clean_text(detail_title),
                depth=0,
                parent_url=page_url,
            )
        )
    if (
        int(ctx.link_rejections.get("dead_listing_page", 0)) > dead_listing_count_before
        and len(ctx.dead_listing_page_examples) < 5
    ):
        ctx.dead_listing_page_examples.append(f"{candidate_url} | {ctx.company}")


# mutation — modifies in-place state
def _collect_listing_detail_links(
    ctx: StaticSourceContext,
    detail_links: list[StaticDetailCandidate],
    detail_seen: set[str],
    listing_html: str,
    page_url: str,
) -> None:
    pattern = (
        r'(?is)<(?:div|tr)[^>]*class=["\'][^"\']*job-listing-item[^"\']*["\']'
        r"[^>]*>(.*?)</(?:div|tr)>"
    )
    for row_match in re.finditer(pattern, listing_html):
        row_html = row_match.group(1) or ""
        link_match = re.search(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', row_html)
        if not link_match:
            continue
        anchor_text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", link_match.group(2) or ""))
        _add_listing_detail_link(
            ctx,
            detail_links,
            detail_seen,
            clean_text(link_match.group(1)),
            anchor_text,
            False,
            page_url,
        )
    for match in re.finditer(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', listing_html):
        anchor_text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", match.group(2) or ""))
        _add_listing_detail_link(
            ctx,
            detail_links,
            detail_seen,
            clean_text(match.group(1)),
            anchor_text,
            True,
            page_url,
        )
    for raw in find_urls_in_text(listing_html):
        _add_listing_detail_link(
            ctx,
            detail_links,
            detail_seen,
            clean_text(raw),
            "",
            True,
            page_url,
        )


# mutation — modifies in-place state
def _extract_listing_candidates(
    ctx: StaticSourceContext,
    *,
    page_url: str,
    source_budget_s: int,
    listing_htmls: list[str],
) -> tuple[int, list[StaticDetailCandidate], int]:
    detail_links: list[StaticDetailCandidate] = []
    detail_seen: set[str] = set()
    listing_jobs_found = 0
    provisional_rows_found = 0
    for listing_html in listing_htmls:
        ctx.emit_heartbeat()
        parsed_count, parsed_provisional = _append_parsed_listing_rows(
            ctx,
            listing_html,
            page_url,
            detail_links,
            detail_seen,
        )
        listing_jobs_found += parsed_count
        provisional_rows_found += parsed_provisional
        if listing_jobs_found == 0:
            rendered_count, has_job_like_title, rendered_provisional = _append_rendered_card_rows(
                ctx,
                listing_html,
                page_url,
                source_budget_s,
                detail_links,
                detail_seen,
            )
            listing_jobs_found += rendered_count
            provisional_rows_found += rendered_provisional
            if listing_jobs_found > 0 and has_job_like_title and provisional_rows_found == 0:
                _record_listing_only_rendered_meta(ctx)
                continue
        _collect_listing_detail_links(
            ctx,
            detail_links,
            detail_seen,
            listing_html,
            page_url,
        )
    if (
        listing_jobs_found == 0
        and not detail_links
        and not provisional_rows_found
        and int(ctx.link_rejections.get("dead_listing_page", 0)) <= 0
    ):
        # Generic list-only fallback: block-structured listing with job-like headings
        # but no detail links (e.g. a server-rendered board with no per-role URLs).
        fallback_count = _append_block_title_fallback_rows(ctx, listing_htmls, page_url)
        if fallback_count:
            listing_jobs_found += fallback_count
            ctx.entry_report["classification"] = "ok_with_jobs"
            ctx.entry_report["extractorHint"] = "block_title_fallback"
    return listing_jobs_found, detail_links, provisional_rows_found
