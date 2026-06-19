"""Reusable static plugin for thin ATS-wrapper pages."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.static._rendered_cards import extract_rendered_card_jobs
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.static_runtime_support import is_static_fetch_fallback_exception
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

_EXPECTED_ATS_WRAPPER_FETCH_EXCEPTIONS = (OSError, RuntimeError, ValueError)

_HOSTS = frozenset(
    {
        "naughtydog.com",
        "www.naughtydog.com",
        "jobs.zenimax.com",
    }
)
_ATS_HREF_TOKENS = (
    "greenhouse",
    "jobvite",
    "applytojob",
    "smartrecruiters",
    "lever",
    "workday",
    "bamboohr",
    "personio",
    "ashby",
    "teamtailor",
    "recruitee",
    "pinpoint",
    "target-req",
    "gh_jid",
    "jid",
)


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in _HOSTS


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    **kwargs: Any,
) -> list[RawJob]:
    _ = (retries, backoff_s, kwargs)
    if not pages:
        return []
    page_url = clean_text(pages[0])
    if not page_url:
        return []
    company = (
        clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name"))
        or "Unknown"
    )
    source_id = (source_row.get("id") or "").strip() or "ats_wrapper"

    try:
        html = fetch_text(page_url, timeout_s)
    except _EXPECTED_ATS_WRAPPER_FETCH_EXCEPTIONS as exc:
        if not is_static_fetch_fallback_exception(exc):
            raise
        classification, recommend = _heuristics.classify_fetch_exception(exc)
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            classification,
            browser_fallback_recommended=bool(recommend),
            extractor_hint="fetch_failed",
            error=str(exc),
        )
        return []

    rows = extract_rendered_card_jobs(
        html,
        page_url=page_url,
        company=company,
        source_id=source_id,
        href_tokens=_ATS_HREF_TOKENS,
        allow_any_anchor=True,
    )
    if not rows and callable(try_playwright):
        browser_html, _ = try_playwright(page_url, max(3, min(timeout_s, 25)))
        if browser_html:
            html = browser_html
            rows = extract_rendered_card_jobs(
                html,
                page_url=page_url,
                company=company,
                source_id=source_id,
                href_tokens=_ATS_HREF_TOKENS,
                allow_any_anchor=True,
            )

    if rows:
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_OK_WITH_JOBS,
            detail_fetch_required=False,
            detail_traversal_mode="listing_only",
        )
        source_name = clean_text(source_row.get("name")) or company
        for row in rows:
            row["source"] = source_name
        return rows

    ats_links = _heuristics.detect_outbound_ats_links(html, base_url=page_url)
    if _heuristics.detect_no_openings(html):
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_EMPTY_CONFIRMED,
            browser_fallback_recommended=False,
            empty_confirmed=True,
            extractor_hint="explicit_no_openings_marker",
            ats_links=ats_links,
            detail_fetch_required=False,
            detail_traversal_mode="listing_only",
        )
        return []

    source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
        _heuristics.CLASSIFICATION_SITE_CHANGED
        if ats_links
        else _heuristics.CLASSIFICATION_PARSER_STALE,
        browser_fallback_recommended=False,
        extractor_hint=(
            "ats_wrapper_present_but_empty"
            if ats_links
            else "ats_wrapper_listing_present_but_empty"
        ),
        ats_links=ats_links,
        detail_fetch_required=False,
        detail_traversal_mode="listing_only",
    )
    return []
