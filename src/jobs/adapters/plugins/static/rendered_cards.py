"""Reusable static plugin for rendered card/list-style career pages."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.static._rendered_cards import extract_rendered_card_jobs
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.page_gating import classify_job_page
from src.jobs.text_utils import clean_text

_HOSTS = frozenset(
    {
        "workwithindies.com",
        "www.workwithindies.com",
        "romerogames.com",
        "www.romerogames.com",
        "starbreeze.com",
        "www.starbreeze.com",
        "stepico.com",
        "www.stepico.com",
        "mobge.net",
        "www.mobge.net",
        "juegostudio.com",
        "www.juegostudio.com",
        "jetpackinteractive.ca",
        "www.jetpackinteractive.ca",
        "sozap.com",
        "www.sozap.com",
        "smokingguninc.com",
        "www.smokingguninc.com",
        "sybogames.com",
        "www.sybogames.com",
        "whatwapp.com",
        "www.whatwapp.com",
        "kinaliworks.com",
        "www.kinaliworks.com",
        "applovin.com",
        "www.applovin.com",
        "offroadgames.co",
        "www.offroadgames.co",
        "zenosinteractive.com",
        "www.zenosinteractive.com",
        "careers.bohemia.net",
        "pixiongames.com",
        "www.pixiongames.com",
        "leartesstudios.com",
        "www.leartesstudios.com",
        "jobs.moonrover.games",
        "gs-studio.eu",
        "www.gs-studio.eu",
        "careers.bungie.com",
        "hitberrygames.com",
        "www.hitberrygames.com",
        "purebang.com",
        "www.purebang.com",
        "ultra-factory.com",
        "www.ultra-factory.com",
        "rollicgames.com",
        "www.rollicgames.com",
    }
)


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in _HOSTS


def _fetch_listing_html(
    *,
    fetch_text: Callable[[str, int], str],
    try_playwright: Callable[[str, int], tuple[str, str]] | None,
    page_url: str,
    timeout_s: int,
    source_row: dict[str, Any],
) -> str:
    try:
        return fetch_text(page_url, timeout_s)
    except Exception as exc:  # noqa: BLE001
        classification, recommend = _heuristics.classify_fetch_exception(exc)
        if callable(try_playwright) and recommend:
            html, _ = try_playwright(page_url, max(3, min(timeout_s, 25)))
            if html:
                source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
                    _heuristics.CLASSIFICATION_JS_REQUIRED,
                    browser_fallback_recommended=True,
                    extractor_hint="fetch_failed_browser_rendered",
                    error=str(exc),
                )
                return html
            source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
                classification,
                browser_fallback_recommended=True,
                extractor_hint="fetch_failed_browser_empty",
                error=str(exc),
            )
            return ""
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            classification,
            browser_fallback_recommended=bool(recommend),
            extractor_hint="fetch_failed",
            error=str(exc),
        )
        return ""


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
    source_id = (source_row.get("id") or "").strip() or "rendered_cards"

    html = _fetch_listing_html(
        fetch_text=fetch_text,
        try_playwright=try_playwright,
        page_url=page_url,
        timeout_s=timeout_s,
        source_row=source_row,
    )
    if not html:
        return []

    rows = extract_rendered_card_jobs(
        html,
        page_url=page_url,
        company=company,
        source_id=source_id,
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
            if isinstance(row, dict):
                row.pop("_renderedCardMode", None)
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

    job_like, gate_reason = classify_job_page(
        html,
        page_url,
        profile=source_row if isinstance(source_row, dict) else None,
    )
    if not job_like and gate_reason == "dead_listing_page":
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_DEAD_LISTING_PAGE,
            browser_fallback_recommended=False,
            extractor_hint="regular_page_rejected",
            ats_links=ats_links,
            detail_fetch_required=False,
            detail_traversal_mode="listing_only",
            deadListingPageCount=1,
            deadListingPageExamples=[f"{page_url} | {company}"],
        )
        return []

    likely_js = _heuristics.detect_js_shell(html) or _heuristics.visible_text_len(html) < 400
    source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
        _heuristics.CLASSIFICATION_JS_REQUIRED
        if likely_js
        else _heuristics.CLASSIFICATION_PARSER_STALE,
        browser_fallback_recommended=bool(likely_js),
        extractor_hint=(
            "rendered_cards_js_shell_suspected"
            if likely_js
            else "rendered_cards_listing_present_but_empty"
        ),
        ats_links=ats_links,
        detail_fetch_required=False,
        detail_traversal_mode="listing_only",
    )
    return []
