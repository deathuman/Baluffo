from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse, urlunparse

from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.static._runner import (
    fetch_static_plugin_html,
    first_static_page,
    record_static_plugin_empty_parse,
    stamp_static_plugin_rows,
    static_plugin_blocked_by_js_shell,
    static_plugin_context_values,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text
from src.scrapers.domain_profiles import domain_profile_for_url


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("careers.activision.com",)


def _canonical_page_url(page_url: str) -> str:
    profile = domain_profile_for_url(page_url)
    canonical_path = clean_text(profile.get("canonical_listing_path"))
    if not canonical_path or not canonical_path.startswith("/"):
        return page_url
    parsed = urlparse(page_url)
    path = clean_text(parsed.path)
    if path and path != "/":
        return page_url
    return urlunparse((parsed.scheme or "https", parsed.netloc, canonical_path, "", "", ""))


def _generic_rows(
    *,
    html: str,
    page_url: str,
    company: str,
    source_id: str,
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    return parse_jobpostings_from_html(
        html,
        base_url=page_url,
        fallback_company=company,
        fallback_source_id_prefix=f"static:{source_id}",
    )


def _activision_anchor_rows(*, html: str, company: str, source_id: str) -> list[RawJob]:
    rows: list[RawJob] = []
    seen = set()
    for match in re.finditer(
        r'(?is)<a[^>]+href=["\']([^"\']+/job/[^"\']+)["\'][^>]*>(.*?)</a>', html
    ):
        href = clean_text(match.group(1))
        title = clean_text(re.sub(r"(?is)<[^>]+>", " ", match.group(2) or ""))
        if not href or not title or href in seen:
            continue
        seen.add(href)
        rows.append(
            {
                "sourceJobId": f"static:{source_id}:{hashlib.sha1(href.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": href,
                "sector": "Game",
                "postedAt": "",
            }
        )
    return rows


def _record_activision_empty(*, html: str, page_url: str, source_row: dict[str, Any]) -> None:
    ats_links = _heuristics.detect_outbound_ats_links(html, base_url=page_url)
    if _heuristics.detect_no_openings(html):
        record_static_plugin_empty_parse(html=html, page_url=page_url, source_row=source_row)
        return
    source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
        _heuristics.CLASSIFICATION_PARSER_STALE,
        browser_fallback_recommended=False,
        extractor_hint="search_results_present_but_plugin_empty",
        ats_links=ats_links,
        detail_fetch_required=False,
        detail_traversal_mode="listing_only",
    )


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    **kwargs: Any,
) -> list[RawJob]:
    _ = (retries, backoff_s, kwargs)
    if not pages or not callable(parse_jobpostings_from_html):
        return []
    page_url = first_static_page(pages)
    if not page_url:
        return []
    page_url = _canonical_page_url(page_url)
    company, source_id, source_name = static_plugin_context_values(
        source_row=source_row,
        default_company="Activision",
        default_source_id="activision",
        default_source_name="activision",
    )
    html = fetch_static_plugin_html(
        fetch_text=fetch_text,
        page_url=page_url,
        timeout_s=timeout_s,
        source_row=source_row,
    )
    if not html or static_plugin_blocked_by_js_shell(
        html=html,
        page_url=page_url,
        source_row=source_row,
    ):
        return []

    rows = _generic_rows(
        html=html,
        page_url=page_url,
        company=company,
        source_id=source_id,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
    )
    if not rows and callable(try_playwright):
        browser_html, _ = try_playwright(page_url, max(3, min(timeout_s, 20)))
        if browser_html:
            html = browser_html
            rows = _generic_rows(
                html=html,
                page_url=page_url,
                company=company,
                source_id=source_id,
                parse_jobpostings_from_html=parse_jobpostings_from_html,
            )
    if not rows:
        rows = _activision_anchor_rows(html=html, company=company, source_id=source_id)
    cleaned = stamp_static_plugin_rows(rows=rows, company=company, source_name=source_name)
    if not cleaned:
        _record_activision_empty(html=html, page_url=page_url, source_row=source_row)
    else:
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_OK_WITH_JOBS,
            detail_fetch_required=False,
            detail_traversal_mode="listing_only",
        )
    return cleaned
