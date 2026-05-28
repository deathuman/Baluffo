from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse, urlunparse

from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticContext,
    SimpleStaticPlugin,
    simple_static_run,
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


def _activision_anchor_rows(*, html: str, company: str, source_id: str) -> list[RawJob]:
    rows: list[RawJob] = []
    seen: set[str] = set()
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
    if not pages or not callable(parse_jobpostings_from_html):
        return []
    page_url = _canonical_page_url(pages[0] if pages else "")
    if not page_url:
        return []

    def _parse_html(ctx: SimpleStaticContext) -> list[dict[str, Any]]:
        rows = ctx.parse_jobpostings_from_html(
            ctx.html,
            base_url=ctx.page_url,
            fallback_company=ctx.company,
            fallback_source_id_prefix=f"static:{ctx.source_id}",
        )
        if rows:
            return rows
        if callable(try_playwright):
            browser_html, _ = try_playwright(ctx.page_url, max(3, min(timeout_s, 20)))
            if browser_html:
                rows = ctx.parse_jobpostings_from_html(
                    browser_html,
                    base_url=ctx.page_url,
                    fallback_company=ctx.company,
                    fallback_source_id_prefix=f"static:{ctx.source_id}",
                )
                if rows:
                    return rows
        return _activision_anchor_rows(html=ctx.html, company=ctx.company, source_id=ctx.source_id)

    return simple_static_run(
        spec=SimpleStaticPlugin(
            source_id="activision",
            default_company="Activision",
            playwright_on_js_shell=True,
            parser_stale_hint="search_results_present_but_plugin_empty",
            empty_detail_fetch_required=False,
            empty_detail_traversal_mode="listing_only",
        ),
        parse_html=_parse_html,
    )(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        pages=[page_url],
        source_row=source_row,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        try_playwright=try_playwright,
        **kwargs,
    )
