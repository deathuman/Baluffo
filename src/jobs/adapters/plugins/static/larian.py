"""
Static plugin for larian.com careers. Fetches first page and parses with
parse_jobpostings_from_html. Uses try_playwright when provided on fetch failure
or when HTML looks like a JS shell so JS-rendered listings can be parsed.
When JSON-LD returns no jobs, falls back to extracting job links from the page
(/careers/<uuid>) and building minimal job rows.
"""
from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from src.jobs import common
from src.jobs.adapters.plugins.static._heuristics import detect_js_shell
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.scrapers import domain_profiles


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity == "larian.com"


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: List[str],
    source_row: Dict[str, Any],
    parse_jobpostings_from_html: Callable[..., List[Dict[str, Any]]] | None = None,
    try_playwright: Optional[Callable[[str, int], Tuple[str, str]]] = None,
    **kwargs: Any,
) -> List[RawJob]:
    if not pages or not callable(parse_jobpostings_from_html):
        return []
    page_url = common.clean_text(pages[0])
    if not page_url:
        return []
    company = common.clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name")) or "Larian"
    source_id = (source_row.get("id") or "").strip() or "larian"
    html = ""
    try:
        html = fetch_text(page_url, timeout_s)
    except Exception:  # noqa: BLE001
        if try_playwright:
            html, _ = try_playwright(page_url, max(3, min(timeout_s, 30)))
        if not html:
            return []
    if try_playwright and html and detect_js_shell(html):
        html2, _ = try_playwright(page_url, max(3, min(timeout_s, 30)))
        if html2:
            html = html2
    rows = parse_jobpostings_from_html(
        html,
        base_url=page_url,
        fallback_company=company,
        fallback_source_id_prefix=f"static:{source_id}",
    )
    if not rows and html:
        profile = domain_profiles.domain_profile_for_url(page_url)
        base_host = (urlparse(page_url).netloc or "").lower()
        seen_links: set[str] = set()
        for match in re.finditer(
            r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
            html,
        ):
            href = common.clean_text(match.group(1))
            anchor_inner = match.group(2) or ""
            anchor_text = common.strip_html_text(
                re.sub(r"(?is)<[^>]+>", " ", anchor_inner)
            ).strip() or "Job"
            if not href:
                continue
            absolute = urljoin(page_url, href)
            if (urlparse(absolute).netloc or "").lower() != base_host:
                continue
            if not domain_profiles.is_probable_job_detail_url(absolute, profile):
                continue
            if absolute in seen_links:
                continue
            seen_links.add(absolute)
            rows.append({
                "title": anchor_text[: 200],
                "company": company,
                "jobLink": absolute,
                "sourceJobId": f"{source_id}:{absolute}",
                "city": "",
                "country": "",
                "workType": "",
                "contractType": "",
                "sector": "Game",
                "postedAt": "",
            })
    for row in rows:
        if isinstance(row, dict):
            row["adapter"] = "static"
            row["studio"] = company
            row["source"] = common.clean_text(source_row.get("name")) or "larian"
    return [r for r in rows if isinstance(r, dict)]
