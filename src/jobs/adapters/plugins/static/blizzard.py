"""Static plugin for Blizzard Entertainment careers (careers.blizzard.com)."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.plugins.static import phapp as _phapp
from src.jobs.adapters.plugins.static._runner import (
    fetch_static_plugin_html,
    first_static_page,
    record_static_plugin_empty_parse,
    stamp_static_plugin_rows,
    static_plugin_blocked_by_js_shell,
    static_plugin_context_values,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.provider_parsers import parse_generic_location_fields
from src.jobs.adapters.static_runtime_support import fetch_static_html_or_none
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, normalize_url


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("careers.blizzard.com", "www.careers.blizzard.com")


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
    company, source_id, source_name = static_plugin_context_values(
        source_row=source_row,
        default_company="Blizzard Entertainment",
        default_source_id="blizzard",
        default_source_name="blizzard",
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
        # The production jobsite is a JS shell: the dedicated parse can't see the
        # jobs, so fall back to the shared phApp sitemap recovery path.
        return _phapp.run(
            fetch_text=fetch_text,
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
            pages=pages,
            source_row=source_row,
            **kwargs,
        )

    rows = parse_jobpostings_from_html(
        html,
        base_url=page_url,
        fallback_company=company,
        fallback_source_id_prefix=f"static:{source_id}",
    )
    if not rows:
        rows = _collect_blizzard_jobs(
            fetch_text=fetch_text,
            timeout_s=timeout_s,
            page_url=page_url,
            listing_html=html,
            company=company,
            source_id=source_id,
        )
    if not rows and callable(try_playwright):
        browser_html, _ = try_playwright(page_url, max(3, min(timeout_s, 30)))
        if browser_html:
            rows = _collect_blizzard_jobs(
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                page_url=page_url,
                listing_html=browser_html,
                company=company,
                source_id=source_id,
            )
    cleaned = stamp_static_plugin_rows(rows=rows, company=company, source_name=source_name)
    if not cleaned:
        recovered = _phapp.run(
            fetch_text=fetch_text,
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
            pages=pages,
            source_row=source_row,
            **kwargs,
        )
        if recovered:
            return recovered
        record_static_plugin_empty_parse(html=html, page_url=page_url, source_row=source_row)
    return cleaned


def _extract_blizzard_role_links(html: str, page_url: str) -> list[str]:
    out: list[str] = []
    seen = set()
    for href in re.findall(r'(?is)<a[^>]+href=["\']([^"\']+)["\']', html):
        absolute = normalize_url(urljoin(page_url, clean_text(href)))
        if not absolute or absolute in seen:
            continue
        parsed = urlparse(absolute)
        path = (parsed.path or "").lower().rstrip("/")
        if not path.startswith("/global/en/"):
            continue
        if path in {
            "/global/en",
            "/global/en/home",
            "/global/en/search-results",
            "/global/en/jobcart",
            "/global/en/cookiesettings",
        }:
            continue
        tail = path.split("/global/en/", 1)[-1]
        if "/" in tail:
            continue
        seen.add(absolute)
        out.append(absolute)
    return out


def _extract_blizzard_search_results_links(html: str, page_url: str) -> list[str]:
    out: list[str] = []
    seen = set()
    for href in re.findall(r'(?is)<a[^>]+href=["\']([^"\']*search-results[^"\']*)["\']', html):
        absolute = normalize_url(urljoin(page_url, clean_text(href)))
        if not absolute or absolute in seen:
            continue
        seen.add(absolute)
        out.append(absolute)
    return out


def _parse_blizzard_search_results(*, html: str, company: str, source_id: str) -> list[RawJob]:
    rows: list[RawJob] = []
    seen = set()
    pattern = re.compile(
        r'(?is)<a[^>]+href=["\']([^"\']+/global/en/job/([^/"\']+)/[^"\']+)["\'][^>]*>(.*?)</a>'
    )
    for match in pattern.finditer(html):
        absolute = normalize_url(clean_text(match.group(1)))
        job_id = clean_text(match.group(2))
        title = strip_html_text(match.group(3))
        if not absolute or not title or absolute in seen:
            continue
        seen.add(absolute)
        context = html[max(0, match.start() - 200) : min(len(html), match.end() + 1600)]
        location_match = re.search(
            r"Location.*?([A-Z][A-Za-z .'-]+,\s*[A-Z][A-Za-z .'-]+(?:,\s*[A-Z][A-Za-z .'-]+)*)",
            strip_html_text(context),
            flags=re.I,
        )
        posted_match = re.search(
            r"Posted Date\s*([A-Za-z]+\s+\d{1,2}\s+\d{4})", strip_html_text(context), flags=re.I
        )
        location = clean_text(location_match.group(1)) if location_match else ""
        city, country, _ = parse_generic_location_fields(location)
        if not country and "United States" in location:
            country = "United States of America"
        rows.append(
            {
                "sourceJobId": f"static:{source_id}:{job_id or len(rows) + 1}",
                "title": title,
                "company": company,
                "city": city,
                "country": country or "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": absolute,
                "sector": "Game",
                "postedAt": clean_text(posted_match.group(1)) if posted_match else "",
            }
        )
    return rows


def _collect_blizzard_jobs(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    page_url: str,
    listing_html: str,
    company: str,
    source_id: str,
) -> list[RawJob]:
    rows: list[RawJob] = []
    seen_links = set()
    search_pages = _extract_blizzard_search_results_links(listing_html, page_url)
    if not search_pages:
        for role_url in _extract_blizzard_role_links(listing_html, page_url):
            role_html = fetch_static_html_or_none(fetch_text, role_url, timeout_s)
            if role_html is None:
                continue
            for search_url in _extract_blizzard_search_results_links(role_html, role_url):
                if search_url not in search_pages:
                    search_pages.append(search_url)
    for search_url in search_pages:
        results_html = fetch_static_html_or_none(fetch_text, search_url, timeout_s)
        if results_html is None:
            continue
        for row in _parse_blizzard_search_results(
            html=results_html,
            company=company,
            source_id=source_id,
        ):
            link = normalize_url(row.get("jobLink"))
            if not link or link in seen_links:
                continue
            seen_links.add(link)
            rows.append(row)
    return rows
