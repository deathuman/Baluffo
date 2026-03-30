"""Static plugin for Blizzard Entertainment careers (careers.blizzard.com)."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.types import AdapterPluginContext
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
    page_url = clean_text(pages[0])
    if not page_url:
        return []

    company = (
        clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name"))
        or "Blizzard Entertainment"
    )
    source_id = (source_row.get("id") or "").strip() or "blizzard"

    try:
        html = fetch_text(page_url, timeout_s)
    except Exception as exc:  # noqa: BLE001
        classification, recommend = _heuristics.classify_fetch_exception(exc)
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            classification,
            browser_fallback_recommended=bool(recommend),
            extractor_hint="fetch_failed",
            error=str(exc),
        )
        return []

    ats_links = _heuristics.detect_outbound_ats_links(html, base_url=page_url)
    if _heuristics.detect_js_shell(html):
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_BLOCKED_OR_CHALLENGE,
            browser_fallback_recommended=True,
            extractor_hint="js_shell_detected",
            ats_links=ats_links,
        )
        return []

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
    for row in rows:
        if isinstance(row, dict):
            row["adapter"] = "static"
            row["studio"] = company
            row["source"] = clean_text(source_row.get("name")) or "blizzard"
    cleaned = [r for r in rows if isinstance(r, dict)]
    if not cleaned:
        if _heuristics.detect_no_openings(html):
            source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
                _heuristics.CLASSIFICATION_EMPTY_CONFIRMED,
                browser_fallback_recommended=False,
                empty_confirmed=True,
                extractor_hint="explicit_no_openings_marker",
                ats_links=ats_links,
            )
        else:
            likely_js = (
                _heuristics.detect_js_shell(html) or _heuristics.visible_text_len(html) < 400
            )
            source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
                _heuristics.CLASSIFICATION_BLOCKED_OR_CHALLENGE
                if likely_js
                else _heuristics.CLASSIFICATION_FETCH_OK_EXTRACT_ZERO,
                browser_fallback_recommended=True,
                extractor_hint="parse_empty_js_shell_suspected"
                if likely_js
                else "parse_empty",
                ats_links=ats_links,
            )
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
        city = clean_text(location.split(",", 1)[0]) if "," in location else location
        country = "United States of America" if "United States" in location else ""
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
            try:
                role_html = fetch_text(role_url, timeout_s)
            except Exception:
                continue
            for search_url in _extract_blizzard_search_results_links(role_html, role_url):
                if search_url not in search_pages:
                    search_pages.append(search_url)
    for search_url in search_pages:
        try:
            results_html = fetch_text(search_url, timeout_s)
        except Exception:
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
