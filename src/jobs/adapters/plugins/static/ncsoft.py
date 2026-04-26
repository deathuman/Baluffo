"""Static plugin for NCSoft North America careers."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.parsers.location import normalize_location_details
from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, normalize_url

_JOB_LINK_RE = re.compile(
    r'(?is)<a[^>]+href=["\']([^"\']*/(?:en-us|en-US)/careers/\d+[^"\']*)["\'][^>]*>(.*?)</a>'
)
_HEADING_RE = re.compile(r"(?is)<h[1-2][^>]*>(.*?)</h[1-2]>")
_TITLE_RE = re.compile(r"(?is)<title[^>]*>(.*?)</title>")
_US_STATE_CODES = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "IA",
    "ID",
    "IL",
    "IN",
    "KS",
    "KY",
    "LA",
    "MA",
    "MD",
    "ME",
    "MI",
    "MN",
    "MO",
    "MS",
    "MT",
    "NC",
    "ND",
    "NE",
    "NH",
    "NJ",
    "NM",
    "NV",
    "NY",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VA",
    "VT",
    "WA",
    "WI",
    "WV",
    "WY",
}


def can_handle(ctx: AdapterPluginContext) -> bool:
    return (ctx.source_identity or "").strip().lower() == "nca.ncsoft.com"


def _fetch_html(
    *,
    fetch_text: Callable[[str, int], str],
    fetch_html_cached: Callable[..., tuple[str, bool]] | None,
    url: str,
    timeout_s: int,
) -> str:
    if callable(fetch_html_cached):
        html, _ = fetch_html_cached(url)
        return html
    return fetch_text(url, timeout_s)


def _job_links(html: str, page_url: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for href, body in _JOB_LINK_RE.findall(html or ""):
        link = normalize_url(urljoin(page_url, clean_text(href))) or ""
        if not link or link in seen:
            continue
        seen.add(link)
        out.append((link, strip_html_text(body)))
    return out


def _first_meaningful_line(text: str) -> str:
    for raw_line in re.split(r"[\r\n|]+", text or ""):
        line = clean_text(raw_line)
        if not line:
            continue
        lower = line.lower()
        if lower in {"careers", "apply", "apply now", "job description"}:
            continue
        if re.fullmatch(r"[A-Za-z .'-]+,\s*[A-Z]{2}(?:\s*·\s*.+)?", line):
            continue
        return clean_text(re.split(r"\s+·\s+", line, maxsplit=1)[0])
    return ""


def _detail_title(detail_html: str, fallback: str) -> str:
    for pattern in (_HEADING_RE, _TITLE_RE):
        for match in pattern.findall(detail_html or ""):
            title = _first_meaningful_line(strip_html_text(match))
            if title:
                return title
    return _first_meaningful_line(fallback) or clean_text(fallback)


def _detail_location(detail_html: str, fallback: str = "") -> dict[str, Any]:
    text_candidates = [
        strip_html_text(match)
        for match in re.findall(r"(?is)<(?:div|span|section|p|li)[^>]*>(.*?)</", detail_html or "")
    ]
    text_candidates.append(strip_html_text(detail_html or ""))
    location_text = ""
    for text in text_candidates:
        match = re.search(
            r"\b([A-Z][A-Za-z.'-]*(?:\s+[A-Z][A-Za-z.'-]*){0,2},\s*(?:[A-Z]{2}|United States|USA|Canada|South Korea))\b",
            text,
        )
        if match:
            location_text = clean_text(match.group(1))
            break
    if not location_text:
        location_text = clean_text(fallback)
    state_match = re.fullmatch(r"(.+),\s*([A-Z]{2})", location_text)
    if state_match and state_match.group(2) in _US_STATE_CODES:
        city = clean_text(state_match.group(1))
        return normalize_location_details({"city": city, "country": "US"})
    return normalize_location_details(location_text)


def _row(
    *,
    link: str,
    title: str,
    company: str,
    source_id: str,
    source_name: str,
    location_details: dict[str, Any],
) -> RawJob:
    return {
        "sourceJobId": f"static:{source_id}:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
        "title": title,
        "company": company,
        "city": clean_text(location_details.get("city")),
        "country": clean_text(location_details.get("country")) or "Unknown",
        "workType": "",
        "contractType": "",
        "jobLink": link,
        "sector": "Game",
        "postedAt": "",
        "adapter": "static",
        "studio": company,
        "source": source_name,
        "locations": location_details.get("locations") or [],
        "locationSummary": clean_text(location_details.get("locationSummary")),
    }


def _listing_html(
    *,
    fetch_text: Callable[[str, int], str],
    fetch_html_cached: Callable[..., tuple[str, bool]] | None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None,
    page_url: str,
    timeout_s: int,
    source_row: dict[str, Any],
) -> str:
    try:
        return _fetch_html(
            fetch_text=fetch_text,
            fetch_html_cached=fetch_html_cached,
            url=page_url,
            timeout_s=timeout_s,
        )
    except Exception as exc:  # noqa: BLE001
        classification, recommend = _heuristics.classify_fetch_exception(exc)
        if callable(try_playwright) and recommend:
            html, _ = try_playwright(page_url, max(3, min(timeout_s, 25)))
            return html
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            classification,
            browser_fallback_recommended=bool(recommend),
            extractor_hint="fetch_failed",
            error=str(exc),
        )
        return ""


def _rows_from_links(
    links: list[tuple[str, str]],
    *,
    fetch_text: Callable[[str, int], str],
    fetch_html_cached: Callable[..., tuple[str, bool]] | None,
    timeout_s: int,
    company: str,
    source_id: str,
    source_name: str,
) -> list[RawJob]:
    rows: list[RawJob] = []
    for link, anchor_text in links:
        try:
            detail_html = _fetch_html(
                fetch_text=fetch_text,
                fetch_html_cached=fetch_html_cached,
                url=link,
                timeout_s=timeout_s,
            )
        except Exception:  # noqa: BLE001
            detail_html = ""
        title = _detail_title(detail_html, anchor_text)
        if not title:
            continue
        rows.append(
            _row(
                link=link,
                title=title,
                company=company,
                source_id=source_id,
                source_name=source_name,
                location_details=_detail_location(detail_html, anchor_text),
            )
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
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    fetch_html_cached: Callable[..., tuple[str, bool]] | None = None,
    **kwargs: Any,
) -> list[RawJob]:
    _ = (retries, backoff_s, kwargs)
    if not pages:
        return []
    page_url = clean_text(pages[0])
    if not page_url:
        return []
    company = clean_text(source_row.get("company") or source_row.get("studio")) or "NCSoft"
    source_name = clean_text(source_row.get("name")) or company
    source_id = clean_text(source_row.get("id")) or "ncsoft"

    html = _listing_html(
        fetch_text=fetch_text,
        fetch_html_cached=fetch_html_cached,
        try_playwright=try_playwright,
        page_url=page_url,
        timeout_s=timeout_s,
        source_row=source_row,
    )
    if not html:
        return []

    links = _job_links(html, page_url)
    if not links and callable(try_playwright):
        browser_html, _ = try_playwright(page_url, max(3, min(timeout_s, 25)))
        if browser_html:
            html = browser_html
            links = _job_links(html, page_url)

    rows = _rows_from_links(
        links,
        fetch_text=fetch_text,
        fetch_html_cached=fetch_html_cached,
        timeout_s=timeout_s,
        company=company,
        source_id=source_id,
        source_name=source_name,
    )

    if rows:
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_OK_WITH_JOBS,
            detail_fetch_required=True,
            detail_traversal_mode="listing_links",
        )
        return rows

    source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
        _heuristics.CLASSIFICATION_JS_REQUIRED,
        browser_fallback_recommended=True,
        extractor_hint="ncsoft_listing_empty_after_browser",
    )
    return []
