"""Static plugin for frontier.co.uk careers pages."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import html_fragment_lines, strip_html_text
from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.static._runner import static_listing_job_row
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.provider_parsers import (
    normalize_location_details,
    parse_generic_location_fields,
)
from src.jobs.adapters.static_runtime_support import is_static_fetch_fallback_exception
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, normalize_url

_EXPECTED_FRONTIER_FETCH_EXCEPTIONS = (OSError, RuntimeError, ValueError)

_HOSTS = frozenset({"frontier.co.uk", "www.frontier.co.uk"})
_IGNORED_TOKENS = frozenset(
    {
        "details",
        "apply",
        "apply now",
        "learn more",
        "read more",
        "view",
        "view details",
        "view job",
        "more details",
    }
)
_ANCHOR_RE = re.compile(
    r"(?is)<a\b(?P<attrs>[^>]*)href\s*=\s*(?P<quote>[\"\'])(?P<href>.*?)(?P=quote)(?P<tail>[^>]*)>(?P<body>.*?)</a>"
)
_LI_RE = re.compile(r"(?is)<li\b[^>]*>(.*?)</li>")


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in _HOSTS


def _window(text: str, start: int, end: int, *, pad: int = 2200) -> str:
    left = max(0, start - pad)
    right = min(len(text), end + pad)
    return text[left:right]


def _pick_title(window_before: str, window: str) -> str:
    headings = list(re.finditer(r"(?is)<h[1-6]\b[^>]*>(.*?)</h[1-6]>", window_before or ""))
    for match in reversed(headings):
        title = clean_text(strip_html_text(match.group(1) or ""))
        if title and title.lower() not in _IGNORED_TOKENS:
            return title
    for line in html_fragment_lines(window):
        title = clean_text(line)
        if not title or title.lower() in _IGNORED_TOKENS:
            continue
        if title.lower().startswith(("location", "term", "type", "contract")):
            continue
        return title
    return ""


def _pick_location_and_terms(window: str) -> tuple[str, str, str]:
    dd_values = [
        clean_text(strip_html_text(value))
        for value in re.findall(r"(?is)<dd\b[^>]*>(.*?)</dd>", window or "")
    ]
    dd_values = [value for value in dd_values if value]
    if not dd_values:
        return "", "", ""
    location = ""
    work_type = ""
    contract_type = ""
    for value in dd_values:
        location, work_type, contract_type = _apply_frontier_dd_value(
            value,
            location=location,
            work_type=work_type,
            contract_type=contract_type,
        )
    return location, work_type, contract_type


def _apply_frontier_dd_value(
    value: str, *, location: str, work_type: str, contract_type: str
) -> tuple[str, str, str]:
    kind, parsed_value = _frontier_dd_kind(value)
    if kind == "contract" and not contract_type:
        contract_type = value
    elif kind == "work_type" and not work_type:
        work_type = value
    elif kind == "location":
        location = value
    elif kind == "parsed_work_type" and not work_type:
        work_type = parsed_value
    elif not work_type:
        work_type = value
    return location, work_type, contract_type


def _frontier_dd_kind(value: str) -> tuple[str, str]:
    lower = value.lower()
    city, country, parsed_work_type = parse_generic_location_fields(value)
    if any(token in lower for token in ("full time", "part time", "fixed term", "permanent")):
        return "contract", ""
    if any(token in lower for token in ("remote", "hybrid", "onsite", "on site", "in person")):
        return "work_type", ""
    if city or country != "Unknown":
        return "location", ""
    if any(
        token in lower for token in ("contract", "permanent", "temporary", "fixed term", "intern")
    ):
        return "contract", ""
    if parsed_work_type:
        return "parsed_work_type", parsed_work_type
    return "fallback", ""


def _frontier_li_detail_href(li_html: str) -> str:
    href_match = re.search(
        r"(?is)<a\b[^>]*href\s*=\s*(?P<quote>[\"\'])(?P<href>.*?)(?P=quote)[^>]*>\s*Details\s*</a>",
        li_html,
    )
    href = clean_text(href_match.group("href")) if href_match else ""
    return href if href and "/careers/" in href.lower() else ""


def _frontier_li_detail_html(li_html: str) -> str:
    detail_match = re.search(
        r"(?is)<div\b[^>]*class\s*=\s*(?P<quote>[\"\'])c-careers-job-listing__department-list-detail(?P=quote)[^>]*>(.*?)</div>",
        li_html,
    )
    return detail_match.group(2) if detail_match else li_html


def _frontier_title_from_detail_html(detail_html: str) -> str:
    heading_match = re.search(r"(?is)<h[1-6]\b[^>]*>(.*?)</h[1-6]>", detail_html)
    if heading_match:
        return clean_text(strip_html_text(heading_match.group(1) or ""))
    for line in html_fragment_lines(detail_html):
        candidate = clean_text(line)
        if candidate and candidate.lower() not in _IGNORED_TOKENS:
            return candidate
    return ""


def _frontier_job_row(
    *,
    source_id: str,
    link: str,
    title: str,
    company: str,
    detail_html: str,
) -> RawJob:
    location, work_type, contract_type = _pick_location_and_terms(detail_html)
    location_details = normalize_location_details(location)
    return static_listing_job_row(
        source_id=source_id,
        link=link,
        title=title,
        company=company,
        city=clean_text(location_details.get("city")),
        country=clean_text(location_details.get("country")) or "Unknown",
        work_type=work_type,
        contract_type=contract_type,
    )


def _extract_from_li_blocks(
    html: str, *, page_url: str, company: str, source_id: str
) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen_links: set[str] = set()
    for li_match in _LI_RE.finditer(html or ""):
        li_html = li_match.group(1) or ""
        if "Details" not in li_html and "details" not in li_html.lower():
            continue
        href = _frontier_li_detail_href(li_html)
        if not href:
            continue
        link = normalize_url(urljoin(page_url, href))
        if not link or link in seen_links:
            continue
        detail_html = _frontier_li_detail_html(li_html)
        title = _frontier_title_from_detail_html(detail_html)
        if not title:
            continue
        seen_links.add(link)
        jobs.append(
            _frontier_job_row(
                source_id=source_id,
                link=link,
                title=title,
                company=company,
                detail_html=detail_html,
            )
        )
    return jobs


def _extract_jobs(html: str, *, page_url: str, company: str, source_id: str) -> list[RawJob]:
    jobs = _extract_from_li_blocks(html, page_url=page_url, company=company, source_id=source_id)
    if jobs:
        return jobs
    seen_links: set[str] = set()
    for match in _ANCHOR_RE.finditer(html or ""):
        href = clean_text(match.group("href"))
        body = clean_text(strip_html_text(match.group("body") or ""))
        if not href:
            continue
        if "details" not in body.lower():
            continue
        if "/careers/jobs" not in href.lower():
            continue
        link = normalize_url(urljoin(page_url, href))
        if not link or link in seen_links:
            continue
        window = _window(html, match.start(), match.end())
        title = _pick_title(html[: match.start()], window)
        if not title:
            continue
        location, work_type, contract_type = _pick_location_and_terms(window)
        location_details = normalize_location_details(location)
        seen_links.add(link)
        jobs.append(
            static_listing_job_row(
                source_id=source_id,
                link=link,
                title=title,
                company=company,
                city=clean_text(location_details.get("city")),
                country=clean_text(location_details.get("country")) or "Unknown",
                work_type=work_type,
                contract_type=contract_type,
            )
        )
    return jobs


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
        or "Frontier"
    )
    source_id = (source_row.get("id") or "").strip() or "frontier"

    try:
        html = fetch_text(page_url, timeout_s)
    except _EXPECTED_FRONTIER_FETCH_EXCEPTIONS as exc:
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

    rows = _extract_jobs(html, page_url=page_url, company=company, source_id=source_id)
    if not rows and callable(try_playwright):
        browser_html, _ = try_playwright(page_url, max(3, min(timeout_s, 25)))
        if browser_html:
            html = browser_html
            rows = _extract_jobs(html, page_url=page_url, company=company, source_id=source_id)

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

    if _heuristics.detect_no_openings(html):
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_EMPTY_CONFIRMED,
            browser_fallback_recommended=False,
            empty_confirmed=True,
            extractor_hint="explicit_no_openings_marker",
            detail_fetch_required=False,
            detail_traversal_mode="listing_only",
        )
        return []

    browser_recommended = bool(_heuristics.detect_js_shell(html))
    source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
        _heuristics.CLASSIFICATION_JS_REQUIRED
        if browser_recommended
        else _heuristics.CLASSIFICATION_PARSER_STALE,
        browser_fallback_recommended=browser_recommended,
        extractor_hint=(
            "js_shell_detected" if browser_recommended else "frontier_listing_present_but_empty"
        ),
        detail_fetch_required=False,
        detail_traversal_mode="listing_only",
    )
    return []
