"""Static plugin for Nintendo / CSOD careers pages."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import html_fragment_lines, strip_html_text
from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.provider_parsers import normalize_location_details
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, normalize_url

_HOSTS = frozenset({"jobs.nintendo.de", "nintendoeurope.csod.com"})
_IGNORED_TOKENS = frozenset(
    {
        "apply",
        "details",
        "learn more",
        "read more",
        "view job",
        "view details",
        "more details",
    }
)
_ANCHOR_RE = re.compile(
    r"(?is)<a\b(?P<attrs>[^>]*)href\s*=\s*(?P<quote>[\"\'])(?P<href>.*?)(?P=quote)(?P<tail>[^>]*)>(?P<body>.*?)</a>"
)
_LI_RE = re.compile(
    r'(?is)<li\b[^>]*class\s*=\s*(?P<quote>["\'])(?P<class>.*?)(?P=quote)[^>]*>(?P<body>.*?)</li>'
)


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in _HOSTS


def _window(text: str, start: int, end: int, *, pad: int = 2000) -> str:
    left = max(0, start - pad)
    right = min(len(text), end + pad)
    return text[left:right]


def _pick_title(window_before: str, window: str, anchor_body: str) -> str:
    title = clean_text(strip_html_text(anchor_body))
    if title and title.lower() not in _IGNORED_TOKENS:
        return title
    headings = list(re.finditer(r"(?is)<h[1-6]\b[^>]*>(.*?)</h[1-6]>", window_before or ""))
    for match in reversed(headings):
        heading = clean_text(strip_html_text(match.group(1) or ""))
        if heading and heading.lower() not in _IGNORED_TOKENS:
            return heading
    for line in html_fragment_lines(window):
        heading = clean_text(line)
        if not heading or heading.lower() in _IGNORED_TOKENS:
            continue
        if heading.lower().startswith(("location", "term", "type", "contract")):
            continue
        return heading
    return ""


def _extract_from_li_blocks(
    html: str, *, page_url: str, company: str, source_id: str
) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen_links: set[str] = set()
    for li_match in _LI_RE.finditer(html or ""):
        li_class = clean_text(li_match.group("class"))
        li_html = li_match.group("body") or ""
        if "SearchResult_job_item" not in li_class and "target-req" not in li_html:
            continue
        href_match = re.search(
            r"(?is)<a\b[^>]*href\s*=\s*(?P<quote>[\"\'])(?P<href>.*?)(?P=quote)[^>]*>\s*Details\s*</a>",
            li_html,
        )
        if not href_match:
            continue
        href = clean_text(href_match.group("href"))
        if "target-req" not in href.lower():
            continue
        link = normalize_url(urljoin(page_url, href))
        if not link or link in seen_links:
            continue
        title_match = re.search(r"(?is)<h[1-6]\b[^>]*>(.*?)</h[1-6]>", li_html)
        title = clean_text(strip_html_text(title_match.group(1) or "")) if title_match else ""
        if not title:
            for line in html_fragment_lines(li_html):
                candidate = clean_text(line)
                if candidate and candidate.lower() not in _IGNORED_TOKENS:
                    title = candidate
                    break
        if not title:
            continue
        location = ""
        work_type = ""
        contract_type = ""
        location_match = re.search(
            r"(?is)<p\b[^>]*class\s*=\s*(?P<quote>[\"\'])SearchResult_job_item__location[^\"\']*(?P=quote)[^>]*>(.*?)</p>",
            li_html,
        )
        if location_match:
            location = clean_text(strip_html_text(location_match.group(2) or ""))
        if not location:
            for line in html_fragment_lines(li_html):
                lower = line.lower()
                if " | " in line or "," in line or "germany" in lower or "netherlands" in lower:
                    location = line
                    break
        sub_match = re.search(
            r"(?is)<ul\b[^>]*class\s*=\s*(?P<quote>[\"\'])SearchResult_job_item__sub[^\"\']*(?P=quote)[^>]*>(.*?)</ul>",
            li_html,
        )
        if sub_match:
            sub_items = [
                clean_text(strip_html_text(item))
                for item in re.findall(r"(?is)<li\b[^>]*>(.*?)</li>", sub_match.group(2) or "")
            ]
            sub_items = [item for item in sub_items if item]
            for item in sub_items:
                lower = item.lower()
                if not work_type and any(
                    token in lower for token in ("full-time", "part-time", "remote", "hybrid")
                ):
                    work_type = item
                    continue
                if not contract_type and any(
                    token in lower for token in ("temporary", "permanent", "contract", "fixed-term")
                ):
                    contract_type = item
                    continue
        seen_links.add(link)
        location_details = normalize_location_details(location)
        jobs.append(
            {
                "sourceJobId": f"static:{source_id}:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": clean_text(location_details.get("city")),
                "country": clean_text(location_details.get("country")) or "Unknown",
                "workType": work_type,
                "contractType": contract_type,
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
                "adapter": "static",
                "studio": company,
                "source": "",
                "locations": location_details.get("locations") or [],
                "locationSummary": clean_text(location_details.get("locationSummary")),
            }
        )
    return jobs


def _extract_jobs(html: str, *, page_url: str, company: str, source_id: str) -> list[RawJob]:
    jobs = _extract_from_li_blocks(html, page_url=page_url, company=company, source_id=source_id)
    if jobs:
        return jobs
    seen_links: set[str] = set()
    for match in _ANCHOR_RE.finditer(html or ""):
        attrs = clean_text(match.group("attrs"))
        href = clean_text(match.group("href"))
        anchor_body = clean_text(strip_html_text(match.group("body") or ""))
        if not href:
            continue
        if "target-req" not in f"{attrs} {href}".lower():
            continue
        link = normalize_url(urljoin(page_url, href))
        if not link or link in seen_links:
            continue
        window = _window(html, match.start(), match.end())
        title = _pick_title(html[: match.start()], window, anchor_body)
        if not title:
            continue
        lines = [clean_text(line) for line in html_fragment_lines(window) if clean_text(line)]
        location = ""
        work_type = ""
        contract_type = ""
        for line in lines:
            lower = line.lower()
            if line == title or line.lower() in _IGNORED_TOKENS:
                continue
            if not location and ("," in line or "germany" in lower or "netherlands" in lower):
                location = line
                continue
            if not work_type and any(
                token in lower for token in ("full time", "part time", "hybrid", "remote")
            ):
                work_type = line
                continue
            if not contract_type and any(
                token in lower for token in ("permanent", "contract", "temporary", "fixed term")
            ):
                contract_type = line
                continue
        seen_links.add(link)
        location_details = normalize_location_details(location)
        jobs.append(
            {
                "sourceJobId": f"static:{source_id}:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": clean_text(location_details.get("city")),
                "country": clean_text(location_details.get("country")) or "Unknown",
                "workType": work_type,
                "contractType": contract_type,
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
                "adapter": "static",
                "studio": company,
                "source": "",
                "locations": location_details.get("locations") or [],
                "locationSummary": clean_text(location_details.get("locationSummary")),
            }
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
        or "Nintendo"
    )
    source_id = (source_row.get("id") or "").strip() or "nintendo_csod"

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
            "js_shell_detected" if browser_recommended else "nintendo_listing_present_but_empty"
        ),
        detail_fetch_required=False,
        detail_traversal_mode="listing_only",
    )
    return []
