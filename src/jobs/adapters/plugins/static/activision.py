from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse, urlunparse

from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text
from src.scrapers.domain_profiles import domain_profile_for_url


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("careers.activision.com",)


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
    # Jobs list is at /search-results; use it when the source only has the root URL.
    profile = domain_profile_for_url(page_url)
    canonical_path = clean_text(profile.get("canonical_listing_path"))
    if canonical_path and canonical_path.startswith("/"):
        parsed = urlparse(page_url)
        path = clean_text(parsed.path)
        if not path or path == "/":
            page_url = urlunparse(
                (parsed.scheme or "https", parsed.netloc, canonical_path, "", "", "")
            )

    company = (
        clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name"))
        or "Activision"
    )
    source_id = (source_row.get("id") or "").strip() or "activision"

    try:
        html = fetch_text(page_url, timeout_s)
    except Exception as exc:  # noqa: BLE001
        classification, recommend = _heuristics.classify_fetch_exception(exc)
        source_row["_staticPluginMeta"] = {
            "classification": classification,
            "browserFallbackRecommended": bool(recommend),
            "extractorHint": "fetch_failed",
            "error": str(exc),
        }
        return []

    ats_links = _heuristics.detect_outbound_ats_links(html, base_url=page_url)
    if _heuristics.detect_js_shell(html):
        source_row["_staticPluginMeta"] = {
            "classification": _heuristics.CLASSIFICATION_BLOCKED_OR_CHALLENGE,
            "browserFallbackRecommended": True,
            "extractorHint": "js_shell_detected",
            "atsLinks": ats_links[:5],
        }
        return []

    rows = parse_jobpostings_from_html(
        html,
        base_url=page_url,
        fallback_company=company,
        fallback_source_id_prefix=f"static:{source_id}",
    )
    if not rows and callable(try_playwright):
        browser_html, _ = try_playwright(page_url, max(3, min(timeout_s, 20)))
        if browser_html:
            html = browser_html
            rows = parse_jobpostings_from_html(
                html,
                base_url=page_url,
                fallback_company=company,
                fallback_source_id_prefix=f"static:{source_id}",
            )
    if not rows:
        seen = set()
        for match in re.finditer(
            r'(?is)<a[^>]+href=["\']([^"\']+/job/[^"\']+)["\'][^>]*>(.*?)</a>', html
        ):
            href = clean_text(match.group(1))
            title = clean_text(re.sub(r"(?is)<[^>]+>", " ", match.group(2) or ""))
            if not href or not title:
                continue
            link = href
            if link in seen:
                continue
            seen.add(link)
            rows.append(
                {
                    "sourceJobId": f"static:{source_id}:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
                    "title": title,
                    "company": company,
                    "city": "",
                    "country": "Unknown",
                    "workType": "",
                    "contractType": "",
                    "jobLink": link,
                    "sector": "Game",
                    "postedAt": "",
                }
            )
    for row in rows:
        if isinstance(row, dict):
            row["adapter"] = "static"
            row["studio"] = company
            row["source"] = clean_text(source_row.get("name")) or "activision"
    cleaned = [r for r in rows if isinstance(r, dict)]
    if not cleaned:
        if _heuristics.detect_no_openings(html):
            source_row["_staticPluginMeta"] = {
                "classification": _heuristics.CLASSIFICATION_EMPTY_CONFIRMED,
                "browserFallbackRecommended": False,
                "emptyConfirmed": True,
                "extractorHint": "explicit_no_openings_marker",
                "atsLinks": ats_links[:5],
            }
        else:
            source_row["_staticPluginMeta"] = {
                "classification": _heuristics.CLASSIFICATION_PARSER_STALE,
                "browserFallbackRecommended": False,
                "extractorHint": "search_results_present_but_plugin_empty",
                "atsLinks": ats_links[:5],
                "detailFetchRequired": False,
                "detailTraversalMode": "listing_only",
            }
    else:
        source_row["_staticPluginMeta"] = {
            "detailFetchRequired": False,
            "detailTraversalMode": "listing_only",
        }
    return cleaned
