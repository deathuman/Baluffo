from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text
from src.scrapers import domain_profiles


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("supercell.com", "www.supercell.com")


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
    _ = (retries, backoff_s, kwargs)
    if not pages or not callable(parse_jobpostings_from_html):
        return []
    page_url = clean_text(pages[0])
    if not page_url:
        return []

    company = clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name")) or "Supercell"
    source_id = (source_row.get("id") or "").strip() or "supercell"

    html = ""
    try:
        html = fetch_text(page_url, timeout_s)
    except Exception as exc:  # noqa: BLE001
        if try_playwright:
            html, _ = try_playwright(page_url, max(3, min(timeout_s, 30)))
        if not html:
            classification, recommend = _heuristics.classify_fetch_exception(exc)
            source_row["_staticPluginMeta"] = {
                "classification": classification,
                "browserFallbackRecommended": bool(recommend),
                "extractorHint": "fetch_failed",
                "error": str(exc),
            }
            return []

    ats_links = _heuristics.detect_outbound_ats_links(html, base_url=page_url) if html else []
    if html and _heuristics.detect_js_shell(html) and try_playwright:
        html2, _ = try_playwright(page_url, max(3, min(timeout_s, 30)))
        if html2:
            html = html2
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
    if not rows and html:
        profile = domain_profiles.domain_profile_for_url(page_url)
        base_host = (urlparse(page_url).netloc or "").lower()
        seen_links: set[str] = set()
        for match in re.finditer(
            r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
            html,
        ):
            href = clean_text(match.group(1))
            anchor_inner = match.group(2) or ""
            anchor_text = strip_html_text(
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
                "title": anchor_text[:200],
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
            row["source"] = clean_text(source_row.get("name")) or "supercell"
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
            # If the HTML path yields nothing, escalate to browser fallback for this site.
            # This avoids repeated extract-zero failures on JS-rendered listings.
            likely_js = _heuristics.detect_js_shell(html) or _heuristics.visible_text_len(html) < 400
            source_row["_staticPluginMeta"] = {
                "classification": _heuristics.CLASSIFICATION_BLOCKED_OR_CHALLENGE if likely_js else _heuristics.CLASSIFICATION_FETCH_OK_EXTRACT_ZERO,
                "browserFallbackRecommended": True,
                "extractorHint": "parse_empty_js_shell_suspected" if likely_js else "parse_empty",
                "atsLinks": ats_links[:5],
            }
    return cleaned

