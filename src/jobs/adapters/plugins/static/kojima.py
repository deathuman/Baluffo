from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, normalize_url, sanitize_public_text


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("www.kojimaproductions.jp", "kojimaproductions.jp")


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]] | None = None,
    maybe_fetch_kojima_job_listing_html: Callable[..., str] | None = None,
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
        or "Kojima Productions"
    )
    source_id = (source_row.get("id") or "").strip() or "kojima"

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

    # Prefer dynamic listing HTML if available; this is already a known special-case.
    try:
        if callable(maybe_fetch_kojima_job_listing_html):
            dynamic = maybe_fetch_kojima_job_listing_html(
                page_url=page_url,
                page_html=html,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
            )
            if dynamic and dynamic not in html:
                html = dynamic
    except Exception:
        # Fall back to the original HTML if the dynamic step fails.
        pass

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
    if not rows:
        rows = _parse_kojima_listing_rows(
            html=html,
            base_url=page_url,
            company=company,
            source_id=source_id,
        )
    if not rows and callable(try_playwright):
        browser_html, _ = try_playwright(page_url, max(3, min(timeout_s, 30)))
        if browser_html:
            rows = _parse_kojima_listing_rows(
                html=browser_html,
                base_url=page_url,
                company=company,
                source_id=source_id,
            )
    for row in rows:
        if isinstance(row, dict):
            row["adapter"] = "static"
            row["studio"] = company
            row["source"] = clean_text(source_row.get("name")) or "kojima"
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
            likely_js = (
                _heuristics.detect_js_shell(html) or _heuristics.visible_text_len(html) < 400
            )
            source_row["_staticPluginMeta"] = {
                "classification": _heuristics.CLASSIFICATION_BLOCKED_OR_CHALLENGE
                if likely_js
                else _heuristics.CLASSIFICATION_FETCH_OK_EXTRACT_ZERO,
                "browserFallbackRecommended": True,
                "extractorHint": "parse_empty_js_shell_suspected" if likely_js else "parse_empty",
                "atsLinks": ats_links[:5],
            }
    return cleaned


def _parse_kojima_listing_rows(
    *, html: str, base_url: str, company: str, source_id: str
) -> list[RawJob]:
    rows: list[RawJob] = []
    seen = set()
    excluded_paths = {
        "/en/careers",
        "/en/careers_interview",
        "/en/careers_faq",
        "/en/ourculture",
        "/en/new-kjpstudio",
        "/en/faqs",
        "/en/contact-us",
        "/en/terms-of-use",
        "/en/cookie-policy",
        "/en/newsletter-signup",
    }
    role_pattern = re.compile(
        r"(programmer|artist|designer|engineer|writer|specialist|relations|support)", flags=re.I
    )
    for href, inner in re.findall(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html):
        absolute = normalize_url(urljoin(base_url, clean_text(href)))
        if not absolute or absolute in seen:
            continue
        parsed = urlparse(absolute)
        path = (parsed.path or "").strip()
        if not path.startswith("/en/"):
            continue
        if path.lower() in excluded_paths:
            continue
        text = sanitize_public_text(strip_html_text(inner))
        if not role_pattern.search(text):
            continue
        lines = [
            sanitize_public_text(part)
            for part in re.split(r"[\r\n]+", re.sub(r"(?is)<br\s*/?>", "\n", inner))
        ]
        lines = [part for part in lines if part]
        title = sanitize_public_text(lines[0] if lines else text.split("  ")[0])
        city = ""
        country = ""
        if len(lines) >= 3 and "," in lines[-1]:
            city = sanitize_public_text(lines[-1].split(",", 1)[0])
            country = sanitize_public_text(lines[-1].split(",", 1)[1]) or "Japan"
        elif len(lines) >= 3:
            city = sanitize_public_text(lines[-1])
        source_job_id = path.rstrip("/").split("/")[-1] or f"{source_id}-{len(rows) + 1}"
        rows.append(
            {
                "sourceJobId": f"static:{source_id}:{source_job_id}",
                "title": title,
                "company": company,
                "city": city,
                "country": country or "Japan",
                "workType": "",
                "contractType": "",
                "jobLink": absolute,
                "sector": "Game",
                "postedAt": "",
            }
        )
        seen.add(absolute)
    return rows
