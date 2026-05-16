from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.static_runtime_support import classify_static_fetch_exception
from src.jobs.common.no_openings import contains_no_openings_marker
from src.jobs.text_utils import clean_text, normalize_url

# Canonical classification values for static/scrapy_static diagnostics and browser queue.
# Use these when setting classification in plugins and adapter code so reporting stays consistent.
CLASSIFICATION_OK_WITH_JOBS = "ok_with_jobs"
CLASSIFICATION_EMPTY_CONFIRMED = "empty_confirmed"
CLASSIFICATION_NEEDS_REVIEW = "needs_review"
CLASSIFICATION_FETCH_OK_EXTRACT_ZERO = CLASSIFICATION_NEEDS_REVIEW
CLASSIFICATION_JS_REQUIRED = "js_required"
CLASSIFICATION_SITE_CHANGED = "site_changed"
CLASSIFICATION_BLOCKED_OR_CHALLENGE = "blocked_or_challenge"
CLASSIFICATION_PARSER_STALE = "parser_stale"
CLASSIFICATION_DEAD_LISTING_PAGE = "dead_listing_page"


def normalize_html(html: str) -> str:
    return str(html or "")


def visible_text_len(html: str) -> int:
    text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", normalize_html(html)))
    return len(clean_text(text))


def detect_js_shell(html: str) -> bool:
    """Best-effort detection for JS-rendered app shells.

    This is intentionally conservative: it aims to detect pages that almost
    certainly require a browser/JS to render listings.
    """
    s = normalize_html(html)
    lower = s.lower()
    if visible_text_len(s) < 180:
        # Very little visible text; if also looks like an SPA shell, flag it.
        if any(
            tok in lower
            for tok in (
                '<div id="root"',
                '<div id="app"',
                "data-reactroot",
                "ng-version",
                "__next_data__",
            )
        ):
            return True
        if any(tok in lower for tok in ("window.__", "webpackjsonp", "react", "next.js")):
            return True
    return False


def detect_no_openings(html: str) -> bool:
    """Detect explicit 'no openings' markers to allow a proven-empty result."""
    return contains_no_openings_marker(normalize_html(html))


def detect_outbound_ats_links(html: str, *, base_url: str) -> list[str]:
    """Find outbound links to known ATS/job platforms."""
    s = normalize_html(html)
    links: list[str] = []
    for m in re.finditer(r'(?is)<a[^>]+href=["\']([^"\']+)["\']', s):
        href = clean_text(m.group(1))
        if not href:
            continue
        absolute = normalize_url(urljoin(base_url, href)) or ""
        if not absolute:
            continue
        lower = absolute.lower()
        if any(
            host in lower
            for host in (
                "greenhouse.io",
                "lever.co",
                "bamboohr.com",
                "myworkdayjobs.com",
                "workday.com",
                "smartrecruiters.com",
                "ashbyhq.com",
                "teamtailor.com",
                "personio.de",
                "jobvite.com",
            )
        ):
            links.append(absolute)
    # Dedup but keep stable order.
    out: list[str] = []
    seen = set()
    for url in links:
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def classify_fetch_exception(exc: Exception) -> tuple[str, bool]:
    return classify_static_fetch_exception(exc)


def build_static_plugin_meta(
    classification: str,
    *,
    browser_fallback_recommended: bool | None = None,
    extractor_hint: str | None = None,
    ats_links: list[str] | None = None,
    empty_confirmed: bool | None = None,
    detail_fetch_required: bool | None = None,
    detail_traversal_mode: str | None = None,
    error: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Build a consistent _staticPluginMeta payload for static plugins."""
    meta: dict[str, Any] = {"classification": classification}
    if browser_fallback_recommended is not None:
        meta["browserFallbackRecommended"] = browser_fallback_recommended
    if extractor_hint:
        meta["extractorHint"] = extractor_hint
    if ats_links is not None:
        meta["atsLinks"] = list(ats_links[:5])
    if empty_confirmed is not None:
        meta["emptyConfirmed"] = empty_confirmed
    if detail_fetch_required is not None:
        meta["detailFetchRequired"] = detail_fetch_required
    if detail_traversal_mode:
        meta["detailTraversalMode"] = detail_traversal_mode
    if error:
        meta["error"] = error
    for key, value in extra.items():
        if value is not None:
            meta[key] = value
    return meta
