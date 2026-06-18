"""Source-check fetch helpers: HTML fetch with fallback and alternate URLs.

Uses injected deps (fetch_text, playwright, html_extractor, etc.) so admin_bridge
or other callers wire discovery and bridge modules once. Used by check_static_source.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def fetch_html_with_fallback(
    url: str,
    timeout_s: int,
    *,
    fetch_text: Callable[[str, int], str],
    looks_like_challenge: Callable[[str], bool],
    has_extractable_job_data: Callable[[str, str], bool],
    try_playwright: Callable[[str, int], tuple[str, str]],
    is_http_forbidden: Callable[[Exception], bool],
) -> tuple[str, str, bool, bool]:
    """Return (html, error, browser_attempted, browser_used)."""
    try:
        html = fetch_text(url, timeout_s)
    except (OSError, RuntimeError, ValueError) as exc:
        if not is_http_forbidden(exc):
            return "", f"{url}: {exc}", False, False
        browser_html, browser_error = try_playwright(url, timeout_s)
        if browser_html:
            return browser_html, "", True, True
        if browser_error:
            return "", f"{url}: {browser_error}", True, False
        return "", f"{url}: {exc}", True, False

    if not looks_like_challenge(html) or has_extractable_job_data(html, url):
        return html, "", False, False
    browser_html, browser_error = try_playwright(url, timeout_s)
    if browser_html:
        return browser_html, "", True, True
    if browser_error:
        return "", f"{url}: {browser_error}", True, False
    return html, "", True, False


def html_has_extractable_job_data(html: str, page_url: str, *, html_extractor: Any) -> bool:
    """True if html contains job-like links or embedded job signals (uses html_extractor)."""
    if html_extractor.extract_job_like_links(html, page_url):
        return True
    if html_extractor.extract_embedded_job_urls(html, page_url):
        return True
    embedded_links, embedded_signals = html_extractor.extract_embedded_job_filter_signals(
        html, page_url
    )
    return bool(embedded_links or embedded_signals)


def fetch_static_page_with_alternates(
    page_url: str,
    timeout_s: int,
    *,
    fetch_html_with_fallback_fn: Callable[[str, int], tuple[str, str, bool, bool]],
    suggest_alternate_urls: Callable[[str], list],
    discover_redirect_career_candidates: Callable[[str, int], Any],
    is_not_found_error_text: Callable[[str], bool],
) -> tuple[str, str, bool, bool, str]:
    """Return (html, error, browser_attempted, browser_used, alt_url_used)."""
    html, fetch_error, attempted, used = fetch_html_with_fallback_fn(page_url, timeout_s)
    if not fetch_error or not is_not_found_error_text(fetch_error):
        return html, fetch_error, attempted, used, ""

    alt_candidates = list(suggest_alternate_urls(page_url)[:3])
    for redirect_candidate in discover_redirect_career_candidates(page_url, timeout_s):
        if redirect_candidate not in alt_candidates:
            alt_candidates.append(redirect_candidate)
    for alt_url in alt_candidates[:6]:
        alt_html, alt_error, alt_attempted, alt_used = fetch_html_with_fallback_fn(
            alt_url, timeout_s
        )
        attempted = attempted or alt_attempted
        used = used or alt_used
        if alt_error:
            continue
        return alt_html, "", attempted, used, alt_url
    return html, fetch_error, attempted, used, ""
