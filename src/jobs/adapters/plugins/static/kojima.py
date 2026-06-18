from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import strip_html_text
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
from src.jobs.adapters.static_runtime_support import is_static_fetch_fallback_exception
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, normalize_url, sanitize_public_text


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("www.kojimaproductions.jp", "kojimaproductions.jp")


def _fetch_kojima_html(
    *,
    fetch_text: Callable[[str, int], str],
    page_url: str,
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_row: dict[str, Any],
    maybe_fetch_kojima_job_listing_html: Callable[..., str] | None,
) -> str:
    html = fetch_static_plugin_html(
        fetch_text=fetch_text,
        page_url=page_url,
        timeout_s=timeout_s,
        source_row=source_row,
    )
    if not html:
        return ""
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
                return dynamic
    except OSError:
        pass
    except RuntimeError as exc:
        if not is_static_fetch_fallback_exception(exc):
            raise
    return html


def _parse_kojima_rows(
    *,
    html: str,
    page_url: str,
    company: str,
    source_id: str,
    timeout_s: int,
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]],
    try_playwright: Callable[[str, int], tuple[str, str]] | None,
) -> list[dict[str, Any]]:
    rows = parse_jobpostings_from_html(
        html,
        base_url=page_url,
        fallback_company=company,
        fallback_source_id_prefix=f"static:{source_id}",
    )
    if rows:
        return rows
    rows = _parse_kojima_listing_rows(
        html=html,
        base_url=page_url,
        company=company,
        source_id=source_id,
    )
    if rows or not callable(try_playwright):
        return rows
    browser_html, _ = try_playwright(page_url, max(3, min(timeout_s, 30)))
    if not browser_html:
        return rows
    return _parse_kojima_listing_rows(
        html=browser_html,
        base_url=page_url,
        company=company,
        source_id=source_id,
    )


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
    page_url = first_static_page(pages)
    if not page_url:
        return []
    company, source_id, source_name = static_plugin_context_values(
        source_row=source_row,
        default_company="Kojima Productions",
        default_source_id="kojima",
        default_source_name="kojima",
    )
    html = _fetch_kojima_html(
        fetch_text=fetch_text,
        page_url=page_url,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_row=source_row,
        maybe_fetch_kojima_job_listing_html=maybe_fetch_kojima_job_listing_html,
    )
    if not html or static_plugin_blocked_by_js_shell(
        html=html,
        page_url=page_url,
        source_row=source_row,
    ):
        return []

    rows = _parse_kojima_rows(
        html=html,
        page_url=page_url,
        company=company,
        source_id=source_id,
        timeout_s=timeout_s,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        try_playwright=try_playwright,
    )
    cleaned = stamp_static_plugin_rows(rows=rows, company=company, source_name=source_name)
    if not cleaned:
        record_static_plugin_empty_parse(html=html, page_url=page_url, source_row=source_row)
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
        if len(lines) >= 3:
            city, country, _ = parse_generic_location_fields(lines[-1])
            if not city and country == "Unknown":
                country = "Japan"
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
