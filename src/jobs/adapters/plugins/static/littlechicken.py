"""Static plugin for littlechicken.nl careers."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.static._runner import (
    fetch_static_plugin_html,
    static_plugin_context_values,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, normalize_url

_JOB_LINK_RE = re.compile(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>')
_DETAIL_FIELDS = (
    "sourceJobId",
    "title",
    "company",
    "city",
    "country",
    "workType",
    "contractType",
    "postedAt",
)


def can_handle(ctx: AdapterPluginContext) -> bool:
    return (ctx.source_identity or "").strip().lower() in {
        "www.littlechicken.nl",
        "littlechicken.nl",
    }


def _fetch_pages(pages, fetch_text, timeout_s, source_row):
    fetched: dict[str, str] = {}
    listings: list[str] = []
    details: list[str] = []
    for raw_url in pages:
        url = clean_text(raw_url)
        if not url or url in fetched:
            continue
        html = fetch_static_plugin_html(
            fetch_text=fetch_text, page_url=url, timeout_s=timeout_s, source_row=source_row
        )
        if not html:
            continue
        fetched[url] = html
        (details if "/job/" in (urlparse(url).path or "").lower() else listings).append(url)
    return fetched, listings, details


def _add_row(rows: list[RawJob], seen: set[str], row: dict[str, Any], link: str) -> bool:
    link = normalize_url(link)
    if not link or link in seen:
        return False
    seen.add(link)
    rows.append(dict(row))
    return True


def _add_listing_rows(rows, seen, details, html, listing_url, company, source_id, parser) -> None:
    for row in parser(
        html,
        base_url=listing_url,
        fallback_company=company,
        fallback_source_id_prefix=f"static:{source_id}",
    ):
        link = normalize_url(row.get("jobLink"))
        if _add_row(rows, seen, row, link) and link not in details:
            details.append(link)
    for href, inner in _JOB_LINK_RE.findall(html):
        if "/job/" not in clean_text(href).lower():
            continue
        link = normalize_url(urljoin(listing_url, clean_text(href)))
        title = strip_html_text(inner)
        row = {
            "sourceJobId": f"static:{source_id}:{link}",
            "title": title,
            "company": company,
            "city": "Amsterdam",
            "country": "NL",
            "workType": "",
            "contractType": "",
            "jobLink": link,
            "sector": "Game",
            "postedAt": "",
        }
        if link and title and _add_row(rows, seen, row, link) and link not in details:
            details.append(link)


def _new_detail_row(row, link, company, source_id):
    return {
        "sourceJobId": clean_text(row.get("sourceJobId")) or f"static:{source_id}:{link}",
        "title": clean_text(row.get("title")),
        "company": clean_text(row.get("company")) or company,
        "city": clean_text(row.get("city")),
        "country": clean_text(row.get("country")) or "Unknown",
        "workType": clean_text(row.get("workType")),
        "contractType": clean_text(row.get("contractType")),
        "jobLink": link,
        "sector": clean_text(row.get("sector")) or "Game",
        "postedAt": clean_text(row.get("postedAt")),
    }


def _merge_detail_row(rows, seen, row, link, company, source_id) -> None:
    existing = next((item for item in rows if normalize_url(item.get("jobLink")) == link), None)
    if existing is None:
        rows.append(_new_detail_row(row, link, company, source_id))
        seen.add(link)
        return
    if clean_text(existing.get("title")).lower() in {"read more", "job"} and clean_text(
        row.get("title")
    ):
        existing["title"] = row.get("title")
    for field in _DETAIL_FIELDS:
        if not clean_text(existing.get(field)) and clean_text(row.get(field)):
            existing[field] = row.get(field)


def _merge_details(
    rows, seen, details, fetched, fetch_text, timeout_s, company, source_id, parser
) -> None:
    for detail_url in details:
        detail_html = fetched.get(detail_url)
        if detail_html is None:
            try:
                detail_html = fetch_text(detail_url, timeout_s)
            except Exception:
                continue
        for row in parser(
            detail_html,
            base_url=detail_url,
            fallback_company=company,
            fallback_source_id_prefix=f"static:{source_id}",
        ):
            link = normalize_url(row.get("jobLink")) or normalize_url(detail_url)
            if link:
                _merge_detail_row(rows, seen, row, link, company, source_id)


def _clean(rows: list[RawJob], company: str, source_name: str) -> list[RawJob]:
    cleaned = [
        r
        for r in rows
        if isinstance(r, dict) and clean_text(r.get("title")) and normalize_url(r.get("jobLink"))
    ]
    for row in cleaned:
        row.update(
            adapter="static",
            studio=company,
            source=source_name,
            sector=clean_text(row.get("sector")) or "Game",
            company=clean_text(row.get("company")) or company,
            jobLink=normalize_url(row.get("jobLink")) or "",
        )
    return cleaned


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]] | None = None,
    **kwargs: Any,
) -> list[RawJob]:
    _ = (retries, backoff_s, kwargs)
    if not pages or not callable(parse_jobpostings_from_html):
        return []
    company, source_id, source_name = static_plugin_context_values(
        source_row=source_row,
        default_company="Little Chicken",
        default_source_id="littlechicken",
        default_source_name="littlechicken",
    )
    fetched, listings, details = _fetch_pages(pages, fetch_text, timeout_s, source_row)
    rows: list[RawJob] = []
    seen: set[str] = set()
    for listing_url in listings:
        _add_listing_rows(
            rows,
            seen,
            details,
            fetched.get(listing_url) or "",
            listing_url,
            company,
            source_id,
            parse_jobpostings_from_html,
        )
    _merge_details(
        rows,
        seen,
        details,
        fetched,
        fetch_text,
        timeout_s,
        company,
        source_id,
        parse_jobpostings_from_html,
    )
    cleaned = _clean(rows, company, source_name)
    if not cleaned:
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_FETCH_OK_EXTRACT_ZERO,
            browser_fallback_recommended=False,
            extractor_hint="listing_cards_empty",
        )
    return cleaned
