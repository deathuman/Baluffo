"""Static plugin for littlechicken.nl careers."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, normalize_url


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("www.littlechicken.nl", "littlechicken.nl")


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
    _ = kwargs
    if not pages or not callable(parse_jobpostings_from_html):
        return []
    company = (
        clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name"))
        or "Little Chicken"
    )
    source_id = (source_row.get("id") or "").strip() or "littlechicken"
    source_name = clean_text(source_row.get("name")) or "littlechicken"

    fetched_pages: dict[str, str] = {}
    listing_urls: list[str] = []
    detail_urls: list[str] = []
    for raw_url in pages:
        page_url = clean_text(raw_url)
        if not page_url or page_url in fetched_pages:
            continue
        try:
            fetched_pages[page_url] = fetch_text(page_url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            classification, recommend = _heuristics.classify_fetch_exception(exc)
            source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
                classification,
                browser_fallback_recommended=bool(recommend),
                extractor_hint="fetch_failed",
                error=str(exc),
            )
            continue
        if "/job/" in (urlparse(page_url).path or "").lower():
            detail_urls.append(page_url)
        else:
            listing_urls.append(page_url)

    rows: list[RawJob] = []
    seen_links = set()

    for listing_url in listing_urls:
        html = fetched_pages.get(listing_url) or ""
        parsed = parse_jobpostings_from_html(
            html,
            base_url=listing_url,
            fallback_company=company,
            fallback_source_id_prefix=f"static:{source_id}",
        )
        for row in parsed:
            link = normalize_url(row.get("jobLink"))
            if not link or link in seen_links:
                continue
            seen_links.add(link)
            rows.append(dict(row))
            if link not in detail_urls:
                detail_urls.append(link)

        for href, inner in re.findall(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html):
            if "/job/" not in clean_text(href).lower():
                continue
            link = normalize_url(urljoin(listing_url, clean_text(href)))
            title = strip_html_text(inner)
            if not link or not title or link in seen_links:
                continue
            seen_links.add(link)
            rows.append(
                {
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
            )
            if link not in detail_urls:
                detail_urls.append(link)

    for detail_url in detail_urls:
        cached_html = fetched_pages.get(detail_url)
        if cached_html is None:
            try:
                detail_html = fetch_text(detail_url, timeout_s)
            except Exception:
                continue
        else:
            detail_html = cached_html
        parsed = parse_jobpostings_from_html(
            detail_html,
            base_url=detail_url,
            fallback_company=company,
            fallback_source_id_prefix=f"static:{source_id}",
        )
        for row in parsed:
            link = normalize_url(row.get("jobLink")) or normalize_url(detail_url)
            if not link:
                continue
            existing = next(
                (item for item in rows if normalize_url(item.get("jobLink")) == link), None
            )
            if existing is None:
                existing = {
                    "sourceJobId": clean_text(row.get("sourceJobId"))
                    or f"static:{source_id}:{link}",
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
                rows.append(existing)
                seen_links.add(link)
                continue
            if clean_text(existing.get("title")).lower() in {"read more", "job"} and clean_text(
                row.get("title")
            ):
                existing["title"] = row.get("title")
            for field in (
                "sourceJobId",
                "title",
                "company",
                "city",
                "country",
                "workType",
                "contractType",
                "postedAt",
            ):
                if not clean_text(existing.get(field)) and clean_text(row.get(field)):
                    existing[field] = row.get(field)

    cleaned = [
        r
        for r in rows
        if isinstance(r, dict) and clean_text(r.get("title")) and normalize_url(r.get("jobLink"))
    ]
    for row in cleaned:
        row["adapter"] = "static"
        row["studio"] = company
        row["source"] = source_name
        row["sector"] = clean_text(row.get("sector")) or "Game"
        row["company"] = clean_text(row.get("company")) or company
        row["jobLink"] = normalize_url(row.get("jobLink")) or ""

    if not cleaned:
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_FETCH_OK_EXTRACT_ZERO,
            browser_fallback_recommended=False,
            extractor_hint="listing_cards_empty",
        )
    return cleaned
