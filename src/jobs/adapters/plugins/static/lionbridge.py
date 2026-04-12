from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import (
    html_fragment_lines,
    iter_anchor_fragments,
    iter_block_fragments,
)
from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.provider_parsers import normalize_location_details
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text


def can_handle(ctx: AdapterPluginContext) -> bool:
    return (ctx.source_identity or "").strip().lower() == "careers.lionbridge.com"


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    **kwargs: Any,
) -> list[RawJob]:
    _ = (retries, backoff_s, kwargs)
    if not pages:
        return []
    page_url = clean_text(pages[0])
    company = (
        clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name"))
        or "Lionbridge Games"
    )
    source_id = clean_text(source_row.get("id")) or "lionbridge"
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
    jobs: list[RawJob] = []
    seen: set[str] = set()
    for row_html in iter_block_fragments(html or "", "tr"):
        anchor = next(
            (
                item
                for item in iter_anchor_fragments(row_html)
                if "/jobs/" in clean_text(item.get("href"))
            ),
            None,
        )
        if not anchor:
            continue
        href = clean_text(anchor.get("href"))
        title = clean_text(anchor.get("text"))
        if not href or not title or title.lower() in {"all jobs", "skip to jobs search results"}:
            continue
        link = clean_text(urljoin(page_url, href))
        if not link or link in seen:
            continue
        seen.add(link)
        meta = [
            clean_text(line)
            for line in html_fragment_lines(row_html)
            if clean_text(line) and clean_text(line) != title
        ]
        location = ""
        work_type = ""
        for line in meta:
            if not location and any(
                token in line.lower()
                for token in (
                    "united",
                    "mexico",
                    "japan",
                    "berlin",
                    "yokohama",
                    "warsaw",
                    "remote",
                    "spain",
                    "india",
                )
            ):
                location = line
            elif not work_type and line.lower() in {"remote", "hybrid", "onsite"}:
                work_type = line
        location_details = normalize_location_details(location)
        jobs.append(
            {
                "sourceJobId": f"static:{source_id}:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": clean_text(location_details.get("city")),
                "country": clean_text(location_details.get("country")) or "Unknown",
                "workType": work_type,
                "contractType": "",
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
                "adapter": "static",
                "studio": company,
                "source": clean_text(source_row.get("name")) or company,
                "summary": " | ".join(meta[:3]),
                "locations": location_details.get("locations") or [],
                "locationSummary": clean_text(location_details.get("locationSummary")),
            }
        )
    if not jobs:
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_PARSER_STALE,
            browser_fallback_recommended=False,
            extractor_hint="lionbridge_listing_present_but_plugin_empty",
            detail_fetch_required=False,
            detail_traversal_mode="listing_only",
        )
        return []
    source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
        _heuristics.CLASSIFICATION_OK_WITH_JOBS,
        detail_fetch_required=False,
        detail_traversal_mode="listing_only",
    )
    return jobs
