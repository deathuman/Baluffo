from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import (
    extract_first_tag_text,
    html_fragment_lines,
    iter_anchor_fragments,
)
from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text


def can_handle(ctx: AdapterPluginContext) -> bool:
    return (ctx.source_identity or "").strip().lower() == "globalstep.com"


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
    if not page_url:
        return []
    company = (
        clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name"))
        or "GlobalStep"
    )
    source_id = clean_text(source_row.get("id")) or "globalstep"
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
    for anchor in iter_anchor_fragments(html or ""):
        href = clean_text(anchor.get("href"))
        if "/jobs/" not in href:
            continue
        title = clean_text(extract_first_tag_text(anchor.get("body", ""), ["h2", "h3"]))
        if not href or not title:
            continue
        link = clean_text(urljoin(page_url, href))
        if not link or link in seen:
            continue
        seen.add(link)
        lines = html_fragment_lines(anchor.get("body", ""))
        meta = [line for line in lines if line != title and line.lower() != "more details"]
        location = meta[0] if meta else ""
        contract_type = meta[1] if len(meta) > 1 else ""
        jobs.append(
            {
                "sourceJobId": f"static:{source_id}:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": location,
                "country": "Unknown",
                "workType": "",
                "contractType": contract_type,
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
                "adapter": "static",
                "studio": company,
                "source": clean_text(source_row.get("name")) or company,
                "summary": " | ".join(meta[:3]),
            }
        )

    if not jobs:
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_PARSER_STALE,
            browser_fallback_recommended=False,
            extractor_hint="globalstep_listing_present_but_plugin_empty",
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
