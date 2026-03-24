from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import extract_tag_texts, iter_anchor_fragments
from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text


def can_handle(ctx: AdapterPluginContext) -> bool:
    return (ctx.source_identity or "").strip().lower() == "careers.embark-studios.com"


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
        or "Embark Studios"
    )
    source_id = clean_text(source_row.get("id")) or "embark"
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
    jobs: list[RawJob] = []
    seen: set[str] = set()
    for anchor in iter_anchor_fragments(html or ""):
        href = clean_text(anchor.get("href"))
        if "/jobs/" not in href:
            continue
        pieces = [
            clean_text(item)
            for item in extract_tag_texts(anchor.get("body", ""), ["div", "span"])
            if clean_text(item)
        ]
        title = pieces[0] if pieces else clean_text(anchor.get("text"))
        if not href or not title:
            continue
        link = clean_text(urljoin(page_url, href))
        if not link or link in seen:
            continue
        seen.add(link)
        location = pieces[1] if len(pieces) > 1 else ""
        jobs.append(
            {
                "sourceJobId": f"static:{source_id}:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": location,
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
                "adapter": "static",
                "studio": company,
                "source": clean_text(source_row.get("name")) or company,
                "summary": " | ".join(pieces[1:3]),
            }
        )
    if not jobs:
        source_row["_staticPluginMeta"] = {
            "classification": _heuristics.CLASSIFICATION_PARSER_STALE,
            "browserFallbackRecommended": False,
            "extractorHint": "embark_listing_present_but_plugin_empty",
            "detailFetchRequired": False,
            "detailTraversalMode": "listing_only",
        }
        return []
    source_row["_staticPluginMeta"] = {
        "detailFetchRequired": False,
        "detailTraversalMode": "listing_only",
    }
    return jobs
