from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("www.naconstudiomilan.com", "naconstudiomilan.com")


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
    company = clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name")) or "Nacon Studio Milan"
    source_id = clean_text(source_row.get("id")) or "naconstudiomilan"
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
    seen = set()
    for match in re.finditer(
        r'(?is)<h4[^>]*>\s*(.*?)\s*</h4>.*?<a[^>]+href=["\']([^"\']*/careers/[^"\']+/)["\'][^>]*>\s*Learn more\s*</a>',
        html,
    ):
        title = clean_text(re.sub(r"(?is)<[^>]+>", " ", match.group(1) or ""))
        link = clean_text(urljoin(page_url, match.group(2) or ""))
        if not title or not link or link in seen:
            continue
        seen.add(link)
        jobs.append(
            {
                "sourceJobId": f"static:{source_id}:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
                "adapter": "static",
                "studio": company,
                "source": clean_text(source_row.get("name")) or company,
            }
        )

    if not jobs:
        source_row["_staticPluginMeta"] = {
            "classification": _heuristics.CLASSIFICATION_PARSER_STALE,
            "browserFallbackRecommended": False,
            "extractorHint": "listing_cards_present_but_plugin_empty",
        }
    return jobs
