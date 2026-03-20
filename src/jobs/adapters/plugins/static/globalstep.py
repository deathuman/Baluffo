from __future__ import annotations

import hashlib
from typing import Any, Callable, Dict, List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

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
    pages: List[str],
    source_row: Dict[str, Any],
    **kwargs: Any,
) -> List[RawJob]:
    _ = (retries, backoff_s, kwargs)
    if not pages:
        return []
    page_url = clean_text(pages[0])
    if not page_url:
        return []
    company = clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name")) or "GlobalStep"
    source_id = clean_text(source_row.get("id")) or "globalstep"
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

    soup = BeautifulSoup(html or "", "html.parser")
    jobs: List[RawJob] = []
    seen: set[str] = set()
    for anchor in soup.select('a[href*="/jobs/"]'):
        href = clean_text(anchor.get("href"))
        title_node = anchor.find(["h2", "h3"])
        title = clean_text(title_node.get_text(" ", strip=True) if title_node else "")
        if not href or not title:
            continue
        link = clean_text(urljoin(page_url, href))
        if not link or link in seen:
            continue
        seen.add(link)
        lines = [clean_text(line) for line in anchor.get_text("\n", strip=True).splitlines() if clean_text(line)]
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
        source_row["_staticPluginMeta"] = {
            "classification": _heuristics.CLASSIFICATION_PARSER_STALE,
            "browserFallbackRecommended": False,
            "extractorHint": "globalstep_listing_present_but_plugin_empty",
            "detailFetchRequired": False,
            "detailTraversalMode": "listing_only",
        }
        return []
    source_row["_staticPluginMeta"] = {"detailFetchRequired": False, "detailTraversalMode": "listing_only"}
    return jobs
