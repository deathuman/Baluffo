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
    return "amberstudiocareers" in ((ctx.source_identity or "").strip().lower())


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
    company = clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name")) or "Amber"
    source_id = clean_text(source_row.get("id")) or "amber-jobvite"
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
    for anchor in soup.select('a[href*="/amberstudiocareers/job/"]'):
        href = clean_text(anchor.get("href"))
        if not href:
            continue
        link = clean_text(urljoin(page_url, href))
        if not link or link in seen:
            continue
        lines = [clean_text(line) for line in anchor.get_text("\n", strip=True).splitlines() if clean_text(line)]
        title = lines[0] if lines else ""
        location = lines[1] if len(lines) > 1 else ""
        if not title:
            continue
        seen.add(link)
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
            }
        )
    if not jobs:
        source_row["_staticPluginMeta"] = {
            "classification": _heuristics.CLASSIFICATION_PARSER_STALE,
            "browserFallbackRecommended": False,
            "extractorHint": "jobvite_listing_present_but_plugin_empty",
            "detailFetchRequired": False,
            "detailTraversalMode": "listing_only",
        }
        return []
    source_row["_staticPluginMeta"] = {"detailFetchRequired": False, "detailTraversalMode": "listing_only"}
    return jobs
