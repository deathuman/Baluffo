from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin, urlparse

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
    identity = (ctx.source_identity or "").strip().lower()
    return identity == "hrmos.co"


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

    company = clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name")) or _company_name_from_url(page_url) or "HRMOS"
    source_id = clean_text(source_row.get("id")) or f"hrmos:{_company_name_from_url(page_url) or 'listing'}"
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

    jobs = _parse_listing_rows(html=html, page_url=page_url, company=company, source_id=source_id, source_name=clean_text(source_row.get("name")) or company)
    if not jobs:
        source_row["_staticPluginMeta"] = {
            "classification": _heuristics.CLASSIFICATION_PARSER_STALE,
            "browserFallbackRecommended": False,
            "extractorHint": "hrmos_listing_present_but_plugin_empty",
            "detailFetchRequired": False,
            "detailTraversalMode": "listing_only",
        }
        return []

    source_row["_staticPluginMeta"] = {
        "detailFetchRequired": False,
        "detailTraversalMode": "listing_only",
    }
    return jobs


def _parse_listing_rows(*, html: str, page_url: str, company: str, source_id: str, source_name: str) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen: set[str] = set()

    for anchor in iter_anchor_fragments(html or ""):
        href = clean_text(anchor.get("href"))
        if "/pages/" not in href or "/jobs/" not in href:
            continue
        if not href:
            continue
        absolute = clean_text(urljoin(page_url, href))
        if not absolute or absolute in seen:
            continue
        segments = html_fragment_lines(anchor.get("body", ""))
        if not segments:
            continue
        title = clean_text(extract_first_tag_text(anchor.get("body", ""), ["h1", "h2", "h3", "h4"]))
        if not title:
            title = segments[0]
        if not title:
            continue
        seen.add(absolute)
        meta = [segment for segment in segments if segment and segment != title]
        location = ""
        contract_type = ""
        for line in meta:
            lowered = line.lower()
            if (
                not location
                and len(line) <= 80
                and any(token in lowered for token in ("remote", "tokyo", "japan", "osaka", "fukuoka", "kyoto", "sapporo", "nagoya"))
            ):
                location = line
            if not contract_type and any(token in lowered for token in ("full", "contract", "intern", "temporary", "part-time")):
                contract_type = line
        jobs.append(
            {
                "sourceJobId": f"static:{source_id}:{hashlib.sha1(absolute.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": location,
                "country": "Unknown",
                "workType": "",
                "contractType": contract_type,
                "jobLink": absolute,
                "sector": "Game",
                "postedAt": "",
                "adapter": "static",
                "studio": company,
                "source": source_name,
                "summary": " | ".join(meta[:4]),
            }
        )
    return jobs


def _company_name_from_url(page_url: str) -> str:
    path = urlparse(page_url).path.strip("/").split("/")
    if len(path) >= 2 and path[0] == "pages":
        slug = path[1].replace("-", " ").strip()
        return " ".join(part.capitalize() for part in slug.split())
    return ""
