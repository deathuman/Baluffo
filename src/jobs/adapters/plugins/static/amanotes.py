from __future__ import annotations

import json
import re
from collections.abc import Callable
from html import unescape
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.parsers.location import normalize_location_details
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, norm_text

_NEXT_DATA_RE = re.compile(r'(?is)<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>')
_HOSTS = {"careers.amanotes.com", "www.careers.amanotes.com"}


def can_handle(ctx: AdapterPluginContext) -> bool:
    return (ctx.source_identity or "").strip().lower() in _HOSTS


def _next_data_positions(html: str) -> list[Any]:
    match = _NEXT_DATA_RE.search(html)
    if not match:
        return []
    try:
        payload = json.loads(unescape(match.group(1).strip()))
    except json.JSONDecodeError:
        return []
    positions = ((payload.get("props") or {}).get("pageProps") or {}).get("positions") or []
    return positions if isinstance(positions, list) else []


def _amanotes_location(raw_location: str) -> tuple[dict[str, Any], str]:
    normalized_location = re.sub(r"[\s_-]+", " ", norm_text(raw_location)).strip()
    location_details = normalize_location_details(raw_location)
    if norm_text(raw_location) in {"hcm", "hcmc", "ho chi minh city"}:
        location_details = {
            "city": "HCMC",
            "country": "Vietnam",
            "locations": [{"city": "HCMC", "country": "Vietnam"}],
            "locationSummary": "HCMC, Vietnam",
        }
    work_type = (
        raw_location
        if "remote" in normalized_location
        or normalized_location in {"hybrid", "onsite", "on site", "worldwide"}
        else ""
    )
    if location_details["city"] == "Remote" and location_details["country"] == "Remote":
        location_details = normalize_location_details("")
    return location_details, work_type


def _amanotes_row(
    *,
    position: dict[str, Any],
    page_url: str,
    company: str,
    source_name: str,
    source_id: str,
) -> RawJob | None:
    title = clean_text(position.get("title"))
    location_details, work_type = _amanotes_location(clean_text(position.get("location")))
    slug = clean_text((position.get("slug") or {}).get("current"))
    lever_id = clean_text(position.get("leverId") or position.get("_id"))
    if not title or not slug or not lever_id:
        return None
    return {
        "title": title,
        "company": company,
        "city": location_details["city"],
        "country": "" if location_details["country"] == "Unknown" else location_details["country"],
        "locations": location_details["locations"],
        "locationSummary": location_details["locationSummary"],
        "workType": work_type,
        "contractType": clean_text(position.get("type")),
        "jobLink": urljoin(page_url, f"/jobs/{slug}/{lever_id}"),
        "sector": clean_text(position.get("team")) or "Game",
        "sourceJobId": f"{source_id}:{lever_id}",
        "postedAt": "",
        "adapter": "static",
        "studio": company,
        "source": source_name,
    }


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
    html = fetch_text(page_url, timeout_s)
    positions = _next_data_positions(html)
    if not positions:
        return []

    company = (
        clean_text(source_row.get("company") or source_row.get("studio") or source_row.get("name"))
        or "Amanotes"
    )
    source_name = clean_text(source_row.get("name")) or company
    source_id = clean_text(source_row.get("id")) or "amanotes"
    rows: list[RawJob] = []
    for position in positions:
        if not isinstance(position, dict):
            continue
        row = _amanotes_row(
            position=position,
            page_url=page_url,
            company=company,
            source_name=source_name,
            source_id=source_id,
        )
        if row:
            rows.append(row)
    return rows
