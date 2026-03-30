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
    identity = (ctx.source_identity or "").strip().lower()
    return identity == "www.riotgames.com"


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

    jobs = _parse_listing_rows(
        html=html,
        page_url=page_url,
        company=clean_text(
            source_row.get("company") or source_row.get("studio") or source_row.get("name")
        )
        or "Riot Games",
        source_id=clean_text(source_row.get("id")) or "riot",
        source_name=clean_text(source_row.get("name")) or "Riot Games",
    )
    if not jobs:
        source_row["_staticPluginMeta"] = _heuristics.build_static_plugin_meta(
            _heuristics.CLASSIFICATION_PARSER_STALE,
            browser_fallback_recommended=False,
            extractor_hint="riot_listing_present_but_plugin_empty",
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


def _parse_listing_rows(
    *, html: str, page_url: str, company: str, source_id: str, source_name: str
) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen: set[str] = set()

    for anchor in iter_anchor_fragments(html or ""):
        href = clean_text(anchor.get("href"))
        if "/en/j/" not in href:
            continue
        if not href:
            continue
        absolute = clean_text(urljoin(page_url, href))
        if not absolute or absolute in seen:
            continue
        lines = html_fragment_lines(anchor.get("body", ""))
        title = clean_text(
            extract_first_tag_text(anchor.get("body", ""), ["h1", "h2", "h3", "h4", "h5", "h6"])
        )
        if not title:
            title = clean_text(lines[0] if lines else anchor.get("text"))
        if not title:
            continue
        seen.add(absolute)
        location = ""
        craft = ""
        for line in lines[1:]:
            lowered = line.lower()
            if not craft and any(
                token in lowered
                for token in ("art", "engineering", "design", "publishing", "security", "data")
            ):
                craft = line
            if not location and any(
                token in lowered
                for token in (
                    "remote",
                    "los angeles",
                    "dublin",
                    "seoul",
                    "shanghai",
                    "singapore",
                    "berlin",
                )
            ):
                location = line
        jobs.append(
            {
                "sourceJobId": f"static:{source_id}:{hashlib.sha1(absolute.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": location,
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": absolute,
                "sector": craft or "Game",
                "postedAt": "",
                "adapter": "static",
                "studio": company,
                "source": source_name,
                "summary": " | ".join(lines[1:5]),
            }
        )
    return jobs
