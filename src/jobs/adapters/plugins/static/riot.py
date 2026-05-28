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
from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticContext,
    SimpleStaticPlugin,
    simple_static_run,
    static_job_row,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.provider_parsers import normalize_location_details
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity == "www.riotgames.com"


def _parse_html(ctx: SimpleStaticContext) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    seen: set[str] = set()

    for anchor in iter_anchor_fragments(ctx.html or ""):
        href = clean_text(anchor.get("href"))
        if "/en/j/" not in href:
            continue
        if not href:
            continue
        absolute = clean_text(urljoin(ctx.page_url, href))
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
        location_details = normalize_location_details(location)
        row = static_job_row(
            ctx,
            link=absolute,
            title=title,
            company=ctx.company,
            city=clean_text(location_details.get("city")),
            country=clean_text(location_details.get("country")) or "Unknown",
            workType="",
            contractType="",
            postedAt="",
        )
        row["sourceJobId"] = (
            f"static:{ctx.source_id}:{hashlib.sha1(absolute.encode('utf-8')).hexdigest()[:10]}"
        )
        row["sector"] = craft or "Game"
        row["summary"] = " | ".join(lines[1:5])
        row["locations"] = location_details.get("locations") or []
        row["locationSummary"] = clean_text(location_details.get("locationSummary"))
        jobs.append(row)
    return jobs


_riot_run = simple_static_run(
    spec=SimpleStaticPlugin(
        source_id="riot",
        default_company="Riot Games",
        parser_stale_hint="riot_listing_present_but_plugin_empty",
        empty_detail_fetch_required=False,
        empty_detail_traversal_mode="listing_only",
    ),
    parse_html=_parse_html,
)


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
    return _riot_run(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        pages=pages,
        source_row=source_row,
        **kwargs,
    )
