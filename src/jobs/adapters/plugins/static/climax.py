from __future__ import annotations

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
    run_simple_static_plugin,
    static_job_row,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.provider_parsers import normalize_location_details
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

_SPEC = SimpleStaticPlugin(
    source_id="climax",
    default_company="Climax Studios",
    parser_stale_hint="climax_listing_present_but_plugin_empty",
)


def can_handle(ctx: AdapterPluginContext) -> bool:
    return (ctx.source_identity or "").strip().lower() == "www.climaxstudios.com"


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen: set[str] = set()
    for anchor in iter_anchor_fragments(ctx.html or ""):
        href = clean_text(anchor.get("href"))
        if "/join-our-team/jobs/" not in href:
            continue
        title = clean_text(
            extract_first_tag_text(anchor.get("body", ""), ["h2", "h3"]) or anchor.get("text")
        )
        if not href or not title:
            continue
        link = clean_text(urljoin(ctx.page_url, href))
        if not link or link in seen:
            continue
        seen.add(link)
        text_lines = html_fragment_lines(anchor.get("body", ""))
        meta = [line for line in text_lines if line != title]
        location = ""
        contract_type = ""
        for line in meta:
            if not location and "united kingdom" in line.lower():
                location = clean_text(line.replace("Location ", "", 1))
            elif not contract_type and "," in line:
                contract_type = line
        location_details = normalize_location_details(location)
        jobs.append(
            static_job_row(
                ctx,
                link=link,
                title=title,
                city=clean_text(location_details.get("city")),
                country=clean_text(location_details.get("country")) or "Unknown",
                contract_type=contract_type,
                summary=" | ".join(meta[:3]),
                locations=location_details.get("locations") or [],
                locationSummary=clean_text(location_details.get("locationSummary")),
            )
        )
    return jobs


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
    return run_simple_static_plugin(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        pages=pages,
        source_row=source_row,
        spec=_SPEC,
        parse_html=_parse_html,
        **kwargs,
    )
