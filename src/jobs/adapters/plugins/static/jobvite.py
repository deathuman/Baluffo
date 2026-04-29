from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import html_fragment_lines, iter_anchor_fragments
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
    source_id="amber-jobvite",
    default_company="Amber",
    parser_stale_hint="jobvite_listing_present_but_plugin_empty",
)


def can_handle(ctx: AdapterPluginContext) -> bool:
    return "amberstudiocareers" in ((ctx.source_identity or "").strip().lower())


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen: set[str] = set()
    for anchor in iter_anchor_fragments(ctx.html or ""):
        href = clean_text(anchor.get("href"))
        if "/amberstudiocareers/job/" not in href:
            continue
        if not href:
            continue
        link = clean_text(urljoin(ctx.page_url, href))
        if not link or link in seen:
            continue
        lines = html_fragment_lines(anchor.get("body", ""))
        title = lines[0] if lines else ""
        location = lines[1] if len(lines) > 1 else ""
        if not title:
            continue
        seen.add(link)
        location_details = normalize_location_details(location)
        jobs.append(
            static_job_row(
                ctx,
                link=link,
                title=title,
                city=clean_text(location_details.get("city")),
                country=clean_text(location_details.get("country")) or "Unknown",
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
