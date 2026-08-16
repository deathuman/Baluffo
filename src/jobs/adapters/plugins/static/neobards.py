from __future__ import annotations

import re
from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import iter_anchor_fragments
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

_SPEC = SimpleStaticPlugin(
    source_id="neobards",
    default_company="NeoBards",
    parser_stale_hint="neobards_listing_present_but_plugin_empty",
)

_NEOBARDS_NAV_HREF = re.compile(r"/(?:zh-hant|ja)(?:/|$)|/careers?/?$", re.IGNORECASE)


def can_handle(ctx: AdapterPluginContext) -> bool:
    return "neobards" in ((ctx.source_identity or "").strip().lower())


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen: set[str] = set()
    for anchor in iter_anchor_fragments(ctx.html or ""):
        href = clean_text(anchor.get("href"))
        if not href or _NEOBARDS_NAV_HREF.search(href):
            continue
        # Job anchors carry a "Link to:" title attribute; nav/language links do not.
        attrs_text = clean_text(anchor.get("attrs") or "")
        if "link to:" not in attrs_text.lower():
            continue
        link = clean_text(urljoin(ctx.page_url, href))
        if not link or link in seen:
            continue
        lines = [
            clean_text(line) for line in (anchor.get("body") or "").splitlines() if clean_text(line)
        ]
        title = lines[0] if lines else ""
        if not title or len(title) < 4:
            continue
        location = lines[1] if len(lines) > 1 else ""
        location_details = normalize_location_details(location)
        seen.add(link)
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


run = simple_static_run(_SPEC, _parse_html)
