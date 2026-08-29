from __future__ import annotations

import re
from html import unescape
from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticContext,
    SimpleStaticPlugin,
    simple_static_run,
    static_identity_handler,
    static_job_row,
)
from src.jobs.adapters.provider_parsers import normalize_location_details
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

_SPEC = SimpleStaticPlugin(
    source_id="outerdawn",
    default_company="Outerdawn",
    parser_stale_hint="outerdawn_listing_present_but_plugin_empty",
)

can_handle = static_identity_handler("www.outerdawn.com", "outerdawn.com")

# Webflow job rows: each role is a <div class="careerrow w-dyn-item"> block with an
# <h3 class="contentbox__heading"> title, a <div class="contentbox__subheading">
# location, and an <a href="/careers/<slug>" class="buttongeneric">"Read & Apply"</a> link.
_BLOCK_SEP = re.compile(r'(?is)(?=class="careerrow[^"]*")')
_TITLE_RE = re.compile(r'(?is)<h[23][^>]*class="[^"]*contentbox__heading[^"]*"[^>]*>(.*?)</h[23]>')
_LOCATION_RE = re.compile(
    r'(?is)class="[^"]*contentbox__subheading[^"]*"[^>]*>(.*?)</(?:div|span|p)>'
)
_LINK_RE = re.compile(r'(?is)href="([^"]*/careers/[^"]+)"')


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen: set[str] = set()
    blocks = _BLOCK_SEP.split(ctx.html or "")
    for block in blocks[1:]:
        if "careerrow" not in block[:60]:
            continue
        title_match = _TITLE_RE.search(block)
        link_match = _LINK_RE.search(block)
        if not title_match or not link_match:
            continue
        title = clean_text(strip_html_text(title_match.group(1)))
        link = clean_text(urljoin(ctx.page_url, unescape(link_match.group(1))))
        if not title or not link or link in seen:
            continue
        seen.add(link)
        location_text = ""
        location_match = _LOCATION_RE.search(block)
        if location_match:
            location_text = clean_text(strip_html_text(location_match.group(1)))
        location_details = normalize_location_details(location_text)
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
