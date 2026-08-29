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
    source_id="immersity",
    default_company="Leia Inc.",
    parser_stale_hint="immersity_listing_present_but_plugin_empty",
)

can_handle = static_identity_handler("immersity.ai", "www.immersity.ai")

# Webflow careers page: each role is a <div class="careers_cms_item w-dyn-item"> block with a
# <h2 class="u-text-style-h4"> title, a <div class="u-text-style-h6 u-color-faded"> location,
# and an <a href="/company-careers/<slug>" class="g_clickable_link w-inline-block"> "View Job" link.
_BLOCK_SEP = re.compile(r'(?is)(?=class="[^"]*careers_cms_item[^"]*")')
_TITLE_RE = re.compile(r'(?is)<h2[^>]*class="[^"]*u-text-style-h4[^"]*"[^>]*>(.*?)</h2>')
_LOCATION_RE = re.compile(r'(?is)class="[^"]*u-color-faded[^"]*"[^>]*>(.*?)</div>')
_LINK_RE = re.compile(r'(?is)href="([^"]*/company-careers/[^"]+)"')


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen: set[str] = set()
    blocks = _BLOCK_SEP.split(ctx.html or "")
    for block in blocks[1:]:
        if "careers_cms_item" not in block[:80]:
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
