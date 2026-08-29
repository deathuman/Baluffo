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
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

_SPEC = SimpleStaticPlugin(
    source_id="perfectgarbage",
    default_company="Perfect Garbage",
    parser_stale_hint="perfectgarbage_listing_present_but_plugin_empty",
)

can_handle = static_identity_handler("perfectgarbage.com", "www.perfectgarbage.com")

# Squarespace careers page: each posting is a plain anchor to the Work With Indies games-jobs
# board with a "Hiring: <title>" label, e.g.
# <a href="https://www.workwithindies.com/careers/perfect-garbage-senior-programmer">Hiring: Senior Programmer</a>
_LINK_RE = re.compile(
    r'(?is)<a[^>]+href="([^"]*workwithindies\.com/careers/perfect-garbage-[^"]+)"[^>]*>(.*?)</a>'
)
_HIRING_PREFIX_RE = re.compile(r"(?is)^\s*hiring\s*:\s*")


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen: set[str] = set()
    for href, inner in _LINK_RE.findall(ctx.html or ""):
        title = clean_text(strip_html_text(inner))
        title = _HIRING_PREFIX_RE.sub("", title).strip()
        link = clean_text(urljoin(ctx.page_url, unescape(href)))
        if not title or not link or link in seen:
            continue
        seen.add(link)
        jobs.append(static_job_row(ctx, link=link, title=title))
    return jobs


run = simple_static_run(_SPEC, _parse_html)
