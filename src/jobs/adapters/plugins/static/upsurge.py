from __future__ import annotations

import re
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
    source_id="upsurge",
    default_company="Upsurge Studios",
    parser_stale_hint="upsurge_listing_present_but_plugin_empty",
)

can_handle = static_identity_handler("upsurgestudios.com", "www.upsurgestudios.com")

# Upsurge careers page: each role is a <section class=CareerSummary> with an
# <h3 class=CareerSummary__Title> holding the role name, followed by a
# <table class=CareerSummary__Data> of Job Description / Requirements rows.
# Note: the source HTML emits unquoted attribute values (class=CareerSummary__Title).
_BLOCK_SEP = re.compile(r'(?is)(?=class\s*=\s*["\']*CareerSummary\b)')
_TITLE_RE = re.compile(
    r'(?is)<h3[^>]*class\s*=\s*["\']*[^\s"\'<>]*CareerSummary__Title[^\s"\'<>]*["\']*[^>]*>(.*?)</h3>'
)


def _slug(title: str) -> str:
    return clean_text(title).lower().replace("&", "and").replace(" ", "-")


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen: set[str] = set()
    blocks = _BLOCK_SEP.split(ctx.html or "")
    for block in blocks[1:]:
        title_match = _TITLE_RE.search(block)
        if not title_match:
            continue
        title = clean_text(strip_html_text(title_match.group(1)))
        if not title:
            continue
        # No per-role detail URL exists, so anchor the row to the careers page with
        # a title-derived fragment to keep rows distinct and on-domain.
        link = clean_text(urljoin(ctx.page_url, "#" + _slug(title)))
        if link in seen:
            continue
        seen.add(link)
        jobs.append(static_job_row(ctx, link=link, title=title))
    return jobs


run = simple_static_run(_SPEC, _parse_html)
