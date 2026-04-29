from __future__ import annotations

import re
from urllib.parse import urljoin

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
    source_id="naconstudiomilan",
    default_company="Nacon Studio Milan",
    parser_stale_hint="listing_cards_present_but_plugin_empty",
    empty_detail_fetch_required=None,
    empty_detail_traversal_mode="",
)


can_handle = static_identity_handler("www.naconstudiomilan.com", "naconstudiomilan.com")


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    jobs: list[RawJob] = []
    seen = set()
    for match in re.finditer(
        r'(?is)<h4[^>]*>\s*(.*?)\s*</h4>.*?<a[^>]+href=["\']([^"\']*/careers/[^"\']+/)["\'][^>]*>\s*Learn more\s*</a>',
        ctx.html,
    ):
        title = clean_text(re.sub(r"(?is)<[^>]+>", " ", match.group(1) or ""))
        link = clean_text(urljoin(ctx.page_url, match.group(2) or ""))
        if not title or not link or link in seen:
            continue
        seen.add(link)
        jobs.append(static_job_row(ctx, link=link, title=title))
    return jobs


run = simple_static_run(_SPEC, _parse_html)
