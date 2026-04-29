from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticContext,
    SimpleStaticPlugin,
    run_simple_static_plugin,
    static_job_row,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

_SPEC = SimpleStaticPlugin(
    source_id="naconstudiomilan",
    default_company="Nacon Studio Milan",
    parser_stale_hint="listing_cards_present_but_plugin_empty",
    empty_detail_fetch_required=None,
    empty_detail_traversal_mode="",
)


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity in ("www.naconstudiomilan.com", "naconstudiomilan.com")


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
