from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticContext,
    SimpleStaticPlugin,
    generic_parser_then_detail_links,
    simple_static_run,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob
from src.scrapers import domain_profiles


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity == "larian.com"


def _parse_html(ctx: SimpleStaticContext) -> list[dict[str, Any]]:
    profile = domain_profiles.domain_profile_for_url(ctx.page_url)
    return generic_parser_then_detail_links(
        ctx,
        extra_anchor_filter=lambda url: domain_profiles.is_probable_job_detail_url(url, profile),
    )


_larian_run = simple_static_run(
    spec=SimpleStaticPlugin(
        source_id="larian",
        default_company="Larian",
        playwright_on_fetch_error=True,
        playwright_on_js_shell=True,
        parser_stale_hint="larian_listing_empty",
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
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    **kwargs: Any,
) -> list[RawJob]:
    return _larian_run(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        pages=pages,
        source_row=source_row,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        try_playwright=try_playwright,
        **kwargs,
    )
