"""
Static plugin for example.org (and subdomains). Fetches first page and parses with
parse_jobpostings_from_html. Use as a template for adding new static site plugins.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticContext,
    SimpleStaticPlugin,
    run_simple_static_plugin,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.models import RawJob

_SPEC = SimpleStaticPlugin(
    source_id="example_org",
    default_company="Unknown",
    fallback_source="example_org",
    use_page_gate=True,
    require_generic_parser=True,
)


def can_handle(ctx: AdapterPluginContext) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    return identity == "example.org"


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    return ctx.parse_jobpostings_from_html(
        ctx.html,
        base_url=ctx.page_url,
        fallback_company=ctx.company,
        fallback_source_id_prefix=f"static:{ctx.source_id}",
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
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        **kwargs,
    )
