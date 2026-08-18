"""
Static plugin for cdprojektred.com careers. Fetches listing and parses with
parse_jobpostings_from_html. Uses try_playwright when provided on fetch failure
or JS shell so JS-rendered content can be extracted.
"""

from __future__ import annotations

from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticContext,
    SimpleStaticPlugin,
    simple_static_run,
    static_identity_handler,
)
from src.jobs.models import RawJob

_SPEC = SimpleStaticPlugin(
    source_id="cdprojektred",
    default_company="CD Projekt Red",
    fallback_source="cdprojektred",
    playwright_on_fetch_error=True,
    playwright_on_js_shell=True,
    require_generic_parser=True,
)


can_handle = static_identity_handler("cdprojektred.com", "www.cdprojektred.com")


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    parser = ctx.parse_jobpostings_from_html
    if not callable(parser):
        return []
    return parser(
        ctx.html,
        base_url=ctx.page_url,
        fallback_company=ctx.company,
        fallback_source_id_prefix=f"static:{ctx.source_id}",
    )


run = simple_static_run(_SPEC, _parse_html)
