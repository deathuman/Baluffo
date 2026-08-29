from __future__ import annotations

import re

from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticContext,
    SimpleStaticPlugin,
    looks_like_listing_role_title,
    simple_static_run,
    static_identity_handler,
    static_list_only_job_rows,
)
from src.jobs.models import RawJob

_SPEC = SimpleStaticPlugin(
    source_id="playstack",
    default_company="Playstack",
    parser_stale_hint="playstack_listing_present_but_plugin_empty",
)

can_handle = static_identity_handler("playstack.com", "www.playstack.com")

# Astro-built careers page: each role is a <span id="dynamic-title">Role</span> in a
# card grid with no per-role detail link. The page hero heading shares the same
# id="dynamic-title" markup ("Join Our Team"), so extracted rows are post-filtered
# with the shared job-title / section-header checks. Duplicate titles and HTML-entity
# variants ("PC & Console …" vs "PC and Console …") collapse via the anchor slug.
_BLOCK_SEP = re.compile(r'(?is)(?=id="dynamic-title")')
_TITLE_RE = re.compile(r'(?is)id="dynamic-title"[^>]*>(.*?)</span>')


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    rows = static_list_only_job_rows(ctx, block_sep=_BLOCK_SEP, title_re=_TITLE_RE)
    return [row for row in rows if looks_like_listing_role_title(row.get("title") or "")]


run = simple_static_run(_SPEC, _parse_html)
