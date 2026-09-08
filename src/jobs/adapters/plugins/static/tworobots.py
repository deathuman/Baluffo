from __future__ import annotations

import re

from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticContext,
    SimpleStaticPlugin,
    simple_static_run,
    static_identity_handler,
    static_list_only_job_rows,
)
from src.jobs.models import RawJob

_SPEC = SimpleStaticPlugin(
    source_id="tworobots",
    default_company="Two Robots Studios",
    parser_stale_hint="tworobots_listing_present_but_plugin_empty",
)

can_handle = static_identity_handler("trb.tworobots.com", "www.trb.tworobots.com")

# Server-rendered role cards on the careers page: each opening is a
# <div class="role-card ..."> block with a <div class="role-title">Title</div>
# and a <div class="role-sub">Dept · Location · Type</div>. All roles share a
# single external Airtable apply form (no per-role detail URL), so rows are
# anchored on-page with the shared query-anchor scheme to stay distinct.
_BLOCK_SEP = re.compile(r'(?is)(?=class="[^"]*role-card)')
_TITLE_RE = re.compile(r'(?is)class="[^"]*role-title[^"]*"[^>]*>(.*?)</div>')


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    return static_list_only_job_rows(ctx, block_sep=_BLOCK_SEP, title_re=_TITLE_RE)


run = simple_static_run(_SPEC, _parse_html)
