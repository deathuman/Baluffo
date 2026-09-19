from __future__ import annotations

import re

from src.jobs.adapters.plugins.static._runner import static_list_only_leaf

# Server-rendered role cards on the careers page: each opening is a
# <div class="role-card ..."> block with a <div class="role-title">Title</div>
# and a <div class="role-sub">Dept · Location · Type</div>. All roles share a
# single external Airtable apply form (no per-role detail URL), so rows are
# anchored on-page with the shared query-anchor scheme to stay distinct.
_BLOCK_SEP = re.compile(r'(?is)(?=class="[^"]*role-card)')
_TITLE_RE = re.compile(r'(?is)class="[^"]*role-title[^"]*"[^>]*>(.*?)</div>')

_SPEC, can_handle, run = static_list_only_leaf(
    source_id="tworobots",
    default_company="Two Robots Studios",
    parser_stale_hint="tworobots_listing_present_but_plugin_empty",
    identities=("trb.tworobots.com", "www.trb.tworobots.com"),
    block_sep=_BLOCK_SEP,
    title_re=_TITLE_RE,
)
