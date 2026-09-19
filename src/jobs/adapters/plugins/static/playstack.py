from __future__ import annotations

import re

from src.jobs.adapters.plugins.static._runner import (
    looks_like_listing_role_title,
    static_list_only_leaf,
)

# Astro-built careers page: each role is a <span id="dynamic-title">Role</span> in a
# card grid with no per-role detail link. The page hero heading shares the same
# id="dynamic-title" markup ("Join Our Team"), so extracted rows are post-filtered
# with the shared job-title / section-header checks. Duplicate titles and HTML-entity
# variants ("PC & Console …" vs "PC and Console …") collapse via the anchor slug.
_BLOCK_SEP = re.compile(r'(?is)(?=id="dynamic-title")')
_TITLE_RE = re.compile(r'(?is)id="dynamic-title"[^>]*>(.*?)</span>')

_SPEC, can_handle, run = static_list_only_leaf(
    source_id="playstack",
    default_company="Playstack",
    parser_stale_hint="playstack_listing_present_but_plugin_empty",
    identities=("playstack.com", "www.playstack.com"),
    block_sep=_BLOCK_SEP,
    title_re=_TITLE_RE,
    row_filter=lambda row: looks_like_listing_role_title(row.get("title") or ""),
)
