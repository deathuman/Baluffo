from __future__ import annotations

import re

from src.jobs.adapters.plugins.static._runner import static_list_only_leaf
from src.jobs.text_utils import clean_text

# Elementor tabs: each role is a
# <div class="elementor-tab-title elementor-tab-desktop-title" …>ROLE</div> with no
# per-role detail link. Only the desktop-title variant is split: the
# elementor-tab-mobile-title divs repeat the same labels, which would otherwise
# double rows (dedup by anchor would collapse them anyway, but this keeps the parse
# clean). The page's own nav includes a "work with us" tab that is not a role.
_BLOCK_SEP = re.compile(r'(?is)(?=class="elementor-tab-title elementor-tab-desktop-title")')
_TITLE_RE = re.compile(
    r'(?is)class="elementor-tab-title elementor-tab-desktop-title"[^>]*>(.*?)</div>'
)

_NAV_TAB_TITLES = frozenset({"work with us"})

_SPEC, can_handle, run = static_list_only_leaf(
    source_id="animvs",
    default_company="Animus Game Studio",
    parser_stale_hint="animvs_listing_present_but_plugin_empty",
    identities=("animvs.com", "www.animvs.com"),
    block_sep=_BLOCK_SEP,
    title_re=_TITLE_RE,
    row_filter=lambda row: clean_text(row.get("title") or "").casefold() not in _NAV_TAB_TITLES,
)
