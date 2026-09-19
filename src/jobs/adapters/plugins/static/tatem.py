from __future__ import annotations

import re

from src.jobs.adapters.plugins.static._runner import static_list_only_leaf

# Tilda careers page: each role is a
# <div class="t-card__title t-name t-name_lg t650__bottommargin" field="li_title__…">Role</div>
# card with the details inline — no per-role detail link (the only anchor is the
# page's own /tatemjobs link).
_BLOCK_SEP = re.compile(r'(?is)(?=class="t-card__title)')
_TITLE_RE = re.compile(r'(?is)class="t-card__title[^"]*"[^>]*>(.*?)</div>')

_SPEC, can_handle, run = static_list_only_leaf(
    source_id="tatem",
    default_company="Tatem Games",
    parser_stale_hint="tatem_listing_present_but_plugin_empty",
    identities=("tatem.games", "www.tatem.games"),
    block_sep=_BLOCK_SEP,
    title_re=_TITLE_RE,
)
