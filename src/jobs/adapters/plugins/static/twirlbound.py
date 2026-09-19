from __future__ import annotations

import re

from src.jobs.adapters.plugins.static._runner import static_list_only_leaf

# WordPress ub-content-toggle accordions: each opening is a
# <p class="wp-block-ub-content-toggle-accordion-title …"><strong>Role</strong></p>
# with the posting details inline in the panel — no per-role detail link. The class
# carries a random uuid suffix per accordion, so titles are matched on the stable
# wp-block-ub-content-toggle-accordion-title prefix.
_BLOCK_SEP = re.compile(r'(?is)(?=class="[^"]*wp-block-ub-content-toggle-accordion-title)')
_TITLE_RE = re.compile(
    r'(?is)class="[^"]*wp-block-ub-content-toggle-accordion-title[^"]*"[^>]*><strong>(.*?)</strong>'
)

_SPEC, can_handle, run = static_list_only_leaf(
    source_id="twirlbound",
    default_company="Twirlbound",
    parser_stale_hint="twirlbound_listing_present_but_plugin_empty",
    identities=("twirlbound.com", "www.twirlbound.com"),
    block_sep=_BLOCK_SEP,
    title_re=_TITLE_RE,
)
