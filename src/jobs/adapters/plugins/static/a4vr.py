from __future__ import annotations

import re

from src.jobs.adapters.plugins.static._runner import static_list_only_leaf

# Squarespace careers page: each posting is an <h2><strong>POSITION: ...</strong></h2>
# block with no per-role detail link. Blocks are split only on "POSITION:" headings:
# the trailing "INITIATIVBEWERBUNG - TALENTE FÜR VR/AR" h2 is a speculative "send us
# your CV" block, not an opening, so it is never a split point (and is filtered
# defensively below in case markup order ever changes).
_BLOCK_SEP = re.compile(r"(?is)(?=<h2[^>]*><strong>POSITION:)")
# The "POSITION: " label is page chrome, not part of the role name; it is excluded
# from the capture so titles and anchor slugs carry the clean role name.
_TITLE_RE = re.compile(r"(?is)<h2[^>]*><strong>POSITION:\s*(.*?)</strong></h2>")


def _is_speculative(title: str) -> bool:
    lowered = title.casefold()
    return any(
        token in lowered
        for token in (
            "initiativbewerbung",
            "initiative application",
            "spontaneous",
            "speculative",
        )
    )


# Defensive: the speculative INITIATIVBEWERBUNG block is never a split point, but
# drop it explicitly in case the page markup order ever changes.
_SPEC, can_handle, run = static_list_only_leaf(
    source_id="a4vr",
    default_company="A4VR",
    parser_stale_hint="a4vr_listing_present_but_plugin_empty",
    identities=("a4vr.com", "www.a4vr.com"),
    block_sep=_BLOCK_SEP,
    title_re=_TITLE_RE,
    row_filter=lambda row: not _is_speculative(row.get("title") or ""),
)
