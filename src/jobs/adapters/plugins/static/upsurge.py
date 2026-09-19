from __future__ import annotations

import re

from src.jobs.adapters.plugins.static._runner import static_list_only_leaf

# Upsurge careers page: each role is a <section class=CareerSummary> with an
# <h3 class=CareerSummary__Title> holding the role name, followed by a
# <table class=CareerSummary__Data> of Job Description / Requirements rows.
# Note: the source HTML emits unquoted attribute values (class=CareerSummary__Title).
_BLOCK_SEP = re.compile(r'(?is)(?=class\s*=\s*["\']*CareerSummary\b)')
_TITLE_RE = re.compile(
    r'(?is)<h3[^>]*class\s*=\s*["\']*[^\s"\'<>]*CareerSummary__Title[^\s"\'<>]*["\']*[^>]*>(.*?)</h3>'
)

# List-only board: the page emits no per-role links, so the shared helper anchors
# each row to the careers page with a title-derived ?static-role=<slug> query
# parameter (query params survive pipeline URL normalization; fragments do not).
_SPEC, can_handle, run = static_list_only_leaf(
    source_id="upsurge",
    default_company="Upsurge Studios",
    parser_stale_hint="upsurge_listing_present_but_plugin_empty",
    identities=("upsurgestudios.com", "www.upsurgestudios.com"),
    block_sep=_BLOCK_SEP,
    title_re=_TITLE_RE,
)
