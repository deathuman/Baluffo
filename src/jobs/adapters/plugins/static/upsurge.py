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
    source_id="upsurge",
    default_company="Upsurge Studios",
    parser_stale_hint="upsurge_listing_present_but_plugin_empty",
)

can_handle = static_identity_handler("upsurgestudios.com", "www.upsurgestudios.com")

# Upsurge careers page: each role is a <section class=CareerSummary> with an
# <h3 class=CareerSummary__Title> holding the role name, followed by a
# <table class=CareerSummary__Data> of Job Description / Requirements rows.
# Note: the source HTML emits unquoted attribute values (class=CareerSummary__Title).
_BLOCK_SEP = re.compile(r'(?is)(?=class\s*=\s*["\']*CareerSummary\b)')
_TITLE_RE = re.compile(
    r'(?is)<h3[^>]*class\s*=\s*["\']*[^\s"\'<>]*CareerSummary__Title[^\s"\'<>]*["\']*[^>]*>(.*?)</h3>'
)


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    # List-only board: the page emits no per-role links, so the shared helper anchors
    # each row to the careers page with a title-derived ?static-role=<slug> query
    # parameter (query params survive pipeline URL normalization; fragments do not).
    return static_list_only_job_rows(ctx, block_sep=_BLOCK_SEP, title_re=_TITLE_RE)


run = simple_static_run(_SPEC, _parse_html)
