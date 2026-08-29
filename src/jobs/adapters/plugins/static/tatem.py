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
    source_id="tatem",
    default_company="Tatem Games",
    parser_stale_hint="tatem_listing_present_but_plugin_empty",
)

can_handle = static_identity_handler("tatem.games", "www.tatem.games")

# Tilda careers page: each role is a
# <div class="t-card__title t-name t-name_lg t650__bottommargin" field="li_title__…">Role</div>
# card with the details inline — no per-role detail link (the only anchor is the
# page's own /tatemjobs link).
_BLOCK_SEP = re.compile(r'(?is)(?=class="t-card__title)')
_TITLE_RE = re.compile(r'(?is)class="t-card__title[^"]*"[^>]*>(.*?)</div>')


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    return static_list_only_job_rows(ctx, block_sep=_BLOCK_SEP, title_re=_TITLE_RE)


run = simple_static_run(_SPEC, _parse_html)
