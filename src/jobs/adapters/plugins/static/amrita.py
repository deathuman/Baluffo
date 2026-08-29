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
    source_id="amrita",
    default_company="Amrita Studio",
    parser_stale_hint="amrita_listing_present_but_plugin_empty",
)

can_handle = static_identity_handler("amrita.studio", "www.amrita.studio")

# SP Page Builder accordion: each role is a
# <span class="sppb-panel-title" aria-label="Role"><i …></i> Role</span> heading with
# no per-role detail link. The aria-label carries the clean role name, so it is used
# directly as the title source (the visible text repeats it after a FontAwesome icon).
_BLOCK_SEP = re.compile(r'(?is)(?=class="sppb-panel-title")')
_TITLE_RE = re.compile(r'(?is)class="sppb-panel-title" aria-label="([^"]+)"')


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    return static_list_only_job_rows(ctx, block_sep=_BLOCK_SEP, title_re=_TITLE_RE)


run = simple_static_run(_SPEC, _parse_html)
