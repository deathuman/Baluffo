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
    source_id="twirlbound",
    default_company="Twirlbound",
    parser_stale_hint="twirlbound_listing_present_but_plugin_empty",
)

can_handle = static_identity_handler("twirlbound.com", "www.twirlbound.com")

# WordPress ub-content-toggle accordions: each opening is a
# <p class="wp-block-ub-content-toggle-accordion-title …"><strong>Role</strong></p>
# with the posting details inline in the panel — no per-role detail link. The class
# carries a random uuid suffix per accordion, so titles are matched on the stable
# wp-block-ub-content-toggle-accordion-title prefix.
_BLOCK_SEP = re.compile(r'(?is)(?=class="[^"]*wp-block-ub-content-toggle-accordion-title)')
_TITLE_RE = re.compile(
    r'(?is)class="[^"]*wp-block-ub-content-toggle-accordion-title[^"]*"[^>]*><strong>(.*?)</strong>'
)


def _parse_html(ctx: SimpleStaticContext) -> list[RawJob]:
    return static_list_only_job_rows(ctx, block_sep=_BLOCK_SEP, title_re=_TITLE_RE)


run = simple_static_run(_SPEC, _parse_html)
