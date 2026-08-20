"""Static listing extraction helpers (thin coordinator).

AI boundary owns: static listing page extraction, candidate link discovery, and listing payload
shaping; public entrypoints and the patched-helper re-export surface stay here.
AI boundary implement in: this coordinator re-exports the implementation leaves (common, state,
flow, plugin, rows, traversal, runner); `process_static_source` stays as the public entrypoint.
AI boundary search before contracts: static runtime, page gating, HTML parsers, and jobs_static tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused static listing tests.
"""

from __future__ import annotations

from src.jobs.adapters.html_parsers import (
    maybe_fetch_kojima_job_listing_html as maybe_fetch_kojima_job_listing_html,
)
from src.jobs.adapters.html_parsers import (
    parse_jobpostings_from_html as parse_jobpostings_from_html,
)
from src.jobs.adapters.plugins.static._rendered_cards import (
    extract_rendered_card_jobs as extract_rendered_card_jobs,
)
from src.jobs.adapters.static_detail_heuristics import (
    process_detail_html as process_detail_html,
)
from src.jobs.adapters.static_detail_heuristics import (
    process_detail_link as process_detail_link,
)
from src.jobs.adapters.static_listing_flow import (
    _handle_skip_and_revalidation as _handle_skip_and_revalidation,
)
from src.jobs.adapters.static_listing_plugin import (
    _plugin_static_artifact_detail_result as _plugin_static_artifact_detail_result,
)
from src.jobs.adapters.static_listing_plugin import (
    _probe_empty_plugin_listing as _probe_empty_plugin_listing,
)
from src.jobs.adapters.static_listing_plugin import (
    _run_plugin_fast_path as _run_plugin_fast_path,
)
from src.jobs.adapters.static_listing_plugin import (
    _static_plugin_context as _static_plugin_context,
)
from src.jobs.adapters.static_listing_runner import StaticFetchRunner as StaticFetchRunner
from src.jobs.adapters.static_runtime import StaticSourceContext


def process_static_source(ctx: StaticSourceContext) -> None:
    if _handle_skip_and_revalidation(ctx):
        return
    if _run_plugin_fast_path(ctx):
        return
    StaticFetchRunner(ctx).run()
