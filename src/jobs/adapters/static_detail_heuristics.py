"""Static detail-page heuristic helpers - thin coordinator.

AI boundary owns: static detail URL filtering, detail-page scoring, and non-job page rejection heuristics.
AI boundary implement in: this coordinator re-exports the four implementation leaves (static_detail_heuristics_{filter,config,parse,entry}.py); the extract_rendered_card_jobs alias stays here.
AI boundary search before contracts: static listing/runtime, page gating, and detail heuristic tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused static detail tests.
"""

from __future__ import annotations

from src.jobs.adapters.plugins.static._rendered_cards import (
    extract_rendered_card_jobs as _extract_rendered_card_jobs,
)
from src.jobs.adapters.static_detail_heuristics_config import (
    choose_detail_traversal_mode,
    source_detail_concurrency_for,
    source_detail_limit_for,
    source_detail_retries_for,
)
from src.jobs.adapters.static_detail_heuristics_entry import (
    process_detail_html,
    process_detail_link,
)
from src.jobs.adapters.static_detail_heuristics_filter import (
    add_detail_link,
    is_known_non_job_detail_url,
    is_probable_job_detail_url,
)
from src.jobs.adapters.static_detail_heuristics_parse import (
    _is_one_man_studio_noise_city,
)

extract_rendered_card_jobs = _extract_rendered_card_jobs
