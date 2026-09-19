"""Shared fetch-report normalization coordinator: composed leaves plus the compatibility surface.

AI boundary owns: shared fetch-report normalization across bridge and jobs report contracts.
AI boundary implement in: the fetch_report_normalization_* leaves -- row primitives in fetch_report_normalization_primitives, enrichment in fetch_report_normalization_row_enrichment, loss accounting in fetch_report_normalization_loss, detail rows in fetch_report_normalization_detail, timing in fetch_report_normalization_timing, and social channels in fetch_report_normalization_social.
AI boundary search before contracts: fetch-report route leaves, jobs report contracts, and DATA_CONTRACT.md.
AI boundary verify: `npm run lint:repo-guardrails` plus focused fetch-report normalization tests.
"""

from __future__ import annotations

import ast
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.shared.json_shapes import as_json_list, as_json_object, json_object_rows

# Compatibility surface: every name below was reachable from this module before
# the split. The preamble is kept whole so incidental names such as ast, json
# and dataclass stay reachable (`ast` in particular is a monkeypatch target),
# and `__all__` marks them as intentional re-exports for ruff.
__all__ = [
    "Any",
    "Callable",
    "FINALIZATION_TIMING_KEYS",
    "annotations",
    "apply_jobs_fetch_report_details",
    "apply_jobs_fetch_report_zero_kept_classification",
    "as_json_list",
    "as_json_object",
    "ast",
    "coerce_fetch_report_detail_row",
    "dataclass",
    "enrich_fetch_report_dead_listing_fields",
    "enrich_fetch_report_source_row_metadata",
    "enrich_jobs_fetch_report_dynamic_redundant_provider_fields",
    "enrich_jobs_fetch_report_provider_migration_fields",
    "enrich_jobs_fetch_report_site_changed_url_surface",
    "enrich_jobs_fetch_report_source_row_fields",
    "json",
    "json_object_rows",
    "normalize_bridge_fetch_report_source_row",
    "normalize_fetch_report_detail_stats",
    "normalize_fetch_report_loss",
    "normalize_fetch_report_social_channel",
    "normalize_fetch_report_social_summary",
    "normalize_fetch_report_source_row_base",
    "normalize_fetch_report_stage_timings",
    "normalize_fetch_report_timing_summary",
    "normalize_finalization_timing",
    "normalize_jobs_fetch_report_detail_item",
    "normalize_jobs_fetch_report_source_row_base",
]


# Leaf composition. The graph is strictly acyclic: primitives depend on nothing
# in this package, every other leaf depends only on primitives, and this
# coordinator depends on the leaves. Any import order therefore resolves.
from src.shared.fetch_report_normalization_detail import (
    apply_jobs_fetch_report_details,
    normalize_bridge_fetch_report_source_row,
    normalize_fetch_report_detail_stats,
    normalize_jobs_fetch_report_detail_item,
)
from src.shared.fetch_report_normalization_loss import (
    apply_jobs_fetch_report_zero_kept_classification,
    normalize_fetch_report_loss,
)
from src.shared.fetch_report_normalization_primitives import (
    FINALIZATION_TIMING_KEYS,
    coerce_fetch_report_detail_row,
    normalize_fetch_report_source_row_base,
    normalize_jobs_fetch_report_source_row_base,
)
from src.shared.fetch_report_normalization_row_enrichment import (
    enrich_fetch_report_dead_listing_fields,
    enrich_fetch_report_source_row_metadata,
    enrich_jobs_fetch_report_dynamic_redundant_provider_fields,
    enrich_jobs_fetch_report_provider_migration_fields,
    enrich_jobs_fetch_report_site_changed_url_surface,
    enrich_jobs_fetch_report_source_row_fields,
)
from src.shared.fetch_report_normalization_social import (
    normalize_fetch_report_social_channel,
    normalize_fetch_report_social_summary,
)
from src.shared.fetch_report_normalization_timing import (
    normalize_fetch_report_stage_timings,
    normalize_fetch_report_timing_summary,
    normalize_finalization_timing,
)
