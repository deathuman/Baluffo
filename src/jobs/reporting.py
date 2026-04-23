"""Stable jobs reporting compatibility surface."""

from __future__ import annotations

from src.jobs.common.contracts import (
    normalize_fetch_report_payload,
    normalize_runtime_payload,
    normalize_source_report_row,
)

from .reporting_breakdowns import (
    build_blank_residue_breakdown,
    build_needs_review_breakdown,
    build_unknown_static_breakdown,
)
from .reporting_queues import (
    build_browser_fallback_queue,
    build_parser_regression_queue,
    count_site_changed_diagnosed_sources,
    count_site_changed_missing_old_url_sources,
)
from .reporting_social import (
    SOCIAL_EXPERIMENT_REVIEW_FILENAME,
    SOCIAL_EXPERIMENT_SAMPLE_SIZE,
    build_social_experiment_review_payload,
    build_social_experiment_review_sample,
    summarize_social_experiment,
)
from .reporting_summary import build_pipeline_summary, format_source_error

__all__ = [
    "SOCIAL_EXPERIMENT_REVIEW_FILENAME",
    "SOCIAL_EXPERIMENT_SAMPLE_SIZE",
    "build_blank_residue_breakdown",
    "normalize_fetch_report_payload",
    "normalize_runtime_payload",
    "normalize_source_report_row",
    "build_browser_fallback_queue",
    "build_needs_review_breakdown",
    "build_pipeline_summary",
    "count_site_changed_diagnosed_sources",
    "count_site_changed_missing_old_url_sources",
    "build_parser_regression_queue",
    "build_social_experiment_review_payload",
    "build_social_experiment_review_sample",
    "build_unknown_static_breakdown",
    "format_source_error",
    "summarize_social_experiment",
]
