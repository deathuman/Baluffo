from __future__ import annotations

"""Stable import surface for discovery reporting helpers."""

from .candidate_review import build_candidate_review_payload, enrich_candidates_for_review
from .reporting_backlog import build_m5_strategic_backlog, update_candidate_review_metadata
from .reporting_candidates import merge_candidate_streams, stage_curated_seed_candidates
from .reporting_progress import (
    build_discovery_task_progress,
    build_stage_summary,
    emit_log,
    write_discovery_progress_report,
)

__all__ = [
    "build_discovery_task_progress",
    "build_candidate_review_payload",
    "build_m5_strategic_backlog",
    "build_stage_summary",
    "enrich_candidates_for_review",
    "emit_log",
    "merge_candidate_streams",
    "stage_curated_seed_candidates",
    "update_candidate_review_metadata",
    "write_discovery_progress_report",
]
