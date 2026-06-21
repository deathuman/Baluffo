"""Source execution stage for `src.jobs.pipeline`.

This module stays the stable owner of the patch-sensitive execution helpers while delegating the
actual source loop, report assembly, and progress plumbing into focused leaves.

AI boundary owns: stable patch-sensitive source execution stage wiring for jobs pipeline runs.
AI boundary implement in: this file for source stage orchestration only; source loop, progress, and result assembly stay in focused leaves.
AI boundary search before contracts: jobs_fetcher compatibility seams, pipeline source leaves, and source execution tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused source execution tests.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass
from threading import BoundedSemaphore
from typing import Any

from src.jobs.common.config import SOURCE_DIAGNOSTICS as _SOURCE_DIAGNOSTICS
from src.jobs.common.taxonomy import (
    ClassificationContext,
    FailureBucket,
    ZeroExtractDiagnosis,
    ZeroKeptClassification,
    assess_zero_extract,
)
from src.jobs.pipeline_runtime_summary import (
    update_fetch_work_item_progress as _update_fetch_work_item_progress,
)
from src.jobs.text_utils import clean_text, norm_text
from src.shared.utils import now_iso as _now_iso

from . import pipeline_source_loop as pipeline_source_loop_mod
from . import pipeline_source_progress as pipeline_source_progress_mod
from . import pipeline_source_results as pipeline_source_results_mod
from .state_source_records import set_browser_fallback_state as _set_browser_fallback_state

TryPlaywrightFn = Callable[[str, int], tuple[str, str]]

pipeline_source_loop_mod.root = sys.modules[__name__]
pipeline_source_progress_mod.root = sys.modules[__name__]
pipeline_source_results_mod.root = sys.modules[__name__]

# Preserve the stable root-owned helpers that the focused leaf modules resolve through `root`.
SOURCE_DIAGNOSTICS = _SOURCE_DIAGNOSTICS
update_fetch_work_item_progress = _update_fetch_work_item_progress
now_iso = _now_iso
set_browser_fallback_state = _set_browser_fallback_state


def resolve_fetch_browser_fallback_helper() -> TryPlaywrightFn | None:
    try:
        from src.bridge.source_check_http import try_fetch_with_playwright

        return try_fetch_with_playwright
    except ImportError:
        return None


def _build_capped_try_playwright(
    try_playwright: TryPlaywrightFn,
    *,
    max_concurrent: int,
) -> TryPlaywrightFn:
    gate = BoundedSemaphore(max(1, int(max_concurrent or 1)))

    def capped_try_playwright(url: str, timeout_s: int) -> tuple[str, str]:
        gate.acquire()
        try:
            return try_playwright(url, timeout_s)
        finally:
            gate.release()

    return capped_try_playwright


def _default_adapter_for_loader(name: str, base_meta: dict[str, Any]) -> str:
    adapter = clean_text(base_meta.get("adapter"))
    if adapter:
        return adapter
    if clean_text(name).startswith("static_source::"):
        return "static"
    return "custom"


def _is_provider_family_adapter(adapter_name: str) -> bool:
    return norm_text(adapter_name) in {
        "ashby",
        "breezy",
        "greenhouse",
        "jazzhr",
        "lever",
        "oracle_hcm",
        "personio",
        "pinpoint",
        "recruitee",
        "smartrecruiters",
        "teamtailor",
        "workable",
    }


def _is_social_subsource_report(source_name: str, adapter_name: str) -> bool:
    return norm_text(adapter_name) == "social" and clean_text(source_name) in {
        "social_x",
        "social_mastodon",
    }


def _failure_bucket_from_zero_extract_context(
    cls_context: ClassificationContext,
    zero_kept_classification: str = "",
) -> FailureBucket:
    if clean_text(zero_kept_classification) == ZeroKeptClassification.LEGIT_EMPTY.value:
        return FailureBucket.NO_OPENINGS
    diagnosis = assess_zero_extract(cls_context).diagnosis
    if diagnosis == ZeroExtractDiagnosis.EMPTY_CONFIRMED:
        return FailureBucket.NO_OPENINGS
    if diagnosis == ZeroExtractDiagnosis.JS_REQUIRED:
        return FailureBucket.JS_REQUIRED
    if diagnosis == ZeroExtractDiagnosis.SITE_CHANGED:
        return FailureBucket.SITE_CHANGED
    if diagnosis == ZeroExtractDiagnosis.ANTI_BOT_OR_CHALLENGE:
        return FailureBucket.ANTI_BOT_OR_CHALLENGE
    if diagnosis == ZeroExtractDiagnosis.NEEDS_REVIEW:
        return FailureBucket.NEEDS_REVIEW
    return FailureBucket.UNKNOWN


@dataclass(frozen=True)
class SourceExecutionStageConfig:
    max_workers: int
    timeout_s: int
    retries: int
    backoff_s: float
    static_detail_concurrency: int
    google_sheets_redirect_concurrency: int
    started_at: str
    show_progress: bool
    force_refresh_all: bool
    browser_fallback_cooldown_minutes: int


run_source_execution_stage = pipeline_source_loop_mod.run_source_execution_stage

__all__ = [
    "SourceExecutionStageConfig",
    "resolve_fetch_browser_fallback_helper",
    "run_source_execution_stage",
]
