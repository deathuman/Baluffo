"""Lifecycle, timing, and memory-management helpers for pipeline finalization.

AI boundary owns: runtime timing summaries, job lifecycle state application/archiving,
and RSS memory logging during the finalization phase.
AI boundary implement in: this file for lifecycle/timing helpers; output writing and the
finalization conductor stay in sibling finalize_* leaves and ``pipeline_finalize.py``.
AI boundary search before contracts: pipeline finalization tests and lifecycle state contracts.
AI boundary verify: `npm run lint:repo-guardrails` plus focused pipeline finalization tests.
"""

from __future__ import annotations

import ctypes
import gc
from pathlib import Path
from typing import Any

from src.jobs.dedup import CanonicalDeduplicator
from src.jobs.models import CanonicalJob
from src.jobs.pipeline_run_setup import canonicalize_existing_output_row
from src.jobs.pipeline_timing import build_runtime_timing_summary, percentile_ms
from src.jobs.state_lifecycle import (
    apply_job_lifecycle_state,
    build_lifecycle_source_evidence,
    lifecycle_archive_state_path,
    write_job_lifecycle_archive_state,
)
from src.jobs.text_utils import clean_text, norm_text
from src.pipeline_io import read_existing_output


def _runtime_timing_summary(
    source_reports: list[dict[str, Any]], *, wall_clock_duration_ms: int
) -> dict[str, Any]:
    return build_runtime_timing_summary(
        source_reports,
        wall_clock_duration_ms=wall_clock_duration_ms,
        clean_text_fn=clean_text,
        norm_text_fn=norm_text,
        percentile_ms_fn=percentile_ms,
    )


def _deduplicate_or_preserve_previous(
    *,
    paths,
    canonical_rows: list[CanonicalJob],
    preserve_previous_on_empty: bool,
    started_at: str,
) -> tuple[list[CanonicalJob], dict[str, Any], bool]:
    deduplicator = CanonicalDeduplicator()
    deduped_rows = deduplicator.process(canonical_rows)
    preserved_previous = False
    if preserve_previous_on_empty and not deduped_rows:
        previous_rows = read_existing_output(
            paths.json_path,
            started_at,
            canonicalize_job=canonicalize_existing_output_row,
            clean_text=clean_text,
            canonical_job_cls=CanonicalJob,
        )
        if previous_rows:
            deduped_rows = list(previous_rows)  # already CanonicalJob
            preserved_previous = True
    return deduped_rows, deduplicator.stats, preserved_previous


def _lifecycle_missing_context(
    *,
    source_reports: list[dict[str, Any]],
    selected_loaders: list[tuple[str, Any]],
    using_default_loaders: bool,
    effective_seed_from_existing_output: bool,
) -> dict[str, Any]:
    selected_loader_names = {name for name, _ in selected_loaders}
    # Existing-output seeding is a cache/dedup implementation detail. It must not
    # suppress trustworthy per-source missing evidence.
    may_mark_missing = using_default_loaders
    return build_lifecycle_source_evidence(
        source_reports,
        selected_source_names=selected_loader_names,
        allow_missing=may_mark_missing,
    )


def _apply_lifecycle_state(
    *,
    deduped_rows: list[CanonicalJob],
    observed_rows: list[CanonicalJob],
    lifecycle_rows: dict[str, dict[str, Any]],
    source_reports: list[dict[str, Any]],
    selected_loaders: list[tuple[str, Any]],
    using_default_loaders: bool,
    effective_seed_from_existing_output: bool,
    lifecycle_finished_at: str,
) -> tuple[
    list[CanonicalJob],
    dict[str, dict[str, Any]],
    dict[int, dict[str, dict[str, Any]]],
    dict[str, int],
]:
    source_evidence = _lifecycle_missing_context(
        source_reports=source_reports,
        selected_loaders=selected_loaders,
        using_default_loaders=using_default_loaders,
        effective_seed_from_existing_output=effective_seed_from_existing_output,
    )
    return apply_job_lifecycle_state(
        deduped_rows=deduped_rows,
        observed_rows=observed_rows,
        lifecycle_rows=lifecycle_rows,
        finished_at=lifecycle_finished_at,
        allow_mark_missing=False,
        eligible_missing_sources=source_evidence.get("eligibleMissingSources", set()),
        source_evidence=source_evidence,
    )


def _return_freed_memory_to_os() -> None:
    """Run gc and glibc ``malloc_trim`` so freed Python-heap memory returns to
    the OS instead of staying in the process freelists.

    Without this, the ~500-700 MiB freed after the identity/lifecycle phases
    keeps counting against the container RSS high-water mark, leaving the
    write-heavy phases no headroom on the pi4-tight seat.
    """
    gc.collect()
    try:
        malloc_trim = getattr(ctypes.CDLL(None), "malloc_trim", None)
        if malloc_trim is not None:
            malloc_trim(0)
    except (AttributeError, OSError, TypeError):
        pass  # non-glibc platforms have no malloc_trim; nothing to return


def _write_lifecycle_archive_rows(
    *,
    lifecycle_state_path: Path,
    archive_rows_by_year: dict[int, dict[str, dict[str, Any]]],
) -> None:
    for archive_year, rows in archive_rows_by_year.items():
        if not rows:
            continue
        archive_path = lifecycle_archive_state_path(lifecycle_state_path, archive_year)
        write_job_lifecycle_archive_state(archive_path, rows)


def _lifecycle_summary_payload(lifecycle_counts_map: dict[str, int]) -> dict[str, int]:
    return {
        "activeCount": int(lifecycle_counts_map.get("active") or 0),
        "newCount": int(lifecycle_counts_map.get("new") or 0),
        "carriedInitializedCount": int(lifecycle_counts_map.get("carriedInitialized") or 0),
        "reappearedCount": int(lifecycle_counts_map.get("reappeared") or 0),
        "likelyRemovedCount": int(lifecycle_counts_map.get("likelyRemoved") or 0),
        "archivedCount": int(lifecycle_counts_map.get("archived") or 0),
        "preservedBecauseSourceFailedCount": int(
            lifecycle_counts_map.get("preservedBecauseSourceFailed") or 0
        ),
        "preservedBecauseSourceSkippedCount": int(
            lifecycle_counts_map.get("preservedBecauseSourceSkipped") or 0
        ),
        "eligibleMissingSourceCount": int(
            lifecycle_counts_map.get("eligibleMissingSourceCount") or 0
        ),
        "ineligibleMissingSourceCount": int(
            lifecycle_counts_map.get("ineligibleMissingSourceCount") or 0
        ),
        "availabilityAvailableCount": int(lifecycle_counts_map.get("availabilityAvailable") or 0),
        "availabilityOverdueCount": int(lifecycle_counts_map.get("availabilityOverdue") or 0),
        "availabilityUnavailableCount": int(
            lifecycle_counts_map.get("availabilityUnavailable") or 0
        ),
    }


def _process_rss_mib() -> int:
    try:
        with open("/proc/self/status", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024
    except (OSError, ValueError, IndexError):
        pass
    return 0


def _log_rss(marker: str) -> None:
    print(f"[jobs_fetcher] INFO rssMiB={_process_rss_mib()} {marker}", flush=True)
