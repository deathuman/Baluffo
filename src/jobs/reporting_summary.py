"""Summary helpers for jobs pipeline reporting output."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from src.jobs.models import CanonicalJob
from src.jobs.text_utils import clean_text, norm_text

from .common import config as common_config
from .reporting_breakdowns import (
    build_blank_residue_breakdown,
    build_needs_review_breakdown,
    build_unknown_static_breakdown,
)

TARGET_PROFESSIONS = common_config.TARGET_PROFESSIONS


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def format_source_error(source_name: str, error: Any) -> str:
    message = clean_text(str(error))
    prefix = f"{clean_text(source_name)}:"
    if not message:
        return "unknown error"
    if message.lower().startswith(prefix.lower()):
        return message
    return f"{source_name}: {message}"


def _cache_rows(source_reports: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in source_reports:
        if not isinstance(row, dict):
            continue
        rows.append(row)
        details = _as_list(row.get("details"))
        rows.extend([item for item in details if isinstance(item, dict)])
    return rows


def _has_ok_warning(row: dict[str, Any]) -> bool:
    if norm_text(row.get("status")) != "ok":
        return False
    for key in ("warning", "error", "diagnostic", "message"):
        if clean_text(row.get(key)):
            return True
    for key in ("warnings", "errors", "partialErrors"):
        values = row.get(key)
        if isinstance(values, list) and any(clean_text(item) for item in values):
            return True
    return False


def build_pipeline_summary(
    dedup_stats: dict[str, int],
    deduped_rows: Sequence[CanonicalJob],
    source_reports: Sequence[dict[str, Any]],
    canonical_count: int,
    preserved_previous: bool,
    active_source_count: int,
    pending_source_count: int,
    newly_approved_since_last_run: int,
    *,
    json_bytes: int,
    csv_bytes: int,
    light_json_bytes: int,
    lifecycle_counts_map: dict[str, int] | None = None,
    summary_source_rows: Sequence[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    deduped_payload = [
        row.to_dict() if isinstance(row, CanonicalJob) else dict(row) for row in deduped_rows
    ]
    lifecycle = lifecycle_counts_map or {}
    cache_rows = _cache_rows(source_reports)
    cache_decision_counts: dict[str, int] = {}
    for row in cache_rows:
        decision = clean_text(row.get("cacheDecision"))
        if decision:
            cache_decision_counts[decision] = int(cache_decision_counts.get(decision, 0)) + 1
    source_count_rows = [
        row for row in (summary_source_rows or source_reports) if isinstance(row, dict)
    ]
    raw_fetched = int(
        sum(
            int(row.get("fetchedCount") or 0)
            for row in source_count_rows
            if norm_text(row.get("status")) == "ok"
        )
    )
    canonical_kept = int(canonical_count)
    canonical_dropped = max(0, raw_fetched - canonical_kept)
    dedup_merged = int(dedup_stats.get("mergedCount") or 0)
    final_output = len(deduped_payload)
    ok_source_count = sum(1 for row in source_count_rows if row["status"] == "ok")
    ok_with_warning_count = sum(1 for row in source_count_rows if _has_ok_warning(row))
    return {
        **dedup_stats,
        "rawFetched": raw_fetched,
        "canonicalDropped": canonical_dropped,
        "canonicalKept": canonical_kept,
        "dedupMerged": dedup_merged,
        "finalOutput": final_output,
        "rawFetchedCount": raw_fetched,
        "uniqueOutputCount": len(deduped_payload),
        "sourceBundleCollisions": sum(
            1 for row in deduped_payload if int(row.get("sourceBundleCount") or 0) > 1
        ),
        "targetRoleCount": sum(
            1 for row in deduped_payload if norm_text(row.get("profession")) in TARGET_PROFESSIONS
        ),
        "netherlandsCount": sum(
            1 for row in deduped_payload if clean_text(row.get("country")).upper() == "NL"
        ),
        "remoteCount": sum(
            1 for row in deduped_payload if norm_text(row.get("workType")) == "remote"
        ),
        "targetRoleNetherlandsCount": sum(
            1
            for row in deduped_payload
            if norm_text(row.get("profession")) in TARGET_PROFESSIONS
            and clean_text(row.get("country")).upper() == "NL"
        ),
        "targetRoleRemoteCount": sum(
            1
            for row in deduped_payload
            if norm_text(row.get("profession")) in TARGET_PROFESSIONS
            and norm_text(row.get("workType")) == "remote"
        ),
        "preservedPreviousOutput": preserved_previous,
        "sourceCount": len(source_count_rows),
        "successfulSources": ok_source_count,
        "okCleanSources": max(0, ok_source_count - ok_with_warning_count),
        "okWithWarningSources": ok_with_warning_count,
        "failedSources": sum(1 for row in source_count_rows if row["status"] == "error"),
        "excludedSources": sum(1 for row in source_count_rows if row["status"] == "excluded"),
        "cacheSkippedCount": sum(
            1
            for row in cache_rows
            if norm_text(row.get("status")) == "excluded"
            and clean_text(row.get("cacheDecision")) in {"skip_fresh", "cooldown_skip"}
        ),
        "revalidatedCount": sum(
            1 for row in cache_rows if clean_text(row.get("cacheDecision")) == "revalidate_only"
        ),
        "notModifiedCount": sum(
            1
            for row in cache_rows
            if clean_text(row.get("cacheDecisionReason")) == "not_modified_304"
        ),
        "listingOnlyCount": sum(
            1 for row in cache_rows if clean_text(row.get("cacheDecision")) == "listing_only"
        ),
        "detailSkippedByListingFingerprintCount": sum(
            1 for row in cache_rows if bool(row.get("detailSkippedByListingFingerprint"))
        ),
        "cacheDecisionCounts": cache_decision_counts,
        "activeSourceCount": active_source_count,
        "pendingSourceCount": pending_source_count,
        "newlyApprovedSinceLastRun": newly_approved_since_last_run,
        "jsonBytes": int(json_bytes),
        "csvBytes": int(csv_bytes),
        "lightJsonBytes": int(light_json_bytes),
        "sizeGuardrailExceeded": bool(json_bytes > 50_000_000 or csv_bytes > 50_000_000),
        "recordGuardrailExceeded": bool(len(deduped_payload) > 100_000),
        "lifecycleActiveCount": int(lifecycle.get("active") or 0),
        "lifecycleLikelyRemovedCount": int(lifecycle.get("likelyRemoved") or 0),
        "lifecycleArchivedCount": int(lifecycle.get("archived") or 0),
        "lifecycleTrackedCount": int(lifecycle.get("totalTracked") or 0),
        "blankResidueBreakdown": build_blank_residue_breakdown(source_reports),
        "needsReviewBreakdown": build_needs_review_breakdown(source_reports),
        "unknownStaticBreakdown": build_unknown_static_breakdown(source_reports),
    }
