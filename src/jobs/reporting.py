"""Reporting helpers for jobs pipeline output."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from typing import Any

from src.jobs.adapters import community
from src.jobs.common import config as common_config
from src.jobs.common.contracts import (
    normalize_fetch_report_payload,
    normalize_runtime_payload,
    normalize_source_report_row,
)
from src.jobs.models import CanonicalJob
from src.jobs.text_utils import clean_text, norm_text
from src.scrapers.domain_profiles import domain_profile_for_url, pick_canonical_listing_url

TARGET_PROFESSIONS = common_config.TARGET_PROFESSIONS
DEFAULT_FETCH_STRATEGY = common_config.DEFAULT_FETCH_STRATEGY
DEFAULT_ADAPTER_HTTP_CONCURRENCY = common_config.DEFAULT_ADAPTER_HTTP_CONCURRENCY
DEFAULT_STATIC_DETAIL_CONCURRENCY = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = community.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
DEFAULT_HOT_SOURCE_CADENCE_MINUTES = common_config.DEFAULT_HOT_SOURCE_CADENCE_MINUTES
DEFAULT_COLD_SOURCE_CADENCE_MINUTES = common_config.DEFAULT_COLD_SOURCE_CADENCE_MINUTES
DEFAULT_SOCIAL_LOOKBACK_MINUTES = common_config.DEFAULT_SOCIAL_LOOKBACK_MINUTES
DEFAULT_SOCIAL_MIN_CONFIDENCE = common_config.DEFAULT_SOCIAL_MIN_CONFIDENCE
DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE = common_config.DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE


def format_source_error(source_name: str, error: Any) -> str:
    message = clean_text(str(error))
    prefix = f"{clean_text(source_name)}:"
    if not message:
        return "unknown error"
    if message.lower().startswith(prefix.lower()):
        return message
    return f"{source_name}: {message}"


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
) -> dict[str, Any]:
    deduped_payload = [row.to_dict() if isinstance(row, CanonicalJob) else dict(row) for row in deduped_rows]
    lifecycle = lifecycle_counts_map or {}
    cache_rows: list[dict[str, Any]] = []
    for row in source_reports:
        if not isinstance(row, dict):
            continue
        cache_rows.append(row)
        details = row.get("details") if isinstance(row.get("details"), list) else []
        cache_rows.extend([item for item in details if isinstance(item, dict)])
    cache_decision_counts: dict[str, int] = {}
    for row in cache_rows:
        decision = clean_text(row.get("cacheDecision"))
        if decision:
            cache_decision_counts[decision] = int(cache_decision_counts.get(decision, 0)) + 1
    raw_fetched = int(sum(int(row.get("fetchedCount") or 0) for row in source_reports if norm_text(row.get("status")) == "ok"))
    canonical_kept = int(canonical_count)
    canonical_dropped = max(0, raw_fetched - canonical_kept)
    dedup_merged = int(dedup_stats.get("mergedCount") or 0)
    final_output = len(deduped_payload)
    return {
        **dedup_stats,
        "rawFetched": raw_fetched,
        "canonicalDropped": canonical_dropped,
        "canonicalKept": canonical_kept,
        "dedupMerged": dedup_merged,
        "finalOutput": final_output,
        "rawFetchedCount": canonical_count,
        "uniqueOutputCount": len(deduped_payload),
        "sourceBundleCollisions": sum(1 for row in deduped_payload if int(row.get("sourceBundleCount") or 0) > 1),
        "targetRoleCount": sum(1 for row in deduped_payload if norm_text(row.get("profession")) in TARGET_PROFESSIONS),
        "netherlandsCount": sum(1 for row in deduped_payload if clean_text(row.get("country")).upper() == "NL"),
        "remoteCount": sum(1 for row in deduped_payload if norm_text(row.get("workType")) == "remote"),
        "targetRoleNetherlandsCount": sum(
            1
            for row in deduped_payload
            if norm_text(row.get("profession")) in TARGET_PROFESSIONS and clean_text(row.get("country")).upper() == "NL"
        ),
        "targetRoleRemoteCount": sum(
            1
            for row in deduped_payload
            if norm_text(row.get("profession")) in TARGET_PROFESSIONS and norm_text(row.get("workType")) == "remote"
        ),
        "preservedPreviousOutput": preserved_previous,
        "sourceCount": len(source_reports),
        "successfulSources": sum(1 for row in source_reports if row["status"] == "ok"),
        "failedSources": sum(1 for row in source_reports if row["status"] == "error"),
        "excludedSources": sum(1 for row in source_reports if row["status"] == "excluded"),
        "cacheSkippedCount": sum(1 for row in cache_rows if norm_text(row.get("status")) == "excluded" and clean_text(row.get("cacheDecision")) in {"skip_fresh", "cooldown_skip"}),
        "revalidatedCount": sum(1 for row in cache_rows if clean_text(row.get("cacheDecision")) == "revalidate_only"),
        "notModifiedCount": sum(1 for row in cache_rows if clean_text(row.get("cacheDecisionReason")) == "not_modified_304"),
        "listingOnlyCount": sum(1 for row in cache_rows if clean_text(row.get("cacheDecision")) == "listing_only"),
        "detailSkippedByListingFingerprintCount": sum(1 for row in cache_rows if bool(row.get("detailSkippedByListingFingerprint"))),
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
    }


def build_browser_fallback_queue(
    source_reports: Sequence[dict[str, Any]],
    *,
    generated_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen = set()
    for report in source_reports:
        details = report.get("details") if isinstance(report, dict) else None
        if not isinstance(details, list):
            continue
        for item in details:
            if not isinstance(item, dict):
                continue
            classification = norm_text(item.get("classification"))
            recommend = bool(item.get("browserFallbackRecommended"))
            if not recommend or classification not in common_config.STATIC_CLASSIFICATIONS_FOR_BROWSER_QUEUE:
                continue
            source_id = clean_text(item.get("sourceId"))
            name = clean_text(item.get("name"))
            studio = clean_text(item.get("studio"))
            pages = item.get("pages") if isinstance(item.get("pages"), list) else []
            clean_pages = [clean_text(page) for page in pages if clean_text(page)]
            canonical = pick_canonical_listing_url(clean_pages) if clean_pages else None
            if not canonical:
                continue
            # Do not add job_provider domains (e.g. Remedy/Jobylon) to the queue; they stay on static/specialized path.
            profile = domain_profile_for_url(canonical)
            if clean_text(profile.get("job_provider")):
                continue
            dedupe_key = hashlib.sha1("|".join(["scrapy_static", source_id or name, canonical]).encode("utf-8")).hexdigest()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            rows.append(
                {
                    "dedupeKey": dedupe_key,
                    "adapter": "scrapy_static",
                    "sourceId": source_id,
                    "name": name,
                    "studio": studio,
                    "page": canonical,
                    "classification": classification,
                    "reason": clean_text(item.get("error")) or classification,
                    "generatedAt": clean_text(generated_at),
                }
            )
    rows.sort(key=lambda row: (clean_text(row.get("studio")), clean_text(row.get("name")), clean_text(row.get("page"))))
    return rows



__all__ = [
    normalize_fetch_report_payload,
    normalize_runtime_payload,
    normalize_source_report_row,
]
