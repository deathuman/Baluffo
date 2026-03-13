"""Reporting helpers for jobs pipeline output."""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Sequence

from scripts.jobs import common
from scripts.jobs.models import CanonicalJob

SCHEMA_VERSION = common.SCHEMA_VERSION
TARGET_PROFESSIONS = common.TARGET_PROFESSIONS
DEFAULT_FETCH_STRATEGY = common.DEFAULT_FETCH_STRATEGY
DEFAULT_ADAPTER_HTTP_CONCURRENCY = common.DEFAULT_ADAPTER_HTTP_CONCURRENCY
DEFAULT_STATIC_DETAIL_CONCURRENCY = common.DEFAULT_STATIC_DETAIL_CONCURRENCY
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = common.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
DEFAULT_HOT_SOURCE_CADENCE_MINUTES = common.DEFAULT_HOT_SOURCE_CADENCE_MINUTES
DEFAULT_COLD_SOURCE_CADENCE_MINUTES = common.DEFAULT_COLD_SOURCE_CADENCE_MINUTES
DEFAULT_SOCIAL_LOOKBACK_MINUTES = common.DEFAULT_SOCIAL_LOOKBACK_MINUTES
DEFAULT_SOCIAL_MIN_CONFIDENCE = common.DEFAULT_SOCIAL_MIN_CONFIDENCE
DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE = common.DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE
SOURCE_REPORT_META = common.SOURCE_REPORT_META

clean_text = common.clean_text
norm_text = common.norm_text
_clamped_int = common._clamped_int


def format_source_error(source_name: str, error: Any) -> str:
    message = clean_text(str(error))
    prefix = f"{clean_text(source_name)}:"
    if not message:
        return "unknown error"
    if message.lower().startswith(prefix.lower()):
        return message
    return f"{source_name}: {message}"


def build_pipeline_summary(
    dedup_stats: Dict[str, int],
    deduped_rows: Sequence[CanonicalJob],
    source_reports: Sequence[Dict[str, Any]],
    canonical_count: int,
    preserved_previous: bool,
    active_source_count: int,
    pending_source_count: int,
    newly_approved_since_last_run: int,
    *,
    json_bytes: int,
    csv_bytes: int,
    light_json_bytes: int,
    lifecycle_counts_map: Dict[str, int] | None = None,
) -> Dict[str, Any]:
    deduped_payload = [row.to_dict() if isinstance(row, CanonicalJob) else dict(row) for row in deduped_rows]
    lifecycle = lifecycle_counts_map or {}
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
    source_reports: Sequence[Dict[str, Any]],
    *,
    generated_at: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
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
            if not recommend or classification not in {"fetch_ok_extract_zero", "blocked_or_challenge", "timeout"}:
                continue
            source_id = clean_text(item.get("sourceId"))
            name = clean_text(item.get("name"))
            studio = clean_text(item.get("studio"))
            pages = item.get("pages") if isinstance(item.get("pages"), list) else []
            clean_pages = [clean_text(page) for page in pages if clean_text(page)] or [""]
            for page in clean_pages:
                dedupe_key = hashlib.sha1("|".join(["scrapy_static", source_id or name, page]).encode("utf-8")).hexdigest()
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
                        "page": page,
                        "classification": classification,
                        "reason": clean_text(item.get("error")) or classification,
                        "generatedAt": clean_text(generated_at),
                    }
                )
    rows.sort(key=lambda row: (clean_text(row.get("studio")), clean_text(row.get("name")), clean_text(row.get("page"))))
    return rows


normalize_runtime_payload = common.normalize_runtime_payload
normalize_source_report_row = common.normalize_source_report_row
normalize_fetch_report_payload = common.normalize_fetch_report_payload
