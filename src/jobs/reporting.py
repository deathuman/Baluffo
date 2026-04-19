"""Reporting helpers for jobs pipeline output."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Sequence
from typing import Any

from src.jobs.adapters import community
from src.jobs.common.contracts import (
    normalize_fetch_report_payload,
    normalize_runtime_payload,
    normalize_source_report_row,
)
from src.jobs.models import CanonicalJob
from src.jobs.text_utils import clean_text, norm_text
from src.scrapers.domain_profiles import domain_profile_for_url, pick_canonical_listing_url

from .common import config as common_config

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
SOCIAL_EXPERIMENT_REVIEW_FILENAME = "social-experiment-review.json"
SOCIAL_EXPERIMENT_SAMPLE_SIZE = 50
OFFICIAL_BOARD_SOURCE_ADAPTERS = {
    "greenhouse",
    "teamtailor",
    "lever",
    "smartrecruiters",
    "workable",
    "recruitee",
    "pinpoint",
    "ashby",
    "bamboohr",
    "breezy",
    "jazzhr",
    "workday",
    "personio",
    "static",
}
OFFICIAL_BOARD_SOURCE_NAMES = {"epic_games_careers"}
UNKNOWN_STATIC_BREAKDOWN_SHAPES = (
    "no_jobs_extracted",
    "transport_network",
    "anti_bot_challenge",
    "other_static",
)
NEEDS_REVIEW_BREAKDOWN_SHAPES = (
    "blank_residue",
    "no_jobs_extracted",
    "transport_network",
    "anti_bot_challenge",
    "site_changed",
    "no_openings",
    "ambiguous_review",
    "other_static",
)


def _classify_blank_residue_shape(report: dict[str, Any]) -> str:
    failure_bucket = norm_text(report.get("failureBucket"))
    zero_kept = norm_text(report.get("zeroKeptClassification"))
    if not failure_bucket and not zero_kept:
        return "blank_residue"
    return _classify_needs_review_shape(report)


def format_source_error(source_name: str, error: Any) -> str:
    message = clean_text(str(error))
    prefix = f"{clean_text(source_name)}:"
    if not message:
        return "unknown error"
    if message.lower().startswith(prefix.lower()):
        return message
    return f"{source_name}: {message}"


def _canonical_sort_key(row: dict[str, Any]) -> tuple[str, int, str, str, str]:
    dedup_key = clean_text(row.get("dedupKey"))
    row_id = int(row.get("id") or 0)
    return (
        dedup_key or f"id:{row_id:020d}",
        row_id,
        clean_text(row.get("title")),
        clean_text(row.get("company")),
        clean_text(row.get("jobLink")),
    )


def _social_channel_for_source(source_name: Any) -> str:
    name = clean_text(source_name)
    if name == "social_reddit":
        return "reddit"
    if name == "social_mastodon":
        return "mastodon"
    if name == "social_x":
        return "x"
    return ""


def _source_bundle_items(row: dict[str, Any]) -> list[dict[str, Any]]:
    bundle = row.get("sourceBundle") if isinstance(row.get("sourceBundle"), list) else []
    return [item for item in bundle if isinstance(item, dict)]


def _row_origin_info(row: dict[str, Any]) -> tuple[list[str], bool]:
    channels: set[str] = set()
    official = False
    for item in _source_bundle_items(row):
        channel = _social_channel_for_source(item.get("source"))
        if channel in {"reddit", "mastodon"}:
            channels.add(channel)
        source_name = clean_text(item.get("source"))
        adapter = clean_text(item.get("adapter"))
        if source_name in OFFICIAL_BOARD_SOURCE_NAMES or adapter in OFFICIAL_BOARD_SOURCE_ADAPTERS:
            official = True
    return sorted(channels), official


def build_social_experiment_review_sample(
    deduped_rows: Sequence[CanonicalJob],
    *,
    sample_size: int = SOCIAL_EXPERIMENT_SAMPLE_SIZE,
) -> list[dict[str, Any]]:
    sample: list[dict[str, Any]] = []
    for row in deduped_rows:
        payload = row.to_dict() if isinstance(row, CanonicalJob) else dict(row)
        channels, official = _row_origin_info(payload)
        if not channels:
            continue
        sample.append(
            {
                "dedupKey": clean_text(payload.get("dedupKey")),
                "id": int(payload.get("id") or 0),
                "title": clean_text(payload.get("title")),
                "company": clean_text(payload.get("company")),
                "jobLink": clean_text(payload.get("jobLink")),
                "channels": channels,
                "officialBoardOrigin": bool(official),
                "sourceBundleCount": int(payload.get("sourceBundleCount") or 0),
                "reviewDecision": clean_text(payload.get("reviewDecision")),
                "reviewNotes": clean_text(payload.get("reviewNotes")),
            }
        )
    sample.sort(key=_canonical_sort_key)
    return sample[: max(0, int(sample_size or 0))]


def build_social_experiment_review_payload(
    review_rows: Sequence[dict[str, Any]],
    *,
    generated_at: str,
    pilot_window_start_at: str,
    pilot_window_end_at: str,
    review_artifact_path: str,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    reviewed_count = 0
    false_positive_count = 0
    for row in review_rows:
        if not isinstance(row, dict):
            continue
        decision = clean_text(row.get("reviewDecision"))
        reviewed = decision in {"true_positive", "false_positive"}
        if reviewed:
            reviewed_count += 1
        if decision == "false_positive":
            false_positive_count += 1
        rows.append(
            {
                "dedupKey": clean_text(row.get("dedupKey")),
                "id": int(row.get("id") or 0),
                "title": clean_text(row.get("title")),
                "company": clean_text(row.get("company")),
                "jobLink": clean_text(row.get("jobLink")),
                "channels": [
                    clean_text(item)
                    for item in (row.get("channels") or [])
                    if clean_text(item) in {"reddit", "mastodon", "x"}
                ],
                "officialBoardOrigin": bool(row.get("officialBoardOrigin")),
                "sourceBundleCount": int(row.get("sourceBundleCount") or 0),
                "reviewDecision": decision,
                "reviewNotes": clean_text(row.get("reviewNotes")),
            }
        )
    rows.sort(key=_canonical_sort_key)
    sample_size = len(rows) if reviewed_count > 0 else 0
    false_positive_rate = (
        float(false_positive_count) / float(reviewed_count) if reviewed_count > 0 else 0.0
    )
    return {
        "schemaVersion": 1,
        "generatedAt": clean_text(generated_at),
        "pilotWindowStartAt": clean_text(pilot_window_start_at),
        "pilotWindowEndAt": clean_text(pilot_window_end_at),
        "candidateCount": len(rows),
        "sampleSize": sample_size,
        "reviewedCount": reviewed_count,
        "falsePositiveCount": false_positive_count,
        "falsePositiveRate": false_positive_rate,
        "reviewArtifactPath": clean_text(review_artifact_path),
        "rows": rows,
    }


def _classify_unknown_static_shape(report: dict[str, Any]) -> str:
    error_lower = clean_text(report.get("error")).lower()
    failure_bucket = norm_text(report.get("failureBucket"))
    zero_kept = norm_text(report.get("zeroKeptClassification"))
    if failure_bucket == "no_openings" or zero_kept == "legit_empty":
        return "no_jobs_extracted"
    if failure_bucket == "js_required" or zero_kept == "broken_extraction":
        return "no_jobs_extracted"
    if "no jobs extracted from source pages" in error_lower:
        return "no_jobs_extracted"
    if any(
        marker in error_lower
        for marker in (
            "timeout",
            "timed out",
            "time_budget_exceeded",
            "network error",
            "fetch failed",
            "connection reset",
            "connection aborted",
            "name resolution",
            "temporary failure",
            "dns",
        )
    ):
        return "transport_network"
    if failure_bucket == "timeout":
        return "transport_network"
    if any(
        marker in error_lower
        for marker in (
            "429",
            "403",
            "blocked",
            "captcha",
            "challenge",
            "too many requests",
            "rate limit",
        )
    ):
        return "anti_bot_challenge"
    return "other_static"


def _classify_needs_review_shape(report: dict[str, Any]) -> str:
    error_lower = clean_text(report.get("error")).lower()
    failure_bucket = norm_text(report.get("failureBucket"))
    zero_kept = norm_text(report.get("zeroKeptClassification"))

    if not failure_bucket or not zero_kept:
        return "blank_residue"

    if failure_bucket == "no_openings" or zero_kept == "legit_empty":
        return "no_openings"
    if "no jobs extracted from source pages" in error_lower:
        return "no_jobs_extracted"
    if failure_bucket == "js_required" or zero_kept == "broken_extraction":
        return "no_jobs_extracted"
    if failure_bucket == "site_changed" or "site changed" in error_lower:
        return "site_changed"
    if failure_bucket == "timeout" or any(
        marker in error_lower
        for marker in (
            "timeout",
            "timed out",
            "time_budget_exceeded",
            "network error",
            "fetch failed",
            "connection reset",
            "connection aborted",
            "name resolution",
            "temporary failure",
            "dns",
        )
    ):
        return "transport_network"
    if failure_bucket == "anti_bot_or_challenge" or any(
        marker in error_lower
        for marker in (
            "429",
            "403",
            "blocked",
            "captcha",
            "challenge",
            "too many requests",
            "rate limit",
        )
    ):
        return "anti_bot_challenge"
    if failure_bucket == "needs_review" or zero_kept == "needs_review":
        return "ambiguous_review"
    return "other_static"


def build_unknown_static_breakdown(
    source_reports: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    by_shape: dict[str, dict[str, Any]] = {
        shape: {"count": 0, "totalDurationMs": 0, "examples": []}
        for shape in UNKNOWN_STATIC_BREAKDOWN_SHAPES
    }
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        if clean_text(report.get("adapter")) != "static":
            continue
        kept_count = int(report.get("keptCount") or 0)
        failure_bucket = norm_text(report.get("failureBucket"))
        if kept_count > 0 and failure_bucket not in {"unknown", ""}:
            continue
        shape = _classify_unknown_static_shape(report)
        duration_ms = max(0, int(report.get("durationMs") or 0))
        entry = {
            "name": clean_text(report.get("name")) or "unknown",
            "studio": clean_text(report.get("studio")) or "",
            "adapter": clean_text(report.get("adapter")) or "unknown",
            "durationMs": duration_ms,
            "shape": shape,
            "error": clean_text(report.get("error")),
            "keptCount": int(report.get("keptCount") or 0),
            "fetchedCount": int(report.get("fetchedCount") or 0),
            "failureBucket": clean_text(report.get("failureBucket")) or "unknown",
            "zeroKeptClassification": clean_text(report.get("zeroKeptClassification")) or "",
        }
        rows.append(entry)
        bucket = by_shape[shape]
        bucket["count"] += 1
        bucket["totalDurationMs"] += duration_ms
        if len(bucket["examples"]) < 3:
            bucket["examples"].append(entry["name"])

    top_by_wall_time = sorted(
        rows,
        key=lambda row: (
            -int(row.get("durationMs") or 0),
            clean_text(row.get("studio")),
            clean_text(row.get("name")),
        ),
    )[:20]
    by_frequency = sorted(
        (
            {
                "shape": shape,
                "count": values["count"],
                "totalDurationMs": values["totalDurationMs"],
                "examples": list(values["examples"]),
                "share": (float(values["count"]) / float(len(rows)) if rows else 0.0),
            }
            for shape, values in by_shape.items()
        ),
        key=lambda row: (
            -int(row.get("count") or 0),
            -int(row.get("totalDurationMs") or 0),
            row["shape"],
        ),
    )
    return {
        "byShape": by_shape,
        "topByWallTime": top_by_wall_time,
        "topByFrequency": by_frequency,
    }


def build_needs_review_breakdown(
    source_reports: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    by_shape: dict[str, dict[str, Any]] = {
        shape: {"count": 0, "totalDurationMs": 0, "examples": []}
        for shape in NEEDS_REVIEW_BREAKDOWN_SHAPES
    }
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        if clean_text(report.get("adapter")) != "static":
            continue
        kept_count = int(report.get("keptCount") or 0)
        failure_bucket = norm_text(report.get("failureBucket"))
        zero_kept = norm_text(report.get("zeroKeptClassification"))
        if kept_count > 0:
            continue
        if failure_bucket not in {"needs_review", ""} and zero_kept not in {"needs_review", ""}:
            continue
        shape = _classify_needs_review_shape(report)
        duration_ms = max(0, int(report.get("durationMs") or 0))
        entry = {
            "name": clean_text(report.get("name")) or "unknown",
            "studio": clean_text(report.get("studio")) or "",
            "adapter": clean_text(report.get("adapter")) or "unknown",
            "durationMs": duration_ms,
            "shape": shape,
            "error": clean_text(report.get("error")),
            "keptCount": int(report.get("keptCount") or 0),
            "fetchedCount": int(report.get("fetchedCount") or 0),
            "failureBucket": clean_text(report.get("failureBucket")) or "",
            "zeroKeptClassification": clean_text(report.get("zeroKeptClassification")) or "",
        }
        rows.append(entry)
        bucket = by_shape[shape]
        bucket["count"] += 1
        bucket["totalDurationMs"] += duration_ms
        if len(bucket["examples"]) < 3:
            bucket["examples"].append(entry["name"])

    top_by_wall_time = sorted(
        rows,
        key=lambda row: (
            -int(row.get("durationMs") or 0),
            clean_text(row.get("studio")),
            clean_text(row.get("name")),
        ),
    )[:20]
    by_frequency = sorted(
        (
            {
                "shape": shape,
                "count": values["count"],
                "totalDurationMs": values["totalDurationMs"],
                "examples": list(values["examples"]),
                "share": (float(values["count"]) / float(len(rows)) if rows else 0.0),
            }
            for shape, values in by_shape.items()
        ),
        key=lambda row: (
            -int(row.get("count") or 0),
            -int(row.get("totalDurationMs") or 0),
            row["shape"],
        ),
    )
    return {
        "byShape": by_shape,
        "topByWallTime": top_by_wall_time,
        "topByFrequency": by_frequency,
    }


def build_blank_residue_breakdown(
    source_reports: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    by_shape: dict[str, dict[str, Any]] = {
        shape: {"count": 0, "totalDurationMs": 0, "examples": []}
        for shape in NEEDS_REVIEW_BREAKDOWN_SHAPES
    }
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        if clean_text(report.get("adapter")) != "static":
            continue
        if clean_text(report.get("status")) == "excluded":
            continue
        kept_count = int(report.get("keptCount") or 0)
        if kept_count > 0:
            continue
        failure_bucket = norm_text(report.get("failureBucket"))
        zero_kept = norm_text(report.get("zeroKeptClassification"))
        if failure_bucket and zero_kept:
            continue
        shape = _classify_blank_residue_shape(report)
        duration_ms = max(0, int(report.get("durationMs") or 0))
        entry = {
            "name": clean_text(report.get("name")) or "unknown",
            "studio": clean_text(report.get("studio")) or "",
            "adapter": clean_text(report.get("adapter")) or "unknown",
            "durationMs": duration_ms,
            "shape": shape,
            "error": clean_text(report.get("error")),
            "keptCount": kept_count,
            "fetchedCount": int(report.get("fetchedCount") or 0),
            "failureBucket": clean_text(report.get("failureBucket")) or "",
            "zeroKeptClassification": clean_text(report.get("zeroKeptClassification")) or "",
        }
        rows.append(entry)
        bucket = by_shape[shape]
        bucket["count"] += 1
        bucket["totalDurationMs"] += duration_ms
        if len(bucket["examples"]) < 3:
            bucket["examples"].append(entry["name"])

    top_by_wall_time = sorted(
        rows,
        key=lambda row: (
            -int(row.get("durationMs") or 0),
            clean_text(row.get("studio")),
            clean_text(row.get("name")),
        ),
    )[:20]
    by_frequency = sorted(
        (
            {
                "shape": shape,
                "count": values["count"],
                "totalDurationMs": values["totalDurationMs"],
                "examples": list(values["examples"]),
                "share": (float(values["count"]) / float(len(rows)) if rows else 0.0),
            }
            for shape, values in by_shape.items()
        ),
        key=lambda row: (
            -int(row.get("count") or 0),
            -int(row.get("totalDurationMs") or 0),
            row["shape"],
        ),
    )
    return {
        "byShape": by_shape,
        "topByWallTime": top_by_wall_time,
        "topByFrequency": by_frequency,
    }


def summarize_social_experiment(
    source_reports: Sequence[dict[str, Any]],
    deduped_rows: Sequence[CanonicalJob],
    *,
    pilot_window_start_at: str,
    pilot_window_end_at: str,
    review_payload: dict[str, Any] | None = None,
    review_artifact_path: str = "",
) -> dict[str, Any]:
    social_rows = [
        row
        for row in source_reports
        if isinstance(row, dict)
        and clean_text(row.get("name")) in {"social_reddit", "social_mastodon"}
    ]
    by_channel: dict[str, dict[str, Any]] = {}
    social_unique_total = 0
    social_overlap_total = 0
    for channel, source_name in {"reddit": "social_reddit", "mastodon": "social_mastodon"}.items():
        channel_report = next(
            (row for row in social_rows if clean_text(row.get("name")) == source_name),
            {},
        )
        channel_kept = int(channel_report.get("keptCount") or 0)
        channel_low_conf = int(channel_report.get("lowConfidenceDropped") or 0)
        unique_count = 0
        overlap_count = 0
        for row in deduped_rows:
            payload = row.to_dict() if isinstance(row, CanonicalJob) else dict(row)
            channels, official = _row_origin_info(payload)
            if channel not in channels:
                continue
            if official:
                overlap_count += 1
            else:
                unique_count += 1
        duplicate_count = max(0, channel_kept - unique_count - overlap_count)
        duplicate_rate = (duplicate_count / channel_kept) if channel_kept > 0 else 0.0
        by_channel[channel] = {
            "keptCount": channel_kept,
            "uniqueKeptCount": unique_count,
            "officialBoardOverlapCount": overlap_count,
            "duplicateCount": duplicate_count,
            "duplicateRate": duplicate_rate,
            "lowConfidenceDropped": channel_low_conf,
        }
        social_unique_total += unique_count
        social_overlap_total += overlap_count

    kept_total = sum(int(row.get("keptCount") or 0) for row in social_rows)
    low_conf_total = sum(int(row.get("lowConfidenceDropped") or 0) for row in social_rows)
    duplicate_total = max(0, kept_total - social_unique_total - social_overlap_total)
    duplicate_rate_total = (duplicate_total / kept_total) if kept_total > 0 else 0.0
    review_payload = review_payload if isinstance(review_payload, dict) else {}
    reviewed_count = int(review_payload.get("reviewedCount") or 0)
    false_positive_count = int(review_payload.get("falsePositiveCount") or 0)
    false_positive_rate = float(review_payload.get("falsePositiveRate") or 0.0)
    candidate_count = int(review_payload.get("candidateCount") or 0)
    sample_size = candidate_count if reviewed_count > 0 else 0
    return {
        "pilotWindowStartAt": clean_text(pilot_window_start_at),
        "pilotWindowEndAt": clean_text(pilot_window_end_at),
        "scheduledRunCount": 1 if social_rows else 0,
        "keptCount": kept_total,
        "uniqueKeptCount": social_unique_total,
        "officialBoardOverlapCount": social_overlap_total,
        "duplicateCount": duplicate_total,
        "duplicateRate": duplicate_rate_total,
        "lowConfidenceDropped": low_conf_total,
        "sampleSize": sample_size,
        "reviewedCount": reviewed_count,
        "falsePositiveCount": false_positive_count,
        "falsePositiveRate": false_positive_rate if reviewed_count > 0 else 0.0,
        "reviewArtifactPath": clean_text(review_artifact_path),
        "channels": by_channel,
    }


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
        "successfulSources": sum(1 for row in source_count_rows if row["status"] == "ok"),
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
            recommend = bool(item.get("browserFallbackRecommended"))
            if not recommend:
                continue
            classification = norm_text(item.get("classification"))
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
            dedupe_key = hashlib.sha1(
                "|".join(["scrapy_static", source_id or name, canonical]).encode("utf-8")
            ).hexdigest()
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
    rows.sort(
        key=lambda row: (
            clean_text(row.get("studio")),
            clean_text(row.get("name")),
            clean_text(row.get("page")),
        )
    )
    return rows


def count_site_changed_diagnosed_sources(source_reports: Sequence[dict[str, Any]]) -> int:
    count = 0
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        if norm_text(report.get("failureBucket")) == "site_changed":
            count += 1
    return count


def _parser_regression_pages(report: dict[str, Any]) -> list[str]:
    pages: list[str] = []
    listing_url = clean_text(report.get("listingUrl"))
    if listing_url:
        pages.append(listing_url)
    top_pages = report.get("pages") if isinstance(report.get("pages"), list) else []
    pages.extend(clean_text(page) for page in top_pages if clean_text(page))
    details = report.get("details") if isinstance(report.get("details"), list) else []
    for item in details:
        if not isinstance(item, dict):
            continue
        item_pages = item.get("pages") if isinstance(item.get("pages"), list) else []
        pages.extend(clean_text(page) for page in item_pages if clean_text(page))
    provider_url = clean_text(report.get("providerUrl"))
    if provider_url:
        pages.append(provider_url)
    deduped: list[str] = []
    seen = set()
    for page in pages:
        if page and page not in seen:
            seen.add(page)
            deduped.append(page)
    return deduped


def count_site_changed_missing_old_url_sources(
    source_reports: Sequence[dict[str, Any]],
) -> int:
    count = 0
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        if norm_text(report.get("failureBucket")) != "site_changed":
            continue
        if not _parser_regression_pages(report):
            count += 1
    return count


def build_parser_regression_queue(
    source_reports: Sequence[dict[str, Any]],
    *,
    generated_at: str,
    resolve_redirect_url: Callable[[str], str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen = set()
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        source_name = (
            clean_text(report.get("name")) or clean_text(report.get("domain")) or "unknown"
        )
        source_id = clean_text(report.get("sourceId"))
        adapter = clean_text(report.get("adapter")) or "custom"
        if norm_text(report.get("failureBucket")) != "site_changed":
            continue
        clean_pages = _parser_regression_pages(report)
        old_url = pick_canonical_listing_url(clean_pages) if clean_pages else ""
        if not old_url:
            continue
        dedupe_key = hashlib.sha1(
            "|".join(["parser_regression", source_id or source_name, old_url]).encode("utf-8")
        ).hexdigest()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        current_url = ""
        if callable(resolve_redirect_url):
            try:
                resolved = clean_text(resolve_redirect_url(old_url))
            except Exception:  # noqa: BLE001
                resolved = ""
            if resolved and resolved != old_url:
                current_url = resolved
        listing_changed = bool(report.get("listingChanged")) or bool(
            report.get("listingFingerprintChanged")
        )
        last_status = clean_text(report.get("status")) or "error"
        row = {
            "dedupeKey": dedupe_key,
            "generatedAt": clean_text(generated_at),
            "source": clean_text(report.get("studio")) or source_name,
            "oldUrl": old_url,
            "lastStatus": last_status,
            "listingFingerprintChanged": bool(listing_changed),
            "classification": "site_changed",
            "adapter": adapter,
        }
        if current_url:
            row["currentUrl"] = current_url
        rows.append(row)
    rows.sort(
        key=lambda row: (
            0 if bool(row.get("listingFingerprintChanged")) else 1,
            0 if clean_text(row.get("lastStatus")) == "error" else 1,
            clean_text(row.get("source")),
            clean_text(row.get("oldUrl")),
        )
    )
    return rows


__all__ = [
    normalize_fetch_report_payload,
    normalize_runtime_payload,
    normalize_source_report_row,
    build_browser_fallback_queue,
    count_site_changed_diagnosed_sources,
    count_site_changed_missing_old_url_sources,
    build_parser_regression_queue,
]
