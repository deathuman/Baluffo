"""Derived source-health triage contract for jobs fetch reports."""

from __future__ import annotations

from collections import Counter
from typing import Any

from src.jobs.common.numbers import _clamped_int
from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import as_json_object, json_object_rows

_LEGIT_EMPTY_ZERO_CLASSIFICATIONS = {"legit_empty", "no_openings"}
_LEGIT_EMPTY_FAILURE_BUCKETS = {"no_openings"}
_LEGIT_EMPTY_CLASSIFICATIONS = {
    "empty_confirmed",
    "no_openings",
    "ok_no_jobs",
    "gameprog_no_current_openings",
}
_TRIAGE_ROW_LIMIT = 10


def _source_health_row(row: dict[str, Any]) -> dict[str, Any]:
    status = norm_text(row.get("status")) or "error"
    kept_count = _clamped_int(row.get("keptCount"), 0, 0)
    failure_count = _clamped_int(
        row.get("failureCount"), _clamped_int(row.get("consecutiveFailures"), 0, 0), 0
    )
    zero_job_streak = _clamped_int(
        row.get("zeroJobStreak"), _clamped_int(row.get("consecutiveZeroKept"), 0, 0), 0
    )
    last_success = clean_text(row.get("lastSuccessfulFetchAt")) or clean_text(
        row.get("lastSuccessAt")
    )
    last_seen = (
        clean_text(row.get("lastSeenInFetchAt"))
        or clean_text(row.get("lastCheckedAt"))
        or clean_text(row.get("lastRunAt"))
    )
    last_jobs_kept = _clamped_int(
        row.get("lastJobsKept"), _clamped_int(row.get("lastKeptCount"), 0, 0), 0
    )
    health_score = _clamped_int(row.get("healthScore"), 100, 0)
    if status == "excluded":
        health = "unknown"
        reason = "excluded"
    elif status == "error" or failure_count > 0:
        health = "broken"
        reason = "latest fetch failed"
    elif zero_job_streak >= 3:
        health = "broken"
        reason = "repeated zero-job fetches"
    elif kept_count > 0 or last_jobs_kept > 0:
        health = "healthy"
        reason = "last fetch kept jobs"
    elif status == "ok" or zero_job_streak > 0 or last_success or last_seen:
        health = "warning"
        reason = "latest fetch kept no jobs"
    else:
        health = "unknown"
        reason = "no fetch history"
    return {
        "name": clean_text(row.get("name")),
        "adapter": clean_text(row.get("adapter")),
        "status": status,
        "keptCount": kept_count,
        "fetchedCount": _clamped_int(row.get("fetchedCount"), 0, 0),
        "durationMs": _clamped_int(row.get("durationMs"), 0, 0),
        "lastStatus": clean_text(row.get("lastStatus")),
        "lastRunAt": clean_text(row.get("lastRunAt")),
        "lastCheckedAt": clean_text(row.get("lastCheckedAt")),
        "lastSuccessAt": last_success,
        "lastSuccessfulFetchAt": last_success,
        "lastSeenInFetchAt": last_seen,
        "lastKeptCount": _clamped_int(
            row.get("lastKeptCount"), _clamped_int(row.get("keptCount"), 0, 0), 0
        ),
        "lastJobsKept": last_jobs_kept,
        "failureCount": failure_count,
        "consecutiveFailures": _clamped_int(row.get("consecutiveFailures"), 0, 0),
        "zeroJobStreak": zero_job_streak,
        "consecutiveZeroKept": _clamped_int(row.get("consecutiveZeroKept"), 0, 0),
        "healthScore": health_score,
        "health": norm_text(row.get("health")) or health,
        "healthReason": clean_text(row.get("healthReason")) or reason,
        "failureBucket": clean_text(row.get("failureBucket")),
        "classification": clean_text(row.get("classification")),
        "zeroKeptClassification": clean_text(row.get("zeroKeptClassification")),
        "browserFallbackRecommended": bool(row.get("browserFallbackRecommended")),
        "error": clean_text(row.get("error")),
        "exclusionReason": clean_text(row.get("exclusionReason")),
        "coveredByProviderSourceId": clean_text(row.get("coveredByProviderSourceId")),
        "coveredByProviderAdapter": clean_text(row.get("coveredByProviderAdapter")),
        "providerCoverageStatus": clean_text(row.get("providerCoverageStatus")),
        "providerCoverageConsecutiveSuccesses": _clamped_int(
            row.get("providerCoverageConsecutiveSuccesses"), 0, 0
        ),
        "providerCoverageLatestKeptCount": _clamped_int(
            row.get("providerCoverageLatestKeptCount"), 0, 0
        ),
        "migrationSourceIdentity": clean_text(row.get("migrationSourceIdentity")),
    }


def _breakdown_rows(counter: Counter[str], examples: dict[str, list[str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, count in counter.most_common(_TRIAGE_ROW_LIMIT):
        rows.append({"key": key, "count": int(count), "examples": list(examples.get(key) or [])})
    return rows


def _push_example(examples: dict[str, list[str]], key: str, name: str) -> None:
    if not key or not name:
        return
    bucket_examples = examples.setdefault(key, [])
    if name not in bucket_examples and len(bucket_examples) < 4:
        bucket_examples.append(name)


def _zero_kept_needs_review(row: dict[str, Any]) -> bool:
    if _clamped_int(row.get("keptCount"), 0, 0) != 0:
        return False
    if norm_text(row.get("status")) == "excluded":
        return False
    if norm_text(row.get("zeroKeptClassification")) in _LEGIT_EMPTY_ZERO_CLASSIFICATIONS:
        return False
    if norm_text(row.get("failureBucket")) in _LEGIT_EMPTY_FAILURE_BUCKETS:
        return False
    if norm_text(row.get("classification")) in _LEGIT_EMPTY_CLASSIFICATIONS:
        return False
    return True


def derive_source_health(source_rows: Any) -> dict[str, Any]:
    rows = [_source_health_row(row) for row in json_object_rows(source_rows)]
    failed_rows = [row for row in rows if row["status"] == "error"]
    excluded_rows = [row for row in rows if row["status"] == "excluded"]
    dynamic_redundant_rows = [
        row for row in excluded_rows if row["exclusionReason"] == "dynamic_redundant_provider"
    ]
    zero_kept_rows = [row for row in rows if row["keptCount"] == 0 and row["status"] != "excluded"]
    zero_review_rows = [row for row in zero_kept_rows if _zero_kept_needs_review(row)]
    browser_rows = [row for row in rows if row["browserFallbackRecommended"]]

    failure_counts: Counter[str] = Counter()
    failure_examples: dict[str, list[str]] = {}
    classification_counts: Counter[str] = Counter()
    classification_examples: dict[str, list[str]] = {}
    attention_rows: list[dict[str, Any]] = []

    for row in rows:
        name = clean_text(row.get("name"))
        failure_bucket = clean_text(row.get("failureBucket"))
        classification = clean_text(row.get("classification"))
        if failure_bucket:
            failure_counts[failure_bucket] += 1
            _push_example(failure_examples, failure_bucket, name)
        if classification:
            classification_counts[classification] += 1
            _push_example(classification_examples, classification, name)
        if (
            row["status"] == "error"
            or row["browserFallbackRecommended"]
            or _zero_kept_needs_review(row)
        ):
            attention_rows.append(row)

    attention_rows.sort(
        key=lambda row: (
            row["status"] != "error",
            not bool(row["browserFallbackRecommended"]),
            not _zero_kept_needs_review(row),
            -int(row["durationMs"]),
            clean_text(row.get("name")),
        )
    )

    return {
        "totalSources": len(rows),
        "okSources": sum(1 for row in rows if row["status"] == "ok"),
        "failedSources": len(failed_rows),
        "excludedSources": len(excluded_rows),
        "skippedSources": sum(1 for row in excluded_rows if clean_text(row.get("exclusionReason"))),
        "dynamicRedundantStaticSources": len(dynamic_redundant_rows),
        "zeroKeptSources": len(zero_kept_rows),
        "zeroKeptNeedsReviewSources": len(zero_review_rows),
        "browserFallbackRecommendedSources": len(browser_rows),
        "sourcesNeedingAttention": attention_rows[:_TRIAGE_ROW_LIMIT],
        "zeroKeptNeedsReview": sorted(
            zero_review_rows,
            key=lambda row: (-int(row["durationMs"]), clean_text(row.get("name"))),
        )[:_TRIAGE_ROW_LIMIT],
        "browserFallbackRecommended": sorted(
            browser_rows,
            key=lambda row: (-int(row["durationMs"]), clean_text(row.get("name"))),
        )[:_TRIAGE_ROW_LIMIT],
        "dynamicRedundantStatic": sorted(
            dynamic_redundant_rows,
            key=lambda row: clean_text(row.get("name")),
        )[:_TRIAGE_ROW_LIMIT],
        "slowestSources": sorted(
            [row for row in rows if int(row["durationMs"]) > 0],
            key=lambda row: (-int(row["durationMs"]), clean_text(row.get("name"))),
        )[:_TRIAGE_ROW_LIMIT],
        "topProductiveSources": sorted(
            [row for row in rows if int(row["keptCount"]) > 0],
            key=lambda row: (
                -int(row["keptCount"]),
                -int(row["durationMs"]),
                clean_text(row.get("name")),
            ),
        )[:_TRIAGE_ROW_LIMIT],
        "topFailureBuckets": _breakdown_rows(failure_counts, failure_examples),
        "topClassifications": _breakdown_rows(classification_counts, classification_examples),
    }


def normalize_source_health_payload(payload: Any, source_rows: Any) -> dict[str, Any]:
    derived = derive_source_health(source_rows)
    src = as_json_object(payload)
    if not src:
        return derived
    normalized = dict(derived)
    for key in (
        "totalSources",
        "okSources",
        "failedSources",
        "excludedSources",
        "skippedSources",
        "dynamicRedundantStaticSources",
        "zeroKeptSources",
        "zeroKeptNeedsReviewSources",
        "browserFallbackRecommendedSources",
    ):
        normalized[key] = _clamped_int(src.get(key), normalized[key], 0)
    for key in (
        "sourcesNeedingAttention",
        "zeroKeptNeedsReview",
        "browserFallbackRecommended",
        "dynamicRedundantStatic",
        "slowestSources",
        "topProductiveSources",
    ):
        if key not in src:
            continue
        payload_rows = json_object_rows(src.get(key))
        normalized[key] = [_source_health_row(row) for row in payload_rows[:_TRIAGE_ROW_LIMIT]]
    for key in ("topFailureBuckets", "topClassifications"):
        if key not in src:
            continue
        payload_rows = json_object_rows(src.get(key))
        normalized[key] = [
            {
                "key": clean_text(row.get("key")),
                "count": _clamped_int(row.get("count"), 0, 0),
                "examples": [
                    clean_text(item) for item in (row.get("examples") or []) if clean_text(item)
                ]
                if isinstance(row.get("examples"), list)
                else [],
            }
            for row in payload_rows[:_TRIAGE_ROW_LIMIT]
            if clean_text(row.get("key"))
        ]
    return normalized
