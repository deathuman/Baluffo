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
    return {
        "name": clean_text(row.get("name")),
        "adapter": clean_text(row.get("adapter")),
        "status": norm_text(row.get("status")) or "error",
        "keptCount": _clamped_int(row.get("keptCount"), 0, 0),
        "fetchedCount": _clamped_int(row.get("fetchedCount"), 0, 0),
        "durationMs": _clamped_int(row.get("durationMs"), 0, 0),
        "failureBucket": clean_text(row.get("failureBucket")),
        "classification": clean_text(row.get("classification")),
        "zeroKeptClassification": clean_text(row.get("zeroKeptClassification")),
        "browserFallbackRecommended": bool(row.get("browserFallbackRecommended")),
        "error": clean_text(row.get("error")),
        "exclusionReason": clean_text(row.get("exclusionReason")),
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
        "zeroKeptSources",
        "zeroKeptNeedsReviewSources",
        "browserFallbackRecommendedSources",
    ):
        normalized[key] = _clamped_int(src.get(key), normalized[key], 0)
    for key in (
        "sourcesNeedingAttention",
        "zeroKeptNeedsReview",
        "browserFallbackRecommended",
        "slowestSources",
        "topProductiveSources",
    ):
        payload_rows = json_object_rows(src.get(key))
        normalized[key] = [_source_health_row(row) for row in payload_rows[:_TRIAGE_ROW_LIMIT]]
    for key in ("topFailureBuckets", "topClassifications"):
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
