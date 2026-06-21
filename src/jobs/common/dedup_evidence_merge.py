"""Merge example and non-primary merge gate helpers for dedup evidence.

Extracted from reporting_dedup_evidence.py as part of the dedup evidence split.

AI boundary owns: dedup merge examples, non-primary merge gates, and evidence row helpers.
AI boundary implement in: this file for merge evidence helpers; core identity and dedup decisions stay in src.jobs.dedup.
AI boundary search before contracts: dedup evidence bundle, reporting_dedup_evidence, and merge evidence tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused dedup merge evidence tests.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.jobs.common.contracts_dedup_evidence import DedupMergeExampleRow
from src.jobs.text_utils import clean_text, normalize_url
from src.shared.json_shapes import json_object_rows

REVIEW_QUEUE_CAUSE_KEYS = (
    "category_or_department_bucket",
    "open_application_family",
    "listing_page_bundle",
    "spreadsheet_role_bucket_needs_review",
    "google_sheets_role_bucket_needs_review",
    "parser_or_directory_text_pollution",
    "non_provider_url_identity_needs_review",
    "provider_static_disagreement",
    "likely_legitimate_multi_role_family",
    "unknown",
)


def _merge_reason_counts(dedup_stats: Mapping[str, Any]) -> dict[str, int]:
    primary = max(0, int(dedup_stats.get("mergedByPrimaryUrl") or 0))
    secondary = max(0, int(dedup_stats.get("mergedBySecondaryKey") or 0))
    social = max(0, int(dedup_stats.get("mergedBySocialKey") or 0))
    known_mirror_pair = max(0, int(dedup_stats.get("mergedByKnownMirrorPair") or 0))
    sparse_explicit = dedup_stats.get("mergedBySparseIdentity")
    total = max(0, int(dedup_stats.get("mergedCount") or 0))
    sparse = (
        max(0, int(sparse_explicit or 0))
        if sparse_explicit is not None
        else max(0, total - primary - secondary - social - known_mirror_pair)
    )
    known = primary + secondary + social + known_mirror_pair + sparse
    return {
        "primaryUrl": primary,
        "secondaryKey": secondary,
        "socialKey": social,
        "knownMirrorPair": known_mirror_pair,
        "sparseIdentity": sparse,
        "unknown": max(0, total - known),
    }


def _current_run_merge_examples(dedup_stats: Mapping[str, Any]) -> list[DedupMergeExampleRow]:
    examples: list[DedupMergeExampleRow] = []
    for row in json_object_rows(dedup_stats.get("collisionSamples")):
        examples.append(_current_run_merge_example(row))
    return examples


def _current_run_merge_example(row: Mapping[str, Any]) -> DedupMergeExampleRow:
    merge_reason = clean_text(row.get("reason")) or "unknown"
    gate_tier = clean_text(row.get("gateTier"))
    gate_tier_reason = clean_text(row.get("gateTierReason"))
    blocks_lifecycle = (
        gate_tier == "blocking"
        if gate_tier
        else merge_reason not in {"primary_url", "known_mirror_pair"}
    )
    example = {
        "mergeReason": merge_reason,
        "existingDedupKey": clean_text(row.get("existingDedupKey")),
        "incomingSource": clean_text(row.get("incomingSource")),
        "title": clean_text(row.get("incomingTitle")),
        "company": clean_text(row.get("incomingCompany")),
        "incomingJobLink": normalize_url(row.get("incomingJobLink")),
        "bundleEvidenceOrigin": "current_run",
        "blocksLifecycle": blocks_lifecycle,
        "nonBlockingReason": "" if blocks_lifecycle else gate_tier_reason,
        "recommendedReviewAction": "review_current_run_merge" if blocks_lifecycle else "monitor",
        "suspectedCause": (
            "current_run_non_primary_merge"
            if blocks_lifecycle
            else gate_tier_reason or "known_mirror_pair"
        ),
    }
    if merge_reason == "known_mirror_pair":
        example["nonBlockingReason"] = "known_gracklehq_gamesjobsdirect_mirror_pair"
        example["suspectedCause"] = "known_mirror_pair"
    return example


def _current_run_merge_examples_by_reason(
    dedup_stats: Mapping[str, Any], *, limit_per_reason: int = 5
) -> dict[str, list[DedupMergeExampleRow]]:
    by_reason = {
        "secondaryKey": [],
        "sparseIdentity": [],
        "socialKey": [],
        "knownMirrorPair": [],
        "primaryUrl": [],
        "unknown": [],
    }
    reason_keys = {
        "secondary_key": "secondaryKey",
        "sparse_identity": "sparseIdentity",
        "social_key": "socialKey",
        "known_mirror_pair": "knownMirrorPair",
        "primary_url": "primaryUrl",
    }
    samples_by_reason = dedup_stats.get("collisionSamplesByReason")
    if isinstance(samples_by_reason, Mapping):
        for raw_reason, raw_rows in samples_by_reason.items():
            reason = clean_text(raw_reason)
            key = reason_keys.get(reason, "unknown")
            for row in json_object_rows(raw_rows):
                if len(by_reason[key]) >= max(0, int(limit_per_reason)):
                    break
                by_reason[key].append(_current_run_merge_example(row))
        return by_reason
    for example in _current_run_merge_examples(dedup_stats):
        reason = clean_text(example.get("mergeReason"))
        key = reason_keys.get(reason, "unknown")
        if len(by_reason[key]) < max(0, int(limit_per_reason)):
            by_reason[key].append(example)
    return by_reason


def _empty_blocking_merge_examples_by_reason() -> dict[str, list[DedupMergeExampleRow]]:
    return {
        "secondaryKey": [],
        "sparseIdentity": [],
        "socialKey": [],
        "unknown": [],
    }


def _blocking_merge_reason_key(raw_reason: Any) -> str:
    reason_keys = {
        "secondary_key": "secondaryKey",
        "sparse_identity": "sparseIdentity",
        "social_key": "socialKey",
    }
    return reason_keys.get(clean_text(raw_reason), "unknown")


def _append_blocking_merge_example(
    bucket: list[DedupMergeExampleRow], row: Mapping[str, Any], *, limit: int
) -> None:
    if len(bucket) >= max(0, int(limit)):
        return
    example = _current_run_merge_example(row)
    if example.get("blocksLifecycle") is True:
        bucket.append(example)


def _blocking_merge_examples_from_sample_map(
    samples_by_reason: Mapping[str, Any], *, limit_per_reason: int
) -> dict[str, list[DedupMergeExampleRow]]:
    by_reason = _empty_blocking_merge_examples_by_reason()
    for raw_reason, raw_rows in samples_by_reason.items():
        key = _blocking_merge_reason_key(raw_reason)
        for row in json_object_rows(raw_rows):
            _append_blocking_merge_example(by_reason[key], row, limit=limit_per_reason)
    return by_reason


def _blocking_merge_examples_from_legacy_samples(
    dedup_stats: Mapping[str, Any], *, limit_per_reason: int
) -> dict[str, list[DedupMergeExampleRow]]:
    by_reason = _empty_blocking_merge_examples_by_reason()
    rows_by_reason = _current_run_merge_examples_by_reason(
        dedup_stats, limit_per_reason=limit_per_reason
    )
    for reason, rows in rows_by_reason.items():
        if reason not in by_reason:
            continue
        for row in rows:
            if row.get("blocksLifecycle") is True:
                by_reason[reason].append(row)
    return by_reason


def _current_run_blocking_merge_examples_by_reason(
    dedup_stats: Mapping[str, Any], *, limit_per_reason: int = 5
) -> dict[str, list[DedupMergeExampleRow]]:
    samples_by_reason = dedup_stats.get("currentRunBlockingMergeSamplesByReason")
    if isinstance(samples_by_reason, Mapping):
        return _blocking_merge_examples_from_sample_map(
            samples_by_reason, limit_per_reason=limit_per_reason
        )
    return _blocking_merge_examples_from_legacy_samples(
        dedup_stats, limit_per_reason=limit_per_reason
    )


def _current_run_non_primary_merge_counts(
    merge_reason_counts: Mapping[str, Any],
    *,
    blocking_reason_counts: Mapping[str, Any] | None = None,
    monitor_reason_counts: Mapping[str, Any] | None = None,
) -> dict[str, int]:
    if blocking_reason_counts is not None or monitor_reason_counts is not None:
        blocking_counts = dict(blocking_reason_counts or {})
        monitor_counts = dict(monitor_reason_counts or {})
        secondary = max(0, int(blocking_counts.get("secondaryKey") or 0))
        sparse = max(0, int(blocking_counts.get("sparseIdentity") or 0))
        social = max(0, int(blocking_counts.get("socialKey") or 0))
        unknown = max(0, int(blocking_counts.get("unknown") or 0))
        monitor_secondary = max(0, int(monitor_counts.get("secondaryKey") or 0))
        monitor_sparse = max(0, int(monitor_counts.get("sparseIdentity") or 0))
        monitor_social = max(0, int(monitor_counts.get("socialKey") or 0))
        monitor_unknown = max(0, int(monitor_counts.get("unknown") or 0))
        known_mirror_pair = max(0, int(merge_reason_counts.get("knownMirrorPair") or 0))
        return {
            "secondaryKey": secondary,
            "sparseIdentity": sparse,
            "socialKey": social,
            "unknown": unknown,
            "knownMirrorPair": known_mirror_pair,
            "blocking": secondary + sparse + social + unknown,
            "monitor": monitor_secondary + monitor_sparse + monitor_social + monitor_unknown,
            "monitorSecondaryKey": monitor_secondary,
            "monitorSparseIdentity": monitor_sparse,
            "monitorSocialKey": monitor_social,
            "monitorUnknown": monitor_unknown,
            "nonBlockingKnownMirrorPair": known_mirror_pair,
        }
    secondary = max(0, int(merge_reason_counts.get("secondaryKey") or 0))
    sparse = max(0, int(merge_reason_counts.get("sparseIdentity") or 0))
    social = max(0, int(merge_reason_counts.get("socialKey") or 0))
    unknown = max(0, int(merge_reason_counts.get("unknown") or 0))
    known_mirror_pair = max(0, int(merge_reason_counts.get("knownMirrorPair") or 0))
    return {
        "secondaryKey": secondary,
        "sparseIdentity": sparse,
        "socialKey": social,
        "unknown": unknown,
        "knownMirrorPair": known_mirror_pair,
        "blocking": secondary + sparse + social + unknown,
        "nonBlockingKnownMirrorPair": known_mirror_pair,
    }


def _nonzero_counts(counts: Mapping[str, Any]) -> dict[str, int]:
    return {
        str(key): int(value)
        for key, value in counts.items()
        if isinstance(value, int | float) and int(value) > 0
    }


def _mapping_value(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    return value if isinstance(value, Mapping) else {}


def _review_cause_counts_by_key(counts: Mapping[str, Any]) -> dict[str, int]:
    return {key: int(counts.get(key, 0)) for key in REVIEW_QUEUE_CAUSE_KEYS}
