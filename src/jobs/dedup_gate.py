"""Merge gate tiers, reason accounting, and sample recording.

AI boundary owns: merge gate tier classification, merge reason counts, bounded
merge samples, and final merge-into-target application.
AI boundary implement in: this leaf for gate/accounting; targeting lives in
``dedup_targeting.py`` and the public entry point in ``dedup.py``.
"""

from __future__ import annotations

from typing import Any

from src.jobs.canonicalize import (
    clean_text,
    normalize_url,
)
from src.jobs.dedup_identity import (
    _is_provider_gracklehq_redirect_alias,
    _merge_source_class,
    _sparse_identity_key,
    dedup_secondary_key,
    fingerprint_url,
)
from src.jobs.dedup_record_merge import _merge_records_with_source_bundle_state
from src.jobs.dedup_state import _apply_location_state, _source_bundle_working_sample
from src.jobs.dedup_targeting import (
    _dedup_key,
    _index_row_keys,
    _index_smartrecruiters_title_location_alias_keys,
    _social_key,
)
from src.jobs.models import CanonicalJob


def _merge_reason_count_key(merge_reason: str) -> str:
    return {
        "primary_url": "primaryUrl",
        "secondary_key": "secondaryKey",
        "social_key": "socialKey",
        "known_mirror_pair": "knownMirrorPair",
        "sparse_identity": "sparseIdentity",
    }.get(clean_text(merge_reason), "unknown")


def _merge_gate_tier(
    *,
    merge_reason: str,
    existing: CanonicalJob,
    payload: dict[str, Any],
) -> tuple[str, str]:
    reason = clean_text(merge_reason)
    if reason == "primary_url":
        return "monitor", "primary_url"
    if reason == "known_mirror_pair":
        return "monitor", "known_gracklehq_gamesjobsdirect_mirror_pair"
    if reason == "social_key":
        return "blocking", "trusted_social_identity"
    existing_class = _merge_source_class(existing.to_dict())
    incoming_class = _merge_source_class(payload)
    if reason in {"secondary_key", "sparse_identity"} and _is_provider_gracklehq_redirect_alias(
        existing=existing,
        payload=payload,
    ):
        return "monitor", "provider_gracklehq_redirect_alias"
    if "provider" in {existing_class, incoming_class} or "social" in {
        existing_class,
        incoming_class,
    }:
        return "blocking", "trusted_identity"
    return "monitor", "weak_non_provider_identity"


def _increment_count(mapping: dict[str, int], key: str) -> None:
    mapping[key] = int(mapping.get(key) or 0) + 1


def _merge_reason_payload(counts: dict[str, int]) -> dict[str, int]:
    return {
        "secondaryKey": int(counts.get("secondaryKey") or 0),
        "sparseIdentity": int(counts.get("sparseIdentity") or 0),
        "socialKey": int(counts.get("socialKey") or 0),
        "unknown": int(counts.get("unknown") or 0),
    }


def _merge_gate_tier_payload(counts: dict[str, int]) -> dict[str, int]:
    return {
        "blocking": int(counts.get("blocking") or 0),
        "monitor": int(counts.get("monitor") or 0),
        "blockingTrustedIdentity": int(counts.get("blockingTrustedIdentity") or 0),
        "blockingTrustedSocialIdentity": int(counts.get("blockingTrustedSocialIdentity") or 0),
        "monitorWeakNonProviderIdentity": int(counts.get("monitorWeakNonProviderIdentity") or 0),
        "monitorPrimaryUrl": int(counts.get("monitorPrimaryUrl") or 0),
        "monitorKnownMirrorPair": int(counts.get("monitorKnownMirrorPair") or 0),
        "monitorProviderGracklehqRedirectAlias": int(
            counts.get("monitorProviderGracklehqRedirectAlias") or 0
        ),
    }


def _merge_gate_tier_reason_count_key(gate_tier: str, gate_tier_reason: str) -> str:
    return {
        ("blocking", "trusted_identity"): "blockingTrustedIdentity",
        ("blocking", "trusted_social_identity"): "blockingTrustedSocialIdentity",
        ("monitor", "weak_non_provider_identity"): "monitorWeakNonProviderIdentity",
        ("monitor", "primary_url"): "monitorPrimaryUrl",
        (
            "monitor",
            "known_gracklehq_gamesjobsdirect_mirror_pair",
        ): "monitorKnownMirrorPair",
        ("monitor", "provider_gracklehq_redirect_alias"): ("monitorProviderGracklehqRedirectAlias"),
    }.get((clean_text(gate_tier), clean_text(gate_tier_reason)), "")


def _record_merge_sample(
    *,
    merge_samples: list[dict[str, str]],
    merge_samples_by_reason: dict[str, list[dict[str, str]]],
    blocking_merge_samples_by_reason: dict[str, list[dict[str, str]]],
    merge_reason: str,
    existing: CanonicalJob,
    payload: dict[str, Any],
    gate_tier: str,
    gate_tier_reason: str,
    limit_per_reason: int = 5,
) -> None:
    sample = {
        "reason": merge_reason or "unknown",
        "existingDedupKey": clean_text(existing.dedupKey),
        "existingSource": clean_text(existing.source),
        "existingJobLink": normalize_url(existing.jobLink),
        "existingSourceJobId": clean_text(existing.sourceJobId),
        "incomingSource": clean_text(payload.get("source")),
        "incomingTitle": clean_text(payload.get("title")),
        "incomingCompany": clean_text(payload.get("company")),
        "incomingJobLink": normalize_url(payload.get("jobLink")),
        "incomingSourceJobId": clean_text(payload.get("sourceJobId")),
        "gateTier": clean_text(gate_tier),
        "gateTierReason": clean_text(gate_tier_reason),
    }
    bucket = clean_text(merge_reason) if clean_text(merge_reason) else "unknown"
    if bucket not in {
        "secondary_key",
        "sparse_identity",
        "social_key",
        "known_mirror_pair",
        "primary_url",
    }:
        bucket = "unknown"
    bucket_samples = merge_samples_by_reason.setdefault(bucket, [])
    if len(bucket_samples) < max(0, int(limit_per_reason)):
        bucket_samples.append(dict(sample))
    if gate_tier == "blocking" and bucket not in {"known_mirror_pair", "primary_url"}:
        blocking_bucket_samples = blocking_merge_samples_by_reason.setdefault(bucket, [])
        if len(blocking_bucket_samples) < max(0, int(limit_per_reason)):
            blocking_bucket_samples.append(dict(sample))
    if len(merge_samples) >= 10:
        return
    merge_samples.append(sample)


def _merge_into_target(
    *,
    target_idx: int,
    current: CanonicalJob,
    merged_rows: list[CanonicalJob],
    by_primary: dict[str, int],
    by_secondary: dict[str, int],
    by_sparse_identity: dict[str, int],
    by_social: dict[str, int],
    by_smartrecruiters_title_location_alias: dict[str, int],
    source_bundles_by_idx: list[list[dict[str, Any]]],
    source_bundle_keys_by_idx: list[set[str]],
    source_bundle_counts_by_idx: list[int],
    location_entries_by_idx: list[list[dict[str, Any]]],
    placeholder_location_entries_by_idx: list[list[dict[str, Any]]],
    location_keys_by_idx: list[set[str]],
) -> None:
    merged_payload, source_bundle_count = _merge_records_with_source_bundle_state(
        existing=merged_rows[target_idx],
        candidate=current,
        source_bundle=source_bundles_by_idx[target_idx],
        source_bundle_keys=source_bundle_keys_by_idx[target_idx],
        source_bundle_count=source_bundle_counts_by_idx[target_idx],
        location_entries=location_entries_by_idx[target_idx],
        placeholder_location_entries=placeholder_location_entries_by_idx[target_idx],
        location_keys=location_keys_by_idx[target_idx],
    )
    source_bundle_counts_by_idx[target_idx] = source_bundle_count
    merged = CanonicalJob.from_mapping(merged_payload)
    primary = fingerprint_url(merged_payload.get("jobLink"))
    secondary = dedup_secondary_key(merged)
    merged_social_key = _social_key(merged_payload)
    if primary or secondary or merged_social_key:
        merged_payload["dedupKey"] = _dedup_key(
            item=merged_payload,
            primary=primary,
            secondary=secondary,
            social_key=merged_social_key,
        )
    merged_rows[target_idx] = CanonicalJob.from_mapping(merged_payload)
    _index_row_keys(
        idx=target_idx,
        primary=primary,
        secondary=secondary,
        sparse_identity=_sparse_identity_key(merged_rows[target_idx]),
        social_key=merged_social_key,
        by_primary=by_primary,
        by_secondary=by_secondary,
        by_sparse_identity=by_sparse_identity,
        by_social=by_social,
    )
    _index_smartrecruiters_title_location_alias_keys(
        idx=target_idx,
        row=merged_rows[target_idx],
        by_smartrecruiters_title_location_alias=by_smartrecruiters_title_location_alias,
    )


def _attach_source_bundle_state(
    *,
    rows: list[CanonicalJob],
    source_bundles_by_idx: list[list[dict[str, Any]]],
    source_bundle_counts_by_idx: list[int],
    location_entries_by_idx: list[list[dict[str, Any]]],
    placeholder_location_entries_by_idx: list[list[dict[str, Any]]],
) -> list[CanonicalJob]:
    attached: list[CanonicalJob] = []
    for idx, row in enumerate(rows):
        payload = row.to_dict()
        if idx < len(source_bundles_by_idx):
            source_bundle = source_bundles_by_idx[idx]
            payload["sourceBundle"] = _source_bundle_working_sample(source_bundle, primary=payload)
            payload["sourceBundleCount"] = max(
                int(row.sourceBundleCount or 0),
                source_bundle_counts_by_idx[idx],
                len(source_bundle),
            )
        if idx < len(location_entries_by_idx):
            _apply_location_state(
                merged=payload,
                location_entries=location_entries_by_idx[idx],
                placeholder_location_entries=placeholder_location_entries_by_idx[idx],
            )
        attached.append(CanonicalJob.from_mapping(payload))
    return attached


def _merge_reason_counts(merge_reason: str) -> tuple[int, int, int, int, int]:
    return (
        1 if merge_reason == "primary_url" else 0,
        1 if merge_reason == "secondary_key" else 0,
        1 if merge_reason == "social_key" else 0,
        1 if merge_reason == "known_mirror_pair" else 0,
        1 if merge_reason == "sparse_identity" else 0,
    )
