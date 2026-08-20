"""Deduplication helpers for canonical jobs.

AI boundary owns: canonical job identity, duplicate grouping, and deduplication decisions.
AI boundary implement in: this file coordinates dedup leaves (identity, preferences, state,
record merge, targeting, gate); the public entry point and class live here.
AI boundary search before contracts: CanonicalJob contracts, reporting dedup evidence, and dedup tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused dedup tests.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from src.jobs.canonicalize import clean_text, norm_text
from src.jobs.common.datetime_utils import posted_ts
from src.jobs.dedup_gate import (
    _attach_source_bundle_state,
    _increment_count,
    _merge_gate_tier,
    _merge_gate_tier_payload,
    _merge_gate_tier_reason_count_key,
    _merge_into_target,
    _merge_reason_count_key,
    _merge_reason_counts,
    _merge_reason_payload,
    _record_merge_sample,
)
from src.jobs.dedup_identity import (
    SOCIAL_SOURCE_NAMES as SOCIAL_SOURCE_NAMES,
)
from src.jobs.dedup_identity import (
    _has_meaningful_locations,
    _sparse_identity_key,
    dedup_secondary_key,
)
from src.jobs.dedup_identity import (
    choose_base_record as choose_base_record,
)
from src.jobs.dedup_identity import (
    company_preference_score as company_preference_score,
)
from src.jobs.dedup_identity import (
    fingerprint_url as fingerprint_url,
)
from src.jobs.dedup_identity import (
    record_richness as record_richness,
)
from src.jobs.dedup_record_merge import _enrich_unknown_company_from_gracklehq_redirect
from src.jobs.dedup_targeting import (
    _append_new_dedup_row,
    _find_merge_target,
    _social_key,
)
from src.jobs.interfaces import JobProcessor
from src.jobs.models import CanonicalJob


def _sort_enrich_and_number(rows: list[CanonicalJob]) -> list[CanonicalJob]:
    rows.sort(
        key=lambda item: (
            int(item.focusScore or 0),
            posted_ts(item.postedAt),
            norm_text(item.title),
        ),
        reverse=True,
    )
    enriched_rows = _enrich_unknown_company_from_gracklehq_redirect(rows)
    return [
        CanonicalJob.from_mapping({**row.to_dict(), "id": idx})
        for idx, row in enumerate(enriched_rows, start=1)
    ]


def deduplicate_jobs(
    rows: Sequence[CanonicalJob | dict[str, Any]],
) -> tuple[list[CanonicalJob], dict[str, Any]]:
    merged_rows: list[CanonicalJob] = []
    by_primary: dict[str, int] = {}
    by_secondary: dict[str, int] = {}
    by_sparse_identity: dict[str, int] = {}
    by_social: dict[str, int] = {}
    by_smartrecruiters_title_location_alias: dict[str, int] = {}
    source_bundles_by_idx: list[list[dict[str, Any]]] = []
    source_bundle_keys_by_idx: list[set[str]] = []
    source_bundle_counts_by_idx: list[int] = []
    location_entries_by_idx: list[list[dict[str, Any]]] = []
    placeholder_location_entries_by_idx: list[list[dict[str, Any]]] = []
    location_keys_by_idx: list[set[str]] = []
    merges = 0
    merged_by_primary = 0
    merged_by_secondary = 0
    merged_by_social = 0
    merged_by_known_mirror_pair = 0
    merged_by_sparse_identity = 0
    merge_samples: list[dict[str, str]] = []
    merge_samples_by_reason: dict[str, list[dict[str, str]]] = {
        "secondary_key": [],
        "sparse_identity": [],
        "social_key": [],
        "known_mirror_pair": [],
        "primary_url": [],
        "unknown": [],
    }
    blocking_merge_samples_by_reason: dict[str, list[dict[str, str]]] = {
        "secondary_key": [],
        "sparse_identity": [],
        "social_key": [],
        "unknown": [],
    }
    merge_gate_tier_counts: dict[str, int] = {}
    blocking_non_primary_reason_counts: dict[str, int] = {}
    monitor_non_primary_reason_counts: dict[str, int] = {}
    current_run_merged_dedup_keys: set[str] = set()
    current_run_known_mirror_pair_dedup_keys: set[str] = set()
    google_sheets_generic_role_guard_samples: list[dict[str, str]] = []
    google_sheets_generic_role_guard_counts: dict[str, int] = {}

    for row in rows:
        current = row if isinstance(row, CanonicalJob) else CanonicalJob.from_mapping(row)
        payload = current.to_dict()
        primary = fingerprint_url(payload.get("jobLink"))
        secondary = dedup_secondary_key(current)
        social_key = _social_key(payload)
        sparse_identity = _sparse_identity_key(current)
        target_idx, merge_reason = _find_merge_target(
            primary=primary,
            secondary=secondary,
            social_key=social_key,
            sparse_identity=sparse_identity,
            current=current,
            current_has_meaningful_locations=_has_meaningful_locations(current),
            merged_rows=merged_rows,
            by_primary=by_primary,
            by_secondary=by_secondary,
            by_social=by_social,
            by_sparse_identity=by_sparse_identity,
            by_smartrecruiters_title_location_alias=by_smartrecruiters_title_location_alias,
            google_sheets_generic_role_guard_samples=(google_sheets_generic_role_guard_samples),
            google_sheets_generic_role_guard_counts=google_sheets_generic_role_guard_counts,
        )
        if target_idx is None:
            _append_new_dedup_row(
                payload=payload,
                primary=primary,
                secondary=secondary,
                sparse_identity=sparse_identity,
                social_key=social_key,
                merged_rows=merged_rows,
                by_primary=by_primary,
                by_secondary=by_secondary,
                by_sparse_identity=by_sparse_identity,
                by_social=by_social,
                by_smartrecruiters_title_location_alias=(by_smartrecruiters_title_location_alias),
                source_bundles_by_idx=source_bundles_by_idx,
                source_bundle_keys_by_idx=source_bundle_keys_by_idx,
                source_bundle_counts_by_idx=source_bundle_counts_by_idx,
                location_entries_by_idx=location_entries_by_idx,
                placeholder_location_entries_by_idx=placeholder_location_entries_by_idx,
                location_keys_by_idx=location_keys_by_idx,
            )
            continue

        merges += 1
        (
            primary_inc,
            secondary_inc,
            social_inc,
            known_mirror_inc,
            sparse_identity_inc,
        ) = _merge_reason_counts(merge_reason)
        merged_by_primary += primary_inc
        merged_by_secondary += secondary_inc
        merged_by_social += social_inc
        merged_by_known_mirror_pair += known_mirror_inc
        merged_by_sparse_identity += sparse_identity_inc
        gate_tier, gate_tier_reason = _merge_gate_tier(
            merge_reason=merge_reason,
            existing=merged_rows[target_idx],
            payload=payload,
        )
        _increment_count(merge_gate_tier_counts, gate_tier)
        gate_tier_reason_count_key = _merge_gate_tier_reason_count_key(gate_tier, gate_tier_reason)
        if gate_tier_reason_count_key:
            _increment_count(merge_gate_tier_counts, gate_tier_reason_count_key)
        reason_count_key = _merge_reason_count_key(merge_reason)
        if reason_count_key not in {"primaryUrl", "knownMirrorPair"}:
            if gate_tier == "blocking":
                _increment_count(blocking_non_primary_reason_counts, reason_count_key)
            else:
                _increment_count(monitor_non_primary_reason_counts, reason_count_key)
        _record_merge_sample(
            merge_samples=merge_samples,
            merge_samples_by_reason=merge_samples_by_reason,
            blocking_merge_samples_by_reason=blocking_merge_samples_by_reason,
            merge_reason=merge_reason,
            existing=merged_rows[target_idx],
            payload=payload,
            gate_tier=gate_tier,
            gate_tier_reason=gate_tier_reason,
        )
        _merge_into_target(
            target_idx=target_idx,
            current=current,
            merged_rows=merged_rows,
            by_primary=by_primary,
            by_secondary=by_secondary,
            by_sparse_identity=by_sparse_identity,
            by_social=by_social,
            by_smartrecruiters_title_location_alias=by_smartrecruiters_title_location_alias,
            source_bundles_by_idx=source_bundles_by_idx,
            source_bundle_keys_by_idx=source_bundle_keys_by_idx,
            source_bundle_counts_by_idx=source_bundle_counts_by_idx,
            location_entries_by_idx=location_entries_by_idx,
            placeholder_location_entries_by_idx=placeholder_location_entries_by_idx,
            location_keys_by_idx=location_keys_by_idx,
        )
        merged_key = clean_text(merged_rows[target_idx].dedupKey)
        if merged_key:
            current_run_merged_dedup_keys.add(merged_key)
            if merge_reason == "known_mirror_pair":
                current_run_known_mirror_pair_dedup_keys.add(merged_key)

    merged_rows = _attach_source_bundle_state(
        rows=merged_rows,
        source_bundles_by_idx=source_bundles_by_idx,
        source_bundle_counts_by_idx=source_bundle_counts_by_idx,
        location_entries_by_idx=location_entries_by_idx,
        placeholder_location_entries_by_idx=placeholder_location_entries_by_idx,
    )
    merged_rows = _sort_enrich_and_number(merged_rows)
    return merged_rows, {
        "inputCount": len(rows),
        "mergedCount": merges,
        "outputCount": len(merged_rows),
        "mergedByPrimaryUrl": merged_by_primary,
        "mergedBySecondaryKey": merged_by_secondary,
        "mergedBySocialKey": merged_by_social,
        "mergedByKnownMirrorPair": merged_by_known_mirror_pair,
        "mergedBySparseIdentity": merged_by_sparse_identity,
        "collisionSamplesCount": len(merge_samples),
        "collisionSamples": merge_samples,
        "collisionSamplesByReason": merge_samples_by_reason,
        "currentRunBlockingMergeSamplesByReason": blocking_merge_samples_by_reason,
        "currentRunMergeGateTierCounts": _merge_gate_tier_payload(merge_gate_tier_counts),
        "currentRunBlockingNonPrimaryMergeReasonCounts": _merge_reason_payload(
            blocking_non_primary_reason_counts
        ),
        "currentRunMonitorNonPrimaryMergeReasonCounts": _merge_reason_payload(
            monitor_non_primary_reason_counts
        ),
        "currentRunMergedDedupKeys": sorted(current_run_merged_dedup_keys),
        "currentRunKnownMirrorPairDedupKeys": sorted(current_run_known_mirror_pair_dedup_keys),
        "sheetRoleBucketGuardBlockedCount": int(
            google_sheets_generic_role_guard_counts.get("total") or 0
        ),
        "sheetRoleBucketGuardBlockedReasonCounts": {
            "secondaryKey": int(google_sheets_generic_role_guard_counts.get("secondary_key") or 0),
            "sparseIdentity": int(
                google_sheets_generic_role_guard_counts.get("sparse_identity") or 0
            ),
        },
        "sheetRoleBucketGuardBlockedSamples": google_sheets_generic_role_guard_samples,
        "googleSheetsGenericRoleGuardBlockedCount": int(
            google_sheets_generic_role_guard_counts.get("total") or 0
        ),
        "googleSheetsGenericRoleGuardBlockedReasonCounts": {
            "secondaryKey": int(google_sheets_generic_role_guard_counts.get("secondary_key") or 0),
            "sparseIdentity": int(
                google_sheets_generic_role_guard_counts.get("sparse_identity") or 0
            ),
        },
        "googleSheetsGenericRoleGuardBlockedSamples": google_sheets_generic_role_guard_samples,
    }


class CanonicalDeduplicator(JobProcessor):
    """Structural deduplicator implementing the JobProcessor protocol."""

    def __init__(self) -> None:
        self.stats: dict[str, Any] = {}

    def process(self, jobs: list[CanonicalJob], **options: Any) -> list[CanonicalJob]:
        merged, stats = deduplicate_jobs(jobs)
        self.stats = stats
        return merged
