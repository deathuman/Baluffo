"""Merge target selection and dedup index building.

AI boundary owns: finding a merge target for a row across primary/secondary/social/
sparse/alias indexes, with google-sheets and provider/static guards.
AI boundary implement in: this leaf for target selection; gate accounting lives in
``dedup_gate.py`` and is applied after targeting in the coordinator.
"""

from __future__ import annotations

import hashlib
from typing import Any

from src.jobs.canonicalize import (
    clean_text,
    compute_focus_score,
    compute_quality_score,
    norm_text,
    normalize_url,
)
from src.jobs.dedup_identity import (
    _SHEET_ROLE_BUCKET_GUARD_REASON,
    SOCIAL_SOURCE_NAMES,
    _has_meaningful_locations,
    _has_sheet_role_bucket_title,
    _is_elevato_static_google_sheets_pair,
    _is_elevato_static_row,
    _is_google_sheets_elevato_row,
    _is_google_sheets_row,
    _is_provider_gracklehq_redirect_alias,
    _merge_source_class,
    fingerprint_url,
)
from src.jobs.dedup_record_merge import _is_gracklehq_gamesjobsdirect_known_mirror_pair
from src.jobs.dedup_state import (
    _apply_location_state_sample,
    _location_state_from_payload,
    _source_bundle_state_from_payload,
    _source_bundle_working_sample,
)
from src.jobs.models import CanonicalJob

from .common.smartrecruiters_identity import (
    is_smartrecruiters_title_location_alias_match,
    smartrecruiters_title_location_alias_keys,
)


def _has_provider_identity(payload: dict[str, Any]) -> bool:
    return _merge_source_class(payload) == "provider"


def _blocks_trusted_distinct_non_primary_merge(
    *,
    current: CanonicalJob,
    target: CanonicalJob,
    current_primary: str,
) -> bool:
    current_payload = current.to_dict()
    target_payload = target.to_dict()
    if "provider" not in {
        _merge_source_class(current_payload),
        _merge_source_class(target_payload),
    }:
        return False
    if _is_provider_gracklehq_redirect_alias(existing=target, payload=current_payload):
        return False
    target_primary = fingerprint_url(target_payload.get("jobLink"))
    if current_primary and target_primary and current_primary == target_primary:
        return False
    current_source_job_id = norm_text(current_payload.get("sourceJobId"))
    target_source_job_id = norm_text(target_payload.get("sourceJobId"))
    if (
        _has_provider_identity(current_payload)
        and _has_provider_identity(target_payload)
        and current_source_job_id
        and target_source_job_id
    ):
        return current_source_job_id != target_source_job_id
    return bool(current_primary and target_primary and current_primary != target_primary)


def _find_elevato_static_google_sheets_target(
    current: CanonicalJob, merged_rows: list[CanonicalJob]
) -> int | None:
    if not (_is_elevato_static_row(current) or _is_google_sheets_elevato_row(current)):
        return None
    for idx, target in enumerate(merged_rows):
        if _is_elevato_static_google_sheets_pair(current, target):
            return idx
    return None


def _social_key(payload: dict[str, Any]) -> str:
    if clean_text(payload.get("source")) in SOCIAL_SOURCE_NAMES and clean_text(
        payload.get("sourceJobId")
    ):
        return f"{clean_text(payload.get('source'))}|{clean_text(payload.get('sourceJobId'))}"
    return ""


def _dedup_key(
    *,
    item: dict[str, Any],
    primary: str,
    secondary: str,
    social_key: str,
) -> str:
    if primary:
        return f"url:{primary}"
    if secondary:
        return f"secondary:{hashlib.sha1(secondary.encode('utf-8')).hexdigest()}"
    if social_key:
        return f"social:{hashlib.sha1(social_key.encode('utf-8')).hexdigest()}"
    fallback = "|".join([norm_text(item.get("company")), norm_text(item.get("title"))])
    return f"secondary:{hashlib.sha1(fallback.encode('utf-8')).hexdigest()}"


def _find_merge_target(
    *,
    primary: str,
    secondary: str,
    social_key: str,
    sparse_identity: str,
    current: CanonicalJob,
    current_has_meaningful_locations: bool,
    merged_rows: list[CanonicalJob],
    by_primary: dict[str, int],
    by_secondary: dict[str, int],
    by_social: dict[str, int],
    by_sparse_identity: dict[str, int],
    by_smartrecruiters_title_location_alias: dict[str, int],
    google_sheets_generic_role_guard_samples: list[dict[str, str]],
    google_sheets_generic_role_guard_counts: dict[str, int],
) -> tuple[int | None, str]:
    if primary and primary in by_primary:
        return by_primary[primary], "primary_url"
    secondary_target_idx, secondary_reason, secondary_blocked = _find_secondary_merge_target(
        primary=primary,
        secondary=secondary,
        current=current,
        merged_rows=merged_rows,
        by_secondary=by_secondary,
        google_sheets_generic_role_guard_samples=(google_sheets_generic_role_guard_samples),
        google_sheets_generic_role_guard_counts=(google_sheets_generic_role_guard_counts),
    )
    if secondary_blocked or secondary_target_idx is not None:
        return secondary_target_idx, secondary_reason
    if social_key and social_key in by_social:
        return by_social[social_key], "social_key"
    sparse_target_idx, sparse_reason, sparse_blocked = _find_sparse_merge_target(
        primary=primary,
        sparse_identity=sparse_identity,
        current=current,
        current_has_meaningful_locations=current_has_meaningful_locations,
        merged_rows=merged_rows,
        by_sparse_identity=by_sparse_identity,
        google_sheets_generic_role_guard_samples=google_sheets_generic_role_guard_samples,
        google_sheets_generic_role_guard_counts=google_sheets_generic_role_guard_counts,
    )
    if sparse_blocked or sparse_target_idx is not None:
        return sparse_target_idx, sparse_reason
    elevato_target_idx = _find_elevato_static_google_sheets_target(current, merged_rows)
    if elevato_target_idx is not None:
        return elevato_target_idx, "sparse_identity"
    alias_target_idx = _find_smartrecruiters_title_location_alias_target(
        current=current,
        merged_rows=merged_rows,
        by_smartrecruiters_title_location_alias=by_smartrecruiters_title_location_alias,
    )
    if alias_target_idx is not None:
        return alias_target_idx, "secondary_key"
    return None, ""


def _find_smartrecruiters_title_location_alias_target(
    *,
    current: CanonicalJob,
    merged_rows: list[CanonicalJob],
    by_smartrecruiters_title_location_alias: dict[str, int],
) -> int | None:
    current_payload = current.to_dict()
    for alias_key in smartrecruiters_title_location_alias_keys(current_payload):
        alias_target_idx = by_smartrecruiters_title_location_alias.get(alias_key)
        if alias_target_idx is None:
            continue
        alias_target = merged_rows[alias_target_idx]
        if is_smartrecruiters_title_location_alias_match(current_payload, alias_target.to_dict()):
            return alias_target_idx
    return None


def _blocks_google_sheets_generic_role_url_merge(
    *,
    current: CanonicalJob,
    target: CanonicalJob,
    current_primary: str,
) -> bool:
    if _is_google_sheets_row(current) and _has_sheet_role_bucket_title(current):
        if not _is_google_sheets_row(target):
            return True
        target_primary = fingerprint_url(target.jobLink)
        if _has_sheet_role_bucket_title(target):
            return bool(current_primary and target_primary and current_primary != target_primary)
    if not _is_google_sheets_row(current) or not _is_google_sheets_row(target):
        return False
    if not _has_sheet_role_bucket_title(current) and not _has_sheet_role_bucket_title(target):
        return False
    target_primary = fingerprint_url(target.jobLink)
    return bool(current_primary and target_primary and current_primary != target_primary)


def _record_google_sheets_generic_role_guard_sample(
    *,
    samples: list[dict[str, str]],
    counts: dict[str, int],
    blocked_merge_reason: str,
    current: CanonicalJob,
    target: CanonicalJob,
) -> None:
    counts["total"] = int(counts.get("total") or 0) + 1
    counts[blocked_merge_reason] = int(counts.get(blocked_merge_reason) or 0) + 1
    if len(samples) >= 10:
        return
    current_payload = current.to_dict()
    target_payload = target.to_dict()
    samples.append(
        {
            "classification": "fixed_by_generic_role_guard",
            "blockedMergeReason": clean_text(blocked_merge_reason),
            "guardReason": _SHEET_ROLE_BUCKET_GUARD_REASON,
            "existingDedupKey": clean_text(target.dedupKey),
            "targetSource": clean_text(target_payload.get("source")),
            "targetTitle": clean_text(target_payload.get("title")),
            "targetCompany": clean_text(target_payload.get("company")),
            "targetJobLink": normalize_url(target_payload.get("jobLink")),
            "targetSourceJobId": clean_text(target_payload.get("sourceJobId")),
            "incomingSource": clean_text(current_payload.get("source")),
            "incomingTitle": clean_text(current_payload.get("title")),
            "incomingCompany": clean_text(current_payload.get("company")),
            "incomingJobLink": normalize_url(current_payload.get("jobLink")),
            "incomingSourceJobId": clean_text(current_payload.get("sourceJobId")),
        }
    )


def _find_secondary_merge_target(
    *,
    primary: str,
    secondary: str,
    current: CanonicalJob,
    merged_rows: list[CanonicalJob],
    by_secondary: dict[str, int],
    google_sheets_generic_role_guard_samples: list[dict[str, str]],
    google_sheets_generic_role_guard_counts: dict[str, int],
) -> tuple[int | None, str, bool]:
    if not secondary or secondary not in by_secondary:
        return None, "", False
    target_idx = by_secondary[secondary]
    target = merged_rows[target_idx]
    if _blocks_google_sheets_generic_role_url_merge(
        current=current,
        target=target,
        current_primary=primary,
    ):
        _record_google_sheets_generic_role_guard_sample(
            samples=google_sheets_generic_role_guard_samples,
            counts=google_sheets_generic_role_guard_counts,
            blocked_merge_reason="secondary_key",
            current=current,
            target=target,
        )
        return None, "", False
    if _is_gracklehq_gamesjobsdirect_known_mirror_pair(current, target):
        return target_idx, "known_mirror_pair", False
    if _blocks_trusted_distinct_non_primary_merge(
        current=current,
        target=target,
        current_primary=primary,
    ):
        return None, "", True
    return target_idx, "secondary_key", False


def _find_sparse_merge_target(
    *,
    primary: str,
    sparse_identity: str,
    current: CanonicalJob,
    current_has_meaningful_locations: bool,
    merged_rows: list[CanonicalJob],
    by_sparse_identity: dict[str, int],
    google_sheets_generic_role_guard_samples: list[dict[str, str]],
    google_sheets_generic_role_guard_counts: dict[str, int],
) -> tuple[int | None, str, bool]:
    if not sparse_identity or sparse_identity not in by_sparse_identity:
        return None, "", False
    target_idx = by_sparse_identity[sparse_identity]
    target = merged_rows[target_idx]
    if _blocks_google_sheets_generic_role_url_merge(
        current=current,
        target=target,
        current_primary=primary,
    ):
        if _is_google_sheets_row(target):
            _record_google_sheets_generic_role_guard_sample(
                samples=google_sheets_generic_role_guard_samples,
                counts=google_sheets_generic_role_guard_counts,
                blocked_merge_reason="sparse_identity",
                current=current,
                target=target,
            )
            return None, "", True
    if _has_meaningful_locations(target) and current_has_meaningful_locations:
        return None, "", False
    if _blocks_trusted_distinct_non_primary_merge(
        current=current,
        target=target,
        current_primary=primary,
    ):
        return None, "", True
    return target_idx, "sparse_identity", False


def _index_row_keys(
    *,
    idx: int,
    primary: str,
    secondary: str,
    sparse_identity: str,
    social_key: str,
    by_primary: dict[str, int],
    by_secondary: dict[str, int],
    by_sparse_identity: dict[str, int],
    by_social: dict[str, int],
) -> None:
    if primary:
        by_primary[primary] = idx
    if secondary:
        by_secondary[secondary] = idx
    if sparse_identity:
        by_sparse_identity[sparse_identity] = idx
    if social_key:
        by_social[social_key] = idx


def _index_smartrecruiters_title_location_alias_keys(
    *,
    idx: int,
    row: CanonicalJob | dict[str, Any],
    by_smartrecruiters_title_location_alias: dict[str, int],
) -> None:
    payload = row.to_dict() if isinstance(row, CanonicalJob) else dict(row)
    for key in smartrecruiters_title_location_alias_keys(payload):
        by_smartrecruiters_title_location_alias[key] = idx


def _append_new_dedup_row(
    *,
    payload: dict[str, Any],
    primary: str,
    secondary: str,
    sparse_identity: str,
    social_key: str,
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
    item = dict(payload)
    source_bundle, source_bundle_keys, source_bundle_count = _source_bundle_state_from_payload(item)
    location_entries, placeholder_location_entries, location_keys = _location_state_from_payload(
        item
    )
    item["sourceBundle"] = _source_bundle_working_sample(source_bundle, primary=item)
    item["sourceBundleCount"] = source_bundle_count
    _apply_location_state_sample(
        merged=item,
        location_entries=location_entries,
        placeholder_location_entries=placeholder_location_entries,
    )
    item["dedupKey"] = _dedup_key(
        item=item,
        primary=primary,
        secondary=secondary,
        social_key=social_key,
    )
    item["qualityScore"] = compute_quality_score(item)
    item["focusScore"] = compute_focus_score(item)
    merged_rows.append(CanonicalJob.from_mapping(item))
    source_bundles_by_idx.append(source_bundle)
    source_bundle_keys_by_idx.append(source_bundle_keys)
    source_bundle_counts_by_idx.append(source_bundle_count)
    location_entries_by_idx.append(location_entries)
    placeholder_location_entries_by_idx.append(placeholder_location_entries)
    location_keys_by_idx.append(location_keys)
    _index_row_keys(
        idx=len(merged_rows) - 1,
        primary=primary,
        secondary=secondary,
        sparse_identity=sparse_identity,
        social_key=social_key,
        by_primary=by_primary,
        by_secondary=by_secondary,
        by_sparse_identity=by_sparse_identity,
        by_social=by_social,
    )
    _index_smartrecruiters_title_location_alias_keys(
        idx=len(merged_rows) - 1,
        row=merged_rows[-1],
        by_smartrecruiters_title_location_alias=by_smartrecruiters_title_location_alias,
    )
