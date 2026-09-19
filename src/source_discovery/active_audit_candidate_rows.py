"""Active audit candidate row identity, counting, and rerun selection.

AI boundary owns: candidate row merging by identity, validated-candidate promotion, artifact counts, and rejection/rerun key selection.
AI boundary implement in: this file for candidate row semantics; batch execution stays in active_audit_batch_runtime.
AI boundary search before contracts: rejection ledger keys, recovered active maps, and active audit runtime tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_active_audit_runtime.py -q`.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from typing import Any

from src.shared.json_shapes import (
    as_json_list as _as_list,
)
from src.shared.json_shapes import (
    as_json_object as _as_dict,
)
from src.shared.utils import int_or_default as _safe_int

from . import audit_ledger
from .active_audit_contracts import ActiveAuditArtifactCounts as ActiveAuditArtifactCounts


def _dict_rows(value: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in _as_list(value) if isinstance(row, dict)]


def merge_unique_candidate_rows(
    existing: Any,
    incoming: list[dict[str, Any]],
    *,
    unique_rows: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    return unique_rows([*_dict_rows(existing), *[dict(row) for row in incoming]])


def merge_rows_by_identity(
    existing: Any,
    incoming: list[dict[str, Any]],
    *,
    identity_fn: Callable[[dict[str, Any]], str],
) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    passthrough: list[dict[str, Any]] = []
    for row in [*_as_list(existing), *incoming]:
        if not isinstance(row, dict):
            continue
        row_id = identity_fn(row)
        if row_id:
            rows[row_id] = dict(row)
        else:
            passthrough.append(dict(row))
    return [*passthrough, *rows.values()]


def validated_active_candidates_from_artifact(
    artifact: dict[str, Any],
    *,
    active_key: str,
    identity_fn: Callable[[dict[str, Any]], str],
    validation_metadata: dict[str, Any],
    source_directory: str,
    static_transform: Callable[[dict[str, Any]], dict[str, Any] | None],
    unique_rows: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    for item in _as_list(artifact.get(active_key)):
        if not isinstance(item, dict):
            continue
        row = dict(item)
        if str(row.get("probeStatus") or "").strip().lower() != "ok":
            continue
        if _safe_int(row.get("jobsFound") or row.get("sampleCount")) <= 0:
            continue
        if not identity_fn(row):
            continue
        row.update(dict(validation_metadata))
        row["sourceDirectory"] = str(row.get("sourceDirectory") or source_directory)
        adapter = str(row.get("adapter") or "").strip().lower()
        if adapter == "static":
            static_row = static_transform(row)
            if static_row is not None:
                static_candidates.append(static_row)
        elif adapter:
            provider_candidates.append(row)
    return unique_rows(provider_candidates), unique_rows(static_candidates)


def failure_count(artifact: dict[str, Any]) -> int:
    return audit_ledger.failure_count(artifact)


def active_audit_artifact_counts(
    artifact: dict[str, Any],
    *,
    all_candidates_key: str,
    active_candidates_key: str,
    zero_candidates_key: str,
    rejected_key: str,
    browser_candidates_key: str,
    recovered_predicate: Callable[[dict[str, Any]], bool],
    failure_bucket_fn: Callable[[dict[str, Any]], str],
) -> ActiveAuditArtifactCounts:
    rejected = _dict_rows(artifact.get(rejected_key))
    reason_counts = Counter(str(row.get("reason") or "unknown") for row in rejected)
    detail_counts = Counter(str(row.get("reasonDetail") or "unknown") for row in rejected)
    active = _dict_rows(artifact.get(active_candidates_key))
    all_candidates = _dict_rows(artifact.get(all_candidates_key))
    recovered_candidates = [row for row in all_candidates if recovered_predicate(row)]
    recovered_active = [row for row in active if recovered_predicate(row)]
    technical_failures = [row for row in rejected if failure_bucket_fn(row) == "technical_failure"]
    coverage_misses = [row for row in rejected if failure_bucket_fn(row) == "coverage_miss"]
    adapter_counts = Counter(str(row.get("adapter") or "unknown") for row in active)
    browser_recovery = _as_dict(artifact.get("browserRecovery"))
    lost_recovery = _as_dict(artifact.get("lostRecoveryAudit"))
    return ActiveAuditArtifactCounts(
        rejected_rows=rejected,
        active_rows=active,
        all_candidates=all_candidates,
        recovered_candidates=recovered_candidates,
        recovered_active=recovered_active,
        technical_failures=technical_failures,
        coverage_misses=coverage_misses,
        reason_counts=dict(reason_counts),
        detail_counts=dict(detail_counts),
        active_adapter_counts=dict(adapter_counts),
        zero_job_count=len(_as_list(artifact.get(zero_candidates_key))),
        failure_count=failure_count(artifact),
        failure_sample_count=len(_as_list(artifact.get("failureSamples"))),
        browser_recovery_candidate_count=len(_as_list(artifact.get(browser_candidates_key))),
        browser_recovery_processed_count=_safe_int(browser_recovery.get("processedCount")),
        browser_recovered_active_count=_safe_int(browser_recovery.get("activeCandidates")),
        lost_recovered_active_count=_safe_int(lost_recovery.get("lostCount")),
    )


def row_identity_keys(
    row: dict[str, Any],
    *,
    url: str,
    entry_url: str,
) -> set[str]:
    keys: set[str] = set()
    if url:
        keys.add(f"url:{url}")
    if entry_url:
        keys.add(f"entry:{entry_url}")
    return keys


def rejection_rerun_key(
    rejection: dict[str, Any],
    *,
    candidate_url_fields: tuple[str, ...] = ("careersUrl", "listing_url"),
) -> str:
    url = str(rejection.get("url") or "").strip()
    if url:
        return f"url:{url}"
    candidate = _as_dict(rejection.get("candidate"))
    entry_url = str(candidate.get("sourceDirectoryEntryUrl") or "").strip()
    if entry_url:
        return f"entry:{entry_url}"
    for field_name in candidate_url_fields:
        candidate_url = str(candidate.get(field_name) or "").strip()
        if candidate_url:
            return f"url:{candidate_url}"
    return ""


def select_rerun_rows(
    artifact: dict[str, Any],
    representative_rows: list[dict[str, Any]],
    rerun_reasons: set[str],
    *,
    rejected_key: str,
    rejection_key_fn: Callable[[dict[str, Any]], str],
    row_keys_fn: Callable[[dict[str, Any]], set[str]],
) -> tuple[list[dict[str, Any]], set[str]]:
    if not rerun_reasons:
        return representative_rows, set()
    requested_keys = {
        key
        for rejection in _as_list(artifact.get(rejected_key))
        if isinstance(rejection, dict)
        and str(rejection.get("reason") or "").strip() in rerun_reasons
        for key in [rejection_key_fn(rejection)]
        if key
    }
    if not requested_keys:
        return [], set()
    rows = [row for row in representative_rows if row_keys_fn(row) & requested_keys]
    return rows, requested_keys


def prune_rerun_rejections(
    artifact: dict[str, Any],
    *,
    rejected_key: str,
    rerun_reasons: set[str],
    rerun_row_keys: set[str],
    rejection_key_fn: Callable[[dict[str, Any]], str],
) -> None:
    if not rerun_reasons or not rerun_row_keys:
        return
    kept: list[dict[str, Any]] = []
    for rejection in _as_list(artifact.get(rejected_key)):
        if not isinstance(rejection, dict):
            continue
        reason = str(rejection.get("reason") or "").strip()
        key = rejection_key_fn(rejection)
        if reason in rerun_reasons and key in rerun_row_keys:
            continue
        kept.append(dict(rejection))
    artifact[rejected_key] = kept


def rejection_lookup_keys(
    rejection: dict[str, Any],
    *,
    candidate_identity_fn: Callable[[dict[str, Any]], str],
    candidate_url_key_fn: Callable[[dict[str, Any]], str],
) -> set[str]:
    candidate = _as_dict(rejection.get("candidate"))
    return {
        str(rejection.get("sourceId") or "").strip(),
        candidate_identity_fn(candidate),
        candidate_url_key_fn(candidate),
        f"url:{str(rejection.get('url') or '').strip()}",
        f"entry:{str(rejection.get('sourceDirectoryEntryUrl') or '').strip()}",
        f"entry:{str(candidate.get('sourceDirectoryEntryUrl') or '').strip()}",
    }


def index_rejections_by_identity(
    artifact: dict[str, Any],
    *,
    rejected_key: str,
    lookup_keys_fn: Callable[[dict[str, Any]], set[str]],
) -> dict[str, list[dict[str, Any]]]:
    indexed: dict[str, list[dict[str, Any]]] = {}
    for rejection in _as_list(artifact.get(rejected_key)):
        if not isinstance(rejection, dict):
            continue
        for key in lookup_keys_fn(rejection):
            if key and key not in {"url:", "entry:"}:
                indexed.setdefault(key, []).append(dict(rejection))
    return indexed


def recovered_active_by_identity(
    artifact: dict[str, Any],
    *,
    active_key: str,
    recovered_predicate: Callable[[dict[str, Any]], bool],
    identity_fn: Callable[[dict[str, Any]], str],
) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in _as_list(artifact.get(active_key)):
        if not isinstance(row, dict) or not recovered_predicate(row):
            continue
        row_id = identity_fn(row)
        if row_id:
            rows[row_id] = dict(row)
    return rows


def compare_recovered_active_maps(
    *,
    previous: dict[str, dict[str, Any]],
    current: dict[str, dict[str, Any]],
    current_rejections: dict[str, list[dict[str, Any]]],
    classify_lost: Callable[
        [dict[str, Any], dict[str, list[dict[str, Any]]]], tuple[str, dict[str, Any]]
    ],
    lost_row_builder: Callable[[str, str, dict[str, Any], dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    lost_rows: list[dict[str, Any]] = []
    cause_counts: Counter[str] = Counter()
    for row_id, previous_candidate in previous.items():
        if row_id in current:
            continue
        cause, matched_rejection = classify_lost(previous_candidate, current_rejections)
        cause_counts[cause] += 1
        lost_rows.append(lost_row_builder(row_id, cause, previous_candidate, matched_rejection))
    return {
        "previousRecoveredActiveCount": len(previous),
        "currentRecoveredActiveCount": len(current),
        "lostCount": len(lost_rows),
        "lossCauseCounts": dict(cause_counts),
        "lostCandidates": sorted(lost_rows, key=lambda row: str(row.get("sourceId") or "")),
    }
