"""GameDevMap lost-recovery comparison and audit report summaries.

AI boundary owns: lost-recovery classification, recovered-source comparison, audit report summaries, and validated-candidate promotion.
AI boundary implement in: this file for recovery comparison and report summaries; audit execution stays in gamedevmap_active_dry_run.
AI boundary search before contracts: rejection ledger keys, active_audit_runtime validated-candidate contract, and GameDevMap tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_gamedevmap_active_dry_run.py -q`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src import source_registry as source_registry_module
from src.source_registry import unique_sources

from . import (
    active_audit_runtime,
    audit_report_summary,
)
from .gamedevmap_artifact_store import (
    _safe_int as _safe_int,
)
from .gamedevmap_artifact_store import (
    gamedevmap_active_dry_run_path as gamedevmap_active_dry_run_path,
)
from .gamedevmap_recovery_queue import _candidate_url_key as _candidate_url_key
from .prevalidated_queue_policy import apply_prevalidated_queue_overrides
from .probe_runtime import candidate_id as probe_candidate_id


def _recovered_active_by_id(artifact: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return active_audit_runtime.recovered_active_by_identity(
        artifact,
        active_key="activeCandidates",
        recovered_predicate=lambda row: bool(row.get("gamedevmapRecovery")),
        identity_fn=probe_candidate_id,
    )


def _index_current_rejections(artifact: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    return active_audit_runtime.index_rejections_by_identity(
        artifact,
        rejected_key="rejectedForActivation",
        lookup_keys_fn=lambda rejection: active_audit_runtime.rejection_lookup_keys(
            rejection,
            candidate_identity_fn=probe_candidate_id,
            candidate_url_key_fn=_candidate_url_key,
        ),
    )


def _classify_lost_recovery(
    previous_candidate: dict[str, Any],
    current_rejections: dict[str, list[dict[str, Any]]],
) -> tuple[str, dict[str, Any]]:
    keys = [
        probe_candidate_id(previous_candidate),
        _candidate_url_key(previous_candidate),
        f"entry:{str(previous_candidate.get('sourceDirectoryEntryUrl') or '').strip()}",
    ]
    matched = next(
        (
            rejection
            for key in keys
            for rejection in current_rejections.get(key, [])
            if key and key != "entry:"
        ),
        {},
    )
    reason = str(matched.get("reason") or "").strip()
    detail = str(matched.get("reasonDetail") or "").strip()
    error = str(matched.get("error") or "").strip().lower()
    if reason == "homepage_fetch_failed":
        return "homepage_fetch_failure", matched
    if reason == "probe_failed":
        return "probe_failure", matched
    if reason == "zero_jobs":
        return "zero_jobs", matched
    if detail in {"social_profile_host", "third_party_profile_host"}:
        return "skipped_profile_host", matched
    if detail == "recovery_fetch_failed" or "timeout" in error or "timed out" in error:
        return "recovery_timeout_or_fetch_failed", matched
    if str(previous_candidate.get("gamedevmapRecoverySource") or "") == "same_party_recovery_url":
        return "skipped_wave_two", matched
    return "unknown", matched


def compare_gamedevmap_recovered_sources(
    *,
    current_artifact: dict[str, Any],
    previous_artifact: dict[str, Any],
) -> dict[str, Any]:
    previous = _recovered_active_by_id(previous_artifact)
    current = _recovered_active_by_id(current_artifact)
    current_rejections = _index_current_rejections(current_artifact)
    return active_audit_runtime.compare_recovered_active_maps(
        previous=previous,
        current=current,
        current_rejections=current_rejections,
        classify_lost=_classify_lost_recovery,
        lost_row_builder=lambda row_id, cause, previous_candidate, matched_rejection: {
            "sourceId": row_id,
            "cause": cause,
            "name": str(previous_candidate.get("name") or ""),
            "adapter": str(previous_candidate.get("adapter") or ""),
            "jobsFound": _safe_int(previous_candidate.get("jobsFound")),
            "recoverySource": str(previous_candidate.get("gamedevmapRecoverySource") or ""),
            "careersUrl": str(
                previous_candidate.get("careersUrl") or previous_candidate.get("listing_url") or ""
            ),
            "matchedCurrentRejection": matched_rejection,
        },
    )


def apply_gamedevmap_lost_recovery_audit(
    artifact: dict[str, Any],
    *,
    compare_artifact_path: Path | str | None,
) -> None:
    if compare_artifact_path is None:
        return
    previous = source_registry_module.load_json_object(Path(compare_artifact_path), {})
    if not isinstance(previous, dict) or not previous:
        artifact["lostRecoveryAudit"] = {
            "error": f"compare artifact not found or invalid: {compare_artifact_path}",
            "lostCount": 0,
            "lossCauseCounts": {},
            "lostCandidates": [],
        }
        return
    artifact["lostRecoveryAudit"] = compare_gamedevmap_recovered_sources(
        current_artifact=artifact,
        previous_artifact=previous,
    )


def gamedevmap_audit_report_summary(
    artifact: dict[str, Any],
    *,
    cache_hit: bool = False,
    output_path: Path | str | None = None,
) -> dict[str, Any]:
    summary = audit_report_summary.as_dict(artifact.get("summary"))
    runtime = audit_report_summary.as_dict(artifact.get("runtime"))
    timings = audit_report_summary.as_dict(artifact.get("timings"))
    totals_ms = audit_report_summary.as_dict(timings.get("totalsMs"))
    active_split = audit_report_summary.active_candidate_split(summary)
    return {
        "cacheHit": bool(cache_hit),
        "complete": bool(audit_report_summary.as_dict(artifact.get("progress")).get("complete")),
        "auditDurationMs": audit_report_summary.safe_int(totals_ms.get("totalMs")),
        "activeCandidates": active_split["activeCandidates"],
        "activeProviderCandidates": active_split["activeProviderCandidates"],
        "activeStaticCandidates": active_split["activeStaticCandidates"],
        "recoveredActiveCandidates": audit_report_summary.safe_int(
            summary.get("recoveredActiveCandidates")
        ),
        "browserRecoveryCandidates": audit_report_summary.safe_int(
            summary.get("browserRecoveryCandidates")
        ),
        "browserRecoveredActiveCandidates": audit_report_summary.safe_int(
            summary.get("browserRecoveredActiveCandidates")
        ),
        "artifactSizeBytes": audit_report_summary.artifact_size_bytes(
            summary=summary, runtime=runtime
        ),
        "timingTotalsMs": dict(totals_ms),
        "topFailureBuckets": audit_report_summary.top_failure_buckets(
            rejected_reason_detail_counts=summary.get("rejectedReasonDetailCounts"),
            failure_counts=artifact.get("failureCounts"),
        ),
        "lostRecoveredActiveCandidates": audit_report_summary.safe_int(
            summary.get("lostRecoveredActiveCandidates")
        ),
        "outputPath": str(output_path or gamedevmap_active_dry_run_path()),
    }


def _validated_static_audit_candidate(
    row: dict[str, Any],
    *,
    promote_validated_static: bool,
    validated_static_queue_cap: int,
    validated_static_domain_cap: int,
) -> dict[str, Any] | None:
    if not promote_validated_static:
        return None
    return apply_prevalidated_queue_overrides(
        row,
        adapter_cap=validated_static_queue_cap,
        domain_cap=validated_static_domain_cap,
    )


def gamedevmap_validated_candidates_from_artifact(
    artifact: dict[str, Any],
    *,
    promote_validated_static: bool = True,
    validated_static_queue_cap: int = 0,
    validated_static_domain_cap: int = 0,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return active_audit_runtime.validated_active_candidates_from_artifact(
        artifact,
        active_key="activeCandidates",
        identity_fn=probe_candidate_id,
        validation_metadata={
            "prevalidatedDiscovery": True,
            "gamedevmapAuditValidated": True,
        },
        source_directory="gamedevmap",
        static_transform=lambda row: _validated_static_audit_candidate(
            row,
            promote_validated_static=promote_validated_static,
            validated_static_queue_cap=validated_static_queue_cap,
            validated_static_domain_cap=validated_static_domain_cap,
        ),
        unique_rows=unique_sources,
    )
