"""Dedup audit gate payload builder.

Extracted from reporting_dedup_evidence.py as part of the dedup evidence split.

AI boundary owns: audit gate payload assembly and lifecycle UX readiness.
AI boundary implement in: this file for audit gate payload; review queue and bundle report stay in sibling leaves.
AI boundary search before contracts: dedup evidence merge, audit gate details, and review queue tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused dedup evidence tests.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.jobs.common.contracts_dedup_evidence import (
    DedupAuditGatePayload,
)
from src.jobs.common.dedup_evidence_audit_gate import (
    _audit_gate_blockers_and_warnings,
    _audit_gate_examples,
    _audit_gate_high_risk_count,
    _audit_gate_provider_static_disagreement_count,
    _build_audit_gate_details,
)
from src.jobs.common.dedup_evidence_merge import (
    REVIEW_QUEUE_CAUSE_KEYS,
    _current_run_non_primary_merge_counts,
    _mapping_value,
    _nonzero_counts,
    _review_cause_counts_by_key,
)


def build_dedup_audit_gate(dedup_evidence: Mapping[str, Any]) -> DedupAuditGatePayload:
    """Summarize whether dedup evidence is ready for read-only lifecycle UX."""
    merged_count = max(0, int(dedup_evidence.get("mergedCount") or 0))
    source_bundle_collision_count = max(
        0, int(dedup_evidence.get("sourceBundleCollisionCount") or 0)
    )
    current_run_source_bundle_collision_count = max(
        0, int(dedup_evidence.get("currentRunSourceBundleCollisionCount") or 0)
    )
    carried_source_bundle_collision_count = max(
        0, int(dedup_evidence.get("carriedSourceBundleCollisionCount") or 0)
    )
    current_run_high_risk_review_queue_count = max(
        0, int(dedup_evidence.get("currentRunHighRiskReviewQueueCount") or 0)
    )
    carried_high_risk_review_queue_count = max(
        0, int(dedup_evidence.get("carriedHighRiskReviewQueueCount") or 0)
    )
    current_run_blocking_review_queue_count = max(
        0, int(dedup_evidence.get("currentRunBlockingReviewQueueCount") or 0)
    )
    carried_blocking_review_queue_count = max(
        0, int(dedup_evidence.get("carriedBlockingReviewQueueCount") or 0)
    )
    current_run_monitor_review_queue_count = max(
        0, int(dedup_evidence.get("currentRunMonitorReviewQueueCount") or 0)
    )
    carried_monitor_review_queue_count = max(
        0, int(dedup_evidence.get("carriedMonitorReviewQueueCount") or 0)
    )
    merge_reason_counts = _mapping_value(dedup_evidence, "mergeReasonCounts")
    blocking_non_primary_reason_counts = _mapping_value(
        dedup_evidence, "currentRunBlockingNonPrimaryMergeReasonCounts"
    )
    monitor_non_primary_reason_counts = _mapping_value(
        dedup_evidence, "currentRunMonitorNonPrimaryMergeReasonCounts"
    )
    merge_gate_tier_counts = _mapping_value(dedup_evidence, "currentRunMergeGateTierCounts")
    review_queue_cause_counts = _mapping_value(dedup_evidence, "reviewQueueCauseCounts")
    current_run_blocking_review_queue_cause_counts = _mapping_value(
        dedup_evidence, "currentRunBlockingReviewQueueCauseCounts"
    )
    carried_blocking_review_queue_cause_counts = _mapping_value(
        dedup_evidence, "carriedBlockingReviewQueueCauseCounts"
    )
    current_run_monitor_review_queue_cause_counts = _mapping_value(
        dedup_evidence, "currentRunMonitorReviewQueueCauseCounts"
    )
    carried_monitor_review_queue_cause_counts = _mapping_value(
        dedup_evidence, "carriedMonitorReviewQueueCauseCounts"
    )
    provider_static_disagreement_counts = _mapping_value(
        dedup_evidence, "providerStaticDisagreementCounts"
    )
    provider_static_disagreement_gate_counts = _mapping_value(
        dedup_evidence, "providerStaticDisagreementGateCounts"
    )
    google_sheets_role_bucket_audit = _mapping_value(dedup_evidence, "googleSheetsRoleBucketAudit")
    title_company_collision_audit_counts = _mapping_value(
        dedup_evidence, "providerStaticTitleCompanyCollisionAuditCounts"
    )
    provider_static_disagreement_count = _audit_gate_provider_static_disagreement_count(
        provider_static_disagreement_counts=provider_static_disagreement_counts,
        review_queue_cause_counts=review_queue_cause_counts,
        risk_reason_counts=_mapping_value(dedup_evidence, "riskReasonCounts"),
        outlier_reason_counts=_mapping_value(dedup_evidence, "outlierReasonCounts"),
    )
    provider_static_current_run_count = max(
        0, int(provider_static_disagreement_counts.get("currentRun") or 0)
    )
    provider_static_carried_count = max(
        0, int(provider_static_disagreement_counts.get("carried") or 0)
    )
    current_run_provider_static_disagreement_blocking_count = max(
        0, int(provider_static_disagreement_gate_counts.get("currentRunBlocked") or 0)
    )
    carried_provider_static_disagreement_blocking_count = max(
        0, int(provider_static_disagreement_gate_counts.get("carriedBlocked") or 0)
    )
    provider_static_location_pollution_count = max(
        0, int(title_company_collision_audit_counts.get("carried_location_pollution") or 0)
    )
    provider_static_auto_safe_warning_count = max(
        0, int(provider_static_disagreement_gate_counts.get("autoSafeWarning") or 0)
    )
    provider_static_reviewed_safe_warning_count = max(
        0, int(provider_static_disagreement_gate_counts.get("reviewedSafeWarning") or 0)
    )
    if (
        "currentRunHighRiskReviewQueueCount" in dedup_evidence
        or "carriedHighRiskReviewQueueCount" in dedup_evidence
    ):
        high_risk_review_queue_count = (
            current_run_high_risk_review_queue_count + carried_high_risk_review_queue_count
        )
    else:
        high_risk_review_queue_count = _audit_gate_high_risk_count(review_queue_cause_counts)
    if (
        "currentRunBlockingReviewQueueCount" in dedup_evidence
        or "carriedBlockingReviewQueueCount" in dedup_evidence
    ):
        blocking_review_queue_count = (
            current_run_blocking_review_queue_count + carried_blocking_review_queue_count
        )
    else:
        current_run_blocking_review_queue_count = current_run_high_risk_review_queue_count
        carried_blocking_review_queue_count = carried_high_risk_review_queue_count
        blocking_review_queue_count = high_risk_review_queue_count
    primary_url_merge_count = max(0, int(merge_reason_counts.get("primaryUrl") or 0))
    if (
        "currentRunBlockingNonPrimaryMergeReasonCounts" in dedup_evidence
        or "currentRunMonitorNonPrimaryMergeReasonCounts" in dedup_evidence
    ):
        current_run_non_primary_merge_counts = _current_run_non_primary_merge_counts(
            merge_reason_counts,
            blocking_reason_counts=blocking_non_primary_reason_counts,
            monitor_reason_counts=monitor_non_primary_reason_counts,
        )
        current_run_non_primary_merges = max(
            0, int(current_run_non_primary_merge_counts.get("blocking") or 0)
        )
    else:
        current_run_non_primary_merges = max(
            0,
            merged_count - int(merge_reason_counts.get("primaryUrl") or 0),
        )
        current_run_non_primary_merges = max(
            0,
            current_run_non_primary_merges - int(merge_reason_counts.get("knownMirrorPair") or 0),
        )
        current_run_non_primary_merge_counts = _current_run_non_primary_merge_counts(
            merge_reason_counts
        )
    carried_collision_likely_historical_count = (
        carried_source_bundle_collision_count
        if carried_source_bundle_collision_count
        else source_bundle_collision_count
        if merged_count == 0
        else 0
    )
    blockers, warnings = _audit_gate_blockers_and_warnings(
        primary_url_merge_count=primary_url_merge_count,
        current_run_non_primary_merges=current_run_non_primary_merges,
        current_run_provider_static_disagreement_blocking_count=(
            current_run_provider_static_disagreement_blocking_count
        ),
        carried_provider_static_disagreement_blocking_count=(
            carried_provider_static_disagreement_blocking_count
        ),
        provider_static_location_pollution_count=provider_static_location_pollution_count,
        provider_static_auto_safe_warning_count=provider_static_auto_safe_warning_count,
        provider_static_reviewed_safe_warning_count=provider_static_reviewed_safe_warning_count,
        current_run_blocking_review_queue_count=current_run_blocking_review_queue_count,
        carried_blocking_review_queue_count=carried_blocking_review_queue_count,
        current_run_monitor_review_queue_count=current_run_monitor_review_queue_count,
        carried_monitor_review_queue_count=carried_monitor_review_queue_count,
        carried_collision_likely_historical_count=carried_collision_likely_historical_count,
        blocking_review_queue_count=blocking_review_queue_count,
    )
    blocker_details, warning_details = _build_audit_gate_details(
        dedup_evidence=dedup_evidence,
        blockers=blockers,
        warnings=warnings,
        primary_url_merge_count=primary_url_merge_count,
        current_run_non_primary_merge_counts=current_run_non_primary_merge_counts,
        current_run_non_primary_merges=current_run_non_primary_merges,
        current_run_blocking_review_queue_count=current_run_blocking_review_queue_count,
        carried_blocking_review_queue_count=carried_blocking_review_queue_count,
        current_run_monitor_review_queue_count=current_run_monitor_review_queue_count,
        carried_monitor_review_queue_count=carried_monitor_review_queue_count,
        current_run_blocking_review_queue_cause_counts=(
            current_run_blocking_review_queue_cause_counts
        ),
        carried_blocking_review_queue_cause_counts=(carried_blocking_review_queue_cause_counts),
        current_run_monitor_review_queue_cause_counts=current_run_monitor_review_queue_cause_counts,
        carried_monitor_review_queue_cause_counts=carried_monitor_review_queue_cause_counts,
        current_run_provider_static_disagreement_blocking_count=(
            current_run_provider_static_disagreement_blocking_count
        ),
        carried_provider_static_disagreement_blocking_count=(
            carried_provider_static_disagreement_blocking_count
        ),
        provider_static_location_pollution_count=provider_static_location_pollution_count,
        provider_static_auto_safe_warning_count=provider_static_auto_safe_warning_count,
        provider_static_reviewed_safe_warning_count=provider_static_reviewed_safe_warning_count,
        provider_static_disagreement_gate_counts=provider_static_disagreement_gate_counts,
        carried_collision_likely_historical_count=carried_collision_likely_historical_count,
    )

    status = "blocked" if blockers else "warning" if warnings else "pass"
    return {
        "status": status,
        "lifecycleUxReady": not blockers,
        "currentRunMergedCount": merged_count,
        "currentRunNonPrimaryMergeCounts": current_run_non_primary_merge_counts,
        "currentRunMergeGateTierCounts": {
            key: int(merge_gate_tier_counts.get(key, 0))
            for key in (
                "blocking",
                "monitor",
                "blockingTrustedIdentity",
                "blockingTrustedSocialIdentity",
                "monitorWeakNonProviderIdentity",
                "monitorPrimaryUrl",
                "monitorKnownMirrorPair",
                "monitorProviderGracklehqRedirectAlias",
            )
        },
        "sourceBundleCollisionCount": source_bundle_collision_count,
        "currentRunSourceBundleCollisionCount": current_run_source_bundle_collision_count,
        "carriedSourceBundleCollisionCount": carried_source_bundle_collision_count,
        "highRiskReviewQueueCount": high_risk_review_queue_count,
        "currentRunHighRiskReviewQueueCount": current_run_high_risk_review_queue_count,
        "carriedHighRiskReviewQueueCount": carried_high_risk_review_queue_count,
        "blockingReviewQueueCount": blocking_review_queue_count,
        "currentRunBlockingReviewQueueCount": current_run_blocking_review_queue_count,
        "carriedBlockingReviewQueueCount": carried_blocking_review_queue_count,
        "monitorReviewQueueCount": (
            current_run_monitor_review_queue_count + carried_monitor_review_queue_count
        ),
        "currentRunMonitorReviewQueueCount": current_run_monitor_review_queue_count,
        "carriedMonitorReviewQueueCount": carried_monitor_review_queue_count,
        "providerStaticDisagreementCount": provider_static_disagreement_count,
        "providerStaticDisagreementCurrentRunCount": provider_static_current_run_count,
        "providerStaticDisagreementCarriedCount": provider_static_carried_count,
        "providerStaticDisagreementBlockedCount": max(
            0,
            current_run_provider_static_disagreement_blocking_count
            + carried_provider_static_disagreement_blocking_count,
        ),
        "providerStaticDisagreementWarningCount": max(
            0, int(provider_static_disagreement_gate_counts.get("warning") or 0)
        ),
        "googleSheetsGenericRoleGuardActive": True,
        "googleSheetsRoleBucketUnresolvedCount": max(
            0, int(google_sheets_role_bucket_audit.get("unresolvedRoleBucketCount") or 0)
        ),
        "googleSheetsRoleBucketGuardBlockedCount": max(
            0,
            int(google_sheets_role_bucket_audit.get("blockedByDifferentPrimaryUrlCount") or 0),
        ),
        "googleSheetsRoleBucketHistoricalCount": max(
            0,
            int(google_sheets_role_bucket_audit.get("likelyHistoricalCollisionCount") or 0),
        ),
        "carriedCollisionLikelyHistoricalCount": carried_collision_likely_historical_count,
        "reviewQueueCauseCounts": {
            key: int(review_queue_cause_counts.get(key, 0)) for key in REVIEW_QUEUE_CAUSE_KEYS
        },
        "currentRunBlockingReviewQueueCauseCounts": _review_cause_counts_by_key(
            current_run_blocking_review_queue_cause_counts
        ),
        "carriedBlockingReviewQueueCauseCounts": _review_cause_counts_by_key(
            carried_blocking_review_queue_cause_counts
        ),
        "currentRunMonitorReviewQueueCauseCounts": _review_cause_counts_by_key(
            current_run_monitor_review_queue_cause_counts
        ),
        "carriedMonitorReviewQueueCauseCounts": _review_cause_counts_by_key(
            carried_monitor_review_queue_cause_counts
        ),
        "blockers": blockers,
        "warnings": warnings,
        "blockerDetails": blocker_details,
        "warningDetails": warning_details,
        "examples": _audit_gate_examples(dedup_evidence),
        "nonzeroReviewQueueCauseCounts": _nonzero_counts(review_queue_cause_counts),
    }
