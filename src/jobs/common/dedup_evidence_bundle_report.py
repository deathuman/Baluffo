"""Dedup bundle report builder.

Extracted from reporting_dedup_evidence.py as part of the dedup evidence split.

AI boundary owns: dedup evidence bundle report assembly and top/risky limits.
AI boundary implement in: this file for bundle report; review queue and audit gate payload stay in sibling leaves.
AI boundary search before contracts: dedup evidence bundle, google sheets, provider/static, and merge evidence tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused dedup evidence tests.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any, cast

from src.jobs.common.contracts_dedup_evidence import (
    DedupEvidencePayload,
)
from src.jobs.common.dedup_evidence_audit_gate_payload import build_dedup_audit_gate
from src.jobs.common.dedup_evidence_bundle import (
    IDENTITY_QUALITY_KEYS,
    IDENTITY_SHAPE_KEYS,
    NON_PROVIDER_IDENTITY_PROVENANCE_KEYS,
    OUTLIER_REASON_KEYS,
    _limit_provider_static_examples,
    _payload,
    _risky_reasons,
    _source_bundle,
    _source_class,
)
from src.jobs.common.dedup_evidence_google_sheets import (
    GOOGLE_SHEETS_BUCKET_INTENT_KEYS,
    GOOGLE_SHEETS_BUNDLE_SHAPE_KEYS,
    GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_KEYS,
    GOOGLE_SHEETS_WEAK_GROUPING_AUDIT_KEYS,
    _google_sheets_role_bucket_audit_summary,
    _is_google_sheets_role_bucket_summary,
)
from src.jobs.common.dedup_evidence_merge import (
    REVIEW_QUEUE_CAUSE_KEYS,
    _current_run_blocking_merge_examples_by_reason,
    _current_run_merge_examples,
    _current_run_merge_examples_by_reason,
    _current_run_non_primary_merge_counts,
    _merge_reason_counts,
)
from src.jobs.common.dedup_evidence_provider_static import (
    PROVIDER_STATIC_DISAGREEMENT_CLASSIFICATION_KEYS,
    PROVIDER_STATIC_TITLE_COMPANY_COLLISION_AUDIT_KEYS,
    _company_countryless_location_token_counts,
    _provider_static_disagreement_origin_update,
    _provider_static_row_with_gate_fields,
    _provider_static_title_company_collision_audit,
    _review_pressure_origin_counts,
    _update_review_pressure_cause_counts,
)
from src.jobs.common.dedup_evidence_review_queue import (
    _job_summary,
    _recommended_review_action,
    _should_include_review_queue_row,
)
from src.jobs.models import CanonicalJob
from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import json_object_rows

TOP_MERGED_LIMIT = 10

RISKY_EXAMPLE_LIMIT = 10

REVIEW_QUEUE_ACTION_KEYS = (
    "review_many_urls_same_title",
    "review_listing_url_bundle",
    "review_category_title_bundle",
    "review_open_application_bundle",
    "review_provider_static_disagreement",
    "monitor",
)


def build_dedup_evidence(
    dedup_stats: Mapping[str, Any],
    canonical_rows: Sequence[CanonicalJob | Mapping[str, Any]],
    *,
    top_limit: int = TOP_MERGED_LIMIT,
    risky_limit: int = RISKY_EXAMPLE_LIMIT,
    seeded_from_existing_output: bool = False,
    review_state: Any = None,
) -> DedupEvidencePayload:
    """Build compact diagnostics without changing dedup decisions."""
    rows = [_payload(row) for row in canonical_rows]
    composition: Counter[str] = Counter()
    risk_reason_counts: Counter[str] = Counter()
    outlier_reason_counts: Counter[str] = Counter()
    identity_shape_counts: Counter[str] = Counter()
    review_queue_counts: Counter[str] = Counter()
    review_queue_cause_counts: Counter[str] = Counter()
    identity_quality_counts: Counter[str] = Counter()
    non_provider_identity_provenance_counts: Counter[str] = Counter()
    google_sheets_bundle_shape_counts: Counter[str] = Counter()
    google_sheets_role_bucket_audit_counts: Counter[str] = Counter()
    google_sheets_bucket_intent_counts: Counter[str] = Counter()
    google_sheets_weak_grouping_audit_counts: Counter[str] = Counter()
    provider_static_disagreement_classification_counts: Counter[str] = Counter()
    top_rows: list[dict[str, Any]] = []
    risky_rows: list[dict[str, Any]] = []
    location_divergence_rows: list[dict[str, Any]] = []
    review_queue_rows: list[dict[str, Any]] = []
    carried_bundle_rows: list[dict[str, Any]] = []
    google_sheets_role_bucket_rows: list[dict[str, Any]] = []
    provider_static_disagreement_rows: list[Mapping[str, Any]] = []
    source_bundle_collision_count = 0
    current_run_source_bundle_collision_count = 0
    carried_source_bundle_collision_count = 0
    current_run_high_risk_review_queue_count = 0
    carried_high_risk_review_queue_count = 0
    current_run_blocking_review_queue_count = 0
    carried_blocking_review_queue_count = 0
    current_run_monitor_review_queue_count = 0
    carried_monitor_review_queue_count = 0
    current_run_blocking_review_queue_cause_counts: Counter[str] = Counter()
    carried_blocking_review_queue_cause_counts: Counter[str] = Counter()
    current_run_monitor_review_queue_cause_counts: Counter[str] = Counter()
    carried_monitor_review_queue_cause_counts: Counter[str] = Counter()
    current_run_provider_static_disagreement_count = 0
    carried_provider_static_disagreement_count = 0
    current_run_merged_dedup_keys = {
        clean_text(value) for value in dedup_stats.get("currentRunMergedDedupKeys") or []
    }
    current_run_known_mirror_pair_dedup_keys = {
        clean_text(value) for value in dedup_stats.get("currentRunKnownMirrorPairDedupKeys") or []
    }

    for row in rows:
        bundle = _source_bundle(row)
        for item in bundle:
            composition[_source_class(item)] += 1
        bundle_count = max(int(row.get("sourceBundleCount") or 0), len(bundle))
        if bundle_count > 1:
            source_bundle_collision_count += 1
            summary = _job_summary(row, bundle)
            dedup_key = clean_text(summary.get("dedupKey"))
            origin = (
                "current_run"
                if not seeded_from_existing_output or dedup_key in current_run_merged_dedup_keys
                else "carried_from_existing_output"
            )
            summary["bundleEvidenceOrigin"] = origin
            if origin == "current_run":
                current_run_source_bundle_collision_count += 1
            else:
                carried_source_bundle_collision_count += 1
                carried_bundle_rows.append(summary)
            if _is_google_sheets_role_bucket_summary(summary):
                google_sheets_role_bucket_rows.append(summary)
            top_rows.append(summary)
            outlier_reason_counts.update([summary["outlierReason"]])
            identity_shape_counts.update([summary["identityShape"]])
            identity_quality_counts.update([summary["identityQuality"]])
            non_provider_identity_provenance_counts.update(
                [summary["nonProviderIdentityProvenance"]]
            )
            google_sheets_bundle_shape_counts.update([summary["googleSheetsBundleShape"]])
            google_sheets_role_bucket_audit_counts.update([summary["googleSheetsRoleBucketAudit"]])
            google_sheets_bucket_intent_counts.update([summary["googleSheetsBucketIntent"]])
            google_sheets_weak_grouping_audit_counts.update(
                [summary["googleSheetsWeakGroupingAudit"]]
            )
            review_action = _recommended_review_action(summary)
            review_queue_counts.update([review_action])
            review_queue_cause_counts.update([summary["suspectedCause"]])
            (
                current_high_risk,
                carried_high_risk,
                current_blocking,
                carried_blocking,
                current_monitor,
                carried_monitor,
            ) = _review_pressure_origin_counts(
                summary=summary,
                origin=origin,
                current_run_known_mirror_pair_dedup_keys=current_run_known_mirror_pair_dedup_keys,
                review_action=review_action,
            )
            current_run_high_risk_review_queue_count += current_high_risk
            carried_high_risk_review_queue_count += carried_high_risk
            current_run_blocking_review_queue_count += current_blocking
            carried_blocking_review_queue_count += carried_blocking
            current_run_monitor_review_queue_count += current_monitor
            carried_monitor_review_queue_count += carried_monitor
            _update_review_pressure_cause_counts(
                summary=summary,
                current_blocking=current_blocking,
                carried_blocking=carried_blocking,
                current_monitor=current_monitor,
                carried_monitor=carried_monitor,
                current_run_blocking_review_queue_cause_counts=(
                    current_run_blocking_review_queue_cause_counts
                ),
                carried_blocking_review_queue_cause_counts=(
                    carried_blocking_review_queue_cause_counts
                ),
                current_run_monitor_review_queue_cause_counts=(
                    current_run_monitor_review_queue_cause_counts
                ),
                carried_monitor_review_queue_cause_counts=(
                    carried_monitor_review_queue_cause_counts
                ),
            )
            if _should_include_review_queue_row(summary, review_action):
                review_queue_rows.append({**summary, "recommendedReviewAction": review_action})
            if int(summary.get("distinctLocationCount") or 0) > 1:
                location_divergence_rows.append(summary)
            current_disagreement, carried_disagreement, disagreement_rows = (
                _provider_static_disagreement_origin_update(summary, bundle)
            )
            current_run_provider_static_disagreement_count += current_disagreement
            carried_provider_static_disagreement_count += carried_disagreement
            provider_static_disagreement_rows.extend(disagreement_rows)
            provider_static_disagreement_classification_counts.update(
                row.get("disagreementClassification", "needs_manual_review")
                for row in disagreement_rows
            )
            reasons = _risky_reasons(row, bundle)
            if reasons:
                risk_reason_counts.update(reasons)
                risky_rows.append({**summary, "riskReasons": reasons})

    top_rows.sort(
        key=lambda row: (
            -int(row.get("sourceBundleCount") or 0),
            norm_text(row.get("company")),
            norm_text(row.get("title")),
            norm_text(row.get("dedupKey")),
        )
    )
    risky_rows.sort(
        key=lambda row: (
            ",".join(str(reason) for reason in row.get("riskReasons") or []),
            norm_text(row.get("company")),
            norm_text(row.get("title")),
            norm_text(row.get("dedupKey")),
        )
    )
    location_divergence_rows.sort(
        key=lambda row: (
            -int(row.get("distinctLocationCount") or 0),
            -int(row.get("sourceBundleCount") or 0),
            norm_text(row.get("company")),
            norm_text(row.get("title")),
            norm_text(row.get("dedupKey")),
        )
    )
    action_order = {action: index for index, action in enumerate(REVIEW_QUEUE_ACTION_KEYS)}
    review_queue_rows.sort(
        key=lambda row: (
            action_order.get(str(row.get("recommendedReviewAction") or ""), len(action_order)),
            -int(row.get("sourceBundleCount") or 0),
            norm_text(row.get("company")),
            norm_text(row.get("title")),
            norm_text(row.get("dedupKey")),
        )
    )
    carried_bundle_rows.sort(
        key=lambda row: (
            -int(row.get("sourceBundleCount") or 0),
            norm_text(row.get("company")),
            norm_text(row.get("title")),
            norm_text(row.get("dedupKey")),
        )
    )
    provider_static_title_company_collision_rows = [
        row
        for row in provider_static_disagreement_rows
        if row.get("disagreementClassification") == "title_company_collision"
    ]
    repeated_countryless_tokens = _company_countryless_location_token_counts(
        provider_static_title_company_collision_rows
    )
    provider_static_disagreement_rows = [
        {
            **row,
            **(
                {
                    "carriedLocationPollutionAudit": audit,
                    "carriedLocationPollutionEvidence": evidence,
                }
                if clean_text(row.get("disagreementClassification")) == "title_company_collision"
                else {
                    "carriedLocationPollutionAudit": "",
                    "carriedLocationPollutionEvidence": [],
                }
            ),
        }
        for row in provider_static_disagreement_rows
        for audit, evidence in [
            _provider_static_title_company_collision_audit(row, repeated_countryless_tokens)
            if clean_text(row.get("disagreementClassification")) == "title_company_collision"
            else ("", [])
        ]
    ]
    provider_static_disagreement_rows = [
        _provider_static_row_with_gate_fields(row, review_state or {})
        for row in provider_static_disagreement_rows
    ]
    disposition_order = {"blocked": 0, "warning": 1}
    provider_static_disagreement_rows.sort(
        key=lambda row: (
            disposition_order.get(clean_text(row.get("disagreementGateDisposition")), 9),
            norm_text(row.get("bundleEvidenceOrigin")),
            norm_text(row.get("company")),
            norm_text(row.get("title")),
            norm_text(row.get("dedupKey")),
        )
    )
    provider_static_title_company_collision_rows = [
        row
        for row in provider_static_disagreement_rows
        if row.get("disagreementClassification") == "title_company_collision"
    ]
    title_company_current_run_count = sum(
        1
        for row in provider_static_title_company_collision_rows
        if row.get("bundleEvidenceOrigin") == "current_run"
    )
    title_company_carried_count = sum(
        1
        for row in provider_static_title_company_collision_rows
        if row.get("bundleEvidenceOrigin") != "current_run"
    )
    provider_static_title_company_collision_audit_counts = Counter(
        clean_text(row.get("carriedLocationPollutionAudit")) or "unknown"
        for row in provider_static_title_company_collision_rows
    )
    provider_static_disagreement_gate_counts = Counter(
        clean_text(row.get("disagreementGateDisposition")) or "blocked"
        for row in provider_static_disagreement_rows
    )
    current_run_provider_static_disagreement_blocked_count = sum(
        1
        for row in provider_static_disagreement_rows
        if row.get("bundleEvidenceOrigin") == "current_run"
        and clean_text(row.get("disagreementGateDisposition")) == "blocked"
    )
    carried_provider_static_disagreement_blocked_count = sum(
        1
        for row in provider_static_disagreement_rows
        if row.get("bundleEvidenceOrigin") != "current_run"
        and clean_text(row.get("disagreementGateDisposition")) == "blocked"
    )
    carried_provider_static_disagreement_warning_count = sum(
        1
        for row in provider_static_disagreement_rows
        if row.get("bundleEvidenceOrigin") != "current_run"
        and clean_text(row.get("disagreementGateDisposition")) == "warning"
    )
    provider_static_auto_safe_warning_count = sum(
        1
        for row in provider_static_disagreement_rows
        if clean_text(row.get("disagreementGateDisposition")) == "warning"
        and any(
            clean_text(item).startswith("auto_safe_")
            for item in row.get("disagreementGateEvidence") or []
        )
    )
    provider_static_reviewed_safe_warning_count = sum(
        1
        for row in provider_static_disagreement_rows
        if clean_text(row.get("dedupReviewStatus")) == "reviewed_safe"
        and clean_text(row.get("disagreementGateDisposition")) == "warning"
    )
    provider_static_confirmed_blocking_count = sum(
        1
        for row in provider_static_disagreement_rows
        if clean_text(row.get("dedupReviewStatus")) == "confirmed_blocking"
        and clean_text(row.get("disagreementGateDisposition")) == "blocked"
    )
    provider_static_location_pollution_warning_count = sum(
        1
        for row in provider_static_title_company_collision_rows
        if clean_text(row.get("carriedLocationPollutionAudit")) == "carried_location_pollution"
        and clean_text(row.get("disagreementGateDisposition")) == "warning"
    )
    provider_static_disagreement_count = (
        current_run_provider_static_disagreement_count + carried_provider_static_disagreement_count
    )
    sheet_guard_reason_counts = (
        dedup_stats.get("sheetRoleBucketGuardBlockedReasonCounts")
        or dedup_stats.get("googleSheetsGenericRoleGuardBlockedReasonCounts")
        or {}
    )
    google_sheets_guard_samples = json_object_rows(
        dedup_stats.get("sheetRoleBucketGuardBlockedSamples")
        or dedup_stats.get("googleSheetsGenericRoleGuardBlockedSamples")
    )
    google_sheets_guard_blocked_count = max(
        0,
        int(
            dedup_stats.get("sheetRoleBucketGuardBlockedCount")
            or dedup_stats.get("googleSheetsGenericRoleGuardBlockedCount")
            or 0
        ),
    )
    google_sheets_role_bucket_audit = _google_sheets_role_bucket_audit_summary(
        role_bucket_rows=google_sheets_role_bucket_rows,
        guard_samples=google_sheets_guard_samples,
        guard_blocked_count=google_sheets_guard_blocked_count,
        limit=risky_limit,
    )
    merge_reason_counts = _merge_reason_counts(dedup_stats)
    has_non_primary_tier_counts = (
        "currentRunBlockingNonPrimaryMergeReasonCounts" in dedup_stats
        or "currentRunMonitorNonPrimaryMergeReasonCounts" in dedup_stats
    )
    blocking_non_primary_reason_counts = (
        dedup_stats.get("currentRunBlockingNonPrimaryMergeReasonCounts") or {}
    )
    monitor_non_primary_reason_counts = (
        dedup_stats.get("currentRunMonitorNonPrimaryMergeReasonCounts") or {}
    )
    if not has_non_primary_tier_counts:
        blocking_non_primary_reason_counts = _current_run_non_primary_merge_counts(
            merge_reason_counts
        )
    merge_gate_tier_counts = dedup_stats.get("currentRunMergeGateTierCounts") or {}

    payload = {
        "schemaVersion": 1,
        "mergedCount": max(0, int(dedup_stats.get("mergedCount") or 0)),
        "collisionSamplesCount": max(0, int(dedup_stats.get("collisionSamplesCount") or 0)),
        "mergeReasonCounts": merge_reason_counts,
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
        "currentRunBlockingNonPrimaryMergeReasonCounts": {
            key: int(blocking_non_primary_reason_counts.get(key, 0))
            for key in ("secondaryKey", "sparseIdentity", "socialKey", "unknown")
        },
        "currentRunMonitorNonPrimaryMergeReasonCounts": {
            key: int(monitor_non_primary_reason_counts.get(key, 0))
            for key in ("secondaryKey", "sparseIdentity", "socialKey", "unknown")
        },
        "currentRunMergeExamples": _current_run_merge_examples(dedup_stats),
        "currentRunMergeExamplesByReason": _current_run_merge_examples_by_reason(dedup_stats),
        "currentRunBlockingMergeExamplesByReason": (
            _current_run_blocking_merge_examples_by_reason(dedup_stats)
        ),
        "sheetRoleBucketGuardBlockedCount": google_sheets_guard_blocked_count,
        "sheetRoleBucketGuardBlockedReasonCounts": {
            "secondaryKey": max(0, int(sheet_guard_reason_counts.get("secondaryKey") or 0)),
            "sparseIdentity": max(0, int(sheet_guard_reason_counts.get("sparseIdentity") or 0)),
        },
        "sheetRoleBucketGuardBlockedSamples": google_sheets_guard_samples,
        "googleSheetsGenericRoleGuardBlockedCount": google_sheets_guard_blocked_count,
        "googleSheetsGenericRoleGuardBlockedReasonCounts": {
            "secondaryKey": max(0, int(sheet_guard_reason_counts.get("secondaryKey") or 0)),
            "sparseIdentity": max(0, int(sheet_guard_reason_counts.get("sparseIdentity") or 0)),
        },
        "googleSheetsGenericRoleGuardBlockedSamples": google_sheets_guard_samples,
        "sourceBundleCollisionCount": source_bundle_collision_count,
        "currentRunSourceBundleCollisionCount": current_run_source_bundle_collision_count,
        "carriedSourceBundleCollisionCount": carried_source_bundle_collision_count,
        "currentRunHighRiskReviewQueueCount": current_run_high_risk_review_queue_count,
        "carriedHighRiskReviewQueueCount": carried_high_risk_review_queue_count,
        "currentRunBlockingReviewQueueCount": current_run_blocking_review_queue_count,
        "carriedBlockingReviewQueueCount": carried_blocking_review_queue_count,
        "currentRunMonitorReviewQueueCount": current_run_monitor_review_queue_count,
        "carriedMonitorReviewQueueCount": carried_monitor_review_queue_count,
        "providerStaticDisagreementCounts": {
            "total": provider_static_disagreement_count,
            "currentRun": current_run_provider_static_disagreement_count,
            "carried": carried_provider_static_disagreement_count,
        },
        "providerStaticDisagreementGateCounts": {
            "blocked": int(provider_static_disagreement_gate_counts.get("blocked", 0)),
            "warning": int(provider_static_disagreement_gate_counts.get("warning", 0)),
            "currentRunBlocked": current_run_provider_static_disagreement_blocked_count,
            "carriedBlocked": carried_provider_static_disagreement_blocked_count,
            "carriedWarning": carried_provider_static_disagreement_warning_count,
            "autoSafeWarning": provider_static_auto_safe_warning_count,
            "locationPollutionWarning": provider_static_location_pollution_warning_count,
            "reviewedSafeWarning": provider_static_reviewed_safe_warning_count,
            "confirmedBlocking": provider_static_confirmed_blocking_count,
        },
        "providerStaticDisagreementClassificationCounts": {
            key: int(provider_static_disagreement_classification_counts.get(key, 0))
            for key in PROVIDER_STATIC_DISAGREEMENT_CLASSIFICATION_KEYS
        },
        "providerStaticTitleCompanyCollisionCounts": {
            "total": len(provider_static_title_company_collision_rows),
            "currentRun": title_company_current_run_count,
            "carried": title_company_carried_count,
        },
        "providerStaticTitleCompanyCollisionAuditCounts": {
            key: int(provider_static_title_company_collision_audit_counts.get(key, 0))
            for key in PROVIDER_STATIC_TITLE_COMPANY_COLLISION_AUDIT_KEYS
        },
        "sourceBundleComposition": {
            key: int(composition.get(key, 0)) for key in ("provider", "static", "social", "other")
        },
        "riskReasonCounts": {
            key: int(risk_reason_counts.get(key, 0))
            for key in (
                "same_title_company_different_location",
                "provider_static_duplicate_disagreement",
                "missing_provider_ids",
                "weak_title_company_only_evidence",
            )
        },
        "outlierReasonCounts": {
            key: int(outlier_reason_counts.get(key, 0)) for key in OUTLIER_REASON_KEYS
        },
        "identityShapeCounts": {
            key: int(identity_shape_counts.get(key, 0)) for key in IDENTITY_SHAPE_KEYS
        },
        "identityQualityCounts": {
            key: int(identity_quality_counts.get(key, 0)) for key in IDENTITY_QUALITY_KEYS
        },
        "nonProviderIdentityProvenanceCounts": {
            key: int(non_provider_identity_provenance_counts.get(key, 0))
            for key in NON_PROVIDER_IDENTITY_PROVENANCE_KEYS
        },
        "googleSheetsBundleShapeCounts": {
            key: int(google_sheets_bundle_shape_counts.get(key, 0))
            for key in GOOGLE_SHEETS_BUNDLE_SHAPE_KEYS
        },
        "googleSheetsRoleBucketAuditCounts": {
            key: int(google_sheets_role_bucket_audit_counts.get(key, 0))
            for key in GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_KEYS
        },
        "googleSheetsBucketIntentCounts": {
            key: int(google_sheets_bucket_intent_counts.get(key, 0))
            for key in GOOGLE_SHEETS_BUCKET_INTENT_KEYS
        },
        "googleSheetsWeakGroupingAuditCounts": {
            key: int(google_sheets_weak_grouping_audit_counts.get(key, 0))
            for key in GOOGLE_SHEETS_WEAK_GROUPING_AUDIT_KEYS
        },
        "googleSheetsRoleBucketAudit": google_sheets_role_bucket_audit,
        "reviewQueueCounts": {
            key: int(review_queue_counts.get(key, 0)) for key in REVIEW_QUEUE_ACTION_KEYS
        },
        "reviewQueueCauseCounts": {
            key: int(review_queue_cause_counts.get(key, 0)) for key in REVIEW_QUEUE_CAUSE_KEYS
        },
        "currentRunBlockingReviewQueueCauseCounts": {
            key: int(current_run_blocking_review_queue_cause_counts.get(key, 0))
            for key in REVIEW_QUEUE_CAUSE_KEYS
        },
        "carriedBlockingReviewQueueCauseCounts": {
            key: int(carried_blocking_review_queue_cause_counts.get(key, 0))
            for key in REVIEW_QUEUE_CAUSE_KEYS
        },
        "currentRunMonitorReviewQueueCauseCounts": {
            key: int(current_run_monitor_review_queue_cause_counts.get(key, 0))
            for key in REVIEW_QUEUE_CAUSE_KEYS
        },
        "carriedMonitorReviewQueueCauseCounts": {
            key: int(carried_monitor_review_queue_cause_counts.get(key, 0))
            for key in REVIEW_QUEUE_CAUSE_KEYS
        },
        "reviewQueue": review_queue_rows[: max(0, int(risky_limit))],
        "providerStaticDisagreementExamples": _limit_provider_static_examples(
            provider_static_disagreement_rows, risky_limit
        ),
        "providerStaticTitleCompanyCollisionExamples": (
            _limit_provider_static_examples(
                provider_static_title_company_collision_rows, risky_limit
            )
        ),
        "carriedBundleExamples": carried_bundle_rows[: max(0, int(risky_limit))],
        "carriedBundleReconciliationRecommendation": {
            "recommendedAction": "rebuild_carried_source_bundle_metadata",
            "destructiveActionAllowed": False,
            "requiresExplicitMaintenanceRun": True,
        }
        if carried_source_bundle_collision_count
        else {},
        "topMergedJobs": top_rows[: max(0, int(top_limit))],
        "topSourceBundleOutliers": top_rows[: max(0, int(top_limit))],
        "locationDivergenceExamples": location_divergence_rows[: max(0, int(risky_limit))],
        "riskyMergeExamples": risky_rows[: max(0, int(risky_limit))],
        "riskyMergeExampleCount": len(risky_rows),
    }
    payload["dedupAuditGate"] = build_dedup_audit_gate(payload)
    return cast(DedupEvidencePayload, payload)
