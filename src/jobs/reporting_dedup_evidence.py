"""Read-only deduplication evidence for fetch reports.

AI boundary owns: dedup evidence report rows, source overlap summaries, and read-only fetch-report diagnostics.
AI boundary implement in: this file for evidence presentation; dedup policy and canonical identity stay in dedup/canonicalize.
AI boundary search before contracts: fetch-report contracts, bridge report normalization, and dedup evidence tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused dedup evidence tests.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any, cast

from src.jobs.common.contracts_dedup_evidence import (
    DedupAuditGatePayload,
    DedupEvidencePayload,
)
from src.jobs.common.dedup_evidence_audit_gate import (
    _audit_gate_blockers_and_warnings,
    _audit_gate_examples,
    _audit_gate_high_risk_count,
    _audit_gate_provider_static_disagreement_count,
    _build_audit_gate_details,
)
from src.jobs.common.dedup_evidence_bundle import (
    IDENTITY_QUALITY_KEYS,
    IDENTITY_SHAPE_KEYS,
    NON_PROVIDER_IDENTITY_PROVENANCE_KEYS,
    OUTLIER_REASON_KEYS,
    _dominant_source_class,
    _has_any_strong_identity,
    _identity_caveats,
    _identity_quality,
    _identity_quality_evidence,
    _identity_shape,
    _limit_provider_static_examples,
    _meaningful_locations,
    _non_provider_identity_evidence,
    _non_provider_identity_provenance,
    _non_provider_source_job_id_count,
    _outlier_reason,
    _payload,
    _provider_source_job_id_count,
    _risky_reasons,
    _shared_primary_url,
    _shared_url,
    _source_bundle,
    _source_class,
    _source_class_counts,
    _title_company_pollution_signals,
    _title_shape,
    _unique_job_link_count,
    _unique_url_host_count,
    _unique_url_path_prefix_count,
    _url_host,
    _url_path,
)
from src.jobs.common.dedup_evidence_google_sheets import (
    GOOGLE_SHEETS_BUCKET_INTENT_KEYS,
    GOOGLE_SHEETS_BUNDLE_SHAPE_KEYS,
    GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_KEYS,
    GOOGLE_SHEETS_WEAK_GROUPING_AUDIT_KEYS,
    _google_sheets_bucket_intent,
    _google_sheets_bucket_intent_evidence,
    _google_sheets_bundle_evidence,
    _google_sheets_bundle_shape,
    _google_sheets_role_bucket_audit,
    _google_sheets_role_bucket_audit_evidence,
    _google_sheets_role_bucket_audit_summary,
    _google_sheets_weak_grouping_audit,
    _google_sheets_weak_grouping_evidence,
    _is_google_sheets_role_bucket_summary,
)
from src.jobs.common.dedup_evidence_merge import (
    REVIEW_QUEUE_CAUSE_KEYS,
    _current_run_blocking_merge_examples_by_reason,
    _current_run_merge_examples,
    _current_run_merge_examples_by_reason,
    _current_run_non_primary_merge_counts,
    _mapping_value,
    _merge_reason_counts,
    _nonzero_counts,
    _review_cause_counts_by_key,
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
from src.jobs.models import CanonicalJob
from src.jobs.text_utils import clean_text, norm_text, normalize_url
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


def _is_weak_non_provider_review_summary(summary: Mapping[str, Any]) -> bool:
    if str(summary.get("outlierReason") or "") == "provider_static_disagreement":
        return False
    dominant_source_class = str(summary.get("dominantSourceClass") or "")
    if dominant_source_class in {"provider", "social"}:
        return False
    if max(0, int(summary.get("providerSourceJobIdCount") or 0)) > 0:
        return False
    identity_quality = str(summary.get("identityQuality") or "")
    if identity_quality == "provider_id_strong":
        return False
    cause = str(summary.get("suspectedCause") or "")
    if cause in {
        "category_or_department_bucket",
        "google_sheets_role_bucket_needs_review",
        "listing_page_bundle",
        "non_provider_url_identity_needs_review",
        "parser_or_directory_text_pollution",
        "spreadsheet_role_bucket_needs_review",
    }:
        return True
    if cause == "unknown" and identity_quality in {
        "missing_identity",
        "many_urls_many_hosts_weak",
        "many_urls_same_host_weak",
        "other_source_id_untrusted",
        "shared_listing_url_weak",
    }:
        return True
    return False


def _recommended_review_action(summary: Mapping[str, Any]) -> str:
    caveats = {str(caveat) for caveat in summary.get("identityCaveats") or []}
    identity_shape = str(summary.get("identityShape") or "")
    title_shape = str(summary.get("titleShape") or "")
    outlier_reason = str(summary.get("outlierReason") or "")
    if outlier_reason == "provider_static_disagreement":
        return "review_provider_static_disagreement"
    if _is_weak_non_provider_review_summary(summary):
        return "monitor"
    if identity_shape == "shared_listing_or_category_url":
        return "review_listing_url_bundle"
    if title_shape == "category_like" or "category_like_title" in caveats:
        return "review_category_title_bundle"
    if (
        title_shape == "speculative_or_open_application"
        or "speculative_or_open_application_title" in caveats
    ):
        return "review_open_application_bundle"
    if identity_shape == "many_unique_urls_same_title":
        return "review_many_urls_same_title"
    return "monitor"


def _should_include_review_queue_row(summary: Mapping[str, Any], review_action: str) -> bool:
    if review_action != "monitor":
        return True
    cause = str(summary.get("suspectedCause") or "")
    if cause in {
        "category_or_department_bucket",
        "google_sheets_role_bucket_needs_review",
        "listing_page_bundle",
        "non_provider_url_identity_needs_review",
        "parser_or_directory_text_pollution",
        "spreadsheet_role_bucket_needs_review",
    }:
        return True
    identity_quality = str(summary.get("identityQuality") or "")
    return cause == "unknown" and identity_quality in {
        "missing_identity",
        "many_urls_many_hosts_weak",
        "many_urls_same_host_weak",
        "other_source_id_untrusted",
        "shared_listing_url_weak",
    }


def _suspected_cause(summary: Mapping[str, Any]) -> str:
    caveats = {str(caveat) for caveat in summary.get("identityCaveats") or []}
    pollution = {str(signal) for signal in summary.get("titleCompanyPollutionSignals") or []}
    identity_shape = str(summary.get("identityShape") or "")
    identity_quality = str(summary.get("identityQuality") or "")
    title_shape = str(summary.get("titleShape") or "")
    outlier_reason = str(summary.get("outlierReason") or "")
    dominant_source_class = str(summary.get("dominantSourceClass") or "")
    provenance = str(summary.get("nonProviderIdentityProvenance") or "")
    google_sheets_shape = str(summary.get("googleSheetsBundleShape") or "")
    google_sheets_audit = str(summary.get("googleSheetsRoleBucketAudit") or "")
    if outlier_reason == "provider_static_disagreement":
        return "provider_static_disagreement"
    if title_shape == "speculative_or_open_application":
        return "open_application_family"
    if provenance == "google_sheets_row_identity" and google_sheets_shape == "role_category_bucket":
        return "spreadsheet_role_bucket_needs_review"
    if google_sheets_audit in {
        "listing_or_search_url_bucket",
        "parser_normalized_role_title",
        "role_family_needs_manual_review",
    }:
        return "google_sheets_role_bucket_needs_review"
    if title_shape == "category_like" or "category_like_title" in caveats:
        return "category_or_department_bucket"
    if pollution and dominant_source_class == "other":
        return "parser_or_directory_text_pollution"
    if identity_shape == "shared_listing_or_category_url":
        return "listing_page_bundle"
    if (
        dominant_source_class == "other"
        and identity_shape == "many_unique_urls_same_title"
        and identity_quality
        in {
            "many_urls_same_host_weak",
            "many_urls_many_hosts_weak",
            "other_source_id_untrusted",
        }
    ):
        return "non_provider_url_identity_needs_review"
    if identity_quality in {"provider_id_strong", "shared_detail_url_strong"} and (
        identity_shape == "provider_id_backed" or outlier_reason == "multi_location_strong_identity"
    ):
        return "likely_legitimate_multi_role_family"
    return "unknown"


def _google_sheets_cause_evidence(summary: Mapping[str, Any]) -> list[str]:
    fields = (
        ("google_sheets_shape", "googleSheetsBundleShape", {"", "not_google_sheets", "unknown"}),
        (
            "google_sheets_audit",
            "googleSheetsRoleBucketAudit",
            {"", "not_google_sheets_role_bucket", "unknown"},
        ),
        (
            "google_sheets_intent",
            "googleSheetsBucketIntent",
            {"", "not_google_sheets_bucket", "unknown"},
        ),
        (
            "google_sheets_weak_audit",
            "googleSheetsWeakGroupingAudit",
            {"", "not_weak_google_sheets_grouping", "unknown"},
        ),
    )
    evidence: list[str] = []
    for label, key, ignored in fields:
        value = str(summary.get(key) or "")
        if value not in ignored:
            evidence.append(f"{label}:{value}")
    return evidence


def _cause_evidence(summary: Mapping[str, Any]) -> list[str]:
    evidence: list[str] = []
    for label, key in (
        ("cause", "suspectedCause"),
        ("identity", "identityShape"),
        ("quality", "identityQuality"),
        ("title", "titleShape"),
        ("outlier", "outlierReason"),
        ("dominant_source", "dominantSourceClass"),
    ):
        value = str(summary.get(key) or "")
        if value:
            evidence.append(f"{label}:{value}")
    evidence.extend(_google_sheets_cause_evidence(summary))
    if summary.get("hasStrongIdentity"):
        evidence.append("strong_identity")
    for signal in summary.get("titleCompanyPollutionSignals") or []:
        evidence.append(f"pollution:{signal}")
    provenance = str(summary.get("nonProviderIdentityProvenance") or "")
    if provenance:
        evidence.append(f"provenance:{provenance}")
    for caveat in summary.get("identityCaveats") or []:
        evidence.append(f"caveat:{caveat}")
    return evidence[:10]


def _job_summary(row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    source_names = sorted(
        {clean_text(item.get("source")) for item in bundle if clean_text(item.get("source"))}
    )
    source_classes = _source_class_counts(bundle)
    meaningful_locations = _meaningful_locations(row)
    shared_url = _shared_url(bundle)
    summary = {
        "id": clean_text(row.get("id")),
        "dedupKey": clean_text(row.get("dedupKey")),
        "title": clean_text(row.get("title")),
        "company": clean_text(row.get("company")),
        "jobLink": normalize_url(row.get("jobLink")),
        "locationSummary": clean_text(row.get("locationSummary")),
        "sourceBundleCount": max(int(row.get("sourceBundleCount") or 0), len(bundle)),
        "sourceClasses": source_classes,
        "sources": source_names[:8],
        "sampleSources": source_names[:8],
        "outlierReason": _outlier_reason(row, bundle),
        "distinctLocationCount": len(meaningful_locations),
        "sampleLocations": meaningful_locations[:5],
        "uniqueJobLinkCount": _unique_job_link_count(bundle),
        "sharedPrimaryUrl": _shared_primary_url(bundle),
        "sharedUrlHost": _url_host(shared_url) if shared_url else "",
        "sharedUrlPath": _url_path(shared_url) if shared_url else "",
        "uniqueUrlHostCount": _unique_url_host_count(bundle),
        "uniqueUrlPathPrefixCount": _unique_url_path_prefix_count(bundle),
        "urlHostDiversity": _unique_url_host_count(bundle),
        "urlPathPrefixDiversity": _unique_url_path_prefix_count(bundle),
        "providerSourceJobIdCount": _provider_source_job_id_count(bundle),
        "nonProviderSourceJobIdCount": _non_provider_source_job_id_count(bundle),
        "hasStrongIdentity": _has_any_strong_identity(bundle),
        "dominantSourceClass": _dominant_source_class(source_classes),
        "identityShape": _identity_shape(row, bundle),
        "identityQuality": _identity_quality(row, bundle),
        "identityQualityEvidence": _identity_quality_evidence(row, bundle),
        "nonProviderIdentityProvenance": _non_provider_identity_provenance(row, bundle),
        "nonProviderIdentityEvidence": _non_provider_identity_evidence(row, bundle),
        "googleSheetsBundleShape": _google_sheets_bundle_shape(row, bundle),
        "googleSheetsBundleEvidence": _google_sheets_bundle_evidence(row, bundle),
        "googleSheetsRoleBucketAudit": _google_sheets_role_bucket_audit(row, bundle),
        "googleSheetsRoleBucketAuditEvidence": _google_sheets_role_bucket_audit_evidence(
            row, bundle
        ),
        "googleSheetsBucketIntent": _google_sheets_bucket_intent(row, bundle),
        "googleSheetsBucketIntentEvidence": _google_sheets_bucket_intent_evidence(row, bundle),
        "googleSheetsWeakGroupingAudit": _google_sheets_weak_grouping_audit(row, bundle),
        "googleSheetsWeakGroupingEvidence": _google_sheets_weak_grouping_evidence(row, bundle),
        "titleShape": _title_shape(row),
        "identityCaveats": _identity_caveats(row, bundle),
        "titleCompanyPollutionSignals": _title_company_pollution_signals(row),
    }
    summary["suspectedCause"] = _suspected_cause(summary)
    summary["causeEvidence"] = _cause_evidence(summary)
    return summary


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
