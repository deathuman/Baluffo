"""Review queue helpers for dedup evidence.

Extracted from reporting_dedup_evidence.py as part of the dedup evidence split.

AI boundary owns: review queue rows, suspected cause, and job summary helpers.
AI boundary implement in: this file for review queue evidence; audit gate and bundle report stay in payload/bundle leaves.
AI boundary search before contracts: dedup evidence bundle, google sheets, and provider/static evidence.
AI boundary verify: `npm run lint:repo-guardrails` plus focused dedup evidence tests.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from src.jobs.common.dedup_evidence_bundle import (
    _dominant_source_class,
    _has_any_strong_identity,
    _identity_caveats,
    _identity_quality,
    _identity_quality_evidence,
    _identity_shape,
    _meaningful_locations,
    _non_provider_identity_evidence,
    _non_provider_identity_provenance,
    _non_provider_source_job_id_count,
    _outlier_reason,
    _provider_source_job_id_count,
    _shared_primary_url,
    _shared_url,
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
    _google_sheets_bucket_intent,
    _google_sheets_bucket_intent_evidence,
    _google_sheets_bundle_evidence,
    _google_sheets_bundle_shape,
    _google_sheets_role_bucket_audit,
    _google_sheets_role_bucket_audit_evidence,
    _google_sheets_weak_grouping_audit,
    _google_sheets_weak_grouping_evidence,
)
from src.jobs.text_utils import clean_text, normalize_url


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
