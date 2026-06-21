"""Google Sheets role-bucket audit helpers for dedup evidence.

Extracted from reporting_dedup_evidence.py as part of the dedup evidence split.

AI boundary owns: Google Sheets dedup evidence buckets, examples, and audit support rows.
AI boundary implement in: this file for Google Sheets evidence only; generic dedup policy stays in dedup/canonicalize.
AI boundary search before contracts: dedup evidence bundle, Google Sheets fetcher tests, and quality audits.
AI boundary verify: `npm run lint:repo-guardrails` plus focused Google Sheets evidence tests.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

from src.jobs.common.contracts_dedup_evidence import (
    DedupMergeExampleRow,
    DedupReviewQueueRow,
    GoogleSheetsRoleBucketAuditPayload,
)
from src.jobs.common.dedup_evidence_bundle import (
    _company_tokens,
    _has_generic_role_bucket_title,
    _identity_caveats,
    _identity_shape,
    _looks_role_bucket_title,
    _meaningful_locations,
    _non_provider_identity_provenance,
    _non_provider_source_job_id_count,
    _non_provider_source_job_id_values,
    _path_tokens,
    _provider_source_job_id_count,
    _sample_url_paths,
    _sheet_row_indexes,
    _sheet_row_span,
    _source_job_id_prefix,
    _source_job_id_shape,
    _title_company_pollution_signals,
    _title_shape,
    _title_tokens,
    _unique_job_link_count,
    _unique_url_host_count,
    _unique_url_path_prefix_count,
    _url_path_looks_listing_or_search,
    _url_paths_look_job_detail,
)
from src.jobs.text_utils import clean_text, norm_text, normalize_url

GOOGLE_SHEETS_BUNDLE_SHAPE_KEYS = (
    "role_category_bucket",
    "company_role_family",
    "single_location_many_urls",
    "multi_location_many_urls",
    "spreadsheet_row_collision",
    "not_google_sheets",
    "unknown",
)
GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_KEYS = (
    "likely_spreadsheet_category_bucket",
    "role_family_needs_manual_review",
    "job_detail_urls_same_role",
    "listing_or_search_url_bucket",
    "parser_normalized_role_title",
    "not_google_sheets_role_bucket",
    "unknown",
)
GOOGLE_SHEETS_BUCKET_INTENT_KEYS = (
    "likely_spreadsheet_taxonomy_bucket",
    "possible_role_family",
    "weak_title_company_grouping",
    "listing_or_search_bucket",
    "parser_normalized_bucket",
    "not_google_sheets_bucket",
    "unknown",
)
GOOGLE_SHEETS_WEAK_GROUPING_AUDIT_KEYS = (
    "role_bucket_detail_url_grouping",
    "role_bucket_listing_grouping",
    "single_token_title_many_urls",
    "two_token_title_many_urls",
    "concrete_title_many_urls",
    "parser_pollution_grouping",
    "not_weak_google_sheets_grouping",
    "unknown",
)
GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_CLASSIFICATION_KEYS = (
    "fixed_by_generic_role_guard",
    "allowed_same_primary_url",
    "historical_carried_bundle",
    "unresolved_current_run_role_bucket",
    "parser_or_sheet_category_noise",
    "needs_narrow_dedup_guard",
)


def _google_sheets_bundle_shape(row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]) -> str:
    if _non_provider_identity_provenance(row, bundle) != "google_sheets_row_identity":
        return "not_google_sheets"
    ids = _non_provider_source_job_id_values(bundle)
    if not bundle:
        return "unknown"
    if len(ids) != len(set(ids)):
        return "spreadsheet_row_collision"
    if _title_shape(row) == "category_like" or _looks_role_bucket_title(row):
        return "role_category_bucket"
    if _unique_job_link_count(bundle) > 1 and len(_meaningful_locations(row)) > 1:
        return "multi_location_many_urls"
    if _unique_job_link_count(bundle) > 1:
        return "single_location_many_urls"
    if len(bundle) > 1:
        return "company_role_family"
    return "unknown"


def _google_sheets_role_bucket_audit(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> str:
    if _non_provider_identity_provenance(row, bundle) != "google_sheets_row_identity":
        return "not_google_sheets_role_bucket"
    if _google_sheets_bundle_shape(row, bundle) not in {
        "role_category_bucket",
        "single_location_many_urls",
        "multi_location_many_urls",
        "company_role_family",
    }:
        return "not_google_sheets_role_bucket"
    if _title_company_pollution_signals(row):
        return "parser_normalized_role_title"
    paths = _sample_url_paths(bundle)
    if paths and all(_url_path_looks_listing_or_search(path) for path in paths):
        return "listing_or_search_url_bucket"
    if (
        _url_paths_look_job_detail(bundle)
        and _google_sheets_bundle_shape(row, bundle) == "role_category_bucket"
    ):
        return "job_detail_urls_same_role"
    if _has_generic_role_bucket_title(row):
        return "likely_spreadsheet_category_bucket"
    if _unique_job_link_count(bundle) > 1:
        return "role_family_needs_manual_review"
    return "unknown"


def _google_sheets_role_bucket_audit_evidence(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> list[str]:
    ids = _non_provider_source_job_id_values(bundle)
    paths = _sample_url_paths(bundle)
    path_tokens = sorted({token for path in paths for token in _path_tokens(path)})
    evidence = [
        f"audit:{_google_sheets_role_bucket_audit(row, bundle)}",
        f"shape:{_google_sheets_bundle_shape(row, bundle)}",
        f"source_count:{len(bundle)}",
        f"unique_urls:{_unique_job_link_count(bundle)}",
        f"locations:{len(_meaningful_locations(row))}",
        f"title_tokens:{len(_title_tokens(row))}",
        f"company_tokens:{len(_company_tokens(row))}",
        f"path_tokens:{len(path_tokens)}",
    ]
    if paths:
        evidence.append(
            "paths_listing_or_search"
            if all(_url_path_looks_listing_or_search(path) for path in paths)
            else "paths_job_detail_like"
            if _url_paths_look_job_detail(bundle)
            else "paths_mixed_or_unclear"
        )
    for signal in _title_company_pollution_signals(row):
        evidence.append(f"pollution:{signal}")
    for token in _title_tokens(row)[:4]:
        evidence.append(f"title_token:{token}")
    for token in path_tokens[:4]:
        evidence.append(f"path_token:{token}")
    for shape in sorted({_source_job_id_shape(value) for value in ids})[:3]:
        evidence.append(f"id_shape:{shape}")
    return evidence[:16]


def _google_sheets_bucket_intent(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> str:
    if _non_provider_identity_provenance(row, bundle) != "google_sheets_row_identity":
        return "not_google_sheets_bucket"
    audit = _google_sheets_role_bucket_audit(row, bundle)
    shape = _google_sheets_bundle_shape(row, bundle)
    if audit == "parser_normalized_role_title":
        return "parser_normalized_bucket"
    if audit == "listing_or_search_url_bucket":
        return "listing_or_search_bucket"
    if shape == "role_category_bucket" or _has_generic_role_bucket_title(row):
        return "likely_spreadsheet_taxonomy_bucket"
    if audit == "role_family_needs_manual_review" and len(_title_tokens(row)) <= 2:
        return "weak_title_company_grouping"
    if audit == "role_family_needs_manual_review" or shape in {
        "company_role_family",
        "single_location_many_urls",
        "multi_location_many_urls",
    }:
        return "possible_role_family"
    if (
        _identity_shape(row, bundle) == "many_unique_urls_same_title"
        and _provider_source_job_id_count(bundle) == 0
        and _non_provider_source_job_id_count(bundle) > 0
    ):
        return "weak_title_company_grouping"
    return "unknown"


def _google_sheets_bucket_intent_evidence(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> list[str]:
    paths = _sample_url_paths(bundle)
    evidence = [
        f"intent:{_google_sheets_bucket_intent(row, bundle)}",
        f"shape:{_google_sheets_bundle_shape(row, bundle)}",
        f"audit:{_google_sheets_role_bucket_audit(row, bundle)}",
        f"source_count:{len(bundle)}",
        f"unique_urls:{_unique_job_link_count(bundle)}",
        f"url_hosts:{_unique_url_host_count(bundle)}",
        f"url_path_prefixes:{_unique_url_path_prefix_count(bundle)}",
        f"locations:{len(_meaningful_locations(row))}",
        f"title_tokens:{len(_title_tokens(row))}",
        f"company_tokens:{len(_company_tokens(row))}",
    ]
    if paths:
        evidence.append(
            "paths_listing_or_search"
            if all(_url_path_looks_listing_or_search(path) for path in paths)
            else "paths_job_detail_like"
            if _url_paths_look_job_detail(bundle)
            else "paths_mixed_or_unclear"
        )
    if _looks_role_bucket_title(row):
        evidence.append("role_bucket_title")
    if _has_generic_role_bucket_title(row):
        evidence.append("generic_role_bucket_title")
    for signal in _title_company_pollution_signals(row):
        evidence.append(f"pollution:{signal}")
    for token in _title_tokens(row)[:4]:
        evidence.append(f"title_token:{token}")
    return evidence[:16]


def _google_sheets_weak_grouping_audit(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> str:
    if _non_provider_identity_provenance(row, bundle) != "google_sheets_row_identity":
        return "not_weak_google_sheets_grouping"
    if _title_company_pollution_signals(row):
        return "parser_pollution_grouping"
    paths = _sample_url_paths(bundle)
    if paths and all(_url_path_looks_listing_or_search(path) for path in paths):
        return "role_bucket_listing_grouping"
    if _url_paths_look_job_detail(bundle) and (
        _looks_role_bucket_title(row) or _has_generic_role_bucket_title(row)
    ):
        return "role_bucket_detail_url_grouping"
    if _google_sheets_bucket_intent(row, bundle) != "weak_title_company_grouping":
        return "not_weak_google_sheets_grouping"
    title_token_count = len(_title_tokens(row))
    if title_token_count <= 1:
        return "single_token_title_many_urls"
    if title_token_count == 2:
        return "two_token_title_many_urls"
    if _unique_job_link_count(bundle) > 1:
        return "concrete_title_many_urls"
    return "unknown"


def _google_sheets_weak_grouping_evidence(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> list[str]:
    ids = _non_provider_source_job_id_values(bundle)
    indexes = _sheet_row_indexes(bundle)
    paths = _sample_url_paths(bundle)
    path_tokens = sorted({token for path in paths for token in _path_tokens(path)})
    evidence = [
        f"audit:{_google_sheets_weak_grouping_audit(row, bundle)}",
        f"intent:{_google_sheets_bucket_intent(row, bundle)}",
        f"source_count:{len(bundle)}",
        f"unique_urls:{_unique_job_link_count(bundle)}",
        f"url_hosts:{_unique_url_host_count(bundle)}",
        f"url_path_prefixes:{_unique_url_path_prefix_count(bundle)}",
        f"sheet_rows:{len(indexes)}",
        f"sheet_row_span:{_sheet_row_span(bundle)}",
        f"locations:{len(_meaningful_locations(row))}",
        f"title_tokens:{len(_title_tokens(row))}",
        f"company_tokens:{len(_company_tokens(row))}",
    ]
    if indexes:
        evidence.append(f"sheet_row_min:{indexes[0]}")
        evidence.append(f"sheet_row_max:{indexes[-1]}")
    if paths:
        evidence.append(
            "paths_listing_or_search"
            if all(_url_path_looks_listing_or_search(path) for path in paths)
            else "paths_job_detail_like"
            if _url_paths_look_job_detail(bundle)
            else "paths_mixed_or_unclear"
        )
    for signal in _title_company_pollution_signals(row):
        evidence.append(f"pollution:{signal}")
    for token in _title_tokens(row)[:3]:
        evidence.append(f"title_token:{token}")
    for token in path_tokens[:3]:
        evidence.append(f"path_token:{token}")
    for value in ids[:3]:
        evidence.append(f"source_id:{value}")
    return evidence[:20]


def _google_sheets_bundle_evidence(
    row: Mapping[str, Any], bundle: Sequence[Mapping[str, Any]]
) -> list[str]:
    ids = _non_provider_source_job_id_values(bundle)
    prefixes = sorted(
        {_source_job_id_prefix(value) for value in ids if _source_job_id_prefix(value)}
    )
    evidence = [
        f"shape:{_google_sheets_bundle_shape(row, bundle)}",
        f"source_count:{len(bundle)}",
        f"unique_urls:{_unique_job_link_count(bundle)}",
        f"url_hosts:{_unique_url_host_count(bundle)}",
        f"url_path_prefixes:{_unique_url_path_prefix_count(bundle)}",
        f"locations:{len(_meaningful_locations(row))}",
        f"title_shape:{_title_shape(row)}",
        f"title_tokens:{len(_title_tokens(row))}",
        f"company_tokens:{len(_company_tokens(row))}",
        f"source_id_prefixes:{len(prefixes)}",
    ]
    for path in _sample_url_paths(bundle)[:3]:
        evidence.append(f"url_path:{path}")
    for shape in sorted({_source_job_id_shape(value) for value in ids})[:3]:
        evidence.append(f"id_shape:{shape}")
    if _looks_role_bucket_title(row):
        evidence.append("role_bucket_title")
    for caveat in _identity_caveats(row, bundle):
        evidence.append(f"caveat:{caveat}")
    return evidence[:16]


def _is_google_sheets_role_bucket_summary(summary: Mapping[str, Any]) -> bool:
    if clean_text(summary.get("nonProviderIdentityProvenance")) != "google_sheets_row_identity":
        return False
    if clean_text(summary.get("googleSheetsBundleShape")) in {
        "role_category_bucket",
        "single_location_many_urls",
        "multi_location_many_urls",
        "company_role_family",
    }:
        return True
    return clean_text(summary.get("suspectedCause")) in {
        "spreadsheet_role_bucket_needs_review",
        "google_sheets_role_bucket_needs_review",
    }


def _google_sheets_role_bucket_audit_classification(summary: Mapping[str, Any]) -> str:
    if not _is_google_sheets_role_bucket_summary(summary):
        return ""
    if summary.get("sharedPrimaryUrl"):
        return "allowed_same_primary_url"
    if clean_text(summary.get("bundleEvidenceOrigin")) != "current_run":
        return "historical_carried_bundle"
    audit = clean_text(summary.get("googleSheetsRoleBucketAudit"))
    intent = clean_text(summary.get("googleSheetsBucketIntent"))
    weak_audit = clean_text(summary.get("googleSheetsWeakGroupingAudit"))
    if (
        audit
        in {
            "likely_spreadsheet_category_bucket",
            "listing_or_search_url_bucket",
            "parser_normalized_role_title",
        }
        or intent
        in {
            "likely_spreadsheet_taxonomy_bucket",
            "listing_or_search_bucket",
            "parser_normalized_bucket",
        }
        or weak_audit
        in {
            "role_bucket_listing_grouping",
            "parser_pollution_grouping",
        }
    ):
        return "parser_or_sheet_category_noise"
    if audit == "role_family_needs_manual_review" or intent in {
        "possible_role_family",
        "weak_title_company_grouping",
    }:
        return "needs_narrow_dedup_guard"
    return "unresolved_current_run_role_bucket"


def _google_sheets_guard_audit_example(row: Mapping[str, Any]) -> DedupMergeExampleRow:
    return {
        "classification": "fixed_by_generic_role_guard",
        "title": clean_text(row.get("incomingTitle")),
        "company": clean_text(row.get("incomingCompany")),
        "incomingSource": clean_text(row.get("incomingSource")),
        "incomingJobLink": normalize_url(row.get("incomingJobLink")),
        "incomingSourceJobId": clean_text(row.get("incomingSourceJobId")),
        "targetTitle": clean_text(row.get("targetTitle")),
        "targetCompany": clean_text(row.get("targetCompany")),
        "targetSource": clean_text(row.get("targetSource")),
        "targetJobLink": normalize_url(row.get("targetJobLink")),
        "targetSourceJobId": clean_text(row.get("targetSourceJobId")),
        "blockedMergeReason": clean_text(row.get("blockedMergeReason")) or "unknown",
        "guardReason": clean_text(row.get("guardReason")) or "unknown",
        "bundleEvidenceOrigin": "current_run",
        "evidence": [
            "different_concrete_primary_urls",
            f"blocked_merge_reason:{clean_text(row.get('blockedMergeReason')) or 'unknown'}",
            f"guard_reason:{clean_text(row.get('guardReason')) or 'unknown'}",
        ],
    }


def _google_sheets_role_bucket_audit_example(summary: Mapping[str, Any]) -> DedupReviewQueueRow:
    classification = _google_sheets_role_bucket_audit_classification(summary)
    evidence = [
        f"classification:{classification or 'unknown'}",
        f"origin:{clean_text(summary.get('bundleEvidenceOrigin')) or 'unknown'}",
        f"shape:{clean_text(summary.get('googleSheetsBundleShape')) or 'unknown'}",
        f"audit:{clean_text(summary.get('googleSheetsRoleBucketAudit')) or 'unknown'}",
        f"intent:{clean_text(summary.get('googleSheetsBucketIntent')) or 'unknown'}",
        f"weak_audit:{clean_text(summary.get('googleSheetsWeakGroupingAudit')) or 'unknown'}",
        f"shared_primary_url:{str(bool(summary.get('sharedPrimaryUrl'))).lower()}",
        f"unique_urls:{int(summary.get('uniqueJobLinkCount') or 0)}",
        f"url_hosts:{int(summary.get('uniqueUrlHostCount') or 0)}",
        f"url_path_prefixes:{int(summary.get('uniqueUrlPathPrefixCount') or 0)}",
    ]
    evidence.extend(str(item) for item in summary.get("googleSheetsRoleBucketAuditEvidence") or [])
    evidence.extend(str(item) for item in summary.get("googleSheetsWeakGroupingEvidence") or [])
    return {
        "classification": classification or "unknown",
        "title": clean_text(summary.get("title")),
        "company": clean_text(summary.get("company")),
        "dedupKey": clean_text(summary.get("dedupKey")),
        "sourceBundleCount": max(0, int(summary.get("sourceBundleCount") or 0)),
        "bundleEvidenceOrigin": clean_text(summary.get("bundleEvidenceOrigin")) or "unknown",
        "suspectedCause": clean_text(summary.get("suspectedCause")),
        "googleSheetsBundleShape": clean_text(summary.get("googleSheetsBundleShape")),
        "googleSheetsRoleBucketAudit": clean_text(summary.get("googleSheetsRoleBucketAudit")),
        "googleSheetsBucketIntent": clean_text(summary.get("googleSheetsBucketIntent")),
        "googleSheetsWeakGroupingAudit": clean_text(summary.get("googleSheetsWeakGroupingAudit")),
        "sharedPrimaryUrl": bool(summary.get("sharedPrimaryUrl")),
        "uniqueJobLinkCount": max(0, int(summary.get("uniqueJobLinkCount") or 0)),
        "urlHostDiversity": max(0, int(summary.get("uniqueUrlHostCount") or 0)),
        "urlPathPrefixDiversity": max(0, int(summary.get("uniqueUrlPathPrefixCount") or 0)),
        "evidence": list(dict.fromkeys(evidence))[:16],
    }


def _google_sheets_role_bucket_audit_summary(
    *,
    role_bucket_rows: Sequence[Mapping[str, Any]],
    guard_samples: Sequence[Mapping[str, Any]],
    guard_blocked_count: int,
    limit: int = 10,
) -> GoogleSheetsRoleBucketAuditPayload:
    classification_counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    current_run_count = 0
    carried_count = 0
    classification_counts["fixed_by_generic_role_guard"] += max(0, int(guard_blocked_count))
    for row in role_bucket_rows:
        classification = _google_sheets_role_bucket_audit_classification(row)
        if not classification:
            continue
        classification_counts.update([classification])
        if clean_text(row.get("bundleEvidenceOrigin")) == "current_run":
            current_run_count += 1
        else:
            carried_count += 1
        examples.append(_google_sheets_role_bucket_audit_example(row))
    for row in guard_samples:
        examples.append(_google_sheets_guard_audit_example(row))
    examples.sort(
        key=lambda row: (
            GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_CLASSIFICATION_KEYS.index(row.get("classification"))
            if row.get("classification") in GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_CLASSIFICATION_KEYS
            else len(GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_CLASSIFICATION_KEYS),
            norm_text(row.get("company")),
            norm_text(row.get("title") or row.get("targetTitle")),
            norm_text(row.get("dedupKey")),
        )
    )
    unresolved_count = sum(
        classification_counts.get(key, 0)
        for key in (
            "unresolved_current_run_role_bucket",
            "parser_or_sheet_category_noise",
            "needs_narrow_dedup_guard",
        )
    )
    return {
        "totalRoleBucketCount": len(role_bucket_rows) + max(0, int(guard_blocked_count)),
        "currentRunRoleBucketCount": current_run_count,
        "carriedHistoricalRoleBucketCount": carried_count,
        "blockedByDifferentPrimaryUrlCount": max(0, int(guard_blocked_count)),
        "allowedSamePrimaryUrlCount": int(classification_counts.get("allowed_same_primary_url", 0)),
        "likelyHistoricalCollisionCount": int(
            classification_counts.get("historical_carried_bundle", 0)
        ),
        "likelyParserCategoryBucketCount": int(
            classification_counts.get("parser_or_sheet_category_noise", 0)
        ),
        "unresolvedRoleBucketCount": unresolved_count,
        "classificationCounts": {
            key: int(classification_counts.get(key, 0))
            for key in GOOGLE_SHEETS_ROLE_BUCKET_AUDIT_CLASSIFICATION_KEYS
        },
        "examples": examples[: max(0, int(limit))],
    }
