"""Local dedup disagreement review state helpers."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import as_json_object

DEDUP_REVIEW_STATE_SCHEMA_VERSION = "1.0"
DEDUP_REVIEW_STATUSES = frozenset({"reviewed_safe", "confirmed_blocking"})
DEDUP_REVIEW_ACTIONS = frozenset({"reviewed_safe", "confirmed_blocking", "clear_review"})
_PAIR_KEY_SEPARATOR = "||"
_VALUE_SEPARATOR = ","
_NOTES_LIMIT = 500
_ACTOR_LIMIT = 80


def _clean_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    cleaned = {clean_text(value) for value in values if clean_text(value)}
    return sorted(cleaned)


def _norm_join(values: Iterable[str]) -> str:
    normalized = sorted({norm_text(value) for value in values if norm_text(value)})
    return _VALUE_SEPARATOR.join(normalized)


def _review_status(value: Any) -> str:
    status = norm_text(value)
    return status if status in DEDUP_REVIEW_STATUSES else ""


def _bounded_text(value: Any, limit: int) -> str:
    return clean_text(value)[:limit]


def dedup_review_pair_key(
    *,
    disagreement_classification: str,
    provider_source_job_ids: Sequence[str] | None = None,
    static_source_job_ids: Sequence[str] | None = None,
    dedup_key: str = "",
) -> str:
    classification = norm_text(disagreement_classification)
    if not classification:
        return ""
    provider_ids = _norm_join(provider_source_job_ids or [])
    static_ids = _norm_join(static_source_job_ids or [])
    if provider_ids and static_ids:
        return _PAIR_KEY_SEPARATOR.join((classification, provider_ids, static_ids))
    fallback_dedup_key = norm_text(dedup_key)
    if fallback_dedup_key:
        return _PAIR_KEY_SEPARATOR.join((classification, f"dedup:{fallback_dedup_key}"))
    return ""


def normalize_dedup_review_pair(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    disagreement_classification = clean_text(src.get("disagreementClassification"))
    provider_source_job_ids = _clean_list(src.get("providerSourceJobIds"))
    static_source_job_ids = _clean_list(src.get("staticSourceJobIds"))
    dedup_key = clean_text(src.get("dedupKey"))
    return {
        "reviewKey": dedup_review_pair_key(
            disagreement_classification=disagreement_classification,
            provider_source_job_ids=provider_source_job_ids,
            static_source_job_ids=static_source_job_ids,
            dedup_key=dedup_key,
        ),
        "title": clean_text(src.get("title")),
        "company": clean_text(src.get("company")),
        "dedupKey": dedup_key,
        "bundleEvidenceOrigin": clean_text(src.get("bundleEvidenceOrigin")),
        "disagreementClassification": disagreement_classification,
        "providerSourceJobIds": provider_source_job_ids,
        "staticSourceJobIds": static_source_job_ids,
        "providerSources": _clean_list(src.get("providerSources")),
        "staticSources": _clean_list(src.get("staticSources")),
        "providerUrls": _clean_list(src.get("providerUrls")),
        "staticUrls": _clean_list(src.get("staticUrls")),
        "sharedIdentifierTokens": _clean_list(src.get("sharedIdentifierTokens")),
        "distinctLocationCount": max(0, int(src.get("distinctLocationCount") or 0)),
        "sampleLocations": _clean_list(src.get("sampleLocations"))[:5],
        "identityQuality": clean_text(src.get("identityQuality")),
        "carriedLocationPollutionAudit": clean_text(src.get("carriedLocationPollutionAudit")),
        "reviewStatus": _review_status(src.get("reviewStatus")),
        "reviewedAt": clean_text(src.get("reviewedAt")),
        "reviewedBy": _bounded_text(src.get("reviewedBy"), _ACTOR_LIMIT),
        "reviewNote": _bounded_text(src.get("reviewNote"), _NOTES_LIMIT),
    }


def _summary_from_pairs(pairs: Mapping[str, Mapping[str, Any]]) -> dict[str, int]:
    rows = [normalize_dedup_review_pair(row) for row in pairs.values()]
    status_counts = Counter(row["reviewStatus"] for row in rows if row["reviewStatus"])
    return {
        "totalPairs": len(rows),
        "reviewedSafeCount": int(status_counts.get("reviewed_safe", 0)),
        "confirmedBlockingCount": int(status_counts.get("confirmed_blocking", 0)),
    }


def normalize_dedup_review_state_artifact(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    raw_pairs = as_json_object(src.get("pairs"))
    pairs: dict[str, dict[str, Any]] = {}
    for raw_key, raw_row in raw_pairs.items():
        row = normalize_dedup_review_pair(raw_row)
        key = clean_text(row.get("reviewKey")) or clean_text(raw_key)
        if key:
            row["reviewKey"] = key
            pairs[key] = row
    return {
        "schemaVersion": clean_text(src.get("schemaVersion")) or DEDUP_REVIEW_STATE_SCHEMA_VERSION,
        "updatedAt": clean_text(src.get("updatedAt")),
        "summary": _summary_from_pairs(pairs),
        "pairs": pairs,
    }


def read_dedup_review_state_artifact(path: Path) -> tuple[dict[str, Any], str]:
    artifact_path = Path(path)
    if not artifact_path.exists():
        return normalize_dedup_review_state_artifact({}), "missing_dedup_review_state_artifact"
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return (
            normalize_dedup_review_state_artifact({}),
            "malformed_dedup_review_state_artifact",
        )
    if not isinstance(payload, dict):
        return (
            normalize_dedup_review_state_artifact({}),
            "malformed_dedup_review_state_artifact",
        )
    return normalize_dedup_review_state_artifact(payload), ""


def find_dedup_review_pair(review_state: Any, row: Mapping[str, Any]) -> dict[str, Any]:
    artifact = normalize_dedup_review_state_artifact(review_state)
    key = dedup_review_pair_key(
        disagreement_classification=clean_text(row.get("disagreementClassification")),
        provider_source_job_ids=_clean_list(row.get("providerSourceJobIds")),
        static_source_job_ids=_clean_list(row.get("staticSourceJobIds")),
        dedup_key=clean_text(row.get("dedupKey")),
    )
    if not key:
        return {}
    return normalize_dedup_review_pair(artifact.get("pairs", {}).get(key, {}))


def dedup_review_pair_public_fields(row: Any) -> dict[str, Any]:
    pair = normalize_dedup_review_pair(row)
    return {
        "dedupReviewStatus": pair["reviewStatus"],
        "dedupReviewNote": pair["reviewNote"],
        "dedupReviewUpdatedAt": pair["reviewedAt"],
        "dedupReviewUpdatedBy": pair["reviewedBy"],
    }


def _carried_disagreement_auto_disposition(
    classification: str,
    classification_evidence: list[str],
    carried_location_pollution_audit: str,
    provider_backed: bool,
    static_backed: bool,
    single_location: bool,
    same_host: bool,
    has_shared_tokens: bool,
    has_concrete_shared_job_identity: bool = False,
) -> str:
    if (
        classification == "provider_redirect_or_canonical_url"
        and provider_backed
        and static_backed
        and single_location
        and has_concrete_shared_job_identity
    ):
        return "auto_safe_carried_provider_redirect_or_canonical_url"
    if (
        classification == "static_parser_url_variant"
        and provider_backed
        and single_location
        and has_concrete_shared_job_identity
    ):
        return "auto_safe_carried_static_parser_url_variant"
    if (
        classification == "title_company_collision"
        and carried_location_pollution_audit
        in {"carried_location_variant", "carried_provider_identity_location_conflict"}
        and provider_backed
        and static_backed
        and (same_host or has_shared_tokens)
    ):
        return carried_location_pollution_audit
    if (
        classification
        in {
            "same_job_different_urls",
            "provider_redirect_or_canonical_url",
            "static_parser_url_variant",
        }
        and provider_backed
        and static_backed
        and single_location
        and "smartrecruiters_same_board_title_location_alias" in classification_evidence
    ):
        return "auto_safe_carried_smartrecruiters_title_location_alias"
    return ""


def _current_disagreement_auto_disposition(
    classification: str,
    classification_evidence: list[str],
    provider_static_only: bool,
    provider_backed: bool,
    static_has_url: bool,
    single_location: bool,
    concrete_shared_token_count: int,
) -> str:
    has_concrete_single_job_identity = concrete_shared_token_count == 1
    if (
        classification == "provider_redirect_or_canonical_url"
        and provider_backed
        and static_has_url
        and single_location
        and has_concrete_single_job_identity
    ):
        return "auto_safe_current_provider_redirect_or_canonical_url"
    if (
        classification == "static_parser_url_variant"
        and provider_backed
        and static_has_url
        and single_location
        and has_concrete_single_job_identity
    ):
        return "auto_safe_current_static_parser_url_variant"
    if (
        classification in {"same_job_different_urls", "static_parser_url_variant"}
        and provider_backed
        and static_has_url
        and single_location
        and concrete_shared_token_count == 0
        and "known_gracklehq_gamesjobsdirect_mirror_pair" in classification_evidence
    ):
        return "auto_safe_current_known_mirror_pair"
    if (
        classification
        in {
            "same_job_different_urls",
            "provider_redirect_or_canonical_url",
            "static_parser_url_variant",
        }
        and provider_backed
        and static_has_url
        and single_location
        and "smartrecruiters_same_board_title_location_alias" in classification_evidence
    ):
        return "auto_safe_current_smartrecruiters_title_location_alias"
    return ""


def _carried_disagreement_blocker_reason(classification: str) -> str:
    if classification == "same_job_different_urls":
        return "carried_same_job_different_urls_requires_review"
    if classification == "title_company_collision":
        return "possible_real_multi_location_conflict"
    return "carried_unresolved_disagreement"


def dedup_operator_review_fields(row: Mapping[str, Any]) -> dict[str, str]:
    classification = clean_text(row.get("disagreementClassification")) or "unknown"
    disposition = clean_text(row.get("disagreementGateDisposition")) or "blocked"
    review_status = clean_text(row.get("dedupReviewStatus"))
    evidence = [clean_text(item) for item in row.get("disagreementGateEvidence") or []]
    collision_hint = clean_text(row.get("collisionReviewHint"))
    if review_status == "reviewed_safe":
        return {
            "operatorReviewRecommendation": "safe_duplicate",
            "operatorReviewReason": "manual_reviewed_safe",
        }
    if review_status == "confirmed_blocking":
        return {
            "operatorReviewRecommendation": "real_blocker",
            "operatorReviewReason": "manual_confirmed_blocking",
        }
    if disposition == "warning":
        if any(item.startswith("auto_safe_") for item in evidence):
            return {
                "operatorReviewRecommendation": "safe_duplicate",
                "operatorReviewReason": "auto_safe_provider_static_variant",
            }
        if "carried_location_pollution" in evidence:
            return {
                "operatorReviewRecommendation": "safe_duplicate",
                "operatorReviewReason": "carried_location_pollution_warning",
            }
        return {
            "operatorReviewRecommendation": "safe_duplicate",
            "operatorReviewReason": "warning_not_blocking",
        }
    if (
        classification == "title_company_collision"
        and collision_hint == "different_locations_same_title_company"
    ):
        return {
            "operatorReviewRecommendation": "real_blocker",
            "operatorReviewReason": "different_locations_same_title_company",
        }
    return {
        "operatorReviewRecommendation": "needs_review",
        "operatorReviewReason": f"{classification}_blocked",
    }


def dedup_disagreement_gate_disposition(
    row: Mapping[str, Any], review_pair: Mapping[str, Any] | None = None
) -> tuple[str, list[str]]:
    pair = normalize_dedup_review_pair(review_pair or {})
    classification = clean_text(row.get("disagreementClassification"))
    origin = clean_text(row.get("bundleEvidenceOrigin"))
    review_status = pair["reviewStatus"]
    provider_ids = _clean_list(row.get("providerSourceJobIds"))
    static_ids = _clean_list(row.get("staticSourceJobIds"))
    shared_tokens = _clean_list(row.get("sharedIdentifierTokens"))
    concrete_shared_tokens = _clean_list(row.get("concreteSharedIdentifierTokens"))
    classification_evidence = _clean_list(row.get("disagreementClassificationEvidence"))
    provider_hosts = _clean_list(row.get("providerUrlHosts"))
    static_hosts = _clean_list(row.get("staticUrlHosts"))
    static_urls = _clean_list(row.get("staticUrls"))
    location_count = max(0, int(row.get("distinctLocationCount") or 0))
    carried_location_pollution_audit = clean_text(row.get("carriedLocationPollutionAudit"))
    is_carried = origin == "carried_from_existing_output"
    same_host = bool(set(provider_hosts) & set(static_hosts))
    provider_backed = bool(provider_ids)
    static_backed = bool(static_ids)
    provider_static_only = row.get("providerStaticOnly") is True
    single_location = (
        location_count <= 1 or "single_effective_location_variant" in classification_evidence
    )
    evidence = [
        f"classification:{classification or 'unknown'}",
        f"origin:{origin or 'unknown'}",
        f"review_status:{review_status or 'none'}",
        f"provider_ids:{len(provider_ids)}",
        f"static_ids:{len(static_ids)}",
        f"shared_tokens:{len(shared_tokens)}",
        f"concrete_shared_tokens:{len(concrete_shared_tokens)}",
        f"locations:{location_count}",
    ]
    if provider_static_only:
        evidence.append("provider_static_only:true")
    if carried_location_pollution_audit:
        evidence.append(f"carried_location_audit:{carried_location_pollution_audit}")
    if review_status == "confirmed_blocking":
        return "blocked", [*evidence, "manual_review_confirmed_blocking"]
    if review_status == "reviewed_safe":
        return "warning", [*evidence, "manual_review_reviewed_safe"]
    if (
        classification == "title_company_collision"
        and carried_location_pollution_audit == "carried_location_pollution"
    ):
        return "warning", [*evidence, "carried_location_pollution"]
    if not is_carried:
        auto_disposition = _current_disagreement_auto_disposition(
            classification,
            classification_evidence,
            provider_static_only,
            provider_backed,
            bool(static_urls),
            single_location,
            len(concrete_shared_tokens),
        )
        if auto_disposition:
            return "warning", [*evidence, auto_disposition]
        return "blocked", [*evidence, "current_run_or_unclassified_origin"]
    auto_disposition = _carried_disagreement_auto_disposition(
        classification,
        classification_evidence,
        carried_location_pollution_audit,
        provider_backed,
        static_backed,
        single_location,
        same_host,
        bool(shared_tokens),
        len(concrete_shared_tokens) == 1,
    )
    if auto_disposition:
        return "warning", [*evidence, auto_disposition]
    return "blocked", [*evidence, _carried_disagreement_blocker_reason(classification)]


def _gate_counter_fields(row: Mapping[str, Any]) -> dict[str, int]:
    origin = clean_text(row.get("bundleEvidenceOrigin"))
    disposition = clean_text(row.get("disagreementGateDisposition"))
    review_status = clean_text(row.get("dedupReviewStatus"))
    fields = {
        "blocked": 1 if disposition == "blocked" else 0,
        "warning": 1 if disposition == "warning" else 0,
        "currentRunBlocked": 1 if origin == "current_run" and disposition == "blocked" else 0,
        "carriedBlocked": 1 if origin != "current_run" and disposition == "blocked" else 0,
        "carriedWarning": 1 if origin != "current_run" and disposition == "warning" else 0,
        "autoSafeWarning": 0,
        "locationPollutionWarning": 0,
        "reviewedSafeWarning": 1
        if review_status == "reviewed_safe" and disposition == "warning"
        else 0,
        "confirmedBlocking": 1
        if review_status == "confirmed_blocking" and disposition == "blocked"
        else 0,
    }
    if disposition == "warning":
        evidence = [clean_text(item) for item in row.get("disagreementGateEvidence") or []]
        if any(item.startswith("auto_safe_") for item in evidence):
            fields["autoSafeWarning"] = 1
    if (
        clean_text(row.get("carriedLocationPollutionAudit")) == "carried_location_pollution"
        and disposition == "warning"
    ):
        fields["locationPollutionWarning"] = 1
    return fields


def merge_dedup_review_state_into_dedup_evidence(
    dedup_evidence: Mapping[str, Any], review_state: Any
) -> dict[str, Any]:
    payload = dict(as_json_object(dedup_evidence))
    artifact = normalize_dedup_review_state_artifact(review_state)
    raw_examples = payload.get("providerStaticDisagreementExamples")
    if not isinstance(raw_examples, list):
        return payload
    gate_counts = {
        key: max(0, int(value or 0))
        for key, value in as_json_object(
            payload.get("providerStaticDisagreementGateCounts")
        ).items()
    }
    updated_examples: list[dict[str, Any]] = []
    for raw_row in raw_examples:
        if not isinstance(raw_row, dict):
            continue
        prior_row = dict(raw_row)
        review_pair = find_dedup_review_pair(artifact, prior_row)
        disposition, evidence = dedup_disagreement_gate_disposition(prior_row, review_pair)
        updated_row = {
            **prior_row,
            **dedup_review_pair_public_fields(review_pair),
            "disagreementGateDisposition": disposition,
            "disagreementGateEvidence": evidence,
        }
        updated_row = {**updated_row, **dedup_operator_review_fields(updated_row)}
        for key, value in _gate_counter_fields(prior_row).items():
            gate_counts[key] = max(0, int(gate_counts.get(key, 0)) - value)
        for key, value in _gate_counter_fields(updated_row).items():
            gate_counts[key] = max(0, int(gate_counts.get(key, 0)) + value)
        updated_examples.append(updated_row)
    payload["providerStaticDisagreementExamples"] = updated_examples
    payload["providerStaticTitleCompanyCollisionExamples"] = [
        row
        for row in updated_examples
        if clean_text(row.get("disagreementClassification")) == "title_company_collision"
    ]
    payload["providerStaticDisagreementGateCounts"] = gate_counts
    return payload


def apply_dedup_review_action(
    *,
    prior_artifact: Any,
    action_payload: Any,
    updated_at: str,
    default_updated_by: str = "admin",
) -> tuple[dict[str, Any], dict[str, Any]]:
    artifact = normalize_dedup_review_state_artifact(prior_artifact)
    payload = as_json_object(action_payload)
    action = norm_text(payload.get("action"))
    if action not in DEDUP_REVIEW_ACTIONS:
        raise ValueError("invalid dedup review action")
    disagreement_classification = clean_text(payload.get("disagreementClassification"))
    provider_source_job_ids = _clean_list(payload.get("providerSourceJobIds"))
    static_source_job_ids = _clean_list(payload.get("staticSourceJobIds"))
    dedup_key = clean_text(payload.get("dedupKey"))
    key = dedup_review_pair_key(
        disagreement_classification=disagreement_classification,
        provider_source_job_ids=provider_source_job_ids,
        static_source_job_ids=static_source_job_ids,
        dedup_key=dedup_key,
    )
    if not key:
        raise ValueError(
            "disagreementClassification with provider/static source job ids or dedupKey is required"
        )
    if action == "clear_review":
        artifact["pairs"].pop(key, None)
        next_artifact = {
            "schemaVersion": DEDUP_REVIEW_STATE_SCHEMA_VERSION,
            "updatedAt": clean_text(updated_at),
            "pairs": artifact["pairs"],
        }
        return normalize_dedup_review_state_artifact(next_artifact), {}
    prior_pair = normalize_dedup_review_pair(artifact["pairs"].get(key, {}))
    pair = {
        **prior_pair,
        "reviewKey": key,
        "title": clean_text(payload.get("title")) or prior_pair["title"],
        "company": clean_text(payload.get("company")) or prior_pair["company"],
        "dedupKey": dedup_key or prior_pair["dedupKey"],
        "bundleEvidenceOrigin": clean_text(payload.get("bundleEvidenceOrigin"))
        or prior_pair["bundleEvidenceOrigin"],
        "disagreementClassification": disagreement_classification
        or prior_pair["disagreementClassification"],
        "providerSourceJobIds": provider_source_job_ids or prior_pair["providerSourceJobIds"],
        "staticSourceJobIds": static_source_job_ids or prior_pair["staticSourceJobIds"],
        "providerSources": _clean_list(payload.get("providerSources"))
        or prior_pair["providerSources"],
        "staticSources": _clean_list(payload.get("staticSources")) or prior_pair["staticSources"],
        "providerUrls": _clean_list(payload.get("providerUrls")) or prior_pair["providerUrls"],
        "staticUrls": _clean_list(payload.get("staticUrls")) or prior_pair["staticUrls"],
        "sharedIdentifierTokens": _clean_list(payload.get("sharedIdentifierTokens"))
        or prior_pair["sharedIdentifierTokens"],
        "distinctLocationCount": max(
            0, int(payload.get("distinctLocationCount") or prior_pair["distinctLocationCount"] or 0)
        ),
        "sampleLocations": _clean_list(payload.get("sampleLocations"))
        or prior_pair["sampleLocations"],
        "identityQuality": clean_text(payload.get("identityQuality"))
        or prior_pair["identityQuality"],
        "carriedLocationPollutionAudit": clean_text(payload.get("carriedLocationPollutionAudit"))
        or prior_pair["carriedLocationPollutionAudit"],
        "reviewStatus": action,
        "reviewedAt": clean_text(updated_at),
        "reviewedBy": _bounded_text(payload.get("reviewedBy") or default_updated_by, _ACTOR_LIMIT),
        "reviewNote": _bounded_text(payload.get("reviewNote"), _NOTES_LIMIT)
        if "reviewNote" in payload
        else prior_pair["reviewNote"],
    }
    normalized_pair = normalize_dedup_review_pair(pair)
    artifact["pairs"][key] = normalized_pair
    next_artifact = {
        "schemaVersion": DEDUP_REVIEW_STATE_SCHEMA_VERSION,
        "updatedAt": clean_text(updated_at),
        "pairs": artifact["pairs"],
    }
    return normalize_dedup_review_state_artifact(next_artifact), normalized_pair
