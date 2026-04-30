"""Read-only redundant static proposal contract helpers."""

from __future__ import annotations

from collections import Counter
from typing import Any

from src.jobs.common.contracts_provider_static_overlap import (
    PROVIDER_STATIC_OVERLAP_STATUSES,
    normalize_provider_static_overlap_pair,
)
from src.jobs.common.contracts_static_suppression_policy import (
    normalize_static_suppression_policy_pair,
    normalize_static_suppression_policy_payload,
)
from src.jobs.common.numbers import _clamped_int
from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import as_json_object, json_object_rows

REDUNDANT_STATIC_PROPOSALS = frozenset(
    {
        "safe_redundant_static",
        "keep_static",
        "needs_more_history",
        "needs_review",
        "provider_unstable",
        "static_only_jobs_detected",
    }
)
REDUNDANT_STATIC_RECOMMENDED_ACTIONS = frozenset(
    {
        "keep_runtime_suppression",
        "keep_static_active",
        "collect_more_history",
        "review_pair",
        "pause_suppression",
    }
)
_PROPOSAL_LIMIT = 20
_STATIC_ONLY_REASON = "static_only_jobs_detected"
_PROVIDER_UNSTABLE_STATUSES = {"unstable_provider", "failed_provider", "needs_review"}


def _float_confidence(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = float(default)
    return round(max(0.0, min(1.0, parsed)), 2)


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [clean_text(item) for item in value if clean_text(item)]


def _audit_status(value: Any) -> str:
    status = norm_text(value)
    return status if status in PROVIDER_STATIC_OVERLAP_STATUSES else "not_audited"


def _proposal_action(proposal: str) -> str:
    return {
        "safe_redundant_static": "keep_runtime_suppression",
        "keep_static": "keep_static_active",
        "needs_more_history": "collect_more_history",
        "needs_review": "review_pair",
        "provider_unstable": "pause_suppression",
        "static_only_jobs_detected": "pause_suppression",
    }.get(proposal, "review_pair")


def normalize_redundant_static_proposal_row(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    proposal = norm_text(src.get("proposal"))
    if proposal not in REDUNDANT_STATIC_PROPOSALS:
        proposal = "needs_review"
    action = norm_text(src.get("recommendedAction"))
    if action not in REDUNDANT_STATIC_RECOMMENDED_ACTIONS:
        action = _proposal_action(proposal)
    return {
        "staticSourceId": clean_text(src.get("staticSourceId")),
        "staticSourceName": clean_text(src.get("staticSourceName")),
        "providerSourceId": clean_text(src.get("providerSourceId")),
        "providerSourceName": clean_text(src.get("providerSourceName")),
        "proposal": proposal,
        "confidence": _float_confidence(src.get("confidence"), 0.5),
        "reasons": _string_list(src.get("reasons")),
        "recommendedAction": action,
        "destructiveActionAllowed": False,
        "lastAuditStatus": _audit_status(src.get("lastAuditStatus") or src.get("auditStatus")),
        "providerCoverageStatus": clean_text(src.get("providerCoverageStatus")),
        "providerCoverageConsecutiveSuccesses": _clamped_int(
            src.get("providerCoverageConsecutiveSuccesses")
            or src.get("providerConsecutiveSuccesses"),
            0,
            0,
        ),
        "providerCoverageLatestKeptCount": _clamped_int(
            src.get("providerCoverageLatestKeptCount") or src.get("latestProviderKeptCount"),
            0,
            0,
        ),
        "staticOnlyCount": _clamped_int(src.get("staticOnlyCount"), 0, 0),
        "overlapCount": _clamped_int(src.get("overlapCount"), 0, 0),
    }


def _summary_from_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    proposals = [normalize_redundant_static_proposal_row(row) for row in rows]
    counts = Counter(row["proposal"] for row in proposals)
    return {
        "totalProposalCount": len(proposals),
        "safeRedundantCount": counts.get("safe_redundant_static", 0),
        "keepStaticCount": counts.get("keep_static", 0),
        "needsMoreHistoryCount": counts.get("needs_more_history", 0),
        "needsReviewCount": counts.get("needs_review", 0),
        "providerUnstableCount": counts.get("provider_unstable", 0),
        "staticOnlyDetectedCount": counts.get("static_only_jobs_detected", 0),
        "proposals": proposals[:_PROPOSAL_LIMIT],
    }


def normalize_redundant_static_proposals_payload(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    proposals = [
        normalize_redundant_static_proposal_row(row)
        for row in json_object_rows(src.get("proposals"))[:_PROPOSAL_LIMIT]
    ]
    derived = _summary_from_rows(proposals)
    return {
        "totalProposalCount": _clamped_int(
            src.get("totalProposalCount"), derived["totalProposalCount"], 0
        ),
        "safeRedundantCount": _clamped_int(
            src.get("safeRedundantCount"), derived["safeRedundantCount"], 0
        ),
        "keepStaticCount": _clamped_int(src.get("keepStaticCount"), derived["keepStaticCount"], 0),
        "needsMoreHistoryCount": _clamped_int(
            src.get("needsMoreHistoryCount"), derived["needsMoreHistoryCount"], 0
        ),
        "needsReviewCount": _clamped_int(
            src.get("needsReviewCount"), derived["needsReviewCount"], 0
        ),
        "providerUnstableCount": _clamped_int(
            src.get("providerUnstableCount"), derived["providerUnstableCount"], 0
        ),
        "staticOnlyDetectedCount": _clamped_int(
            src.get("staticOnlyDetectedCount"), derived["staticOnlyDetectedCount"], 0
        ),
        "proposals": proposals,
    }


def _identity_key(row: dict[str, Any]) -> tuple[str, str]:
    static_key = norm_text(row.get("staticSourceId")) or norm_text(row.get("staticSourceName"))
    provider_key = norm_text(row.get("providerSourceId")) or norm_text(
        row.get("providerSourceName")
    )
    return static_key, provider_key


def _coverage_index(provider_coverage: Any) -> dict[str, dict[str, Any]]:
    coverage = as_json_object(provider_coverage)
    rows: list[dict[str, Any]] = []
    for key in (
        "validatedProviders",
        "unstableOrFailedProviders",
        "needsReviewProviders",
        "probingProviders",
        "readyLaterProviders",
    ):
        rows.extend(json_object_rows(coverage.get(key)))
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = clean_text(row.get("name"))
        if name:
            indexed[norm_text(name)] = row
        migration_identity = clean_text(row.get("migrationSourceIdentity"))
        if migration_identity:
            indexed[norm_text(migration_identity)] = row
    return indexed


def _candidate_from_policy(row: Any) -> dict[str, Any]:
    pair = normalize_static_suppression_policy_pair(row)
    return {
        "staticSourceId": pair["staticSourceId"],
        "staticSourceName": pair["staticSourceName"],
        "providerSourceId": pair["providerSourceId"],
        "providerSourceName": pair["providerSourceName"],
        "policyDecision": pair["decision"],
        "policyReason": pair["reason"],
        "lastAuditStatus": pair["lastAuditStatus"],
        "auditReasons": list(pair["auditReasons"]),
        "providerCoverageStatus": pair["providerCoverageStatus"],
        "providerCoverageConsecutiveSuccesses": pair["providerCoverageConsecutiveSuccesses"],
        "providerCoverageLatestKeptCount": pair["providerCoverageLatestKeptCount"],
        "staticOnlyCount": pair["staticOnlyCount"],
        "overlapCount": pair["overlapCount"],
    }


def _candidate_from_overlap(row: Any) -> dict[str, Any]:
    pair = normalize_provider_static_overlap_pair(row)
    return {
        "staticSourceId": pair["staticSourceId"],
        "staticSourceName": pair["staticSourceName"],
        "providerSourceId": pair["providerSourceId"],
        "providerSourceName": pair["providerSourceName"],
        "lastAuditStatus": pair["auditStatus"],
        "auditReasons": list(pair["auditReasons"]),
        "providerCoverageStatus": pair["providerCoverageStatus"],
        "providerCoverageConsecutiveSuccesses": pair["providerConsecutiveSuccesses"],
        "providerCoverageLatestKeptCount": pair["latestProviderKeptCount"],
        "staticOnlyCount": pair["staticOnlyCount"],
        "overlapCount": pair["overlapCount"],
    }


def _merge_candidate(existing: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key in (
        "staticSourceId",
        "staticSourceName",
        "providerSourceId",
        "providerSourceName",
        "policyDecision",
        "policyReason",
        "lastAuditStatus",
        "providerCoverageStatus",
    ):
        if clean_text(candidate.get(key)):
            merged[key] = candidate[key]
    for key in (
        "providerCoverageConsecutiveSuccesses",
        "providerCoverageLatestKeptCount",
        "staticOnlyCount",
        "overlapCount",
    ):
        merged[key] = max(int(merged.get(key) or 0), int(candidate.get(key) or 0))
    reasons = []
    for item in [*merged.get("auditReasons", []), *candidate.get("auditReasons", [])]:
        reason = clean_text(item)
        if reason and reason not in reasons:
            reasons.append(reason)
    merged["auditReasons"] = reasons
    return merged


def _apply_provider_coverage(
    candidate: dict[str, Any], provider_coverage: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    provider_tokens = (
        norm_text(candidate.get("providerSourceName")),
        norm_text(candidate.get("providerSourceId")),
        norm_text(candidate.get("staticSourceId")),
    )
    coverage_row: dict[str, Any] = {}
    for token in provider_tokens:
        if token and token in provider_coverage:
            coverage_row = provider_coverage[token]
            break
    if not coverage_row:
        return candidate
    merged = dict(candidate)
    for key in (
        "providerCoverageStatus",
        "providerCoverageConsecutiveSuccesses",
        "providerCoverageLatestKeptCount",
    ):
        value = coverage_row.get(key)
        if clean_text(value) or isinstance(value, (int, float)):
            merged[key] = value
    return merged


def _base_row(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "staticSourceId": clean_text(candidate.get("staticSourceId")),
        "staticSourceName": clean_text(candidate.get("staticSourceName")),
        "providerSourceId": clean_text(candidate.get("providerSourceId")),
        "providerSourceName": clean_text(candidate.get("providerSourceName")),
        "lastAuditStatus": _audit_status(candidate.get("lastAuditStatus")),
        "providerCoverageStatus": clean_text(candidate.get("providerCoverageStatus")),
        "providerCoverageConsecutiveSuccesses": _clamped_int(
            candidate.get("providerCoverageConsecutiveSuccesses"), 0, 0
        ),
        "providerCoverageLatestKeptCount": _clamped_int(
            candidate.get("providerCoverageLatestKeptCount"), 0, 0
        ),
        "staticOnlyCount": _clamped_int(candidate.get("staticOnlyCount"), 0, 0),
        "overlapCount": _clamped_int(candidate.get("overlapCount"), 0, 0),
    }


def _classified_row(candidate: dict[str, Any]) -> dict[str, Any]:
    row = _base_row(candidate)
    audit_reasons = _string_list(candidate.get("auditReasons"))
    policy_decision = norm_text(candidate.get("policyDecision"))
    provider_status = norm_text(row["providerCoverageStatus"])
    successes = int(row["providerCoverageConsecutiveSuccesses"] or 0)
    kept = int(row["providerCoverageLatestKeptCount"] or 0)
    audit_status = row["lastAuditStatus"]
    reasons: list[str] = []

    def finish(proposal: str, confidence: float, *extra_reasons: str) -> dict[str, Any]:
        for reason in [*audit_reasons, *extra_reasons]:
            clean = clean_text(reason)
            if clean and clean not in reasons:
                reasons.append(clean)
        return normalize_redundant_static_proposal_row(
            {
                **row,
                "proposal": proposal,
                "confidence": confidence,
                "reasons": reasons,
                "recommendedAction": _proposal_action(proposal),
            }
        )

    if row["staticOnlyCount"] > 0 or _STATIC_ONLY_REASON in set(audit_reasons):
        return finish("static_only_jobs_detected", 0.95, "static_only_jobs_detected")
    if provider_status in _PROVIDER_UNSTABLE_STATUSES or audit_status == "provider_unstable":
        return finish("provider_unstable", 0.9, f"provider_status:{provider_status or 'unknown'}")
    if audit_status == "insufficient_history" or (
        audit_status == "not_audited" and row["overlapCount"] == 0
    ):
        return finish("needs_more_history", 0.6, "insufficient_static_provider_history")
    if (
        policy_decision in {"suppressed", "warning"}
        and provider_status == "validated_provider"
        and successes >= 2
        and kept > 0
        and row["staticOnlyCount"] == 0
        and audit_status in {"safe", "not_audited"}
    ):
        return finish("safe_redundant_static", 0.9, "runtime_suppression_supported")
    if audit_status == "needs_review":
        return finish("needs_review", 0.7, "overlap_audit_needs_review")
    return finish("keep_static", 0.75, "evaluated_pair_not_redundant")


def build_redundant_static_proposals_summary(
    *,
    static_suppression_policy: Any,
    provider_static_overlap: Any,
    provider_coverage: Any,
) -> dict[str, Any]:
    candidates: dict[tuple[str, str], dict[str, Any]] = {}
    policy = normalize_static_suppression_policy_payload(static_suppression_policy)
    for row in [
        *policy["suppressedPairs"],
        *policy["pausedPairs"],
        *policy["warningPairs"],
    ]:
        candidate = _candidate_from_policy(row)
        key = _identity_key(candidate)
        if key[0] and key[1]:
            candidates[key] = _merge_candidate(candidates.get(key, {}), candidate)
    for row in json_object_rows(as_json_object(provider_static_overlap).get("pairs")):
        candidate = _candidate_from_overlap(row)
        key = _identity_key(candidate)
        if key[0] and key[1]:
            candidates[key] = _merge_candidate(candidates.get(key, {}), candidate)

    coverage = _coverage_index(provider_coverage)
    proposals = [
        _classified_row(_apply_provider_coverage(candidate, coverage))
        for _key, candidate in sorted(candidates.items())
    ]
    return _summary_from_rows(proposals)
