"""Runtime static suppression safety policy contract helpers."""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from src.jobs.common.contracts_provider_static_overlap import (
    PROVIDER_STATIC_OVERLAP_STATUSES,
    build_provider_static_overlap_pair,
    normalize_provider_static_overlap_pair,
)
from src.jobs.common.numbers import _clamped_int
from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import as_json_object, json_object_rows

STATIC_SUPPRESSION_DECISIONS = frozenset({"suppressed", "paused", "warning"})
_PAIR_LIMIT = 20
_PAUSE_AUDIT_STATUSES = {"needs_review", "provider_unstable"}
_STATIC_ONLY_REASON = "static_only_jobs_detected"


def _audit_reasons(payload: Any) -> list[str]:
    return (
        [clean_text(item) for item in payload if clean_text(item)]
        if isinstance(payload, list)
        else []
    )


def _provider_successes(src: dict[str, Any]) -> int:
    return _clamped_int(
        src.get("providerCoverageConsecutiveSuccesses") or src.get("providerConsecutiveSuccesses"),
        0,
        0,
    )


def _provider_kept(src: dict[str, Any]) -> int:
    return _clamped_int(
        src.get("providerCoverageLatestKeptCount") or src.get("latestProviderKeptCount"),
        0,
        0,
    )


def normalize_static_suppression_policy_pair(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    decision = norm_text(src.get("decision"))
    if decision not in STATIC_SUPPRESSION_DECISIONS:
        decision = "suppressed"
    last_audit_status = norm_text(src.get("lastAuditStatus") or src.get("auditStatus"))
    if last_audit_status and last_audit_status not in PROVIDER_STATIC_OVERLAP_STATUSES:
        last_audit_status = "not_audited"
    return {
        "staticSourceId": clean_text(src.get("staticSourceId")),
        "staticSourceName": clean_text(src.get("staticSourceName")),
        "providerSourceId": clean_text(src.get("providerSourceId")),
        "providerSourceName": clean_text(src.get("providerSourceName")),
        "decision": decision,
        "reason": clean_text(src.get("reason")),
        "lastAuditStatus": last_audit_status,
        "providerCoverageStatus": clean_text(src.get("providerCoverageStatus")),
        "providerCoverageConsecutiveSuccesses": _provider_successes(src),
        "providerCoverageLatestKeptCount": _provider_kept(src),
        "auditReasons": _audit_reasons(src.get("auditReasons")),
        "staticOnlyCount": _clamped_int(src.get("staticOnlyCount"), 0, 0),
        "overlapCount": _clamped_int(src.get("overlapCount"), 0, 0),
    }


def _policy_pair_from_overlap(
    payload: Any, *, decision: str = "suppressed", reason: str = ""
) -> dict[str, Any]:
    pair = normalize_provider_static_overlap_pair(payload)
    return normalize_static_suppression_policy_pair(
        {
            "staticSourceId": pair["staticSourceId"],
            "staticSourceName": pair["staticSourceName"],
            "providerSourceId": pair["providerSourceId"],
            "providerSourceName": pair["providerSourceName"],
            "decision": decision,
            "reason": reason,
            "lastAuditStatus": pair["auditStatus"],
            "providerCoverageStatus": pair["providerCoverageStatus"],
            "providerCoverageConsecutiveSuccesses": pair["providerConsecutiveSuccesses"],
            "providerCoverageLatestKeptCount": pair["latestProviderKeptCount"],
            "auditReasons": pair["auditReasons"],
            "staticOnlyCount": pair["staticOnlyCount"],
            "overlapCount": pair["overlapCount"],
        }
    )


def _summary_from_pairs(pairs: Iterable[dict[str, Any]]) -> dict[str, Any]:
    normalized = [normalize_static_suppression_policy_pair(pair) for pair in pairs]
    suppressed = [pair for pair in normalized if pair["decision"] == "suppressed"]
    paused = [pair for pair in normalized if pair["decision"] == "paused"]
    warning = [pair for pair in normalized if pair["decision"] == "warning"]
    return {
        "eligibleCount": len(normalized),
        "suppressedCount": len(suppressed),
        "pausedCount": len(paused),
        "warningCount": len(warning),
        "suppressedPairs": suppressed[:_PAIR_LIMIT],
        "pausedPairs": paused[:_PAIR_LIMIT],
        "warningPairs": warning[:_PAIR_LIMIT],
    }


def normalize_static_suppression_policy_payload(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    suppressed = [
        normalize_static_suppression_policy_pair(row)
        for row in json_object_rows(src.get("suppressedPairs"))[:_PAIR_LIMIT]
    ]
    paused = [
        normalize_static_suppression_policy_pair(row)
        for row in json_object_rows(src.get("pausedPairs"))[:_PAIR_LIMIT]
    ]
    warning = [
        normalize_static_suppression_policy_pair(row)
        for row in json_object_rows(src.get("warningPairs"))[:_PAIR_LIMIT]
    ]
    pairs = suppressed + paused + warning
    derived = _summary_from_pairs(pairs)
    return {
        "eligibleCount": _clamped_int(src.get("eligibleCount"), derived["eligibleCount"], 0),
        "suppressedCount": _clamped_int(src.get("suppressedCount"), derived["suppressedCount"], 0),
        "pausedCount": _clamped_int(src.get("pausedCount"), derived["pausedCount"], 0),
        "warningCount": _clamped_int(src.get("warningCount"), derived["warningCount"], 0),
        "suppressedPairs": suppressed,
        "pausedPairs": paused,
        "warningPairs": warning,
    }


def _all_policy_pairs(payload: Any) -> list[dict[str, Any]]:
    policy = normalize_static_suppression_policy_payload(payload)
    return [
        *policy["suppressedPairs"],
        *policy["pausedPairs"],
        *policy["warningPairs"],
    ]


def normalize_prior_static_suppression_evidence(report_payload: Any) -> dict[str, Any]:
    src = as_json_object(report_payload)
    policy_src = as_json_object(src.get("staticSuppressionPolicy"))
    if policy_src:
        return {"pairs": _all_policy_pairs(policy_src)}
    overlap_src = as_json_object(src.get("providerStaticOverlap"))
    if overlap_src:
        return {
            "pairs": [
                _policy_pair_from_overlap(row)
                for row in json_object_rows(overlap_src.get("pairs"))[:_PAIR_LIMIT]
            ]
        }
    return {"pairs": []}


def read_prior_static_suppression_evidence(report_path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(report_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"pairs": []}
    if not isinstance(payload, dict):
        return {"pairs": []}
    return normalize_prior_static_suppression_evidence(payload)


def _identity_tokens(*values: str) -> set[str]:
    return {clean_text(value) for value in values if clean_text(value)}


def _matches_prior_pair(
    pair: dict[str, Any],
    *,
    static_source_id: str,
    static_source_name: str,
    provider_source_id: str,
    provider_source_name: str,
) -> bool:
    static_tokens = _identity_tokens(static_source_id, static_source_name)
    provider_tokens = _identity_tokens(provider_source_id, provider_source_name)
    pair_static = _identity_tokens(pair.get("staticSourceId", ""), pair.get("staticSourceName", ""))
    pair_provider = _identity_tokens(
        pair.get("providerSourceId", ""), pair.get("providerSourceName", "")
    )
    return bool(static_tokens & pair_static) and bool(provider_tokens & pair_provider)


def find_prior_static_suppression_pair(
    prior_evidence: Any,
    *,
    static_source_id: str,
    static_source_name: str,
    provider_source_id: str,
    provider_source_name: str,
) -> dict[str, Any]:
    src = as_json_object(prior_evidence)
    for row in json_object_rows(src.get("pairs")):
        pair = normalize_static_suppression_policy_pair(row)
        if _matches_prior_pair(
            pair,
            static_source_id=static_source_id,
            static_source_name=static_source_name,
            provider_source_id=provider_source_id,
            provider_source_name=provider_source_name,
        ):
            return pair
    return {}


def decide_static_suppression_from_prior_pair(prior_pair: dict[str, Any]) -> tuple[str, str]:
    if not prior_pair:
        return "suppressed", "missing_prior_evidence"
    pair = normalize_static_suppression_policy_pair(prior_pair)
    reasons = set(pair["auditReasons"])
    if pair["staticOnlyCount"] > 0 or _STATIC_ONLY_REASON in reasons:
        return "paused", "prior_static_only_jobs_detected"
    if pair["lastAuditStatus"] == "provider_unstable":
        return "paused", "prior_provider_unstable"
    if pair["lastAuditStatus"] == "needs_review":
        return "paused", "prior_audit_needs_review"
    if pair["lastAuditStatus"] == "insufficient_history":
        return "warning", "prior_insufficient_history"
    if pair["lastAuditStatus"] == "safe":
        return "suppressed", "prior_audit_safe"
    return "suppressed", "no_blocking_prior_audit"


def build_static_suppression_policy_pair(
    *,
    static_source_id: str,
    static_source_name: str,
    provider_source_id: str,
    provider_source_name: str,
    provider_row: dict[str, Any],
    decision: str,
    reason: str,
    audit_pair: Any = None,
) -> dict[str, Any]:
    audit_src = as_json_object(audit_pair)
    if "lastAuditStatus" in audit_src or "decision" in audit_src:
        prior = normalize_static_suppression_policy_pair(audit_src)
        audit = {
            "auditStatus": prior["lastAuditStatus"],
            "providerCoverageStatus": prior["providerCoverageStatus"],
            "providerConsecutiveSuccesses": prior["providerCoverageConsecutiveSuccesses"],
            "latestProviderKeptCount": prior["providerCoverageLatestKeptCount"],
            "auditReasons": prior["auditReasons"],
            "staticOnlyCount": prior["staticOnlyCount"],
            "overlapCount": prior["overlapCount"],
        }
    else:
        audit = normalize_provider_static_overlap_pair(audit_pair)
    provider_status = clean_text(provider_row.get("providerCoverageStatus")) or clean_text(
        audit.get("providerCoverageStatus")
    )
    provider_successes = _clamped_int(
        provider_row.get("providerCoverageConsecutiveSuccesses")
        or audit.get("providerConsecutiveSuccesses"),
        0,
        0,
    )
    provider_kept = _clamped_int(
        provider_row.get("providerCoverageLatestKeptCount") or audit.get("latestProviderKeptCount"),
        0,
        0,
    )
    return normalize_static_suppression_policy_pair(
        {
            "staticSourceId": static_source_id,
            "staticSourceName": static_source_name,
            "providerSourceId": provider_source_id,
            "providerSourceName": provider_source_name,
            "decision": decision,
            "reason": reason,
            "lastAuditStatus": audit.get("auditStatus", ""),
            "providerCoverageStatus": provider_status,
            "providerCoverageConsecutiveSuccesses": provider_successes,
            "providerCoverageLatestKeptCount": provider_kept,
            "auditReasons": audit.get("auditReasons", []),
            "staticOnlyCount": audit.get("staticOnlyCount", 0),
            "overlapCount": audit.get("overlapCount", 0),
        }
    )


def build_static_suppression_policy_summary(pairs: Iterable[dict[str, Any]]) -> dict[str, Any]:
    return _summary_from_pairs(pairs)


def _overlap_pair_index(provider_static_overlap: Any) -> list[dict[str, Any]]:
    return [
        normalize_provider_static_overlap_pair(row)
        for row in json_object_rows(as_json_object(provider_static_overlap).get("pairs"))
    ]


def _find_overlap_pair(
    pairs: list[dict[str, Any]],
    *,
    static_source_id: str,
    static_source_name: str,
    provider_source_id: str,
    provider_source_name: str,
) -> dict[str, Any]:
    for pair in pairs:
        if _matches_prior_pair(
            pair,
            static_source_id=static_source_id,
            static_source_name=static_source_name,
            provider_source_id=provider_source_id,
            provider_source_name=provider_source_name,
        ):
            return pair
    return {}


def refresh_static_suppression_policy_with_current_evidence(
    selection_policy: Any,
    *,
    source_state_rows: dict[str, dict[str, Any]],
    canonical_rows: Any,
    provider_static_overlap: Any,
) -> dict[str, Any]:
    selected_pairs = _all_policy_pairs(selection_policy)
    if not selected_pairs:
        return normalize_static_suppression_policy_payload({})
    overlap_pairs = _overlap_pair_index(provider_static_overlap)
    refreshed: list[dict[str, Any]] = []
    for pair in selected_pairs:
        audit_pair = _find_overlap_pair(
            overlap_pairs,
            static_source_id=pair["staticSourceId"],
            static_source_name=pair["staticSourceName"],
            provider_source_id=pair["providerSourceId"],
            provider_source_name=pair["providerSourceName"],
        )
        if not audit_pair:
            audit_pair = build_provider_static_overlap_pair(
                static_source_id=pair["staticSourceId"],
                static_source_name=pair["staticSourceName"],
                provider_source_id=pair["providerSourceId"],
                provider_source_name=pair["providerSourceName"],
                source_state_rows=source_state_rows,
                canonical_rows=canonical_rows,
            )
        provider_row = source_state_rows.get(pair["providerSourceName"]) or source_state_rows.get(
            pair["providerSourceId"]
        )
        refreshed.append(
            build_static_suppression_policy_pair(
                static_source_id=pair["staticSourceId"],
                static_source_name=pair["staticSourceName"],
                provider_source_id=pair["providerSourceId"],
                provider_source_name=pair["providerSourceName"],
                provider_row=provider_row if isinstance(provider_row, dict) else {},
                decision=pair["decision"],
                reason=pair["reason"],
                audit_pair=audit_pair,
            )
        )
    return build_static_suppression_policy_summary(refreshed)
