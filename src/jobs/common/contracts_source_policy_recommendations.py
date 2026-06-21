"""Generated source-policy recommendation artifact helpers.

AI boundary owns: source-policy recommendation contract rows, summaries, and warning payloads.
AI boundary implement in: this file for recommendation artifact shape; review state and bridge route envelopes stay in sibling leaves.
AI boundary search before contracts: source-policy routes, review-state contracts, provider coverage, and recommendation tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused source-policy recommendation tests.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from src.jobs.common.contracts_redundant_static_proposals import (
    REDUNDANT_STATIC_PROPOSALS,
    REDUNDANT_STATIC_RECOMMENDED_ACTIONS,
    normalize_redundant_static_proposal_row,
)
from src.jobs.common.contracts_source_policy_review_state import (
    SOURCE_POLICY_MANUAL_SUPPRESSION_OVERRIDES,
    SOURCE_POLICY_REVIEW_STATES,
    find_source_policy_review_pair,
    source_policy_review_pair_public_fields,
)
from src.jobs.common.numbers import _clamped_int
from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import as_json_object, json_object_rows

SOURCE_POLICY_RECOMMENDATION_SCHEMA_VERSION = "1.0"
SOURCE_POLICY_RECOMMENDATIONS = frozenset(
    {
        "stable_safe_redundant",
        "needs_review",
        "static_only_detected",
        "needs_more_history",
        "keep_static",
    }
)
_HISTORY_LIMIT = 10


def _float_confidence(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = float(default)
    return round(max(0.0, min(1.0, parsed)), 2)


def _source_pair_key(row: dict[str, Any]) -> tuple[str, str]:
    static_key = norm_text(row.get("staticSourceId")) or norm_text(row.get("staticSourceName"))
    provider_key = norm_text(row.get("providerSourceId")) or norm_text(
        row.get("providerSourceName")
    )
    return static_key, provider_key


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [clean_text(item) for item in value if clean_text(item)]


def _proposal_value(value: Any) -> str:
    proposal = norm_text(value)
    return proposal if proposal in REDUNDANT_STATIC_PROPOSALS else "needs_more_history"


def _recommended_action(value: Any, default: str = "collect_more_history") -> str:
    action = norm_text(value)
    if action in REDUNDANT_STATIC_RECOMMENDED_ACTIONS:
        return action
    return default


def _recommendation_value(value: Any) -> str:
    recommendation = norm_text(value)
    return (
        recommendation if recommendation in SOURCE_POLICY_RECOMMENDATIONS else "needs_more_history"
    )


def _review_state_value(value: Any) -> str:
    state = norm_text(value)
    return state if state in SOURCE_POLICY_REVIEW_STATES else "new"


def _manual_override_value(value: Any) -> str:
    override = norm_text(value)
    return override if override in SOURCE_POLICY_MANUAL_SUPPRESSION_OVERRIDES else "none"


def _action_for_recommendation(recommendation: str) -> str:
    return {
        "stable_safe_redundant": "keep_runtime_suppression",
        "needs_review": "review_pair",
        "static_only_detected": "pause_suppression",
        "needs_more_history": "collect_more_history",
        "keep_static": "keep_static_active",
    }.get(recommendation, "collect_more_history")


def normalize_source_policy_recommendation_history_row(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    proposal = _proposal_value(src.get("proposal"))
    return {
        "observedAt": clean_text(src.get("observedAt")),
        "proposal": proposal,
        "recommendedAction": _recommended_action(src.get("recommendedAction")),
        "confidence": _float_confidence(src.get("confidence"), 0.0),
        "lastAuditStatus": clean_text(src.get("lastAuditStatus")),
        "providerCoverageStatus": clean_text(src.get("providerCoverageStatus")),
        "providerCoverageConsecutiveSuccesses": _clamped_int(
            src.get("providerCoverageConsecutiveSuccesses"), 0, 0
        ),
        "providerCoverageLatestKeptCount": _clamped_int(
            src.get("providerCoverageLatestKeptCount"), 0, 0
        ),
        "staticOnlyCount": _clamped_int(src.get("staticOnlyCount"), 0, 0),
        "overlapCount": _clamped_int(src.get("overlapCount"), 0, 0),
        "reasons": _string_list(src.get("reasons")),
    }


def _history_row_from_proposal(proposal_row: dict[str, Any], observed_at: str) -> dict[str, Any]:
    return normalize_source_policy_recommendation_history_row(
        {"observedAt": observed_at, **proposal_row}
    )


def normalize_source_policy_recommendation_pair(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    history = [
        normalize_source_policy_recommendation_history_row(row)
        for row in json_object_rows(src.get("history"))[-_HISTORY_LIMIT:]
    ]
    recommendation = _recommendation_value(src.get("currentRecommendation"))
    action = _recommended_action(
        src.get("currentRecommendedAction"), _action_for_recommendation(recommendation)
    )
    return {
        "staticSourceId": clean_text(src.get("staticSourceId")),
        "staticSourceName": clean_text(src.get("staticSourceName")),
        "providerSourceId": clean_text(src.get("providerSourceId")),
        "providerSourceName": clean_text(src.get("providerSourceName")),
        "currentRecommendation": recommendation,
        "currentRecommendedAction": action,
        "confidence": _float_confidence(src.get("confidence"), 0.0),
        "firstSeenAt": clean_text(src.get("firstSeenAt")),
        "lastSeenAt": clean_text(src.get("lastSeenAt")),
        "safeRunCount": _clamped_int(src.get("safeRunCount"), 0, 0),
        "consecutiveSafeRunCount": _clamped_int(src.get("consecutiveSafeRunCount"), 0, 0),
        "needsReviewRunCount": _clamped_int(src.get("needsReviewRunCount"), 0, 0),
        "staticOnlyDetectedRunCount": _clamped_int(src.get("staticOnlyDetectedRunCount"), 0, 0),
        "providerUnstableRunCount": _clamped_int(src.get("providerUnstableRunCount"), 0, 0),
        "needsMoreHistoryRunCount": _clamped_int(src.get("needsMoreHistoryRunCount"), 0, 0),
        "lastProposal": _proposal_value(src.get("lastProposal")),
        "lastAuditStatus": clean_text(src.get("lastAuditStatus")),
        "destructiveActionAllowed": False,
        "reviewState": _review_state_value(src.get("reviewState")),
        "manualSuppressionOverride": _manual_override_value(src.get("manualSuppressionOverride")),
        "snoozedUntil": clean_text(src.get("snoozedUntil")),
        "notes": clean_text(src.get("notes")),
        "reviewUpdatedAt": clean_text(src.get("reviewUpdatedAt")),
        "reviewUpdatedBy": clean_text(src.get("reviewUpdatedBy")),
        "history": history,
    }


def _summary_from_pairs(pairs: list[dict[str, Any]]) -> dict[str, int]:
    recommendation_counts = Counter(pair["currentRecommendation"] for pair in pairs)
    return {
        "totalPairs": len(pairs),
        "stableSafeCount": recommendation_counts.get("stable_safe_redundant", 0),
        "needsReviewCount": recommendation_counts.get("needs_review", 0),
        "staticOnlyDetectedCount": recommendation_counts.get("static_only_detected", 0),
        "unstableProviderCount": sum(
            1 for pair in pairs if int(pair.get("providerUnstableRunCount") or 0) > 0
        ),
        "moreHistoryCount": recommendation_counts.get("needs_more_history", 0),
    }


def normalize_source_policy_recommendations_artifact(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    pairs = [
        normalize_source_policy_recommendation_pair(row)
        for row in json_object_rows(src.get("pairs"))
        if _source_pair_key(as_json_object(row)) != ("", "")
    ]
    return {
        "schemaVersion": clean_text(src.get("schemaVersion"))
        or SOURCE_POLICY_RECOMMENDATION_SCHEMA_VERSION,
        "updatedAt": clean_text(src.get("updatedAt")),
        "summary": _summary_from_pairs(pairs),
        "pairs": pairs,
    }


def read_source_policy_recommendations_artifact(path: Path) -> tuple[dict[str, Any], str]:
    artifact_path = Path(path)
    if not artifact_path.exists():
        return normalize_source_policy_recommendations_artifact({}), "missing_prior_artifact"
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return normalize_source_policy_recommendations_artifact({}), "malformed_prior_artifact"
    if not isinstance(payload, dict):
        return normalize_source_policy_recommendations_artifact({}), "malformed_prior_artifact"
    return normalize_source_policy_recommendations_artifact(payload), ""


def _recommendation_for_pair(pair: dict[str, Any]) -> str:
    latest = norm_text(pair.get("lastProposal"))
    consecutive_safe = int(pair.get("consecutiveSafeRunCount") or 0)
    static_only_runs = int(pair.get("staticOnlyDetectedRunCount") or 0)
    provider_unstable_runs = int(pair.get("providerUnstableRunCount") or 0)
    if (
        latest == "safe_redundant_static"
        and consecutive_safe >= 3
        and static_only_runs == 0
        and provider_unstable_runs == 0
    ):
        return "stable_safe_redundant"
    if latest == "static_only_jobs_detected" or static_only_runs > 0:
        return "static_only_detected"
    if latest in {"needs_review", "provider_unstable"} or provider_unstable_runs > 0:
        return "needs_review"
    if latest == "keep_static":
        return "keep_static"
    return "needs_more_history"


def _updated_pair(
    prior_pair: dict[str, Any], proposal_row: dict[str, Any], *, observed_at: str
) -> dict[str, Any]:
    proposal = normalize_redundant_static_proposal_row(proposal_row)
    prior = normalize_source_policy_recommendation_pair(prior_pair)
    history = [
        *prior["history"],
        _history_row_from_proposal(proposal, observed_at),
    ][-_HISTORY_LIMIT:]
    proposal_value = proposal["proposal"]
    safe_run_count = int(prior["safeRunCount"]) + int(proposal_value == "safe_redundant_static")
    consecutive_safe = (
        int(prior["consecutiveSafeRunCount"]) + 1
        if proposal_value == "safe_redundant_static"
        else 0
    )
    next_pair = {
        "staticSourceId": proposal["staticSourceId"] or prior["staticSourceId"],
        "staticSourceName": proposal["staticSourceName"] or prior["staticSourceName"],
        "providerSourceId": proposal["providerSourceId"] or prior["providerSourceId"],
        "providerSourceName": proposal["providerSourceName"] or prior["providerSourceName"],
        "confidence": proposal["confidence"],
        "firstSeenAt": prior["firstSeenAt"] or observed_at,
        "lastSeenAt": observed_at,
        "safeRunCount": safe_run_count,
        "consecutiveSafeRunCount": consecutive_safe,
        "needsReviewRunCount": int(prior["needsReviewRunCount"])
        + int(proposal_value == "needs_review"),
        "staticOnlyDetectedRunCount": int(prior["staticOnlyDetectedRunCount"])
        + int(proposal_value == "static_only_jobs_detected"),
        "providerUnstableRunCount": int(prior["providerUnstableRunCount"])
        + int(proposal_value == "provider_unstable"),
        "needsMoreHistoryRunCount": int(prior["needsMoreHistoryRunCount"])
        + int(proposal_value == "needs_more_history"),
        "lastProposal": proposal_value,
        "lastAuditStatus": proposal["lastAuditStatus"],
        "destructiveActionAllowed": False,
        "history": history,
    }
    recommendation = _recommendation_for_pair(next_pair)
    next_pair["currentRecommendation"] = recommendation
    next_pair["currentRecommendedAction"] = _action_for_recommendation(recommendation)
    return normalize_source_policy_recommendation_pair(next_pair)


def build_source_policy_recommendations_artifact(
    *,
    prior_artifact: Any,
    redundant_static_proposals: Any,
    observed_at: str,
    review_state: Any = None,
) -> dict[str, Any]:
    prior = normalize_source_policy_recommendations_artifact(prior_artifact)
    indexed = {
        _source_pair_key(pair): pair
        for pair in prior["pairs"]
        if _source_pair_key(pair) != ("", "")
    }
    proposal_rows = json_object_rows(as_json_object(redundant_static_proposals).get("proposals"))
    for row in proposal_rows:
        proposal = normalize_redundant_static_proposal_row(row)
        key = _source_pair_key(proposal)
        if key == ("", ""):
            continue
        indexed[key] = _updated_pair(indexed.get(key, {}), proposal, observed_at=observed_at)
    pairs = []
    for key in sorted(indexed):
        pair = indexed[key]
        review_pair = find_source_policy_review_pair(
            review_state or {},
            static_source_id=pair["staticSourceId"],
            static_source_name=pair["staticSourceName"],
            provider_source_id=pair["providerSourceId"],
            provider_source_name=pair["providerSourceName"],
        )
        if review_pair:
            pair = {**pair, **source_policy_review_pair_public_fields(review_pair)}
        pairs.append(normalize_source_policy_recommendation_pair(pair))
    return {
        "schemaVersion": SOURCE_POLICY_RECOMMENDATION_SCHEMA_VERSION,
        "updatedAt": clean_text(observed_at),
        "summary": _summary_from_pairs(pairs),
        "pairs": pairs,
    }


def merge_source_policy_review_state_into_recommendations(
    *, recommendations_artifact: Any, review_state: Any
) -> dict[str, Any]:
    artifact = normalize_source_policy_recommendations_artifact(recommendations_artifact)
    pairs = []
    for pair in artifact["pairs"]:
        review_pair = find_source_policy_review_pair(
            review_state or {},
            static_source_id=pair["staticSourceId"],
            static_source_name=pair["staticSourceName"],
            provider_source_id=pair["providerSourceId"],
            provider_source_name=pair["providerSourceName"],
        )
        if review_pair:
            pair = {**pair, **source_policy_review_pair_public_fields(review_pair)}
        pairs.append(normalize_source_policy_recommendation_pair(pair))
    return {
        "schemaVersion": artifact["schemaVersion"],
        "updatedAt": artifact["updatedAt"],
        "summary": _summary_from_pairs(pairs),
        "pairs": pairs,
    }
