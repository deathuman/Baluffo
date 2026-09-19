#!/usr/bin/env python3
"""Source-sync and conservative static-cleanup proposals.

Leaf of ``scripts/source_policy_soak_report.py``; every unit body is byte-identical to the pre-split
module. The coordinator imports and re-exports these names.
"""

from __future__ import annotations

import hashlib
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
del _repo_root

from scripts.source_policy_soak_report_evidence import (
    _gate,
    _pair_key,
    _read_json_artifact,
    _source_identity_tokens,
)
from scripts.source_policy_soak_report_spec import (
    CONSERVATIVE_CLEANUP_EXAMPLE_LIMIT,
    CONSERVATIVE_CLEANUP_MIN_SAFE_RUNS,
    CONSERVATIVE_CLEANUP_PROPOSAL_STALE_AFTER_SECONDS,
    SOURCE_SYNC_ALLOWED_KEYS,
    SOURCE_SYNC_FORBIDDEN_TOKENS,
    _int_value,
    as_json_object,
    clean_text,
    json_object_rows,
    norm_text,
    normalize_source_policy_recommendations_artifact,
    normalize_source_policy_review_state_artifact,
)

__all__ = [
    "Any",
    "CONSERVATIVE_CLEANUP_EXAMPLE_LIMIT",
    "CONSERVATIVE_CLEANUP_MIN_SAFE_RUNS",
    "CONSERVATIVE_CLEANUP_PROPOSAL_STALE_AFTER_SECONDS",
    "Counter",
    "SOURCE_SYNC_ALLOWED_KEYS",
    "SOURCE_SYNC_FORBIDDEN_TOKENS",
    "UTC",
    "_active_static_row_by_token",
    "_backup_source_policy",
    "_cleanup_readiness_hash",
    "_cleanup_row_readiness_key",
    "_conservative_static_cleanup_proposals_section",
    "_find_forbidden_source_sync_tokens",
    "_gate",
    "_int_value",
    "_overlap_counts",
    "_pair_key",
    "_parse_iso_datetime",
    "_proposal_by_pair",
    "_read_json_artifact",
    "_source_identity_tokens",
    "_source_sync_section",
    "_suppression_evidence_for_pair",
    "_timestamp_age_seconds",
    "as_json_object",
    "clean_text",
    "datetime",
    "hashlib",
    "json",
    "json_object_rows",
    "norm_text",
    "normalize_source_policy_recommendations_artifact",
    "normalize_source_policy_review_state_artifact",
]


def _overlap_counts(overlap: dict[str, Any]) -> dict[str, int]:
    pairs = json_object_rows(overlap.get("pairs"))
    statuses = Counter(clean_text(pair.get("auditStatus")) for pair in pairs)
    return {
        "overlapSafeCount": int(overlap.get("safePairCount") or statuses.get("safe", 0)),
        "overlapNeedsReviewCount": int(
            overlap.get("needsReviewPairCount") or statuses.get("needs_review", 0)
        ),
        "overlapInsufficientHistoryCount": int(
            overlap.get("insufficientHistoryPairCount") or statuses.get("insufficient_history", 0)
        ),
    }


def _backup_source_policy(backup_payload_path: Path | None) -> tuple[dict[str, Any], str]:
    if backup_payload_path is None:
        return {
            "supplied": False,
            "status": "not_supplied",
            "sourcePolicyReviewPairs": 0,
            "sourcePolicyRecommendationPairs": 0,
        }, ""
    payload, status, warning = _read_json_artifact(backup_payload_path)
    if status != "ok":
        return {
            "supplied": True,
            "status": status,
            "path": str(backup_payload_path),
            "sourcePolicyReviewPairs": 0,
            "sourcePolicyRecommendationPairs": 0,
        }, warning
    counts = as_json_object(as_json_object(payload).get("counts"))
    source_policy = as_json_object(as_json_object(payload).get("sourcePolicy"))
    review = normalize_source_policy_review_state_artifact(source_policy.get("reviewState"))
    recommendations = normalize_source_policy_recommendations_artifact(
        source_policy.get("recommendations")
    )
    return {
        "supplied": True,
        "status": "ok",
        "path": str(backup_payload_path),
        "sourcePolicyReviewPairs": int(
            counts.get("sourcePolicyReviewPairs") or len(as_json_object(review.get("pairs")))
        ),
        "sourcePolicyRecommendationPairs": int(
            counts.get("sourcePolicyRecommendationPairs")
            or len(json_object_rows(recommendations.get("pairs")))
        ),
        "warnings": [
            clean_text(item) for item in source_policy.get("warnings", []) if clean_text(item)
        ]
        if isinstance(source_policy.get("warnings"), list)
        else [],
    }, ""


def _find_forbidden_source_sync_tokens(value: Any) -> list[str]:
    found: set[str] = set()

    def walk(item: Any) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                if key in SOURCE_SYNC_FORBIDDEN_TOKENS:
                    found.add(key)
                walk(child)
        elif isinstance(item, list):
            for child in item:
                walk(child)
        elif isinstance(item, str) and item in SOURCE_SYNC_FORBIDDEN_TOKENS:
            found.add(item)

    walk(value)
    return sorted(found)


def _source_sync_section(
    sync_payload: Any, status: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    sync = as_json_object(sync_payload)
    if status == "missing":
        return {"present": False, "clean": True, "unexpectedTopLevelKeys": []}, []
    if status == "malformed":
        return {"present": True, "clean": False, "unexpectedTopLevelKeys": []}, []
    top_keys = set(sync)
    unexpected = sorted(top_keys - SOURCE_SYNC_ALLOWED_KEYS)
    forbidden = _find_forbidden_source_sync_tokens(sync)
    gates: list[dict[str, Any]] = []
    if unexpected:
        gates.append(
            _gate(
                "source_sync_unexpected_top_level_keys",
                "failed",
                "source-sync.json contains non-registry top-level keys.",
                {"keys": unexpected},
            )
        )
    if forbidden:
        gates.append(
            _gate(
                "source_sync_contains_source_policy",
                "failed",
                "source-sync.json contains source-policy or review-state payload.",
                {"tokens": forbidden},
            )
        )
    return {
        "present": True,
        "clean": not unexpected and not forbidden,
        "unexpectedTopLevelKeys": unexpected,
        "forbiddenTokens": forbidden,
        "allowedTopLevelKeys": sorted(SOURCE_SYNC_ALLOWED_KEYS),
    }, gates


def _active_static_row_by_token(active_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    rows_by_token: dict[str, dict[str, Any]] = {}
    for row in active_rows:
        if clean_text(row.get("adapter")) != "static":
            continue
        for token in _source_identity_tokens(row):
            rows_by_token.setdefault(token, row)
    return rows_by_token


def _proposal_by_pair(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {_pair_key(row): row for row in rows if _pair_key(row) != "||"}


def _suppression_evidence_for_pair(
    pair_key: str,
    *,
    suppressed_pairs: list[dict[str, Any]],
    suppression_eligibility: dict[str, Any],
) -> tuple[str, str]:
    if pair_key in {_pair_key(row) for row in suppressed_pairs}:
        return "observed_dynamic_suppression", "dynamic_redundant_provider"
    for row in json_object_rows(suppression_eligibility.get("missingLinkedStaticRows")):
        if _pair_key(row) == pair_key:
            reason = clean_text(row.get("selectionReason")) or clean_text(row.get("reason"))
            if reason:
                return "suppression_absence_explained", reason
    return "", ""


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _timestamp_age_seconds(previous: str, current: str) -> int | None:
    prior = _parse_iso_datetime(previous)
    current_dt = _parse_iso_datetime(current)
    if prior is None or current_dt is None:
        return None
    return max(0, int((current_dt - prior).total_seconds()))


def _cleanup_row_readiness_key(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "staticSourceId": clean_text(row.get("staticSourceId")),
        "providerSourceId": clean_text(row.get("providerSourceId")),
        "proposalDisposition": clean_text(row.get("proposalDisposition")),
        "proposalReadiness": clean_text(row.get("proposalReadiness")),
        "proposalReadinessReason": clean_text(row.get("proposalReadinessReason")),
        "blockers": [
            clean_text(reason) for reason in row.get("blockers", []) if clean_text(reason)
        ],
    }


def _cleanup_readiness_hash(
    *,
    proposal_generated_at: str,
    proposal_report_run_id: str,
    proposal_freshness_status: str,
    source_sync_clean: bool,
    rows: list[dict[str, Any]],
) -> str:
    payload = {
        "proposalGeneratedAt": clean_text(proposal_generated_at),
        "proposalReportRunId": clean_text(proposal_report_run_id),
        "proposalFreshnessStatus": clean_text(proposal_freshness_status),
        "sourceSyncClean": bool(source_sync_clean),
        "rows": [_cleanup_row_readiness_key(row) for row in rows],
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )
    ).hexdigest()
    return digest


def _conservative_static_cleanup_proposals_section(
    *,
    recommendation_pairs: list[dict[str, Any]],
    proposal_rows: list[dict[str, Any]],
    suppressed_pairs: list[dict[str, Any]],
    suppression_eligibility: dict[str, Any],
    active_rows: list[dict[str, Any]],
    source_sync: dict[str, Any],
    proposal_generated_at: str,
    proposal_report_run_id: str,
    report_generated_at: str,
) -> dict[str, Any]:
    active_static_by_token = _active_static_row_by_token(active_rows)
    current_proposals_by_pair = _proposal_by_pair(proposal_rows)
    proposals: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    freshness_age_seconds = _timestamp_age_seconds(proposal_generated_at, report_generated_at)
    freshness_status = (
        "stale"
        if freshness_age_seconds is None
        or freshness_age_seconds > CONSERVATIVE_CLEANUP_PROPOSAL_STALE_AFTER_SECONDS
        else "fresh"
    )

    for pair in recommendation_pairs:
        if clean_text(pair.get("currentRecommendation")) != "stable_safe_redundant":
            continue
        pair_key = _pair_key(pair)
        static_id = clean_text(pair.get("staticSourceId"))
        active_static = active_static_by_token.get(static_id)
        current = current_proposals_by_pair.get(pair_key, {})
        suppression_status, suppression_reason = _suppression_evidence_for_pair(
            pair_key,
            suppressed_pairs=suppressed_pairs,
            suppression_eligibility=suppression_eligibility,
        )
        blockers: list[str] = []
        if int(pair.get("consecutiveSafeRunCount") or 0) < CONSERVATIVE_CLEANUP_MIN_SAFE_RUNS:
            blockers.append("insufficient_clean_soak_runs")
        if int(pair.get("staticOnlyDetectedRunCount") or 0) > 0:
            blockers.append("static_only_evidence_present")
        if not bool(source_sync.get("clean")):
            blockers.append("source_sync_not_clean")
        if not active_static:
            blockers.append("static_source_not_active")
        elif clean_text(active_static.get("adapter")) != "static":
            blockers.append("static_source_adapter_not_static")
        if not suppression_status:
            blockers.append("dynamic_suppression_not_observed_or_explained")

        row = {
            "staticSourceId": static_id,
            "staticSourceName": clean_text(pair.get("staticSourceName")),
            "providerSourceId": clean_text(pair.get("providerSourceId")),
            "providerSourceName": clean_text(pair.get("providerSourceName")),
            "proposalDisposition": "blocked" if blockers else "proposal_ready",
            "proposalReadiness": (
                "blocked"
                if blockers
                else ("stale" if freshness_status == "stale" else "actionable")
            ),
            "proposalReadinessReason": (
                ", ".join(blockers)
                if blockers
                else (
                    "proposal evidence is stale; refresh the cleanup proposal report before taking action"
                    if freshness_status == "stale"
                    else "proposal evidence is fresh and actionable"
                )
            ),
            "proposalReadinessEvidence": [
                *(
                    [f"blocker:{reason}" for reason in blockers]
                    if blockers
                    else [f"proposal_freshness:{freshness_status}"]
                ),
                f"proposal_disposition:{'blocked' if blockers else 'proposal_ready'}",
            ],
            "proposal": "conservative_static_cleanup_candidate",
            "recommendedAction": "move_static_to_hidden_pending",
            "destructiveActionAllowed": False,
            "requiresExplicitAdminAction": True,
            "decisionLogEvidenceRequired": True,
            "requiredCleanRunCount": CONSERVATIVE_CLEANUP_MIN_SAFE_RUNS,
            "cleanRunEvidenceCount": int(pair.get("consecutiveSafeRunCount") or 0),
            "safeRunCount": int(pair.get("safeRunCount") or 0),
            "staticOnlyDetectedRunCount": int(pair.get("staticOnlyDetectedRunCount") or 0),
            "sourceSyncClean": bool(source_sync.get("clean")),
            "suppressionEvidenceStatus": suppression_status,
            "suppressionEvidenceReason": suppression_reason,
            "proposalGeneratedAt": clean_text(proposal_generated_at),
            "proposalReportRunId": clean_text(proposal_report_run_id),
            "proposalFreshnessStatus": freshness_status,
            "proposalFreshnessAgeSeconds": freshness_age_seconds,
            "lastProposal": clean_text(pair.get("lastProposal")),
            "lastAuditStatus": clean_text(pair.get("lastAuditStatus"))
            or clean_text(current.get("lastAuditStatus")),
            "providerCoverageStatus": clean_text(current.get("providerCoverageStatus")),
            "providerCoverageConsecutiveSuccesses": _int_value(
                current.get("providerCoverageConsecutiveSuccesses")
            ),
            "providerCoverageLatestKeptCount": _int_value(
                current.get("providerCoverageLatestKeptCount")
            ),
            "overlapCount": _int_value(current.get("overlapCount")),
            "staticOnlyCount": _int_value(current.get("staticOnlyCount")),
            "evidenceReasons": [
                reason
                for reason in (
                    "source_policy_recommendation_stable_safe_redundant",
                    "consecutive_safe_run_threshold_met"
                    if int(pair.get("consecutiveSafeRunCount") or 0)
                    >= CONSERVATIVE_CLEANUP_MIN_SAFE_RUNS
                    else "",
                    "source_sync_clean" if bool(source_sync.get("clean")) else "",
                    suppression_status,
                    "static_only_evidence_absent"
                    if int(pair.get("staticOnlyDetectedRunCount") or 0) == 0
                    else "",
                )
                if reason
            ],
            "blockers": blockers,
        }
        if blockers:
            blocked.append(row)
        else:
            proposals.append(row)

    def row_sort_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
        blocker_text = "|".join(clean_text(item) for item in row.get("blockers", []) if item)
        provider_key = norm_text(row.get("providerSourceName")) or norm_text(
            row.get("providerSourceId")
        )
        static_key = norm_text(row.get("staticSourceName")) or norm_text(row.get("staticSourceId"))
        return (
            blocker_text,
            provider_key,
            static_key,
            clean_text(row.get("recommendedAction")),
        )

    proposals.sort(key=row_sort_key)
    blocked.sort(key=row_sort_key)
    blocked_reason_counts = Counter(
        clean_text(reason)
        for row in blocked
        for reason in row.get("blockers", [])
        if clean_text(reason)
    )
    proposal_readiness_rows = [*proposals, *blocked]
    readiness_hash = _cleanup_readiness_hash(
        proposal_generated_at=proposal_generated_at,
        proposal_report_run_id=proposal_report_run_id,
        proposal_freshness_status=freshness_status,
        source_sync_clean=bool(source_sync.get("clean")),
        rows=proposal_readiness_rows,
    )

    return {
        "minimumCleanRunCount": CONSERVATIVE_CLEANUP_MIN_SAFE_RUNS,
        "totalCandidateCount": len(proposals) + len(blocked),
        "proposalCount": len(proposals),
        "blockedCount": len(blocked),
        "staleCount": sum(
            1 for row in proposals if clean_text(row.get("proposalReadiness")) == "stale"
        ),
        "proposalGeneratedAt": clean_text(proposal_generated_at),
        "proposalReportRunId": clean_text(proposal_report_run_id),
        "proposalFreshnessStatus": freshness_status,
        "proposalFreshnessAgeSeconds": freshness_age_seconds,
        "proposalStaleThresholdSeconds": CONSERVATIVE_CLEANUP_PROPOSAL_STALE_AFTER_SECONDS,
        "proposalReadinessHash": readiness_hash,
        "blockedReasonCounts": dict(
            sorted(blocked_reason_counts.items(), key=lambda item: (-item[1], item[0]))
        ),
        "proposals": proposals,
        "blockedCandidates": blocked,
        "proposalReadyExamples": proposals[:CONSERVATIVE_CLEANUP_EXAMPLE_LIMIT],
        "blockedExamples": blocked[:CONSERVATIVE_CLEANUP_EXAMPLE_LIMIT],
    }
