#!/usr/bin/env python3
"""Read-only source-policy/runtime evidence soak report."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.jobs.common.contracts_provider_coverage import normalize_provider_coverage_payload
from src.jobs.common.contracts_provider_static_overlap import (
    normalize_provider_static_overlap_payload,
)
from src.jobs.common.contracts_redundant_static_proposals import (
    normalize_redundant_static_proposals_payload,
)
from src.jobs.common.contracts_source_policy_recommendations import (
    normalize_source_policy_recommendations_artifact,
)
from src.jobs.common.contracts_source_policy_review_state import (
    normalize_source_policy_review_state_artifact,
)
from src.jobs.common.contracts_static_suppression_policy import (
    normalize_static_suppression_policy_payload,
)
from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import as_json_object, json_object_rows
from src.shared.utils import now_iso
from src.source_discovery.config import SUPPORTED_PROVIDERS
from src.source_discovery.provider_migration_advisory import enrich_provider_migration_rows

SCHEMA_VERSION = "1.0"
JSON_REPORT_NAME = "source-policy-soak-report.json"
MARKDOWN_REPORT_NAME = "source-policy-soak-report.md"

ARTIFACT_PATHS = {
    "sourceDiscoveryReport": "source-discovery-report.json",
    "sourceDiscoveryCandidates": "source-discovery-candidates.json",
    "jobsFetchReport": "jobs-fetch-report.json",
    "jobsSourceState": "jobs-source-state.json",
    "sourcePolicyRecommendations": "source-policy-recommendations.json",
    "sourcePolicyReviewState": "source-policy-review-state.json",
    "sourceRegistryActive": "source-registry-active.json",
    "sourceRegistryPending": "source-registry-pending.json",
    "sourceRegistryRejected": "source-registry-rejected.json",
    "sourceRegistryTombstones": "source-registry-tombstones.json",
    "sourceSync": "source-sync.json",
}

SOURCE_SYNC_ALLOWED_KEYS = {"schemaVersion", "generatedAt", "source", "active", "pending"}
SOURCE_SYNC_FORBIDDEN_TOKENS = {
    "sourcePolicy",
    "sourcePolicyReviewState",
    "sourcePolicyRecommendations",
    "reviewState",
    "manualSuppressionOverride",
    "force_pause",
    "recommendations",
    "redundantStaticProposals",
}
PROVIDER_MIGRATION_ACTIONS = {
    "add_provider_source",
    "review_provider_migration",
    "already_covered_by_provider",
    "unsupported_provider",
    "needs_probe",
    "keep_static",
    "insufficient_evidence",
}


def _read_json_artifact(path: Path) -> tuple[Any, str, str]:
    if not path.exists():
        return {}, "missing", ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {}, "malformed", f"{path.name} is malformed: {exc}"
    return payload, "ok", ""


def _artifact_inputs(data_dir: Path) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    payloads: dict[str, Any] = {}
    inputs: dict[str, Any] = {}
    warnings: list[str] = []
    for key, filename in ARTIFACT_PATHS.items():
        path = data_dir / filename
        payload, status, warning = _read_json_artifact(path)
        payloads[key] = payload
        inputs[key] = {"path": str(path), "status": status}
        if warning:
            warnings.append(warning)
    if inputs["jobsFetchReport"]["status"] == "missing":
        warnings.append("jobs-fetch-report.json is missing; runtime fetch evidence is unavailable.")
    return payloads, inputs, warnings


def _gate(
    gate_id: str,
    status: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": gate_id,
        "status": status,
        "message": message,
        "details": details or {},
    }


def _warning_gate(
    gate_id: str, message: str, details: dict[str, Any] | None = None
) -> dict[str, Any]:
    return _gate(gate_id, "warning", message, details)


def _source_state_rows(payload: Any) -> dict[str, dict[str, Any]]:
    sources = as_json_object(payload).get("sources")
    if not isinstance(sources, dict):
        return {}
    return {clean_text(key): value for key, value in sources.items() if isinstance(value, dict)}


def _list_rows(payload: Any) -> list[dict[str, Any]]:
    return json_object_rows(payload)


def _discovery_rows(report_payload: Any, candidates_payload: Any) -> list[dict[str, Any]]:
    rows = []
    rows.extend(json_object_rows(as_json_object(report_payload).get("candidates")))
    rows.extend(_list_rows(candidates_payload))
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for row in rows:
        key = (
            clean_text(row.get("sourceId"))
            or clean_text(row.get("name"))
            or json.dumps(row, sort_keys=True)
        )
        if key not in seen:
            seen.add(key)
            unique.append(row)
    return unique


def _source_name(row: dict[str, Any]) -> str:
    return clean_text(row.get("name") or row.get("source") or row.get("sourceId"))


def _source_identity_tokens(row: dict[str, Any]) -> set[str]:
    tokens = {
        clean_text(row.get("id")),
        clean_text(row.get("sourceId")),
        clean_text(row.get("name")),
        clean_text(row.get("source")),
        clean_text(row.get("sourceIdentity")),
        clean_text(row.get("migrationSourceIdentity")),
    }
    return {token for token in tokens if token}


def _pair_key(row: dict[str, Any]) -> tuple[str, str]:
    return (
        norm_text(row.get("staticSourceId")) or norm_text(row.get("staticSourceName")),
        norm_text(row.get("providerSourceId")) or norm_text(row.get("providerSourceName")),
    )


def _policy_pairs(policy: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        *json_object_rows(policy.get("suppressedPairs")),
        *json_object_rows(policy.get("pausedPairs")),
        *json_object_rows(policy.get("warningPairs")),
    ]


def _provider_counts(provider_coverage: dict[str, Any]) -> dict[str, int]:
    status_counts = as_json_object(provider_coverage.get("statusCounts"))
    validated = int(status_counts.get("validated_provider") or 0)
    unstable = int(status_counts.get("unstable_provider") or 0)
    failed = int(status_counts.get("failed_provider") or 0)
    return {
        "validatedProviderCount": validated,
        "unstableFailedProviderCount": unstable + failed,
    }


def _source_token_index(rows: list[dict[str, Any]]) -> set[str]:
    tokens: set[str] = set()
    for row in rows:
        tokens.update(_source_identity_tokens(row))
    return tokens


def _source_state_token_index(source_state_rows: dict[str, dict[str, Any]]) -> set[str]:
    tokens: set[str] = set()
    for name, row in source_state_rows.items():
        tokens.add(clean_text(name))
        tokens.update(_source_identity_tokens(as_json_object(row)))
    return {token for token in tokens if token}


def _provider_coverage_status_by_token(
    provider_coverage: dict[str, Any],
    source_state_rows: dict[str, dict[str, Any]],
) -> dict[str, str]:
    status_by_token: dict[str, str] = {}
    for name, raw in source_state_rows.items():
        row = as_json_object(raw)
        status = clean_text(row.get("providerCoverageStatus"))
        if not status:
            continue
        for token in {clean_text(name), *_source_identity_tokens(row)}:
            if token:
                status_by_token[token] = status
    for key in (
        "probingProviders",
        "validatedProviders",
        "unstableOrFailedProviders",
        "needsReviewProviders",
        "readyLaterProviders",
    ):
        for row in json_object_rows(provider_coverage.get(key)):
            status = clean_text(row.get("providerCoverageStatus"))
            if not status:
                continue
            for token in _source_identity_tokens(row):
                status_by_token[token] = status
    return status_by_token


def _provider_migration_activation_section(
    *,
    discovery_candidates: list[dict[str, Any]],
    active_rows: list[dict[str, Any]],
    pending_rows: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    source_state_rows: dict[str, dict[str, Any]],
    provider_coverage: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    enriched_candidates = enrich_provider_migration_rows(
        discovery_candidates,
        active_rows=active_rows,
        pending_rows=pending_rows,
    )
    action_counts = Counter(
        clean_text(row.get("recommendedAction"))
        for row in enriched_candidates
        if clean_text(row.get("recommendedAction")) in PROVIDER_MIGRATION_ACTIONS
    )
    advisory_total = sum(action_counts.values())
    staged_candidates = [
        row
        for row in discovery_candidates
        if clean_text(row.get("candidateState")) == "staged_provider_candidate"
        or bool(row.get("createdFromAdvisory"))
    ]
    pending_provider_migration = [
        row
        for row in pending_rows
        if clean_text(row.get("pendingReason")) == "provider_migration_candidate"
    ]
    fetched_tokens = _source_token_index(source_rows) | _source_state_token_index(source_state_rows)
    coverage_status_by_token = _provider_coverage_status_by_token(
        provider_coverage, source_state_rows
    )
    fetched_count = 0
    validated_count = 0
    for row in pending_provider_migration:
        tokens = _source_identity_tokens(row)
        if tokens & fetched_tokens:
            fetched_count += 1
        if any(coverage_status_by_token.get(token) == "validated_provider" for token in tokens):
            validated_count += 1

    supported_providers = {clean_text(provider) for provider in SUPPORTED_PROVIDERS}
    active_provider_without_identity = [
        row
        for row in active_rows
        if clean_text(row.get("adapter")) in supported_providers
        and not clean_text(row.get("migrationSourceIdentity"))
    ]
    section = {
        "advisoryTotalCandidates": advisory_total,
        "addProviderSourceCount": int(action_counts.get("add_provider_source", 0)),
        "reviewProviderMigrationCount": int(action_counts.get("review_provider_migration", 0)),
        "stagedProviderCandidateCount": len(staged_candidates),
        "pendingProviderMigrationCandidateCount": len(pending_provider_migration),
        "duplicateActiveSkippedCount": sum(
            1 for row in enriched_candidates if bool(row.get("duplicateOfActiveSource"))
        ),
        "duplicatePendingSkippedCount": sum(
            1 for row in enriched_candidates if bool(row.get("duplicateOfPendingSource"))
        ),
        "unsupportedProviderCount": int(action_counts.get("unsupported_provider", 0)),
        "insufficientEvidenceCount": int(action_counts.get("insufficient_evidence", 0)),
        "needsProbeCount": int(action_counts.get("needs_probe", 0)),
        "activeProviderWithoutMigrationIdentityCount": len(active_provider_without_identity),
        "providerMigrationCandidatesFetchedCount": fetched_count,
        "providerMigrationCandidatesValidatedCount": validated_count,
        "providerMigrationCandidatesNoFetchCount": max(
            0, len(pending_provider_migration) - fetched_count
        ),
        "actionCounts": dict(sorted(action_counts.items())),
    }
    gates: list[dict[str, Any]] = []
    actionable_advisory_count = (
        section["addProviderSourceCount"] + section["reviewProviderMigrationCount"]
    )
    if actionable_advisory_count > 0 and section["stagedProviderCandidateCount"] == 0:
        gates.append(
            _warning_gate(
                "provider_advisory_without_staging",
                "Provider migration advisory found actionable candidates but none were staged.",
                {"actionableAdvisoryCount": actionable_advisory_count},
            )
        )
    if (
        section["stagedProviderCandidateCount"] > 0
        and section["pendingProviderMigrationCandidateCount"] == 0
    ):
        gates.append(
            _warning_gate(
                "staged_provider_without_pending",
                "Staged provider migration candidates exist but none are pending.",
                {"stagedProviderCandidateCount": section["stagedProviderCandidateCount"]},
            )
        )
    if (
        section["pendingProviderMigrationCandidateCount"] > 0
        and section["providerMigrationCandidatesFetchedCount"] == 0
    ):
        gates.append(
            _warning_gate(
                "pending_provider_migration_not_fetched",
                "Pending provider migration candidates exist but none have fetch evidence.",
                {
                    "pendingProviderMigrationCandidateCount": section[
                        "pendingProviderMigrationCandidateCount"
                    ]
                },
            )
        )
    if section["activeProviderWithoutMigrationIdentityCount"] > 0:
        gates.append(
            _warning_gate(
                "active_provider_without_migration_identity",
                "Active provider rows lack migrationSourceIdentity and cannot drive static coverage.",
                {
                    "activeProviderWithoutMigrationIdentityCount": section[
                        "activeProviderWithoutMigrationIdentityCount"
                    ]
                },
            )
        )
    insufficient_or_probe = section["insufficientEvidenceCount"] + section["needsProbeCount"]
    if advisory_total > 0 and insufficient_or_probe > (advisory_total / 2):
        gates.append(
            _warning_gate(
                "provider_migration_mostly_insufficient_or_probe",
                "Most provider migration advisory candidates need more evidence or probing.",
                {
                    "advisoryTotalCandidates": advisory_total,
                    "insufficientOrProbeCount": insufficient_or_probe,
                },
            )
        )
    return section, gates


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


def _build_sections(
    payloads: dict[str, Any],
    backup_payload_path: Path | None,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    gates: list[dict[str, Any]] = []
    source_state_rows = _source_state_rows(payloads["jobsSourceState"])
    fetch_report = as_json_object(payloads["jobsFetchReport"])
    source_rows = json_object_rows(fetch_report.get("sources"))
    provider_coverage = normalize_provider_coverage_payload(
        fetch_report.get("providerCoverage"), source_state_rows
    )
    overlap = normalize_provider_static_overlap_payload(
        fetch_report.get("providerStaticOverlap"), source_rows=source_rows
    )
    policy = normalize_static_suppression_policy_payload(
        fetch_report.get("staticSuppressionPolicy")
    )
    proposals = normalize_redundant_static_proposals_payload(
        fetch_report.get("redundantStaticProposals")
    )
    recommendations = normalize_source_policy_recommendations_artifact(
        payloads["sourcePolicyRecommendations"]
    )
    review_state = normalize_source_policy_review_state_artifact(
        payloads["sourcePolicyReviewState"]
    )
    backup, backup_warning = _backup_source_policy(backup_payload_path)
    if backup_warning:
        warnings.append(backup_warning)

    discovery_candidates = _discovery_rows(
        payloads["sourceDiscoveryReport"], payloads["sourceDiscoveryCandidates"]
    )
    active_rows = _list_rows(payloads["sourceRegistryActive"])
    pending_rows = _list_rows(payloads["sourceRegistryPending"])
    provider_migration_activation, activation_gates = _provider_migration_activation_section(
        discovery_candidates=discovery_candidates,
        active_rows=active_rows,
        pending_rows=pending_rows,
        source_rows=source_rows,
        source_state_rows=source_state_rows,
        provider_coverage=provider_coverage,
    )
    gates.extend(activation_gates)
    staged_provider_candidates_count = int(
        provider_migration_activation.get("stagedProviderCandidateCount") or 0
    )
    pending_provider_migration_count = int(
        provider_migration_activation.get("pendingProviderMigrationCandidateCount") or 0
    )
    source_sync, sync_gates = _source_sync_section(
        payloads["sourceSync"], clean_text(payloads.get("_sourceSyncStatus"))
    )
    gates.extend(sync_gates)

    suppressed_pairs = json_object_rows(policy.get("suppressedPairs"))
    paused_pairs = json_object_rows(policy.get("pausedPairs"))
    warning_pairs = json_object_rows(policy.get("warningPairs"))
    proposal_rows = json_object_rows(proposals.get("proposals"))
    recommendation_pairs = json_object_rows(recommendations.get("pairs"))
    review_pairs = list(as_json_object(review_state.get("pairs")).values())

    static_registry_tokens: set[str] = set()
    for row in active_rows + pending_rows + _list_rows(payloads["sourceRegistryRejected"]):
        if clean_text(row.get("adapter")) == "static":
            static_registry_tokens.update(_source_identity_tokens(row))
    missing_static_pairs = [
        pair
        for pair in suppressed_pairs
        if clean_text(pair.get("staticSourceId")) not in static_registry_tokens
        and clean_text(pair.get("staticSourceName")) not in static_registry_tokens
    ]
    if missing_static_pairs:
        gates.append(
            _warning_gate(
                "static_rows_missing_after_suppression",
                "Suppressed static pairs are not visible in registry rows.",
                {"pairs": [_pair_key(pair) for pair in missing_static_pairs]},
            )
        )
    if int(policy.get("suppressedCount") or 0) > 0 and not as_json_object(
        fetch_report.get("providerCoverage")
    ):
        gates.append(
            _warning_gate(
                "suppression_without_provider_coverage",
                "Dynamic suppression is present but providerCoverage is missing from the fetch report.",
                {"suppressedCount": int(policy.get("suppressedCount") or 0)},
            )
        )
    suppressed_keys = {_pair_key(pair) for pair in suppressed_pairs}
    static_only_suppressed = [
        row
        for row in proposal_rows
        if clean_text(row.get("proposal")) == "static_only_jobs_detected"
        and _pair_key(row) in suppressed_keys
    ]
    if static_only_suppressed:
        gates.append(
            _warning_gate(
                "static_only_detected_while_suppressed",
                "Static-only evidence exists while the matching pair is still suppressed.",
                {"pairs": [_pair_key(row) for row in static_only_suppressed]},
            )
        )
    paused_keys = {_pair_key(pair) for pair in paused_pairs}
    force_pause_mismatches = [
        row
        for row in review_pairs
        if as_json_object(row).get("manualSuppressionOverride") == "force_pause"
        and _pair_key(as_json_object(row)) not in paused_keys
    ]
    if force_pause_mismatches:
        gates.append(
            _warning_gate(
                "force_pause_not_paused",
                "Manual force_pause review state is present without a matching paused policy pair.",
                {"pairs": [_pair_key(as_json_object(row)) for row in force_pause_mismatches]},
            )
        )

    provider_counts = _provider_counts(provider_coverage)
    overlap_counts = _overlap_counts(overlap)
    sections = {
        "discoveryProviderStaging": {
            "stagedProviderCandidatesCount": staged_provider_candidates_count,
            "pendingProviderMigrationCandidateCount": pending_provider_migration_count,
        },
        "providerMigrationActivation": provider_migration_activation,
        "providerCoverageValidation": {
            **provider_counts,
            "totalProviderCandidates": int(provider_coverage.get("totalProviderCandidates") or 0),
            "statusCounts": as_json_object(provider_coverage.get("statusCounts")),
        },
        "dynamicStaticSuppression": {
            "suppressedCount": int(policy.get("suppressedCount") or 0),
            "excludedSourceRowsCount": sum(
                1
                for row in source_rows
                if clean_text(row.get("exclusionReason")) == "dynamic_redundant_provider"
            ),
        },
        "providerStaticOverlapAudit": {
            **overlap_counts,
            "auditedPairCount": int(overlap.get("auditedPairCount") or 0),
        },
        "staticSuppressionSafetyPolicy": {
            "eligibleCount": int(policy.get("eligibleCount") or 0),
            "suppressedCount": int(policy.get("suppressedCount") or 0),
            "pausedCount": int(policy.get("pausedCount") or 0),
            "warningCount": int(policy.get("warningCount") or 0),
        },
        "redundantStaticProposals": {
            "totalProposalCount": int(proposals.get("totalProposalCount") or 0),
            "safeRedundantCount": int(proposals.get("safeRedundantCount") or 0),
            "needsReviewCount": int(proposals.get("needsReviewCount") or 0),
            "providerUnstableCount": int(proposals.get("providerUnstableCount") or 0),
            "staticOnlyDetectedCount": int(proposals.get("staticOnlyDetectedCount") or 0),
        },
        "sourcePolicyRecommendations": {
            "stableSafeRedundantCount": int(
                as_json_object(recommendations.get("summary")).get("stableSafeCount") or 0
            ),
            "totalPairs": len(recommendation_pairs),
        },
        "reviewStateOverrides": {
            "forcePauseOverrideCount": int(
                as_json_object(review_state.get("summary")).get("forcePausedCount") or 0
            ),
            "totalPairs": len(review_pairs),
        },
        "backupSourcePolicy": backup,
        "sourceSyncCleanliness": source_sync,
    }
    summary = {
        "stagedProviderCandidatesCount": staged_provider_candidates_count,
        "pendingProviderMigrationCandidateCount": pending_provider_migration_count,
        "advisoryTotalCandidates": int(
            provider_migration_activation.get("advisoryTotalCandidates") or 0
        ),
        "addProviderSourceCount": int(
            provider_migration_activation.get("addProviderSourceCount") or 0
        ),
        "reviewProviderMigrationCount": int(
            provider_migration_activation.get("reviewProviderMigrationCount") or 0
        ),
        "duplicateActiveSkippedCount": int(
            provider_migration_activation.get("duplicateActiveSkippedCount") or 0
        ),
        "duplicatePendingSkippedCount": int(
            provider_migration_activation.get("duplicatePendingSkippedCount") or 0
        ),
        "unsupportedProviderCount": int(
            provider_migration_activation.get("unsupportedProviderCount") or 0
        ),
        "insufficientEvidenceCount": int(
            provider_migration_activation.get("insufficientEvidenceCount") or 0
        ),
        "needsProbeCount": int(provider_migration_activation.get("needsProbeCount") or 0),
        "activeProviderWithoutMigrationIdentityCount": int(
            provider_migration_activation.get("activeProviderWithoutMigrationIdentityCount") or 0
        ),
        "providerMigrationCandidatesFetchedCount": int(
            provider_migration_activation.get("providerMigrationCandidatesFetchedCount") or 0
        ),
        "providerMigrationCandidatesValidatedCount": int(
            provider_migration_activation.get("providerMigrationCandidatesValidatedCount") or 0
        ),
        "providerMigrationCandidatesNoFetchCount": int(
            provider_migration_activation.get("providerMigrationCandidatesNoFetchCount") or 0
        ),
        **provider_counts,
        "dynamicRedundantStaticSuppressedCount": int(policy.get("suppressedCount") or 0),
        "suppressionPausedCount": int(policy.get("pausedCount") or 0),
        "suppressionWarningCount": int(policy.get("warningCount") or 0),
        **overlap_counts,
        "redundantProposalCount": int(proposals.get("totalProposalCount") or 0),
        "stableSafeRedundantRecommendationCount": int(
            sections["sourcePolicyRecommendations"]["stableSafeRedundantCount"]
        ),
        "forcePauseOverrideCount": int(sections["reviewStateOverrides"]["forcePauseOverrideCount"]),
        "sourcePolicyReviewPairs": int(backup.get("sourcePolicyReviewPairs") or 0),
        "sourcePolicyRecommendationPairs": int(backup.get("sourcePolicyRecommendationPairs") or 0),
        "sourceSyncClean": bool(source_sync.get("clean", True)),
    }
    return sections, summary, gates, warnings


def build_soak_report(data_dir: Path, backup_payload_path: Path | None = None) -> dict[str, Any]:
    payloads, inputs, warnings = _artifact_inputs(Path(data_dir))
    payloads["_sourceSyncStatus"] = inputs["sourceSync"]["status"]
    gates = [
        _warning_gate(
            "malformed_artifact",
            "An input artifact is malformed and was treated as empty.",
            {"artifact": key, "path": value["path"]},
        )
        for key, value in inputs.items()
        if value["status"] == "malformed"
    ]
    sections, summary, section_gates, section_warnings = _build_sections(
        payloads, backup_payload_path
    )
    gates.extend(section_gates)
    warnings.extend(section_warnings)
    if inputs["jobsFetchReport"]["status"] == "missing":
        gates.append(
            _warning_gate(
                "missing_jobs_fetch_report",
                "jobs-fetch-report.json is missing; fetch-runtime evidence is incomplete.",
            )
        )
    status = "ok"
    if any(gate["status"] == "failed" for gate in gates):
        status = "failed"
    elif warnings or any(gate["status"] == "warning" for gate in gates):
        status = "warning"
    return {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": now_iso(),
        "status": status,
        "mutation": {"readOnly": True, "writesOutsideOut": False},
        "inputs": inputs,
        "summary": summary,
        "sections": sections,
        "qualityGates": gates,
        "warnings": warnings,
    }


def _markdown_table(rows: list[tuple[str, Any]]) -> list[str]:
    lines = ["| Metric | Value |", "|--------|-------|"]
    for key, value in rows:
        lines.append(f"| `{key}` | `{value}` |")
    return lines


def render_markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# Source Policy Soak Report",
        "",
        f"- Status: `{report.get('status')}`",
        f"- Generated at: `{report.get('generatedAt')}`",
        "- Mutation: read-only, writes outside `_out/`: `false`",
        "",
        "## Summary",
        "",
        *_markdown_table(list(as_json_object(report.get("summary")).items())),
        "",
        "## Provider Migration Activation",
        "",
        *_markdown_table(
            list(
                as_json_object(
                    as_json_object(report.get("sections")).get("providerMigrationActivation")
                ).items()
            )
        ),
        "",
        "## Quality Gates",
        "",
        "| Gate | Status | Message |",
        "|------|--------|---------|",
    ]
    for gate in json_object_rows(report.get("qualityGates")):
        lines.append(
            f"| `{gate.get('id')}` | `{gate.get('status')}` | {clean_text(gate.get('message'))} |"
        )
    if not json_object_rows(report.get("qualityGates")):
        lines.append("| none | `ok` | No gate warnings or failures. |")
    warnings = [clean_text(item) for item in report.get("warnings", []) if clean_text(item)]
    lines.extend(["", "## Warnings", ""])
    if warnings:
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("- None")
    return "\n".join(lines) + "\n"


def write_soak_report(
    report: dict[str, Any], out_dir: Path, report_format: str = "both"
) -> dict[str, str]:
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, str] = {}
    if report_format in {"json", "both"}:
        json_path = output_dir / JSON_REPORT_NAME
        json_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        outputs["json"] = str(json_path)
    if report_format in {"md", "both"}:
        md_path = output_dir / MARKDOWN_REPORT_NAME
        md_path.write_text(render_markdown_report(report), encoding="utf-8")
        outputs["markdown"] = str(md_path)
    return outputs


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a read-only source-policy soak report.")
    parser.add_argument(
        "--data-dir", default="data", help="Directory containing runtime artifacts."
    )
    parser.add_argument("--out-dir", default="_out", help="Directory for generated report files.")
    parser.add_argument(
        "--backup-payload", default="", help="Optional desktop backup JSON payload."
    )
    parser.add_argument(
        "--format",
        choices=("json", "md", "both"),
        default="both",
        help="Report format to write.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    backup_path = Path(args.backup_payload) if clean_text(args.backup_payload) else None
    report = build_soak_report(Path(args.data_dir), backup_payload_path=backup_path)
    outputs = write_soak_report(report, Path(args.out_dir), args.format)
    for label, path in outputs.items():
        print(f"Wrote {label} report: {path}")
    return 1 if report.get("status") == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
