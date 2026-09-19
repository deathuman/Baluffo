#!/usr/bin/env python3
"""Provider-coverage, migration and static-scope report sections.

Leaf of ``scripts/source_policy_soak_report.py``; every unit body is byte-identical to the pre-split
module. The coordinator imports and re-exports these names.
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
del _repo_root

from scripts.source_policy_soak_report_evidence import (
    _find_linked_static_registry_row,
    _find_provider_registry_row,
    _first_clean_text,
    _first_int_value,
    _provider_coverage_status_by_token,
    _source_evidence_rows,
    _source_identity_tokens,
    _source_row_tokens,
    _source_state_token_index,
    _source_token_index,
    _static_loader_name_for_registry_row,
    _static_registry_identity_tokens,
    _unique_text,
    _warning_gate,
)
from scripts.source_policy_soak_report_spec import (
    CONSERVATIVE_CLEANUP_EXAMPLE_LIMIT,
    PROVIDER_ADAPTER_SOURCE_LOADERS,
    PROVIDER_COVERAGE_GAP_BUCKETS,
    PROVIDER_COVERAGE_GAP_EXAMPLE_LIMIT,
    PROVIDER_COVERAGE_NEXT_ACTION_PRIORITY,
    PROVIDER_ID_FIELDS,
    PROVIDER_MIGRATION_ACTIONS,
    PROVIDER_STAGING_DIAGNOSTIC_COUNT_KEYS,
    PROVIDER_VALIDATION_DIAGNOSTIC_CAUSES,
    STATIC_LIKE_ADAPTERS,
    SUPPORTED_PROVIDERS,
    as_json_list,
    as_json_object,
    build_provider_migration_payload,
    clean_text,
    enrich_provider_migration_rows,
    json_object_rows,
    norm_text,
    source_identity,
)

__all__ = [
    "Any",
    "CONSERVATIVE_CLEANUP_EXAMPLE_LIMIT",
    "Counter",
    "PROVIDER_ADAPTER_SOURCE_LOADERS",
    "PROVIDER_COVERAGE_GAP_BUCKETS",
    "PROVIDER_COVERAGE_GAP_EXAMPLE_LIMIT",
    "PROVIDER_COVERAGE_NEXT_ACTION_PRIORITY",
    "PROVIDER_ID_FIELDS",
    "PROVIDER_MIGRATION_ACTIONS",
    "PROVIDER_STAGING_DIAGNOSTIC_COUNT_KEYS",
    "PROVIDER_VALIDATION_DIAGNOSTIC_CAUSES",
    "STATIC_LIKE_ADAPTERS",
    "SUPPORTED_PROVIDERS",
    "_active_static_rows_for_validated_providers",
    "_find_linked_static_registry_row",
    "_find_provider_registry_row",
    "_first_clean_text",
    "_first_int_value",
    "_has_per_provider_identity",
    "_host_coverage_index",
    "_identity_for_gap_row",
    "_jobs_unified_rows",
    "_kept_output_host_breakdown",
    "_looks_like_provider_source_identity",
    "_provider_coverage_gap_bucket",
    "_provider_coverage_gap_example",
    "_provider_coverage_gaps_section",
    "_provider_coverage_next_action_section",
    "_provider_coverage_status_by_token",
    "_provider_migration_activation_section",
    "_provider_source_rows_missing_migration_identity",
    "_provider_validation_diagnostic_example",
    "_provider_validation_diagnostics",
    "_row_urls_for_scope",
    "_safe_command",
    "_scope_listing_url",
    "_source_evidence_rows",
    "_source_identity_tokens",
    "_source_report_evidence_for_tokens",
    "_source_row_tokens",
    "_source_rows_include_dynamic_suppression",
    "_source_state_evidence_for_tokens",
    "_source_state_token_index",
    "_source_token_index",
    "_static_loader_name_for_registry_row",
    "_static_registry_identity_tokens",
    "_static_registry_scope_conflicts_section",
    "_static_scope_conflict_classification",
    "_unique_text",
    "_url_from_row",
    "_url_host",
    "_warning_gate",
    "as_json_list",
    "as_json_object",
    "build_provider_migration_payload",
    "clean_text",
    "enrich_provider_migration_rows",
    "json_object_rows",
    "norm_text",
    "source_identity",
    "urlparse",
]


def _provider_migration_activation_section(
    *,
    discovery_report: dict[str, Any],
    discovery_candidates: list[dict[str, Any]],
    active_rows: list[dict[str, Any]],
    pending_rows: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    source_state_rows: dict[str, dict[str, Any]],
    provider_coverage: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    discovery_provider_migration = as_json_object(
        as_json_object(discovery_report.get("candidateReview")).get("providerMigration")
    )
    computed_provider_migration = (
        {}
        if discovery_provider_migration
        else build_provider_migration_payload(
            discovery_candidates,
            active_rows=active_rows,
            pending_rows=pending_rows,
            at="",
        )
    )
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
    actual_staged_count = len(staged_candidates)
    pending_provider_migration = [
        row
        for row in pending_rows
        if clean_text(row.get("pendingReason")) == "provider_migration_candidate"
    ]
    pending_provider_adapters = [
        adapter
        for adapter in PROVIDER_ADAPTER_SOURCE_LOADERS
        if any(clean_text(row.get("adapter")) == adapter for row in pending_provider_migration)
    ]
    pending_provider_source_loaders = [
        PROVIDER_ADAPTER_SOURCE_LOADERS[adapter] for adapter in pending_provider_adapters
    ]
    source_evidence_rows = _source_evidence_rows(source_rows)
    fetched_tokens = _source_token_index(source_evidence_rows) | _source_state_token_index(
        source_state_rows
    )
    coverage_status_by_token = _provider_coverage_status_by_token(
        provider_coverage, source_state_rows
    )
    validation_diagnostics = _provider_validation_diagnostics(
        pending_provider_migration=pending_provider_migration,
        source_rows=source_rows,
        source_state_rows=source_state_rows,
        provider_coverage=provider_coverage,
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
        "stagingDiagnosticsSource": "computed_from_candidates"
        if computed_provider_migration
        else "discovery_report"
        if discovery_provider_migration
        else "none",
        "actualStagedProviderCandidateCount": actual_staged_count,
        "stagedProviderCandidateCount": actual_staged_count,
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
        "pendingProviderMigrationAdapters": pending_provider_adapters,
        "pendingProviderMigrationSourceLoaders": pending_provider_source_loaders,
        "providerValidationDiagnostics": validation_diagnostics,
        "actionCounts": dict(sorted(action_counts.items())),
    }
    if discovery_provider_migration:
        discovery_action_counts = as_json_object(discovery_provider_migration.get("actionCounts"))
        if discovery_action_counts:
            section["actionCounts"] = {
                clean_text(key): int(value or 0)
                for key, value in discovery_action_counts.items()
                if clean_text(key)
            }
        section["advisoryTotalCandidates"] = int(
            discovery_provider_migration.get("totalCandidates")
            or section["advisoryTotalCandidates"]
        )
        section["addProviderSourceCount"] = int(
            section["actionCounts"].get("add_provider_source") or 0
        )
        section["reviewProviderMigrationCount"] = int(
            section["actionCounts"].get("review_provider_migration") or 0
        )
        for key in PROVIDER_STAGING_DIAGNOSTIC_COUNT_KEYS:
            section[key] = int(discovery_provider_migration.get(key) or 0)
        section["stagingBlockerCounts"] = as_json_object(
            discovery_provider_migration.get("stagingBlockerCounts")
        )
        section["stagingBlockerExamples"] = json_object_rows(
            discovery_provider_migration.get("stagingBlockerExamples")
        )
        section["stagedProviderCandidateCount"] = int(
            section.get("stagedProviderCandidateCount")
            or discovery_provider_migration.get("stagedProviderCount")
            or section["stagedProviderCandidateCount"]
        )
    elif computed_provider_migration:
        section["computedStageableProviderCandidateCount"] = int(
            computed_provider_migration.get("stageableProviderCandidateCount") or 0
        )
        section["computedWouldStageProviderCandidateCount"] = int(
            computed_provider_migration.get("stagedProviderCandidateCount") or 0
        )
        for key in (
            "stagingSkippedCount",
            "stagingBlockedByDuplicateActiveCount",
            "stagingBlockedByDuplicatePendingCount",
            "stagingBlockedByUnsupportedProviderCount",
            "stagingBlockedByInsufficientEvidenceCount",
            "stagingBlockedByNeedsProbeCount",
            "stagingBlockedByProviderRowBuildFailureCount",
            "stagingBlockedByIdentityCollisionCount",
            "stagingBlockedByAdapterMismatchCount",
        ):
            section[f"computed{key[0].upper()}{key[1:]}"] = int(
                computed_provider_migration.get(key) or 0
            )
        section["computedStagingBlockerCounts"] = as_json_object(
            computed_provider_migration.get("stagingBlockerCounts")
        )
        section["computedStagingBlockerExamples"] = json_object_rows(
            computed_provider_migration.get("stagingBlockerExamples")
        )
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
    diagnostic_stageable_count = int(
        section.get("stageableProviderCandidateCount")
        or section.get("computedStageableProviderCandidateCount")
        or 0
    )
    if diagnostic_stageable_count > 0 and section["stagedProviderCandidateCount"] == 0:
        gates.append(
            _warning_gate(
                "stageable_provider_without_staging",
                "Provider migration diagnostics found stageable candidates but none were staged.",
                {
                    "stageableProviderCandidateCount": diagnostic_stageable_count,
                    "stagingDiagnosticsSource": clean_text(section.get("stagingDiagnosticsSource")),
                },
            )
        )
    provider_row_build_failure_count = int(
        section.get("stagingBlockedByProviderRowBuildFailureCount")
        or section.get("computedStagingBlockedByProviderRowBuildFailureCount")
        or 0
    )
    if provider_row_build_failure_count > 0:
        gates.append(
            _warning_gate(
                "provider_staging_row_build_failure",
                "Provider migration staging could not build provider rows for some candidates.",
                {"providerRowBuildFailureCount": provider_row_build_failure_count},
            )
        )
    identity_collision_count = int(
        section.get("stagingBlockedByIdentityCollisionCount")
        or section.get("computedStagingBlockedByIdentityCollisionCount")
        or 0
    )
    if identity_collision_count > 0:
        gates.append(
            _warning_gate(
                "provider_staging_identity_collision",
                "Provider migration staging found provider identity collisions.",
                {"identityCollisionCount": identity_collision_count},
            )
        )
    adapter_mismatch_count = int(
        section.get("stagingBlockedByAdapterMismatchCount")
        or section.get("computedStagingBlockedByAdapterMismatchCount")
        or 0
    )
    if adapter_mismatch_count > 0:
        gates.append(
            _warning_gate(
                "provider_staging_adapter_mismatch",
                "Provider migration staging skipped candidates because their adapter is not static-like.",
                {"adapterMismatchCount": adapter_mismatch_count},
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


def _safe_command(*parts: str) -> str:
    return " ".join(part for part in parts if part)


def _provider_coverage_next_action_section(
    *,
    provider_migration_activation: dict[str, Any],
    provider_coverage_gaps: dict[str, Any],
    provider_coverage_link_backfill: dict[str, Any],
) -> dict[str, Any]:
    activation = as_json_object(provider_migration_activation)
    gaps = as_json_object(provider_coverage_gaps)
    link_backfill = as_json_object(provider_coverage_link_backfill)
    bucket_counts = as_json_object(gaps.get("bucketCounts"))
    review_candidates = json_object_rows(link_backfill.get("reviewCandidates"))
    blocked_candidates = json_object_rows(link_backfill.get("blockedCandidates"))
    actionable_blocked_candidates = json_object_rows(
        link_backfill.get("actionableBlockedCandidates")
    )
    if "actionableBlockedCandidates" not in link_backfill:
        actionable_blocked_candidates = blocked_candidates
    api_eligible_review_count = sum(
        1 for row in review_candidates if as_json_object(row).get("apiEligible") is True
    )
    provider_validation_diagnostics = as_json_object(
        activation.get("providerValidationDiagnostics")
    )
    provider_validation_cause_counts = as_json_object(
        provider_validation_diagnostics.get("causeCounts")
    )
    evidence_counts = {
        "stagingDiagnosticsSource": clean_text(activation.get("stagingDiagnosticsSource")),
        "actualStagedProviderCandidateCount": int(
            activation.get("actualStagedProviderCandidateCount") or 0
        ),
        "stagedProviderCandidateCount": int(activation.get("stagedProviderCandidateCount") or 0),
        "pendingProviderMigrationCandidateCount": int(
            activation.get("pendingProviderMigrationCandidateCount") or 0
        ),
        "computedStageableProviderCandidateCount": int(
            activation.get("computedStageableProviderCandidateCount") or 0
        ),
        "computedWouldStageProviderCandidateCount": int(
            activation.get("computedWouldStageProviderCandidateCount") or 0
        ),
        "providerMigrationCandidatesFetchedCount": int(
            activation.get("providerMigrationCandidatesFetchedCount") or 0
        ),
        "providerMigrationCandidatesValidatedCount": int(
            activation.get("providerMigrationCandidatesValidatedCount") or 0
        ),
        "providerMigrationCandidatesNoFetchCount": int(
            activation.get("providerMigrationCandidatesNoFetchCount") or 0
        ),
        "pendingProviderMigrationAdapters": [
            clean_text(adapter)
            for adapter in as_json_list(activation.get("pendingProviderMigrationAdapters"))
            if clean_text(adapter)
        ],
        "pendingProviderMigrationSourceLoaders": [
            clean_text(loader)
            for loader in as_json_list(activation.get("pendingProviderMigrationSourceLoaders"))
            if clean_text(loader)
        ],
        "providerValidationDiagnostics": provider_validation_diagnostics,
        "providerValidationZeroKeptFetchedCount": int(
            provider_validation_cause_counts.get("zeroKeptFetched") or 0
        ),
        "providerValidationFetchErrorCount": int(
            provider_validation_cause_counts.get("fetchError") or 0
        ),
        "providerValidationNotFetchedCount": int(
            provider_validation_cause_counts.get("notFetched") or 0
        ),
        "providerValidationMissingDetailEvidenceCount": int(
            provider_validation_cause_counts.get("missingDetailEvidence") or 0
        ),
        "providerValidationValidatedCount": int(
            provider_validation_cause_counts.get("validated") or 0
        ),
        "reviewCandidateCount": len(review_candidates),
        "apiEligibleReviewCandidateCount": api_eligible_review_count,
        "blockedLinkCandidateCount": len(blocked_candidates),
        "actionableBlockedLinkCandidateCount": len(actionable_blocked_candidates),
        "nonActionableBlockedLinkCandidateCount": max(
            0, len(blocked_candidates) - len(actionable_blocked_candidates)
        ),
        "unsupportedProviderDetectedCount": int(
            bucket_counts.get("unsupportedProviderDetected") or 0
        ),
        "providerDetectedNeedsProbeCount": int(
            bucket_counts.get("providerDetectedNeedsProbe") or 0
        ),
        "stagedProviderNotFetchedCount": int(bucket_counts.get("stagedProviderNotFetched") or 0),
        "fetchedButNotValidatedCount": int(bucket_counts.get("fetchedButNotValidated") or 0),
        "validatedProviderMissingMigrationSourceIdentityCount": int(
            bucket_counts.get("validatedProviderMissingMigrationSourceIdentity") or 0
        ),
        "staticStillActiveDespiteValidatedProviderCount": int(
            bucket_counts.get("staticStillActiveDespiteValidatedProvider") or 0
        ),
        "totalProviderCoverageGapCount": int(gaps.get("totalGapCount") or 0),
    }

    def section(
        action: str,
        rationale: str,
        *,
        safe_local_commands: list[str] | None = None,
        requires_human_approval: bool = False,
        blocked_by: list[str] | None = None,
    ) -> dict[str, Any]:
        return {
            "action": action,
            "priority": PROVIDER_COVERAGE_NEXT_ACTION_PRIORITY[action],
            "rationale": rationale,
            "evidenceCounts": evidence_counts,
            "safeLocalCommands": safe_local_commands or [],
            "requiresHumanApproval": requires_human_approval,
            "blockedBy": blocked_by or [],
        }

    def pending_provider_fetch_command(*, force_refresh_all: bool = False) -> str:
        pending_source_loaders = [
            clean_text(loader)
            for loader in evidence_counts["pendingProviderMigrationSourceLoaders"]
            if clean_text(loader)
        ]
        command_parts = ["python", "src/jobs_fetcher.py"]
        if pending_source_loaders:
            command_parts.extend(["--only-sources", ",".join(pending_source_loaders)])
        command_parts.append("--include-pending-provider-migration")
        if force_refresh_all:
            command_parts.append("--force-refresh-all")
        return _safe_command(*command_parts)

    provider_validation_diagnostic_total = sum(
        int(provider_validation_cause_counts.get(cause) or 0)
        for cause in PROVIDER_VALIDATION_DIAGNOSTIC_CAUSES
    )
    non_promotable_provider_validation_count = (
        evidence_counts["providerValidationZeroKeptFetchedCount"]
        + evidence_counts["providerValidationFetchErrorCount"]
    )
    positive_or_unknown_unvalidated_count = max(
        0,
        evidence_counts["fetchedButNotValidatedCount"] - non_promotable_provider_validation_count,
    )
    should_debug_provider_validation = (
        evidence_counts["providerValidationMissingDetailEvidenceCount"] > 0
        or positive_or_unknown_unvalidated_count > 0
        or (
            provider_validation_diagnostic_total == 0
            and (
                evidence_counts["fetchedButNotValidatedCount"] > 0
                or (
                    evidence_counts["providerMigrationCandidatesFetchedCount"] > 0
                    and evidence_counts["providerMigrationCandidatesValidatedCount"]
                    < evidence_counts["providerMigrationCandidatesFetchedCount"]
                )
            )
        )
    )

    if (
        evidence_counts["stagingDiagnosticsSource"] == "computed_from_candidates"
        and evidence_counts["computedWouldStageProviderCandidateCount"] > 0
        and evidence_counts["actualStagedProviderCandidateCount"] == 0
        and evidence_counts["pendingProviderMigrationCandidateCount"] == 0
    ):
        return section(
            "refresh_discovery_staging_evidence",
            "Provider staging evidence is fallback-computed from candidates; refresh discovery so actual pending/staged rows and provider-migration review diagnostics are current.",
            safe_local_commands=[
                _safe_command(
                    "python",
                    "scripts/provider_migration_staging_refresh.py",
                    "--data-dir",
                    "data",
                    "--out-dir",
                    "_out",
                    "--apply-pending",
                ),
                _safe_command(
                    "python",
                    "scripts/source_policy_soak_report.py",
                    "--data-dir",
                    "data",
                    "--out-dir",
                    "_out",
                ),
            ],
            blocked_by=["discovery_report_missing_provider_migration_review"],
        )
    if (
        evidence_counts["pendingProviderMigrationCandidateCount"] > 0
        and evidence_counts["providerMigrationCandidatesFetchedCount"] == 0
        and evidence_counts["providerValidationFetchErrorCount"] == 0
        and evidence_counts["providerValidationMissingDetailEvidenceCount"] == 0
    ):
        return section(
            "fetch_staged_provider_candidates",
            "Pending provider migration candidates exist, but none have provider fetch evidence yet.",
            safe_local_commands=[
                pending_provider_fetch_command(),
                _safe_command(
                    "python",
                    "scripts/source_policy_soak_report.py",
                    "--data-dir",
                    "data",
                    "--out-dir",
                    "_out",
                ),
            ],
            blocked_by=["pending_provider_migration_not_fetched"],
        )
    if should_debug_provider_validation:
        return section(
            "debug_provider_validation",
            "Provider migration candidates have fetch evidence, but validation has not reached validated-provider status.",
            safe_local_commands=[
                pending_provider_fetch_command(force_refresh_all=True),
                _safe_command(
                    "python",
                    "scripts/source_policy_soak_report.py",
                    "--data-dir",
                    "data",
                    "--out-dir",
                    "_out",
                ),
            ],
            blocked_by=["provider_fetch_not_validated"],
        )
    if evidence_counts["unsupportedProviderDetectedCount"] > 0:
        return section(
            "plan_unsupported_provider_family",
            "Unsupported provider-family detections remain after staged/fetched/linkable provider work is not the top blocker.",
            blocked_by=["unsupported_provider_family"],
        )
    if api_eligible_review_count > 0:
        return section(
            "review_one_migration_link",
            "At least one API-eligible migration-link review candidate exists; applying or clearing links requires explicit Admin/human approval.",
            requires_human_approval=True,
            blocked_by=["requires_explicit_admin_migration_link_action"],
        )
    if len(actionable_blocked_candidates) > 0:
        return section(
            "resolve_link_ambiguity",
            "Actionable provider/static link candidates exist, but none are currently API-eligible for review.",
            safe_local_commands=[
                _safe_command(
                    "python",
                    "scripts/source_policy_soak_report.py",
                    "--data-dir",
                    "data",
                    "--out-dir",
                    "_out",
                )
            ],
            blocked_by=sorted(
                {
                    clean_text(blocker)
                    for row in actionable_blocked_candidates
                    for blocker in as_json_list(row.get("blockers"))
                    if clean_text(blocker)
                }
            ),
        )
    return section(
        "none",
        "No provider coverage triage action is currently recommended from the available soak evidence.",
    )


def _identity_for_gap_row(row: dict[str, Any]) -> str:
    if not row:
        return ""
    explicit = clean_text(row.get("sourceIdentity") or row.get("id") or row.get("sourceId"))
    if explicit:
        return explicit
    if any(clean_text(row.get(key)) for key in ("adapter", "name", "listing_url", "api_url")):
        return source_identity(row)
    return ""


def _source_state_evidence_for_tokens(
    source_state_rows: dict[str, dict[str, Any]],
    tokens: set[str],
) -> tuple[str, dict[str, Any]]:
    for name, raw in source_state_rows.items():
        row = as_json_object(raw)
        state_tokens = {clean_text(name), *_source_identity_tokens(row)}
        if tokens & {token for token in state_tokens if token}:
            return clean_text(name), row
    return "", {}


def _source_report_evidence_for_tokens(
    source_rows: list[dict[str, Any]],
    tokens: set[str],
) -> dict[str, Any]:
    for row in source_rows:
        if tokens & _source_row_tokens(row):
            return row
    return {}


def _provider_coverage_gap_example(
    *,
    row: dict[str, Any],
    blocker_reason: str,
    registry_bucket: str = "",
    registry_row: dict[str, Any] | None = None,
    source_row: dict[str, Any] | None = None,
    state_row: dict[str, Any] | None = None,
    state_name: str = "",
    provider_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    registry = as_json_object(registry_row)
    source = as_json_object(source_row)
    state = as_json_object(state_row)
    provider = as_json_object(provider_row)
    latest_kept = _first_int_value(source, "keptCount", "jobsFound")
    if latest_kept == 0:
        latest_kept = _first_int_value(
            state,
            "lastKeptCount",
            "providerCoverageLatestKeptCount",
        )
    example = {
        "blockerReason": clean_text(blocker_reason),
        "sourceIdentity": _identity_for_gap_row(row),
        "sourceName": _first_clean_text(row, "name", "studio", "company") or clean_text(state_name),
        "providerSourceIdentity": _identity_for_gap_row(provider) or _identity_for_gap_row(row),
        "providerSourceName": _first_clean_text(provider, "name", "studio", "company")
        or _first_clean_text(row, "providerSourceName", "name", "studio", "company")
        or clean_text(state_name),
        "detectedProviderFamily": _first_clean_text(
            row, "detectedProviderFamily", "providerFamily", "providerAdapter", "adapter"
        ),
        "detectedProviderUrl": _first_clean_text(
            row, "detectedProviderUrl", "providerUrl", "listing_url", "careersUrl"
        ),
        "detectedProviderId": _first_clean_text(row, "detectedProviderId", "providerId"),
        "currentAdapter": _first_clean_text(row, "currentAdapter", "adapter"),
        "registryBucket": clean_text(registry_bucket),
        "registryState": _first_clean_text(registry, "registryState", "candidateState")
        or clean_text(registry_bucket),
        "latestFetchStatus": _first_clean_text(source, "status", "lastStatus")
        or _first_clean_text(state, "lastStatus", "providerCoverageStatus")
        or _first_clean_text(row, "lastProbeStatus"),
        "keptCount": latest_kept,
        "providerCoverageStatus": _first_clean_text(
            state,
            "providerCoverageStatus",
        )
        or _first_clean_text(row, "providerCoverageStatus"),
        "providerCoverageConsecutiveSuccessCount": _first_int_value(
            state, "providerCoverageConsecutiveSuccesses"
        ),
        "migrationSourceIdentity": _first_clean_text(row, "migrationSourceIdentity")
        or _first_clean_text(state, "migrationSourceIdentity")
        or _first_clean_text(provider, "migrationSourceIdentity"),
    }
    return {key: value for key, value in example.items() if value not in ("", None) and value != []}


def _provider_coverage_gap_bucket(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "count": len(rows),
        "examples": rows[:PROVIDER_COVERAGE_GAP_EXAMPLE_LIMIT],
    }


def _provider_validation_diagnostic_example(
    *,
    row: dict[str, Any],
    cause: str,
    source_loader_name: str,
    registry_row: dict[str, Any],
    source_row: dict[str, Any],
    state_row: dict[str, Any],
    state_name: str,
    aggregate_row: dict[str, Any],
) -> dict[str, Any]:
    example = _provider_coverage_gap_example(
        row=row,
        blocker_reason=cause,
        registry_bucket="pending",
        registry_row=registry_row,
        source_row=source_row,
        state_row=state_row,
        state_name=state_name,
    )
    if source_loader_name:
        example["sourceLoaderName"] = source_loader_name
    latest_error = _first_clean_text(
        source_row,
        "error",
        "lastError",
        "providerCoverageLatestError",
    ) or _first_clean_text(state_row, "lastError", "providerCoverageLatestError")
    if latest_error:
        example["latestFetchError"] = latest_error
    if aggregate_row:
        aggregate_status = _first_clean_text(aggregate_row, "status", "lastStatus")
        aggregate_error = _first_clean_text(aggregate_row, "error", "lastError")
        if aggregate_status:
            example["aggregateFetchStatus"] = aggregate_status
        if aggregate_error:
            example["aggregateFetchError"] = aggregate_error
    return example


def _provider_validation_diagnostics(
    *,
    pending_provider_migration: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    source_state_rows: dict[str, dict[str, Any]],
    provider_coverage: dict[str, Any],
) -> dict[str, Any]:
    evidence_rows = _source_evidence_rows(source_rows)
    coverage_status_by_token = _provider_coverage_status_by_token(
        provider_coverage, source_state_rows
    )
    source_loader_rows = {
        clean_text(row.get("name")): row for row in source_rows if clean_text(row.get("name"))
    }
    cause_counts = {cause: 0 for cause in PROVIDER_VALIDATION_DIAGNOSTIC_CAUSES}
    examples_by_cause: dict[str, list[dict[str, Any]]] = {
        cause: [] for cause in PROVIDER_VALIDATION_DIAGNOSTIC_CAUSES
    }
    for row in pending_provider_migration:
        tokens = _source_identity_tokens(row)
        adapter = clean_text(row.get("adapter"))
        source_loader_name = PROVIDER_ADAPTER_SOURCE_LOADERS.get(adapter, "")
        aggregate_row = as_json_object(source_loader_rows.get(source_loader_name))
        source_row = _source_report_evidence_for_tokens(evidence_rows, tokens)
        state_name, state_row = _source_state_evidence_for_tokens(source_state_rows, tokens)
        provider_status = next(
            (
                coverage_status_by_token[token]
                for token in tokens
                if coverage_status_by_token.get(token)
            ),
            "",
        )
        if provider_status == "validated_provider":
            cause = "validated"
        elif source_row or state_row:
            latest_status = norm_text(
                _first_clean_text(source_row, "status", "lastStatus")
                or _first_clean_text(state_row, "lastStatus", "providerCoverageStatus")
            )
            latest_error = _first_clean_text(
                source_row,
                "error",
                "lastError",
                "providerCoverageLatestError",
            ) or _first_clean_text(state_row, "lastError", "providerCoverageLatestError")
            latest_kept = _first_int_value(source_row, "keptCount", "jobsFound")
            if latest_kept == 0:
                latest_kept = _first_int_value(
                    state_row,
                    "lastKeptCount",
                    "providerCoverageLatestKeptCount",
                )
            if latest_status == "error" or latest_error:
                cause = "fetchError"
            elif latest_status in {"ok", "excluded"} and latest_kept == 0:
                cause = "zeroKeptFetched"
            else:
                cause = "missingDetailEvidence"
        elif aggregate_row:
            cause = "missingDetailEvidence"
        else:
            cause = "notFetched"

        cause_counts[cause] += 1
        if len(examples_by_cause[cause]) < PROVIDER_COVERAGE_GAP_EXAMPLE_LIMIT:
            examples_by_cause[cause].append(
                _provider_validation_diagnostic_example(
                    row=row,
                    cause=cause,
                    source_loader_name=source_loader_name,
                    registry_row=row,
                    source_row=source_row,
                    state_row=state_row,
                    state_name=state_name,
                    aggregate_row=aggregate_row,
                )
            )

    return {
        "causeCounts": cause_counts,
        "examples": {cause: rows for cause, rows in examples_by_cause.items() if rows},
    }


def _looks_like_provider_source_identity(value: Any) -> bool:
    parts = clean_text(value).split(":", 2)
    if len(parts) != 3:
        return False
    adapter, field, identity_value = (norm_text(part) for part in parts)
    supported = {norm_text(provider) for provider in SUPPORTED_PROVIDERS}
    return bool(adapter in supported and field in PROVIDER_ID_FIELDS and identity_value)


def _has_per_provider_identity(row: dict[str, Any], *, state_name: str = "") -> bool:
    if clean_text(row.get("sourceIdentity") or row.get("id") or row.get("sourceId")):
        return True
    if _looks_like_provider_source_identity(state_name):
        return True
    return any(clean_text(row.get(key)) for key in PROVIDER_ID_FIELDS)


def _provider_source_rows_missing_migration_identity(
    *,
    source_rows: list[dict[str, Any]],
    source_state_rows: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    supported = {clean_text(provider) for provider in SUPPORTED_PROVIDERS}
    examples: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in source_rows:
        adapter = clean_text(row.get("adapter"))
        identity = _identity_for_gap_row(row)
        if (
            adapter in supported
            and clean_text(row.get("status")) == "ok"
            and _first_int_value(row, "keptCount", "jobsFound") > 0
            and not clean_text(row.get("migrationSourceIdentity"))
            and _has_per_provider_identity(row)
            and identity not in seen
        ):
            examples.append(
                _provider_coverage_gap_example(
                    row=row,
                    blocker_reason="missing_migration_source_identity",
                    source_row=row,
                )
            )
            seen.add(identity)
    for name, raw in source_state_rows.items():
        row = as_json_object(raw)
        adapter = clean_text(row.get("lastAdapter") or row.get("adapter"))
        identity = _identity_for_gap_row(row) or clean_text(name)
        if (
            adapter in supported
            and clean_text(row.get("lastStatus")) == "ok"
            and _first_int_value(row, "lastKeptCount", "providerCoverageLatestKeptCount") > 0
            and not clean_text(row.get("migrationSourceIdentity"))
            and _has_per_provider_identity(row, state_name=name)
            and identity not in seen
        ):
            examples.append(
                _provider_coverage_gap_example(
                    row=row,
                    blocker_reason="missing_migration_source_identity",
                    state_row=row,
                    state_name=clean_text(name),
                )
            )
            seen.add(identity)
    return examples


def _source_rows_include_dynamic_suppression(
    *,
    source_rows: list[dict[str, Any]],
    static_row: dict[str, Any],
    migration_source_identity: str,
) -> bool:
    static_tokens = {
        clean_text(migration_source_identity),
        *_static_registry_identity_tokens(static_row),
    }
    static_tokens = {token for token in static_tokens if token}
    for row in source_rows:
        if clean_text(row.get("exclusionReason")) != "dynamic_redundant_provider":
            continue
        if _source_row_tokens(row) & static_tokens:
            return True
    return False


def _active_static_rows_for_validated_providers(
    *,
    source_rows: list[dict[str, Any]],
    source_state_rows: dict[str, dict[str, Any]],
    active_rows: list[dict[str, Any]],
    pending_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    examples: list[dict[str, Any]] = []
    for source_name, raw_state_row in source_state_rows.items():
        state_row = as_json_object(raw_state_row)
        if clean_text(state_row.get("providerCoverageStatus")) != "validated_provider":
            continue
        migration_source_identity = clean_text(state_row.get("migrationSourceIdentity"))
        if not migration_source_identity:
            continue
        static_registry = _find_linked_static_registry_row(
            active_rows=active_rows,
            pending_rows=pending_rows,
            rejected_rows=rejected_rows,
            identity=migration_source_identity,
        )
        if not static_registry:
            continue
        static_bucket, static_row = static_registry
        if (
            static_bucket != "active"
            or clean_text(static_row.get("adapter")) not in STATIC_LIKE_ADAPTERS
        ):
            continue
        if _source_rows_include_dynamic_suppression(
            source_rows=source_rows,
            static_row=static_row,
            migration_source_identity=migration_source_identity,
        ):
            continue
        provider_registry = _find_provider_registry_row(
            active_rows=active_rows,
            pending_rows=pending_rows,
            source_name=clean_text(source_name),
            source_state_row=state_row,
        )
        examples.append(
            _provider_coverage_gap_example(
                row=static_row,
                blocker_reason="active_static_not_suppressed",
                registry_bucket=static_bucket,
                registry_row=static_row,
                state_row=state_row,
                state_name=clean_text(source_name),
                provider_row=provider_registry[1] if provider_registry else {},
            )
        )
    return examples


def _provider_coverage_gaps_section(
    *,
    discovery_candidates: list[dict[str, Any]],
    active_rows: list[dict[str, Any]],
    pending_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    source_state_rows: dict[str, dict[str, Any]],
    provider_coverage: dict[str, Any],
) -> dict[str, Any]:
    enriched_candidates = enrich_provider_migration_rows(
        discovery_candidates,
        active_rows=active_rows,
        pending_rows=pending_rows,
    )
    source_evidence_rows = _source_evidence_rows(source_rows)
    fetched_tokens = _source_token_index(source_evidence_rows) | _source_state_token_index(
        source_state_rows
    )
    coverage_status_by_token = _provider_coverage_status_by_token(
        provider_coverage,
        source_state_rows,
    )
    pending_provider_migration = [
        row
        for row in pending_rows
        if clean_text(row.get("pendingReason")) == "provider_migration_candidate"
    ]

    unsupported = [
        _provider_coverage_gap_example(row=row, blocker_reason="unsupported_provider")
        for row in enriched_candidates
        if clean_text(row.get("recommendedAction")) == "unsupported_provider"
    ]
    needs_probe = [
        _provider_coverage_gap_example(row=row, blocker_reason="needs_probe")
        for row in enriched_candidates
        if clean_text(row.get("recommendedAction")) == "needs_probe"
    ]
    staged_not_fetched: list[dict[str, Any]] = []
    fetched_not_validated: list[dict[str, Any]] = []
    for row in pending_provider_migration:
        tokens = _source_identity_tokens(row)
        registry_bucket = "pending"
        source_row = _source_report_evidence_for_tokens(source_evidence_rows, tokens)
        state_name, state_row = _source_state_evidence_for_tokens(source_state_rows, tokens)
        if not (tokens & fetched_tokens):
            staged_not_fetched.append(
                _provider_coverage_gap_example(
                    row=row,
                    blocker_reason="not_fetched",
                    registry_bucket=registry_bucket,
                    registry_row=row,
                )
            )
            continue
        if not any(coverage_status_by_token.get(token) == "validated_provider" for token in tokens):
            fetched_not_validated.append(
                _provider_coverage_gap_example(
                    row=row,
                    blocker_reason="fetched_but_not_validated",
                    registry_bucket=registry_bucket,
                    registry_row=row,
                    source_row=source_row,
                    state_row=state_row,
                    state_name=state_name,
                )
            )

    buckets = {
        "unsupportedProviderDetected": unsupported,
        "providerDetectedNeedsProbe": needs_probe,
        "stagedProviderNotFetched": staged_not_fetched,
        "fetchedButNotValidated": fetched_not_validated,
        "validatedProviderMissingMigrationSourceIdentity": (
            _provider_source_rows_missing_migration_identity(
                source_rows=source_rows,
                source_state_rows=source_state_rows,
            )
        ),
        "staticStillActiveDespiteValidatedProvider": (
            _active_static_rows_for_validated_providers(
                source_rows=source_rows,
                source_state_rows=source_state_rows,
                active_rows=active_rows,
                pending_rows=pending_rows,
                rejected_rows=rejected_rows,
            )
        ),
    }
    bucket_counts = {bucket: len(rows) for bucket, rows in buckets.items()}
    section = {
        "bucketCounts": bucket_counts,
        "totalGapCount": sum(bucket_counts.values()),
    }
    for bucket in PROVIDER_COVERAGE_GAP_BUCKETS:
        section[bucket] = _provider_coverage_gap_bucket(buckets.get(bucket, []))
    return section


def _url_from_row(row: dict[str, Any]) -> str:
    for key in (
        "listing_url",
        "careersUrl",
        "url",
        "api_url",
        "feed_url",
        "board_url",
        "base_url",
        "detectedProviderUrl",
        "currentUrl",
    ):
        value = clean_text(row.get(key))
        if value:
            return value
    pages = row.get("pages")
    if isinstance(pages, list):
        for value in pages:
            text = clean_text(value)
            if text:
                return text
    return ""


def _url_host(url: str) -> str:
    try:
        host = (urlparse(str(url or "")).netloc or "").strip().lower()
    except ValueError:
        return ""
    return host[4:] if host.startswith("www.") else host


def _row_urls_for_scope(row: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    for key in ("listing_url", "base_url", "careersUrl", "url", "api_url", "feed_url", "board_url"):
        value = clean_text(row.get(key))
        if value:
            urls.append(value)
    for value in as_json_list(row.get("pages")):
        text = clean_text(value)
        if text:
            urls.append(text)
    return _unique_text(urls)


def _scope_listing_url(row: dict[str, Any]) -> str:
    for key in ("listing_url", "careersUrl", "url"):
        value = clean_text(row.get(key))
        if value:
            return value
    pages = as_json_list(row.get("pages"))
    for value in pages:
        text = clean_text(value)
        if text:
            return text
    return ""


def _host_coverage_index(active_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    coverage: dict[str, list[dict[str, Any]]] = {}
    for row in active_rows:
        if not isinstance(row, dict):
            continue
        for url in _row_urls_for_scope(row):
            host = _url_host(url)
            if not host:
                continue
            coverage.setdefault(host, []).append(
                {
                    "sourceId": clean_text(row.get("id")) or source_identity(row),
                    "sourceName": clean_text(row.get("name")) or clean_text(row.get("studio")),
                    "adapter": clean_text(row.get("adapter")),
                    "host": host,
                    "url": url,
                }
            )
    return coverage


def _jobs_unified_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        payload = payload.get("jobs")
    return json_object_rows(payload)


def _kept_output_host_breakdown(
    jobs: list[dict[str, Any]],
    source_name: str,
) -> dict[str, Any]:
    host_counts: Counter[str] = Counter()
    for row in jobs:
        if clean_text(row.get("source")) != source_name:
            bundle_match = any(
                isinstance(bundle_row, dict) and clean_text(bundle_row.get("source")) == source_name
                for bundle_row in as_json_list(row.get("sourceBundle"))
            )
            if not bundle_match:
                continue
        host = _url_host(clean_text(row.get("jobLink"))) or "unknown"
        host_counts[host] += 1
    hosts = [
        {"host": host, "keptCount": count}
        for host, count in sorted(host_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    return {
        "totalKeptCount": sum(host_counts.values()),
        "hostCount": len(host_counts),
        "hosts": hosts,
    }


def _static_scope_conflict_classification(
    *,
    covered_hosts: list[str],
    uncovered_hosts: list[str],
    unexpected_kept_hosts: list[str],
    kept_output_total: int,
    kept_output_evidence_available: bool,
) -> tuple[str, str, list[str]]:
    if unexpected_kept_hosts:
        return (
            "manual_scope_review",
            "review_scope_manually",
            ["kept_output_host_not_explained_by_registry_scope"],
        )
    if kept_output_evidence_available and kept_output_total <= 0:
        return (
            "zero_kept_review",
            "review_scope_manually",
            ["zero_kept_conflict_review"],
        )
    if covered_hosts and uncovered_hosts:
        return (
            "manual_scope_review",
            "review_scope_manually",
            ["mixed_covered_and_uncovered_off_listing_hosts"],
        )
    if uncovered_hosts:
        return (
            "needs_split_source",
            "create_or_link_source_after_review",
            ["uncovered_off_listing_hosts"],
        )
    return (
        "shadowed_cross_host",
        "narrow_static_scope_after_review",
        ["off_listing_hosts_covered_by_other_active_sources"],
    )


def _static_registry_scope_conflicts_section(
    *,
    active_rows: list[dict[str, Any]],
    jobs_unified: Any,
) -> dict[str, Any]:
    coverage_index = _host_coverage_index(active_rows)
    jobs = _jobs_unified_rows(jobs_unified)
    kept_output_evidence_available = isinstance(jobs_unified, list) or (
        isinstance(jobs_unified, dict) and isinstance(jobs_unified.get("jobs"), list)
    )
    conflicts: list[dict[str, Any]] = []
    patch_proposals: list[dict[str, Any]] = []
    scanned_static_count = 0
    for row in active_rows:
        if clean_text(row.get("adapter")) != "static":
            continue
        scanned_static_count += 1
        source_id = clean_text(row.get("id")) or source_identity(row)
        listing_url = _scope_listing_url(row)
        listing_host = _url_host(listing_url)
        if not listing_host:
            continue
        source_pages = [
            clean_text(page) for page in as_json_list(row.get("pages")) if clean_text(page)
        ]
        off_pages: list[str] = []
        off_hosts: list[str] = []
        for page_text in source_pages:
            page_host = _url_host(page_text)
            if page_text and page_host and page_host != listing_host:
                off_pages.append(page_text)
                off_hosts.append(page_host)
        off_hosts = _unique_text(off_hosts)
        if not off_hosts:
            continue
        coverage_rows: list[dict[str, Any]] = []
        covered_hosts: list[str] = []
        uncovered_hosts: list[str] = []
        for host in off_hosts:
            host_rows = [
                coverage
                for coverage in coverage_index.get(host, [])
                if clean_text(coverage.get("sourceId")) != source_id
            ]
            if host_rows:
                covered_hosts.append(host)
                coverage_rows.extend(host_rows[:3])
            else:
                uncovered_hosts.append(host)
        source_name = _static_loader_name_for_registry_row(row)
        kept_breakdown = _kept_output_host_breakdown(jobs, source_name)
        kept_hosts = [
            clean_text(host_row.get("host"))
            for host_row in json_object_rows(kept_breakdown.get("hosts"))
        ]
        expected_kept_hosts = {listing_host, *off_hosts}
        unexpected_kept_hosts = [
            host
            for host in kept_hosts
            if host and host != "unknown" and host not in expected_kept_hosts
        ]
        classification, recommended_action, reasons = _static_scope_conflict_classification(
            covered_hosts=covered_hosts,
            uncovered_hosts=uncovered_hosts,
            unexpected_kept_hosts=unexpected_kept_hosts,
            kept_output_total=int(kept_breakdown.get("totalKeptCount") or 0),
            kept_output_evidence_available=kept_output_evidence_available,
        )
        if covered_hosts:
            reasons.append("covered_off_listing_hosts")
        if uncovered_hosts:
            reasons.append("uncovered_off_listing_hosts")
        if kept_output_evidence_available:
            reasons.append("kept_output_evidence_available")
        conflict = {
            "sourceId": source_id,
            "sourceName": clean_text(row.get("name")) or source_name,
            "adapter": "static",
            "listingHost": listing_host,
            "offListingHosts": off_hosts,
            "offListingHostPages": off_pages[:5],
            "coveredOffListingHosts": covered_hosts,
            "uncoveredOffListingHosts": uncovered_hosts,
            "coverageRows": coverage_rows[:8],
            "keptOutputHostBreakdown": kept_breakdown,
            "classification": classification,
            "recommendedAction": recommended_action,
            "reasons": _unique_text(reasons),
            "destructiveActionAllowed": False,
            "requiresExplicitAdminAction": True,
            "behaviorChangeAllowed": False,
        }
        conflicts.append(conflict)
        if (
            classification == "shadowed_cross_host"
            and recommended_action == "narrow_static_scope_after_review"
            and covered_hosts
            and not uncovered_hosts
        ):
            remove_pages = [page for page in source_pages if _url_host(page) in set(covered_hosts)]
            keep_pages = [page for page in source_pages if page not in set(remove_pages)]
            patch_proposals.append(
                {
                    "sourceId": source_id,
                    "sourceName": clean_text(row.get("name")) or source_name,
                    "proposedAction": "narrow_static_scope",
                    "classification": classification,
                    "removePages": remove_pages,
                    "keepPages": keep_pages,
                    "preserveFields": ["id", "listing_url", "careersUrl"],
                    "applyAllowed": False,
                    "requiresExplicitAdminAction": True,
                    "destructiveActionAllowed": False,
                    "behaviorChangeAllowed": False,
                    "reasons": ["shadowed_cross_host", "all_off_listing_hosts_covered"],
                }
            )
    conflicts.sort(
        key=lambda item: (
            clean_text(item.get("classification")),
            clean_text(item.get("sourceName")) or clean_text(item.get("sourceId")),
        )
    )
    classification_counts = Counter(clean_text(row.get("classification")) for row in conflicts)
    summary = {
        "scannedStaticCount": scanned_static_count,
        "conflictCount": len(conflicts),
        "shadowedCrossHostCount": int(classification_counts.get("shadowed_cross_host", 0)),
        "needsSplitSourceCount": int(classification_counts.get("needs_split_source", 0)),
        "manualScopeReviewCount": int(classification_counts.get("manual_scope_review", 0)),
        "zeroKeptReviewCount": int(classification_counts.get("zero_kept_review", 0)),
    }
    return {
        "summary": summary,
        "conflicts": conflicts,
        "patchProposals": patch_proposals,
        "examples": conflicts[:CONSERVATIVE_CLEANUP_EXAMPLE_LIMIT],
    }
