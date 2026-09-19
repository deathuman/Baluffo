#!/usr/bin/env python3
"""Read-only source-policy/runtime evidence soak report.

Thin coordinator: the implementation lives in sibling leaf modules and every
original module attribute is re-exported here, so importers and tests keep
working unchanged."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
del _repo_root

from scripts.source_policy_soak_report_cleanup import (
    UTC,
    _active_static_row_by_token,
    _backup_source_policy,
    _cleanup_readiness_hash,
    _cleanup_row_readiness_key,
    _conservative_static_cleanup_proposals_section,
    _find_forbidden_source_sync_tokens,
    _overlap_counts,
    _parse_iso_datetime,
    _proposal_by_pair,
    _source_sync_section,
    _suppression_evidence_for_pair,
    _timestamp_age_seconds,
    datetime,
    hashlib,
)
from scripts.source_policy_soak_report_evidence import (
    _artifact_inputs,
    _discovery_rows,
    _fetch_only_sources_mode,
    _find_likely_registry_row,
    _find_linked_static_registry_row,
    _find_provider_registry_row,
    _find_registry_row,
    _first_clean_text,
    _first_int_value,
    _gate,
    _list_rows,
    _pair_key,
    _policy_pairs,
    _provider_counts,
    _provider_coverage_status_by_token,
    _read_json_artifact,
    _source_evidence_rows,
    _source_identity_tokens,
    _source_name,
    _source_row_excluded_by_cache,
    _source_row_tokens,
    _source_state_rows,
    _source_state_token_index,
    _source_token_index,
    _static_loader_name_for_registry_row,
    _static_loader_name_index,
    _static_registry_identity_tokens,
    _static_source_url,
    _unique_text,
    _warning_gate,
)
from scripts.source_policy_soak_report_links import (
    _active_registry_static_link,
    _advisory_link_rows,
    _advisory_provider_keys,
    _advisory_static_candidate,
    _ambiguity_candidate_static,
    _ambiguity_groups,
    _block_colliding_static_link_targets,
    _blocked_link_actionability,
    _blocked_link_candidates,
    _blocker_examples,
    _candidate_static_id,
    _company_name_only_blockers,
    _dedupe_link_rows,
    _deterministic_static_ambiguity_resolution,
    _disambiguation_blocker_counts,
    _has_exact_link_evidence,
    _host_matches_pattern,
    _ignored_alternative_row,
    _is_candidate_link,
    _is_strong_source_state_candidate,
    _link_blockers,
    _link_reasons,
    _linked_provider_row,
    _positive_static_history,
    _potential_review_link,
    _provider_coverage_link_backfill_sort_key,
    _provider_id_pair,
    _provider_id_value,
    _provider_identity_keys,
    _provider_link_row,
    _provider_matches_rule,
    _provider_shaped_static_identity,
    _provider_shaped_static_link_blockers,
    _provider_weak_host_rows,
    _recommended_api_payload,
    _registry_backed_static_link,
    _registry_static_candidate,
    _resolution_example,
    _resolve_provider_link_rows,
    _review_candidates,
    _source_state_evidence,
    _source_state_for_static,
    _static_candidate,
    _static_candidates,
    _static_evidence,
    _static_url_key,
    _suppress_unbacked_advisory_links_when_registry_link_exists,
    _why_not_high_confidence,
    _with_ignored_alternative,
    _with_selected_link,
    fnmatch,
    urlparse,
)
from scripts.source_policy_soak_report_markdown import (
    _blocked_candidates_markdown_rows,
    _conservative_cleanup_blocked_markdown_rows,
    _conservative_cleanup_markdown_rows,
    _markdown_cell,
    _markdown_joined_values,
    _markdown_table,
    _migration_link_disambiguation_blocker_summary,
    _provider_coverage_gap_markdown_rows,
    _provider_coverage_next_action_markdown_rows,
    _review_candidates_markdown_rows,
    _static_scope_conflict_markdown_rows,
    _static_scope_patch_proposal_markdown_rows,
    _suppression_eligibility_markdown_rows,
)
from scripts.source_policy_soak_report_sections import (
    _active_static_rows_for_validated_providers,
    _has_per_provider_identity,
    _host_coverage_index,
    _identity_for_gap_row,
    _jobs_unified_rows,
    _kept_output_host_breakdown,
    _looks_like_provider_source_identity,
    _provider_coverage_gap_bucket,
    _provider_coverage_gap_example,
    _provider_coverage_gaps_section,
    _provider_coverage_next_action_section,
    _provider_migration_activation_section,
    _provider_source_rows_missing_migration_identity,
    _provider_validation_diagnostic_example,
    _provider_validation_diagnostics,
    _row_urls_for_scope,
    _safe_command,
    _scope_listing_url,
    _source_report_evidence_for_tokens,
    _source_rows_include_dynamic_suppression,
    _source_state_evidence_for_tokens,
    _static_registry_scope_conflicts_section,
    _static_scope_conflict_classification,
    _url_from_row,
    _url_host,
    urlparse,
)
from scripts.source_policy_soak_report_spec import (
    ARTIFACT_PATHS,
    CONSERVATIVE_CLEANUP_EXAMPLE_LIMIT,
    CONSERVATIVE_CLEANUP_MIN_SAFE_RUNS,
    CONSERVATIVE_CLEANUP_PROPOSAL_STALE_AFTER_SECONDS,
    JSON_REPORT_NAME,
    LEAN_REGISTRY_ARTIFACT_NAMES,
    MARKDOWN_REPORT_NAME,
    PROVIDER_ADAPTER_SOURCE_LOADERS,
    PROVIDER_COVERAGE_GAP_BUCKETS,
    PROVIDER_COVERAGE_GAP_EXAMPLE_LIMIT,
    PROVIDER_COVERAGE_LINK_BACKFILL_EXAMPLE_LIMIT,
    PROVIDER_COVERAGE_NEXT_ACTION_PRIORITY,
    PROVIDER_COVERAGE_REVIEW_BLOCKING_DISAMBIGUATION_REASONS,
    PROVIDER_ID_FIELDS,
    PROVIDER_MIGRATION_ACTIONS,
    PROVIDER_STAGING_DIAGNOSTIC_COUNT_KEYS,
    PROVIDER_VALIDATION_DIAGNOSTIC_CAUSES,
    REDUNDANT_STATIC_IF_PROVIDER,
    REGISTRY_SEED_PATHS,
    ROOT,
    SCHEMA_VERSION,
    SOURCE_SYNC_ALLOWED_KEYS,
    SOURCE_SYNC_FORBIDDEN_TOKENS,
    STATIC_LIKE_ADAPTERS,
    STATIC_LIKE_STAGES,
    STATIC_SCOPE_APPLY_AUDIT_NAME,
    SUPPORTED_PROVIDERS,
    _int_value,
    as_json_list,
    as_json_object,
    build_provider_migration_payload,
    clean_text,
    common_registry_entries,
    enrich_provider_migration_rows,
    json_object_rows,
    load_registry_json_array,
    norm_text,
    normalize_provider_coverage_payload,
    normalize_provider_static_overlap_payload,
    normalize_redundant_static_proposals_payload,
    normalize_source_policy_recommendations_artifact,
    normalize_source_policy_review_state_artifact,
    normalize_static_suppression_policy_payload,
    now_iso,
    read_json,
    runtime_static_source_name_for_registry_row,
    source_identity,
)

__all__ = [
    "ARTIFACT_PATHS",
    "Any",
    "CONSERVATIVE_CLEANUP_EXAMPLE_LIMIT",
    "CONSERVATIVE_CLEANUP_MIN_SAFE_RUNS",
    "CONSERVATIVE_CLEANUP_PROPOSAL_STALE_AFTER_SECONDS",
    "Counter",
    "JSON_REPORT_NAME",
    "LEAN_REGISTRY_ARTIFACT_NAMES",
    "MARKDOWN_REPORT_NAME",
    "PROVIDER_ADAPTER_SOURCE_LOADERS",
    "PROVIDER_COVERAGE_GAP_BUCKETS",
    "PROVIDER_COVERAGE_GAP_EXAMPLE_LIMIT",
    "PROVIDER_COVERAGE_LINK_BACKFILL_EXAMPLE_LIMIT",
    "PROVIDER_COVERAGE_NEXT_ACTION_PRIORITY",
    "PROVIDER_COVERAGE_REVIEW_BLOCKING_DISAMBIGUATION_REASONS",
    "PROVIDER_ID_FIELDS",
    "PROVIDER_MIGRATION_ACTIONS",
    "PROVIDER_STAGING_DIAGNOSTIC_COUNT_KEYS",
    "PROVIDER_VALIDATION_DIAGNOSTIC_CAUSES",
    "REDUNDANT_STATIC_IF_PROVIDER",
    "REGISTRY_SEED_PATHS",
    "ROOT",
    "SCHEMA_VERSION",
    "SOURCE_SYNC_ALLOWED_KEYS",
    "SOURCE_SYNC_FORBIDDEN_TOKENS",
    "STATIC_LIKE_ADAPTERS",
    "STATIC_LIKE_STAGES",
    "STATIC_SCOPE_APPLY_AUDIT_NAME",
    "SUPPORTED_PROVIDERS",
    "UTC",
    "_active_registry_static_link",
    "_active_static_row_by_token",
    "_active_static_rows_for_validated_providers",
    "_advisory_link_rows",
    "_advisory_provider_keys",
    "_advisory_static_candidate",
    "_ambiguity_candidate_static",
    "_ambiguity_groups",
    "_artifact_inputs",
    "_backup_source_policy",
    "_block_colliding_static_link_targets",
    "_blocked_candidates_markdown_rows",
    "_blocked_link_actionability",
    "_blocked_link_candidates",
    "_blocker_examples",
    "_build_sections",
    "_candidate_static_id",
    "_cleanup_readiness_hash",
    "_cleanup_row_readiness_key",
    "_company_name_only_blockers",
    "_conservative_cleanup_blocked_markdown_rows",
    "_conservative_cleanup_markdown_rows",
    "_conservative_static_cleanup_proposals_section",
    "_dedupe_link_rows",
    "_deterministic_static_ambiguity_resolution",
    "_disambiguation_blocker_counts",
    "_discovery_rows",
    "_fetch_only_sources_mode",
    "_find_forbidden_source_sync_tokens",
    "_find_likely_registry_row",
    "_find_linked_static_registry_row",
    "_find_provider_registry_row",
    "_find_registry_row",
    "_first_clean_text",
    "_first_int_value",
    "_gate",
    "_has_exact_link_evidence",
    "_has_per_provider_identity",
    "_host_coverage_index",
    "_host_matches_pattern",
    "_identity_for_gap_row",
    "_ignored_alternative_row",
    "_int_value",
    "_is_candidate_link",
    "_is_strong_source_state_candidate",
    "_jobs_unified_rows",
    "_kept_output_host_breakdown",
    "_link_blockers",
    "_link_reasons",
    "_linked_provider_row",
    "_list_rows",
    "_looks_like_provider_source_identity",
    "_markdown_cell",
    "_markdown_joined_values",
    "_markdown_table",
    "_migration_link_disambiguation_blocker_summary",
    "_overlap_counts",
    "_pair_key",
    "_parse_args",
    "_parse_iso_datetime",
    "_policy_pairs",
    "_positive_static_history",
    "_potential_review_link",
    "_proposal_by_pair",
    "_provider_counts",
    "_provider_coverage_gap_bucket",
    "_provider_coverage_gap_example",
    "_provider_coverage_gap_markdown_rows",
    "_provider_coverage_gaps_section",
    "_provider_coverage_link_backfill_section",
    "_provider_coverage_link_backfill_sort_key",
    "_provider_coverage_next_action_markdown_rows",
    "_provider_coverage_next_action_section",
    "_provider_coverage_status_by_token",
    "_provider_id_pair",
    "_provider_id_value",
    "_provider_identity_keys",
    "_provider_link_row",
    "_provider_matches_rule",
    "_provider_migration_activation_section",
    "_provider_shaped_static_identity",
    "_provider_shaped_static_link_blockers",
    "_provider_source_rows_missing_migration_identity",
    "_provider_validation_diagnostic_example",
    "_provider_validation_diagnostics",
    "_provider_weak_host_rows",
    "_read_json_artifact",
    "_recommended_api_payload",
    "_registry_backed_static_link",
    "_registry_static_candidate",
    "_resolution_example",
    "_resolve_provider_link_rows",
    "_review_candidates",
    "_review_candidates_markdown_rows",
    "_row_urls_for_scope",
    "_rule_link_rows",
    "_safe_command",
    "_scope_listing_url",
    "_source_evidence_rows",
    "_source_identity_tokens",
    "_source_name",
    "_source_report_evidence_for_tokens",
    "_source_row_excluded_by_cache",
    "_source_row_tokens",
    "_source_rows_include_dynamic_suppression",
    "_source_state_evidence",
    "_source_state_evidence_for_tokens",
    "_source_state_for_static",
    "_source_state_rows",
    "_source_state_token_index",
    "_source_sync_section",
    "_source_token_index",
    "_static_candidate",
    "_static_candidates",
    "_static_evidence",
    "_static_loader_diagnostics",
    "_static_loader_name_for_registry_row",
    "_static_loader_name_index",
    "_static_registry_identity_tokens",
    "_static_registry_scope_conflicts_section",
    "_static_scope_conflict_classification",
    "_static_scope_conflict_markdown_rows",
    "_static_scope_patch_proposal_markdown_rows",
    "_static_source_url",
    "_static_url_key",
    "_suppress_unbacked_advisory_links_when_registry_link_exists",
    "_suppression_eligibility_markdown_rows",
    "_suppression_eligibility_section",
    "_suppression_evidence_for_pair",
    "_timestamp_age_seconds",
    "_unique_text",
    "_url_from_row",
    "_url_host",
    "_warning_gate",
    "_why_not_high_confidence",
    "_with_ignored_alternative",
    "_with_selected_link",
    "apply_static_scope_proposal",
    "argparse",
    "as_json_list",
    "as_json_object",
    "build_provider_migration_payload",
    "build_soak_report",
    "clean_text",
    "common_registry_entries",
    "datetime",
    "enrich_provider_migration_rows",
    "fnmatch",
    "hashlib",
    "json",
    "json_object_rows",
    "load_registry_json_array",
    "main",
    "norm_text",
    "normalize_provider_coverage_payload",
    "normalize_provider_static_overlap_payload",
    "normalize_redundant_static_proposals_payload",
    "normalize_source_policy_recommendations_artifact",
    "normalize_source_policy_review_state_artifact",
    "normalize_static_suppression_policy_payload",
    "now_iso",
    "read_json",
    "render_markdown_report",
    "runtime_static_source_name_for_registry_row",
    "source_identity",
    "urlparse",
    "write_soak_report",
]


def _static_loader_diagnostics(
    *,
    static_row: dict[str, Any],
    selected_row: dict[str, Any] | None,
    active_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    if not static_row:
        return {
            "registrySourceIdentity": "",
            "registryId": "",
            "registryName": "",
            "registryListingUrl": "",
            "generatedStaticLoaderName": "",
            "possibleLoaderNames": [],
            "loaderNameMatchStatus": "unknown",
            "loaderNotGeneratedReason": "",
        }
    adapter = clean_text(static_row.get("adapter"))
    registry_identity = source_identity(static_row)
    registry_id = clean_text(static_row.get("id"))
    registry_name = clean_text(static_row.get("name"))
    registry_listing_url = _static_source_url(static_row)
    actual_source_row_name = clean_text(selected_row.get("name")) if selected_row else ""
    generated_loader_name = (
        _static_loader_name_for_registry_row(static_row) if adapter == "static" else ""
    )
    possible_names = _unique_text([generated_loader_name, actual_source_row_name])
    if adapter != "static":
        return {
            "registrySourceIdentity": registry_identity,
            "registryId": registry_id,
            "registryName": registry_name,
            "registryListingUrl": registry_listing_url,
            "generatedStaticLoaderName": generated_loader_name,
            "possibleLoaderNames": possible_names,
            "loaderNameMatchStatus": "row_not_static_loader_compatible",
            "loaderNotGeneratedReason": "",
        }

    generated_rows = common_registry_entries(
        "static",
        studio_source_registry=active_rows,
        redundant_static_rules=REDUNDANT_STATIC_IF_PROVIDER,
    )
    unfiltered_rows = common_registry_entries(
        "static",
        studio_source_registry=active_rows,
        redundant_static_rules=[],
    )
    generated_by_token = _static_loader_name_index(generated_rows)
    unfiltered_by_token = _static_loader_name_index(unfiltered_rows)
    generated_name_for_identity = generated_by_token.get(registry_identity) or (
        generated_by_token.get(registry_id) if registry_id else ""
    )
    unfiltered_name_for_identity = unfiltered_by_token.get(registry_identity) or (
        unfiltered_by_token.get(registry_id) if registry_id else ""
    )
    possible_names = _unique_text(
        [generated_loader_name, generated_name_for_identity, actual_source_row_name]
    )
    loader_not_generated_reason = ""
    if actual_source_row_name and generated_loader_name != actual_source_row_name:
        match_status = "generated_name_mismatch"
    elif generated_name_for_identity:
        match_status = "exact_match"
    elif unfiltered_name_for_identity and not generated_name_for_identity:
        match_status = "loader_not_generated"
        loader_not_generated_reason = "redundant_static_rule_filtered"
    else:
        match_status = "loader_not_generated"
    return {
        "registrySourceIdentity": registry_identity,
        "registryId": registry_id,
        "registryName": registry_name,
        "registryListingUrl": registry_listing_url,
        "generatedStaticLoaderName": generated_loader_name,
        "possibleLoaderNames": possible_names,
        "loaderNameMatchStatus": match_status,
        "loaderNotGeneratedReason": loader_not_generated_reason,
    }


def _suppression_eligibility_section(
    *,
    fetch_report: dict[str, Any],
    source_rows: list[dict[str, Any]],
    source_state_rows: dict[str, dict[str, Any]],
    active_rows: list[dict[str, Any]],
    pending_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    selected_rows_by_static_identity: dict[str, dict[str, Any]] = {}
    source_row_names = {clean_text(row.get("name")) for row in source_rows}
    for row in source_rows:
        for token in _source_row_tokens(row):
            selected_rows_by_static_identity.setdefault(token, row)

    ready_rows: list[dict[str, Any]] = []
    gates: list[dict[str, Any]] = []
    only_sources_mode = _fetch_only_sources_mode(fetch_report)
    for source_name, raw_state_row in source_state_rows.items():
        state_row = as_json_object(raw_state_row)
        migration_source_identity = clean_text(state_row.get("migrationSourceIdentity"))
        if not migration_source_identity:
            continue
        if clean_text(state_row.get("providerCoverageStatus")) != "validated_provider":
            continue
        consecutive_successes = _int_value(state_row.get("providerCoverageConsecutiveSuccesses"))
        latest_kept = _int_value(state_row.get("providerCoverageLatestKeptCount"))
        if consecutive_successes < 2 or latest_kept <= 0:
            continue

        provider_registry = _find_provider_registry_row(
            active_rows=active_rows,
            pending_rows=pending_rows,
            source_name=source_name,
            source_state_row=state_row,
        )
        provider_bucket = provider_registry[0] if provider_registry else ""
        provider_row = provider_registry[1] if provider_registry else {}
        static_registry = _find_linked_static_registry_row(
            active_rows=active_rows,
            pending_rows=pending_rows,
            rejected_rows=rejected_rows,
            identity=migration_source_identity,
        )
        static_bucket = static_registry[0] if static_registry else ""
        static_row = static_registry[1] if static_registry else {}
        static_name = (
            clean_text(static_row.get("name"))
            or clean_text(provider_row.get("migrationSourceName"))
            or clean_text(state_row.get("migrationSourceName"))
        )
        likely_static_registry = None
        if not static_registry:
            likely_static_registry = _find_likely_registry_row(
                active_rows=active_rows,
                pending_rows=pending_rows,
                rejected_rows=rejected_rows,
                identity=migration_source_identity,
                static_name=static_name,
            )
        display_static_row = static_row or (
            likely_static_registry[1] if likely_static_registry else {}
        )
        display_static_bucket = static_bucket or (
            likely_static_registry[0] if likely_static_registry else ""
        )
        selected_row = selected_rows_by_static_identity.get(migration_source_identity)
        linked_static_selected = selected_row is not None
        linked_static_suppressed = bool(
            selected_row
            and clean_text(selected_row.get("exclusionReason")) == "dynamic_redundant_provider"
        )
        adapter = clean_text(display_static_row.get("adapter"))
        registry_state = (
            clean_text(display_static_row.get("registryState")) or display_static_bucket
        )
        hidden_from_default = bool(display_static_row.get("hiddenFromDefault")) or bool(
            display_static_row.get("deferred")
        )
        pending_reason = clean_text(display_static_row.get("pendingReason"))
        expected_loader_name = (
            _static_loader_name_for_registry_row(display_static_row) if display_static_row else ""
        )
        found_in_default_loaders = bool(
            expected_loader_name and expected_loader_name in source_row_names
        )
        loader_diagnostics = _static_loader_diagnostics(
            static_row=display_static_row,
            selected_row=selected_row,
            active_rows=active_rows,
        )
        if linked_static_suppressed:
            reason = "linked_static_suppressed"
        elif linked_static_selected:
            reason = "linked_static_selected_not_suppressed"
        elif not static_registry and likely_static_registry:
            reason = "linked_static_registry_identity_mismatch"
        elif not static_registry:
            reason = "linked_static_missing_from_registry"
        elif display_static_bucket == "rejected":
            reason = "linked_static_rejected"
        elif display_static_bucket == "pending" and hidden_from_default:
            reason = "linked_static_hidden_pending"
        elif display_static_bucket == "pending":
            reason = "linked_static_pending_not_default"
        elif adapter and adapter not in STATIC_LIKE_ADAPTERS:
            reason = "linked_static_adapter_not_static"
        elif display_static_bucket == "active" and adapter in STATIC_LIKE_ADAPTERS:
            reason = "linked_static_not_in_default_loader_set"
        else:
            reason = "unknown"

        provider_id = clean_text(provider_row.get("id")) or clean_text(source_name)
        static_name = (
            clean_text(display_static_row.get("name")) or static_name or migration_source_identity
        )
        actual_source_row_name = clean_text(selected_row.get("name")) if selected_row else ""
        linked_static_found_in_active = bool(static_registry and static_bucket == "active")
        linked_static_found_in_pending = bool(static_registry and static_bucket == "pending")
        linked_static_found_in_rejected = bool(static_registry and static_bucket == "rejected")
        linked_static_found_in_registry = bool(static_registry)
        ready_rows.append(
            {
                "providerSourceId": provider_id,
                "providerSourceName": clean_text(provider_row.get("name"))
                or clean_text(source_name),
                "providerAdapter": clean_text(provider_row.get("adapter"))
                or clean_text(state_row.get("lastAdapter"))
                or clean_text(state_row.get("adapter")),
                "providerBucket": provider_bucket,
                "staticSourceId": migration_source_identity,
                "staticSourceName": static_name,
                "migrationSourceIdentity": migration_source_identity,
                "migrationSourceName": clean_text(provider_row.get("migrationSourceName"))
                or static_name,
                "providerCoverageStatus": clean_text(state_row.get("providerCoverageStatus")),
                "providerCoverageConsecutiveSuccesses": consecutive_successes,
                "providerCoverageLatestKeptCount": latest_kept,
                "providerReplacementReadiness": clean_text(
                    state_row.get("providerReplacementReadiness")
                ),
                "selectionReason": reason,
                "linkedStaticRegistryBucket": display_static_bucket,
                "linkedStaticRegistryState": registry_state,
                "linkedStaticAdapter": adapter,
                "linkedStaticHiddenFromDefault": hidden_from_default,
                "linkedStaticPendingReason": pending_reason,
                "linkedStaticDuplicateOfSourceId": clean_text(
                    display_static_row.get("duplicateOfSourceId")
                ),
                "linkedStaticFoundInRegistry": linked_static_found_in_registry,
                "linkedStaticFoundInSourceRows": linked_static_selected,
                "linkedStaticFoundInSelectedSources": linked_static_selected,
                "expectedStaticLoaderName": expected_loader_name,
                "actualSourceRowName": actual_source_row_name,
                "registrySourceIdentity": loader_diagnostics["registrySourceIdentity"],
                "registryId": loader_diagnostics["registryId"],
                "registryName": loader_diagnostics["registryName"],
                "registryListingUrl": loader_diagnostics["registryListingUrl"],
                "generatedStaticLoaderName": loader_diagnostics["generatedStaticLoaderName"],
                "possibleLoaderNames": loader_diagnostics["possibleLoaderNames"],
                "loaderNameMatchStatus": loader_diagnostics["loaderNameMatchStatus"],
                "loaderNotGeneratedReason": loader_diagnostics["loaderNotGeneratedReason"],
                "registryBucket": display_static_bucket,
                "registryState": registry_state,
                "hiddenFromDefault": hidden_from_default,
                "pendingReason": pending_reason,
                "duplicateOfSourceId": clean_text(display_static_row.get("duplicateOfSourceId")),
                "adapter": adapter,
                "sourceIdentity": clean_text(display_static_row.get("id"))
                or migration_source_identity,
                "loaderName": actual_source_row_name,
                "expectedLoaderName": expected_loader_name,
                "foundInActiveRegistry": linked_static_found_in_active,
                "foundInPendingRegistry": linked_static_found_in_pending,
                "foundInRejectedRegistry": bool(
                    linked_static_found_in_rejected
                    or (likely_static_registry and likely_static_registry[0] == "rejected")
                ),
                "foundInDefaultLoaders": found_in_default_loaders,
                "foundInSourceRows": linked_static_selected,
                "excludedByCadenceOrCache": _source_row_excluded_by_cache(selected_row),
                "onlySourcesMode": only_sources_mode,
                "linkedStaticSelected": linked_static_selected,
                "reason": reason,
            }
        )

    missing_rows = [
        row
        for row in ready_rows
        if row["reason"]
        in {
            "linked_static_missing_from_registry",
            "linked_static_selected_not_suppressed",
            "linked_static_rejected",
            "linked_static_pending_not_default",
            "linked_static_hidden_pending",
            "linked_static_adapter_not_static",
            "linked_static_registry_identity_mismatch",
            "linked_static_not_in_default_loader_set",
            "linked_static_loader_not_generated",
            "unknown",
        }
    ]
    not_selected = [
        row
        for row in ready_rows
        if row["reason"]
        in {
            "linked_static_pending_not_default",
            "linked_static_hidden_pending",
            "linked_static_rejected",
            "linked_static_adapter_not_static",
            "linked_static_registry_identity_mismatch",
            "linked_static_not_in_default_loader_set",
            "linked_static_loader_not_generated",
            "unknown",
        }
    ]
    missing_registry = [
        row for row in ready_rows if row["reason"] == "linked_static_missing_from_registry"
    ]
    identity_mismatch = [
        row for row in ready_rows if row["reason"] == "linked_static_registry_identity_mismatch"
    ]
    if not_selected:
        gates.append(
            _warning_gate(
                "ready_provider_linked_static_not_selected",
                "A ready linked provider could suppress a static source, but the linked static source was not selected in this fetch.",
                {"pairs": [_pair_key(row) for row in not_selected]},
            )
        )
    if missing_registry:
        gates.append(
            _warning_gate(
                "ready_provider_linked_static_missing_from_registry",
                "A ready linked provider points at a static source that is missing from active/pending registry rows.",
                {"pairs": [_pair_key(row) for row in missing_registry]},
            )
        )
    if identity_mismatch:
        gates.append(
            _warning_gate(
                "ready_provider_linked_static_identity_mismatch",
                "A ready linked provider points at a static identity that did not match registry identity tokens, but a likely static row was found by name or URL.",
                {"pairs": [_pair_key(row) for row in identity_mismatch]},
            )
        )
    return (
        {
            "readyLinkedProviderCount": len(ready_rows),
            "selectedLinkedStaticCount": sum(
                1 for row in ready_rows if bool(row.get("linkedStaticSelected"))
            ),
            "missingLinkedStaticCount": len(missing_rows),
            "suppressedLinkedStaticCount": sum(
                1 for row in ready_rows if row.get("reason") == "linked_static_suppressed"
            ),
            "missingLinkedStaticRows": missing_rows,
        },
        gates,
    )


def _rule_link_rows(
    provider: dict[str, Any], static_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rule in REDUNDANT_STATIC_IF_PROVIDER:
        if not _provider_matches_rule(provider, as_json_object(rule)):
            continue
        hosts = rule.get("hosts")
        if not isinstance(hosts, list):
            continue
        matches = [
            static
            for static in static_rows
            if any(
                _host_matches_pattern(clean_text(static.get("host")), str(host)) for host in hosts
            )
        ]
        if not matches:
            continue
        ambiguous = len(matches) > 1
        for static in matches:
            rows.append(
                _provider_link_row(
                    provider,
                    static,
                    confidence=0.65 if ambiguous else 0.95,
                    reasons=["redundant_static_rule_exact_match"],
                    blockers=["ambiguous_static_match"] if ambiguous else [],
                    recommended_action=(
                        "ambiguous_static_match"
                        if ambiguous
                        else "backfill_migration_identity_candidate"
                    ),
                    provider_id_field=clean_text(rule.get("provider_id_field")),
                    provider_id_value=clean_text(rule.get("provider_id_value")),
                )
            )
    return rows


def _provider_coverage_link_backfill_section(
    *,
    active_rows: list[dict[str, Any]],
    pending_rows: list[dict[str, Any]],
    discovery_candidates: list[dict[str, Any]],
    source_state_rows: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    supported = {norm_text(provider) for provider in SUPPORTED_PROVIDERS}
    active_providers = [row for row in active_rows if norm_text(row.get("adapter")) in supported]
    active_without_identity = [
        row for row in active_providers if not clean_text(row.get("migrationSourceIdentity"))
    ]
    already_linked = [
        _linked_provider_row(row)
        for row in active_providers
        if clean_text(row.get("migrationSourceIdentity"))
    ]
    registry_static_rows = _static_candidates(
        [
            *[{**row, "_soakRegistryState": "active"} for row in active_rows],
            *[{**row, "_soakRegistryState": "pending"} for row in pending_rows],
        ],
        source_state_rows,
    )
    static_rows = _static_candidates(
        [
            *[{**row, "_soakRegistryState": "active"} for row in active_rows],
            *[{**row, "_soakRegistryState": "pending"} for row in pending_rows],
            *discovery_candidates,
        ],
        source_state_rows,
    )
    enriched_advisory = enrich_provider_migration_rows(
        discovery_candidates,
        active_rows=active_rows,
        pending_rows=pending_rows,
    )
    links: list[dict[str, Any]] = [*already_linked]
    diagnostic_rows: list[dict[str, Any]] = []
    resolution_examples: list[dict[str, Any]] = []
    resolved_by_advisory = 0
    resolved_by_source_state = 0
    resolved_by_registry_static = 0
    for provider in active_without_identity:
        provider_links = _dedupe_link_rows(
            [
                *_rule_link_rows(provider, registry_static_rows),
                *_advisory_link_rows(
                    provider,
                    [*discovery_candidates, *enriched_advisory],
                    registry_static_rows,
                ),
            ]
        )
        provider_links, resolution = _resolve_provider_link_rows(provider_links)
        provider_links = _suppress_unbacked_advisory_links_when_registry_link_exists(provider_links)
        if resolution:
            resolution_examples.append(resolution)
            if resolution.get("resolutionReason") == "advisory_identity_disambiguation":
                resolved_by_advisory += 1
            elif resolution.get("resolutionReason") == "source_state_disambiguation":
                resolved_by_source_state += 1
            elif resolution.get("resolutionReason") in {
                "registry_static_disambiguation",
                "active_static_canonical_url_disambiguation",
                "static_history_disambiguation",
            }:
                resolved_by_registry_static += 1
        exact_static_ids = {
            clean_text(row.get("staticSourceId"))
            for row in provider_links
            if _has_exact_link_evidence(row)
        }
        provider_diagnostics = _dedupe_link_rows(
            [
                *_provider_weak_host_rows(
                    provider,
                    static_rows,
                    excluded_static_ids={static_id for static_id in exact_static_ids if static_id},
                ),
                *_company_name_only_blockers(
                    provider,
                    static_rows,
                    excluded_static_ids={static_id for static_id in exact_static_ids if static_id},
                ),
            ]
        )
        if provider_links:
            links.extend(provider_links)
            diagnostic_rows.extend(provider_diagnostics)
            continue
        if provider_diagnostics:
            diagnostic_rows.extend(provider_diagnostics)
            continue
        field, value = _provider_id_pair(provider)
        blockers = [] if field and value else ["provider_id_missing"]
        if not blockers:
            blockers = ["insufficient_evidence"]
        diagnostic_rows.append(
            _provider_link_row(
                provider,
                {},
                confidence=0.0,
                reasons=[],
                blockers=blockers,
                recommended_action="insufficient_evidence",
                provider_id_field=field,
                provider_id_value=value,
            )
        )
    links.extend(diagnostic_rows)
    links = _dedupe_link_rows(links)
    _block_colliding_static_link_targets(links)
    candidate_links = [row for row in links if _is_candidate_link(row)]
    blocked_candidates = _blocked_link_candidates(candidate_links)
    actionable_blocked_candidates = [
        row for row in blocked_candidates if clean_text(row.get("actionability")) == "actionable"
    ]
    non_actionable_blocked_candidates = [
        row for row in blocked_candidates if clean_text(row.get("actionability")) != "actionable"
    ]
    blocker_counts = Counter(
        blocker for row in links for blocker in row.get("blockers", []) if clean_text(blocker)
    )
    blocked_reason_counts = Counter(
        blocker
        for row in blocked_candidates
        for blocker in row.get("blockers", [])
        if clean_text(blocker)
    )
    disambiguation_blocker_counts = _disambiguation_blocker_counts(blocked_candidates)
    actionable_blocked_reason_counts = Counter(
        clean_text(row.get("actionabilityReason"))
        for row in actionable_blocked_candidates
        if clean_text(row.get("actionabilityReason"))
    )
    non_actionable_blocked_reason_counts = Counter(
        clean_text(row.get("actionabilityReason"))
        for row in non_actionable_blocked_candidates
        if clean_text(row.get("actionabilityReason"))
    )
    ambiguity_groups = _ambiguity_groups(links)
    exact_rule_match_count = sum(
        1 for row in links if "redundant_static_rule_exact_match" in set(_link_reasons(row))
    )
    provider_url_match_count = sum(
        1
        for row in links
        if "provider_migration_advisory_exact_identity" in set(_link_reasons(row))
    )
    high_confidence = [
        row
        for row in candidate_links
        if float(row.get("confidence") or 0) >= 0.9
        and clean_text(row.get("recommendedAction")) == "backfill_migration_identity_candidate"
        and not _link_blockers(row)
    ]
    medium_confidence = [
        row
        for row in candidate_links
        if 0.75 <= float(row.get("confidence") or 0) < 0.9
        and clean_text(row.get("recommendedAction")) == "needs_review"
        and not _link_blockers(row)
    ]
    review_candidates = _review_candidates(links)
    section = {
        "activeProviderWithoutMigrationIdentityCount": len(active_without_identity),
        "candidateLinkCount": len(candidate_links),
        "blockedCount": len(blocked_candidates),
        "highConfidenceLinkCount": len(high_confidence),
        "mediumConfidenceLinkCount": len(medium_confidence),
        "ambiguousProviderCount": len(ambiguity_groups),
        "ambiguousStaticCandidateCount": sum(
            int(group.get("candidateStaticCount") or 0) for group in ambiguity_groups
        ),
        "exactRuleMatchCount": exact_rule_match_count,
        "hostOnlyMatchCount": int(blocker_counts.get("host_only_match") or 0),
        "providerUrlMatchCount": provider_url_match_count,
        "companyNameOnlyIgnoredCount": int(blocker_counts.get("company_name_only_ignored") or 0),
        "insufficientEvidenceCount": int(blocker_counts.get("insufficient_evidence") or 0),
        "resolvedBySourceStateCount": resolved_by_source_state,
        "resolvedByAdvisoryIdentityCount": resolved_by_advisory,
        "resolvedByRegistryStaticCount": resolved_by_registry_static,
        "unresolvedAmbiguousCount": int(blocker_counts.get("ambiguous_static_match") or 0),
        "blockedReasonCounts": dict(
            sorted(blocked_reason_counts.items(), key=lambda item: (-item[1], item[0]))
        ),
        "disambiguationBlockerCounts": disambiguation_blocker_counts,
        "actionableBlockedCount": len(actionable_blocked_candidates),
        "nonActionableBlockedCount": len(non_actionable_blocked_candidates),
        "actionableBlockedReasonCounts": dict(sorted(actionable_blocked_reason_counts.items())),
        "nonActionableBlockedReasonCounts": dict(
            sorted(non_actionable_blocked_reason_counts.items())
        ),
        "rejectedLinkCount": sum(
            1
            for row in links
            if clean_text(row.get("recommendedAction")) == "insufficient_evidence"
        ),
        "alreadyLinkedCount": len(already_linked),
        "blockerCounts": dict(sorted(blocker_counts.items())),
        "blockerExamples": _blocker_examples(links),
        "ambiguityGroups": ambiguity_groups,
        "ambiguityResolutionExamples": resolution_examples[:8],
        "reviewCandidates": review_candidates,
        "blockedCandidates": blocked_candidates,
        "actionableBlockedCandidates": actionable_blocked_candidates,
        "nonActionableBlockedCandidates": non_actionable_blocked_candidates,
        "blockedExamples": blocked_candidates[:PROVIDER_COVERAGE_LINK_BACKFILL_EXAMPLE_LIMIT],
        "disambiguationBlockedExamples": blocked_candidates[
            :PROVIDER_COVERAGE_LINK_BACKFILL_EXAMPLE_LIMIT
        ],
        "links": links,
    }
    gates: list[dict[str, Any]] = []
    if high_confidence:
        gates.append(
            _warning_gate(
                "provider_coverage_link_high_confidence_candidates",
                "High-confidence provider/static migration identity backfill candidates exist.",
                {"highConfidenceLinkCount": len(high_confidence)},
            )
        )
    if medium_confidence:
        gates.append(
            _warning_gate(
                "provider_coverage_link_resolved_candidates",
                "Provider/static migration identity backfill candidates were resolved by evidence enrichment.",
                {"mediumConfidenceLinkCount": len(medium_confidence)},
            )
        )
    ambiguous_count = int(blocker_counts.get("ambiguous_static_match") or 0)
    if ambiguous_count > 0:
        gates.append(
            _warning_gate(
                "provider_coverage_link_ambiguous_static_match",
                "Provider coverage link backfill found ambiguous static matches.",
                {"ambiguousStaticMatchCount": ambiguous_count},
            )
        )
        gates.append(
            _warning_gate(
                "provider_coverage_link_unresolved_ambiguity_examples",
                "Provider coverage link backfill has unresolved provider/static ambiguity groups.",
                {"ambiguousProviderCount": len(ambiguity_groups)},
            )
        )
    return section, gates


def _build_sections(
    payloads: dict[str, Any],
    backup_payload_path: Path | None,
    *,
    report_generated_at: str,
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
    recommendations_generated_at = clean_text(recommendations.get("updatedAt")) or clean_text(
        report_generated_at
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
    rejected_rows = _list_rows(payloads["sourceRegistryRejected"])
    provider_migration_activation, activation_gates = _provider_migration_activation_section(
        discovery_report=as_json_object(payloads["sourceDiscoveryReport"]),
        discovery_candidates=discovery_candidates,
        active_rows=active_rows,
        pending_rows=pending_rows,
        source_rows=source_rows,
        source_state_rows=source_state_rows,
        provider_coverage=provider_coverage,
    )
    gates.extend(activation_gates)
    provider_coverage_link_backfill, link_backfill_gates = _provider_coverage_link_backfill_section(
        active_rows=active_rows,
        pending_rows=pending_rows,
        discovery_candidates=discovery_candidates,
        source_state_rows=source_state_rows,
    )
    gates.extend(link_backfill_gates)
    suppression_eligibility, suppression_eligibility_gates = _suppression_eligibility_section(
        fetch_report=fetch_report,
        source_rows=source_rows,
        source_state_rows=source_state_rows,
        active_rows=active_rows,
        pending_rows=pending_rows,
        rejected_rows=rejected_rows,
    )
    gates.extend(suppression_eligibility_gates)
    provider_coverage_gaps = _provider_coverage_gaps_section(
        discovery_candidates=discovery_candidates,
        active_rows=active_rows,
        pending_rows=pending_rows,
        rejected_rows=rejected_rows,
        source_rows=source_rows,
        source_state_rows=source_state_rows,
        provider_coverage=provider_coverage,
    )
    provider_coverage_next_action = _provider_coverage_next_action_section(
        provider_migration_activation=provider_migration_activation,
        provider_coverage_gaps=provider_coverage_gaps,
        provider_coverage_link_backfill=provider_coverage_link_backfill,
    )
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
    conservative_cleanup_proposals = _conservative_static_cleanup_proposals_section(
        recommendation_pairs=recommendation_pairs,
        proposal_rows=proposal_rows,
        suppressed_pairs=suppressed_pairs,
        suppression_eligibility=suppression_eligibility,
        active_rows=active_rows,
        source_sync=source_sync,
        proposal_generated_at=recommendations_generated_at,
        proposal_report_run_id=clean_text(fetch_report.get("runId")),
        report_generated_at=report_generated_at,
    )
    static_registry_scope_conflicts = _static_registry_scope_conflicts_section(
        active_rows=active_rows,
        jobs_unified=payloads.get("jobsUnified"),
    )

    static_registry_tokens: set[str] = set()
    for row in active_rows + pending_rows + rejected_rows:
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
        "providerCoverageGaps": provider_coverage_gaps,
        "providerCoverageNextAction": provider_coverage_next_action,
        "providerCoverageLinkBackfill": provider_coverage_link_backfill,
        "suppressionEligibility": suppression_eligibility,
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
        "staticRegistryScopeConflicts": static_registry_scope_conflicts,
        "conservativeStaticCleanupProposals": conservative_cleanup_proposals,
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
        "stageableProviderCandidateCount": int(
            provider_migration_activation.get("stageableProviderCandidateCount") or 0
        ),
        "stagingSkippedCount": int(provider_migration_activation.get("stagingSkippedCount") or 0),
        "stagingBlockedByProviderRowBuildFailureCount": int(
            provider_migration_activation.get("stagingBlockedByProviderRowBuildFailureCount") or 0
        ),
        "stagingBlockedByIdentityCollisionCount": int(
            provider_migration_activation.get("stagingBlockedByIdentityCollisionCount") or 0
        ),
        "stagingBlockedByAdapterMismatchCount": int(
            provider_migration_activation.get("stagingBlockedByAdapterMismatchCount") or 0
        ),
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
        "providerCoverageGapCount": int(provider_coverage_gaps.get("totalGapCount") or 0),
        "providerCoverageNextAction": clean_text(provider_coverage_next_action.get("action")),
        "providerCoverageNextActionPriority": int(
            provider_coverage_next_action.get("priority") or 0
        ),
        "providerCoverageBackfillCandidateLinkCount": int(
            provider_coverage_link_backfill.get("candidateLinkCount") or 0
        ),
        "providerCoverageBackfillHighConfidenceLinkCount": int(
            provider_coverage_link_backfill.get("highConfidenceLinkCount") or 0
        ),
        "providerCoverageBackfillMediumConfidenceLinkCount": int(
            provider_coverage_link_backfill.get("mediumConfidenceLinkCount") or 0
        ),
        "providerCoverageBackfillAlreadyLinkedCount": int(
            provider_coverage_link_backfill.get("alreadyLinkedCount") or 0
        ),
        "suppressionReadyLinkedProviderCount": int(
            suppression_eligibility.get("readyLinkedProviderCount") or 0
        ),
        "suppressionMissingLinkedStaticCount": int(
            suppression_eligibility.get("missingLinkedStaticCount") or 0
        ),
        "suppressionSuppressedLinkedStaticCount": int(
            suppression_eligibility.get("suppressedLinkedStaticCount") or 0
        ),
        **provider_counts,
        "dynamicRedundantStaticSuppressedCount": int(policy.get("suppressedCount") or 0),
        "suppressionPausedCount": int(policy.get("pausedCount") or 0),
        "suppressionWarningCount": int(policy.get("warningCount") or 0),
        **overlap_counts,
        "redundantProposalCount": int(proposals.get("totalProposalCount") or 0),
        "staticRegistryScopeConflictCount": int(
            as_json_object(static_registry_scope_conflicts.get("summary")).get("conflictCount") or 0
        ),
        "staticRegistryScopeShadowedCrossHostCount": int(
            as_json_object(static_registry_scope_conflicts.get("summary")).get(
                "shadowedCrossHostCount"
            )
            or 0
        ),
        "stableSafeRedundantRecommendationCount": int(
            sections["sourcePolicyRecommendations"]["stableSafeRedundantCount"]
        ),
        "conservativeStaticCleanupProposalCount": int(
            conservative_cleanup_proposals.get("proposalCount") or 0
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
    generated_at = now_iso()
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
        payloads, backup_payload_path, report_generated_at=generated_at
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
        "generatedAt": generated_at,
        "status": status,
        "mutation": {"readOnly": True, "writesOutsideOut": False},
        "inputs": inputs,
        "summary": summary,
        "sections": sections,
        "qualityGates": gates,
        "warnings": warnings,
    }


def render_markdown_report(report: dict[str, Any]) -> str:
    conservative_cleanup = as_json_object(
        as_json_object(report.get("sections")).get("conservativeStaticCleanupProposals")
    )
    static_scope_conflicts = as_json_object(
        as_json_object(report.get("sections")).get("staticRegistryScopeConflicts")
    )
    provider_coverage_gaps = as_json_object(
        as_json_object(report.get("sections")).get("providerCoverageGaps")
    )
    conservative_cleanup_summary_items = [
        (key, value)
        for key, value in conservative_cleanup.items()
        if key
        not in {
            "proposals",
            "blockedCandidates",
            "proposalReadyExamples",
            "blockedExamples",
        }
    ]
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
        "## Provider Coverage Next Action",
        "",
        "Advisory only: this is the agent triage recommendation derived from current soak evidence; it does not apply Admin or registry mutations.",
        "",
        *_provider_coverage_next_action_markdown_rows(report),
        "",
        "## Provider Coverage Gaps",
        "",
        "Advisory only: these buckets explain provider discovery and validation gaps without changing source rows or fetch behavior.",
        "",
        *_markdown_table(
            [
                ("totalGapCount", int(provider_coverage_gaps.get("totalGapCount") or 0)),
                *list(as_json_object(provider_coverage_gaps.get("bucketCounts")).items()),
            ]
        ),
        "",
        "### Gap examples",
        "",
        *_provider_coverage_gap_markdown_rows(report),
        "",
        "## Provider Coverage Link Backfill",
        "",
        "Advisory only: no `migrationSourceIdentity` values are written by this report.",
        "",
        (
            "Ambiguity groups: "
            f"{int(as_json_object(as_json_object(report.get('sections')).get('providerCoverageLinkBackfill')).get('ambiguousProviderCount') or 0)} "
            "providers / "
            f"{int(as_json_object(as_json_object(report.get('sections')).get('providerCoverageLinkBackfill')).get('ambiguousStaticCandidateCount') or 0)} "
            "static candidates."
        ),
        (
            "Resolved examples: "
            f"{len(json_object_rows(as_json_object(as_json_object(report.get('sections')).get('providerCoverageLinkBackfill')).get('ambiguityResolutionExamples')))}."
        ),
        (f"Disambiguation blockers: {_migration_link_disambiguation_blocker_summary(report)}"),
        "",
        "### Review candidates",
        "",
        *_review_candidates_markdown_rows(report),
        "",
        "### Blocked candidates",
        "",
        *_blocked_candidates_markdown_rows(report),
        "",
        *_markdown_table(
            list(
                as_json_object(
                    as_json_object(report.get("sections")).get("providerCoverageLinkBackfill")
                ).items()
            )
        ),
        "",
        "## Suppression Eligibility",
        "",
        "Ready linked providers can only emit `dynamic_redundant_provider` when the linked static source is selected in the current fetch.",
        "",
        *_markdown_table(
            list(
                as_json_object(
                    as_json_object(report.get("sections")).get("suppressionEligibility")
                ).items()
            )
        ),
        "",
        "### Missing or unsuppressed linked statics",
        "",
        *_suppression_eligibility_markdown_rows(report),
        "",
        "## Static Registry Scope Conflicts",
        "",
        "Report-only: scope conflict proposals require explicit future Admin action and never edit seed or runtime registry rows.",
        "",
        *_markdown_table(list(as_json_object(static_scope_conflicts.get("summary")).items())),
        "",
        "### Conflict examples",
        "",
        *_static_scope_conflict_markdown_rows(
            json_object_rows(static_scope_conflicts.get("examples"))
            or json_object_rows(static_scope_conflicts.get("conflicts"))
        ),
        "",
        "### Dry-run patch proposals",
        "",
        "Dry-run only: these proposals are review evidence and cannot apply changes.",
        "",
        *_static_scope_patch_proposal_markdown_rows(
            json_object_rows(static_scope_conflicts.get("patchProposals"))
        ),
        "",
        "## Conservative Static Cleanup Proposals",
        "",
        "Report-only: proposals require explicit future Admin action and never delete, reject, tombstone, or edit seed registry defaults.",
        "",
        *_markdown_table(conservative_cleanup_summary_items),
        "",
        "### Proposal-ready examples",
        "",
        *_conservative_cleanup_markdown_rows(
            json_object_rows(conservative_cleanup.get("proposalReadyExamples"))
            or json_object_rows(conservative_cleanup.get("proposals"))
        ),
        "",
        "### Blocked cleanup candidates",
        "",
        *_conservative_cleanup_blocked_markdown_rows(
            json_object_rows(conservative_cleanup.get("blockedExamples"))
            or json_object_rows(conservative_cleanup.get("blockedCandidates"))
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


def apply_static_scope_proposal(
    report: dict[str, Any],
    *,
    data_dir: Path,
    out_dir: Path,
    source_id: str,
) -> dict[str, Any]:
    from src import source_registry

    target_source_id = clean_text(source_id)
    proposals = json_object_rows(
        as_json_object(
            as_json_object(report.get("sections")).get("staticRegistryScopeConflicts")
        ).get("patchProposals")
    )
    matches = [
        proposal
        for proposal in proposals
        if clean_text(proposal.get("sourceId")) == target_source_id
    ]
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one static scope patch proposal for {target_source_id!r}; found {len(matches)}."
        )
    proposal = matches[0]
    remove_pages = [
        clean_text(page) for page in as_json_list(proposal.get("removePages")) if clean_text(page)
    ]
    keep_pages = [
        clean_text(page) for page in as_json_list(proposal.get("keepPages")) if clean_text(page)
    ]
    guardrails_checked = {
        "proposedAction": clean_text(proposal.get("proposedAction")) == "narrow_static_scope",
        "classification": clean_text(proposal.get("classification")) == "shadowed_cross_host",
        "removePagesNonEmpty": bool(remove_pages),
        "keepPagesNonEmpty": bool(keep_pages),
        "requiresExplicitAdminAction": bool(proposal.get("requiresExplicitAdminAction")) is True,
        "applyAllowedFalse": bool(proposal.get("applyAllowed")) is False,
        "destructiveActionAllowedFalse": bool(proposal.get("destructiveActionAllowed")) is False,
        "behaviorChangeAllowedFalse": bool(proposal.get("behaviorChangeAllowed")) is False,
    }
    failed_guardrails = [key for key, value in guardrails_checked.items() if not value]
    if failed_guardrails:
        raise ValueError(
            f"Static scope patch proposal for {target_source_id!r} failed guardrails: {', '.join(failed_guardrails)}."
        )

    active_path = Path(data_dir) / "source-registry-active.json"
    active_rows = source_registry.load_json_array(active_path, [])
    matching_indexes = [
        index
        for index, row in enumerate(active_rows)
        if clean_text(row.get("id")) == target_source_id
    ]
    if len(matching_indexes) != 1:
        raise ValueError(
            f"Expected exactly one active registry row for {target_source_id!r}; found {len(matching_indexes)}."
        )
    row_index = matching_indexes[0]
    next_rows = [dict(row) for row in active_rows]
    previous_row = dict(next_rows[row_index])
    next_rows[row_index]["pages"] = keep_pages
    source_registry.save_json_atomic(active_path, next_rows)

    audit = {
        "sourceId": target_source_id,
        "sourceName": clean_text(proposal.get("sourceName")),
        "proposedAction": clean_text(proposal.get("proposedAction")),
        "removedPages": remove_pages,
        "keptPages": keep_pages,
        "activeRegistryPath": str(active_path),
        "applied": True,
        "guardrailsChecked": guardrails_checked,
        "generatedAt": now_iso(),
        "preservedFields": {
            "id": previous_row.get("id"),
            "listing_url": previous_row.get("listing_url"),
            "careersUrl": previous_row.get("careersUrl"),
        },
    }
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    audit_path = output_dir / STATIC_SCOPE_APPLY_AUDIT_NAME
    audit_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return audit


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
    parser.add_argument(
        "--apply-static-scope-proposal",
        default="",
        help="Explicitly apply one dry-run static scope patch proposal by exact source id.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    backup_path = Path(args.backup_payload) if clean_text(args.backup_payload) else None
    report = build_soak_report(Path(args.data_dir), backup_payload_path=backup_path)
    outputs = write_soak_report(report, Path(args.out_dir), args.format)
    if clean_text(args.apply_static_scope_proposal):
        try:
            audit = apply_static_scope_proposal(
                report,
                data_dir=Path(args.data_dir),
                out_dir=Path(args.out_dir),
                source_id=clean_text(args.apply_static_scope_proposal),
            )
        except ValueError as exc:
            print(f"Refused static scope proposal apply: {exc}", file=sys.stderr)
            return 2
        print(
            "Applied static scope proposal: "
            f"{audit['sourceId']} removed {len(audit['removedPages'])} page(s); "
            f"audit: {Path(args.out_dir) / STATIC_SCOPE_APPLY_AUDIT_NAME}"
        )
    for label, path in outputs.items():
        print(f"Wrote {label} report: {path}")
    return 1 if report.get("status") == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
