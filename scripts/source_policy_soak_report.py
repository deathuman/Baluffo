#!/usr/bin/env python3
"""Read-only source-policy/runtime evidence soak report."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from fnmatch import fnmatch
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.jobs.adapters.static_sources import (
    static_source_name_for_registry_row as runtime_static_source_name_for_registry_row,
)
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
from src.jobs.common.registry import registry_entries as common_registry_entries
from src.jobs.common.registry_defaults import REDUNDANT_STATIC_IF_PROVIDER
from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import as_json_list, as_json_object, json_object_rows
from src.shared.utils import now_iso
from src.source_discovery.config import SUPPORTED_PROVIDERS
from src.source_discovery.provider_migration_advisory import enrich_provider_migration_rows
from src.source_registry_identity import source_identity

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
STATIC_LIKE_ADAPTERS = {"static", "scrapy_static"}
STATIC_LIKE_STAGES = {"generic_static", "seed_careers_page", "sheet_directory"}
PROVIDER_ID_FIELDS = (
    "slug",
    "account",
    "company_id",
    "subdomain",
    "api_url",
    "feed_url",
    "board_url",
    "listing_url",
    "base_url",
)


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


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


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


def _source_row_tokens(row: dict[str, Any]) -> set[str]:
    tokens = set(_source_identity_tokens(row))
    name = clean_text(row.get("name"))
    if name.startswith("static_source::"):
        tokens.add(name[len("static_source::") :])
    return {token for token in tokens if token}


def _find_registry_row(
    *,
    active_rows: list[dict[str, Any]],
    pending_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]] | None = None,
    identity: str,
) -> tuple[str, dict[str, Any]] | None:
    target = clean_text(identity)
    if not target:
        return None
    for bucket, rows in (
        ("active", active_rows),
        ("pending", pending_rows),
        ("rejected", rejected_rows or []),
    ):
        for row in rows:
            if target in _source_identity_tokens(row):
                return bucket, row
    return None


def _static_loader_name_for_registry_row(row: dict[str, Any]) -> str:
    return runtime_static_source_name_for_registry_row(row)


def _unique_text(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = clean_text(value)
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def _static_loader_name_index(rows: list[dict[str, Any]]) -> dict[str, str]:
    index: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = _static_loader_name_for_registry_row(row)
        for token in {*_source_identity_tokens(row), source_identity(row)}:
            index.setdefault(token, name)
    return index


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


def _static_source_url(row: dict[str, Any]) -> str:
    url = clean_text(row.get("listing_url") or row.get("careersUrl") or row.get("url"))
    if url:
        return url
    pages = as_json_list(row.get("pages"))
    for item in pages:
        url = clean_text(item)
        if url:
            return url
    return ""


def _find_likely_registry_row(
    *,
    active_rows: list[dict[str, Any]],
    pending_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    identity: str,
    static_name: str,
) -> tuple[str, dict[str, Any]] | None:
    target_name = norm_text(static_name)
    target_url = norm_text(identity.removeprefix("static:listing_url:"))
    for bucket, rows in (
        ("active", active_rows),
        ("pending", pending_rows),
        ("rejected", rejected_rows),
    ):
        for row in rows:
            if target_name and target_name == norm_text(row.get("name")):
                return bucket, row
            if target_url and target_url == norm_text(_static_source_url(row)):
                return bucket, row
    return None


def _fetch_only_sources_mode(fetch_report: dict[str, Any]) -> bool:
    runtime = as_json_object(fetch_report.get("runtime"))
    if as_json_list(runtime.get("onlySources")):
        return True
    if as_json_list(fetch_report.get("onlySources")):
        return True
    args = as_json_object(runtime.get("args"))
    return bool(as_json_list(args.get("onlySources")) or clean_text(args.get("onlySources")))


def _source_row_excluded_by_cache(row: dict[str, Any] | None) -> bool:
    if not row:
        return False
    cache_decision = clean_text(row.get("cacheDecision"))
    exclusion_reason = clean_text(row.get("exclusionReason"))
    return cache_decision in {"skip_fresh", "cooldown_skip"} or exclusion_reason.startswith(
        "cache_"
    )


def _find_provider_registry_row(
    *,
    active_rows: list[dict[str, Any]],
    pending_rows: list[dict[str, Any]],
    source_name: str,
    source_state_row: dict[str, Any],
) -> tuple[str, dict[str, Any]] | None:
    target_name = clean_text(source_name)
    target_adapter = clean_text(
        source_state_row.get("lastAdapter") or source_state_row.get("adapter")
    )
    for bucket, rows in (("active", active_rows), ("pending", pending_rows)):
        for row in rows:
            if target_name and clean_text(row.get("name")) == target_name:
                return bucket, row
            if (
                target_name
                and target_adapter
                and clean_text(row.get("studio")) == target_name
                and clean_text(row.get("adapter")) == target_adapter
            ):
                return bucket, row
    return None


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
        static_registry = _find_registry_row(
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
        for key in (
            "stageableProviderCandidateCount",
            "stagedProviderCandidateCount",
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
        int(section.get("stageableProviderCandidateCount") or 0) > 0
        and section["stagedProviderCandidateCount"] == 0
    ):
        gates.append(
            _warning_gate(
                "stageable_provider_without_staging",
                "Provider migration diagnostics found stageable candidates but none were staged.",
                {
                    "stageableProviderCandidateCount": int(
                        section.get("stageableProviderCandidateCount") or 0
                    )
                },
            )
        )
    if int(section.get("stagingBlockedByProviderRowBuildFailureCount") or 0) > 0:
        gates.append(
            _warning_gate(
                "provider_staging_row_build_failure",
                "Provider migration staging could not build provider rows for some candidates.",
                {
                    "providerRowBuildFailureCount": int(
                        section.get("stagingBlockedByProviderRowBuildFailureCount") or 0
                    )
                },
            )
        )
    if int(section.get("stagingBlockedByIdentityCollisionCount") or 0) > 0:
        gates.append(
            _warning_gate(
                "provider_staging_identity_collision",
                "Provider migration staging found provider identity collisions.",
                {
                    "identityCollisionCount": int(
                        section.get("stagingBlockedByIdentityCollisionCount") or 0
                    )
                },
            )
        )
    if int(section.get("stagingBlockedByAdapterMismatchCount") or 0) > 0:
        gates.append(
            _warning_gate(
                "provider_staging_adapter_mismatch",
                "Provider migration staging skipped candidates because their adapter is not static-like.",
                {
                    "adapterMismatchCount": int(
                        section.get("stagingBlockedByAdapterMismatchCount") or 0
                    )
                },
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


def _host_matches_pattern(host: str, pattern: str) -> bool:
    clean_host = norm_text(host)
    clean_pattern = norm_text(pattern)
    if not clean_host or not clean_pattern:
        return False
    return (
        fnmatch(clean_host, clean_pattern) if "*" in clean_pattern else clean_host == clean_pattern
    )


def _provider_id_value(row: dict[str, Any], field: str) -> str:
    if field == "adapter":
        return clean_text(row.get("adapter"))
    return clean_text(row.get(field))


def _provider_id_pair(row: dict[str, Any]) -> tuple[str, str]:
    for field in PROVIDER_ID_FIELDS:
        value = clean_text(row.get(field))
        if value:
            return field, value
    return "", ""


def _provider_identity_keys(row: dict[str, Any]) -> set[str]:
    keys = _source_identity_tokens(row)
    keys.add(source_identity(row))
    adapter = clean_text(row.get("adapter"))
    for field in PROVIDER_ID_FIELDS:
        value = clean_text(row.get(field))
        if adapter and value:
            keys.add(f"{adapter}:{field}:{value}".lower())
    return {key for key in keys if key}


def _provider_matches_rule(row: dict[str, Any], rule: dict[str, Any]) -> bool:
    adapter = clean_text(rule.get("adapter"))
    field = clean_text(rule.get("provider_id_field"))
    value = clean_text(rule.get("provider_id_value"))
    if not adapter or not field or not value:
        return False
    return norm_text(row.get("adapter")) == norm_text(adapter) and norm_text(
        _provider_id_value(row, field)
    ) == norm_text(value)


def _static_candidate(row: dict[str, Any]) -> dict[str, Any]:
    adapter = norm_text(row.get("adapter") or row.get("currentAdapter"))
    discovery_stage = norm_text(row.get("discoveryStage") or row.get("discoveryMethod"))
    url = _url_from_row(row)
    if adapter not in STATIC_LIKE_ADAPTERS and discovery_stage not in STATIC_LIKE_STAGES:
        return {}
    if not url:
        return {}
    return {
        "staticSourceId": clean_text(row.get("id"))
        or clean_text(row.get("sourceIdentity"))
        or source_identity(row),
        "staticSourceName": _source_name(row),
        "staticUrl": url,
        "host": _url_host(url),
        "familyKey": norm_text(row.get("studio") or row.get("company") or row.get("name")),
        "registryState": clean_text(row.get("registryState") or row.get("_soakRegistryState")),
        "hiddenFromDefault": bool(row.get("hiddenFromDefault")),
        "duplicateOfSourceId": clean_text(row.get("duplicateOfSourceId")),
        "pendingReason": clean_text(row.get("pendingReason")),
    }


def _source_state_for_static(
    static: dict[str, Any], source_state_rows: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    for token in (
        clean_text(static.get("staticSourceId")),
        clean_text(static.get("staticSourceName")),
    ):
        if token and isinstance(source_state_rows.get(token), dict):
            return source_state_rows[token]
    return {}


def _static_evidence(static: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    registry_state = clean_text(static.get("registryState"))
    hidden = bool(static.get("hiddenFromDefault"))
    duplicate_of = clean_text(static.get("duplicateOfSourceId"))
    pending_reason = clean_text(static.get("pendingReason"))
    last_kept = _int_value(state.get("lastKeptCount"))
    last_status = clean_text(state.get("lastStatus"))
    last_successful_at = clean_text(state.get("lastSuccessfulAt"))
    last_fetched_at = clean_text(state.get("lastFetchedAt"))

    score = 0
    reasons: list[str] = []
    blockers: list[str] = []
    if registry_state == "active":
        score += 30
        reasons.append("active_registry_row")
    elif registry_state == "pending":
        score += 5
        blockers.append("pending_static_row")
    if hidden:
        score -= 25
        blockers.append("hidden_from_default")
    if duplicate_of:
        score -= 25
        blockers.append("duplicate_static_row")
    if pending_reason:
        blockers.append("pending_reason_present")
    if last_kept > 0:
        score += 30
        reasons.append("source_state_kept_jobs")
    if last_status == "ok":
        score += 10
        reasons.append("source_state_ok")
    elif last_status:
        blockers.append("source_state_not_ok")
    if last_successful_at:
        score += 5
        reasons.append("source_state_success_timestamp")
    if last_fetched_at:
        reasons.append("source_state_fetched")
    if not state:
        blockers.append("no_source_state_history")
    return {
        "lastKeptCount": last_kept,
        "lastStatus": last_status,
        "lastSuccessfulAt": last_successful_at,
        "lastFetchedAt": last_fetched_at,
        "evidenceScore": score,
        "evidenceReasons": reasons,
        "disambiguationBlockers": blockers,
    }


def _static_candidates(
    rows: list[dict[str, Any]], source_state_rows: dict[str, dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        candidate = _static_candidate(row)
        key = clean_text(candidate.get("staticSourceId")) if candidate else ""
        if not key or key in seen:
            continue
        seen.add(key)
        candidate.update(
            _static_evidence(
                candidate, _source_state_for_static(candidate, source_state_rows or {})
            )
        )
        out.append(candidate)
    return out


def _provider_link_row(
    provider: dict[str, Any],
    static: dict[str, Any],
    *,
    confidence: float,
    reasons: list[str],
    blockers: list[str] | None = None,
    recommended_action: str,
    provider_id_field: str = "",
    provider_id_value: str = "",
) -> dict[str, Any]:
    fallback_field, fallback_value = _provider_id_pair(provider)
    return {
        "providerSourceId": clean_text(provider.get("id")) or source_identity(provider),
        "providerSourceName": _source_name(provider),
        "providerAdapter": clean_text(provider.get("adapter")),
        "providerIdField": provider_id_field or fallback_field,
        "providerIdValue": provider_id_value or fallback_value,
        "staticSourceId": clean_text(static.get("staticSourceId")),
        "staticSourceName": clean_text(static.get("staticSourceName")),
        "staticUrl": clean_text(static.get("staticUrl")),
        "staticHost": clean_text(static.get("staticHost"))
        or _url_host(clean_text(static.get("staticUrl"))),
        "registryState": clean_text(static.get("registryState")),
        "hiddenFromDefault": bool(static.get("hiddenFromDefault")),
        "duplicateOfSourceId": clean_text(static.get("duplicateOfSourceId")),
        "pendingReason": clean_text(static.get("pendingReason")),
        "lastKeptCount": _int_value(static.get("lastKeptCount")),
        "lastStatus": clean_text(static.get("lastStatus")),
        "lastSuccessfulAt": clean_text(static.get("lastSuccessfulAt")),
        "lastFetchedAt": clean_text(static.get("lastFetchedAt")),
        "evidenceScore": _int_value(static.get("evidenceScore")),
        "evidenceReasons": [
            clean_text(reason)
            for reason in as_json_list(static.get("evidenceReasons"))
            if clean_text(reason)
        ],
        "disambiguationRank": _int_value(static.get("disambiguationRank")),
        "disambiguationBlockers": [
            clean_text(blocker)
            for blocker in as_json_list(static.get("disambiguationBlockers"))
            if clean_text(blocker)
        ],
        "confidence": round(float(confidence), 2),
        "reasons": sorted({clean_text(reason) for reason in reasons if clean_text(reason)}),
        "blockers": sorted(
            {clean_text(blocker) for blocker in blockers or [] if clean_text(blocker)}
        ),
        "recommendedAction": recommended_action,
    }


def _linked_provider_row(provider: dict[str, Any]) -> dict[str, Any]:
    static_id = clean_text(provider.get("migrationSourceIdentity"))
    static = {
        "staticSourceId": static_id,
        "staticSourceName": static_id,
        "staticUrl": clean_text(provider.get("migrationSourceUrl")),
    }
    return _provider_link_row(
        provider,
        static,
        confidence=1.0,
        reasons=["existing_migration_source_identity"],
        recommended_action="already_linked",
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


def _provider_weak_host_rows(
    provider: dict[str, Any],
    static_rows: list[dict[str, Any]],
    *,
    excluded_static_ids: set[str],
) -> list[dict[str, Any]]:
    host = _url_host(_url_from_row(provider))
    if not host:
        return []
    rows: list[dict[str, Any]] = []
    for static in static_rows:
        static_id = clean_text(static.get("staticSourceId"))
        if not static_id or static_id in excluded_static_ids:
            continue
        if clean_text(static.get("host")) != host:
            continue
        rows.append(
            _provider_link_row(
                provider,
                static,
                confidence=0.25,
                reasons=["host_only_match"],
                blockers=["host_only_match"],
                recommended_action="insufficient_evidence",
            )
        )
    return rows


def _advisory_provider_keys(row: dict[str, Any]) -> set[str]:
    keys = {
        clean_text(row.get("existingProviderSourceId")),
        clean_text(row.get("providerStagingCandidateId")),
    }
    adapter = clean_text(row.get("detectedProviderFamily") or row.get("currentAdapter"))
    provider_id = clean_text(row.get("detectedProviderId"))
    for field in PROVIDER_ID_FIELDS:
        if provider_id and adapter:
            keys.add(f"{adapter}:{field}:{provider_id}".lower())
    return {key for key in keys if key}


def _advisory_static_candidate(row: dict[str, Any]) -> dict[str, Any]:
    static_id = clean_text(
        row.get("migrationSourceIdentity")
        or row.get("staticSourceId")
        or row.get("providerStagingSourceIdentity")
    )
    discovery_stage = norm_text(row.get("discoveryStage") or row.get("discoveryMethod"))
    if not static_id and (
        norm_text(row.get("currentAdapter") or row.get("adapter")) in STATIC_LIKE_ADAPTERS
        or discovery_stage in STATIC_LIKE_STAGES
    ):
        static_id = clean_text(row.get("sourceIdentity") or row.get("id"))
    url = clean_text(row.get("staticUrl") or row.get("currentUrl") or _url_from_row(row))
    if not static_id or not url:
        return {}
    return {
        "staticSourceId": static_id,
        "staticSourceName": _source_name(row),
        "staticUrl": url,
    }


def _advisory_link_rows(
    provider: dict[str, Any], advisory_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    provider_keys = _provider_identity_keys(provider)
    rows: list[dict[str, Any]] = []
    for advisory in advisory_rows:
        action = clean_text(advisory.get("recommendedAction"))
        if action != "already_covered_by_provider" and not bool(
            advisory.get("duplicateOfActiveSource")
        ):
            continue
        if not (provider_keys & _advisory_provider_keys(advisory)):
            continue
        static = _advisory_static_candidate(advisory)
        if not static:
            continue
        rows.append(
            _provider_link_row(
                provider,
                static,
                confidence=0.8,
                reasons=["provider_migration_advisory_exact_identity"],
                recommended_action="needs_review",
            )
        )
    return rows


def _company_name_only_blockers(
    provider: dict[str, Any],
    static_rows: list[dict[str, Any]],
    *,
    excluded_static_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    provider_family = norm_text(
        provider.get("studio") or provider.get("company") or provider.get("name")
    )
    if not provider_family:
        return []
    excluded = excluded_static_ids or set()
    rows: list[dict[str, Any]] = []
    for static in static_rows:
        static_id = clean_text(static.get("staticSourceId"))
        if static_id and static_id in excluded:
            continue
        if provider_family == norm_text(static.get("familyKey")):
            rows.append(
                {
                    "providerSourceId": clean_text(provider.get("id")) or source_identity(provider),
                    "providerSourceName": _source_name(provider),
                    "providerAdapter": clean_text(provider.get("adapter")),
                    "staticSourceId": static_id,
                    "staticSourceName": clean_text(static.get("staticSourceName")),
                    "staticUrl": clean_text(static.get("staticUrl")),
                    "staticHost": clean_text(static.get("host")),
                    "confidence": 0.0,
                    "reasons": [],
                    "blockers": ["company_name_only_ignored"],
                    "recommendedAction": "insufficient_evidence",
                }
            )
    return rows


def _blocker_examples(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    examples: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        for blocker in row.get("blockers", []):
            key = clean_text(blocker)
            if not key or key in seen:
                continue
            seen.add(key)
            examples.append(
                {
                    "blocker": key,
                    "providerSourceId": clean_text(row.get("providerSourceId")),
                    "providerSourceName": clean_text(row.get("providerSourceName")),
                    "staticSourceId": clean_text(row.get("staticSourceId")),
                    "staticSourceName": clean_text(row.get("staticSourceName")),
                    "recommendedAction": clean_text(row.get("recommendedAction")),
                }
            )
    return examples[:8]


def _link_reasons(row: dict[str, Any]) -> list[str]:
    return [clean_text(reason) for reason in as_json_list(row.get("reasons")) if clean_text(reason)]


def _link_blockers(row: dict[str, Any]) -> list[str]:
    return [
        clean_text(blocker) for blocker in as_json_list(row.get("blockers")) if clean_text(blocker)
    ]


def _is_candidate_link(row: dict[str, Any]) -> bool:
    action = clean_text(row.get("recommendedAction"))
    return action not in {"already_linked", "insufficient_evidence"}


def _has_exact_link_evidence(row: dict[str, Any]) -> bool:
    reasons = set(_link_reasons(row))
    return bool(
        reasons
        & {
            "redundant_static_rule_exact_match",
            "provider_migration_advisory_exact_identity",
        }
    )


def _candidate_static_id(row: dict[str, Any]) -> str:
    return clean_text(row.get("staticSourceId"))


def _is_strong_source_state_candidate(row: dict[str, Any]) -> bool:
    if clean_text(row.get("registryState")) != "active":
        return False
    if bool(row.get("hiddenFromDefault")) or clean_text(row.get("duplicateOfSourceId")):
        return False
    if _int_value(row.get("lastKeptCount")) <= 0:
        return False
    return clean_text(row.get("lastStatus")) == "ok" or bool(
        clean_text(row.get("lastSuccessfulAt"))
    )


def _with_selected_link(
    row: dict[str, Any],
    *,
    confidence: float,
    reason: str,
) -> dict[str, Any]:
    updated = dict(row)
    updated["confidence"] = round(confidence, 2)
    updated["recommendedAction"] = (
        "backfill_migration_identity_candidate" if confidence >= 0.9 else "needs_review"
    )
    updated["blockers"] = []
    updated["reasons"] = sorted({*_link_reasons(row), reason})
    return updated


def _with_ignored_alternative(row: dict[str, Any], reason: str) -> dict[str, Any]:
    updated = dict(row)
    updated["recommendedAction"] = "insufficient_evidence"
    updated["confidence"] = min(float(row.get("confidence") or 0), 0.5)
    updated["blockers"] = sorted(
        {blocker for blocker in _link_blockers(row) if blocker != "ambiguous_static_match"}
        | {reason}
    )
    return updated


def _resolution_example(
    *,
    selected: dict[str, Any],
    ignored: list[dict[str, Any]],
    reason: str,
) -> dict[str, Any]:
    return {
        "providerSourceId": clean_text(selected.get("providerSourceId")),
        "providerSourceName": clean_text(selected.get("providerSourceName")),
        "selectedStaticSourceId": clean_text(selected.get("staticSourceId")),
        "selectedStaticSourceName": clean_text(selected.get("staticSourceName")),
        "resolutionReason": reason,
        "ignoredStaticSourceIds": [
            clean_text(row.get("staticSourceId"))
            for row in ignored
            if clean_text(row.get("staticSourceId"))
        ],
    }


def _source_state_evidence(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "registryState": clean_text(row.get("registryState")),
        "hiddenFromDefault": bool(row.get("hiddenFromDefault")),
        "duplicateOfSourceId": clean_text(row.get("duplicateOfSourceId")),
        "pendingReason": clean_text(row.get("pendingReason")),
        "lastKeptCount": _int_value(row.get("lastKeptCount")),
        "lastStatus": clean_text(row.get("lastStatus")),
        "lastSuccessfulAt": clean_text(row.get("lastSuccessfulAt")),
        "lastFetchedAt": clean_text(row.get("lastFetchedAt")),
        "evidenceScore": _int_value(row.get("evidenceScore")),
        "evidenceReasons": [
            clean_text(reason)
            for reason in as_json_list(row.get("evidenceReasons"))
            if clean_text(reason)
        ],
    }


def _why_not_high_confidence(row: dict[str, Any]) -> str:
    confidence = float(row.get("confidence") or 0)
    if confidence >= 0.9:
        return ""
    reasons = set(_link_reasons(row))
    if "source_state_disambiguation" in reasons:
        return "Resolved by source-state history only; no exact advisory static identity."
    return "Confidence is below the high-confidence threshold."


def _ignored_alternative_row(row: dict[str, Any]) -> dict[str, Any]:
    blockers = _link_blockers(row)
    return {
        "staticSourceId": clean_text(row.get("staticSourceId")),
        "staticSourceName": clean_text(row.get("staticSourceName")),
        "staticUrl": clean_text(row.get("staticUrl")),
        "blockers": blockers,
        "evidenceScore": _int_value(row.get("evidenceScore")),
        "reasonIgnored": blockers[0] if blockers else clean_text(row.get("recommendedAction")),
    }


def _recommended_api_payload(row: dict[str, Any]) -> dict[str, Any]:
    confidence = round(float(row.get("confidence") or 0), 2)
    recommended_action = (
        "backfill_migration_identity_candidate" if confidence >= 0.9 else "needs_review"
    )
    return {
        "action": "apply_migration_identity_link",
        "providerSourceId": clean_text(row.get("providerSourceId")),
        "staticSourceId": clean_text(row.get("staticSourceId")),
        "staticSourceName": clean_text(row.get("staticSourceName")),
        "confidence": confidence,
        "reasons": _link_reasons(row),
        "recommendationSource": "provider_coverage_link_backfill",
        "recommendedAction": recommended_action,
    }


def _review_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_provider: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        provider_id = clean_text(row.get("providerSourceId"))
        if provider_id:
            by_provider.setdefault(provider_id, []).append(row)
    candidates: list[dict[str, Any]] = []
    for row in rows:
        confidence = round(float(row.get("confidence") or 0), 2)
        blockers = _link_blockers(row)
        if confidence < 0.75 or blockers:
            continue
        action = clean_text(row.get("recommendedAction"))
        if action not in {"backfill_migration_identity_candidate", "needs_review"}:
            continue
        payload = _recommended_api_payload(row)
        alternatives = [
            _ignored_alternative_row(other)
            for other in by_provider.get(clean_text(row.get("providerSourceId")), [])
            if clean_text(other.get("staticSourceId"))
            and clean_text(other.get("staticSourceId")) != clean_text(row.get("staticSourceId"))
            and clean_text(other.get("recommendedAction")) == "insufficient_evidence"
        ]
        candidates.append(
            {
                "providerSourceId": clean_text(row.get("providerSourceId")),
                "providerSourceName": clean_text(row.get("providerSourceName")),
                "providerAdapter": clean_text(row.get("providerAdapter")),
                "providerIdField": clean_text(row.get("providerIdField")),
                "providerIdValue": clean_text(row.get("providerIdValue")),
                "selectedStaticSourceId": clean_text(row.get("staticSourceId")),
                "selectedStaticSourceName": clean_text(row.get("staticSourceName")),
                "selectedStaticUrl": clean_text(row.get("staticUrl")),
                "confidence": confidence,
                "confidenceTier": "high" if confidence >= 0.9 else "medium",
                "resolutionReason": next(
                    (
                        reason
                        for reason in _link_reasons(row)
                        if reason
                        in {
                            "advisory_identity_disambiguation",
                            "source_state_disambiguation",
                        }
                    ),
                    "",
                ),
                "whyNotHighConfidence": _why_not_high_confidence(row),
                "evidenceReasons": _link_reasons(row),
                "sourceStateEvidence": _source_state_evidence(row),
                "ignoredAlternatives": alternatives,
                "recommendedApiPayload": payload,
                "apiEligible": confidence >= 0.75 and not blockers and bool(payload),
            }
        )
    return candidates


def _resolve_provider_link_rows(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    ambiguous = [row for row in rows if "ambiguous_static_match" in set(_link_blockers(row))]
    if len(ambiguous) <= 1:
        return rows, None

    advisory_exact = [
        row
        for row in rows
        if "provider_migration_advisory_exact_identity" in set(_link_reasons(row))
        and _candidate_static_id(row)
    ]
    advisory_static_ids = {_candidate_static_id(row) for row in advisory_exact}
    if len(advisory_static_ids) == 1:
        selected_id = next(iter(advisory_static_ids))
        selected = next(row for row in advisory_exact if _candidate_static_id(row) == selected_id)
        ignored = [
            row
            for row in rows
            if _candidate_static_id(row)
            and _candidate_static_id(row) != selected_id
            and "ambiguous_static_match" in set(_link_blockers(row))
        ]
        return [
            _with_selected_link(
                selected,
                confidence=0.95,
                reason="advisory_identity_disambiguation",
            ),
            *[_with_ignored_alternative(row, "resolved_by_advisory_identity") for row in ignored],
            *[
                row
                for row in rows
                if row is not selected
                and row not in ignored
                and "ambiguous_static_match" not in set(_link_blockers(row))
            ],
        ], _resolution_example(
            selected=selected,
            ignored=ignored,
            reason="advisory_identity_disambiguation",
        )

    strong = [row for row in ambiguous if _is_strong_source_state_candidate(row)]
    if len(strong) == 1 and all(
        row is strong[0] or not _is_strong_source_state_candidate(row) for row in ambiguous
    ):
        selected = strong[0]
        ignored = [row for row in ambiguous if row is not selected]
        return [
            _with_selected_link(
                selected,
                confidence=0.8,
                reason="source_state_disambiguation",
            ),
            *[_with_ignored_alternative(row, "resolved_by_source_state") for row in ignored],
            *[row for row in rows if row not in ambiguous],
        ], _resolution_example(
            selected=selected,
            ignored=ignored,
            reason="source_state_disambiguation",
        )

    ranked = sorted(ambiguous, key=lambda row: _int_value(row.get("evidenceScore")), reverse=True)
    for rank, row in enumerate(ranked, start=1):
        row["disambiguationRank"] = rank
    return rows, None


def _ambiguity_candidate_static(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "staticSourceId": clean_text(row.get("staticSourceId")),
        "staticSourceName": clean_text(row.get("staticSourceName")),
        "staticUrl": clean_text(row.get("staticUrl")),
        "staticHost": clean_text(row.get("staticHost"))
        or _url_host(clean_text(row.get("staticUrl"))),
        "matchReasons": _link_reasons(row),
        "confidence": round(float(row.get("confidence") or 0), 2),
        "blockers": _link_blockers(row),
        "registryState": clean_text(row.get("registryState")),
        "hiddenFromDefault": bool(row.get("hiddenFromDefault")),
        "duplicateOfSourceId": clean_text(row.get("duplicateOfSourceId")),
        "pendingReason": clean_text(row.get("pendingReason")),
        "lastKeptCount": _int_value(row.get("lastKeptCount")),
        "lastStatus": clean_text(row.get("lastStatus")),
        "lastSuccessfulAt": clean_text(row.get("lastSuccessfulAt")),
        "lastFetchedAt": clean_text(row.get("lastFetchedAt")),
        "evidenceScore": _int_value(row.get("evidenceScore")),
        "evidenceReasons": [
            clean_text(reason)
            for reason in as_json_list(row.get("evidenceReasons"))
            if clean_text(reason)
        ],
        "disambiguationRank": _int_value(row.get("disambiguationRank")),
        "disambiguationBlockers": [
            clean_text(blocker)
            for blocker in as_json_list(row.get("disambiguationBlockers"))
            if clean_text(blocker)
        ],
    }


def _ambiguity_groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if "ambiguous_static_match" not in set(_link_blockers(row)):
            continue
        key = clean_text(row.get("providerSourceId"))
        if not key:
            continue
        grouped.setdefault(key, []).append(row)
    groups: list[dict[str, Any]] = []
    for provider_id, group_rows in sorted(grouped.items()):
        first = group_rows[0]
        groups.append(
            {
                "providerSourceId": provider_id,
                "providerSourceName": clean_text(first.get("providerSourceName")),
                "providerAdapter": clean_text(first.get("providerAdapter")),
                "providerIdField": clean_text(first.get("providerIdField")),
                "providerIdValue": clean_text(first.get("providerIdValue")),
                "candidateStaticCount": len(group_rows),
                "candidateStatics": [_ambiguity_candidate_static(row) for row in group_rows],
            }
        )
    return groups[:20]


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
    for provider in active_without_identity:
        provider_links = [
            *_rule_link_rows(provider, static_rows),
            *_advisory_link_rows(provider, [*discovery_candidates, *enriched_advisory]),
        ]
        provider_links, resolution = _resolve_provider_link_rows(provider_links)
        if resolution:
            resolution_examples.append(resolution)
            if resolution.get("resolutionReason") == "advisory_identity_disambiguation":
                resolved_by_advisory += 1
            elif resolution.get("resolutionReason") == "source_state_disambiguation":
                resolved_by_source_state += 1
        exact_static_ids = {
            clean_text(row.get("staticSourceId"))
            for row in provider_links
            if _has_exact_link_evidence(row)
        }
        provider_diagnostics = [
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
    candidate_links = [row for row in links if _is_candidate_link(row)]
    blocker_counts = Counter(
        blocker for row in links for blocker in row.get("blockers", []) if clean_text(blocker)
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
    ]
    medium_confidence = [
        row
        for row in candidate_links
        if 0.75 <= float(row.get("confidence") or 0) < 0.9
        and clean_text(row.get("recommendedAction")) == "needs_review"
    ]
    review_candidates = _review_candidates(links)
    section = {
        "activeProviderWithoutMigrationIdentityCount": len(active_without_identity),
        "candidateLinkCount": len(candidate_links),
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
        "unresolvedAmbiguousCount": int(blocker_counts.get("ambiguous_static_match") or 0),
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


def _review_candidates_markdown_rows(report: dict[str, Any]) -> list[str]:
    candidates = json_object_rows(
        as_json_object(
            as_json_object(report.get("sections")).get("providerCoverageLinkBackfill")
        ).get("reviewCandidates")
    )
    lines = [
        "| Provider | Selected static | Confidence | API eligible | Reason | Last kept | Last status | Why not high confidence |",
        "|----------|-----------------|------------|--------------|--------|-----------|-------------|-------------------------|",
    ]
    if not candidates:
        lines.append("| none | none | `0` | `false` | none | `0` | none | none |")
        return lines
    for candidate in candidates[:10]:
        evidence = as_json_object(candidate.get("sourceStateEvidence"))
        lines.append(
            "| "
            f"{clean_text(candidate.get('providerSourceName')) or clean_text(candidate.get('providerSourceId'))} | "
            f"{clean_text(candidate.get('selectedStaticSourceName')) or clean_text(candidate.get('selectedStaticSourceId'))} | "
            f"`{candidate.get('confidence')}` | "
            f"`{bool(candidate.get('apiEligible'))}` | "
            f"`{clean_text(candidate.get('resolutionReason'))}` | "
            f"`{_int_value(evidence.get('lastKeptCount'))}` | "
            f"`{clean_text(evidence.get('lastStatus'))}` | "
            f"{clean_text(candidate.get('whyNotHighConfidence')) or 'none'} |"
        )
    return lines


def _suppression_eligibility_markdown_rows(report: dict[str, Any]) -> list[str]:
    rows = json_object_rows(
        as_json_object(as_json_object(report.get("sections")).get("suppressionEligibility")).get(
            "missingLinkedStaticRows"
        )
    )
    lines = [
        "| Provider | Linked static | Readiness | Successes | Latest kept | Selection reason | Bucket | Loader match | Loader reason | Generated loader |",
        "|----------|---------------|-----------|-----------|-------------|------------------|--------|--------------|---------------|------------------|",
    ]
    if not rows:
        lines.append("| none | none | none | `0` | `0` | none | none | none | none | none |")
        return lines
    for row in rows[:10]:
        lines.append(
            "| "
            f"{clean_text(row.get('providerSourceName')) or clean_text(row.get('providerSourceId'))} | "
            f"{clean_text(row.get('migrationSourceName')) or clean_text(row.get('migrationSourceIdentity'))} | "
            f"`{clean_text(row.get('providerReplacementReadiness')) or 'unknown'}` | "
            f"`{_int_value(row.get('providerCoverageConsecutiveSuccesses'))}` | "
            f"`{_int_value(row.get('providerCoverageLatestKeptCount'))}` | "
            f"`{clean_text(row.get('selectionReason')) or clean_text(row.get('reason'))}` | "
            f"`{clean_text(row.get('registryBucket')) or 'unknown'}` | "
            f"`{clean_text(row.get('loaderNameMatchStatus')) or 'unknown'}` | "
            f"`{clean_text(row.get('loaderNotGeneratedReason')) or 'none'}` | "
            f"`{clean_text(row.get('generatedStaticLoaderName')) or 'unknown'}` |"
        )
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
        "",
        "### Review candidates",
        "",
        *_review_candidates_markdown_rows(report),
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
