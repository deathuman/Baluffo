#!/usr/bin/env python3
"""Provider/static link row resolution and review candidates.

Leaf of ``scripts/source_policy_soak_report.py``; every unit body is byte-identical to the pre-split
module. The coordinator imports and re-exports these names.
"""

from __future__ import annotations

import sys
from collections import Counter
from fnmatch import fnmatch
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
del _repo_root

from scripts.source_policy_soak_report_evidence import (
    _first_clean_text,
    _first_int_value,
    _source_identity_tokens,
    _source_name,
)
from scripts.source_policy_soak_report_sections import _url_from_row, _url_host
from scripts.source_policy_soak_report_spec import (
    PROVIDER_COVERAGE_REVIEW_BLOCKING_DISAMBIGUATION_REASONS,
    PROVIDER_ID_FIELDS,
    STATIC_LIKE_ADAPTERS,
    STATIC_LIKE_STAGES,
    SUPPORTED_PROVIDERS,
    _int_value,
    as_json_list,
    clean_text,
    norm_text,
    source_identity,
)

__all__ = [
    "Any",
    "Counter",
    "PROVIDER_COVERAGE_REVIEW_BLOCKING_DISAMBIGUATION_REASONS",
    "PROVIDER_ID_FIELDS",
    "STATIC_LIKE_ADAPTERS",
    "STATIC_LIKE_STAGES",
    "SUPPORTED_PROVIDERS",
    "_active_registry_static_link",
    "_advisory_link_rows",
    "_advisory_provider_keys",
    "_advisory_static_candidate",
    "_ambiguity_candidate_static",
    "_ambiguity_groups",
    "_block_colliding_static_link_targets",
    "_blocked_link_actionability",
    "_blocked_link_candidates",
    "_blocker_examples",
    "_candidate_static_id",
    "_company_name_only_blockers",
    "_dedupe_link_rows",
    "_deterministic_static_ambiguity_resolution",
    "_disambiguation_blocker_counts",
    "_first_clean_text",
    "_first_int_value",
    "_has_exact_link_evidence",
    "_host_matches_pattern",
    "_ignored_alternative_row",
    "_int_value",
    "_is_candidate_link",
    "_is_strong_source_state_candidate",
    "_link_blockers",
    "_link_reasons",
    "_linked_provider_row",
    "_positive_static_history",
    "_potential_review_link",
    "_provider_coverage_link_backfill_sort_key",
    "_provider_id_pair",
    "_provider_id_value",
    "_provider_identity_keys",
    "_provider_link_row",
    "_provider_matches_rule",
    "_provider_shaped_static_identity",
    "_provider_shaped_static_link_blockers",
    "_provider_weak_host_rows",
    "_recommended_api_payload",
    "_registry_backed_static_link",
    "_registry_static_candidate",
    "_resolution_example",
    "_resolve_provider_link_rows",
    "_review_candidates",
    "_source_identity_tokens",
    "_source_name",
    "_source_state_evidence",
    "_source_state_for_static",
    "_static_candidate",
    "_static_candidates",
    "_static_evidence",
    "_static_url_key",
    "_suppress_unbacked_advisory_links_when_registry_link_exists",
    "_url_from_row",
    "_url_host",
    "_why_not_high_confidence",
    "_with_ignored_alternative",
    "_with_selected_link",
    "as_json_list",
    "clean_text",
    "fnmatch",
    "norm_text",
    "source_identity",
    "urlparse",
]


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
    last_successful_at = _first_clean_text(state, "lastSuccessfulAt", "lastSuccessAt")
    last_fetched_at = _first_clean_text(state, "lastFetchedAt", "lastRunAt")
    provider_coverage_status = _first_clean_text(state, "providerCoverageStatus")
    provider_coverage_validated = provider_coverage_status == "validated_provider"
    provider_coverage_consecutive_successes = _first_int_value(
        state, "providerCoverageConsecutiveSuccesses"
    )
    provider_coverage_latest_kept_count = _first_int_value(state, "providerCoverageLatestKeptCount")
    provider_replacement_readiness = _first_clean_text(state, "providerReplacementReadiness")

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
    if provider_coverage_validated:
        score += 10
        reasons.append("provider_coverage_validated")
    elif provider_coverage_status:
        blockers.append("source_state_not_ok")
    if provider_coverage_consecutive_successes > 0:
        score += min(provider_coverage_consecutive_successes, 5)
        reasons.append("provider_coverage_success_history")
    if provider_coverage_latest_kept_count > 0:
        reasons.append("provider_coverage_latest_kept")
    if last_successful_at:
        score += 5
        reasons.append("source_state_success_timestamp")
    if last_fetched_at:
        reasons.append("source_state_fetched")
    has_promotable_source_state_signal = any(
        (
            last_kept > 0,
            last_status == "ok",
            bool(last_successful_at),
            provider_coverage_status == "validated_provider",
            provider_coverage_latest_kept_count > 0,
        )
    )
    if not state:
        blockers.append("no_source_state_history")
        if score <= 0:
            blockers.append("static_only_evidence_present")
    elif has_promotable_source_state_signal and "source_state_not_ok" not in blockers:
        if provider_coverage_validated and provider_coverage_consecutive_successes < 2:
            blockers.append("insufficient_provider_success_history")
        elif not provider_coverage_validated:
            blockers.append("source_state_not_ok")
    return {
        "lastKeptCount": last_kept,
        "lastStatus": last_status,
        "lastSuccessfulAt": last_successful_at,
        "lastFetchedAt": last_fetched_at,
        "providerCoverageStatus": provider_coverage_status,
        "providerCoverageConsecutiveSuccesses": provider_coverage_consecutive_successes,
        "providerCoverageLatestKeptCount": provider_coverage_latest_kept_count,
        "providerReplacementReadiness": provider_replacement_readiness,
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
        "lastSuccessfulAt": _first_clean_text(static, "lastSuccessfulAt", "lastSuccessAt"),
        "lastFetchedAt": _first_clean_text(static, "lastFetchedAt", "lastRunAt"),
        "providerCoverageStatus": clean_text(static.get("providerCoverageStatus")),
        "providerCoverageConsecutiveSuccesses": _int_value(
            static.get("providerCoverageConsecutiveSuccesses")
        ),
        "providerCoverageLatestKeptCount": _int_value(
            static.get("providerCoverageLatestKeptCount")
        ),
        "providerReplacementReadiness": clean_text(static.get("providerReplacementReadiness")),
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


def _provider_shaped_static_link_blockers(
    provider: dict[str, Any], static: dict[str, Any]
) -> list[str]:
    static_id = norm_text(static.get("staticSourceId"))
    if not static_id:
        return []
    if static_id in {norm_text(key) for key in _provider_identity_keys(provider)}:
        return ["provider_shaped_self_link"]
    static_adapter = static_id.split(":", 1)[0]
    if static_adapter in {norm_text(provider) for provider in SUPPORTED_PROVIDERS}:
        return ["provider_shaped_static_identity"]
    return []


def _registry_static_candidate(
    static: dict[str, Any], registry_static_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    static_id = clean_text(static.get("staticSourceId"))
    static_url = clean_text(static.get("staticUrl"))
    for candidate in registry_static_rows:
        if static_id and static_id == clean_text(candidate.get("staticSourceId")):
            return dict(candidate)
        if static_url and static_url == clean_text(candidate.get("staticUrl")):
            return dict(candidate)
    return {}


def _advisory_link_rows(
    provider: dict[str, Any],
    advisory_rows: list[dict[str, Any]],
    registry_static_rows: list[dict[str, Any]],
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
        registry_static = _registry_static_candidate(static, registry_static_rows)
        link_static = registry_static or static
        blockers = _provider_shaped_static_link_blockers(provider, link_static)
        rows.append(
            _provider_link_row(
                provider,
                link_static,
                confidence=0.8,
                reasons=["provider_migration_advisory_exact_identity"],
                blockers=blockers,
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


def _dedupe_link_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: list[dict[str, Any]] = []
    seen: dict[tuple[Any, ...], int] = {}
    action_priority = {
        "already_linked": 4,
        "backfill_migration_identity_candidate": 3,
        "needs_review": 2,
        "ambiguous_static_match": 1,
        "insufficient_evidence": 0,
    }
    for row in rows:
        blockers = tuple(_link_blockers(row))
        static_id = clean_text(row.get("staticSourceId"))
        static_url = clean_text(row.get("staticUrl"))
        signature = (
            clean_text(row.get("providerSourceId")),
            static_id,
            static_url,
            blockers,
        )
        if not static_id and not static_url:
            signature = (
                *signature,
                clean_text(row.get("recommendedAction")),
                round(float(row.get("confidence") or 0), 2),
                tuple(_link_reasons(row)),
            )
        if signature not in seen:
            seen[signature] = len(unique)
            unique.append(row)
            continue
        index = seen[signature]
        existing = unique[index]
        existing_rank = (
            action_priority.get(clean_text(existing.get("recommendedAction")), -1),
            float(existing.get("confidence") or 0),
        )
        row_rank = (
            action_priority.get(clean_text(row.get("recommendedAction")), -1),
            float(row.get("confidence") or 0),
        )
        merged = dict(row if row_rank > existing_rank else existing)
        merged["reasons"] = sorted({*_link_reasons(existing), *_link_reasons(row)})
        merged["blockers"] = sorted({*_link_blockers(existing), *_link_blockers(row)})
        unique[index] = merged
    return unique


def _suppress_unbacked_advisory_links_when_registry_link_exists(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    has_registry_backed_candidate = any(
        clean_text(row.get("registryState")) in {"active", "pending"}
        and clean_text(row.get("recommendedAction"))
        in {"backfill_migration_identity_candidate", "needs_review"}
        and not _link_blockers(row)
        for row in rows
    )
    if not has_registry_backed_candidate:
        return rows
    return [
        row
        for row in rows
        if clean_text(row.get("registryState")) in {"active", "pending"}
        or "provider_migration_advisory_exact_identity" not in set(_link_reasons(row))
    ]


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
    if clean_text(row.get("providerCoverageStatus")) != "validated_provider":
        return False
    if _int_value(row.get("lastKeptCount")) <= 0:
        return False
    if _int_value(row.get("providerCoverageConsecutiveSuccesses")) < 2:
        return False
    return clean_text(row.get("lastStatus")) == "ok" or bool(
        clean_text(row.get("lastSuccessfulAt"))
    )


def _static_url_key(row: dict[str, Any]) -> str:
    url = clean_text(row.get("staticUrl"))
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except ValueError:
        return norm_text(url).rstrip("/")
    host = (parsed.netloc or "").lower()
    host = host[4:] if host.startswith("www.") else host
    path = (parsed.path or "/").rstrip("/").lower() or "/"
    query = (parsed.query or "").lower()
    return f"{host}{path}?{query}" if query else f"{host}{path}"


def _active_registry_static_link(row: dict[str, Any]) -> bool:
    return (
        _registry_backed_static_link(row)
        and clean_text(row.get("registryState")) == "active"
        and not bool(row.get("hiddenFromDefault"))
        and not clean_text(row.get("duplicateOfSourceId"))
    )


def _positive_static_history(row: dict[str, Any]) -> bool:
    if _int_value(row.get("lastKeptCount")) <= 0:
        return False
    if (
        clean_text(row.get("providerCoverageStatus"))
        and _int_value(row.get("providerCoverageConsecutiveSuccesses")) < 2
    ):
        return False
    return clean_text(row.get("lastStatus")) == "ok" or bool(
        clean_text(row.get("lastSuccessfulAt"))
    )


def _deterministic_static_ambiguity_resolution(
    ambiguous: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]], str, float] | None:
    registry_static = [row for row in ambiguous if _registry_backed_static_link(row)]
    if not registry_static:
        return None
    non_registry = [row for row in ambiguous if row not in registry_static]
    if (
        len(registry_static) == 1
        and non_registry
        and all(_provider_shaped_static_identity(row) for row in non_registry)
    ):
        selected = registry_static[0]
        return (
            selected,
            [row for row in ambiguous if row is not selected],
            "registry_static_disambiguation",
            0.95,
        )

    url_keys = {_static_url_key(row) for row in registry_static if _static_url_key(row)}
    if len(registry_static) > 1 and len(url_keys) == 1:
        active = [row for row in registry_static if _active_registry_static_link(row)]
        if len(active) == 1:
            selected = active[0]
            return (
                selected,
                [row for row in ambiguous if row is not selected],
                "active_static_canonical_url_disambiguation",
                0.95,
            )

    positive_history = [row for row in registry_static if _positive_static_history(row)]
    if len(registry_static) > 1 and len(positive_history) == 1:
        selected = positive_history[0]
        return (
            selected,
            [row for row in ambiguous if row is not selected],
            "static_history_disambiguation",
            0.8,
        )
    return None


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
    updated["blockers"] = sorted(
        {
            clean_text(blocker)
            for blocker in as_json_list(row.get("disambiguationBlockers"))
            if clean_text(blocker) in PROVIDER_COVERAGE_REVIEW_BLOCKING_DISAMBIGUATION_REASONS
        }
    )
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
        "providerCoverageStatus": clean_text(row.get("providerCoverageStatus")),
        "providerCoverageConsecutiveSuccesses": _int_value(
            row.get("providerCoverageConsecutiveSuccesses")
        ),
        "providerCoverageLatestKeptCount": _int_value(row.get("providerCoverageLatestKeptCount")),
        "providerReplacementReadiness": clean_text(row.get("providerReplacementReadiness")),
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


def _provider_coverage_link_backfill_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        clean_text(row.get("providerSourceName")).lower(),
        clean_text(row.get("providerSourceId")).lower(),
        clean_text(row.get("staticSourceName")).lower(),
        clean_text(row.get("staticSourceId")).lower(),
        -float(row.get("confidence") or 0.0),
        clean_text(row.get("recommendedAction")).lower(),
    )


def _blocked_link_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    blocked = []
    for row in rows:
        if not _link_blockers(row):
            continue
        actionability, reason = _blocked_link_actionability(row)
        blocked.append(
            {
                **row,
                "apiEligible": False,
                "actionability": actionability,
                "actionabilityReason": reason,
            }
        )
    blocked.sort(key=_provider_coverage_link_backfill_sort_key)
    return blocked


def _provider_shaped_static_identity(row: dict[str, Any]) -> bool:
    static_id = norm_text(row.get("staticSourceId"))
    if not static_id:
        return False
    if static_id == norm_text(row.get("providerSourceId")):
        return True
    static_adapter = static_id.split(":", 1)[0]
    return static_adapter in {norm_text(provider) for provider in SUPPORTED_PROVIDERS}


def _registry_backed_static_link(row: dict[str, Any]) -> bool:
    return (
        clean_text(row.get("registryState")) in {"active", "pending"}
        and bool(clean_text(row.get("staticSourceId")))
        and not _provider_shaped_static_identity(row)
    )


def _blocked_link_actionability(row: dict[str, Any]) -> tuple[str, str]:
    blockers = set(_link_blockers(row))
    if "provider_shaped_self_link" in blockers or "provider_shaped_static_identity" in blockers:
        return "non_actionable", "provider_shaped_self_link"
    if "ambiguous_static_match" in blockers:
        if _registry_backed_static_link(row):
            return "actionable", "registry_backed_ambiguous_static_match"
        return "non_actionable", "unbacked_ambiguous_static_match"
    return "actionable", "blocked_link_requires_review"


def _potential_review_link(row: dict[str, Any]) -> bool:
    confidence = float(row.get("confidence") or 0)
    if confidence < 0.75 or _link_blockers(row):
        return False
    if clean_text(row.get("recommendedAction")) not in {
        "backfill_migration_identity_candidate",
        "needs_review",
    }:
        return False
    return bool(clean_text(row.get("providerSourceId")) and clean_text(row.get("staticSourceId")))


def _block_colliding_static_link_targets(rows: list[dict[str, Any]]) -> None:
    by_static: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not _potential_review_link(row):
            continue
        static_id = clean_text(row.get("staticSourceId")).lower()
        by_static.setdefault(static_id, []).append(row)
    for collisions in by_static.values():
        provider_ids = {clean_text(row.get("providerSourceId")).lower() for row in collisions}
        if len(provider_ids) <= 1:
            continue
        for row in collisions:
            row["blockers"] = sorted({*_link_blockers(row), "static_link_target_collision"})


def _disambiguation_blocker_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(
        blocker
        for row in rows
        for blocker in as_json_list(row.get("disambiguationBlockers"))
        if clean_text(blocker)
    )
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _recommended_api_payload(row: dict[str, Any]) -> dict[str, Any]:
    confidence = round(float(row.get("confidence") or 0), 2)
    if not _registry_backed_static_link(row):
        return {}
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

    deterministic = _deterministic_static_ambiguity_resolution(ambiguous)
    if deterministic:
        selected, ignored, reason, confidence = deterministic
        return [
            _with_selected_link(selected, confidence=confidence, reason=reason),
            *[_with_ignored_alternative(row, f"resolved_by_{reason}") for row in ignored],
            *[row for row in rows if row not in ambiguous],
        ], _resolution_example(selected=selected, ignored=ignored, reason=reason)

    ranked = sorted(ambiguous, key=lambda row: _int_value(row.get("evidenceScore")), reverse=True)
    for rank, row in enumerate(ranked, start=1):
        row["disambiguationRank"] = rank
    strong_history = [
        row
        for row in ambiguous
        if _int_value(row.get("lastKeptCount")) > 0
        or clean_text(row.get("lastStatus"))
        or clean_text(row.get("providerCoverageStatus"))
    ]
    if len(strong_history) > 1:
        signatures = Counter(
            (
                clean_text(row.get("lastStatus")),
                _int_value(row.get("lastKeptCount")),
                clean_text(row.get("lastSuccessfulAt")),
                clean_text(row.get("lastFetchedAt")),
                clean_text(row.get("providerCoverageStatus")),
                _int_value(row.get("providerCoverageConsecutiveSuccesses")),
                _int_value(row.get("providerCoverageLatestKeptCount")),
            )
            for row in strong_history
        )
        if any(count > 1 for signature, count in signatures.items() if any(signature)):
            for row in strong_history:
                blockers = set(_link_blockers(row))
                blockers.add("multiple_static_candidates_with_equal_history")
                row["blockers"] = sorted(blockers)
                disambiguation_blockers = {
                    clean_text(blocker)
                    for blocker in as_json_list(row.get("disambiguationBlockers"))
                    if clean_text(blocker)
                }
                disambiguation_blockers.add("multiple_static_candidates_with_equal_history")
                row["disambiguationBlockers"] = sorted(disambiguation_blockers)
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
