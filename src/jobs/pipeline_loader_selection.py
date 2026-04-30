from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

from src.jobs.common.contracts_static_suppression_policy import (
    build_static_suppression_policy_pair,
    build_static_suppression_policy_summary,
    decide_static_suppression_from_prior_pair,
    find_prior_static_suppression_pair,
)
from src.jobs.interfaces import SourceLoader
from src.jobs.text_utils import clean_text, norm_text
from src.shared.utils import env_flag

BOARD_LEVEL_INCREMENTAL_PROVIDER_ADAPTERS = {
    "ashby",
    "breezy",
    "greenhouse",
    "jazzhr",
    "lever",
    "personio",
    "pinpoint",
    "recruitee",
    "smartrecruiters",
    "teamtailor",
    "workable",
}

DETAIL_LEVEL_INCREMENTAL_SOURCE_NAMES = {
    "social_mastodon",
    "social_x",
}

DYNAMIC_REDUNDANT_PROVIDER_REASON = "dynamic_redundant_provider"
_STATIC_SOURCE_PREFIX = "static_source::"


def build_excluded_source_report(
    source_name: str,
    reason: str,
    *,
    source_report_meta: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return {
        "name": source_name,
        "status": "excluded",
        "adapter": clean_text(source_report_meta.get(source_name, {}).get("adapter")) or "custom",
        "fetchStrategy": clean_text(source_report_meta.get(source_name, {}).get("fetchStrategy"))
        or "auto",
        "studio": clean_text(source_report_meta.get(source_name, {}).get("studio")) or "",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": clean_text(reason),
        "exclusionReason": clean_text(reason),
        "durationMs": 0,
    }


def select_pipeline_loaders(
    *,
    source_loaders: list[tuple[str, SourceLoader]] | None,
    social_enabled: bool,
    social_config: dict[str, Any],
    default_source_loaders: Callable[..., list[tuple[str, SourceLoader]]],
) -> tuple[list[tuple[str, SourceLoader]], bool]:
    if source_loaders is not None:
        return list(source_loaders), False
    try:
        return default_source_loaders(
            social_enabled=bool(social_enabled),
            social_config=social_config,
        ), True
    except TypeError:
        return default_source_loaders(), True


def _eligible_dynamic_provider_rows(
    *,
    source_state_rows: dict[str, dict[str, Any]],
    selected_provider_adapters: set[str],
) -> dict[str, tuple[str, dict[str, Any]]]:
    providers_by_static_identity: dict[str, tuple[str, dict[str, Any]]] = {}
    for source_name, row in source_state_rows.items():
        if not isinstance(row, dict):
            continue
        adapter = norm_text(row.get("lastAdapter")) or norm_text(row.get("adapter"))
        if adapter not in selected_provider_adapters:
            continue
        if norm_text(row.get("providerCoverageStatus")) != "validated_provider":
            continue
        if int(row.get("providerCoverageConsecutiveSuccesses") or 0) < 2:
            continue
        if int(row.get("providerCoverageLatestKeptCount") or 0) <= 0:
            continue
        migration_source_identity = clean_text(row.get("migrationSourceIdentity"))
        if migration_source_identity:
            providers_by_static_identity[migration_source_identity] = (clean_text(source_name), row)
    return providers_by_static_identity


def _dynamic_redundant_report(
    *,
    source_name: str,
    provider_name: str,
    provider_row: dict[str, Any],
    build_excluded_source_report: Callable[[str, str], dict[str, Any]],
) -> dict[str, Any]:
    row = build_excluded_source_report(source_name, DYNAMIC_REDUNDANT_PROVIDER_REASON)
    row.update(
        {
            "adapter": "static",
            "fetchStrategy": "auto",
            "exclusionReason": DYNAMIC_REDUNDANT_PROVIDER_REASON,
            "error": DYNAMIC_REDUNDANT_PROVIDER_REASON,
            "coveredByProviderSourceId": provider_name,
            "coveredByProviderAdapter": clean_text(provider_row.get("lastAdapter"))
            or clean_text(provider_row.get("adapter"))
            or clean_text(provider_row.get("detectedProviderFamily")),
            "providerCoverageStatus": clean_text(provider_row.get("providerCoverageStatus")),
            "providerCoverageConsecutiveSuccesses": int(
                provider_row.get("providerCoverageConsecutiveSuccesses") or 0
            ),
            "providerCoverageLatestKeptCount": int(
                provider_row.get("providerCoverageLatestKeptCount") or 0
            ),
            "migrationSourceIdentity": clean_text(provider_row.get("migrationSourceIdentity")),
        }
    )
    return row


def apply_dynamic_redundant_static_exclusions(
    selected_loaders: list[tuple[str, SourceLoader]],
    *,
    source_state_rows: dict[str, dict[str, Any]],
    build_excluded_source_report: Callable[[str, str], dict[str, Any]],
    source_report_meta: dict[str, dict[str, Any]],
    prior_static_suppression_evidence: dict[str, Any] | None = None,
) -> tuple[list[tuple[str, SourceLoader]], list[dict[str, Any]], dict[str, Any]]:
    selected_provider_adapters = {
        norm_text(source_report_meta.get(name, {}).get("adapter"))
        for name, _loader in selected_loaders
        if norm_text(source_report_meta.get(name, {}).get("adapter"))
        in BOARD_LEVEL_INCREMENTAL_PROVIDER_ADAPTERS
    }
    if not selected_provider_adapters:
        return selected_loaders, [], build_static_suppression_policy_summary([])
    providers_by_static_identity = _eligible_dynamic_provider_rows(
        source_state_rows=source_state_rows,
        selected_provider_adapters=selected_provider_adapters,
    )
    if not providers_by_static_identity:
        return selected_loaders, [], build_static_suppression_policy_summary([])

    filtered: list[tuple[str, SourceLoader]] = []
    excluded: list[dict[str, Any]] = []
    policy_pairs: list[dict[str, Any]] = []
    for source_name, loader in selected_loaders:
        if not clean_text(source_name).startswith(_STATIC_SOURCE_PREFIX):
            filtered.append((source_name, loader))
            continue
        static_identity = clean_text(source_name)[len(_STATIC_SOURCE_PREFIX) :]
        provider = providers_by_static_identity.get(static_identity)
        if provider is None:
            filtered.append((source_name, loader))
            continue
        provider_name, provider_row = provider
        prior_pair = find_prior_static_suppression_pair(
            prior_static_suppression_evidence or {},
            static_source_id=static_identity,
            static_source_name=source_name,
            provider_source_id=provider_name,
            provider_source_name=provider_name,
        )
        decision, reason = decide_static_suppression_from_prior_pair(prior_pair)
        policy_pairs.append(
            build_static_suppression_policy_pair(
                static_source_id=static_identity,
                static_source_name=source_name,
                provider_source_id=provider_name,
                provider_source_name=provider_name,
                provider_row=provider_row,
                decision=decision,
                reason=reason,
                audit_pair=prior_pair,
            )
        )
        if decision == "paused":
            filtered.append((source_name, loader))
            continue
        excluded.append(
            _dynamic_redundant_report(
                source_name=source_name,
                provider_name=provider_name,
                provider_row=provider_row,
                build_excluded_source_report=build_excluded_source_report,
            )
        )
    return filtered, excluded, build_static_suppression_policy_summary(policy_pairs)


def sort_selected_loaders(
    selected_loaders: list[tuple[str, SourceLoader]],
    *,
    source_report_meta: dict[str, dict[str, Any]],
    source_state_rows: dict[str, dict[str, Any]],
) -> list[tuple[str, SourceLoader]]:
    def _source_priority(item: tuple[str, SourceLoader]) -> tuple[int, int]:
        source_name = clean_text(item[0])
        adapter = clean_text(source_report_meta.get(source_name, {}).get("adapter"))
        state = (
            source_state_rows.get(source_name)
            if isinstance(source_state_rows.get(source_name), dict)
            else {}
        )
        duration_ms = int((state or {}).get("lastDurationMs") or 0)
        detail_pages = int((state or {}).get("lastDetailPagesVisited") or 0)
        static_priority = 0 if adapter == "static" else 1
        return (static_priority, -(duration_ms + (detail_pages * 25)))

    return sorted(selected_loaders, key=_source_priority)


def apply_source_cadence_exclusions(
    selected_loaders: list[tuple[str, SourceLoader]],
    *,
    respect_source_cadence: bool,
    source_state_rows: dict[str, dict[str, Any]],
    hot_source_cadence_minutes: int,
    cold_source_cadence_minutes: int,
    should_skip_source_by_cadence: Callable[..., bool],
    build_excluded_source_report: Callable[[str, str], dict[str, Any]],
) -> tuple[list[tuple[str, SourceLoader]], list[dict[str, Any]]]:
    if not respect_source_cadence:
        return selected_loaders, []
    cadence_skipped: list[dict[str, Any]] = []
    filtered_loaders: list[tuple[str, SourceLoader]] = []
    for name, loader in selected_loaders:
        if should_skip_source_by_cadence(
            name,
            source_state_rows,
            hot_minutes=hot_source_cadence_minutes,
            cold_minutes=cold_source_cadence_minutes,
        ):
            cadence_skipped.append(build_excluded_source_report(name, "skipped_by_source_cadence"))
            continue
        filtered_loaders.append((name, loader))
    return filtered_loaders, cadence_skipped


def apply_incremental_cache_exclusions(
    selected_loaders: list[tuple[str, SourceLoader]],
    *,
    incremental_cache_enabled: bool,
    force_refresh_all: bool,
    source_state_rows: dict[str, dict[str, Any]],
    get_incremental_cache_decision: Callable[..., dict[str, str]],
    build_excluded_source_report: Callable[[str, str], dict[str, Any]],
    source_report_meta: dict[str, dict[str, Any]],
) -> tuple[list[tuple[str, SourceLoader]], list[dict[str, Any]]]:
    if force_refresh_all or not incremental_cache_enabled:
        return selected_loaders, []
    skipped_rows: list[dict[str, Any]] = []
    filtered_loaders: list[tuple[str, SourceLoader]] = []
    for name, loader in selected_loaders:
        adapter = clean_text(source_report_meta.get(name, {}).get("adapter"))
        if (
            adapter in BOARD_LEVEL_INCREMENTAL_PROVIDER_ADAPTERS
            or name in DETAIL_LEVEL_INCREMENTAL_SOURCE_NAMES
        ):
            filtered_loaders.append((name, loader))
            continue
        decision = get_incremental_cache_decision(
            name,
            source_state_rows,
            adapter=adapter,
            force_refresh_all=force_refresh_all,
        )
        cache_decision = clean_text(decision.get("cacheDecision")) or "run_now"
        cache_reason = clean_text(decision.get("cacheDecisionReason")) or "run_now"
        if cache_decision in {"skip_fresh", "cooldown_skip"}:
            row = build_excluded_source_report(name, f"cache_{cache_reason}")
            row["cacheDecision"] = cache_decision
            row["cacheDecisionReason"] = cache_reason
            skipped_rows.append(row)
            continue
        filtered_loaders.append((name, loader))
    return filtered_loaders, skipped_rows


def build_pipeline_runtime_payload(
    *,
    selected_loaders: list[tuple[str, SourceLoader]],
    max_workers: int,
    max_per_domain: int,
    fetch_strategy: str,
    fetch_client: str,
    adapter_http_concurrency: int,
    static_detail_concurrency: int,
    google_sheets_redirect_concurrency: int,
    seed_from_existing_output: bool,
    incremental_cache_enabled: bool,
    force_refresh_all: bool,
    source_ttl_minutes: int,
    respect_source_cadence: bool,
    hot_source_cadence_minutes: int,
    cold_source_cadence_minutes: int,
    circuit_breaker_failures: int,
    circuit_breaker_cooldown_minutes: int,
    browser_fallback_cooldown_minutes: int,
    browser_fallback_enabled: bool,
    browser_fallback_cap: int,
    ignore_circuit_breaker: bool,
    social_enabled: bool,
    effective_social_config_path: str,
    social_config: dict[str, Any],
    default_social_lookback_minutes: int,
    default_social_min_confidence: int,
    default_fetch_strategy: str,
    default_static_detail_heuristics_profile: str,
    default_scrapy_validation_strict: bool,
    default_canonical_strict_url: bool,
    normalize_runtime_payload: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    return normalize_runtime_payload(
        {
            "maxWorkers": max_workers,
            "maxPerDomain": max_per_domain,
            "fetchStrategy": clean_text(fetch_strategy) or default_fetch_strategy,
            "fetchClient": fetch_client,
            "adapterHttpConcurrency": adapter_http_concurrency,
            "staticDetailConcurrency": static_detail_concurrency,
            "googleSheetsRedirectConcurrency": google_sheets_redirect_concurrency,
            "seedFromExistingOutput": bool(seed_from_existing_output),
            "incrementalCacheEnabled": bool(incremental_cache_enabled),
            "forceRefreshAll": bool(force_refresh_all),
            "sourceTtlMinutes": int(source_ttl_minutes or 0),
            "respectSourceCadence": bool(respect_source_cadence),
            "hotSourceCadenceMinutes": hot_source_cadence_minutes,
            "coldSourceCadenceMinutes": cold_source_cadence_minutes,
            "circuitBreakerFailures": int(circuit_breaker_failures or 0),
            "circuitBreakerCooldownMinutes": int(circuit_breaker_cooldown_minutes or 0),
            "browserFallbackCooldownMinutes": int(browser_fallback_cooldown_minutes or 0),
            "browserFallbackEnabled": bool(browser_fallback_enabled),
            "browserFallbackCap": int(browser_fallback_cap or 0),
            "ignoreCircuitBreaker": bool(ignore_circuit_breaker),
            "socialEnabled": bool(social_enabled),
            "socialConfigPath": str(effective_social_config_path),
            "socialLookbackMinutes": int(
                social_config.get("lookbackMinutes") or default_social_lookback_minutes
            ),
            "socialMinConfidence": int(
                social_config.get("minConfidence") or default_social_min_confidence
            ),
            "staticDetailHeuristicsProfile": norm_text(
                os.getenv("BALUFFO_STATIC_DETAIL_HEURISTICS_PROFILE")
            )
            or default_static_detail_heuristics_profile,
            "scrapyValidationStrict": env_flag(
                "BALUFFO_SCRAPY_VALIDATION_STRICT", default_scrapy_validation_strict
            ),
            "canonicalStrictUrlValidation": env_flag(
                "BALUFFO_CANONICAL_STRICT_URL", default_canonical_strict_url
            ),
            "selectedSourceCount": len(selected_loaders),
        },
        selected_source_count=len(selected_loaders),
    )
