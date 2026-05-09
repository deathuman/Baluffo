from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, cast
from urllib.parse import urlparse

from src import source_registry as source_registry_module
from src.bridge.registry_tombstones import filter_tombstoned_rows, load_tombstones
from src.source_registry import (
    load_json_array,
    source_family_key,
    source_identity,
    static_listing_url_aliases,
)

from . import config as discovery_config_module
from .config import DISCOVERY_STAGES, LOW_EVIDENCE_PROBE_LIMIT
from .core import (
    _evidence_threshold_for_probe,
    adapter_domain_fingerprint,
    classify_static_suppression,
    estimate_probe_priority,
)
from .directory_audit import directory_audit_rows, directory_audit_rows_for_method
from .io_runtime import endpoint_url
from .orchestrator_runtime import DiscoveryRunDeps, DiscoveryRunState
from .probe import validate_candidate_for_probe
from .provider_migration_advisory import stage_provider_candidates_from_advisories
from .runtime_metrics import (
    distribute_duration_by_adapter as _distribute_duration_by_adapter,
)
from .runtime_metrics import (
    increment_adapter_runtime as _increment_adapter_runtime,
)
from .runtime_metrics import (
    record_stage_timing as _record_stage_timing,
)
from .stage_control import discovery_stage_enabled as _discovery_stage_enabled
from .url_patches import apply_url_patches_to_candidate, summarize_url_patch_runtime
from .web_search import is_blocked_generic_static_url

root: Any | None = None
ProviderStaticScanRows = tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]


def _require_root() -> Any:
    if root is None:
        raise RuntimeError("source discovery orchestrator root is not bound")
    return root


def _record_stage_runtime(
    state: DiscoveryRunState,
    *,
    rows: list[dict[str, Any]],
    stage_duration_ms: int,
    failure_rows: list[dict[str, Any]] | None = None,
) -> None:
    failures = failure_rows or []
    _distribute_duration_by_adapter(
        state.adapter_runtime,
        duration_ms=stage_duration_ms,
        rows=rows,
        failure_rows=failures,
    )
    for row in rows:
        _increment_adapter_runtime(state.adapter_runtime, row.get("adapter"), generated=1)
    for row in failures:
        if isinstance(row, dict):
            _increment_adapter_runtime(state.adapter_runtime, row.get("adapter"), failures=1)


def _run_provider_static_scan_stage(
    *,
    orchestrator: Any,
    deps: DiscoveryRunDeps,
    state: DiscoveryRunState,
    enabled: bool,
    stage_key: str,
    progress_phase: str,
    progress_label: str,
    start_log: str,
    complete_log_prefix: str,
    disabled_log: str,
    scan: Callable[[], ProviderStaticScanRows],
    provider_stream: str,
    static_stream: str,
    route_failures: Callable[
        [list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]], None
    ]
    | None = None,
    after_scan: Callable[[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]], None]
    | None = None,
) -> ProviderStaticScanRows:
    if not enabled:
        orchestrator.emit_log(disabled_log)
        return [], [], []

    state.write_progress_report(
        [],
        phase=progress_phase,
        phase_label=progress_label,
        deps=deps,
        root=orchestrator,
    )
    orchestrator.emit_log(start_log)
    stage_started = time.perf_counter()
    provider_rows, static_rows, failures = scan()
    if after_scan is not None:
        after_scan(provider_rows, static_rows, failures)
    stage_rows = [*provider_rows, *static_rows]
    stage_duration_ms = _record_stage_timing(state.stage_timings_ms, stage_key, stage_started)
    _record_stage_runtime(
        state,
        rows=stage_rows,
        failure_rows=failures,
        stage_duration_ms=stage_duration_ms,
    )
    orchestrator.emit_log(
        f"{complete_log_prefix}: "
        f"provider={len(provider_rows)}, static={len(static_rows)}, failures={len(failures)}."
    )
    if route_failures is not None:
        route_failures(provider_rows, static_rows, failures)
    state.streams.append((provider_stream, provider_rows))
    state.streams.append((static_stream, static_rows))
    return provider_rows, static_rows, failures


def _is_prevalidated_discovery_candidate(row: dict[str, Any]) -> bool:
    if str(row.get("probeStatus") or "").strip().lower() != "ok":
        return False
    if not bool(row.get("prevalidatedDiscovery")):
        return False
    try:
        jobs_found = int(row.get("jobsFound") or row.get("sampleCount") or 0)
    except (TypeError, ValueError):
        return False
    return jobs_found > 0


def _route_valid_probe_candidate(
    raw: dict[str, Any],
    *,
    deps: DiscoveryRunDeps,
    state: DiscoveryRunState,
    stage: str,
    low_evidence_probes_used: int,
) -> tuple[bool, int]:
    if _is_prevalidated_discovery_candidate(raw):
        state.prevalidated_probe_inputs.append(raw)
        return True, low_evidence_probes_used

    evidence_score = int(raw.get("evidenceScore") or 0)
    threshold = _evidence_threshold_for_probe(raw, deps.thresholds)
    if evidence_score >= threshold:
        return False, low_evidence_probes_used
    if stage == "provider_pattern":
        state.skipped_low_evidence_probe_count += 1
        state.failures.append(
            {
                "name": raw.get("name"),
                "adapter": raw.get("adapter"),
                "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(),
                "error": (
                    f"pattern evidence score {evidence_score} below probe threshold {threshold}"
                ),
                "stage": "probe_skipped",
                "dropStage": "low_evidence_skipped",
                "dropReason": "probe_threshold",
            }
        )
        return True, low_evidence_probes_used
    if low_evidence_probes_used >= int(
        deps.thresholds.get("lowEvidenceProbeLimit", LOW_EVIDENCE_PROBE_LIMIT)
    ):
        state.skipped_low_evidence_probe_count += 1
        state.failures.append(
            {
                "name": raw.get("name"),
                "adapter": raw.get("adapter"),
                "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(),
                "error": f"evidence score {evidence_score} below probe threshold {threshold}",
                "stage": "probe_skipped",
                "dropStage": "low_evidence_skipped",
                "dropReason": "low_evidence_probe_cap",
            }
        )
        return True, low_evidence_probes_used
    return False, low_evidence_probes_used + 1


def _load_web_search_audit_rows(
    *,
    orchestrator: Any,
    deps: DiscoveryRunDeps,
    stage_enabled: dict[str, bool],
    cache: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    artifact = cache.get("artifact")
    if artifact is None:
        artifact, _cache_hit = orchestrator.run_web_search_directory_audit(
            deps.timeout_s,
            studio_seeds=list(discovery_config_module.STUDIO_SEEDS),
            include_seed_careers=stage_enabled["seedCareersScan"],
            include_web_search=bool(deps.include_web_search and stage_enabled["webSearch"]),
            config=deps.effective_config,
            fetcher=deps.fetcher,
        )
        cache["artifact"] = artifact
    return directory_audit_rows(artifact)


def _web_search_audit_rows_for_method(
    *,
    orchestrator: Any,
    deps: DiscoveryRunDeps,
    stage_enabled: dict[str, bool],
    cache: dict[str, Any],
    discovery_method: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    artifact = cache.get("artifact")
    if artifact is None:
        _load_web_search_audit_rows(
            orchestrator=orchestrator,
            deps=deps,
            stage_enabled=stage_enabled,
            cache=cache,
        )
        artifact = cache.get("artifact")
    return directory_audit_rows_for_method(dict(artifact or {}), discovery_method)


def _seed_careers_scan_rows(
    *,
    orchestrator: Any,
    deps: DiscoveryRunDeps,
    stage_enabled: dict[str, bool],
    web_audit_cache: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    return _web_search_audit_rows_for_method(
        orchestrator=orchestrator,
        deps=deps,
        stage_enabled=stage_enabled,
        cache=web_audit_cache,
        discovery_method="seed_careers_page",
    )


def _web_search_scan_rows(
    *,
    orchestrator: Any,
    deps: DiscoveryRunDeps,
    stage_enabled: dict[str, bool],
    web_audit_cache: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    return _web_search_audit_rows_for_method(
        orchestrator=orchestrator,
        deps=deps,
        stage_enabled=stage_enabled,
        cache=web_audit_cache,
        discovery_method="web_search",
    )


def _prepare_runtime_registry(
    *, orchestrator: Any, deps: DiscoveryRunDeps, state: DiscoveryRunState
) -> tuple[dict[str, bool], set[str], set[str], set[str]]:
    from .directory_audit import clear_directory_audit_summaries

    clear_directory_audit_summaries()
    stage_enabled = {
        "curatedSeed": _discovery_stage_enabled(deps.effective_config, "curatedSeed"),
        "sheetDirectory": _discovery_stage_enabled(deps.effective_config, "sheetDirectory"),
        "providerPatterns": _discovery_stage_enabled(deps.effective_config, "providerPatterns"),
        "seedCareersScan": _discovery_stage_enabled(deps.effective_config, "seedCareersScan"),
        "gamesmap": _discovery_stage_enabled(deps.effective_config, "gamesmap"),
        "gameprog": _discovery_stage_enabled(deps.effective_config, "gameprog"),
        "gamedevmap": _discovery_stage_enabled(deps.effective_config, "gamedevmap"),
        "webSearch": _discovery_stage_enabled(deps.effective_config, "webSearch"),
    }
    state.active = load_json_array(source_registry_module.ACTIVE_PATH, [])
    state.pending_existing = load_json_array(source_registry_module.PENDING_PATH, [])
    state.rejected = load_json_array(source_registry_module.REJECTED_PATH, [])
    state.tombstones = load_tombstones()
    state.active = filter_tombstoned_rows(state.active, state.tombstones)
    state.pending_existing = filter_tombstoned_rows(state.pending_existing, state.tombstones)
    state.rejected = filter_tombstoned_rows(state.rejected, state.tombstones)
    prior_review_candidates = load_json_array(source_registry_module.DISCOVERY_CANDIDATES_PATH, [])
    state.prior_review_candidates_by_id = {
        source_identity(row): dict(row) for row in prior_review_candidates if isinstance(row, dict)
    }
    state.ranking_registry_rows = [
        *[dict(row) for row in state.active if isinstance(row, dict)],
        *[dict(row) for row in state.pending_existing if isinstance(row, dict)],
    ]

    orchestrator.emit_log(
        f"Starting source discovery: mode={deps.mode}, preset={deps.preset_name}, top_n={deps.top_n}, web_search={'on' if deps.include_web_search else 'off'}."
    )
    orchestrator.emit_log(
        "Loaded registries: "
        f"active={len(state.active)}, pending={len(state.pending_existing)}, rejected={len(state.rejected)}."
    )

    existing_rows = [*state.active, *state.rejected]
    seen_ids = {source_identity(row) for row in existing_rows if isinstance(row, dict)}
    seen_domains = {
        fp
        for fp in (
            adapter_domain_fingerprint(row) for row in existing_rows if isinstance(row, dict)
        )
        if fp
    }
    seen_static_aliases = {
        alias_key
        for row in existing_rows
        if isinstance(row, dict)
        for alias_key in _static_alias_keys(row)
    }

    if deps.url_patch_manifest_enabled:
        state.url_patches = orchestrator.load_url_patches(deps.url_patch_manifest_path)
        state.url_patch_stats = summarize_url_patch_runtime(
            loaded=len(state.url_patches),
            added=0,
            updated=0,
            reprobed=0,
        )

    return stage_enabled, seen_ids, seen_domains, seen_static_aliases


def _run_curated_seed_stage(
    *, orchestrator: Any, deps: DiscoveryRunDeps, state: DiscoveryRunState, enabled: bool
) -> None:
    if enabled:
        orchestrator.emit_log("Generating curated seed candidates from static discovery inputs.")
        stage_started = time.perf_counter()
        curated_seed_candidates = orchestrator.stage_curated_seed_candidates()
        stage_duration_ms = _record_stage_timing(
            state.stage_timings_ms, "curatedSeed", stage_started
        )
        _record_stage_runtime(
            state, rows=curated_seed_candidates, stage_duration_ms=stage_duration_ms
        )
        orchestrator.emit_log(
            f"Curated seed generation complete: {len(curated_seed_candidates)} candidate(s)."
        )
        state.streams.append(("curated_seed", curated_seed_candidates))
    else:
        orchestrator.emit_log("Curated seed stage disabled, skipping.")


def _scan_sheet_directory(*, orchestrator: Any, deps: DiscoveryRunDeps) -> ProviderStaticScanRows:
    sheet_artifact, _sheet_cache_hit = orchestrator.run_sheet_directory_audit(
        deps.timeout_s,
        sheet_id=str(discovery_config_module.GAME_STUDIOS_SHEET_ID or "") or None,
        gid=str(discovery_config_module.GAME_STUDIOS_SHEET_GID or "") or None,
        config=deps.effective_config,
        fetcher=deps.fetcher,
    )
    return directory_audit_rows(sheet_artifact)


def _route_sheet_failures(
    *,
    orchestrator: Any,
    deps: DiscoveryRunDeps,
    state: DiscoveryRunState,
    provider_rows: list[dict[str, Any]],
    static_rows: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> None:
    if failures and (deps.fetcher is orchestrator.fetch_text or (provider_rows or static_rows)):
        state.web_failures.extend(failures)


def _run_sheet_directory_stage(
    *, orchestrator: Any, deps: DiscoveryRunDeps, state: DiscoveryRunState, enabled: bool
) -> None:
    _run_provider_static_scan_stage(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        enabled=enabled,
        stage_key="sheetDirectory",
        progress_phase="scanning_sources",
        progress_label="Scanning game studios sheet directory",
        start_log="Scanning game studios sheet directory for candidate sources.",
        complete_log_prefix="Game studios sheet scan complete",
        disabled_log="Game studios sheet stage disabled, skipping.",
        scan=lambda: _scan_sheet_directory(orchestrator=orchestrator, deps=deps),
        provider_stream="sheet_directory",
        static_stream="sheet_directory",
        route_failures=lambda provider, static, failures: _route_sheet_failures(
            orchestrator=orchestrator,
            deps=deps,
            state=state,
            provider_rows=provider,
            static_rows=static,
            failures=failures,
        ),
    )


def _run_provider_patterns_stage(
    *, orchestrator: Any, deps: DiscoveryRunDeps, state: DiscoveryRunState, enabled: bool
) -> None:
    if not enabled:
        orchestrator.emit_log("Provider-pattern stage disabled, skipping.")
        return

    state.write_progress_report(
        [],
        phase="generating_candidates",
        phase_label="Generating provider-pattern candidates",
        deps=deps,
        root=orchestrator,
    )
    orchestrator.emit_log("Generating provider-pattern candidates from the studio seed catalog.")
    stage_started = time.perf_counter()
    provider_pattern_candidates = orchestrator.build_pattern_candidates(
        list(discovery_config_module.STUDIO_SEEDS)
    )
    stage_duration_ms = _record_stage_timing(
        state.stage_timings_ms, "providerPatterns", stage_started
    )
    _record_stage_runtime(
        state, rows=provider_pattern_candidates, stage_duration_ms=stage_duration_ms
    )
    orchestrator.emit_log(
        f"Provider-pattern generation complete: {len(provider_pattern_candidates)} candidate(s)."
    )
    state.streams.append(("provider_pattern", provider_pattern_candidates))


def _run_seed_careers_stage(
    *,
    orchestrator: Any,
    deps: DiscoveryRunDeps,
    state: DiscoveryRunState,
    stage_enabled: dict[str, bool],
    web_audit_cache: dict[str, Any],
) -> None:
    _run_provider_static_scan_stage(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        enabled=stage_enabled["seedCareersScan"],
        stage_key="seedCareersScan",
        progress_phase="scanning_sources",
        progress_label="Scanning known careers pages",
        start_log="Scanning known careers pages from the seed catalog.",
        complete_log_prefix="Seed careers scan complete",
        disabled_log="Seed careers stage disabled, skipping.",
        scan=lambda: _seed_careers_scan_rows(
            orchestrator=orchestrator,
            deps=deps,
            stage_enabled=stage_enabled,
            web_audit_cache=web_audit_cache,
        ),
        provider_stream="web_provider",
        static_stream="generic_static",
        route_failures=lambda _provider, _static, failures: state.web_failures.extend(failures),
    )


def _run_gamesmap_stage(
    *, orchestrator: Any, deps: DiscoveryRunDeps, state: DiscoveryRunState, enabled: bool
) -> None:
    _run_provider_static_scan_stage(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        enabled=enabled,
        stage_key="gamesmap",
        progress_phase="scanning_sources",
        progress_label="Scanning Gamesmap directory",
        start_log="Scanning Gamesmap directory for discoverable studios.",
        complete_log_prefix="Gamesmap scan complete",
        disabled_log="Gamesmap stage disabled, skipping.",
        scan=lambda: orchestrator.discover_gamesmap_candidates(
            deps.timeout_s,
            config=deps.effective_config,
            fetcher=deps.fetcher,
        ),
        provider_stream="web_provider",
        static_stream="generic_static",
        route_failures=lambda _provider, _static, failures: state.web_failures.extend(failures),
    )


def _scan_gameprog(*, orchestrator: Any, deps: DiscoveryRunDeps) -> ProviderStaticScanRows:
    gameprog_config = dict(deps.effective_config.get("gameprog") or {})
    config_with_gameprog = dict(deps.effective_config)
    config_with_gameprog["gameprog"] = gameprog_config
    return cast(
        ProviderStaticScanRows,
        orchestrator.discover_gameprog_candidates(
            deps.timeout_s,
            config=config_with_gameprog,
            fetcher=deps.fetcher,
        ),
    )


def _run_gameprog_stage(
    *, orchestrator: Any, deps: DiscoveryRunDeps, state: DiscoveryRunState, enabled: bool
) -> None:
    _run_provider_static_scan_stage(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        enabled=enabled,
        stage_key="gameprog",
        progress_phase="scanning_sources",
        progress_label="Scanning Gameprog directory",
        start_log="Scanning Gameprog directory for discoverable studios.",
        complete_log_prefix="Gameprog scan complete",
        disabled_log="Gameprog stage disabled, skipping.",
        scan=lambda: _scan_gameprog(orchestrator=orchestrator, deps=deps),
        provider_stream="web_provider",
        static_stream="generic_static",
        route_failures=lambda _provider, _static, failures: state.web_failures.extend(failures),
    )


def _scan_gamedevmap(*, orchestrator: Any, deps: DiscoveryRunDeps) -> ProviderStaticScanRows:
    return cast(
        ProviderStaticScanRows,
        orchestrator.discover_gamedevmap_candidates(
            deps.timeout_s,
            config=deps.effective_config,
            fetcher=deps.fetcher,
        ),
    )


def _capture_gamedevmap_summary(
    *,
    deps: DiscoveryRunDeps,
    state: DiscoveryRunState,
    _provider: list[dict[str, Any]],
    _static: list[dict[str, Any]],
    _failures: list[dict[str, Any]],
) -> None:
    from .gamedevmap_active_dry_run import latest_gamedevmap_audit_report_summary

    state.gamedevmap_audit_summary = latest_gamedevmap_audit_report_summary()


def _run_gamedevmap_stage(
    *, orchestrator: Any, deps: DiscoveryRunDeps, state: DiscoveryRunState, enabled: bool
) -> None:
    _run_provider_static_scan_stage(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        enabled=enabled,
        stage_key="gamedevmap",
        progress_phase="scanning_sources",
        progress_label="Scanning GameDevMap directory",
        start_log="Scanning GameDevMap directory for discoverable studios.",
        complete_log_prefix="GameDevMap scan complete",
        disabled_log="GameDevMap stage disabled, skipping.",
        scan=lambda: _scan_gamedevmap(orchestrator=orchestrator, deps=deps),
        provider_stream="web_provider",
        static_stream="generic_static",
        route_failures=lambda _provider, _static, failures: state.web_failures.extend(failures),
        after_scan=lambda provider, static, failures: _capture_gamedevmap_summary(
            deps=deps,
            state=state,
            _provider=provider,
            _static=static,
            _failures=failures,
        ),
    )


def _run_web_search_stage(
    *,
    orchestrator: Any,
    deps: DiscoveryRunDeps,
    state: DiscoveryRunState,
    stage_enabled: dict[str, bool],
    web_audit_cache: dict[str, Any],
) -> None:
    if not deps.include_web_search:
        return
    _run_provider_static_scan_stage(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        enabled=stage_enabled["webSearch"],
        stage_key="webSearch",
        progress_phase="generating_candidates",
        progress_label="Running web-search discovery queries",
        start_log="Running web-search discovery queries.",
        complete_log_prefix="Web-search discovery complete",
        disabled_log="Web-search stage disabled, skipping.",
        scan=lambda: _web_search_scan_rows(
            orchestrator=orchestrator,
            deps=deps,
            stage_enabled=stage_enabled,
            web_audit_cache=web_audit_cache,
        ),
        provider_stream="web_provider",
        static_stream="generic_static",
        route_failures=lambda _provider, _static, failures: state.web_failures.extend(failures),
    )


def _run_dynamic_directory_stages(
    *,
    orchestrator: Any,
    deps: DiscoveryRunDeps,
    state: DiscoveryRunState,
    stage_enabled: dict[str, bool],
) -> None:
    if deps.mode != "dynamic":
        return

    _run_provider_patterns_stage(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        enabled=stage_enabled["providerPatterns"],
    )
    web_audit_cache: dict[str, Any] = {}
    _run_seed_careers_stage(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        stage_enabled=stage_enabled,
        web_audit_cache=web_audit_cache,
    )
    _run_gamesmap_stage(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        enabled=stage_enabled["gamesmap"],
    )
    _run_gameprog_stage(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        enabled=stage_enabled["gameprog"],
    )
    _run_gamedevmap_stage(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        enabled=stage_enabled["gamedevmap"],
    )
    _run_web_search_stage(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        stage_enabled=stage_enabled,
        web_audit_cache=web_audit_cache,
    )


def _record_duplicate_drop(*, state: DiscoveryRunState, row: dict[str, Any], reason: str) -> None:
    state.skipped_duplicate_count += 1
    state.duplicate_reasons[reason] += 1
    state.dedupe_drop_rows.append(
        {
            "name": row.get("name"),
            "adapter": row.get("adapter"),
            "stage": "dedupe_skipped",
            "error": reason,
            "dropStage": "dedupe_skipped",
            "dropReason": reason,
        }
    )


def _candidate_duplicate_reason(
    *,
    row_id: str,
    row_domain: str,
    seen_ids: set[str],
    seen_domains: set[str],
    local_seen_ids: set[str],
    local_seen_domains: set[str],
) -> str | None:
    if row_id in seen_ids:
        return "existing_id"
    if row_domain and row_domain in seen_domains:
        return "existing_domain"
    if row_id in local_seen_ids:
        return "run_id"
    if row_domain and row_domain in local_seen_domains:
        return "run_domain"
    return None


def _static_alias_keys(row: dict[str, Any]) -> set[str]:
    family_key = source_family_key(row)
    if not family_key:
        return set()
    return {f"{family_key}\t{alias}" for alias in static_listing_url_aliases(row)}


def _dedupe_discovered_candidates(
    *,
    state: DiscoveryRunState,
    discovered: list[dict[str, Any]],
    seen_ids: set[str],
    seen_domains: set[str],
    seen_static_aliases: set[str],
) -> None:
    local_seen_ids = set(seen_ids)
    local_seen_domains = set(seen_domains)
    local_seen_static_aliases = set(seen_static_aliases)
    for row in discovered:
        stage = str(row.get("discoveryStage") or "provider_pattern")
        row_id = source_identity(row)
        row_domain = adapter_domain_fingerprint(row) or ""
        row_static_aliases = _static_alias_keys(row)
        duplicate_reason = _candidate_duplicate_reason(
            row_id=row_id,
            row_domain=row_domain,
            seen_ids=seen_ids,
            seen_domains=seen_domains,
            local_seen_ids=local_seen_ids,
            local_seen_domains=local_seen_domains,
        )
        if not duplicate_reason and row_static_aliases:
            if row_static_aliases & seen_static_aliases:
                duplicate_reason = "existing_static_url_alias"
            elif row_static_aliases & local_seen_static_aliases:
                duplicate_reason = "run_static_url_alias"
        if duplicate_reason:
            _record_duplicate_drop(state=state, row=row, reason=duplicate_reason)
            continue
        local_seen_ids.add(row_id)
        if row_domain:
            local_seen_domains.add(row_domain)
        local_seen_static_aliases.update(row_static_aliases)
        state.survived_dedupe_count_by_stage[stage] = (
            state.survived_dedupe_count_by_stage.get(stage, 0) + 1
        )
        state.filtered.append(row)


def _record_sheet_static_cap_suppressions(
    *, state: DiscoveryRunState, suppressed_rows: list[dict[str, Any]]
) -> None:
    for raw in suppressed_rows:
        stage = str(raw.get("discoveryStage") or "provider_pattern")
        state.suppressed_static_count += 1
        state.suppressed_static_by_reason["sheet_directory_stage_cap"] += 1
        state.suppressed_static_by_stage[stage] += 1
        state.failures.append(
            {
                "name": raw.get("name"),
                "adapter": raw.get("adapter"),
                "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(),
                "error": "sheet_directory_stage_cap",
                "stage": "suppressed_static",
                "dropStage": "suppressed_static",
                "dropReason": "sheet_directory_stage_cap",
            }
        )


def _prepare_dedupe_and_source_state(
    *,
    orchestrator: Any,
    deps: DiscoveryRunDeps,
    state: DiscoveryRunState,
    seen_ids: set[str],
    seen_domains: set[str],
    seen_static_aliases: set[str],
) -> None:
    from .directory_audit import latest_directory_audit_summaries

    state.directory_audit_summaries = latest_directory_audit_summaries()
    stage_started = time.perf_counter()
    discovered = orchestrator.merge_candidate_streams(state.streams)
    staged_providers = stage_provider_candidates_from_advisories(
        discovered,
        active_rows=state.active,
        pending_rows=state.pending_existing,
        at=deps.started_at,
    )
    if staged_providers:
        discovered.extend(staged_providers)
        orchestrator.emit_log(f"Staged provider migration candidate(s): {len(staged_providers)}.")
    for row in discovered:
        stage_key = str(row.get("discoveryStage") or "provider_pattern")
        state.generated_count_by_stage[stage_key] = (
            state.generated_count_by_stage.get(stage_key, 0) + 1
        )
    state.found_endpoint_count = len(discovered)
    orchestrator.emit_log(
        "Generated candidates by stage: "
        + ", ".join(
            f"{stage}={state.generated_count_by_stage.get(stage, 0)}" for stage in DISCOVERY_STAGES
        )
        + f" (total={state.found_endpoint_count})."
    )
    _dedupe_discovered_candidates(
        state=state,
        discovered=discovered,
        seen_ids=seen_ids,
        seen_domains=seen_domains,
        seen_static_aliases=seen_static_aliases,
    )
    _record_stage_timing(state.stage_timings_ms, "dedupeFilter", stage_started)
    state.filtered.sort(key=estimate_probe_priority, reverse=True)
    raw_source_state_rows = orchestrator.read_source_state(
        source_registry_module.ACTIVE_PATH.parent / "jobs-source-state.json"
    )
    state.source_state_rows = (
        dict(raw_source_state_rows) if isinstance(raw_source_state_rows, dict) else {}
    )
    state.filtered, sheet_static_suppressed = orchestrator.apply_sheet_directory_static_probe_cap(
        state.filtered,
        top_n=deps.top_n,
        bypass_cap=deps.sheet_static_probe_cap_bypassed,
        source_state_rows=state.source_state_rows,
    )
    orchestrator.emit_log(
        "After dedupe: "
        + ", ".join(
            f"{stage}={state.survived_dedupe_count_by_stage.get(stage, 0)}"
            for stage in DISCOVERY_STAGES
        )
        + f"; skipped_duplicates={state.skipped_duplicate_count}."
    )
    state.failures = [
        {**row, "dropStage": "page_fetch", "dropReason": "page_fetch"}
        for row in list(state.web_failures)
        if isinstance(row, dict)
    ]
    state.failures.extend(state.dedupe_drop_rows)
    _record_sheet_static_cap_suppressions(state=state, suppressed_rows=sheet_static_suppressed)


def _record_static_suppression(
    *,
    state: DiscoveryRunState,
    raw: dict[str, Any],
    stage: str,
    reason: str,
    domain_url: str,
) -> None:
    state.suppressed_static_count += 1
    state.suppressed_static_by_reason[reason] += 1
    state.suppressed_static_by_stage[stage] += 1
    state.failures.append(
        {
            "name": raw.get("name"),
            "adapter": raw.get("adapter"),
            "domain": (urlparse(domain_url).netloc or "").lower(),
            "error": reason,
            "stage": "suppressed_static",
            "dropStage": "suppressed_static",
            "dropReason": reason,
        }
    )


def _blocked_static_url(raw: dict[str, Any]) -> str:
    if str(raw.get("adapter") or "").strip().lower() != "static":
        return ""
    blocked_url = str(
        raw.get("listing_url") or raw.get("careersUrl") or endpoint_url(raw) or ""
    ).strip()
    if blocked_url and is_blocked_generic_static_url(blocked_url):
        return blocked_url
    return ""


def _route_probe_candidate(
    *,
    raw: dict[str, Any],
    deps: DiscoveryRunDeps,
    state: DiscoveryRunState,
    stage: str,
    low_evidence_probes_used: int,
) -> tuple[bool, int]:
    blocked_url = _blocked_static_url(raw)
    if blocked_url:
        _record_static_suppression(
            state=state,
            raw=raw,
            stage=stage,
            reason="blocked_domain",
            domain_url=blocked_url,
        )
        return True, low_evidence_probes_used

    suppression_reason = classify_static_suppression(
        raw,
        source_state_rows=state.source_state_rows,
        thresholds=deps.thresholds,
    )
    if suppression_reason:
        _record_static_suppression(
            state=state,
            raw=raw,
            stage=stage,
            reason=suppression_reason,
            domain_url=endpoint_url(raw),
        )
        return True, low_evidence_probes_used

    valid, invalid_reason = validate_candidate_for_probe(raw)
    if not valid:
        state.skipped_invalid += 1
        state.validation_skipped_count += 1
        state.failures.append(
            {
                "name": raw.get("name"),
                "adapter": raw.get("adapter"),
                "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(),
                "error": invalid_reason,
                "stage": "validation",
                "dropStage": "validation",
                "dropReason": "validation",
            }
        )
        return True, low_evidence_probes_used

    routed, low_evidence_probes_used = _route_valid_probe_candidate(
        raw,
        deps=deps,
        state=state,
        stage=stage,
        low_evidence_probes_used=low_evidence_probes_used,
    )
    if routed:
        return True, low_evidence_probes_used
    state.probe_inputs.append(raw)
    return True, low_evidence_probes_used


def _prepare_probe_queue(
    *, orchestrator: Any, deps: DiscoveryRunDeps, state: DiscoveryRunState
) -> None:
    low_evidence_probes_used = 0
    state.write_progress_report(
        [],
        phase="generating_candidates",
        phase_label="Generating initial discovery candidates",
        deps=deps,
        root=orchestrator,
    )
    orchestrator.emit_log(f"Starting probe phase for {len(state.filtered)} candidate(s).")
    state.write_progress_report(
        state.queueable_candidates,
        phase="probing_candidates",
        phase_label=f"Probing {len(state.filtered)} candidate(s)",
        deps=deps,
        root=orchestrator,
    )

    for raw in state.filtered:
        raw, _patch_applied = apply_url_patches_to_candidate(raw, state.url_patches)
        stage = str(raw.get("discoveryStage") or "provider_pattern")
        _routed, low_evidence_probes_used = _route_probe_candidate(
            raw=raw,
            deps=deps,
            state=state,
            stage=stage,
            low_evidence_probes_used=low_evidence_probes_used,
        )


def prepare_probe_inputs(*, deps: DiscoveryRunDeps, state: DiscoveryRunState) -> None:
    orchestrator = _require_root()
    stage_enabled, seen_ids, seen_domains, seen_static_aliases = _prepare_runtime_registry(
        orchestrator=orchestrator, deps=deps, state=state
    )
    _run_curated_seed_stage(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        enabled=stage_enabled["curatedSeed"],
    )
    _run_sheet_directory_stage(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        enabled=stage_enabled["sheetDirectory"],
    )
    _run_dynamic_directory_stages(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        stage_enabled=stage_enabled,
    )
    _prepare_dedupe_and_source_state(
        orchestrator=orchestrator,
        deps=deps,
        state=state,
        seen_ids=seen_ids,
        seen_domains=seen_domains,
        seen_static_aliases=seen_static_aliases,
    )
    _prepare_probe_queue(orchestrator=orchestrator, deps=deps, state=state)
