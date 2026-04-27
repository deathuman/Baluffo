from __future__ import annotations

import time
from typing import Any
from urllib.parse import urlparse

from src import source_registry as source_registry_module
from src.bridge.registry_tombstones import filter_tombstoned_rows, load_tombstones
from src.source_registry import load_json_array, source_identity

from . import config as discovery_config_module
from .audit_config import audit_enabled
from .config import DISCOVERY_STAGES, LOW_EVIDENCE_PROBE_LIMIT
from .core import (
    _evidence_threshold_for_probe,
    adapter_domain_fingerprint,
    classify_static_suppression,
    estimate_probe_priority,
)
from .directory_audit import directory_audit_rows
from .io_runtime import endpoint_url
from .orchestrator_runtime import DiscoveryRunDeps, DiscoveryRunState
from .probe import validate_candidate_for_probe
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


def _web_search_audit_enabled(discovery_config: dict[str, Any]) -> bool:
    return audit_enabled(discovery_config, "webSearch", flat_fallback=False)


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
    provider_rows, static_rows, failures = _load_web_search_audit_rows(
        orchestrator=orchestrator,
        deps=deps,
        stage_enabled=stage_enabled,
        cache=cache,
    )
    method = str(discovery_method)
    return (
        [row for row in provider_rows if str(row.get("discoveryMethod") or "") == method],
        [row for row in static_rows if str(row.get("discoveryMethod") or "") == method],
        [row for row in failures if str(row.get("adapter") or "") == method],
    )


def _seed_careers_scan_rows(
    *,
    orchestrator: Any,
    deps: DiscoveryRunDeps,
    stage_enabled: dict[str, bool],
    web_audit_enabled: bool,
    web_audit_cache: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    if web_audit_enabled:
        return _web_search_audit_rows_for_method(
            orchestrator=orchestrator,
            deps=deps,
            stage_enabled=stage_enabled,
            cache=web_audit_cache,
            discovery_method="seed_careers_page",
        )
    return orchestrator.discover_seed_careers_page_candidates(
        deps.timeout_s,
        studio_seeds=list(discovery_config_module.STUDIO_SEEDS),
        fetcher=deps.fetcher,
    )


def _web_search_scan_rows(
    *,
    orchestrator: Any,
    deps: DiscoveryRunDeps,
    stage_enabled: dict[str, bool],
    web_audit_enabled: bool,
    web_audit_cache: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    if web_audit_enabled:
        return _web_search_audit_rows_for_method(
            orchestrator=orchestrator,
            deps=deps,
            stage_enabled=stage_enabled,
            cache=web_audit_cache,
            discovery_method="web_search",
        )
    return orchestrator.discover_web_search_candidates(
        deps.timeout_s,
        studio_seeds=list(discovery_config_module.STUDIO_SEEDS),
        fetcher=deps.fetcher,
    )


def prepare_probe_inputs(*, deps: DiscoveryRunDeps, state: DiscoveryRunState) -> None:
    orchestrator = _require_root()
    from .directory_audit import clear_directory_audit_summaries, latest_directory_audit_summaries

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

    if deps.url_patch_manifest_enabled:
        state.url_patches = orchestrator.load_url_patches(deps.url_patch_manifest_path)
        state.url_patch_stats = summarize_url_patch_runtime(
            loaded=len(state.url_patches),
            added=0,
            updated=0,
            reprobed=0,
        )

    if stage_enabled["curatedSeed"]:
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

    if stage_enabled["sheetDirectory"]:
        state.write_progress_report(
            [],
            phase="scanning_sources",
            phase_label="Scanning game studios sheet directory",
            deps=deps,
            root=orchestrator,
        )
        orchestrator.emit_log("Scanning game studios sheet directory for candidate sources.")
        stage_started = time.perf_counter()
        sheet_cfg = deps.effective_config.get("sheetDirectory")
        sheet_cfg = sheet_cfg if isinstance(sheet_cfg, dict) else {}
        if audit_enabled(sheet_cfg):
            from .directory_audit import directory_audit_rows

            sheet_artifact, _sheet_cache_hit = orchestrator.run_sheet_directory_audit(
                deps.timeout_s,
                sheet_id=str(discovery_config_module.GAME_STUDIOS_SHEET_ID or "") or None,
                gid=str(discovery_config_module.GAME_STUDIOS_SHEET_GID or "") or None,
                config=deps.effective_config,
                fetcher=deps.fetcher,
            )
            provider_sheet_candidates, static_sheet_candidates, sheet_failures = (
                directory_audit_rows(sheet_artifact)
            )
        else:
            provider_sheet_candidates, static_sheet_candidates, sheet_failures = (
                orchestrator.discover_game_studio_sheet_candidates(
                    deps.timeout_s,
                    sheet_id=str(discovery_config_module.GAME_STUDIOS_SHEET_ID or "") or None,
                    gid=str(discovery_config_module.GAME_STUDIOS_SHEET_GID or "") or None,
                    fetcher=deps.fetcher,
                )
            )
        sheet_stage_rows = [*provider_sheet_candidates, *static_sheet_candidates]
        stage_duration_ms = _record_stage_timing(
            state.stage_timings_ms, "sheetDirectory", stage_started
        )
        _record_stage_runtime(
            state,
            rows=sheet_stage_rows,
            failure_rows=sheet_failures,
            stage_duration_ms=stage_duration_ms,
        )
        orchestrator.emit_log(
            "Game studios sheet scan complete: "
            f"provider={len(provider_sheet_candidates)}, static={len(static_sheet_candidates)}, failures={len(sheet_failures)}."
        )
        if sheet_failures:
            if deps.fetcher is orchestrator.fetch_text or (
                provider_sheet_candidates or static_sheet_candidates
            ):
                state.web_failures.extend(sheet_failures)
        state.streams.append(("sheet_directory", provider_sheet_candidates))
        state.streams.append(("sheet_directory", static_sheet_candidates))
    else:
        orchestrator.emit_log("Game studios sheet stage disabled, skipping.")

    if deps.mode == "dynamic":
        if stage_enabled["providerPatterns"]:
            state.write_progress_report(
                [],
                phase="generating_candidates",
                phase_label="Generating provider-pattern candidates",
                deps=deps,
                root=orchestrator,
            )
            orchestrator.emit_log(
                "Generating provider-pattern candidates from the studio seed catalog."
            )
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
                "Provider-pattern generation complete: "
                f"{len(provider_pattern_candidates)} candidate(s)."
            )
            state.streams.append(("provider_pattern", provider_pattern_candidates))
        else:
            orchestrator.emit_log("Provider-pattern stage disabled, skipping.")

        web_audit_enabled = _web_search_audit_enabled(deps.effective_config)
        web_audit_cache: dict[str, Any] = {}

        if stage_enabled["seedCareersScan"]:
            state.write_progress_report(
                [],
                phase="scanning_sources",
                phase_label="Scanning known careers pages",
                deps=deps,
                root=orchestrator,
            )
            orchestrator.emit_log("Scanning known careers pages from the seed catalog.")
            stage_started = time.perf_counter()
            provider_web_candidates, static_web_candidates, seed_failures = _seed_careers_scan_rows(
                orchestrator=orchestrator,
                deps=deps,
                stage_enabled=stage_enabled,
                web_audit_enabled=web_audit_enabled,
                web_audit_cache=web_audit_cache,
            )
            seed_stage_rows = [*provider_web_candidates, *static_web_candidates]
            stage_duration_ms = _record_stage_timing(
                state.stage_timings_ms, "seedCareersScan", stage_started
            )
            _record_stage_runtime(
                state,
                rows=seed_stage_rows,
                failure_rows=seed_failures,
                stage_duration_ms=stage_duration_ms,
            )
            orchestrator.emit_log(
                "Seed careers scan complete: "
                f"provider={len(provider_web_candidates)}, static={len(static_web_candidates)}, failures={len(seed_failures)}."
            )
            state.web_failures.extend(seed_failures)
            state.streams.append(("web_provider", provider_web_candidates))
            state.streams.append(("generic_static", static_web_candidates))
        else:
            orchestrator.emit_log("Seed careers stage disabled, skipping.")

        if stage_enabled["gamesmap"]:
            state.write_progress_report(
                [],
                phase="scanning_sources",
                phase_label="Scanning Gamesmap directory",
                deps=deps,
                root=orchestrator,
            )
            orchestrator.emit_log("Scanning Gamesmap directory for discoverable studios.")
            stage_started = time.perf_counter()
            provider_gamesmap_candidates, static_gamesmap_candidates, gamesmap_failures = (
                orchestrator.discover_gamesmap_candidates(
                    deps.timeout_s,
                    config=deps.effective_config,
                    fetcher=deps.fetcher,
                )
            )
            gamesmap_stage_rows = [*provider_gamesmap_candidates, *static_gamesmap_candidates]
            stage_duration_ms = _record_stage_timing(
                state.stage_timings_ms, "gamesmap", stage_started
            )
            _record_stage_runtime(
                state,
                rows=gamesmap_stage_rows,
                failure_rows=gamesmap_failures,
                stage_duration_ms=stage_duration_ms,
            )
            orchestrator.emit_log(
                "Gamesmap scan complete: "
                f"provider={len(provider_gamesmap_candidates)}, static={len(static_gamesmap_candidates)}, failures={len(gamesmap_failures)}."
            )
            state.web_failures.extend(gamesmap_failures)
            state.streams.append(("web_provider", provider_gamesmap_candidates))
            state.streams.append(("generic_static", static_gamesmap_candidates))
        else:
            orchestrator.emit_log("Gamesmap stage disabled, skipping.")

        if stage_enabled["gameprog"]:
            state.write_progress_report(
                [],
                phase="scanning_sources",
                phase_label="Scanning Gameprog directory",
                deps=deps,
                root=orchestrator,
            )
            orchestrator.emit_log("Scanning Gameprog directory for discoverable studios.")
            stage_started = time.perf_counter()
            gameprog_config = dict(deps.effective_config.get("gameprog") or {})
            config_with_gameprog = dict(deps.effective_config)
            config_with_gameprog["gameprog"] = gameprog_config
            provider_gameprog_candidates, static_gameprog_candidates, gameprog_failures = (
                orchestrator.discover_gameprog_candidates(
                    deps.timeout_s,
                    config=config_with_gameprog,
                    fetcher=deps.fetcher,
                )
            )
            gameprog_stage_rows = [*provider_gameprog_candidates, *static_gameprog_candidates]
            stage_duration_ms = _record_stage_timing(
                state.stage_timings_ms, "gameprog", stage_started
            )
            _record_stage_runtime(
                state,
                rows=gameprog_stage_rows,
                failure_rows=gameprog_failures,
                stage_duration_ms=stage_duration_ms,
            )
            orchestrator.emit_log(
                "Gameprog scan complete: "
                f"provider={len(provider_gameprog_candidates)}, static={len(static_gameprog_candidates)}, failures={len(gameprog_failures)}."
            )
            state.web_failures.extend(gameprog_failures)
            state.streams.append(("web_provider", provider_gameprog_candidates))
            state.streams.append(("generic_static", static_gameprog_candidates))
        else:
            orchestrator.emit_log("Gameprog stage disabled, skipping.")

        if stage_enabled["gamedevmap"]:
            state.write_progress_report(
                [],
                phase="scanning_sources",
                phase_label="Scanning GameDevMap directory",
                deps=deps,
                root=orchestrator,
            )
            orchestrator.emit_log("Scanning GameDevMap directory for discoverable studios.")
            stage_started = time.perf_counter()
            provider_gamedevmap_candidates, static_gamedevmap_candidates, gamedevmap_failures = (
                orchestrator.discover_gamedevmap_candidates(
                    deps.timeout_s,
                    config=deps.effective_config,
                    fetcher=deps.fetcher,
                )
            )
            from .gamedevmap_active_dry_run import latest_gamedevmap_audit_report_summary

            gamedevmap_cfg = deps.effective_config.get("gamedevmap")
            gamedevmap_cfg = gamedevmap_cfg if isinstance(gamedevmap_cfg, dict) else {}
            if audit_enabled(gamedevmap_cfg):
                state.gamedevmap_audit_summary = latest_gamedevmap_audit_report_summary()
            gamedevmap_stage_rows = [
                *provider_gamedevmap_candidates,
                *static_gamedevmap_candidates,
            ]
            stage_duration_ms = _record_stage_timing(
                state.stage_timings_ms, "gamedevmap", stage_started
            )
            _record_stage_runtime(
                state,
                rows=gamedevmap_stage_rows,
                failure_rows=gamedevmap_failures,
                stage_duration_ms=stage_duration_ms,
            )
            orchestrator.emit_log(
                "GameDevMap scan complete: "
                f"provider={len(provider_gamedevmap_candidates)}, static={len(static_gamedevmap_candidates)}, failures={len(gamedevmap_failures)}."
            )
            state.web_failures.extend(gamedevmap_failures)
            state.streams.append(("web_provider", provider_gamedevmap_candidates))
            state.streams.append(("generic_static", static_gamedevmap_candidates))
        else:
            orchestrator.emit_log("GameDevMap stage disabled, skipping.")

        if deps.include_web_search and stage_enabled["webSearch"]:
            state.write_progress_report(
                [],
                phase="generating_candidates",
                phase_label="Running web-search discovery queries",
                deps=deps,
                root=orchestrator,
            )
            orchestrator.emit_log("Running web-search discovery queries.")
            stage_started = time.perf_counter()
            provider_search_candidates, static_search_candidates, search_failures = (
                _web_search_scan_rows(
                    orchestrator=orchestrator,
                    deps=deps,
                    stage_enabled=stage_enabled,
                    web_audit_enabled=web_audit_enabled,
                    web_audit_cache=web_audit_cache,
                )
            )
            search_stage_rows = [*provider_search_candidates, *static_search_candidates]
            stage_duration_ms = _record_stage_timing(
                state.stage_timings_ms, "webSearch", stage_started
            )
            _record_stage_runtime(
                state,
                rows=search_stage_rows,
                failure_rows=search_failures,
                stage_duration_ms=stage_duration_ms,
            )
            orchestrator.emit_log(
                "Web-search discovery complete: "
                f"provider={len(provider_search_candidates)}, static={len(static_search_candidates)}, failures={len(search_failures)}."
            )
            state.web_failures.extend(search_failures)
            state.streams.append(("web_provider", provider_search_candidates))
            state.streams.append(("generic_static", static_search_candidates))
        elif deps.include_web_search:
            orchestrator.emit_log("Web-search stage disabled, skipping.")

    state.directory_audit_summaries = latest_directory_audit_summaries()

    stage_started = time.perf_counter()
    discovered = orchestrator.merge_candidate_streams(state.streams)
    for row in discovered:
        state.generated_count_by_stage[str(row.get("discoveryStage") or "provider_pattern")] += 1
    state.found_endpoint_count = len(discovered)
    orchestrator.emit_log(
        "Generated candidates by stage: "
        + ", ".join(
            f"{stage}={state.generated_count_by_stage.get(stage, 0)}" for stage in DISCOVERY_STAGES
        )
        + f" (total={state.found_endpoint_count})."
    )

    local_seen_ids = set(seen_ids)
    local_seen_domains = set(seen_domains)
    for row in discovered:
        stage = str(row.get("discoveryStage") or "provider_pattern")
        row_id = source_identity(row)
        row_domain = adapter_domain_fingerprint(row)
        if row_id in seen_ids:
            state.skipped_duplicate_count += 1
            state.duplicate_reasons["existing_id"] += 1
            state.dedupe_drop_rows.append(
                {
                    "name": row.get("name"),
                    "adapter": row.get("adapter"),
                    "stage": "dedupe_skipped",
                    "error": "existing_id",
                    "dropStage": "dedupe_skipped",
                    "dropReason": "existing_id",
                }
            )
            continue
        if row_domain and row_domain in seen_domains:
            state.skipped_duplicate_count += 1
            state.duplicate_reasons["existing_domain"] += 1
            state.dedupe_drop_rows.append(
                {
                    "name": row.get("name"),
                    "adapter": row.get("adapter"),
                    "stage": "dedupe_skipped",
                    "error": "existing_domain",
                    "dropStage": "dedupe_skipped",
                    "dropReason": "existing_domain",
                }
            )
            continue
        if row_id in local_seen_ids:
            state.skipped_duplicate_count += 1
            state.duplicate_reasons["run_id"] += 1
            state.dedupe_drop_rows.append(
                {
                    "name": row.get("name"),
                    "adapter": row.get("adapter"),
                    "stage": "dedupe_skipped",
                    "error": "run_id",
                    "dropStage": "dedupe_skipped",
                    "dropReason": "run_id",
                }
            )
            continue
        if row_domain and row_domain in local_seen_domains:
            state.skipped_duplicate_count += 1
            state.duplicate_reasons["run_domain"] += 1
            state.dedupe_drop_rows.append(
                {
                    "name": row.get("name"),
                    "adapter": row.get("adapter"),
                    "stage": "dedupe_skipped",
                    "error": "run_domain",
                    "dropStage": "dedupe_skipped",
                    "dropReason": "run_domain",
                }
            )
            continue
        local_seen_ids.add(row_id)
        if row_domain:
            local_seen_domains.add(row_domain)
        state.survived_dedupe_count_by_stage[stage] += 1
        state.filtered.append(row)
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

    for raw in sheet_static_suppressed:
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
        if str(raw.get("adapter") or "").strip().lower() == "static":
            blocked_url = str(
                raw.get("listing_url") or raw.get("careersUrl") or endpoint_url(raw) or ""
            ).strip()
            if blocked_url and is_blocked_generic_static_url(blocked_url):
                state.suppressed_static_count += 1
                state.suppressed_static_by_reason["blocked_domain"] += 1
                state.suppressed_static_by_stage[stage] += 1
                state.failures.append(
                    {
                        "name": raw.get("name"),
                        "adapter": raw.get("adapter"),
                        "domain": (urlparse(blocked_url).netloc or "").lower(),
                        "error": "blocked_domain",
                        "stage": "suppressed_static",
                        "dropStage": "suppressed_static",
                        "dropReason": "blocked_domain",
                    }
                )
                continue
        suppression_reason = classify_static_suppression(
            raw,
            source_state_rows=state.source_state_rows,
            thresholds=deps.thresholds,
        )
        if suppression_reason:
            state.suppressed_static_count += 1
            state.suppressed_static_by_reason[suppression_reason] += 1
            state.suppressed_static_by_stage[stage] += 1
            state.failures.append(
                {
                    "name": raw.get("name"),
                    "adapter": raw.get("adapter"),
                    "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(),
                    "error": suppression_reason,
                    "stage": "suppressed_static",
                    "dropStage": "suppressed_static",
                    "dropReason": suppression_reason,
                }
            )
            continue
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
            continue
        routed, low_evidence_probes_used = _route_valid_probe_candidate(
            raw,
            deps=deps,
            state=state,
            stage=stage,
            low_evidence_probes_used=low_evidence_probes_used,
        )
        if routed:
            continue
        state.probe_inputs.append(raw)
