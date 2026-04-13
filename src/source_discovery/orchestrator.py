from __future__ import annotations

"""End-to-end discovery orchestration (CLI + core flow).

Phases:
1. Candidate generation (curated seeds, sheet directory, provider patterns, Gamesmap, web search)
2. De-duplication across runs (IDs + endpoint fingerprints)
3. Probe (HTTP checks) with concurrency limits
4. Queue balancing (caps by adapter/domain, top-N)
5. Summary + report writing
"""

import argparse
import asyncio
import os
import time
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from src import source_registry as source_registry_module
from src.bridge.registry_tombstones import filter_tombstoned_rows, load_tombstones
from src.contracts import SCHEMA_VERSION
from src.jobs.browser_fallback import BrowserFallbackCircuitBreaker
from src.jobs.state import read_source_state
from src.shared.utils import now_iso
from src.source_registry import (
    APPROVAL_STATE_PATH as DEFAULT_APPROVAL_STATE_PATH,
)
from src.source_registry import (
    URL_PATCH_MANIFEST_PATH as DEFAULT_URL_PATCH_MANIFEST_PATH,
)
from src.source_registry import (
    apply_discovery_auto_approval,
    load_json_array,
    save_json_atomic,
    source_identity,
    unique_sources,
)

from . import config as discovery_config_module
from .bootstrap import discovery_report_write_path, prime_bridge_discovery_report
from .config import (
    ADAPTER_QUEUE_CAPS,
    DISCOVERY_STAGES,
    DOMAIN_QUEUE_CAP_DEFAULT,
    LOW_EVIDENCE_PROBE_LIMIT,
    UNCAPPED_DISCOVERY_ADAPTER_QUEUE_CAPS,
    UNCAPPED_DISCOVERY_DOMAIN_QUEUE_CAP,
    load_discovery_config,
)
from .core import (
    _evidence_threshold_for_probe,
    adapter_domain_fingerprint,
    apply_queue_balancing,
    apply_sheet_directory_static_probe_cap,
    classify_probe_failure_stage,
    classify_static_suppression,
    compute_candidate_rank,
    compute_candidate_score,
    estimate_probe_priority,
    init_stage_counter,
    normalize_candidate,
    probe_bucket_for,
    probe_concurrency_defaults,
    should_queue_candidate,
)
from .gamedevmap import discover_gamedevmap_candidates
from .gameprog import discover_gameprog_candidates
from .gamesmap import discover_gamesmap_candidates
from .io_runtime import endpoint_url
from .probe import async_probe_candidate, validate_candidate_for_probe
from .provider_patterns import build_pattern_candidates
from .reporting import (
    build_discovery_task_progress,
    build_m5_strategic_backlog,
    build_stage_summary,
    emit_log,
    merge_candidate_streams,
    stage_curated_seed_candidates,
    write_discovery_progress_report,
)
from .reporting import (
    update_candidate_review_metadata as _update_candidate_review_metadata,
)
from .runtime_metrics import (
    DISCOVERY_TIMING_STAGE_KEYS,
)
from .runtime_metrics import (
    adjust_adapter_runtime as _adjust_adapter_runtime,
)
from .runtime_metrics import (
    build_discovery_runtime_payload as _build_discovery_runtime_payload,
)
from .runtime_metrics import (
    distribute_duration_by_adapter as _distribute_duration_by_adapter,
)
from .runtime_metrics import (
    increment_adapter_runtime as _increment_adapter_runtime,
)
from .runtime_metrics import (
    record_stage_timing as _record_stage_timing,
)
from .schemas import DiscoveryReportSchema
from .scoring import resolve_discovery_thresholds, unique_string_list
from .sheet_directory import discover_game_studio_sheet_candidates
from .stage_control import (
    apply_discovery_cli_args_to_config as _apply_discovery_cli_args_to_config,
)
from .stage_control import (
    discovery_stage_enabled as _discovery_stage_enabled,
)
from .url_patches import (
    apply_url_patches_to_candidate,
    load_url_patches,
    merge_url_patches,
    resolve_patch_target,
    save_url_patch_manifest,
    should_attempt_patch_recovery,
    summarize_url_patch_runtime,
)
from .web_search import (
    async_fetch_text_httpx,
    discover_seed_careers_page_candidates,
    discover_web_search_candidates,
    fetch_text,
    is_blocked_generic_static_url,
)


def _discovery_report_write_path() -> Path:
    return discovery_report_write_path()


def _prime_bridge_discovery_report(*, run_id: str, started_at: str, mode: str) -> None:
    prime_bridge_discovery_report(
        run_id=run_id,
        started_at=started_at,
        mode=mode,
        save_json_atomic=save_json_atomic,
        now_iso=now_iso,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover new job source candidates.")
    parser.add_argument("--timeout", type=int, default=12)
    parser.add_argument(
        "--top", type=int, default=0, help="Limit new candidates written this run; 0 = no limit."
    )
    parser.add_argument("--preset", choices=("default", "uncapped"), default="default")
    parser.add_argument("--mode", choices=("dynamic", "static"), default="dynamic")
    parser.add_argument(
        "--no-web-search", action="store_true", help="Disable lightweight web search phase."
    )
    parser.add_argument(
        "--gamesmap-website-only-fallback",
        action="store_true",
        help="Manual-only mode: include Gamesmap homepage-only candidates in this run.",
    )
    parser.add_argument(
        "--gamesmap-max-detail-pages",
        type=int,
        default=0,
        help="Optional Gamesmap crawl cap override for this run; 0 = config default.",
    )
    parser.add_argument(
        "--gamedevmap-enabled",
        action="store_true",
        help="Enable GameDevMap directory scanning.",
    )
    parser.add_argument(
        "--gamedevmap-max-rows",
        type=int,
        default=0,
        help="Optional GameDevMap representative row cap override for this run; 0 = config default.",
    )
    parser.add_argument(
        "--gamedevmap-max-homepage-fetches",
        type=int,
        default=0,
        help="Optional GameDevMap homepage fetch cap override for this run; 0 = config default.",
    )
    parser.add_argument(
        "--only-gamedevmap",
        action="store_true",
        help="Run only the GameDevMap discovery stage and skip other candidate-generation stages.",
    )
    parser.add_argument(
        "--gameprog-enabled",
        action="store_true",
        help="Enable Gameprog directory scanning.",
    )
    parser.add_argument(
        "--gameprog-max-studios",
        type=int,
        default=0,
        help="Optional Gameprog studio cap override for this run; 0 = config default.",
    )
    parser.add_argument(
        "--gameprog-website-only-fallback",
        action="store_true",
        help="Include Gameprog website-only candidates.",
    )
    return parser.parse_args(argv)


def run_discovery(
    *,
    timeout_s: int,
    top_n: int,
    preset: str = "default",
    mode: str = "dynamic",
    include_web_search: bool = True,
    discovery_config: dict[str, Any] | None = None,
    run_id: str = "",
    started_at_override: str = "",
    fetcher=fetch_text,
    cli_args: argparse.Namespace | None = None,
) -> dict[str, Any]:
    started_at = str(started_at_override or now_iso()).strip()
    run_id = str(run_id or "").strip()
    run_started_mono = time.perf_counter()
    emit_log(
        f"Discovery worker run_discovery() begin runId={run_id!r} "
        f"report_path={discovery_report_write_path()!s}"
    )
    _prime_bridge_discovery_report(
        run_id=run_id, started_at=started_at, mode=str(mode or "dynamic")
    )
    effective_config = (
        discovery_config if isinstance(discovery_config, dict) else load_discovery_config()
    )
    if cli_args is not None:
        effective_config = _apply_discovery_cli_args_to_config(effective_config, cli_args)
    thresholds = resolve_discovery_thresholds(effective_config)
    stage_enabled = {
        "curatedSeed": _discovery_stage_enabled(effective_config, "curatedSeed"),
        "sheetDirectory": _discovery_stage_enabled(effective_config, "sheetDirectory"),
        "providerPatterns": _discovery_stage_enabled(effective_config, "providerPatterns"),
        "seedCareersScan": _discovery_stage_enabled(effective_config, "seedCareersScan"),
        "gamesmap": _discovery_stage_enabled(effective_config, "gamesmap"),
        "gameprog": _discovery_stage_enabled(effective_config, "gameprog"),
        "gamedevmap": _discovery_stage_enabled(effective_config, "gamedevmap"),
        "webSearch": _discovery_stage_enabled(effective_config, "webSearch"),
    }
    stage_timings_ms: dict[str, int] = {key: 0 for key in DISCOVERY_TIMING_STAGE_KEYS}
    adapter_runtime: dict[str, dict[str, int | str]] = {}

    preset_name = str(preset or "default").strip().lower() or "default"
    if preset_name not in {"default", "uncapped"}:
        preset_name = "default"
    top_cap_bypassed = int(top_n or 0) <= 0
    sheet_static_probe_cap_bypassed = preset_name in {"default", "uncapped"}
    queue_domain_cap = DOMAIN_QUEUE_CAP_DEFAULT
    queue_adapter_caps = ADAPTER_QUEUE_CAPS
    if preset_name == "uncapped":
        queue_domain_cap = UNCAPPED_DISCOVERY_DOMAIN_QUEUE_CAP
        queue_adapter_caps = UNCAPPED_DISCOVERY_ADAPTER_QUEUE_CAPS

    web_failures: list[dict[str, Any]] = []
    streams: list[tuple[str, list[dict[str, Any]]]] = []
    generated_count_by_stage = init_stage_counter()
    survived_dedupe_count_by_stage = init_stage_counter()
    probed_count_by_stage = init_stage_counter()
    queued_count_by_stage = init_stage_counter()
    duplicate_reasons: Counter[str] = Counter()
    dedupe_drop_rows: list[dict[str, Any]] = []
    found_endpoint_count = 0
    skipped_duplicate_count = 0
    filtered: list[dict[str, Any]] = []
    queueable_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    healthy = 0
    probed = 0
    adapter_counter: Counter[str] = Counter()
    method_counter: Counter[str] = Counter()
    skipped_invalid = 0
    skipped_low_evidence_probe_count = 0
    suppressed_static_count = 0
    suppressed_static_by_reason: Counter[str] = Counter()
    suppressed_static_by_stage: Counter[str] = Counter()
    validation_skipped_count = 0
    queue_filtered_count = 0
    probe_failed_count = 0
    url_patch_manifest_path = source_registry_module.URL_PATCH_MANIFEST_PATH
    url_patch_manifest_enabled = bool(url_patch_manifest_path) and (
        fetcher is fetch_text
        or str(url_patch_manifest_path) != str(DEFAULT_URL_PATCH_MANIFEST_PATH)
    )
    url_patches: dict[str, Any] = {}
    url_patch_stats = summarize_url_patch_runtime(loaded=0, added=0, updated=0, reprobed=0)

    def write_progress_report(
        current_candidates: list[dict[str, Any]], *, phase: str, phase_label: str
    ) -> None:
        report_write_path = discovery_report_write_path()
        write_discovery_progress_report(
            current_candidates=current_candidates,
            phase=phase,
            phase_label=phase_label,
            total_duration_ms=max(0, int((time.perf_counter() - run_started_mono) * 1000)),
            stage_timings_ms=stage_timings_ms,
            adapter_runtime=adapter_runtime,
            preset_name=preset_name,
            top_cap_bypassed=top_cap_bypassed,
            sheet_static_probe_cap_bypassed=sheet_static_probe_cap_bypassed,
            url_patch_stats=dict(url_patch_stats),
            found_endpoint_count=found_endpoint_count,
            generated_count_by_stage=generated_count_by_stage,
            survived_dedupe_count_by_stage=survived_dedupe_count_by_stage,
            probed_count_by_stage=probed_count_by_stage,
            queued_count_by_stage=queued_count_by_stage,
            probed=probed,
            healthy=healthy,
            failures=failures,
            skipped_duplicate_count=skipped_duplicate_count,
            skipped_invalid=skipped_invalid,
            skipped_low_evidence_probe_count=skipped_low_evidence_probe_count,
            validation_skipped_count=validation_skipped_count,
            probe_failed_count=probe_failed_count,
            queue_filtered_count=queue_filtered_count,
            adapter_counter=adapter_counter,
            method_counter=method_counter,
            duplicate_reasons=duplicate_reasons,
            suppressed_static_count=suppressed_static_count,
            suppressed_static_by_reason=dict(suppressed_static_by_reason),
            suppressed_static_by_stage=dict(suppressed_static_by_stage),
            thresholds=thresholds,
            run_id=run_id,
            mode=mode,
            started_at=started_at,
            report_write_path=report_write_path,
            outputs={
                "report": str(report_write_path),
                "candidates": str(source_registry_module.DISCOVERY_CANDIDATES_PATH),
                "pending": str(source_registry_module.PENDING_PATH),
                "urlPatches": str(source_registry_module.URL_PATCH_MANIFEST_PATH),
            },
            save_json_atomic_fn=save_json_atomic,
        )

    write_progress_report([], phase="starting", phase_label="Initializing scan")

    write_progress_report(
        [], phase="generating_candidates", phase_label="Generating seed candidates"
    )

    active = load_json_array(source_registry_module.ACTIVE_PATH, [])
    pending_existing = load_json_array(source_registry_module.PENDING_PATH, [])
    rejected = load_json_array(source_registry_module.REJECTED_PATH, [])
    tombstones = load_tombstones()
    active = filter_tombstoned_rows(active, tombstones)
    pending_existing = filter_tombstoned_rows(pending_existing, tombstones)
    rejected = filter_tombstoned_rows(rejected, tombstones)
    prior_review_candidates = load_json_array(source_registry_module.DISCOVERY_CANDIDATES_PATH, [])
    prior_review_candidates_by_id = {
        source_identity(row): dict(row) for row in prior_review_candidates if isinstance(row, dict)
    }
    ranking_registry_rows = [
        *[dict(row) for row in active if isinstance(row, dict)],
        *[dict(row) for row in pending_existing if isinstance(row, dict)],
    ]

    emit_log(
        f"Starting source discovery: mode={mode}, preset={preset_name}, top_n={top_n}, web_search={'on' if include_web_search else 'off'}."
    )
    emit_log(
        f"Loaded registries: active={len(active)}, pending={len(pending_existing)}, rejected={len(rejected)}."
    )

    # Pending rows are intentionally excluded here so discovery can refresh stale
    # pending candidates with newer probe evidence and job counts.
    existing_rows = [*active, *rejected]
    seen_ids = {source_identity(row) for row in existing_rows if isinstance(row, dict)}
    seen_domains = {
        fp
        for fp in (
            adapter_domain_fingerprint(row) for row in existing_rows if isinstance(row, dict)
        )
        if fp
    }

    if url_patch_manifest_enabled:
        url_patches = load_url_patches(url_patch_manifest_path)
        url_patch_stats = summarize_url_patch_runtime(
            loaded=len(url_patches),
            added=0,
            updated=0,
            reprobed=0,
        )

    if stage_enabled["curatedSeed"]:
        emit_log("Generating curated seed candidates from static discovery inputs.")
        stage_started = time.perf_counter()
        curated_seed_candidates = stage_curated_seed_candidates()
        stage_duration_ms = _record_stage_timing(stage_timings_ms, "curatedSeed", stage_started)
        _distribute_duration_by_adapter(
            adapter_runtime, duration_ms=stage_duration_ms, rows=curated_seed_candidates
        )
        for row in curated_seed_candidates:
            _increment_adapter_runtime(adapter_runtime, row.get("adapter"), generated=1)
        emit_log(f"Curated seed generation complete: {len(curated_seed_candidates)} candidate(s).")
        streams.append(("curated_seed", curated_seed_candidates))
    else:
        emit_log("Curated seed stage disabled, skipping.")

    if stage_enabled["sheetDirectory"]:
        write_progress_report(
            [], phase="scanning_sources", phase_label="Scanning game studios sheet directory"
        )
        emit_log("Scanning game studios sheet directory for candidate sources.")
        stage_started = time.perf_counter()
        provider_sheet_candidates, static_sheet_candidates, sheet_failures = (
            discover_game_studio_sheet_candidates(
                timeout_s,
                sheet_id=str(discovery_config_module.GAME_STUDIOS_SHEET_ID or "") or None,
                gid=str(discovery_config_module.GAME_STUDIOS_SHEET_GID or "") or None,
                fetcher=fetcher,
            )
        )
        sheet_stage_rows = [*provider_sheet_candidates, *static_sheet_candidates]
        stage_duration_ms = _record_stage_timing(stage_timings_ms, "sheetDirectory", stage_started)
        _distribute_duration_by_adapter(
            adapter_runtime,
            duration_ms=stage_duration_ms,
            rows=sheet_stage_rows,
            failure_rows=sheet_failures,
        )
        for row in sheet_stage_rows:
            _increment_adapter_runtime(adapter_runtime, row.get("adapter"), generated=1)
        for row in sheet_failures:
            if isinstance(row, dict):
                _increment_adapter_runtime(adapter_runtime, row.get("adapter"), failures=1)
        emit_log(
            "Game studios sheet scan complete: "
            f"provider={len(provider_sheet_candidates)}, static={len(static_sheet_candidates)}, failures={len(sheet_failures)}."
        )
        # In unit tests we often supply a fetcher that only knows about the URLs
        # relevant to that test. Treat an empty sheet result as a soft failure in
        # that case so tests don't have to stub out the sheet directory URLs.
        if sheet_failures:
            if fetcher is fetch_text or (provider_sheet_candidates or static_sheet_candidates):
                web_failures.extend(sheet_failures)
        streams.append(("sheet_directory", provider_sheet_candidates))
        streams.append(("sheet_directory", static_sheet_candidates))
    else:
        emit_log("Game studios sheet stage disabled, skipping.")

    if mode == "dynamic":
        if stage_enabled["providerPatterns"]:
            write_progress_report(
                [],
                phase="generating_candidates",
                phase_label="Generating provider-pattern candidates",
            )
            emit_log("Generating provider-pattern candidates from the studio seed catalog.")
            stage_started = time.perf_counter()
            provider_pattern_candidates = build_pattern_candidates(
                list(discovery_config_module.STUDIO_SEEDS)
            )
            stage_duration_ms = _record_stage_timing(
                stage_timings_ms, "providerPatterns", stage_started
            )
            _distribute_duration_by_adapter(
                adapter_runtime, duration_ms=stage_duration_ms, rows=provider_pattern_candidates
            )
            for row in provider_pattern_candidates:
                _increment_adapter_runtime(adapter_runtime, row.get("adapter"), generated=1)
            emit_log(
                f"Provider-pattern generation complete: {len(provider_pattern_candidates)} candidate(s)."
            )
            streams.append(("provider_pattern", provider_pattern_candidates))
        else:
            emit_log("Provider-pattern stage disabled, skipping.")

        if stage_enabled["seedCareersScan"]:
            write_progress_report(
                [], phase="scanning_sources", phase_label="Scanning known careers pages"
            )
            emit_log("Scanning known careers pages from the seed catalog.")
            stage_started = time.perf_counter()
            provider_web_candidates, static_web_candidates, seed_failures = (
                discover_seed_careers_page_candidates(
                    timeout_s,
                    studio_seeds=list(discovery_config_module.STUDIO_SEEDS),
                    fetcher=fetcher,
                )
            )
            seed_stage_rows = [*provider_web_candidates, *static_web_candidates]
            stage_duration_ms = _record_stage_timing(
                stage_timings_ms, "seedCareersScan", stage_started
            )
            _distribute_duration_by_adapter(
                adapter_runtime,
                duration_ms=stage_duration_ms,
                rows=seed_stage_rows,
                failure_rows=seed_failures,
            )
            for row in seed_stage_rows:
                _increment_adapter_runtime(adapter_runtime, row.get("adapter"), generated=1)
            for row in seed_failures:
                if isinstance(row, dict):
                    _increment_adapter_runtime(adapter_runtime, row.get("adapter"), failures=1)
            emit_log(
                "Seed careers scan complete: "
                f"provider={len(provider_web_candidates)}, static={len(static_web_candidates)}, failures={len(seed_failures)}."
            )
            web_failures.extend(seed_failures)
            streams.append(("web_provider", provider_web_candidates))
            streams.append(("generic_static", static_web_candidates))
        else:
            emit_log("Seed careers stage disabled, skipping.")

        if stage_enabled["gamesmap"]:
            write_progress_report(
                [], phase="scanning_sources", phase_label="Scanning Gamesmap directory"
            )
            emit_log("Scanning Gamesmap directory for discoverable studios.")
            stage_started = time.perf_counter()
            provider_gamesmap_candidates, static_gamesmap_candidates, gamesmap_failures = (
                discover_gamesmap_candidates(
                    timeout_s,
                    config=effective_config,
                    fetcher=fetcher,
                )
            )
            gamesmap_stage_rows = [*provider_gamesmap_candidates, *static_gamesmap_candidates]
            stage_duration_ms = _record_stage_timing(stage_timings_ms, "gamesmap", stage_started)
            _distribute_duration_by_adapter(
                adapter_runtime,
                duration_ms=stage_duration_ms,
                rows=gamesmap_stage_rows,
                failure_rows=gamesmap_failures,
            )
            for row in gamesmap_stage_rows:
                _increment_adapter_runtime(adapter_runtime, row.get("adapter"), generated=1)
            for row in gamesmap_failures:
                if isinstance(row, dict):
                    _increment_adapter_runtime(adapter_runtime, row.get("adapter"), failures=1)
            emit_log(
                "Gamesmap scan complete: "
                f"provider={len(provider_gamesmap_candidates)}, static={len(static_gamesmap_candidates)}, failures={len(gamesmap_failures)}."
            )
            web_failures.extend(gamesmap_failures)
            streams.append(("web_provider", provider_gamesmap_candidates))
            streams.append(("generic_static", static_gamesmap_candidates))
        else:
            emit_log("Gamesmap stage disabled, skipping.")

        if stage_enabled["gameprog"]:
            write_progress_report(
                [], phase="scanning_sources", phase_label="Scanning Gameprog directory"
            )
            emit_log("Scanning Gameprog directory for discoverable studios.")
            stage_started = time.perf_counter()
            gameprog_config = dict(effective_config.get("gameprog") or {})
            config_with_gameprog = dict(effective_config)
            config_with_gameprog["gameprog"] = gameprog_config
            provider_gameprog_candidates, static_gameprog_candidates, gameprog_failures = (
                discover_gameprog_candidates(
                    timeout_s,
                    config=config_with_gameprog,
                    fetcher=fetcher,
                )
            )
            gameprog_stage_rows = [*provider_gameprog_candidates, *static_gameprog_candidates]
            stage_duration_ms = _record_stage_timing(stage_timings_ms, "gameprog", stage_started)
            _distribute_duration_by_adapter(
                adapter_runtime,
                duration_ms=stage_duration_ms,
                rows=gameprog_stage_rows,
                failure_rows=gameprog_failures,
            )
            for row in gameprog_stage_rows:
                _increment_adapter_runtime(adapter_runtime, row.get("adapter"), generated=1)
            for row in gameprog_failures:
                if isinstance(row, dict):
                    _increment_adapter_runtime(adapter_runtime, row.get("adapter"), failures=1)
            emit_log(
                "Gameprog scan complete: "
                f"provider={len(provider_gameprog_candidates)}, static={len(static_gameprog_candidates)}, failures={len(gameprog_failures)}."
            )
            web_failures.extend(gameprog_failures)
            streams.append(("web_provider", provider_gameprog_candidates))
            streams.append(("generic_static", static_gameprog_candidates))
        else:
            emit_log("Gameprog stage disabled, skipping.")

        if stage_enabled["gamedevmap"]:
            write_progress_report(
                [], phase="scanning_sources", phase_label="Scanning GameDevMap directory"
            )
            emit_log("Scanning GameDevMap directory for discoverable studios.")
            stage_started = time.perf_counter()
            provider_gamedevmap_candidates, static_gamedevmap_candidates, gamedevmap_failures = (
                discover_gamedevmap_candidates(
                    timeout_s,
                    config=effective_config,
                    fetcher=fetcher,
                )
            )
            gamedevmap_stage_rows = [*provider_gamedevmap_candidates, *static_gamedevmap_candidates]
            stage_duration_ms = _record_stage_timing(stage_timings_ms, "gamedevmap", stage_started)
            _distribute_duration_by_adapter(
                adapter_runtime,
                duration_ms=stage_duration_ms,
                rows=gamedevmap_stage_rows,
                failure_rows=gamedevmap_failures,
            )
            for row in gamedevmap_stage_rows:
                _increment_adapter_runtime(adapter_runtime, row.get("adapter"), generated=1)
            for row in gamedevmap_failures:
                if isinstance(row, dict):
                    _increment_adapter_runtime(adapter_runtime, row.get("adapter"), failures=1)
            emit_log(
                "GameDevMap scan complete: "
                f"provider={len(provider_gamedevmap_candidates)}, static={len(static_gamedevmap_candidates)}, failures={len(gamedevmap_failures)}."
            )
            web_failures.extend(gamedevmap_failures)
            streams.append(("web_provider", provider_gamedevmap_candidates))
            streams.append(("generic_static", static_gamedevmap_candidates))
        else:
            emit_log("GameDevMap stage disabled, skipping.")

        if include_web_search and stage_enabled["webSearch"]:
            write_progress_report(
                [],
                phase="generating_candidates",
                phase_label="Running web-search discovery queries",
            )
            emit_log("Running web-search discovery queries.")
            stage_started = time.perf_counter()
            provider_search_candidates, static_search_candidates, search_failures = (
                discover_web_search_candidates(
                    timeout_s,
                    studio_seeds=list(discovery_config_module.STUDIO_SEEDS),
                    fetcher=fetcher,
                )
            )
            search_stage_rows = [*provider_search_candidates, *static_search_candidates]
            stage_duration_ms = _record_stage_timing(stage_timings_ms, "webSearch", stage_started)
            _distribute_duration_by_adapter(
                adapter_runtime,
                duration_ms=stage_duration_ms,
                rows=search_stage_rows,
                failure_rows=search_failures,
            )
            for row in search_stage_rows:
                _increment_adapter_runtime(adapter_runtime, row.get("adapter"), generated=1)
            for row in search_failures:
                if isinstance(row, dict):
                    _increment_adapter_runtime(adapter_runtime, row.get("adapter"), failures=1)
            emit_log(
                "Web-search discovery complete: "
                f"provider={len(provider_search_candidates)}, static={len(static_search_candidates)}, failures={len(search_failures)}."
            )
            web_failures.extend(search_failures)
            streams.append(("web_provider", provider_search_candidates))
            streams.append(("generic_static", static_search_candidates))
        elif include_web_search:
            emit_log("Web-search stage disabled, skipping.")

    stage_started = time.perf_counter()
    discovered = merge_candidate_streams(streams)
    for row in discovered:
        generated_count_by_stage[str(row.get("discoveryStage") or "provider_pattern")] += 1
    found_endpoint_count = len(discovered)
    emit_log(
        "Generated candidates by stage: "
        + ", ".join(
            f"{stage}={generated_count_by_stage.get(stage, 0)}" for stage in DISCOVERY_STAGES
        )
        + f" (total={found_endpoint_count})."
    )

    filtered: list[dict[str, Any]] = []
    skipped_duplicate_count = 0
    local_seen_ids = set(seen_ids)
    local_seen_domains = set(seen_domains)
    for row in discovered:
        stage = str(row.get("discoveryStage") or "provider_pattern")
        row_id = source_identity(row)
        row_domain = adapter_domain_fingerprint(row)
        if row_id in seen_ids:
            skipped_duplicate_count += 1
            duplicate_reasons["existing_id"] += 1
            dedupe_drop_rows.append(
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
            skipped_duplicate_count += 1
            duplicate_reasons["existing_domain"] += 1
            dedupe_drop_rows.append(
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
            skipped_duplicate_count += 1
            duplicate_reasons["run_id"] += 1
            dedupe_drop_rows.append(
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
            skipped_duplicate_count += 1
            duplicate_reasons["run_domain"] += 1
            dedupe_drop_rows.append(
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
        survived_dedupe_count_by_stage[stage] += 1
        filtered.append(row)
    _record_stage_timing(stage_timings_ms, "dedupeFilter", stage_started)

    filtered.sort(key=estimate_probe_priority, reverse=True)
    source_state_rows = read_source_state(
        source_registry_module.ACTIVE_PATH.parent / "jobs-source-state.json"
    )
    filtered, sheet_static_suppressed = apply_sheet_directory_static_probe_cap(
        filtered,
        top_n=top_n,
        bypass_cap=sheet_static_probe_cap_bypassed,
        source_state_rows=source_state_rows,
    )
    emit_log(
        "After dedupe: "
        + ", ".join(
            f"{stage}={survived_dedupe_count_by_stage.get(stage, 0)}" for stage in DISCOVERY_STAGES
        )
        + f"; skipped_duplicates={skipped_duplicate_count}."
    )

    failures = [
        {**row, "dropStage": "page_fetch", "dropReason": "page_fetch"}
        for row in list(web_failures)
        if isinstance(row, dict)
    ]
    failures.extend(dedupe_drop_rows)

    for raw in sheet_static_suppressed:
        stage = str(raw.get("discoveryStage") or "provider_pattern")
        suppressed_static_count += 1
        suppressed_static_by_reason["sheet_directory_stage_cap"] += 1
        suppressed_static_by_stage[stage] += 1
        failures.append(
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

    write_progress_report(
        [], phase="generating_candidates", phase_label="Generating initial discovery candidates"
    )
    emit_log(f"Starting probe phase for {len(filtered)} candidate(s).")
    write_progress_report(
        queueable_candidates,
        phase="probing_candidates",
        phase_label=f"Probing {len(filtered)} candidate(s)",
    )

    probe_inputs: list[dict[str, Any]] = []
    failed_probe_records: list[dict[str, Any]] = []
    for raw in filtered:
        raw, _patch_applied = apply_url_patches_to_candidate(raw, url_patches)
        stage = str(raw.get("discoveryStage") or "provider_pattern")
        if str(raw.get("adapter") or "").strip().lower() == "static":
            blocked_url = str(
                raw.get("listing_url") or raw.get("careersUrl") or endpoint_url(raw) or ""
            ).strip()
            if blocked_url and is_blocked_generic_static_url(blocked_url):
                suppressed_static_count += 1
                suppressed_static_by_reason["blocked_domain"] += 1
                suppressed_static_by_stage[stage] += 1
                failures.append(
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
            source_state_rows=source_state_rows,
            thresholds=thresholds,
        )
        if suppression_reason:
            suppressed_static_count += 1
            suppressed_static_by_reason[suppression_reason] += 1
            suppressed_static_by_stage[stage] += 1
            failures.append(
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
            skipped_invalid += 1
            validation_skipped_count += 1
            failures.append(
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
        evidence_score = int(raw.get("evidenceScore") or 0)
        threshold = _evidence_threshold_for_probe(raw, thresholds)
        if evidence_score < threshold:
            if stage == "provider_pattern":
                skipped_low_evidence_probe_count += 1
                failures.append(
                    {
                        "name": raw.get("name"),
                        "adapter": raw.get("adapter"),
                        "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(),
                        "error": f"pattern evidence score {evidence_score} below probe threshold {threshold}",
                        "stage": "probe_skipped",
                        "dropStage": "low_evidence_skipped",
                        "dropReason": "probe_threshold",
                    }
                )
                continue
            if low_evidence_probes_used >= int(
                thresholds.get("lowEvidenceProbeLimit", LOW_EVIDENCE_PROBE_LIMIT)
            ):
                skipped_low_evidence_probe_count += 1
                failures.append(
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
                continue
            low_evidence_probes_used += 1
        probe_inputs.append(raw)

    try_playwright = None
    try:
        from src.bridge.source_check_http import try_fetch_with_playwright as _try_pw

        _browser_fallback_guard = BrowserFallbackCircuitBreaker.from_state(
            source_state_rows, cooldown_minutes=30
        )
        try_playwright = _browser_fallback_guard.wrap(_try_pw)
    except Exception:  # noqa: S110
        _browser_fallback_guard = None
    playwright_semaphore = asyncio.Semaphore(5) if try_playwright else None

    async def _run_probe_batch(
        rows: list[dict[str, Any]],
    ) -> list[tuple[dict[str, Any], bool, int, str, int]]:
        limits = probe_concurrency_defaults()
        total_sem = asyncio.Semaphore(int(limits["total"]))
        bucket_sems = {
            "static": asyncio.Semaphore(int(limits["static"])),
            "provider": asyncio.Semaphore(int(limits["provider"])),
            "teamtailor": asyncio.Semaphore(int(limits["teamtailor"])),
        }

        async def _call_fetch(url: str, t: int) -> str:
            if fetcher is not fetch_text:
                return await asyncio.to_thread(fetcher, url, t)
            return await async_fetch_text_httpx(client, url, t)

        async def _probe_one(row: dict[str, Any]) -> tuple[dict[str, Any], bool, int, str, int]:
            bucket = probe_bucket_for(row)
            bucket_sem = bucket_sems.get(bucket, bucket_sems["provider"])
            async with total_sem:
                async with bucket_sem:
                    probe_started = time.perf_counter()
                    ok, jobs_found, error = await async_probe_candidate(
                        row,
                        timeout_s,
                        fetcher=_call_fetch,
                        try_playwright=try_playwright,
                        playwright_semaphore=playwright_semaphore,
                    )
                    probe_duration_ms = max(0, int((time.perf_counter() - probe_started) * 1000))
                    return row, ok, jobs_found, error, probe_duration_ms

        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_s)) as client:
            tasks = [asyncio.create_task(_probe_one(row)) for row in rows]
            results: list[tuple[dict[str, Any], bool, int, str, int]] = []
            for fut in asyncio.as_completed(tasks):
                results.append(await fut)
            return results

    completed = 0
    probe_stage_started = time.perf_counter()
    for raw, ok, jobs_found, error, probe_duration_ms in asyncio.run(
        _run_probe_batch(probe_inputs)
    ):
        completed += 1
        stage = str(raw.get("discoveryStage") or "provider_pattern")
        evidence_score = int(raw.get("evidenceScore") or 0)
        probed += 1
        probed_count_by_stage[stage] += 1
        _increment_adapter_runtime(
            adapter_runtime,
            raw.get("adapter"),
            duration_ms=probe_duration_ms,
            probed=1,
        )

        if not ok:
            probe_failed_count += 1
            probe_stage = classify_probe_failure_stage(error)
            _increment_adapter_runtime(adapter_runtime, raw.get("adapter"), failures=1)
            failure_row = {
                "name": raw.get("name"),
                "adapter": raw.get("adapter"),
                "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(),
                "error": error,
                "stage": probe_stage,
                "dropStage": "probe_failed",
                "dropReason": probe_stage,
            }
            failures.append(failure_row)
            failed_probe_records.append({"candidate": dict(raw), "failure": failure_row})
        elif not should_queue_candidate(raw, jobs_found, thresholds):
            queue_filtered_count += 1
            failures.append(
                {
                    "name": raw.get("name"),
                    "adapter": raw.get("adapter"),
                    "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(),
                    "error": f"candidate passed probe but evidence {evidence_score} is below queue threshold",
                    "stage": "queue_filtered",
                    "dropStage": "queue_filtered",
                    "dropReason": "queue_threshold",
                }
            )
        else:
            healthy += 1
            score, reasons = compute_candidate_score(raw, jobs_found)
            normalized = normalize_candidate(raw, score, reasons, jobs_found, probed_at=now_iso())
            prior_candidate = prior_review_candidates_by_id.get(source_identity(normalized))
            rank_score, rank_reasons, promotion_lane = compute_candidate_rank(
                normalized,
                existing_rows=ranking_registry_rows,
                prior_candidate=prior_candidate,
                ranked_at=normalized.get("lastProbedAt") or now_iso(),
            )
            normalized["rankScore"] = int(rank_score)
            normalized["rankReasons"] = unique_string_list(rank_reasons)
            normalized["promotionLane"] = str(promotion_lane or "manual_review")
            queueable_candidates.append(normalized)
            _increment_adapter_runtime(
                adapter_runtime, normalized.get("adapter"), healthy=1, queued=1
            )
            adapter_counter[str(normalized.get("adapter") or "unknown")] += 1
            method_counter[str(normalized.get("discoveryMethod") or "unknown")] += 1

        if completed % 10 == 0:
            emit_log(
                f"Progress: completed={completed}/{len(probe_inputs)}, probed={probed}, queued={len(queueable_candidates)}, "
                f"probe_misses={len([row for row in failures if str(row.get('stage')) == 'probe_miss'])}, "
                f"skipped_low_evidence={skipped_low_evidence_probe_count}."
            )
            write_progress_report(
                queueable_candidates,
                phase="probing_candidates",
                phase_label=f"Probing {len(filtered)} candidate(s)",
            )
    _record_stage_timing(stage_timings_ms, "probe", probe_stage_started)

    patch_added = 0
    patch_updated = 0
    recovered_count = 0
    if failed_probe_records:
        write_progress_report(
            queueable_candidates,
            phase="resolving_url_patches",
            phase_label="Refreshing URL patches",
        )
        new_patches: dict[str, str] = {}
        reprobe_candidates: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
        for record in failed_probe_records:
            candidate = dict(record.get("candidate") or {})
            failure_row = record.get("failure") if isinstance(record.get("failure"), dict) else {}
            error_text = str(failure_row.get("error") or "")
            if not should_attempt_patch_recovery(error_text):
                continue
            original_url = str(endpoint_url(candidate) or candidate.get("careersUrl") or "").strip()
            if not original_url:
                continue
            patched_url = str(
                resolve_patch_target(
                    candidate=candidate, error_text=error_text, timeout_s=timeout_s
                )
                or ""
            ).strip()
            if not patched_url:
                continue
            new_patches[original_url] = patched_url
            patched_candidate, changed = apply_url_patches_to_candidate(
                candidate, {original_url: patched_url}
            )
            if changed:
                reprobe_candidates.append((candidate, patched_candidate, failure_row))

        if new_patches:
            url_patches, patch_added, patch_updated = merge_url_patches(url_patches, new_patches)
            if url_patch_manifest_enabled:
                save_url_patch_manifest(
                    url_patches,
                    path=url_patch_manifest_path,
                    added=patch_added,
                    updated=patch_updated,
                    reprobed=len(reprobe_candidates),
                )
            url_patch_stats["added"] = patch_added
            url_patch_stats["updated"] = patch_updated

        if reprobe_candidates:
            reprobe_results = asyncio.run(
                _run_probe_batch([patched for _original, patched, _failure in reprobe_candidates])
            )
            url_patch_stats["reprobed"] = len(reprobe_results)
            failure_by_identity = {
                str(source_identity(patched_candidate)): (original_candidate, failure_row)
                for original_candidate, patched_candidate, failure_row in reprobe_candidates
            }
            for (
                patched_candidate,
                ok,
                jobs_found,
                reprobe_error,
                _probe_duration_ms,
            ) in reprobe_results:
                _original_candidate, original_failure = failure_by_identity.get(
                    str(source_identity(patched_candidate)),
                    ({}, None),
                )
                if not isinstance(original_failure, dict):
                    continue
                if ok:
                    if original_failure in failures:
                        failures.remove(original_failure)
                    probe_failed_count = max(0, probe_failed_count - 1)
                    _adjust_adapter_runtime(
                        adapter_runtime, patched_candidate.get("adapter"), failures=-1
                    )
                    recovered_count += 1
                    if not should_queue_candidate(patched_candidate, jobs_found, thresholds):
                        queue_filtered_count += 1
                        failures.append(
                            {
                                "name": patched_candidate.get("name"),
                                "adapter": patched_candidate.get("adapter"),
                                "domain": (
                                    urlparse(endpoint_url(patched_candidate)).netloc or ""
                                ).lower(),
                                "error": (
                                    f"candidate passed probe after url patch but evidence "
                                    f"{int(patched_candidate.get('evidenceScore') or 0)} is below queue threshold"
                                ),
                                "stage": "queue_filtered",
                                "dropStage": "queue_filtered",
                                "dropReason": "queue_threshold",
                            }
                        )
                        continue
                    healthy += 1
                    score, reasons = compute_candidate_score(patched_candidate, jobs_found)
                    normalized = normalize_candidate(
                        patched_candidate,
                        score,
                        reasons,
                        jobs_found,
                        probed_at=now_iso(),
                    )
                    prior_candidate = prior_review_candidates_by_id.get(source_identity(normalized))
                    rank_score, rank_reasons, promotion_lane = compute_candidate_rank(
                        normalized,
                        existing_rows=ranking_registry_rows,
                        prior_candidate=prior_candidate,
                        ranked_at=normalized.get("lastProbedAt") or now_iso(),
                    )
                    normalized["rankScore"] = int(rank_score)
                    normalized["rankReasons"] = unique_string_list(rank_reasons)
                    normalized["promotionLane"] = str(promotion_lane or "manual_review")
                    queueable_candidates.append(normalized)
                    _adjust_adapter_runtime(
                        adapter_runtime, normalized.get("adapter"), healthy=1, queued=1
                    )
                    adapter_counter[str(normalized.get("adapter") or "unknown")] += 1
                    method_counter[str(normalized.get("discoveryMethod") or "unknown")] += 1
                else:
                    original_failure["error"] = reprobe_error
                    original_failure["domain"] = (
                        urlparse(endpoint_url(patched_candidate)).netloc or ""
                    ).lower()
                    original_failure["urlPatchRetried"] = True

    queue_balancing_started = time.perf_counter()
    queued_candidates, report_candidates, balancing_summary = apply_queue_balancing(
        queueable_candidates,
        top_n,
        domain_cap=queue_domain_cap,
        adapter_caps=queue_adapter_caps,
    )
    queue_balancing_duration_ms = _record_stage_timing(
        stage_timings_ms, "queueBalancing", queue_balancing_started
    )
    _distribute_duration_by_adapter(
        adapter_runtime, duration_ms=queue_balancing_duration_ms, rows=queued_candidates
    )
    review_timestamp = now_iso()
    for index, row in enumerate(report_candidates):
        if not isinstance(row, dict):
            continue
        if bool(row.get("deferred")):
            row["dropStage"] = "deferred_by_cap"
            row["dropReason"] = str(row.get("deferReason") or "deferred")
        report_candidates[index] = _update_candidate_review_metadata(
            row,
            prior_candidate=prior_review_candidates_by_id.get(source_identity(row)),
            now_iso=review_timestamp,
        )
    queued_ids = {source_identity(row) for row in queued_candidates if isinstance(row, dict)}
    queued_candidates = [
        dict(row)
        for row in report_candidates
        if isinstance(row, dict)
        and source_identity(row) in queued_ids
        and not bool(row.get("deferred"))
    ]
    for row in queued_candidates:
        queued_count_by_stage[str(row.get("discoveryStage") or "provider_pattern")] += 1

    emit_log(
        f"Probe phase finished: healthy={healthy}, queued={len(queued_candidates)}, "
        f"deferred={len([row for row in report_candidates if bool(row.get('deferred'))])}, probe_misses={len([row for row in failures if str(row.get('stage')) == 'probe_miss'])}."
    )
    write_progress_report(
        report_candidates, phase="finalizing", phase_label="Finalizing discovery report"
    )

    save_json_atomic(
        source_registry_module.PENDING_PATH,
        filter_tombstoned_rows(unique_sources([*queued_candidates, *pending_existing]), tombstones),
    )
    save_json_atomic(source_registry_module.DISCOVERY_CANDIDATES_PATH, report_candidates)
    m5_strategic_backlog = build_m5_strategic_backlog(
        report_candidates=report_candidates,
        failures=failures,
        active_rows=active,
        source_state_rows=source_state_rows,
    )
    save_json_atomic(source_registry_module.M5_STRATEGIC_BACKLOG_PATH, m5_strategic_backlog)

    summary = build_stage_summary(
        report_candidates,
        found_endpoint_count=found_endpoint_count,
        generated_count_by_stage=generated_count_by_stage,
        survived_dedupe_count_by_stage=survived_dedupe_count_by_stage,
        probed_count_by_stage=probed_count_by_stage,
        queued_count_by_stage=queued_count_by_stage,
        probed=probed,
        healthy=healthy,
        failures=failures,
        skipped_duplicate_count=skipped_duplicate_count,
        skipped_invalid=skipped_invalid,
        skipped_low_evidence_probe_count=skipped_low_evidence_probe_count,
        validation_skipped_count=validation_skipped_count,
        probe_failed_count=probe_failed_count,
        queue_filtered_count=queue_filtered_count,
        adapter_counter=adapter_counter,
        method_counter=method_counter,
        duplicate_reasons=duplicate_reasons,
        deferred_counts=dict(balancing_summary.get("deferredReasons") or {}),
        queued_by_adapter=dict(balancing_summary.get("queuedByAdapter") or {}),
        deferred_by_adapter=dict(balancing_summary.get("deferredByAdapter") or {}),
        healthy_but_deferred_by_adapter=dict(
            balancing_summary.get("healthyButDeferredByAdapter") or {}
        ),
        suppressed_static_count=suppressed_static_count,
        suppressed_static_by_reason=dict(suppressed_static_by_reason),
        suppressed_static_by_stage=dict(suppressed_static_by_stage),
        thresholds=thresholds,
        phase="completed",
        phase_label="Discovery completed",
    )
    task_progress = build_discovery_task_progress(summary=summary, finished=True)
    failure_counter: Counter[str] = Counter()
    for row in failures:
        stage = str(row.get("stage") or "").strip().lower()
        drop_stage = str(row.get("dropStage") or "").strip().lower()
        drop_reason = str(row.get("dropReason") or "").strip().lower()
        if stage == "dedupe_skipped" or drop_stage == "dedupe_skipped":
            continue
        if stage == "suppressed_static" or drop_stage == "suppressed_static":
            continue
        if drop_reason in {
            "existing_id",
            "existing_domain",
            "run_id",
            "run_domain",
            "blocked_domain",
            "sheet_directory_stage_cap",
        }:
            continue
        adapter = str(row.get("adapter") or "unknown")
        domain = str(row.get("domain") or "").strip()
        failure_counter[f"{adapter}:{domain}" if domain else adapter] += 1

    suppression_summary = {
        "dedupeSkippedCount": int(skipped_duplicate_count),
        "dedupeSkippedByReason": dict(duplicate_reasons),
        "suppressedStaticCount": int(suppressed_static_count),
        "suppressedStaticByReason": dict(suppressed_static_by_reason),
        "suppressedStaticByStage": dict(suppressed_static_by_stage),
    }

    sheet_directory_failures = [
        f for f in failures if isinstance(f, dict) and str(f.get("adapter")) == "sheet_directory"
    ]
    sheet_directory_summary = {
        "fetchFailed": any(
            str(f.get("stage")) == "directory_index_fetch" for f in sheet_directory_failures
        ),
        "parseFailed": any(
            str(f.get("stage")) == "directory_parse" for f in sheet_directory_failures
        ),
        "failureCount": len(sheet_directory_failures),
        "generatedCount": int(
            (summary.get("generatedCountByStage") or {}).get("sheet_directory", 0)
        ),
    }

    report = {
        "schemaVersion": SCHEMA_VERSION,
        "runId": run_id,
        "mode": mode,
        "startedAt": started_at,
        "finishedAt": now_iso(),
        "summary": summary,
        "runtime": {
            **_build_discovery_runtime_payload(
                total_duration_ms=max(0, int((time.perf_counter() - run_started_mono) * 1000)),
                stage_timings_ms=stage_timings_ms,
                adapter_runtime=adapter_runtime,
                preset=preset_name,
                top_cap_bypassed=top_cap_bypassed,
                sheet_static_probe_cap_bypassed=sheet_static_probe_cap_bypassed,
            ),
            "lifecycle": {
                "owner": "discovery_report",
                "heartbeatAt": now_iso(),
            },
        },
        "taskProgress": task_progress,
        "candidates": report_candidates,
        "failures": failures,
        "topFailures": [
            {"key": key, "count": count} for key, count in failure_counter.most_common(5)
        ],
        "suppressionSummary": suppression_summary,
        "sheetDirectorySummary": sheet_directory_summary,
        "outputs": {
            "report": str(discovery_report_write_path()),
            "candidates": str(source_registry_module.DISCOVERY_CANDIDATES_PATH),
            "pending": str(source_registry_module.PENDING_PATH),
            "urlPatches": str(source_registry_module.URL_PATCH_MANIFEST_PATH),
        },
    }
    report["runtime"]["urlPatchStats"] = dict(url_patch_stats)
    report["runtime"]["urlPatchRecoveredCount"] = int(recovered_count)
    state = {
        "active": active,
        "pending": filter_tombstoned_rows([*queued_candidates, *pending_existing], tombstones),
        "rejected": rejected,
    }
    auto_approve_enabled = bool(effective_config.get("autoApproveHealthyPendingOnComplete", True))
    state, auto_approved = apply_discovery_auto_approval(
        state,
        report,
        auto_approve_enabled=auto_approve_enabled,
        approval_state_path=DEFAULT_APPROVAL_STATE_PATH,
        now_iso_fn=now_iso,
    )
    if auto_approved > 0:
        save_json_atomic(
            source_registry_module.ACTIVE_PATH,
            filter_tombstoned_rows(state["active"], tombstones),
        )
        save_json_atomic(
            source_registry_module.PENDING_PATH,
            filter_tombstoned_rows(state["pending"], tombstones),
        )
        save_json_atomic(
            source_registry_module.REJECTED_PATH,
            filter_tombstoned_rows(state["rejected"], tombstones),
        )
        emit_log(f"Auto-approval applied during discovery: approved={auto_approved}.")
    DiscoveryReportSchema.model_validate(report)
    final_report_path = discovery_report_write_path()
    save_json_atomic(final_report_path, report)
    emit_log(f"Discovery report written to {final_report_path}.")
    return report


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    env_run_id = str(os.environ.get("BALUFFO_DISCOVERY_RUN_ID") or "").strip()
    env_started_at = str(os.environ.get("BALUFFO_DISCOVERY_STARTED_AT") or "").strip()
    bridge_spawned = bool(env_run_id and env_started_at)
    if bridge_spawned:
        _prime_bridge_discovery_report(
            run_id=env_run_id,
            started_at=env_started_at,
            mode=str(getattr(args, "mode", None) or "dynamic"),
        )
    if bridge_spawned:
        discovery_config = None
    else:
        discovery_config = _apply_discovery_cli_args_to_config(load_discovery_config(), args)
    report = run_discovery(
        timeout_s=int(args.timeout),
        top_n=int(args.top),
        preset=str(args.preset or "default"),
        mode=str(args.mode),
        include_web_search=not bool(args.no_web_search),
        discovery_config=discovery_config,
        run_id=env_run_id,
        started_at_override=env_started_at,
        cli_args=args if bridge_spawned else None,
    )
    emit_log(
        "Source discovery completed. "
        f"Found endpoints: {report['summary']['foundEndpointCount']}. "
        f"Queued candidates: {report['summary']['queuedCandidateCount']}. "
        f"Deferred candidates: {report['summary'].get('discoverableButDeferredCount', 0)}. "
        f"Failed probes: {report['summary'].get('failedProbeCount', 0)}. "
        f"Probe misses: {report['summary'].get('probeMissCount', 0)}. "
        f"Report: {report['outputs']['report']}"
    )
    return 0
