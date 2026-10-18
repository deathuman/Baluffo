"""Public source-discovery package surface and compatibility exports.

AI boundary owns: source-discovery re-exports, compatibility shims, and package-level helper entrypoints.
AI boundary implement in: this file for package surface changes; discovery behavior stays in source_discovery leaf modules.
AI boundary search before contracts: discovery config, orchestration leaves, source policy callers, and discovery tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused source-discovery package tests.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

from . import config as _config
from .config import (
    ADAPTER_QUEUE_CAPS,
    CAREERS_URL_HINTS,
    COVERAGE_BOARDS_PATH,
    DEFAULT_DISCOVERY_CONFIG,
    DEFAULT_DISCOVERY_THRESHOLDS,
    DISCOVERY_CONFIG_PATH,
    DISCOVERY_LOG_PATH,
    DISCOVERY_STAGES,
    DOMAIN_QUEUE_CAP_DEFAULT,
    DUCKDUCKGO_HTML_SEARCH,
    FETCH_MAX_RETRIES,
    FOCUS_KEYWORDS,
    GAME_STUDIOS_SHEET_GID,
    GAME_STUDIOS_SHEET_ID,
    GAME_STUDIOS_SHEET_URL,
    GENERIC_STATIC_BLOCKED_DOMAINS,
    LOW_EVIDENCE_PROBE_LIMIT,
    MIN_PROVIDER_EVIDENCE_TO_PROBE,
    MIN_PROVIDER_EVIDENCE_TO_QUEUE,
    MIN_STATIC_EVIDENCE_TO_PROBE,
    MIN_STATIC_EVIDENCE_TO_QUEUE,
    PATTERN_PROVIDER_PROBE_THRESHOLD,
    PATTERN_PROVIDER_QUEUE_THRESHOLD,
    RETRYABLE_HTTP_CODES,
    SEED_CATALOG_PATH,
    STATIC_DISCOVERY_CANDIDATES,
    STUDIO_SEEDS,
    SUPPORTED_PROVIDERS,
    UNCAPPED_DISCOVERY_ADAPTER_QUEUE_CAPS,
    UNCAPPED_DISCOVERY_DOMAIN_QUEUE_CAP,
    WEB_SEARCH_QUERY_SUFFIX,
    load_curated_coverage_boards,
    load_studio_seeds,
)


def load_discovery_config():
    """Load discovery config through the owning config module."""
    return _config.load_discovery_config(_config.DISCOVERY_CONFIG_PATH)


from .provider_patterns import build_pattern_candidates as _build_pattern_candidates


def build_pattern_candidates(studio_seeds=None):
    """Build pattern candidates using the owning config module defaults."""
    if studio_seeds is None:
        studio_seeds = _config.STUDIO_SEEDS
    return _build_pattern_candidates(studio_seeds)


from src.shared.utils import now_iso
from src.source_registry import (
    ACTIVE_PATH,
    DISCOVERY_CANDIDATES_PATH,
    DISCOVERY_REPORT_PATH,
    M5_STRATEGIC_BACKLOG_PATH,
    PENDING_PATH,
    REJECTED_PATH,
    URL_PATCH_MANIFEST_PATH,
)

# Every other name is resolved on first attribute access (PEP 562). Importing this
# package used to execute the whole discovery surface -- orchestrator, sheet/directory
# recovery, probes, and (through them) the jobs adapter registry and pipeline -- so any
# bridge module that needed one leaf paid for all of it at startup. `from
# src.source_discovery import run_discovery` and `src.source_discovery.probe_candidate`
# keep working; only the module that defines the name is imported.
_LAZY_EXPORTS: dict[str, str] = {
    "adapter_domain_fingerprint": "core",
    "apply_queue_balancing": "core",
    "apply_sheet_directory_static_probe_cap": "core",
    "classify_probe_failure_stage": "core",
    "classify_static_suppression": "core",
    "compute_candidate_rank": "core",
    "compute_candidate_score": "core",
    "normalize_candidate": "core",
    "probe_concurrency_defaults": "core",
    "queue_family_key": "core",
    "sheet_directory_static_probe_cap": "core",
    "directory_fetch_concurrency_defaults": "directory_fetch",
    "fetch_directory_pages": "directory_fetch",
    "resolve_directory_fetch_limits": "directory_fetch",
    "build_gamedevmap_search_url": "gamedevmap",
    "discover_gamedevmap_candidates": "gamedevmap",
    "parse_gamedevmap_csv": "gamedevmap",
    "select_gamedevmap_representative_rows": "gamedevmap",
    "apply_gamedevmap_lost_recovery_audit": "gamedevmap_active_dry_run",
    "compare_gamedevmap_recovered_sources": "gamedevmap_active_dry_run",
    "discover_gamedevmap_audit_candidates": "gamedevmap_active_dry_run",
    "gamedevmap_active_dry_run_path": "gamedevmap_active_dry_run",
    "gamedevmap_audit_report_summary": "gamedevmap_active_dry_run",
    "gamedevmap_validated_candidates_from_artifact": "gamedevmap_active_dry_run",
    "latest_gamedevmap_audit_report_summary": "gamedevmap_active_dry_run",
    "run_gamedevmap_active_source_dry_run": "gamedevmap_active_dry_run",
    "run_gamedevmap_browser_recovery": "gamedevmap_active_dry_run",
    "run_gamedevmap_source_audit": "gamedevmap_active_dry_run",
    "discover_gameprog_candidates": "gameprog",
    "parse_gameprog_teams_json": "gameprog",
    "discover_gamesmap_candidates": "gamesmap",
    "gamesmap_matches_category": "gamesmap",
    "normalize_gamesmap_category_token": "gamesmap",
    "parse_gamesmap_detail_page": "gamesmap",
    "parse_gamesmap_index_entries": "gamesmap",
    "parse_gamesmap_index_links": "gamesmap",
    "endpoint_url": "io_runtime",
    "main": "orchestrator",
    "parse_args": "orchestrator",
    "run_discovery": "orchestrator",
    "analyze_fetched_page": "page_analysis",
    "extract_explicit_careers_url_from_page": "page_analysis",
    "async_probe_candidate": "probe",
    "probe_candidate": "probe",
    "validate_candidate_for_probe": "probe",
    "build_m5_strategic_backlog": "reporting",
    "emit_log": "reporting",
    "merge_candidate_streams": "reporting",
    "stage_curated_seed_candidates": "reporting",
    "resolve_discovery_thresholds": "scoring",
    "unique_string_list": "scoring",
    "discover_game_studio_sheet_candidates": "sheet_directory",
    "game_studios_sheet_candidate_urls": "sheet_directory",
    "parse_game_studio_sheet_csv": "sheet_directory",
    "run_sheet_directory_audit": "sheet_directory",
    "build_known_careers_url_candidate": "static_candidates",
    "build_static_candidate_from_page": "static_candidates",
    "apply_url_patches_to_candidate": "url_patches",
    "load_url_patch_manifest": "url_patches",
    "load_url_patches": "url_patches",
    "merge_url_patches": "url_patches",
    "save_url_patch_manifest": "url_patches",
    "fetch_text": "web_search",
    "fetch_text_with_retry": "web_search",
    "infer_provider_candidates_from_html": "web_search",
    "infer_web_candidate": "web_search",
    "run_web_search_browser_recovery": "web_search_candidates",
    "run_web_search_directory_audit": "web_search_candidates",
}

__all__ = sorted(
    set(_LAZY_EXPORTS)
    | {
        name
        for name in globals()
        if not name.startswith("_") and name not in {"Any", "annotations", "importlib"}
    }
)


def __getattr__(name: str) -> Any:
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value: Any = getattr(importlib.import_module(f"{__name__}.{module_name}"), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_LAZY_EXPORTS))


if TYPE_CHECKING:  # pragma: no cover - typing-only declarations for the lazy exports
    # PEP 562 __getattr__ is typed Any, which would erase this surface for mypy callers
    # (`sd.run_discovery(...)` returning Any). These imports are never executed; the
    # runtime still resolves every name lazily through __getattr__.
    from .core import (
        adapter_domain_fingerprint,
        apply_queue_balancing,
        apply_sheet_directory_static_probe_cap,
        classify_probe_failure_stage,
        classify_static_suppression,
        compute_candidate_rank,
        compute_candidate_score,
        normalize_candidate,
        probe_concurrency_defaults,
        queue_family_key,
        sheet_directory_static_probe_cap,
    )
    from .directory_fetch import (
        directory_fetch_concurrency_defaults,
        fetch_directory_pages,
        resolve_directory_fetch_limits,
    )
    from .gamedevmap import (
        build_gamedevmap_search_url,
        discover_gamedevmap_candidates,
        parse_gamedevmap_csv,
        select_gamedevmap_representative_rows,
    )
    from .gamedevmap_active_dry_run import (
        apply_gamedevmap_lost_recovery_audit,
        compare_gamedevmap_recovered_sources,
        discover_gamedevmap_audit_candidates,
        gamedevmap_active_dry_run_path,
        gamedevmap_audit_report_summary,
        gamedevmap_validated_candidates_from_artifact,
        latest_gamedevmap_audit_report_summary,
        run_gamedevmap_active_source_dry_run,
        run_gamedevmap_browser_recovery,
        run_gamedevmap_source_audit,
    )
    from .gameprog import (
        discover_gameprog_candidates,
        parse_gameprog_teams_json,
    )
    from .gamesmap import (
        discover_gamesmap_candidates,
        gamesmap_matches_category,
        normalize_gamesmap_category_token,
        parse_gamesmap_detail_page,
        parse_gamesmap_index_entries,
        parse_gamesmap_index_links,
    )
    from .io_runtime import endpoint_url
    from .orchestrator import (
        main,
        parse_args,
        run_discovery,
    )
    from .page_analysis import (
        analyze_fetched_page,
        extract_explicit_careers_url_from_page,
    )
    from .probe import (
        async_probe_candidate,
        probe_candidate,
        validate_candidate_for_probe,
    )
    from .reporting import (
        build_m5_strategic_backlog,
        emit_log,
        merge_candidate_streams,
        stage_curated_seed_candidates,
    )
    from .scoring import (
        resolve_discovery_thresholds,
        unique_string_list,
    )
    from .sheet_directory import (
        discover_game_studio_sheet_candidates,
        game_studios_sheet_candidate_urls,
        parse_game_studio_sheet_csv,
        run_sheet_directory_audit,
    )
    from .static_candidates import (
        build_known_careers_url_candidate,
        build_static_candidate_from_page,
    )
    from .url_patches import (
        apply_url_patches_to_candidate,
        load_url_patch_manifest,
        load_url_patches,
        merge_url_patches,
        save_url_patch_manifest,
    )
    from .web_search import (
        fetch_text,
        fetch_text_with_retry,
        infer_provider_candidates_from_html,
        infer_web_candidate,
    )
    from .web_search_candidates import (
        run_web_search_browser_recovery,
        run_web_search_directory_audit,
    )
