from __future__ import annotations

from . import config as _config
from .config import (
    ADAPTER_QUEUE_CAPS,
    CAREERS_URL_HINTS,
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
    load_studio_seeds,
)


def load_discovery_config():
    """Load discovery config through the owning config module."""
    return _config.load_discovery_config(_config.DISCOVERY_CONFIG_PATH)


from .provider_patterns import build_pattern_candidates as _build_pattern_candidates
from .sheet_directory import (
    discover_game_studio_sheet_candidates,
    game_studios_sheet_candidate_urls,
    parse_game_studio_sheet_csv,
)


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
from .orchestrator import main, parse_args, run_discovery
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
from .scoring import resolve_discovery_thresholds, unique_string_list
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
    discover_web_search_candidates,
    fetch_text,
    fetch_text_with_retry,
    infer_provider_candidates_from_html,
    infer_web_candidate,
)


def discover_seed_careers_page_candidates(timeout_s: int, *, fetcher=None):
    """Discover candidates from seed careers pages using config-owned seed defaults."""
    from .web_search import discover_seed_careers_page_candidates as _discover

    return _discover(timeout_s, studio_seeds=_config.STUDIO_SEEDS, fetcher=fetcher or fetch_text)
