from __future__ import annotations

from . import config as _config
from .config import (  # noqa: F401
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
    GENERIC_STATIC_BLOCKED_DOMAINS,
    GAME_STUDIOS_SHEET_GID,
    GAME_STUDIOS_SHEET_ID,
    GAME_STUDIOS_SHEET_URL,
    MIN_PROVIDER_EVIDENCE_TO_PROBE,
    MIN_PROVIDER_EVIDENCE_TO_QUEUE,
    MIN_STATIC_EVIDENCE_TO_PROBE,
    MIN_STATIC_EVIDENCE_TO_QUEUE,
    LOW_EVIDENCE_PROBE_LIMIT,
    PATTERN_PROVIDER_PROBE_THRESHOLD,
    PATTERN_PROVIDER_QUEUE_THRESHOLD,
    RETRYABLE_HTTP_CODES,
    SEED_CATALOG_PATH,
    STATIC_DISCOVERY_CANDIDATES,
    STUDIO_SEEDS,
    SUPPORTED_PROVIDERS,
    WEB_SEARCH_QUERY_SUFFIX,
    load_studio_seeds,
)


def load_discovery_config():
    """Load discovery config; uses this module's DISCOVERY_CONFIG_PATH so tests can override it."""
    import sys
    pkg = sys.modules[__name__]
    path = getattr(pkg, "DISCOVERY_CONFIG_PATH", _config.DISCOVERY_CONFIG_PATH)
    return _config.load_discovery_config(path)
from .sheet_directory import (  # noqa: F401
    discover_game_studio_sheet_candidates,
    game_studios_sheet_candidate_urls,
    parse_game_studio_sheet_csv,
)
from .provider_patterns import build_pattern_candidates as _build_pattern_candidates


def build_pattern_candidates(studio_seeds=None):
    """Build pattern candidates; uses STUDIO_SEEDS when studio_seeds is not provided."""
    if studio_seeds is None:
        studio_seeds = STUDIO_SEEDS
    return _build_pattern_candidates(studio_seeds)
from .web_search import (  # noqa: F401
    discover_web_search_candidates,
    fetch_text,
    fetch_text_with_retry,
    infer_provider_candidates_from_html,
    infer_web_candidate,
)
from .static_candidates import build_static_candidate_from_page  # noqa: F401
from .reporting import emit_log, merge_candidate_streams, stage_curated_seed_candidates  # noqa: F401
from src.source_registry import (  # noqa: F401
    ACTIVE_PATH,
    DISCOVERY_CANDIDATES_PATH,
    DISCOVERY_REPORT_PATH,
    PENDING_PATH,
    REJECTED_PATH,
)
from .io_runtime import endpoint_url  # noqa: F401
from .scoring import resolve_discovery_thresholds, unique_string_list  # noqa: F401
from .probe import (  # noqa: F401
    async_probe_candidate,
    probe_candidate,
    validate_candidate_for_probe,
)
from src.shared.utils import now_iso  # noqa: F401
from .gamesmap import (  # noqa: F401
    discover_gamesmap_candidates,
    gamesmap_matches_category,
    normalize_gamesmap_category_token,
    parse_gamesmap_detail_page,
    parse_gamesmap_index_entries,
    parse_gamesmap_index_links,
)
from .core import (  # noqa: F401
    adapter_domain_fingerprint,
    apply_queue_balancing,
    apply_sheet_directory_static_probe_cap,
    classify_static_suppression,
    classify_probe_failure_stage,
    compute_candidate_score,
    normalize_candidate,
    probe_concurrency_defaults,
    queue_family_key,
    sheet_directory_static_probe_cap,
)
from .orchestrator import main, parse_args, run_discovery  # noqa: F401


def discover_seed_careers_page_candidates(timeout_s: int, *, fetcher=None):
    """Discover candidates from seed careers pages; uses STUDIO_SEEDS when called from this module."""
    from .web_search import discover_seed_careers_page_candidates as _discover

    return _discover(timeout_s, studio_seeds=STUDIO_SEEDS, fetcher=fetcher or fetch_text)

