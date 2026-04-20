#!/usr/bin/env python3
"""Stable thin CLI facade for the refactored jobs pipeline package."""

from __future__ import annotations

import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen


def _ensure_repo_on_path() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)


_ensure_repo_on_path()

from src.contracts import SCHEMA_VERSION
from src.jobs import canonicalize as _canonicalize
from src.jobs import dedup as _dedup
from src.jobs import parsers as _parsers
from src.jobs import pipeline as _pipeline
from src.jobs import registry as _registry
from src.jobs import reporting as _reporting
from src.jobs import state as _state
from src.jobs import transport as _transport
from src.jobs.adapters import community as _community
from src.jobs.adapters import provider_api as _provider_api
from src.jobs.adapters import social as _social
from src.jobs.adapters import static as _static
from src.jobs.adapters import static_scrapy as _static_scrapy
from src.jobs.common import config as _common_config
from src.jobs.common import datetime_utils as _common_datetime_utils
from src.jobs.common import diagnostics as _common_diagnostics
from src.jobs.common import fetch as _common_fetch
from src.jobs.common import url as _common_url
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.jobs_fetcher_registry import SOURCE_REPORT_META
from src.shared.utils import env_flag, now_iso

SOURCE_DIAGNOSTICS = _common_diagnostics.SOURCE_DIAGNOSTICS
STUDIO_SOURCE_REGISTRY = _registry.STUDIO_SOURCE_REGISTRY
httpx = _transport.httpx


def _module_attr_exports(module: object, names: tuple[str, ...]) -> dict[str, tuple[object, str]]:
    return {name: (module, name) for name in names}


_COMPAT_MODULE_EXPORTS: dict[str, tuple[object, str]] = {}
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _parsers,
        (
            "parse_8bitplay_html",
            "parse_ashby_jobs_from_html",
            "parse_bamboohr_jobs_html",
            "parse_breezy_jobs_html",
            "parse_epic_games_jobs_payload",
            "parse_gamesindustry_html",
            "parse_gamejobs_html",
            "parse_google_sheets_csv",
            "parse_gracklehq_html",
            "parse_greenhouse_jobs_payload",
            "parse_jazzhr_jobs_html",
            "parse_jobpostings_from_html",
            "parse_lever_jobs_payload",
            "parse_mastodon_payload",
            "parse_pinpoint_jobs_payload",
            "parse_personio_feed_xml",
            "parse_recruitee_jobs_payload",
            "parse_reddit_json_payload",
            "parse_reddit_rss_payload",
            "parse_remote_ok_payload",
            "parse_smartrecruiters_jobs_payload",
            "parse_teamtailor_listing_links",
            "parse_wellfound_html",
            "parse_workable_jobs_payload",
            "parse_workday_jobs_html",
            "parse_workwithindies_html",
            "parse_x_payload",
            "parse_x_rss_payload",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _canonicalize,
        (
            "LIGHTWEIGHT_OUTPUT_FIELDS",
            "OPTIONAL_FIELDS",
            "OUTPUT_FIELDS",
            "REQUIRED_FIELDS",
            "UNKNOWN_COMPANY_LABEL",
            "map_profession",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _transport,
        (
            "AsyncHttpTextFetcher",
            "DEFAULT_REDIRECT_HEADERS",
            "PooledRedirectResolver",
            "default_fetch_text",
            "resolve_fetch_text_impl",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _reporting,
        (
            "normalize_fetch_report_payload",
            "normalize_runtime_payload",
            "normalize_source_report_row",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _state,
        (
            "should_skip_source_by_cadence",
            "should_skip_source_by_ttl",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _community,
        (
            "DEFAULT_GOOGLE_SHEET_GID",
            "DEFAULT_GOOGLE_SHEET_ID",
            "DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY",
            "google_sheet_candidate_urls",
            "run_8bitplay_source",
            "run_epic_games_careers_source",
            "run_gamesindustry_source",
            "run_gamejobs_source",
            "run_google_sheets_source",
            "run_gracklehq_source",
            "run_remote_ok_source",
            "run_wellfound_source",
            "run_workwithindies_source",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _provider_api,
        (
            "run_ashby_sources_source",
            "run_bamboohr_sources_source",
            "run_breezy_sources_source",
            "run_greenhouse_boards_source",
            "run_jazzhr_sources_source",
            "run_lever_sources_source",
            "run_personio_sources_source",
            "run_pinpoint_sources_source",
            "run_recruitee_sources_source",
            "run_smartrecruiters_sources_source",
            "run_teamtailor_sources_source",
            "run_workable_sources_source",
            "run_workday_sources_source",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _social,
        (
            "run_social_mastodon_source",
            "run_social_reddit_source",
            "run_social_x_source",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _static,
        (
            "run_static_source_entry_source",
            "run_static_studio_pages_a_i_source",
            "run_static_studio_pages_j_r_source",
            "run_static_studio_pages_s_z_source",
            "run_static_studio_pages_source",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _common_config,
        (
            "DEFAULT_ADAPTER_HTTP_CONCURRENCY",
            "DEFAULT_BACKOFF_S",
            "DEFAULT_COLD_SOURCE_CADENCE_MINUTES",
            "DEFAULT_FETCH_STRATEGY",
            "DEFAULT_HOT_SOURCE_CADENCE_MINUTES",
            "DEFAULT_RETRIES",
            "DEFAULT_SCRAPY_VALIDATION_STRICT",
            "DEFAULT_STATIC_DETAIL_CONCURRENCY",
            "DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE",
            "DEFAULT_TIMEOUT_S",
            "EPIC_CAREERS_API_URL",
            "GAMES_INDUSTRY_URLS",
            "GREENHOUSE_JOBS_URL_TEMPLATE",
            "REMOTE_OK_URLS",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _common_datetime_utils,
        ("to_iso",),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _common_diagnostics,
        ("set_source_diagnostics",),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _common_fetch,
        ("fetch_with_retries",),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _common_url,
        ("fingerprint_url",),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _registry,
        (
            "DEFAULT_OUTPUT_DIR",
            "DEFAULT_SOCIAL_CONFIG_PATH",
            "DEFAULT_SOCIAL_LOOKBACK_MINUTES",
            "DEFAULT_SOCIAL_MIN_CONFIDENCE",
            "GOOGLE_SHEETS_SOURCES",
            "SOURCE_REGISTRY_ACTIVE_PATH",
            "SOURCE_REGISTRY_PENDING_PATH",
            "load_registry_from_file",
            "load_social_config",
            "read_approved_since_last_run",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _dedup,
        ("deduplicate_jobs",),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        _pipeline,
        ("default_source_loaders",),
    )
)

_COMPAT_VALUES: dict[str, object] = {
    "SCHEMA_VERSION": SCHEMA_VERSION,
    "SOURCE_DIAGNOSTICS": SOURCE_DIAGNOSTICS,
    "SOURCE_REPORT_META": SOURCE_REPORT_META,
    "STUDIO_SOURCE_REGISTRY": STUDIO_SOURCE_REGISTRY,
    "clean_text": clean_text,
    "datetime": datetime,
    "env_flag": env_flag,
    "httpx": httpx,
    "norm_text": norm_text,
    "normalize_url": normalize_url,
    "now_iso": now_iso,
    "re": re,
    "timedelta": timedelta,
    "timezone": timezone,
    "urlopen": urlopen,
}

__all__ = [
    "SCHEMA_VERSION",
    "SOURCE_DIAGNOSTICS",
    "SOURCE_REPORT_META",
    "STUDIO_SOURCE_REGISTRY",
    "build_redirect_resolver",
    "default_source_loaders",
    "main",
    "maybe_fetch_kojima_job_listing_html",
    "parse_args",
    "registry_entries",
    "run_pipeline",
    "run_scrapy_static_source",
]


def parse_args(*args, **kwargs):
    return _pipeline.parse_args(*args, **kwargs)


def main(*args, **kwargs):
    return _pipeline.main(*args, **kwargs)


def run_pipeline(*args, **kwargs):
    """Run the jobs pipeline with the current module-level resolver hook."""
    previous = getattr(_pipeline, "build_redirect_resolver", None)
    try:
        _pipeline.build_redirect_resolver = build_redirect_resolver  # type: ignore[assignment]
        return _pipeline.run_pipeline(*args, **kwargs)
    finally:
        if previous is not None:
            _pipeline.build_redirect_resolver = previous  # type: ignore[assignment]


def run_scrapy_static_source(*args, **kwargs):
    """Run scrapy static source with jobs_fetcher registry overrides."""
    previous = getattr(_static_scrapy, "registry_entries", None)
    try:
        _static_scrapy.registry_entries = registry_entries  # type: ignore[assignment]
        return _static.run_scrapy_static_source(*args, **kwargs)
    finally:
        if previous is not None:
            _static_scrapy.registry_entries = previous  # type: ignore[assignment]


def registry_entries(
    adapter: str,
    *,
    enabled_only: bool = True,
):
    return _registry.registry_entries(
        adapter,
        enabled_only=enabled_only,
        registry_rows=STUDIO_SOURCE_REGISTRY,
    )


def build_redirect_resolver(*args, **kwargs):
    previous_httpx = _transport.httpx
    _transport.httpx = httpx
    try:
        return _transport.build_redirect_resolver(*args, **kwargs)
    finally:
        _transport.httpx = previous_httpx


def maybe_fetch_kojima_job_listing_html(*args, **kwargs):
    import src.jobs.adapters.html_parsers as _html_parsers

    _html_parsers.urlopen = urlopen
    return _html_parsers.maybe_fetch_kojima_job_listing_html(*args, **kwargs)


def __getattr__(name: str) -> object:
    if name in _COMPAT_VALUES:
        return _COMPAT_VALUES[name]
    module_attr = _COMPAT_MODULE_EXPORTS.get(name)
    if module_attr is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module, attr_name = module_attr
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_COMPAT_VALUES) | set(_COMPAT_MODULE_EXPORTS))


if __name__ == "__main__":
    raise SystemExit(main())
