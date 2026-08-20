#!/usr/bin/env python3
"""Stable thin CLI facade for the refactored jobs pipeline package.

AI boundary: this file owns CLI compatibility exports and root patch seams only.
AI boundary implement in: `src.jobs.*` pipeline, adapter, dedup, report leaves.
AI boundary search before contracts: bridge task launch and frontend fetcher callers.
AI boundary verify: `npm run test:refactor:changed` plus focused fetcher tests.
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.request import urlopen


def _ensure_repo_on_path() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)


_ensure_repo_on_path()

import src.jobs.common.contracts_fetch_report as contracts_fetch_report_mod
import src.jobs.common.contracts_runtime as contracts_runtime_mod
import src.jobs.common.contracts_source_reports as contracts_source_reports_mod
from src.contracts import SCHEMA_VERSION
from src.jobs import canonicalize as canonicalize_mod
from src.jobs import dedup as dedup_mod
from src.jobs import fetcher_compat_runtime as fetcher_compat_runtime_mod
from src.jobs import pipeline as pipeline_mod
from src.jobs import pipeline_root
from src.jobs import registry as registry_mod
from src.jobs import state_incremental as state_incremental_mod
from src.jobs import transport as transport_mod
from src.jobs.adapters import community as community_mod
from src.jobs.adapters import html_parsers as html_parsers_mod
from src.jobs.adapters import provider_api as provider_api_mod
from src.jobs.adapters import provider_parsers as provider_parsers_mod
from src.jobs.adapters import social as social_mod
from src.jobs.adapters import static as static_mod
from src.jobs.adapters.social_parser import mastodon_parser as social_parsers_mastodon_mod
from src.jobs.adapters.social_parser import reddit_parser as social_parsers_reddit_mod
from src.jobs.adapters.social_parser import x_parser as social_parsers_x_mod
from src.jobs.common import config as common_config_mod
from src.jobs.common import datetime_utils as common_datetime_utils_mod
from src.jobs.common import diagnostics as common_diagnostics_mod
from src.jobs.common import fetch as common_fetch_mod
from src.jobs.common import url as common_url_mod
from src.jobs.common.parsing import parse_remote_ok_payload as _parse_remote_ok_payload
from src.jobs.common.parsing import parse_remotive_payload as _parse_remotive_payload
from src.jobs.game_detection import looks_like_game_job as _looks_like_game_job
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.jobs_fetcher_registry import SOURCE_REPORT_META
from src.shared.utils import env_flag, now_iso

SOURCE_DIAGNOSTICS = common_diagnostics_mod.SOURCE_DIAGNOSTICS
STUDIO_SOURCE_REGISTRY = registry_mod.STUDIO_SOURCE_REGISTRY
httpx = transport_mod.httpx


def _module_attr_exports(module: object, names: tuple[str, ...]) -> dict[str, tuple[object, str]]:
    return {name: (module, name) for name in names}


def _parse_remote_ok_payload_compat(payload: Any) -> list:
    return _parse_remote_ok_payload(payload, looks_like_game_job=_looks_like_game_job)


def _parse_remotive_payload_compat(payload: Any) -> list:
    return _parse_remotive_payload(payload, looks_like_game_job=_looks_like_game_job)


_COMPAT_MODULE_EXPORTS: dict[str, tuple[object, str]] = {}
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        html_parsers_mod,
        (
            "parse_gamesindustry_html",
            "parse_jobpostings_from_html",
            "parse_teamtailor_listing_links",
            "parse_wellfound_html",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        provider_parsers_mod,
        (
            "parse_ashby_jobs_from_html",
            "parse_bamboohr_jobs_html",
            "parse_breezy_jobs_html",
            "parse_epic_games_jobs_payload",
            "parse_greenhouse_jobs_payload",
            "parse_jazzhr_jobs_html",
            "parse_lever_jobs_payload",
            "parse_oracle_hcm_requisitions_payload",
            "parse_personio_feed_xml",
            "parse_pinpoint_jobs_payload",
            "parse_recruitee_jobs_payload",
            "parse_smartrecruiters_jobs_payload",
            "parse_workable_jobs_payload",
            "parse_workday_jobs_html",
        ),
    )
)
_COMPAT_MODULE_EXPORTS["parse_mastodon_payload"] = (
    social_parsers_mastodon_mod,
    "parse_mastodon_payload",
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        social_parsers_reddit_mod,
        (
            "parse_reddit_json_payload",
            "parse_reddit_rss_payload",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        social_parsers_x_mod,
        (
            "parse_x_payload",
            "parse_x_rss_payload",
        ),
    )
)
_COMPAT_MODULE_EXPORTS["parse_remote_ok_payload"] = (
    sys.modules[__name__],
    "_parse_remote_ok_payload_compat",
)
_COMPAT_MODULE_EXPORTS["parse_remotive_payload"] = (
    sys.modules[__name__],
    "_parse_remotive_payload_compat",
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        canonicalize_mod,
        (
            "LIGHTWEIGHT_OUTPUT_FIELDS",
            "OPTIONAL_FIELDS",
            "OUTPUT_FIELDS",
            "REQUIRED_FIELDS",
            "GoogleSheetsProviderTitleResolver",
            "UNKNOWN_COMPANY_LABEL",
            "canonicalize_google_sheets_rows",
            "canonicalize_job",
            "canonicalize_job_with_reason",
            "map_profession",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        transport_mod,
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
    {
        "normalize_fetch_report_payload": (
            contracts_fetch_report_mod,
            "normalize_fetch_report_payload",
        ),
        "normalize_runtime_payload": (contracts_runtime_mod, "normalize_runtime_payload"),
        "normalize_source_report_row": (
            contracts_source_reports_mod,
            "normalize_source_report_row",
        ),
    }
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        state_incremental_mod,
        (
            "should_skip_source_by_cadence",
            "should_skip_source_by_ttl",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        community_mod,
        (
            "DEFAULT_GOOGLE_SHEET_GID",
            "DEFAULT_GOOGLE_SHEET_ID",
            "DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY",
            "google_sheet_candidate_urls",
            "parse_8bitplay_html",
            "parse_gamejobs_html",
            "parse_google_sheets_csv",
            "parse_gracklehq_html",
            "parse_workwithindies_html",
            "run_8bitplay_source",
            "run_epic_games_careers_source",
            "run_gamesindustry_source",
            "run_gamejobs_source",
            "run_google_sheets_source",
            "run_gracklehq_source",
            "run_remote_ok_source",
            "run_remotive_source",
            "run_wellfound_source",
            "run_workwithindies_source",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        provider_api_mod,
        (
            "run_ashby_sources_source",
            "run_bamboohr_sources_source",
            "run_breezy_sources_source",
            "run_greenhouse_boards_source",
            "run_jazzhr_sources_source",
            "run_lever_sources_source",
            "run_oracle_hcm_sources_source",
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
        social_mod,
        (
            "run_social_mastodon_source",
            "run_social_reddit_source",
            "run_social_x_source",
        ),
    )
)
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        static_mod,
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
        common_config_mod,
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
_COMPAT_MODULE_EXPORTS.update(_module_attr_exports(common_datetime_utils_mod, ("to_iso",)))
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(common_diagnostics_mod, ("set_source_diagnostics",))
)
_COMPAT_MODULE_EXPORTS.update(_module_attr_exports(common_fetch_mod, ("fetch_with_retries",)))
_COMPAT_MODULE_EXPORTS.update(_module_attr_exports(common_url_mod, ("fingerprint_url",)))
_COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        registry_mod,
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
_COMPAT_MODULE_EXPORTS.update(_module_attr_exports(dedup_mod, ("deduplicate_jobs",)))
_COMPAT_MODULE_EXPORTS.update(_module_attr_exports(pipeline_mod, ("default_source_loaders",)))

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

pipeline_root.bind_jobs_fetcher(sys.modules[__name__])

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
    return pipeline_mod.parse_args(*args, **kwargs)


def main(*args, **kwargs):
    return pipeline_mod.main(*args, **kwargs)


def run_pipeline(*args, **kwargs):
    return fetcher_compat_runtime_mod.run_pipeline(*args, **kwargs)


def run_scrapy_static_source(*args, **kwargs):
    return fetcher_compat_runtime_mod.run_scrapy_static_source(*args, **kwargs)


def registry_entries(adapter: str, *, enabled_only: bool = True, **kwargs):
    return fetcher_compat_runtime_mod.registry_entries(adapter, enabled_only=enabled_only, **kwargs)


def build_redirect_resolver(*args, **kwargs):
    return fetcher_compat_runtime_mod.build_redirect_resolver(*args, **kwargs)


def maybe_fetch_kojima_job_listing_html(*args, **kwargs):
    return fetcher_compat_runtime_mod.maybe_fetch_kojima_job_listing_html(*args, **kwargs)


def __getattr__(name: str) -> Any:
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
