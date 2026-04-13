#!/usr/bin/env python3
"""Stable thin CLI facade for the refactored jobs pipeline package."""

from __future__ import annotations

import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen

try:
    from src.contracts import SCHEMA_VERSION
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
    from src.jobs.common import config as _common_config
    from src.jobs.common import datetime_utils as _common_datetime_utils
    from src.jobs.common import diagnostics as _common_diagnostics
    from src.jobs.common import fetch as _common_fetch
    from src.jobs.common import url as _common_url
    from src.jobs.text_utils import clean_text, norm_text, normalize_url
    from src.jobs_fetcher_registry import SOURCE_REPORT_META
    from src.shared.utils import env_flag, now_iso
except ModuleNotFoundError:
    # When executed via `python src/jobs_fetcher.py` from a directory that does
    # not have the repository root on sys.path, fall back to resolving the root
    # from this file location to make the `src` package importable.
    root = Path(__file__).resolve().parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from src.contracts import SCHEMA_VERSION
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
    from src.jobs.common import config as _common_config
    from src.jobs.common import datetime_utils as _common_datetime_utils
    from src.jobs.common import diagnostics as _common_diagnostics
    from src.jobs.common import fetch as _common_fetch
    from src.jobs.common import url as _common_url
    from src.jobs.text_utils import clean_text, norm_text, normalize_url
    from src.jobs_fetcher_registry import SOURCE_REPORT_META
    from src.shared.utils import env_flag, now_iso
from src.jobs.canonicalize import (
    LIGHTWEIGHT_OUTPUT_FIELDS,
    OPTIONAL_FIELDS,
    OUTPUT_FIELDS,
    REQUIRED_FIELDS,
    UNKNOWN_COMPANY_LABEL,
    map_profession,
)
from src.jobs.registry import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SOCIAL_CONFIG_PATH,
    DEFAULT_SOCIAL_LOOKBACK_MINUTES,
    DEFAULT_SOCIAL_MIN_CONFIDENCE,
    GOOGLE_SHEETS_SOURCES,
    SOURCE_REGISTRY_ACTIVE_PATH,
    SOURCE_REGISTRY_PENDING_PATH,
    load_registry_from_file,
    load_social_config,
    read_approved_since_last_run,
)


def run_pipeline(*args, **kwargs):
    """Run the jobs pipeline with the current module-level resolver hook."""
    previous = getattr(_pipeline, "build_redirect_resolver", None)
    try:
        _pipeline.build_redirect_resolver = build_redirect_resolver  # type: ignore[assignment]
        return _pipeline.run_pipeline(*args, **kwargs)
    finally:
        if previous is not None:
            _pipeline.build_redirect_resolver = previous  # type: ignore[assignment]


parse_args = _pipeline.parse_args
main = _pipeline.main
default_source_loaders = _pipeline.default_source_loaders

default_fetch_text = _transport.default_fetch_text
resolve_fetch_text_impl = _transport.resolve_fetch_text_impl
PooledRedirectResolver = _transport.PooledRedirectResolver
AsyncHttpTextFetcher = _transport.AsyncHttpTextFetcher
DEFAULT_REDIRECT_HEADERS = _transport.DEFAULT_REDIRECT_HEADERS
fetch_with_retries = _common_fetch.fetch_with_retries

parse_google_sheets_csv = _parsers.parse_google_sheets_csv
parse_gamejobs_html = _parsers.parse_gamejobs_html
parse_workwithindies_html = _parsers.parse_workwithindies_html
parse_8bitplay_html = _parsers.parse_8bitplay_html
parse_gracklehq_html = _parsers.parse_gracklehq_html
parse_remote_ok_payload = _parsers.parse_remote_ok_payload
parse_reddit_json_payload = _parsers.parse_reddit_json_payload
parse_reddit_rss_payload = _parsers.parse_reddit_rss_payload
parse_x_payload = _parsers.parse_x_payload
parse_x_rss_payload = _parsers.parse_x_rss_payload
parse_mastodon_payload = _parsers.parse_mastodon_payload
parse_gamesindustry_html = _parsers.parse_gamesindustry_html
parse_greenhouse_jobs_payload = _parsers.parse_greenhouse_jobs_payload
parse_teamtailor_listing_links = _parsers.parse_teamtailor_listing_links
parse_jobpostings_from_html = _parsers.parse_jobpostings_from_html
parse_lever_jobs_payload = _parsers.parse_lever_jobs_payload
parse_smartrecruiters_jobs_payload = _parsers.parse_smartrecruiters_jobs_payload
parse_workable_jobs_payload = _parsers.parse_workable_jobs_payload
parse_recruitee_jobs_payload = _parsers.parse_recruitee_jobs_payload
parse_pinpoint_jobs_payload = _parsers.parse_pinpoint_jobs_payload
parse_epic_games_jobs_payload = _parsers.parse_epic_games_jobs_payload
parse_ashby_jobs_from_html = _parsers.parse_ashby_jobs_from_html
parse_breezy_jobs_html = _parsers.parse_breezy_jobs_html
parse_bamboohr_jobs_html = _parsers.parse_bamboohr_jobs_html
parse_jazzhr_jobs_html = _parsers.parse_jazzhr_jobs_html
parse_personio_feed_xml = _parsers.parse_personio_feed_xml
parse_wellfound_html = _parsers.parse_wellfound_html
parse_workday_jobs_html = _parsers.parse_workday_jobs_html

normalize_source_report_row = _reporting.normalize_source_report_row
normalize_fetch_report_payload = _reporting.normalize_fetch_report_payload
normalize_runtime_payload = _reporting.normalize_runtime_payload
should_skip_source_by_ttl = _state.should_skip_source_by_ttl
should_skip_source_by_cadence = _state.should_skip_source_by_cadence

run_google_sheets_source = _community.run_google_sheets_source
run_remote_ok_source = _community.run_remote_ok_source
run_gamesindustry_source = _community.run_gamesindustry_source
run_gamejobs_source = _community.run_gamejobs_source
run_workwithindies_source = _community.run_workwithindies_source
run_8bitplay_source = _community.run_8bitplay_source
run_gracklehq_source = _community.run_gracklehq_source
run_epic_games_careers_source = _community.run_epic_games_careers_source
run_wellfound_source = _community.run_wellfound_source
google_sheet_candidate_urls = _community.google_sheet_candidate_urls
run_greenhouse_boards_source = _provider_api.run_greenhouse_boards_source
run_teamtailor_sources_source = _provider_api.run_teamtailor_sources_source
run_lever_sources_source = _provider_api.run_lever_sources_source
run_smartrecruiters_sources_source = _provider_api.run_smartrecruiters_sources_source
run_workable_sources_source = _provider_api.run_workable_sources_source
run_recruitee_sources_source = _provider_api.run_recruitee_sources_source
run_pinpoint_sources_source = _provider_api.run_pinpoint_sources_source
run_ashby_sources_source = _provider_api.run_ashby_sources_source
run_bamboohr_sources_source = _provider_api.run_bamboohr_sources_source
run_breezy_sources_source = _provider_api.run_breezy_sources_source
run_jazzhr_sources_source = _provider_api.run_jazzhr_sources_source
run_personio_sources_source = _provider_api.run_personio_sources_source
run_workday_sources_source = _provider_api.run_workday_sources_source
run_social_reddit_source = _social.run_social_reddit_source
run_social_x_source = _social.run_social_x_source
run_social_mastodon_source = _social.run_social_mastodon_source
run_static_studio_pages_source = _static.run_static_studio_pages_source
run_static_source_entry_source = _static.run_static_source_entry_source
run_static_studio_pages_a_i_source = _static.run_static_studio_pages_a_i_source
run_static_studio_pages_j_r_source = _static.run_static_studio_pages_j_r_source
run_static_studio_pages_s_z_source = _static.run_static_studio_pages_s_z_source
run_scrapy_static_source = _static.run_scrapy_static_source

SOURCE_DIAGNOSTICS = _common_diagnostics.SOURCE_DIAGNOSTICS
STUDIO_SOURCE_REGISTRY = _registry.STUDIO_SOURCE_REGISTRY
REMOTE_OK_URLS = _common_config.REMOTE_OK_URLS
GAMES_INDUSTRY_URLS = _common_config.GAMES_INDUSTRY_URLS
EPIC_CAREERS_API_URL = _common_config.EPIC_CAREERS_API_URL
GREENHOUSE_JOBS_URL_TEMPLATE = _common_config.GREENHOUSE_JOBS_URL_TEMPLATE
DEFAULT_GOOGLE_SHEET_ID = _community.DEFAULT_GOOGLE_SHEET_ID
DEFAULT_GOOGLE_SHEET_GID = _community.DEFAULT_GOOGLE_SHEET_GID
DEFAULT_TIMEOUT_S = _common_config.DEFAULT_TIMEOUT_S
DEFAULT_RETRIES = _common_config.DEFAULT_RETRIES
DEFAULT_BACKOFF_S = _common_config.DEFAULT_BACKOFF_S
DEFAULT_FETCH_STRATEGY = _common_config.DEFAULT_FETCH_STRATEGY
DEFAULT_ADAPTER_HTTP_CONCURRENCY = _common_config.DEFAULT_ADAPTER_HTTP_CONCURRENCY
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = _community.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
DEFAULT_STATIC_DETAIL_CONCURRENCY = _common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY
DEFAULT_HOT_SOURCE_CADENCE_MINUTES = _common_config.DEFAULT_HOT_SOURCE_CADENCE_MINUTES
DEFAULT_COLD_SOURCE_CADENCE_MINUTES = _common_config.DEFAULT_COLD_SOURCE_CADENCE_MINUTES
DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE = _common_config.DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE
DEFAULT_SCRAPY_VALIDATION_STRICT = _common_config.DEFAULT_SCRAPY_VALIDATION_STRICT

set_source_diagnostics = _common_diagnostics.set_source_diagnostics
fingerprint_url = _common_url.fingerprint_url
to_iso = _common_datetime_utils.to_iso
httpx = _transport.httpx


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


__all__ = [
    "AsyncHttpTextFetcher",
    "DEFAULT_ADAPTER_HTTP_CONCURRENCY",
    "DEFAULT_BACKOFF_S",
    "DEFAULT_COLD_SOURCE_CADENCE_MINUTES",
    "DEFAULT_FETCH_STRATEGY",
    "DEFAULT_GOOGLE_SHEET_GID",
    "DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY",
    "DEFAULT_GOOGLE_SHEET_ID",
    "DEFAULT_HOT_SOURCE_CADENCE_MINUTES",
    "DEFAULT_OUTPUT_DIR",
    "DEFAULT_REDIRECT_HEADERS",
    "DEFAULT_RETRIES",
    "DEFAULT_SCRAPY_VALIDATION_STRICT",
    "DEFAULT_SOCIAL_CONFIG_PATH",
    "DEFAULT_SOCIAL_LOOKBACK_MINUTES",
    "DEFAULT_SOCIAL_MIN_CONFIDENCE",
    "DEFAULT_STATIC_DETAIL_CONCURRENCY",
    "DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE",
    "DEFAULT_TIMEOUT_S",
    "GAMES_INDUSTRY_URLS",
    "GOOGLE_SHEETS_SOURCES",
    "GREENHOUSE_JOBS_URL_TEMPLATE",
    "LIGHTWEIGHT_OUTPUT_FIELDS",
    "OPTIONAL_FIELDS",
    "OUTPUT_FIELDS",
    "PooledRedirectResolver",
    "REMOTE_OK_URLS",
    "REQUIRED_FIELDS",
    "SCHEMA_VERSION",
    "SOURCE_DIAGNOSTICS",
    "SOURCE_REGISTRY_ACTIVE_PATH",
    "SOURCE_REGISTRY_PENDING_PATH",
    "SOURCE_REPORT_META",
    "STUDIO_SOURCE_REGISTRY",
    "UNKNOWN_COMPANY_LABEL",
    "build_redirect_resolver",
    "clean_text",
    "datetime",
    "default_fetch_text",
    "default_source_loaders",
    "env_flag",
    "fetch_with_retries",
    "fingerprint_url",
    "google_sheet_candidate_urls",
    "httpx",
    "load_registry_from_file",
    "load_social_config",
    "main",
    "map_profession",
    "maybe_fetch_kojima_job_listing_html",
    "norm_text",
    "normalize_fetch_report_payload",
    "normalize_runtime_payload",
    "normalize_source_report_row",
    "normalize_url",
    "now_iso",
    "parse_args",
    "parse_ashby_jobs_from_html",
    "parse_bamboohr_jobs_html",
    "parse_breezy_jobs_html",
    "parse_epic_games_jobs_payload",
    "parse_gamesindustry_html",
    "parse_gamejobs_html",
    "parse_8bitplay_html",
    "parse_google_sheets_csv",
    "parse_gracklehq_html",
    "parse_greenhouse_jobs_payload",
    "parse_jobpostings_from_html",
    "parse_lever_jobs_payload",
    "parse_mastodon_payload",
    "parse_pinpoint_jobs_payload",
    "parse_personio_feed_xml",
    "parse_reddit_json_payload",
    "parse_reddit_rss_payload",
    "parse_recruitee_jobs_payload",
    "parse_remote_ok_payload",
    "parse_jazzhr_jobs_html",
    "parse_smartrecruiters_jobs_payload",
    "parse_teamtailor_listing_links",
    "parse_wellfound_html",
    "parse_workday_jobs_html",
    "parse_workable_jobs_payload",
    "parse_workwithindies_html",
    "parse_x_payload",
    "parse_x_rss_payload",
    "read_approved_since_last_run",
    "registry_entries",
    "re",
    "resolve_fetch_text_impl",
    "subprocess",
    "run_ashby_sources_source",
    "run_bamboohr_sources_source",
    "run_breezy_sources_source",
    "run_8bitplay_source",
    "run_epic_games_careers_source",
    "run_gamesindustry_source",
    "run_gamejobs_source",
    "run_google_sheets_source",
    "run_greenhouse_boards_source",
    "run_lever_sources_source",
    "run_jazzhr_sources_source",
    "run_pinpoint_sources_source",
    "run_personio_sources_source",
    "run_workday_sources_source",
    "run_pipeline",
    "run_gracklehq_source",
    "run_recruitee_sources_source",
    "run_remote_ok_source",
    "run_scrapy_static_source",
    "run_smartrecruiters_sources_source",
    "run_social_mastodon_source",
    "run_social_reddit_source",
    "run_social_x_source",
    "run_static_source_entry_source",
    "run_static_studio_pages_a_i_source",
    "run_static_studio_pages_j_r_source",
    "run_static_studio_pages_s_z_source",
    "run_static_studio_pages_source",
    "run_teamtailor_sources_source",
    "run_wellfound_source",
    "run_workable_sources_source",
    "run_workwithindies_source",
    "set_source_diagnostics",
    "should_skip_source_by_cadence",
    "should_skip_source_by_ttl",
    "timedelta",
    "timezone",
    "to_iso",
    "urlopen",
]

if __name__ == "__main__":
    raise SystemExit(_pipeline.main())
