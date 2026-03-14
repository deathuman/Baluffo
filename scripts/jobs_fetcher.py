#!/usr/bin/env python3
"""Compatibility wrapper for the refactored jobs pipeline package."""

from __future__ import annotations

import subprocess

from scripts.jobs import common as _common
from scripts.jobs import dedup as _dedup
from scripts.jobs import parsers as _parsers
from scripts.jobs import pipeline as _pipeline
from scripts.jobs import registry as _registry
from scripts.jobs import reporting as _reporting
from scripts.jobs import state as _state
from scripts.jobs import transport as _transport
from scripts.jobs.adapters import community as _community
from scripts.jobs.adapters import provider_api as _provider_api
from scripts.jobs.adapters import social as _social
from scripts.jobs.adapters import static as _static
from scripts.jobs.canonicalize import (
    LIGHTWEIGHT_OUTPUT_FIELDS,
    OPTIONAL_FIELDS,
    OUTPUT_FIELDS,
    REQUIRED_FIELDS,
    UNKNOWN_COMPANY_LABEL,
    map_profession,
)
from scripts.jobs.registry import (
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

run_pipeline = _pipeline.run_pipeline
parse_args = _pipeline.parse_args
main = _pipeline.main
default_source_loaders = _pipeline.default_source_loaders

default_fetch_text = _transport.default_fetch_text
resolve_fetch_text_impl = _transport.resolve_fetch_text_impl
PooledRedirectResolver = _transport.PooledRedirectResolver
AsyncHttpTextFetcher = _transport.AsyncHttpTextFetcher
DEFAULT_REDIRECT_HEADERS = _transport.DEFAULT_REDIRECT_HEADERS
fetch_with_retries = _common.fetch_with_retries

parse_google_sheets_csv = _parsers.parse_google_sheets_csv
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
parse_ashby_jobs_from_html = _parsers.parse_ashby_jobs_from_html
parse_personio_feed_xml = _parsers.parse_personio_feed_xml
parse_wellfound_html = _common.parse_wellfound_html

normalize_source_report_row = _reporting.normalize_source_report_row
normalize_fetch_report_payload = _reporting.normalize_fetch_report_payload
normalize_runtime_payload = _reporting.normalize_runtime_payload
should_skip_source_by_ttl = _state.should_skip_source_by_ttl
should_skip_source_by_cadence = _state.should_skip_source_by_cadence

run_google_sheets_source = _community.run_google_sheets_source
run_remote_ok_source = _community.run_remote_ok_source
run_gamesindustry_source = _community.run_gamesindustry_source
run_epic_games_careers_source = _community.run_epic_games_careers_source
run_wellfound_source = _community.run_wellfound_source
google_sheet_candidate_urls = _community.google_sheet_candidate_urls
run_greenhouse_boards_source = _provider_api.run_greenhouse_boards_source
run_teamtailor_sources_source = _provider_api.run_teamtailor_sources_source
run_lever_sources_source = _provider_api.run_lever_sources_source
run_smartrecruiters_sources_source = _provider_api.run_smartrecruiters_sources_source
run_workable_sources_source = _provider_api.run_workable_sources_source
run_ashby_sources_source = _provider_api.run_ashby_sources_source
run_personio_sources_source = _provider_api.run_personio_sources_source
run_social_reddit_source = _social.run_social_reddit_source
run_social_x_source = _social.run_social_x_source
run_social_mastodon_source = _social.run_social_mastodon_source
run_static_studio_pages_source = _static.run_static_studio_pages_source
run_static_source_entry_source = _static.run_static_source_entry_source
run_static_studio_pages_a_i_source = _static.run_static_studio_pages_a_i_source
run_static_studio_pages_j_r_source = _static.run_static_studio_pages_j_r_source
run_static_studio_pages_s_z_source = _static.run_static_studio_pages_s_z_source
run_scrapy_static_source = _static.run_scrapy_static_source

SCHEMA_VERSION = _common.SCHEMA_VERSION
SOURCE_DIAGNOSTICS = _common.SOURCE_DIAGNOSTICS
SOURCE_REPORT_META = _common.SOURCE_REPORT_META
STUDIO_SOURCE_REGISTRY = _common.STUDIO_SOURCE_REGISTRY
REMOTE_OK_URLS = _common.REMOTE_OK_URLS
GAMES_INDUSTRY_URLS = _common.GAMES_INDUSTRY_URLS
GREENHOUSE_JOBS_URL_TEMPLATE = _common.GREENHOUSE_JOBS_URL_TEMPLATE
DEFAULT_GOOGLE_SHEET_ID = _common.DEFAULT_GOOGLE_SHEET_ID
DEFAULT_GOOGLE_SHEET_GID = _common.DEFAULT_GOOGLE_SHEET_GID
DEFAULT_TIMEOUT_S = _common.DEFAULT_TIMEOUT_S
DEFAULT_RETRIES = _common.DEFAULT_RETRIES
DEFAULT_BACKOFF_S = _common.DEFAULT_BACKOFF_S
DEFAULT_FETCH_STRATEGY = _common.DEFAULT_FETCH_STRATEGY
DEFAULT_ADAPTER_HTTP_CONCURRENCY = _common.DEFAULT_ADAPTER_HTTP_CONCURRENCY
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = _common.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
DEFAULT_STATIC_DETAIL_CONCURRENCY = _common.DEFAULT_STATIC_DETAIL_CONCURRENCY
DEFAULT_HOT_SOURCE_CADENCE_MINUTES = _common.DEFAULT_HOT_SOURCE_CADENCE_MINUTES
DEFAULT_COLD_SOURCE_CADENCE_MINUTES = _common.DEFAULT_COLD_SOURCE_CADENCE_MINUTES
DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE = _common.DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE
DEFAULT_SCRAPY_VALIDATION_STRICT = _common.DEFAULT_SCRAPY_VALIDATION_STRICT

set_source_diagnostics = _common.set_source_diagnostics
clean_text = _common.clean_text
norm_text = _common.norm_text
normalize_url = _common.normalize_url
fingerprint_url = _common.fingerprint_url
to_iso = _common.to_iso
now_iso = _common.now_iso
env_flag = _common.env_flag

datetime = _common.datetime
timedelta = _common.timedelta
timezone = _common.timezone
re = _common.re
urlopen = _common.urlopen
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


def _legacy_row(value):
    return value.to_dict() if hasattr(value, "to_dict") else value


def build_redirect_resolver(*args, **kwargs):
    previous_httpx = _transport.httpx
    _transport.httpx = httpx
    try:
        return _transport.build_redirect_resolver(*args, **kwargs)
    finally:
        _transport.httpx = previous_httpx


def maybe_fetch_kojima_job_listing_html(*args, **kwargs):
    _common.urlopen = urlopen
    return _common.maybe_fetch_kojima_job_listing_html(*args, **kwargs)


def canonicalize_job(*args, **kwargs):
    job = _common.canonicalize_job(*args, **kwargs)
    return _legacy_row(job) if job is not None else None


def canonicalize_job_with_reason(*args, **kwargs):
    job, reason = _common.canonicalize_job_with_reason(*args, **kwargs)
    return (_legacy_row(job) if job is not None else None), reason


def canonicalize_google_sheets_rows(*args, **kwargs):
    rows, drop_reasons, stats = _common.canonicalize_google_sheets_rows(*args, **kwargs)
    return [_legacy_row(row) for row in rows], drop_reasons, stats


def deduplicate_jobs(*args, **kwargs):
    rows, stats = _dedup.deduplicate_jobs(*args, **kwargs)
    return [_legacy_row(row) for row in rows], stats

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
    "canonicalize_google_sheets_rows",
    "canonicalize_job",
    "canonicalize_job_with_reason",
    "clean_text",
    "datetime",
    "deduplicate_jobs",
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
    "parse_gamesindustry_html",
    "parse_google_sheets_csv",
    "parse_greenhouse_jobs_payload",
    "parse_jobpostings_from_html",
    "parse_lever_jobs_payload",
    "parse_mastodon_payload",
    "parse_personio_feed_xml",
    "parse_reddit_json_payload",
    "parse_reddit_rss_payload",
    "parse_remote_ok_payload",
    "parse_smartrecruiters_jobs_payload",
    "parse_teamtailor_listing_links",
    "parse_wellfound_html",
    "parse_workable_jobs_payload",
    "parse_x_payload",
    "parse_x_rss_payload",
    "read_approved_since_last_run",
    "registry_entries",
    "re",
    "resolve_fetch_text_impl",
    "subprocess",
    "run_ashby_sources_source",
    "run_epic_games_careers_source",
    "run_gamesindustry_source",
    "run_google_sheets_source",
    "run_greenhouse_boards_source",
    "run_lever_sources_source",
    "run_personio_sources_source",
    "run_pipeline",
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
