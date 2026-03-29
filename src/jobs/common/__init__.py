#!/usr/bin/env python3
"""Aggregate game job listings into unified JSON/CSV feeds."""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import os
import re
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from html import unescape
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

try:
    import httpx
except Exception:  # noqa: BLE001
    httpx = None

from src.baluffo_config import get_storage_defaults
from src.contracts import SCHEMA_VERSION
from src.jobs.adapters import html_parsers as _html_parsers
from src.jobs.game_detection import GAME_KEYWORDS, looks_like_game_job
from src.jobs.normalizers import (
    COUNTRY_NAME_TO_CODE,
    normalize_country,
    normalize_sector,
    normalize_work_type,
)
from src.jobs.text_utils import (
    TRACKING_QUERY_KEYS,
    clean_text,
    norm_text,
    normalize_url,
)
from src.jobs_fetcher_registry import (
    DEFAULT_SOURCE_LOADER_NAMES,
    EXCLUDED_DEFAULT_SOURCES,
    SOURCE_REPORT_META,
)
from src.pipeline_io import (
    read_existing_output as read_existing_output_from_file,
)
from src.pipeline_io import (
    serialize_rows_for_csv,
    serialize_rows_for_json,
    write_atomic_if_changed,
    write_text_if_changed,
)
from src.shared.regex import find_urls_in_text
from src.shared.utils import env_flag as _shared_env_flag
from src.shared.utils import now_iso as _shared_now_iso

RawJob = dict[str, Any]
SourceLoader = Callable[..., list[RawJob]]
from src.jobs.common import (
    config,
    diagnostics,
    fetch,
    heuristics,
    http,
    parsing,
    registry_defaults,
    social,
    sources,
    url,
)
from src.jobs.common import config as _common_config
from src.jobs.common import diagnostics as _common_diagnostics
from src.jobs.common import heuristics as _common_heuristics
from src.jobs.common import registry_defaults as _common_registry_defaults
from src.jobs.common import social as _common_social
from src.jobs.common import sources as _common_sources
from src.jobs.common.registry_defaults import (
    DEFAULT_STUDIO_SOURCE_REGISTRY,
    REDUNDANT_STATIC_IF_PROVIDER,
)

DEFAULT_TIMEOUT_S = _common_config.DEFAULT_TIMEOUT_S
DEFAULT_RETRIES = _common_config.DEFAULT_RETRIES
DEFAULT_BACKOFF_S = _common_config.DEFAULT_BACKOFF_S
DEFAULT_FETCH_STRATEGY = _common_config.DEFAULT_FETCH_STRATEGY
DEFAULT_ADAPTER_HTTP_CONCURRENCY = _common_config.DEFAULT_ADAPTER_HTTP_CONCURRENCY
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = (
    _common_config.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
)
DEFAULT_HOT_SOURCE_CADENCE_MINUTES = _common_config.DEFAULT_HOT_SOURCE_CADENCE_MINUTES
DEFAULT_COLD_SOURCE_CADENCE_MINUTES = _common_config.DEFAULT_COLD_SOURCE_CADENCE_MINUTES
UNKNOWN_COMPANY_LABEL = _common_config.UNKNOWN_COMPANY_LABEL
UNTRUSTWORTHY_COMPANY_LABELS = _common_config.UNTRUSTWORTHY_COMPANY_LABELS
DEFAULT_OUTPUT_DIR = _common_config.DEFAULT_OUTPUT_DIR
DEFAULT_SOCIAL_CONFIG_PATH = _common_config.DEFAULT_SOCIAL_CONFIG_PATH
DEFAULT_SOCIAL_LOOKBACK_MINUTES = _common_config.DEFAULT_SOCIAL_LOOKBACK_MINUTES
DEFAULT_SOCIAL_MIN_CONFIDENCE = _common_config.DEFAULT_SOCIAL_MIN_CONFIDENCE
GREENHOUSE_JOBS_URL_TEMPLATE = _common_config.GREENHOUSE_JOBS_URL_TEMPLATE
REMOTE_OK_URLS = _common_config.REMOTE_OK_URLS
GAMES_INDUSTRY_URLS = _common_config.GAMES_INDUSTRY_URLS
EPIC_CAREERS_API_URL = _common_config.EPIC_CAREERS_API_URL
WELLFOUND_URLS = _common_config.WELLFOUND_URLS
DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE = _common_config.DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE
DEFAULT_STATIC_DETAIL_CONCURRENCY = _common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY
DEFAULT_SCRAPY_VALIDATION_STRICT = _common_config.DEFAULT_SCRAPY_VALIDATION_STRICT
DEFAULT_CANONICAL_STRICT_URL = _common_config.DEFAULT_CANONICAL_STRICT_URL
SOURCE_REGISTRY_ACTIVE_PATH = _common_config.SOURCE_REGISTRY_ACTIVE_PATH
SOURCE_REGISTRY_PENDING_PATH = _common_config.SOURCE_REGISTRY_PENDING_PATH
SOURCE_APPROVAL_STATE_PATH = _common_config.SOURCE_APPROVAL_STATE_PATH
SCRAPY_BROWSER_QUEUE_PATH = _common_config.SCRAPY_BROWSER_QUEUE_PATH
STATIC_CLASSIFICATIONS_FOR_BROWSER_QUEUE = _common_config.STATIC_CLASSIFICATIONS_FOR_BROWSER_QUEUE
REQUIRED_FIELDS = _common_config.REQUIRED_FIELDS
OPTIONAL_FIELDS = _common_config.OPTIONAL_FIELDS
OUTPUT_FIELDS = _common_config.OUTPUT_FIELDS
LIGHTWEIGHT_OUTPUT_FIELDS = _common_config.LIGHTWEIGHT_OUTPUT_FIELDS
SUPPORTED_REDIRECT_HOSTS = _common_config.SUPPORTED_REDIRECT_HOSTS
DEFAULT_HTTP_HEADERS = _common_config.DEFAULT_HTTP_HEADERS
DEFAULT_REDIRECT_HEADERS = _common_config.DEFAULT_REDIRECT_HEADERS
LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS = _common_config.LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS
LIFECYCLE_ARCHIVE_RETENTION_DAYS = _common_config.LIFECYCLE_ARCHIVE_RETENTION_DAYS
TARGET_PROFESSIONS = _common_config.TARGET_PROFESSIONS
SOURCE_DIAGNOSTICS = _common_diagnostics.SOURCE_DIAGNOSTICS

SOCIAL_SOURCE_NAMES = _common_social.SOCIAL_SOURCE_NAMES
DEFAULT_SOCIAL_CONFIG = _common_social.DEFAULT_SOCIAL_CONFIG
DEFAULT_STUDIO_SOURCE_REGISTRY = _common_registry_defaults.DEFAULT_STUDIO_SOURCE_REGISTRY
REDUNDANT_STATIC_IF_PROVIDER = _common_registry_defaults.REDUNDANT_STATIC_IF_PROVIDER
load_registry_from_file = _common_sources.load_registry_from_file
read_approved_since_last_run = _common_sources.read_approved_since_last_run
load_studio_source_registry = _common_sources.load_studio_source_registry
STUDIO_SOURCE_REGISTRY = load_studio_source_registry(DEFAULT_STUDIO_SOURCE_REGISTRY)

# Heuristics/scoring helpers remain available from the common barrel.
classify_company_type = _common_heuristics.classify_company_type
map_profession = _common_heuristics.map_profession
is_untrustworthy_company_label = _common_heuristics.is_untrustworthy_company_label
normalize_company_value = _common_heuristics.normalize_company_value
compute_quality_score = _common_heuristics.compute_quality_score
title_has_focus_role = _common_heuristics.title_has_focus_role
compute_focus_score = _common_heuristics.compute_focus_score

PREFERRED_IMPORT_SURFACES = [
    "config",
    "diagnostics",
    "heuristics",
    "http",
    "parsing",
    "registry_defaults",
    "social",
    "sources",
    "url",
    "fetch",
]

__all__ = [*PREFERRED_IMPORT_SURFACES]


from src.jobs.common import registry as _common_registry


def registry_entries(adapter: str, *, enabled_only: bool = True) -> list[dict[str, Any]]:
    return _common_registry.registry_entries(
        adapter,
        enabled_only=enabled_only,
        studio_source_registry=STUDIO_SOURCE_REGISTRY,
        redundant_static_rules=REDUNDANT_STATIC_IF_PROVIDER,
    )


set_source_diagnostics = _common_diagnostics.set_source_diagnostics


def now_iso() -> str:
    return _shared_now_iso()


def env_flag(name: str, default: bool) -> bool:
    return _shared_env_flag(name, default)


def _deep_merge_dicts(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {key: value for key, value in base.items()}
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dicts(dict(merged[key]), value)
        else:
            merged[key] = value
    return merged


def load_social_config(
    *,
    config_path: Path,
    enabled: bool = False,
    lookback_minutes: int = DEFAULT_SOCIAL_LOOKBACK_MINUTES,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    try:
        if config_path.exists():
            parsed = json.loads(config_path.read_text(encoding="utf-8"))
            if isinstance(parsed, dict):
                payload = parsed
    except (OSError, json.JSONDecodeError):
        payload = {}
    merged = _deep_merge_dicts(DEFAULT_SOCIAL_CONFIG, payload)
    merged["enabled"] = bool(enabled)
    merged["lookbackMinutes"] = max(1, int(lookback_minutes or DEFAULT_SOCIAL_LOOKBACK_MINUTES))
    merged["minConfidence"] = max(
        0, min(100, int(merged.get("minConfidence") or DEFAULT_SOCIAL_MIN_CONFIDENCE))
    )
    merged["rejectForHirePosts"] = bool(merged.get("rejectForHirePosts", True))
    return merged


from src.jobs.common import url as _common_url
from src.jobs.common.datetime_utils import parse_datetime, posted_ts, to_iso

canonical_url_fingerprint_seed = _common_url.canonical_url_fingerprint_seed
fingerprint_url = _common_url.fingerprint_url
is_supported_redirect_url = _common_url.is_supported_redirect_url
resolve_supported_redirect_url = _common_url.resolve_supported_redirect_url


class PooledRedirectResolver:
    def __new__(
        cls,
        *,
        timeout_s: int = DEFAULT_TIMEOUT_S,
        max_connections: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
    ):
        from src.jobs import transport as transport_pkg

        return transport_pkg.PooledRedirectResolver(
            timeout_s=timeout_s,
            max_connections=max_connections,
        )


def normalize_contract_type(contract_text: Any, title: Any = "") -> str:
    return parsing.normalize_contract_type(contract_text, title)


def classify_company_type(
    company: Any,
    title: Any = "",
    source: Any = "",
    job_link: Any = "",
    source_bundle: Any = None,
) -> str:
    return _common_heuristics.classify_company_type(
        company,
        title,
        source,
        job_link,
        source_bundle,
    )


def map_profession(title: Any) -> str:
    return _common_heuristics.map_profession(title)


def is_untrustworthy_company_label(value: str) -> bool:
    return _common_heuristics.is_untrustworthy_company_label(value)


def normalize_company_value(value: Any) -> str:
    return _common_heuristics.normalize_company_value(value)


def parse_remote_ok_payload(payload: Any) -> list[RawJob]:
    return parsing.parse_remote_ok_payload(payload, looks_like_game_job=looks_like_game_job)


# Shared adapter and helper re-exports live below this point.

# HTML/listing parsers moved to src.jobs.adapters.html_parsers; re-exported for backward compat.
extract_json_ld_blocks = _html_parsers.extract_json_ld_blocks
strip_html_text = _html_parsers.strip_html_text
parse_gamesindustry_changed_date = _html_parsers.parse_gamesindustry_changed_date
iter_job_postings_from_jsonld = _html_parsers.iter_job_postings_from_jsonld
parse_jobposting_locations = _html_parsers.parse_jobposting_locations
parse_jobposting_company = _html_parsers.parse_jobposting_company
parse_jobposting_source_id = _html_parsers.parse_jobposting_source_id
parse_jobpostings_from_html = _html_parsers.parse_jobpostings_from_html
maybe_fetch_kojima_job_listing_html = _html_parsers.maybe_fetch_kojima_job_listing_html
parse_teamtailor_listing_links = _html_parsers.parse_teamtailor_listing_links
parse_gamesindustry_html = _html_parsers.parse_gamesindustry_html
parse_wellfound_html = _html_parsers.parse_wellfound_html


def default_fetch_text(url: str, timeout_s: int) -> str:
    return http.default_fetch_text(url, timeout_s, headers=DEFAULT_HTTP_HEADERS)


class AsyncHttpTextFetcher:
    def __new__(cls, *, max_connections: int = DEFAULT_ADAPTER_HTTP_CONCURRENCY):
        from src.jobs import transport as transport_pkg

        return transport_pkg.AsyncHttpTextFetcher(max_connections=max_connections)


def resolve_fetch_text_impl(
    *,
    fetch_text: Callable[[str, int], str],
    fetch_strategy: str,
    adapter_http_concurrency: int,
) -> tuple[Callable[[str, int], str], str, AsyncHttpTextFetcher | None]:
    from src.jobs import transport as transport_pkg

    return transport_pkg.resolve_fetch_text_impl(
        fetch_text=fetch_text,
        fetch_strategy=fetch_strategy,
        adapter_http_concurrency=adapter_http_concurrency,
    )


from src.jobs.common import fetch as _common_fetch

fetch_with_retries = _common_fetch.fetch_with_retries


from src.jobs.common.legacy_runners import (
    _request_json_with_headers,
    build_static_source_loaders,
    run_ashby_sources_source,
    run_epic_games_careers_source,
    run_gamesindustry_source,
    run_greenhouse_boards_source,
    run_lever_sources_source,
    run_personio_sources_source,
    run_remote_ok_source,
    run_scrapy_static_source,
    run_smartrecruiters_sources_source,
    run_social_mastodon_source,
    run_social_reddit_source,
    run_social_x_source,
    run_static_source_entry_source,
    run_static_studio_pages_a_i_source,
    run_static_studio_pages_j_r_source,
    run_static_studio_pages_s_z_source,
    run_static_studio_pages_source,
    run_teamtailor_sources_source,
    run_wellfound_source,
    run_workable_sources_source,
    static_source_shard,
)


def compute_quality_score(job: RawJob) -> int:
    return _common_heuristics.compute_quality_score(job)


def title_has_focus_role(title: Any) -> bool:
    return _common_heuristics.title_has_focus_role(title)


def compute_focus_score(job: RawJob) -> int:
    return _common_heuristics.compute_focus_score(job)


def dedup_secondary_key(job: RawJob) -> str:
    return "|".join(
        [
            norm_text(job.get("company")),
            norm_text(job.get("title")),
            norm_text(job.get("city")),
            norm_text(job.get("country")),
        ]
    )


def record_richness(job: RawJob) -> int:
    fields = [
        "title",
        "company",
        "city",
        "country",
        "workType",
        "contractType",
        "jobLink",
        "sector",
        "profession",
        "sourceJobId",
        "postedAt",
    ]
    return sum(1 for field in fields if clean_text(job.get(field)))


def company_preference_score(job: RawJob) -> int:
    company = clean_text(job.get("company"))
    if not company:
        return 0
    if norm_text(company) in {norm_text(UNKNOWN_COMPANY_LABEL), "unknown"}:
        return 1
    return 2


def choose_base_record(left: RawJob, right: RawJob) -> tuple[RawJob, RawJob]:
    from src.jobs import dedup as dedup_pkg

    base, other = dedup_pkg.choose_base_record(
        dedup_pkg.CanonicalJob.from_mapping(left),
        dedup_pkg.CanonicalJob.from_mapping(right),
    )
    return base.to_dict(), other.to_dict()


def merge_records(existing: RawJob, candidate: RawJob) -> RawJob:
    from src.jobs import dedup as dedup_pkg

    return dedup_pkg.merge_records(
        dedup_pkg.CanonicalJob.from_mapping(existing),
        dedup_pkg.CanonicalJob.from_mapping(candidate),
    ).to_dict()


def default_source_loaders(
    *,
    social_enabled: bool = False,
    social_config: dict[str, Any] | None = None,
) -> list[tuple[str, SourceLoader]]:
    from src.jobs.adapters import default_source_loaders as package_default_source_loaders

    return package_default_source_loaders(
        social_enabled=social_enabled,
        social_config=social_config,
    )


def format_source_error(source_name: str, error: Any) -> str:
    message = clean_text(str(error))
    prefix = f"{clean_text(source_name)}:"
    if not message:
        return "unknown error"
    if message.lower().startswith(prefix.lower()):
        return message
    return f"{source_name}: {message}"


def build_pipeline_summary(
    dedup_stats: dict[str, int],
    deduped_rows: Sequence[RawJob],
    source_reports: Sequence[dict[str, Any]],
    canonical_count: int,
    preserved_previous: bool,
    active_source_count: int,
    pending_source_count: int,
    newly_approved_since_last_run: int,
    *,
    json_bytes: int,
    csv_bytes: int,
    light_json_bytes: int,
    lifecycle_counts_map: dict[str, int] | None = None,
) -> dict[str, Any]:
    from src.jobs import reporting as reporting_pkg

    return reporting_pkg.build_pipeline_summary(
        dedup_stats,
        [reporting_pkg.CanonicalJob.from_mapping(row) for row in deduped_rows],
        source_reports,
        canonical_count,
        preserved_previous,
        active_source_count,
        pending_source_count,
        newly_approved_since_last_run,
        json_bytes=json_bytes,
        csv_bytes=csv_bytes,
        light_json_bytes=light_json_bytes,
        lifecycle_counts_map=lifecycle_counts_map,
    )


def build_browser_fallback_queue(
    source_reports: Sequence[dict[str, Any]],
    *,
    generated_at: str,
) -> list[dict[str, Any]]:
    from src.jobs import reporting as reporting_pkg

    return reporting_pkg.build_browser_fallback_queue(source_reports, generated_at=generated_at)


def build_parser_regression_queue(
    source_reports: Sequence[dict[str, Any]],
    *,
    generated_at: str,
    resolve_redirect_url: Callable[[str], str] | None = None,
) -> list[dict[str, Any]]:
    from src.jobs import reporting as reporting_pkg

    return reporting_pkg.build_parser_regression_queue(
        source_reports,
        generated_at=generated_at,
        resolve_redirect_url=resolve_redirect_url,
    )


def read_previously_successful_sources(report_path: Path) -> set[str]:
    from src.jobs import state as state_pkg

    return state_pkg.read_previously_successful_sources(report_path)


def read_success_cache(cache_path: Path) -> set[str]:
    from src.jobs import state as state_pkg

    return state_pkg.read_success_cache(cache_path)


def write_success_cache(cache_path: Path, source_reports: Sequence[dict[str, Any]]) -> None:
    from src.jobs import state as state_pkg

    state_pkg.write_success_cache(cache_path, source_reports)


def source_rows_fingerprint(rows: Sequence[RawJob]) -> str:
    from src.jobs import state as state_pkg

    return state_pkg.source_rows_fingerprint(rows)


from src.jobs.common.numbers import _clamped_int


def normalize_source_state_payload(
    payload: dict[str, Any], *, updated_at: str = ""
) -> dict[str, Any]:
    from src.jobs import state as state_pkg

    return state_pkg.normalize_source_state_payload(payload, updated_at=updated_at)


def read_source_state(state_path: Path) -> dict[str, dict[str, Any]]:
    from src.jobs import state as state_pkg

    return state_pkg.read_source_state(state_path)


def write_source_state(state_path: Path, rows: dict[str, dict[str, Any]]) -> None:
    from src.jobs import state as state_pkg

    state_pkg.write_source_state(state_path, rows)


def _job_identity_key(job: dict[str, Any]) -> str:
    dedup = clean_text(job.get("dedupKey"))
    if dedup:
        return dedup
    link_fp = fingerprint_url(job.get("jobLink"))
    if link_fp:
        return f"url:{link_fp}"
    secondary = dedup_secondary_key(job)
    if secondary:
        return f"secondary:{hashlib.sha1(secondary.encode('utf-8')).hexdigest()}"
    return ""


def normalize_job_lifecycle_payload(
    payload: dict[str, Any], *, updated_at: str = ""
) -> dict[str, Any]:
    from src.jobs import state as state_pkg

    return state_pkg.normalize_job_lifecycle_payload(payload, updated_at=updated_at)


def read_job_lifecycle_state(state_path: Path) -> dict[str, dict[str, Any]]:
    from src.jobs import state as state_pkg

    return state_pkg.read_job_lifecycle_state(state_path)


def write_job_lifecycle_state(state_path: Path, rows: dict[str, dict[str, Any]]) -> None:
    from src.jobs import state as state_pkg

    state_pkg.write_job_lifecycle_state(state_path, rows)


def lifecycle_counts(rows: dict[str, dict[str, Any]]) -> dict[str, int]:
    from src.jobs import state as state_pkg

    return state_pkg.lifecycle_counts(rows)


def apply_job_lifecycle_state(
    *,
    deduped_rows: list[RawJob],
    lifecycle_rows: dict[str, dict[str, Any]],
    finished_at: str,
    allow_mark_missing: bool,
    eligible_missing_sources: set[str] | None = None,
    remove_to_archive_days: int = LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS,
    archive_retention_days: int = LIFECYCLE_ARCHIVE_RETENTION_DAYS,
) -> tuple[list[RawJob], dict[str, dict[str, Any]], dict[str, int]]:
    from src.jobs import state as state_pkg

    rows, next_rows, counts = state_pkg.apply_job_lifecycle_state(
        deduped_rows=[state_pkg.CanonicalJob.from_mapping(row) for row in deduped_rows],
        lifecycle_rows=lifecycle_rows,
        finished_at=finished_at,
        allow_mark_missing=allow_mark_missing,
        eligible_missing_sources=eligible_missing_sources,
        remove_to_archive_days=remove_to_archive_days,
        archive_retention_days=archive_retention_days,
    )
    return [row.to_dict() for row in rows], next_rows, counts


def normalize_runtime_payload(
    runtime: dict[str, Any], *, selected_source_count: int
) -> dict[str, Any]:
    src = runtime if isinstance(runtime, dict) else {}
    normalized = {
        "maxWorkers": _clamped_int(src.get("maxWorkers"), 1, 1),
        "maxPerDomain": _clamped_int(src.get("maxPerDomain"), 1, 1),
        "fetchStrategy": clean_text(src.get("fetchStrategy")) or DEFAULT_FETCH_STRATEGY,
        "fetchClient": clean_text(src.get("fetchClient")) or "urllib",
        "adapterHttpConcurrency": _clamped_int(
            src.get("adapterHttpConcurrency"), DEFAULT_ADAPTER_HTTP_CONCURRENCY, 1
        ),
        "staticDetailConcurrency": _clamped_int(
            src.get("staticDetailConcurrency"), DEFAULT_STATIC_DETAIL_CONCURRENCY, 1
        ),
        "googleSheetsRedirectConcurrency": _clamped_int(
            src.get("googleSheetsRedirectConcurrency"),
            DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
            1,
        ),
        "seedFromExistingOutput": bool(src.get("seedFromExistingOutput")),
        "sourceTtlMinutes": _clamped_int(src.get("sourceTtlMinutes"), 0, 0),
        "respectSourceCadence": bool(src.get("respectSourceCadence")),
        "hotSourceCadenceMinutes": _clamped_int(
            src.get("hotSourceCadenceMinutes"), DEFAULT_HOT_SOURCE_CADENCE_MINUTES, 1
        ),
        "coldSourceCadenceMinutes": _clamped_int(
            src.get("coldSourceCadenceMinutes"), DEFAULT_COLD_SOURCE_CADENCE_MINUTES, 1
        ),
        "circuitBreakerFailures": _clamped_int(src.get("circuitBreakerFailures"), 0, 0),
        "circuitBreakerCooldownMinutes": _clamped_int(
            src.get("circuitBreakerCooldownMinutes"), 0, 0
        ),
        "ignoreCircuitBreaker": bool(src.get("ignoreCircuitBreaker")),
        "socialEnabled": bool(src.get("socialEnabled")),
        "socialConfigPath": clean_text(src.get("socialConfigPath")),
        "socialLookbackMinutes": _clamped_int(
            src.get("socialLookbackMinutes"), DEFAULT_SOCIAL_LOOKBACK_MINUTES, 1
        ),
        "socialMinConfidence": _clamped_int(
            src.get("socialMinConfidence"), DEFAULT_SOCIAL_MIN_CONFIDENCE, 0
        ),
        "staticDetailHeuristicsProfile": clean_text(src.get("staticDetailHeuristicsProfile"))
        or DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE,
        "scrapyValidationStrict": bool(
            src.get("scrapyValidationStrict")
            if isinstance(src.get("scrapyValidationStrict"), bool)
            else DEFAULT_SCRAPY_VALIDATION_STRICT
        ),
        "canonicalStrictUrlValidation": bool(
            src.get("canonicalStrictUrlValidation")
            if isinstance(src.get("canonicalStrictUrlValidation"), bool)
            else DEFAULT_CANONICAL_STRICT_URL
        ),
        "selectedSourceCount": _clamped_int(
            src.get("selectedSourceCount"), selected_source_count, 0
        ),
    }
    slowest_sources = src.get("slowestSources")
    if isinstance(slowest_sources, list):
        normalized["slowestSources"] = [
            {
                "name": clean_text(item.get("name")),
                "adapter": clean_text(item.get("adapter")),
                "durationMs": _clamped_int(item.get("durationMs"), 0, 0),
                "keptCount": _clamped_int(item.get("keptCount"), 0, 0),
                "detailPagesVisited": _clamped_int(item.get("detailPagesVisited"), 0, 0),
                "detailYieldPct": _clamped_int(item.get("detailYieldPct"), 0, 0),
            }
            for item in slowest_sources
            if isinstance(item, dict) and clean_text(item.get("name"))
        ][:10]
    return normalized


from src.jobs.common.contracts import (
    normalize_fetch_report_payload,
    normalize_runtime_payload,
    normalize_source_report_row,
    normalize_task_state_payload,
)


def should_skip_source_by_ttl(
    source_name: str, state_rows: dict[str, dict[str, Any]], ttl_minutes: int
) -> bool:
    from src.jobs import state as state_pkg

    return state_pkg.should_skip_source_by_ttl(source_name, state_rows, ttl_minutes)


def should_skip_source_by_cadence(
    source_name: str,
    state_rows: dict[str, dict[str, Any]],
    *,
    hot_minutes: int,
    cold_minutes: int,
) -> bool:
    from src.jobs import state as state_pkg

    return state_pkg.should_skip_source_by_cadence(
        source_name,
        state_rows,
        hot_minutes=hot_minutes,
        cold_minutes=cold_minutes,
    )


def circuit_breaker_until(
    source_name: str, state_rows: dict[str, dict[str, Any]], failure_threshold: int
) -> datetime | None:
    from src.jobs import state as state_pkg

    return state_pkg.circuit_breaker_until(source_name, state_rows, failure_threshold)


def apply_circuit_breaker_exclusions(
    selected_loaders: list[tuple[str, SourceLoader]],
    *,
    source_state_rows: dict[str, dict[str, Any]],
    circuit_breaker_failures: int,
    circuit_breaker_cooldown_minutes: int,
    ignore_circuit_breaker: bool,
) -> tuple[list[tuple[str, SourceLoader]], list[dict[str, Any]]]:
    from src.jobs import state as state_pkg

    return state_pkg.apply_circuit_breaker_exclusions(
        selected_loaders,
        source_state_rows=source_state_rows,
        circuit_breaker_failures=circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
        ignore_circuit_breaker=ignore_circuit_breaker,
    )


def append_excluded_default_sources(source_reports: list[dict[str, Any]]) -> None:
    from src.jobs import state as state_pkg

    state_pkg.append_excluded_default_sources(source_reports)


def update_source_state_rows(
    *,
    source_state_rows: dict[str, dict[str, Any]],
    source_reports: list[dict[str, Any]],
    canonical_rows: list[RawJob],
    finished_at: str,
    circuit_breaker_failures: int,
    circuit_breaker_cooldown_minutes: int,
) -> dict[str, dict[str, Any]]:
    from src.jobs import state as state_pkg

    return state_pkg.update_source_state_rows(
        source_state_rows=source_state_rows,
        source_reports=source_reports,
        canonical_rows=canonical_rows,
        finished_at=finished_at,
        circuit_breaker_failures=circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
    )


def run_pipeline(
    *,
    output_dir: Path,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    retries: int = DEFAULT_RETRIES,
    backoff_s: float = DEFAULT_BACKOFF_S,
    preserve_previous_on_empty: bool = True,
    fetch_text: Callable[[str, int], str] = default_fetch_text,
    source_loaders: list[tuple[str, SourceLoader]] | None = None,
    seed_from_existing_output: bool = False,
    source_ttl_minutes: int = 0,
    max_workers: int = 1,
    max_per_domain: int = 2,
    fetch_strategy: str = DEFAULT_FETCH_STRATEGY,
    adapter_http_concurrency: int = DEFAULT_ADAPTER_HTTP_CONCURRENCY,
    google_sheets_redirect_concurrency: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
    respect_source_cadence: bool = False,
    hot_source_cadence_minutes: int = DEFAULT_HOT_SOURCE_CADENCE_MINUTES,
    cold_source_cadence_minutes: int = DEFAULT_COLD_SOURCE_CADENCE_MINUTES,
    circuit_breaker_failures: int = 3,
    circuit_breaker_cooldown_minutes: int = 180,
    ignore_circuit_breaker: bool = False,
    social_enabled: bool = False,
    social_config_path: Path | None = None,
    social_lookback_minutes: int = DEFAULT_SOCIAL_LOOKBACK_MINUTES,
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    show_progress: bool = True,
    selection_exclusions: list[dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> dict[str, Any]:
    from src.jobs import pipeline as pipeline_pkg

    return pipeline_pkg.run_pipeline(
        output_dir=output_dir,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        preserve_previous_on_empty=preserve_previous_on_empty,
        fetch_text=fetch_text,
        source_loaders=source_loaders,
        seed_from_existing_output=seed_from_existing_output,
        source_ttl_minutes=source_ttl_minutes,
        max_workers=max_workers,
        max_per_domain=max_per_domain,
        fetch_strategy=fetch_strategy,
        adapter_http_concurrency=adapter_http_concurrency,
        google_sheets_redirect_concurrency=google_sheets_redirect_concurrency,
        respect_source_cadence=respect_source_cadence,
        hot_source_cadence_minutes=hot_source_cadence_minutes,
        cold_source_cadence_minutes=cold_source_cadence_minutes,
        circuit_breaker_failures=circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
        ignore_circuit_breaker=ignore_circuit_breaker,
        social_enabled=social_enabled,
        social_config_path=social_config_path,
        social_lookback_minutes=social_lookback_minutes,
        static_detail_concurrency=static_detail_concurrency,
        show_progress=show_progress,
        selection_exclusions=selection_exclusions,
        force_refresh_all=force_refresh_all,
    )


def parse_args() -> argparse.Namespace:
    from src.jobs import pipeline as pipeline_pkg

    return pipeline_pkg.parse_args()


def main() -> int:
    from src.jobs import pipeline as pipeline_pkg

    return pipeline_pkg.main()


if __name__ == "__main__":
    raise SystemExit(main())
