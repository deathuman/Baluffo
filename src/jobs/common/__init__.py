#!/usr/bin/env python3
"""Compatibility facade for shared jobs helpers and constants.

New code should import leaf modules directly. This module keeps the small
set of compatibility names still used by the legacy fetcher facade and tests.
"""

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
from . import config as _common_config
from . import diagnostics as _common_diagnostics
from . import heuristics as _common_heuristics
from . import http as _common_http
from . import parsing as _common_parsing
from . import registry_defaults as _common_registry_defaults
from . import social as _common_social
from . import sources as _common_sources
from .registry_defaults import (
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
DEFAULT_SOCIAL_LOOKBACK_MINUTES = _common_config.DEFAULT_SOCIAL_LOOKBACK_MINUTES
DEFAULT_SOCIAL_MIN_CONFIDENCE = _common_config.DEFAULT_SOCIAL_MIN_CONFIDENCE
UNKNOWN_COMPANY_LABEL = _common_config.UNKNOWN_COMPANY_LABEL
UNTRUSTWORTHY_COMPANY_LABELS = _common_config.UNTRUSTWORTHY_COMPANY_LABELS
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
STUDIO_SOURCE_REGISTRY = _common_sources.load_studio_source_registry(DEFAULT_STUDIO_SOURCE_REGISTRY)

PREFERRED_IMPORT_SURFACES = [
    "registry_entries",
    "set_source_diagnostics",
    "default_fetch_text",
    "fetch_with_retries",
    "parse_jobpostings_from_html",
    "parse_teamtailor_listing_links",
    "clean_text",
    "norm_text",
    "normalize_url",
    "fingerprint_url",
    "to_iso",
    "now_iso",
    "env_flag",
]

__all__ = [*PREFERRED_IMPORT_SURFACES]


from src.jobs.adapters.html_parsers import (
    parse_jobpostings_from_html,
    parse_teamtailor_listing_links,
)

from .datetime_utils import to_iso
from .fetch import fetch_with_retries
from .http import default_fetch_text as _common_default_fetch_text
from .registry import registry_entries as _registry_entries
from .url import fingerprint_url


def registry_entries(adapter: str, *, enabled_only: bool = True) -> list[dict[str, Any]]:
    return _registry_entries(
        adapter,
        enabled_only=enabled_only,
        studio_source_registry=STUDIO_SOURCE_REGISTRY,
        redundant_static_rules=REDUNDANT_STATIC_IF_PROVIDER,
    )


set_source_diagnostics = _common_diagnostics.set_source_diagnostics
now_iso = _shared_now_iso
env_flag = _shared_env_flag


def default_fetch_text(url: str, timeout_s: int) -> str:
    return _common_default_fetch_text(url, timeout_s, headers=DEFAULT_HTTP_HEADERS)
