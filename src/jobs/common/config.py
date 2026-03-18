"""Shared defaults/constants for jobs fetching pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from src.baluffo_config import get_storage_defaults

DEFAULT_TIMEOUT_S = 20
DEFAULT_RETRIES = 2
DEFAULT_BACKOFF_S = 1.6
DEFAULT_FETCH_STRATEGY = "auto"
DEFAULT_ADAPTER_HTTP_CONCURRENCY = 24
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = 8
DEFAULT_HOT_SOURCE_CADENCE_MINUTES = 15
DEFAULT_COLD_SOURCE_CADENCE_MINUTES = 60

UNKNOWN_COMPANY_LABEL = "Unknown company"
UNTRUSTWORTHY_COMPANY_LABELS = {
    "game",
    "tech",
    "game company",
    "tech company",
    "gaming company",
    "technology company",
    "giant enemy crab",
    "farbridge",
    "enduring games",
}

_STORAGE_DEFAULTS = get_storage_defaults()
DEFAULT_OUTPUT_DIR = Path(_STORAGE_DEFAULTS["data_dir"])
DEFAULT_SOCIAL_CONFIG_PATH = Path(_STORAGE_DEFAULTS["social_sources_config_path"])
DEFAULT_SOCIAL_LOOKBACK_MINUTES = 30
DEFAULT_SOCIAL_MIN_CONFIDENCE = 40

DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE = "balanced"
DEFAULT_STATIC_DETAIL_CONCURRENCY = 6
DEFAULT_SCRAPY_VALIDATION_STRICT = True
DEFAULT_CANONICAL_STRICT_URL = False

SOURCE_REGISTRY_ACTIVE_PATH = DEFAULT_OUTPUT_DIR / "source-registry-active.json"
SOURCE_REGISTRY_PENDING_PATH = DEFAULT_OUTPUT_DIR / "source-registry-pending.json"
SOURCE_APPROVAL_STATE_PATH = DEFAULT_OUTPUT_DIR / "source-approval-state.json"
SCRAPY_BROWSER_QUEUE_PATH = DEFAULT_OUTPUT_DIR / "jobs-browser-fallback-queue.json"

# Classifications that cause a static/scrapy_static source to be added to the browser fallback queue.
# Keep in sync with plugins/static/_heuristics.py CLASSIFICATION_* constants.
STATIC_CLASSIFICATIONS_FOR_BROWSER_QUEUE: frozenset = frozenset(
    {
        "fetch_ok_extract_zero",
        "blocked_or_challenge",
        "timeout",
    }
)

REQUIRED_FIELDS = [
    "title",
    "company",
    "city",
    "country",
    "workType",
    "contractType",
    "jobLink",
    "sector",
    "profession",
]

OPTIONAL_FIELDS = [
    "source",
    "sourceJobId",
    "fetchedAt",
    "postedAt",
    "status",
    "firstSeenAt",
    "lastSeenAt",
    "removedAt",
    "dedupKey",
    "qualityScore",
    "focusScore",
    "sourceBundleCount",
    "sourceBundle",
]

OUTPUT_FIELDS = ["id", *REQUIRED_FIELDS, "companyType", "description", *OPTIONAL_FIELDS]
LIGHTWEIGHT_OUTPUT_FIELDS = [
    "id",
    "title",
    "company",
    "city",
    "country",
    "workType",
    "contractType",
    "jobLink",
    "sector",
    "profession",
    "source",
    "postedAt",
    "status",
    "lastSeenAt",
    "qualityScore",
    "focusScore",
    "sourceBundleCount",
]

SUPPORTED_REDIRECT_HOSTS = {"gracklehq.com", "www.gracklehq.com"}
DEFAULT_HTTP_HEADERS = {
    "User-Agent": "BaluffoJobsFetcher/1.0 (+https://github.com/)",
    "Accept": "application/json,text/html,text/csv,*/*",
}
DEFAULT_REDIRECT_HEADERS = {
    "User-Agent": DEFAULT_HTTP_HEADERS["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS = 14
LIFECYCLE_ARCHIVE_RETENTION_DAYS = 120

TARGET_PROFESSIONS = {"technical-artist", "environment-artist"}

# Mutable diagnostics map used across fetch runs.
SOURCE_DIAGNOSTICS: Dict[str, Dict[str, Any]] = {}

