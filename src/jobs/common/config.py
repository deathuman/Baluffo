"""Shared defaults/constants for jobs fetching pipeline."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from src.baluffo_config import get_storage_defaults

DEFAULT_TIMEOUT_S = 15
DEFAULT_RETRIES = 2
DEFAULT_BACKOFF_S = 1.2
DEFAULT_FETCH_STRATEGY = "auto"
DEFAULT_FETCH_MAX_WORKERS = 12
DEFAULT_FETCH_MAX_PER_DOMAIN = 3
DEFAULT_STATIC_FETCH_MAX_PER_DOMAIN = 6
DEFAULT_ADAPTER_HTTP_CONCURRENCY = 48
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = 8
DEFAULT_HOT_SOURCE_CADENCE_MINUTES = 15
DEFAULT_COLD_SOURCE_CADENCE_MINUTES = 60
DEFAULT_INCREMENTAL_FETCH_ENABLED = True
DEFAULT_INCREMENTAL_PROVIDER_STABLE_MINUTES = 60
DEFAULT_INCREMENTAL_STATIC_LISTING_MINUTES = 180
DEFAULT_INCREMENTAL_EMPTY_SOURCE_MINUTES = 12 * 60
DEFAULT_INCREMENTAL_EMPTY_SOURCE_MIN_ZERO_RUNS = 2
DEFAULT_INCREMENTAL_DEAD_SOURCE_MINUTES = 24 * 60

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
REMOTE_OK_URLS = [
    "https://remoteok.com/api",
    "https://remoteok.io/api",
]
REMOTIVE_API_URLS = [
    "https://remotive.com/api/remote-jobs",
]
GAMES_INDUSTRY_URLS = [
    "https://jobs.gamesindustry.biz",
    "https://jobs.gamesindustry.biz/jobs",
]
EPIC_CAREERS_API_URL = "https://greenhouse-service.debc.live.use1a.on.epicgames.com/api/job"
WELLFOUND_URLS = [
    "https://wellfound.com/jobs?query=game+developer",
    "https://wellfound.com/jobs?query=unity",
    "https://wellfound.com/jobs?query=unreal",
]
GREENHOUSE_JOBS_URL_TEMPLATE = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"

DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE = "balanced"
DEFAULT_STATIC_DETAIL_CONCURRENCY = 10
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
        "anti_bot_or_challenge",
        "blocked_or_challenge",
        "rate_limited",
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
    "lifecycleEvent",
    "lifecycleReason",
    "availabilityId",
    "availabilityStatus",
    "availabilityCheckedAt",
    "availabilityVerifiedAt",
    "availabilityUnavailableAt",
    "availabilityEvidence",
    "dedupKey",
    "qualityScore",
    "focusScore",
    "sourceBundleCount",
    "sourceBundle",
    "locations",
    "locationSummary",
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
    "locations",
    "locationSummary",
    "source",
    "postedAt",
    "status",
    "lastSeenAt",
    "removedAt",
    "lifecycleEvent",
    "lifecycleReason",
    "availabilityId",
    "availabilityStatus",
    "availabilityCheckedAt",
    "availabilityVerifiedAt",
    "availabilityUnavailableAt",
    "availabilityEvidence",
    "qualityScore",
    "focusScore",
    "sourceBundleCount",
]

OUTPUT_SIZE_GUARDRAIL_LIMITS = {
    "json": 80_000_000,
    "lightJson": 60_000_000,
}

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
AVAILABILITY_OVERDUE_FAILURE_COUNT = 2
AVAILABILITY_OVERDUE_DAYS = 7
AVAILABILITY_HISTORY_DAYS = 30

STRICT_GAME_ONLY_ENABLED = os.environ.get("BALUFFO_STRICT_GAME_ONLY", "").strip() in (
    "1",
    "true",
    "yes",
)

TARGET_PROFESSIONS = {"technical-artist", "environment-artist"}

# Mutable diagnostics map used across fetch runs.
SOURCE_DIAGNOSTICS: dict[str, dict[str, Any]] = {}
