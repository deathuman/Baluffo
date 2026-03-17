#!/usr/bin/env python3
"""Aggregate game job listings into unified JSON/CSV feeds."""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import hashlib
import json
import os
import re
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from html import unescape
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET
try:
    import httpx
except Exception:  # noqa: BLE001
    httpx = None

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.shared.regex import find_urls_in_text
from src.shared.utils import env_flag as _shared_env_flag, now_iso as _shared_now_iso
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
from src.jobs.adapters import html_parsers as _html_parsers
from src.jobs.game_detection import GAME_KEYWORDS, looks_like_game_job
from src.contracts import SCHEMA_VERSION
from src.baluffo_config import get_storage_defaults
from src.jobs_fetcher_registry import (
    DEFAULT_SOURCE_LOADER_NAMES,
    EXCLUDED_DEFAULT_SOURCES,
    SOURCE_REPORT_META,
)
from src.pipeline_io import (
    read_existing_output as read_existing_output_from_file,
    serialize_rows_for_csv,
    serialize_rows_for_json,
    write_atomic_if_changed,
    write_text_if_changed,
)

RawJob = Dict[str, Any]
SourceLoader = Callable[..., List[RawJob]]

REMOTE_OK_URLS = [
    "https://remoteok.com/api",
    "https://remoteok.io/api",
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
DEFAULT_STUDIO_SOURCE_REGISTRY = [
    {
        "name": "Guerrilla Games",
        "studio": "Guerrilla Games",
        "adapter": "greenhouse",
        "slug": "guerrilla-games",
        "nlPriority": True,
        "enabledByDefault": True,
    },
    {
        "name": "PlayStation Global",
        "studio": "PlayStation Global",
        "adapter": "greenhouse",
        "slug": "sonyinteractiveentertainmentglobal",
        "nlPriority": True,
        "enabledByDefault": True,
    },
    {
        "name": "Paradox Careers",
        "studio": "Paradox Interactive",
        "adapter": "teamtailor",
        "listing_url": "https://career.paradoxplaza.com/jobs",
        "base_url": "https://career.paradoxplaza.com",
        "company": "Paradox Interactive",
        "nlPriority": True,
        "enabledByDefault": True,
    },
    {
        "name": "Little Chicken",
        "studio": "Little Chicken",
        "adapter": "static",
        "company": "Little Chicken",
        "pages": [
            "https://www.littlechicken.nl/about-us/jobs/",
            "https://www.littlechicken.nl/job/",
        ],
        "nlPriority": True,
        "enabledByDefault": True,
    },
    {
        "name": "Larian Studios",
        "studio": "Larian Studios",
        "adapter": "greenhouse",
        "slug": "larian-studios",
        "nlPriority": True,
        "enabledByDefault": False,
    },
    {
        "name": "Bandai Namco Entertainment America (Greenhouse)",
        "studio": "Bandai Namco Entertainment America Inc.",
        "adapter": "greenhouse",
        "slug": "bandainamco",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Jagex (Lever)",
        "studio": "Jagex",
        "adapter": "lever",
        "account": "jagex",
        "api_url": "https://api.lever.co/v0/postings/jagex?mode=json",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Sandbox VR (Lever)",
        "studio": "Sandbox VR",
        "adapter": "lever",
        "account": "sandboxvr",
        "api_url": "https://api.lever.co/v0/postings/sandboxvr?mode=json",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Voodoo (Lever)",
        "studio": "Voodoo",
        "adapter": "lever",
        "account": "voodoo",
        "api_url": "https://api.lever.co/v0/postings/voodoo?mode=json",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "CD PROJEKT RED (SmartRecruiters)",
        "studio": "CD PROJEKT RED",
        "adapter": "smartrecruiters",
        "company_id": "CDPROJEKTRED",
        "api_url": "https://api.smartrecruiters.com/v1/companies/CDPROJEKTRED/postings",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Gameloft (SmartRecruiters)",
        "studio": "Gameloft",
        "adapter": "smartrecruiters",
        "company_id": "Gameloft",
        "api_url": "https://api.smartrecruiters.com/v1/companies/Gameloft/postings",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Hutch (Workable)",
        "studio": "Hutch",
        "adapter": "workable",
        "account": "hutch",
        "api_url": "https://apply.workable.com/api/v1/widget/accounts/hutch?details=true",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Wargaming (Workable)",
        "studio": "Wargaming",
        "adapter": "workable",
        "account": "wargaming",
        "api_url": "https://apply.workable.com/api/v1/widget/accounts/wargaming?details=true",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "InnoGames (Personio)",
        "studio": "InnoGames",
        "adapter": "personio",
        "feed_url": "https://innogames.jobs.personio.de/xml",
        "nlPriority": True,
        "enabledByDefault": True,
    },
    {
        "name": "Travian (Personio)",
        "studio": "Travian",
        "adapter": "personio",
        "feed_url": "https://travian.jobs.personio.de/xml",
        "nlPriority": True,
        "enabledByDefault": True,
    },
    {
        "name": "Jagex (Ashby)",
        "studio": "Jagex",
        "adapter": "ashby",
        "board_url": "https://jobs.ashbyhq.com/jagex/jobs",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Scopely (Ashby)",
        "studio": "Scopely",
        "adapter": "ashby",
        "board_url": "https://jobs.ashbyhq.com/scopely/jobs",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Ubisoft (SmartRecruiters)",
        "studio": "Ubisoft",
        "adapter": "smartrecruiters",
        "company_id": "Ubisoft2",
        "api_url": "https://api.smartrecruiters.com/v1/companies/Ubisoft2/postings",
        "nlPriority": False,
        "enabledByDefault": True,
    },
]

# Static sources whose host matches one of these are skipped when the registry
# already has the corresponding provider source (avoids duplicate extract-zero static runs).
REDUNDANT_STATIC_IF_PROVIDER: List[Dict[str, Any]] = [
    {
        "hosts": ["cdprojektred.com", "www.cdprojektred.com"],
        "adapter": "smartrecruiters",
        "provider_id_field": "company_id",
        "provider_id_value": "CDPROJEKTRED",
    },
    {
        "hosts": ["ubisoft.com", "www.ubisoft.com"],
        "adapter": "smartrecruiters",
        "provider_id_field": "company_id",
        "provider_id_value": "Ubisoft2",
    },
    {
        "hosts": ["xsolla.com", "www.xsolla.com"],
        "adapter": "lever",
        "provider_id_field": "account",
        "provider_id_value": "xsolla",
    },
    {
        "hosts": ["bandainamcoent.com", "www.bandainamcoent.com"],
        "adapter": "greenhouse",
        "provider_id_field": "slug",
        "provider_id_value": "bandainamco",
    },
]

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
DEFAULT_OUTPUT_DIR = _STORAGE_DEFAULTS["data_dir"]
DEFAULT_SOCIAL_CONFIG_PATH = _STORAGE_DEFAULTS["social_sources_config_path"]
DEFAULT_SOCIAL_LOOKBACK_MINUTES = 30
SOCIAL_SOURCE_NAMES = {"social_reddit", "social_x", "social_mastodon"}
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
STATIC_CLASSIFICATIONS_FOR_BROWSER_QUEUE: frozenset = frozenset({
    "fetch_ok_extract_zero",
    "blocked_or_challenge",
    "timeout",
})

DEFAULT_SOCIAL_CONFIG: Dict[str, Any] = {
    "enabled": False,
    "minConfidence": DEFAULT_SOCIAL_MIN_CONFIDENCE,
    "rejectForHirePosts": True,
    "reddit": {
        "enabled": True,
        "subreddits": ["gamedev", "gameDevClassifieds", "gamedevjobs"],
        "maxPostsPerSubreddit": 50,
        "rssFallback": True,
        "htmlFallback": True,
    },
    "x": {
        "enabled": True,
        "minConfidence": 20,
        "queries": [
            "#gamedevjobs",
            "#gamejobs",
            "\"game designer\" \"we're hiring\"",
            "\"gamedev\" \"hiring\"",
        ],
        "maxPostsPerQuery": 25,
        "api": {
            "enabled": True,
            "endpoint": "https://api.x.com/2/tweets/search/recent",
            "bearerTokenEnv": "BALUFFO_X_BEARER_TOKEN",
        },
        "scraperFallback": {
            "enabled": False,
            "endpoint": "",
        },
        "rssFallback": {
            "enabled": True,
            "instances": [
                "https://xcancel.com",
                "https://nitter.net",
                "https://nitter.poast.org",
            ],
        },
    },
    "mastodon": {
        "enabled": True,
        "instances": ["https://mastodon.gamedev.place"],
        "hashtags": ["gamedevjobs", "gamejobs", "hiring", "unityjobs", "unrealjobs"],
        "maxPostsPerTag": 40,
    },
}


def load_registry_from_file(path: Path, fallback: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        if not path.exists():
            return [dict(row) for row in fallback]
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return [dict(row) for row in fallback]
        rows = [row for row in payload if isinstance(row, dict)]
        return rows if rows else [dict(row) for row in fallback]
    except (OSError, json.JSONDecodeError):
        return [dict(row) for row in fallback]


def load_studio_source_registry() -> List[Dict[str, Any]]:
    return load_registry_from_file(SOURCE_REGISTRY_ACTIVE_PATH, DEFAULT_STUDIO_SOURCE_REGISTRY)


STUDIO_SOURCE_REGISTRY = load_studio_source_registry()


def read_approved_since_last_run(path: Path) -> int:
    try:
        if not path.exists():
            return 0
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return 0
        return int(payload.get("approvedSinceLastRun") or 0)
    except (OSError, ValueError, json.JSONDecodeError):
        return 0

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
SOURCE_DIAGNOSTICS: Dict[str, Dict[str, Any]] = {}


def _static_source_primary_host(row: Dict[str, Any]) -> str:
    """Return the host of the first page URL for a static source, normalized (lower, no leading www.)."""
    pages = row.get("pages") if isinstance(row.get("pages"), list) else []
    url = (pages[0] if pages else None) or clean_text(row.get("listing_url")) or ""
    if not url:
        return ""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        host = (parsed.netloc or "").strip().lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:  # noqa: BLE001
        return ""


def _provider_keys_present_in_registry(*, enabled_only: bool = True) -> set:
    """Return set of (adapter, provider_id_value) for provider sources in the registry."""
    out: set = set()
    for rule in REDUNDANT_STATIC_IF_PROVIDER:
        ad = clean_text(rule.get("adapter"))
        field = clean_text(rule.get("provider_id_field"))
        val = clean_text(rule.get("provider_id_value"))
        if not ad or not field or not val:
            continue
        for row in STUDIO_SOURCE_REGISTRY:
            if clean_text(row.get("adapter")) != ad:
                continue
            if enabled_only and not bool(row.get("enabledByDefault", True)):
                continue
            if clean_text(row.get(field)) == val:
                out.add((ad, val))
                break
    return out


def _scrapy_static_registry_from_browser_queue(*, enabled_only: bool = True) -> List[Dict[str, Any]]:
    """Build synthetic scrapy_static registry rows from the browser fallback queue.

    This lets the scrapy_static_sources adapter run a browser-capable pass for
    sources that the static adapter has already classified as browser-required.
    Groups queue rows by sourceId and emits one registry row per source with
    the single best URL (shortest path) so we run Scrapy once per source.
    """
    try:
        if not SCRAPY_BROWSER_QUEUE_PATH.exists():
            return []
        payload = json.loads(SCRAPY_BROWSER_QUEUE_PATH.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return []
    except (OSError, json.JSONDecodeError):
        return []

    by_source: Dict[str, List[Dict[str, Any]]] = {}
    for row in payload:
        if not isinstance(row, dict):
            continue
        if clean_text(row.get("adapter")) != "scrapy_static":
            continue
        page = clean_text(row.get("page"))
        if not page:
            continue
        source_id = clean_text(row.get("sourceId")) or f"scrapy_static:{hashlib.sha1(page.encode('utf-8')).hexdigest()[:12]}"
        by_source.setdefault(source_id, []).append(row)

    rows: List[Dict[str, Any]] = []
    for source_id, group in by_source.items():
        def path_len(r: Dict[str, Any]) -> int:
            return len((urlparse(clean_text(r.get("page")) or "").path))
        best = min(group, key=path_len)
        page = clean_text(best.get("page")) or ""
        if not page:
            continue
        name = clean_text(best.get("name")) or clean_text(best.get("studio")) or "scrapy_static_source"
        studio = clean_text(best.get("studio")) or name
        rows.append({
            "name": name,
            "studio": studio,
            "adapter": "scrapy_static",
            "pages": [page],
            "id": source_id,
            "enabledByDefault": True,
            "fetchStrategy": "http",
            "cadenceMinutes": 0,
        })
    return rows


def registry_entries(adapter: str, *, enabled_only: bool = True) -> List[Dict[str, Any]]:
    if adapter == "scrapy_static":
        # For scrapy_static we currently derive sources from the browser fallback
        # queue rather than the studio source registry.
        return _scrapy_static_registry_from_browser_queue(enabled_only=enabled_only)

    rows = []
    for row in STUDIO_SOURCE_REGISTRY:
        if clean_text(row.get("adapter")) != adapter:
            continue
        if enabled_only and not bool(row.get("enabledByDefault", True)):
            continue
        normalized = dict(row)
        normalized["fetchStrategy"] = clean_text(row.get("fetchStrategy")) or "auto"
        normalized["cadenceMinutes"] = _clamped_int(row.get("cadenceMinutes"), 0, 0)
        rows.append(normalized)
    if adapter == "static" and REDUNDANT_STATIC_IF_PROVIDER:
        provider_keys = _provider_keys_present_in_registry(enabled_only=enabled_only)
        filtered = []
        for r in rows:
            host = _static_source_primary_host(r)
            if not host:
                filtered.append(r)
                continue
            skip = False
            for rule in REDUNDANT_STATIC_IF_PROVIDER:
                hosts = rule.get("hosts")
                if not isinstance(hosts, list):
                    continue
                if host not in [str(h).strip().lower() for h in hosts]:
                    continue
                ad = clean_text(rule.get("adapter"))
                val = clean_text(rule.get("provider_id_value"))
                if (ad, val) in provider_keys:
                    skip = True
                    break
            if not skip:
                filtered.append(r)
        rows = filtered
    return rows


def set_source_diagnostics(
    source_name: str,
    *,
    adapter: str,
    studio: str,
    details: Optional[List[Dict[str, Any]]] = None,
    partial_errors: Optional[List[str]] = None,
) -> None:
    SOURCE_DIAGNOSTICS[source_name] = {
        "adapter": clean_text(adapter) or "unknown",
        "studio": clean_text(studio) or "multiple",
        "details": details or [],
        "partialErrors": partial_errors or [],
    }


def now_iso() -> str:
    return _shared_now_iso()


def env_flag(name: str, default: bool) -> bool:
    return _shared_env_flag(name, default)


def _deep_merge_dicts(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {key: value for key, value in base.items()}
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
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
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
    merged["minConfidence"] = max(0, min(100, int(merged.get("minConfidence") or DEFAULT_SOCIAL_MIN_CONFIDENCE)))
    merged["rejectForHirePosts"] = bool(merged.get("rejectForHirePosts", True))
    return merged


def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        num = float(value)
        if num <= 0:
            return None
        if num > 10_000_000_000:
            num /= 1000.0
        try:
            return datetime.fromtimestamp(num, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    text = clean_text(value)
    if not text:
        return None
    if re.fullmatch(r"\d{10,13}", text):
        return parse_datetime(int(text))
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_iso(value: Any) -> str:
    dt = parse_datetime(value)
    return dt.isoformat() if dt else ""


def posted_ts(value: Any) -> float:
    dt = parse_datetime(value)
    return dt.timestamp() if dt else 0.0


def canonical_url_fingerprint_seed(url: Any) -> str:
    normalized = normalize_url(url)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    host = parsed.netloc.lower()
    path = parsed.path or "/"

    if host in {"jobs.smartrecruiters.com", "api.smartrecruiters.com"}:
        jobs_match = re.match(r"^/([^/]+)/(\d+)(?:-[^/]+)?$", path)
        if jobs_match:
            company_id, posting_id = jobs_match.groups()
            return f"smartrecruiters:{company_id.lower()}:{posting_id}"
        api_match = re.match(r"^/v1/companies/([^/]+)/postings/(\d+)$", path)
        if api_match:
            company_id, posting_id = api_match.groups()
            return f"smartrecruiters:{company_id.lower()}:{posting_id}"

    return normalized


def fingerprint_url(url: Any) -> str:
    seed = canonical_url_fingerprint_seed(url)
    return hashlib.sha1(seed.encode("utf-8")).hexdigest() if seed else ""


def is_supported_redirect_url(url: Any) -> bool:
    normalized = normalize_url(url)
    if not normalized:
        return False
    parsed = urlparse(normalized)
    return parsed.netloc.lower() in SUPPORTED_REDIRECT_HOSTS and parsed.path.startswith("/rd/")


def resolve_supported_redirect_url(url: Any, *, timeout_s: int = DEFAULT_TIMEOUT_S) -> str:
    normalized = normalize_url(url)
    if not is_supported_redirect_url(normalized):
        return normalized
    last_error: Optional[Exception] = None
    for method in ("HEAD", "GET"):
        request = Request(normalized, headers=DEFAULT_REDIRECT_HEADERS, method=method)
        try:
            with urlopen(request, timeout=max(1, int(timeout_s or DEFAULT_TIMEOUT_S))) as response:
                resolved = normalize_url(response.geturl())
                return resolved or normalized
        except HTTPError as exc:
            last_error = exc
            if method == "HEAD" and int(getattr(exc, "code", 0) or 0) in {400, 403, 405, 429, 500, 501, 503}:
                continue
            return normalized
        except (URLError, ValueError) as exc:
            last_error = exc
            if method == "HEAD":
                continue
            break
    _ = last_error
    return normalized


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


def build_redirect_resolver(
    *,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    max_connections: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
) -> PooledRedirectResolver:
    from src.jobs import transport as transport_pkg

    return transport_pkg.build_redirect_resolver(timeout_s=timeout_s, max_connections=max_connections)


def normalize_contract_type(contract_text: Any, title: Any = "") -> str:
    lower = f"{norm_text(contract_text)} {norm_text(title)}"
    if "internship" in lower or re.search(r"\bintern\b", lower):
        return "Internship"
    if "full-time" in lower or "full time" in lower or "permanent" in lower:
        return "Full-time"
    if (
        "temporary" in lower
        or "contract" in lower
        or "freelance" in lower
        or "part-time" in lower
        or "part time" in lower
        or "fixed-term" in lower
        or "fixed term" in lower
    ):
        return "Temporary"
    return "Unknown"


def classify_company_type(company: Any, title: Any = "") -> str:
    text = f"{norm_text(company)} {norm_text(title)}"
    if re.search(r"\b(game|gaming|games|esports|studio|studios|interactive|publisher|entertainment)\b", text):
        return "Game"
    return "Tech"


def map_profession(title: Any) -> str:
    lower = norm_text(title)
    if "technical animator" in lower:
        return "technical-animator"
    if "technical director" in lower or re.search(r"\btd\b", lower):
        return "technical-director"
    if (
        "technical artist" in lower
        or "tech artist" in lower
        or "tech-art" in lower
        or "tech art" in lower
        or "shader artist" in lower
        or "material artist" in lower
    ):
        return "technical-artist"
    if (
        "environment artist" in lower
        or "environment art" in lower
        or "world artist" in lower
        or "terrain artist" in lower
    ):
        return "environment-artist"
    if "character artist" in lower:
        return "character-artist"
    if "rigging" in lower or "rigger" in lower:
        return "rigging"
    if "vfx artist" in lower or "visual effects artist" in lower or "fx artist" in lower:
        return "vfx-artist"
    if "ui artist" in lower or "ux artist" in lower or "ui/ux" in lower:
        return "ui-ux-artist"
    if "concept artist" in lower:
        return "concept-artist"
    if "3d artist" in lower or "3d modeler" in lower or "3d modeller" in lower:
        return "3d-artist"
    if "art director" in lower:
        return "art-director"
    if "gameplay" in lower or "game mechanics" in lower:
        return "gameplay"
    if "graphics" in lower or "rendering" in lower or "shader" in lower:
        return "graphics"
    if "engine" in lower or "architecture" in lower or "systems" in lower:
        return "engine"
    if re.search(r"\bai\b", lower) or "artificial intelligence" in lower or "behavior" in lower:
        return "ai"
    if "animator" in lower or "animation" in lower:
        return "animator"
    if "tools" in lower or "pipeline" in lower:
        return "tools"
    if "designer" in lower:
        return "designer"
    return "other"


def is_untrustworthy_company_label(value: str) -> bool:
    return norm_text(value) in UNTRUSTWORTHY_COMPANY_LABELS


def normalize_company_value(value: Any) -> str:
    company = clean_text(value)
    if not company:
        return ""
    if is_untrustworthy_company_label(company):
        return UNKNOWN_COMPANY_LABEL
    return company


def parse_remote_ok_payload(payload: Any) -> List[RawJob]:
    if isinstance(payload, list):
        rows = [row for row in payload if isinstance(row, dict)]
    elif isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
        rows = [row for row in payload["jobs"] if isinstance(row, dict)]
    else:
        return []

    jobs: List[RawJob] = []
    for row in rows:
        title = clean_text(row.get("position") or row.get("title"))
        company = clean_text(row.get("company") or row.get("company_name"))
        tags = row.get("tags") or []
        tags_text = " ".join(str(tag) for tag in tags) if isinstance(tags, list) else clean_text(tags)
        description = clean_text(row.get("description"))
        if not title or not company:
            continue
        if not looks_like_game_job(title, company, tags_text, description):
            continue
        location = clean_text(row.get("location") or "Remote")
        remote = "remote" in norm_text(location)
        jobs.append(
            {
                "sourceJobId": clean_text(row.get("id")),
                "title": title,
                "company": company,
                "city": "Remote" if remote else "",
                "country": "Remote" if remote else location,
                "workType": "Remote" if remote else location,
                "contractType": tags_text,
                "jobLink": clean_text(row.get("url") or row.get("apply_url")),
                "sector": clean_text(row.get("category") or ""),
                "postedAt": row.get("date") or row.get("epoch") or row.get("time"),
            }
        )
    return jobs


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
    request = Request(
        url,
        headers=DEFAULT_HTTP_HEADERS,
    )
    try:
        with urlopen(request, timeout=timeout_s) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")
    except HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code} for {url}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error for {url}: {exc.reason}") from exc


class AsyncHttpTextFetcher:
    def __new__(cls, *, max_connections: int = DEFAULT_ADAPTER_HTTP_CONCURRENCY):
        from src.jobs import transport as transport_pkg

        return transport_pkg.AsyncHttpTextFetcher(max_connections=max_connections)


def resolve_fetch_text_impl(
    *,
    fetch_text: Callable[[str, int], str],
    fetch_strategy: str,
    adapter_http_concurrency: int,
) -> Tuple[Callable[[str, int], str], str, Optional[AsyncHttpTextFetcher]]:
    from src.jobs import transport as transport_pkg

    return transport_pkg.resolve_fetch_text_impl(
        fetch_text=fetch_text,
        fetch_strategy=fetch_strategy,
        adapter_http_concurrency=adapter_http_concurrency,
    )


def fetch_with_retries(url: str, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> str:
    attempts = max(0, retries) + 1
    last_error: Optional[Exception] = None
    for attempt in range(attempts):
        try:
            return fetch_text(url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < attempts - 1:
                message = str(exc) if exc is not None else ""
                # Special-case rate limiting. Async fetcher raises RuntimeError("HTTP 429 for <url>").
                if "HTTP 429" in message:
                    # Back off more aggressively so we don't hammer the provider.
                    time.sleep(max(backoff_s * (2 ** attempt), 8.0 * (attempt + 1)))
                else:
                    time.sleep(backoff_s * (2 ** attempt))
    raise RuntimeError(str(last_error) if last_error else f"Unknown fetch error for {url}")


def _community_adapter():
    from src.jobs.adapters import community

    return community


def _social_adapter():
    from src.jobs.adapters import social

    return social


def _provider_api_adapter():
    from src.jobs.adapters import provider_api

    return provider_api


def _static_adapter():
    from src.jobs.adapters import static

    return static


def run_remote_ok_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _community_adapter().run_remote_ok_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def _request_json_with_headers(url: str, *, timeout_s: int, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    return _social_adapter()._request_json_with_headers(url, timeout_s=timeout_s, headers=headers)


def run_social_reddit_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: Dict[str, Any],
) -> List[RawJob]:
    return _social_adapter().run_social_reddit_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        social_config=social_config,
    )


def run_social_x_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: Dict[str, Any],
) -> List[RawJob]:
    return _social_adapter().run_social_x_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        social_config=social_config,
    )


def run_social_mastodon_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: Dict[str, Any],
) -> List[RawJob]:
    return _social_adapter().run_social_mastodon_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        social_config=social_config,
    )


def run_gamesindustry_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _community_adapter().run_gamesindustry_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_epic_games_careers_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _community_adapter().run_epic_games_careers_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_wellfound_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _community_adapter().run_wellfound_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_greenhouse_boards_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _provider_api_adapter().run_greenhouse_boards_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_teamtailor_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _provider_api_adapter().run_teamtailor_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_scrapy_static_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> List[RawJob]:
    return _static_adapter().run_scrapy_static_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def static_source_shard(row: Dict[str, Any]) -> str:
    return _static_adapter().static_source_shard(row)


def run_static_studio_pages_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    sources: Optional[List[Dict[str, Any]]] = None,
    shard: Optional[str] = None,
    diagnostics_name: str = "static_studio_pages",
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: Optional[Dict[str, Dict[str, Any]]] = None,
    try_playwright: Optional[Callable[[str, int], Tuple[str, str]]] = None,
) -> List[RawJob]:
    return _static_adapter().run_static_studio_pages_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        sources=sources,
        shard=shard,
        diagnostics_name=diagnostics_name,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        try_playwright=try_playwright,
    )


def run_static_source_entry_source(
    *,
    source_row: Dict[str, Any],
    diagnostics_name: str,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: Optional[Dict[str, Dict[str, Any]]] = None,
    try_playwright: Optional[Callable[[str, int], Tuple[str, str]]] = None,
) -> List[RawJob]:
    return _static_adapter().run_static_source_entry_source(
        source_row=source_row,
        diagnostics_name=diagnostics_name,
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        try_playwright=try_playwright,
    )


def run_static_studio_pages_a_i_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: Optional[Dict[str, Dict[str, Any]]] = None,
    try_playwright: Optional[Callable[[str, int], Tuple[str, str]]] = None,
) -> List[RawJob]:
    return _static_adapter().run_static_studio_pages_a_i_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        try_playwright=try_playwright,
    )


def build_static_source_loaders() -> List[Tuple[str, SourceLoader]]:
    return _static_adapter().build_static_source_loaders()


def run_static_studio_pages_j_r_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: Optional[Dict[str, Dict[str, Any]]] = None,
    try_playwright: Optional[Callable[[str, int], Tuple[str, str]]] = None,
) -> List[RawJob]:
    return _static_adapter().run_static_studio_pages_j_r_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        try_playwright=try_playwright,
    )


def run_static_studio_pages_s_z_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: Optional[Dict[str, Dict[str, Any]]] = None,
    try_playwright: Optional[Callable[[str, int], Tuple[str, str]]] = None,
) -> List[RawJob]:
    return _static_adapter().run_static_studio_pages_s_z_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        try_playwright=try_playwright,
    )


def run_lever_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _provider_api_adapter().run_lever_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_smartrecruiters_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _provider_api_adapter().run_smartrecruiters_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_workable_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _provider_api_adapter().run_workable_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_ashby_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _provider_api_adapter().run_ashby_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_personio_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return _provider_api_adapter().run_personio_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def canonicalize_job_with_reason(
    raw: Any,
    *,
    source: str,
    fetched_at: str,
    resolve_redirect_url: Optional[Callable[[str], str]] = None,
    resolved_job_link: Any = None,
) -> Tuple[Optional[RawJob], str]:
    from src.jobs import canonicalize as canonicalize_pkg

    normalized, reason = canonicalize_pkg.canonicalize_job_with_reason(
        raw,
        source=source,
        fetched_at=fetched_at,
        resolve_redirect_url=resolve_redirect_url,
        resolved_job_link=resolved_job_link,
    )
    return (normalized.to_dict() if normalized is not None else None), reason


def canonicalize_job(
    raw: RawJob,
    *,
    source: str,
    fetched_at: str,
    resolve_redirect_url: Optional[Callable[[str], str]] = None,
    resolved_job_link: Any = None,
) -> Optional[RawJob]:
    from src.jobs import canonicalize as canonicalize_pkg

    normalized = canonicalize_pkg.canonicalize_job(
        raw,
        source=source,
        fetched_at=fetched_at,
        resolve_redirect_url=resolve_redirect_url,
        resolved_job_link=resolved_job_link,
    )
    return normalized.to_dict() if normalized is not None else None


def compute_quality_score(job: RawJob) -> int:
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
    filled = sum(1 for field in fields if clean_text(job.get(field)))
    return max(0, min(100, int(round((filled / len(fields)) * 100))))


def title_has_focus_role(title: Any) -> bool:
    lower = norm_text(title)
    if not lower:
        return False
    focus_tokens = (
        "technical artist",
        "tech artist",
        "tech-art",
        "tech art",
        "environment artist",
        "environment art",
        "world artist",
        "terrain artist",
        "material artist",
        "shader artist",
    )
    return any(token in lower for token in focus_tokens)


def compute_focus_score(job: RawJob) -> int:
    score = 0
    profession = norm_text(job.get("profession"))
    title = job.get("title")
    country = clean_text(job.get("country")).upper()
    work_type = clean_text(job.get("workType")).lower()

    if profession in TARGET_PROFESSIONS:
        score += 55
    elif title_has_focus_role(title):
        score += 45

    if country == "NL":
        score += 20
        if work_type == "hybrid":
            score += 3
        elif work_type == "onsite":
            score += 5

    if work_type == "remote":
        score += 16

    posted = parse_datetime(job.get("postedAt"))
    if posted:
        age_days = max(0.0, (datetime.now(timezone.utc) - posted).total_seconds() / 86400.0)
        if age_days <= 7:
            score += 12
        elif age_days <= 30:
            score += 8
        else:
            score += 3

    return max(0, min(100, score))


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


def choose_base_record(left: RawJob, right: RawJob) -> Tuple[RawJob, RawJob]:
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


def deduplicate_jobs(rows: Sequence[RawJob]) -> Tuple[List[RawJob], Dict[str, int]]:
    from src.jobs import dedup as dedup_pkg

    merged_rows, stats = dedup_pkg.deduplicate_jobs([dedup_pkg.CanonicalJob.from_mapping(row) for row in rows])
    return [row.to_dict() for row in merged_rows], stats


def default_source_loaders(
    *,
    social_enabled: bool = False,
    social_config: Optional[Dict[str, Any]] = None,
) -> List[Tuple[str, SourceLoader]]:
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
    dedup_stats: Dict[str, int],
    deduped_rows: Sequence[RawJob],
    source_reports: Sequence[Dict[str, Any]],
    canonical_count: int,
    preserved_previous: bool,
    active_source_count: int,
    pending_source_count: int,
    newly_approved_since_last_run: int,
    *,
    json_bytes: int,
    csv_bytes: int,
    light_json_bytes: int,
    lifecycle_counts_map: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
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
    source_reports: Sequence[Dict[str, Any]],
    *,
    generated_at: str,
) -> List[Dict[str, Any]]:
    from src.jobs import reporting as reporting_pkg

    return reporting_pkg.build_browser_fallback_queue(source_reports, generated_at=generated_at)


def read_previously_successful_sources(report_path: Path) -> set[str]:
    from src.jobs import state as state_pkg

    return state_pkg.read_previously_successful_sources(report_path)


def read_success_cache(cache_path: Path) -> set[str]:
    from src.jobs import state as state_pkg

    return state_pkg.read_success_cache(cache_path)


def write_success_cache(cache_path: Path, source_reports: Sequence[Dict[str, Any]]) -> None:
    from src.jobs import state as state_pkg

    state_pkg.write_success_cache(cache_path, source_reports)


def source_rows_fingerprint(rows: Sequence[RawJob]) -> str:
    from src.jobs import state as state_pkg

    return state_pkg.source_rows_fingerprint(rows)


def _clamped_int(value: Any, default: int = 0, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(minimum, parsed)


def normalize_source_state_payload(payload: Dict[str, Any], *, updated_at: str = "") -> Dict[str, Any]:
    from src.jobs import state as state_pkg

    return state_pkg.normalize_source_state_payload(payload, updated_at=updated_at)


def read_source_state(state_path: Path) -> Dict[str, Dict[str, Any]]:
    from src.jobs import state as state_pkg

    return state_pkg.read_source_state(state_path)


def write_source_state(state_path: Path, rows: Dict[str, Dict[str, Any]]) -> None:
    from src.jobs import state as state_pkg

    state_pkg.write_source_state(state_path, rows)


def _job_identity_key(job: Dict[str, Any]) -> str:
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


def normalize_job_lifecycle_payload(payload: Dict[str, Any], *, updated_at: str = "") -> Dict[str, Any]:
    from src.jobs import state as state_pkg

    return state_pkg.normalize_job_lifecycle_payload(payload, updated_at=updated_at)


def read_job_lifecycle_state(state_path: Path) -> Dict[str, Dict[str, Any]]:
    from src.jobs import state as state_pkg

    return state_pkg.read_job_lifecycle_state(state_path)


def write_job_lifecycle_state(state_path: Path, rows: Dict[str, Dict[str, Any]]) -> None:
    from src.jobs import state as state_pkg

    state_pkg.write_job_lifecycle_state(state_path, rows)


def lifecycle_counts(rows: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    from src.jobs import state as state_pkg

    return state_pkg.lifecycle_counts(rows)


def apply_job_lifecycle_state(
    *,
    deduped_rows: List[RawJob],
    lifecycle_rows: Dict[str, Dict[str, Any]],
    finished_at: str,
    allow_mark_missing: bool,
    eligible_missing_sources: Optional[set[str]] = None,
    remove_to_archive_days: int = LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS,
    archive_retention_days: int = LIFECYCLE_ARCHIVE_RETENTION_DAYS,
) -> Tuple[List[RawJob], Dict[str, Dict[str, Any]], Dict[str, int]]:
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


def normalize_runtime_payload(runtime: Dict[str, Any], *, selected_source_count: int) -> Dict[str, Any]:
    src = runtime if isinstance(runtime, dict) else {}
    normalized = {
        "maxWorkers": _clamped_int(src.get("maxWorkers"), 1, 1),
        "maxPerDomain": _clamped_int(src.get("maxPerDomain"), 1, 1),
        "fetchStrategy": clean_text(src.get("fetchStrategy")) or DEFAULT_FETCH_STRATEGY,
        "fetchClient": clean_text(src.get("fetchClient")) or "urllib",
        "adapterHttpConcurrency": _clamped_int(src.get("adapterHttpConcurrency"), DEFAULT_ADAPTER_HTTP_CONCURRENCY, 1),
        "staticDetailConcurrency": _clamped_int(src.get("staticDetailConcurrency"), DEFAULT_STATIC_DETAIL_CONCURRENCY, 1),
        "googleSheetsRedirectConcurrency": _clamped_int(
            src.get("googleSheetsRedirectConcurrency"), DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY, 1
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
        "circuitBreakerCooldownMinutes": _clamped_int(src.get("circuitBreakerCooldownMinutes"), 0, 0),
        "ignoreCircuitBreaker": bool(src.get("ignoreCircuitBreaker")),
        "socialEnabled": bool(src.get("socialEnabled")),
        "socialConfigPath": clean_text(src.get("socialConfigPath")),
        "socialLookbackMinutes": _clamped_int(src.get("socialLookbackMinutes"), DEFAULT_SOCIAL_LOOKBACK_MINUTES, 1),
        "socialMinConfidence": _clamped_int(src.get("socialMinConfidence"), DEFAULT_SOCIAL_MIN_CONFIDENCE, 0),
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
        "selectedSourceCount": _clamped_int(src.get("selectedSourceCount"), selected_source_count, 0),
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


def normalize_source_report_row(row: Dict[str, Any]) -> Dict[str, Any]:
    src = row if isinstance(row, dict) else {}
    def _normalize_loss(loss: Any) -> Dict[str, Any]:
        payload = loss if isinstance(loss, dict) else {}
        drop_reasons = payload.get("canonicalDropReasons") if isinstance(payload.get("canonicalDropReasons"), dict) else {}
        return {
            "rawFetched": _clamped_int(payload.get("rawFetched"), 0, 0),
            "canonicalDropped": _clamped_int(payload.get("canonicalDropped"), 0, 0),
            "canonicalKept": _clamped_int(payload.get("canonicalKept"), 0, 0),
            "dedupMerged": _clamped_int(payload.get("dedupMerged"), 0, 0),
            "finalOutput": _clamped_int(payload.get("finalOutput"), 0, 0),
            "canonicalDropReasons": {
                "missing_title": _clamped_int(drop_reasons.get("missing_title"), 0, 0),
                "missing_company": _clamped_int(drop_reasons.get("missing_company"), 0, 0),
                "missing_job_link": _clamped_int(drop_reasons.get("missing_job_link"), 0, 0),
                "invalid_url": _clamped_int(drop_reasons.get("invalid_url"), 0, 0),
                "invalid_payload": _clamped_int(drop_reasons.get("invalid_payload"), 0, 0),
            },
            "scrapyRunnerRejectedValidation": _clamped_int(payload.get("scrapyRunnerRejectedValidation"), 0, 0),
            "scrapyParentInvalidPayload": _clamped_int(payload.get("scrapyParentInvalidPayload"), 0, 0),
            "staticNonJobUrlRejected": _clamped_int(payload.get("staticNonJobUrlRejected"), 0, 0),
            "staticDuplicateLinkRejected": _clamped_int(payload.get("staticDuplicateLinkRejected"), 0, 0),
            "staticDetailParseEmpty": _clamped_int(payload.get("staticDetailParseEmpty"), 0, 0),
        }

    normalized = {
        "name": clean_text(src.get("name")),
        "status": norm_text(src.get("status")) or "error",
        "adapter": clean_text(src.get("adapter")) or "custom",
        "fetchStrategy": clean_text(src.get("fetchStrategy")) or "auto",
        "studio": clean_text(src.get("studio")),
        "fetchedCount": _clamped_int(src.get("fetchedCount"), 0, 0),
        "keptCount": _clamped_int(src.get("keptCount"), 0, 0),
        "lowConfidenceDropped": _clamped_int(src.get("lowConfidenceDropped"), 0, 0),
        "error": clean_text(src.get("error")),
        "durationMs": _clamped_int(src.get("durationMs"), 0, 0),
    }
    raw_stage_timings = src.get("stageTimingsMs") if isinstance(src.get("stageTimingsMs"), dict) else {}
    clean_stage_timings = {
        "listingFetch": _clamped_int(raw_stage_timings.get("listingFetch"), 0, 0),
        "parseCsv": _clamped_int(raw_stage_timings.get("parseCsv"), 0, 0),
        "candidateExtraction": _clamped_int(raw_stage_timings.get("candidateExtraction"), 0, 0),
        "detailFetch": _clamped_int(raw_stage_timings.get("detailFetch"), 0, 0),
        "redirectResolve": _clamped_int(raw_stage_timings.get("redirectResolve"), 0, 0),
        "canonicalization": _clamped_int(raw_stage_timings.get("canonicalization"), 0, 0),
    }
    if any(clean_stage_timings.values()):
        normalized["stageTimingsMs"] = clean_stage_timings
    exclusion_reason = clean_text(src.get("exclusionReason"))
    if exclusion_reason:
        normalized["exclusionReason"] = exclusion_reason
    if isinstance(src.get("loss"), dict):
        normalized["loss"] = _normalize_loss(src.get("loss"))
    details = src.get("details")
    if isinstance(details, list):
        clean_details: List[Any] = []
        for item in details:
            if isinstance(item, dict):
                clean_item = {
                    "adapter": clean_text(item.get("adapter")),
                    "studio": clean_text(item.get("studio")),
                    "name": clean_text(item.get("name")),
                    "status": norm_text(item.get("status")) or "error",
                    "fetchedCount": _clamped_int(item.get("fetchedCount"), 0, 0),
                    "keptCount": _clamped_int(item.get("keptCount"), 0, 0),
                    "error": clean_text(item.get("error")),
                    "classification": clean_text(item.get("classification")) or "",
                    "browserFallbackRecommended": bool(item.get("browserFallbackRecommended")),
                }
                top_reject_reasons = item.get("top_reject_reasons")
                if isinstance(top_reject_reasons, list):
                    clean_item["top_reject_reasons"] = [
                        clean_text(reason) for reason in top_reject_reasons if clean_text(reason)
                    ][:5]
                stats = item.get("stats")
                if isinstance(stats, dict):
                    clean_item["stats"] = {
                        "downloader/request_count": _clamped_int(stats.get("downloader/request_count"), 0, 0),
                        "downloader/response_count": _clamped_int(stats.get("downloader/response_count"), 0, 0),
                        "downloader/response_status_count/200": _clamped_int(
                            stats.get("downloader/response_status_count/200"), 0, 0
                        ),
                        "retry/count": _clamped_int(stats.get("retry/count"), 0, 0),
                        "item_scraped_count": _clamped_int(stats.get("item_scraped_count"), 0, 0),
                        "candidate_links_found": _clamped_int(stats.get("candidate_links_found"), 0, 0),
                        "detail_pages_visited": _clamped_int(stats.get("detail_pages_visited"), 0, 0),
                        "jobs_emitted": _clamped_int(stats.get("jobs_emitted"), 0, 0),
                        "fetch_cache_hits": _clamped_int(stats.get("fetch_cache_hits"), 0, 0),
                        "detail_yield_percent": _clamped_int(stats.get("detail_yield_percent"), 0, 0),
                        "redirect_candidates": _clamped_int(stats.get("redirect_candidates"), 0, 0),
                        "redirect_resolved": _clamped_int(stats.get("redirect_resolved"), 0, 0),
                        "redirect_cache_hits": _clamped_int(stats.get("redirect_cache_hits"), 0, 0),
                        "parse_csv_ms": _clamped_int(stats.get("parse_csv_ms"), 0, 0),
                        "listing_fetch_ms": _clamped_int(stats.get("listing_fetch_ms"), 0, 0),
                        "candidate_extraction_ms": _clamped_int(stats.get("candidate_extraction_ms"), 0, 0),
                        "detail_fetch_ms": _clamped_int(stats.get("detail_fetch_ms"), 0, 0),
                        "redirect_resolve_ms": _clamped_int(stats.get("redirect_resolve_ms"), 0, 0),
                        "jobs_rejected_validation": _clamped_int(stats.get("jobs_rejected_validation"), 0, 0),
                        "finish_reason": clean_text(stats.get("finish_reason")),
                    }
                if isinstance(item.get("loss"), dict):
                    clean_item["loss"] = _normalize_loss(item.get("loss"))
                source_id = clean_text(item.get("sourceId"))
                if source_id:
                    clean_item["sourceId"] = source_id
                pages = item.get("pages")
                if isinstance(pages, list):
                    clean_pages = [clean_text(page) for page in pages if clean_text(page)]
                    if clean_pages:
                        clean_item["pages"] = clean_pages
                clean_details.append(clean_item)
                continue
            text = clean_text(item)
            if text:
                clean_details.append(text)
        if clean_details:
            normalized["details"] = clean_details
    return normalized


def normalize_task_state_payload(
    payload: Dict[str, Any],
    *,
    started_at: str,
    finished_at: str = "",
    report_path: str = "",
) -> Dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    rows = src.get("tasks")
    normalized_rows: List[Dict[str, Any]] = []
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            normalized_rows.append({
                "name": clean_text(row.get("name")),
                "status": norm_text(row.get("status")) or "queued",
                "startedAt": clean_text(row.get("startedAt")),
                "finishedAt": clean_text(row.get("finishedAt")),
                "durationMs": _clamped_int(row.get("durationMs"), 0, 0),
                "heartbeatAt": clean_text(row.get("heartbeatAt")),
                "error": clean_text(row.get("error")),
            })
    summary = src.get("summary") if isinstance(src.get("summary"), dict) else {}
    return {
        "schemaVersion": SCHEMA_VERSION,
        "startedAt": clean_text(src.get("startedAt")) or clean_text(started_at),
        "finishedAt": clean_text(src.get("finishedAt")) or clean_text(finished_at),
        "summary": {
            "queued": _clamped_int(summary.get("queued"), 0, 0),
            "running": _clamped_int(summary.get("running"), 0, 0),
            "ok": _clamped_int(summary.get("ok"), 0, 0),
            "error": _clamped_int(summary.get("error"), 0, 0),
        },
        "tasks": normalized_rows,
        "outputs": {"report": clean_text((src.get("outputs") or {}).get("report")) or clean_text(report_path)},
    }


def normalize_fetch_report_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    summary = src.get("summary") if isinstance(src.get("summary"), dict) else {}
    outputs = src.get("outputs") if isinstance(src.get("outputs"), dict) else {}
    changed = outputs.get("changed") if isinstance(outputs.get("changed"), dict) else {}
    source_rows_raw = src.get("sources")
    source_rows = source_rows_raw if isinstance(source_rows_raw, list) else []
    runtime = src.get("runtime") if isinstance(src.get("runtime"), dict) else {}
    return {
        "schemaVersion": SCHEMA_VERSION,
        "startedAt": clean_text(src.get("startedAt")),
        "finishedAt": clean_text(src.get("finishedAt")),
        "runtime": normalize_runtime_payload(runtime, selected_source_count=len(source_rows)),
        "summary": dict(summary),
        "sources": [normalize_source_report_row(row) for row in source_rows if isinstance(row, dict)],
        "outputs": {
            "json": clean_text(outputs.get("json")),
            "csv": clean_text(outputs.get("csv")),
            "lightJson": clean_text(outputs.get("lightJson")),
            "report": clean_text(outputs.get("report")),
            "lifecycleState": clean_text(outputs.get("lifecycleState")),
            "browserFallbackQueue": clean_text(outputs.get("browserFallbackQueue")),
            "changed": {
                "json": bool(changed.get("json")),
                "csv": bool(changed.get("csv")),
                "lightJson": bool(changed.get("lightJson")),
            },
        },
    }


def should_skip_source_by_ttl(source_name: str, state_rows: Dict[str, Dict[str, Any]], ttl_minutes: int) -> bool:
    if ttl_minutes <= 0:
        return False
    entry = state_rows.get(source_name)
    if not isinstance(entry, dict):
        return False
    if int(entry.get("consecutiveFailures") or 0) > 0:
        return False
    last_success = parse_datetime(entry.get("lastSuccessAt"))
    if not last_success:
        return False
    age_seconds = max(0.0, (datetime.now(timezone.utc) - last_success).total_seconds())
    return age_seconds < float(ttl_minutes * 60)


def should_skip_source_by_cadence(
    source_name: str,
    state_rows: Dict[str, Dict[str, Any]],
    *,
    hot_minutes: int,
    cold_minutes: int,
) -> bool:
    entry = state_rows.get(source_name)
    if not isinstance(entry, dict):
        return False
    if int(entry.get("consecutiveFailures") or 0) > 0:
        return False
    baseline = parse_datetime(entry.get("lastSuccessAt"))
    if not baseline:
        return False
    cadence_minutes = max(1, int(cold_minutes or 1))
    last_changed = parse_datetime(entry.get("lastChangedAt"))
    if last_changed:
        age_since_change_seconds = max(0.0, (datetime.now(timezone.utc) - last_changed).total_seconds())
        if age_since_change_seconds <= 24 * 60 * 60:
            cadence_minutes = max(1, int(hot_minutes or 1))
    age_seconds = max(0.0, (datetime.now(timezone.utc) - baseline).total_seconds())
    return age_seconds < float(cadence_minutes * 60)


def circuit_breaker_until(source_name: str, state_rows: Dict[str, Dict[str, Any]], failure_threshold: int) -> Optional[datetime]:
    if failure_threshold <= 0:
        return None
    entry = state_rows.get(source_name)
    if not isinstance(entry, dict):
        return None
    if int(entry.get("consecutiveFailures") or 0) < failure_threshold:
        return None
    until = parse_datetime(entry.get("quarantinedUntilAt"))
    if until:
        return until
    return None


def _build_excluded_source_report(source_name: str, reason: str) -> Dict[str, Any]:
    return {
        "name": source_name,
        "status": "excluded",
        "adapter": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("adapter")) or "custom",
        "fetchStrategy": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("fetchStrategy")) or "auto",
        "studio": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("studio")) or "",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": clean_text(reason),
        "exclusionReason": clean_text(reason),
        "durationMs": 0,
    }


def apply_circuit_breaker_exclusions(
    selected_loaders: List[Tuple[str, SourceLoader]],
    *,
    source_state_rows: Dict[str, Dict[str, Any]],
    circuit_breaker_failures: int,
    circuit_breaker_cooldown_minutes: int,
    ignore_circuit_breaker: bool,
) -> Tuple[List[Tuple[str, SourceLoader]], List[Dict[str, Any]]]:
    if ignore_circuit_breaker or circuit_breaker_failures <= 0 or circuit_breaker_cooldown_minutes <= 0:
        return list(selected_loaders), []
    filtered: List[Tuple[str, SourceLoader]] = []
    excluded_rows: List[Dict[str, Any]] = []
    now_dt = datetime.now(timezone.utc)
    for name, loader in selected_loaders:
        blocked_until = circuit_breaker_until(name, source_state_rows, circuit_breaker_failures)
        if blocked_until and blocked_until > now_dt:
            excluded_rows.append(_build_excluded_source_report(name, f"circuit_breaker_active_until:{blocked_until.isoformat()}"))
            continue
        filtered.append((name, loader))
    return filtered, excluded_rows


def append_excluded_default_sources(source_reports: List[Dict[str, Any]]) -> None:
    for source_name, reason in EXCLUDED_DEFAULT_SOURCES.items():
        source_reports.append(_build_excluded_source_report(source_name, reason))


def update_source_state_rows(
    *,
    source_state_rows: Dict[str, Dict[str, Any]],
    source_reports: List[Dict[str, Any]],
    canonical_rows: List[RawJob],
    finished_at: str,
    circuit_breaker_failures: int,
    circuit_breaker_cooldown_minutes: int,
) -> Dict[str, Dict[str, Any]]:
    for report in source_reports:
        name = clean_text(report.get("name"))
        if not name:
            continue
        entry = dict(source_state_rows.get(name) or {})
        entry["lastRunAt"] = finished_at
        entry["lastStatus"] = clean_text(report.get("status"))
        entry["lastDurationMs"] = int(report.get("durationMs") or 0)
        entry["lastFetchedCount"] = int(report.get("fetchedCount") or 0)
        entry["lastKeptCount"] = int(report.get("keptCount") or 0)
        details = report.get("details") if isinstance(report.get("details"), list) else []
        static_detail = details[0] if len(details) == 1 and isinstance(details[0], dict) else {}
        static_stats = static_detail.get("stats") if isinstance(static_detail, dict) and isinstance(static_detail.get("stats"), dict) else {}
        entry["lastCandidateLinksFound"] = int(static_stats.get("candidate_links_found") or 0)
        entry["lastDetailPagesVisited"] = int(static_stats.get("detail_pages_visited") or 0)
        entry["lastDetailYieldPct"] = int(static_stats.get("detail_yield_percent") or 0)
        entry["lastRedirectCandidates"] = int(static_stats.get("redirect_candidates") or 0)
        entry["lastRedirectResolved"] = int(static_stats.get("redirect_resolved") or 0)
        entry["lastRedirectCacheHits"] = int(static_stats.get("redirect_cache_hits") or 0)
        stage_timings = report.get("stageTimingsMs") if isinstance(report.get("stageTimingsMs"), dict) else {}
        clean_stage_timings = {
            "listingFetch": int(stage_timings.get("listingFetch") or 0),
            "parseCsv": int(stage_timings.get("parseCsv") or 0),
            "candidateExtraction": int(stage_timings.get("candidateExtraction") or 0),
            "detailFetch": int(stage_timings.get("detailFetch") or 0),
            "redirectResolve": int(stage_timings.get("redirectResolve") or 0),
            "canonicalization": int(stage_timings.get("canonicalization") or 0),
        }
        if any(clean_stage_timings.values()):
            entry["lastStageTimingsMs"] = clean_stage_timings
        else:
            entry.pop("lastStageTimingsMs", None)
        if entry["lastStatus"] == "ok":
            entry["lastSuccessAt"] = finished_at
            reported_fingerprint = clean_text(report.get("sourceFingerprint"))
            if not reported_fingerprint and entry["lastKeptCount"] > 0:
                reported_fingerprint = source_rows_fingerprint(
                    [row for row in canonical_rows if clean_text(row.get("source")) == name]
                )
            previous_fingerprint = clean_text(entry.get("lastFingerprint"))
            if reported_fingerprint:
                entry["lastFingerprint"] = reported_fingerprint
                if reported_fingerprint != previous_fingerprint:
                    entry["lastChangedAt"] = finished_at
            entry["consecutiveFailures"] = 0
            entry.pop("quarantinedUntilAt", None)
            entry.pop("lastFailureAt", None)
            entry.pop("lastError", None)
        elif entry["lastStatus"] == "error":
            failure_count = int(entry.get("consecutiveFailures") or 0) + 1
            entry["consecutiveFailures"] = failure_count
            entry["lastFailureAt"] = finished_at
            entry["lastError"] = clean_text(report.get("error"))
            if circuit_breaker_failures > 0 and failure_count >= circuit_breaker_failures and circuit_breaker_cooldown_minutes > 0:
                entry["quarantinedUntilAt"] = (
                    datetime.now(timezone.utc) + timedelta(minutes=circuit_breaker_cooldown_minutes)
                ).isoformat()
        source_state_rows[name] = entry
    return source_state_rows


def run_pipeline(
    *,
    output_dir: Path,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    retries: int = DEFAULT_RETRIES,
    backoff_s: float = DEFAULT_BACKOFF_S,
    preserve_previous_on_empty: bool = True,
    fetch_text: Callable[[str, int], str] = default_fetch_text,
    source_loaders: Optional[List[Tuple[str, SourceLoader]]] = None,
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
    social_config_path: Optional[Path] = None,
    social_lookback_minutes: int = DEFAULT_SOCIAL_LOOKBACK_MINUTES,
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    show_progress: bool = True,
    selection_exclusions: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
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
    )


def parse_args() -> argparse.Namespace:
    from src.jobs import pipeline as pipeline_pkg

    return pipeline_pkg.parse_args()


def main() -> int:
    from src.jobs import pipeline as pipeline_pkg

    return pipeline_pkg.main()

if __name__ == "__main__":
    raise SystemExit(main())

