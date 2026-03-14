#!/usr/bin/env python3
"""Aggregate game job listings into unified JSON/CSV feeds."""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import hashlib
import inspect
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

from scripts.contracts import SCHEMA_VERSION
from scripts.baluffo_config import get_storage_defaults
from scripts.jobs_fetcher_registry import (
    DEFAULT_SOURCE_LOADER_NAMES,
    EXCLUDED_DEFAULT_SOURCES,
    SOURCE_REPORT_META,
)
from scripts.pipeline_io import (
    read_existing_output as read_existing_output_from_file,
    serialize_rows_for_csv,
    serialize_rows_for_json,
    write_text_if_changed,
)

RawJob = Dict[str, Any]
SourceLoader = Callable[..., List[RawJob]]

DEFAULT_GOOGLE_SHEET_ID = "1ZOJpVS3CcnrkwhpRgkP7tzf3wc4OWQj-uoWFfv4oHZE"
DEFAULT_GOOGLE_SHEET_GID = "1560329579"
GOOGLE_SHEETS_SOURCES = [
    {"name": "google_sheets", "sheetId": DEFAULT_GOOGLE_SHEET_ID, "gid": DEFAULT_GOOGLE_SHEET_GID},
    {"name": "google_sheets_1er2oaxo", "sheetId": "1eR2oAXOuflr8CZeGoz3JTrsgNj3KuefbdXJOmNtjEVM", "gid": "0"},
    {"name": "google_sheets_1mvqhxat", "sheetId": "1MvqHXAtXP_6ogtfrLM0g_RzGdJQyx5Q8mhPX4lZECkI", "gid": "0"},
]
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
TRACKING_QUERY_KEYS = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "source"}
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

GAME_KEYWORDS = {
    "game",
    "gaming",
    "unity",
    "unreal",
    "gamedev",
    "gameplay",
    "technical artist",
    "tech art",
    "tech artist",
    "shader",
    "shader artist",
    "material artist",
    "world artist",
    "terrain artist",
    "environment art",
    "environment artist",
    "character artist",
    "engine programmer",
    "graphics programmer",
}
SOCIAL_HIRING_KEYWORDS = {
    "hiring",
    "we're hiring",
    "we are hiring",
    "job opening",
    "open role",
    "join our team",
    "looking for",
    "vacancy",
    "position",
    "apply now",
    "paid",
}
SOCIAL_FOR_HIRE_KEYWORDS = {
    "for hire",
    "available for work",
    "looking for work",
    "hire me",
    "open to work",
}

COUNTRY_NAME_TO_CODE = {
    "united states": "US",
    "usa": "US",
    "united kingdom": "GB",
    "uk": "GB",
    "netherlands": "NL",
    "italy": "IT",
    "france": "FR",
    "germany": "DE",
    "sweden": "SE",
    "norway": "NO",
    "denmark": "DK",
    "spain": "ES",
    "brazil": "BR",
    "india": "IN",
    "canada": "CA",
    "remote": "Remote",
}

TARGET_PROFESSIONS = {"technical-artist", "environment-artist"}
SOURCE_DIAGNOSTICS: Dict[str, Dict[str, Any]] = {}


def registry_entries(adapter: str, *, enabled_only: bool = True) -> List[Dict[str, Any]]:
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
    return datetime.now(timezone.utc).isoformat()


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", clean_text(value)).strip().lower()


def env_flag(name: str, default: bool) -> bool:
    raw = norm_text(os.getenv(name))
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(default)


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


def normalize_url(url: Any) -> str:
    raw = clean_text(url)
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
    except ValueError:
        return ""
    if parsed.scheme not in {"http", "https"}:
        return ""
    pairs = []
    for key, values in parse_qs(parsed.query, keep_blank_values=True).items():
        lower_key = key.lower()
        if lower_key.startswith("utm_") or lower_key in TRACKING_QUERY_KEYS:
            continue
        for value in values:
            pairs.append((key, value))
    pairs.sort(key=lambda item: (item[0].lower(), item[1]))
    query = urlencode(pairs, doseq=True)
    path = parsed.path.rstrip("/") or "/"
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, "", query, ""))


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
        from scripts.jobs import transport as transport_pkg

        return transport_pkg.PooledRedirectResolver(
            timeout_s=timeout_s,
            max_connections=max_connections,
        )


def build_redirect_resolver(
    *,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    max_connections: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
) -> PooledRedirectResolver:
    from scripts.jobs import transport as transport_pkg

    return transport_pkg.build_redirect_resolver(timeout_s=timeout_s, max_connections=max_connections)


def normalize_country(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return "Unknown"
    if text == "Remote":
        return "Remote"
    if len(text) == 2 and text.isalpha():
        return text.upper()
    lower = text.lower()
    return COUNTRY_NAME_TO_CODE.get(lower, text)


def normalize_work_type(value: Any) -> str:
    lower = norm_text(value)
    if "remote" in lower:
        return "Remote"
    if "hybrid" in lower or "mixed" in lower:
        return "Hybrid"
    return "Onsite"


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


def normalize_sector(value: Any, company: Any = "", title: Any = "") -> str:
    lower = norm_text(value)
    if re.search(r"\b(game|gaming|esports|studio|publisher)\b", lower):
        return "Game"
    if re.search(r"\b(tech|technology|software|it)\b", lower):
        return "Tech"
    return "Game" if classify_company_type(company, title) == "Game" else "Tech"


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


def looks_like_game_job(*values: Any) -> bool:
    text = " ".join(norm_text(value) for value in values if value is not None)
    return bool(text) and any(keyword in text for keyword in GAME_KEYWORDS)


def find_column_index(headers: Sequence[str], exact_names: Sequence[str], contains_names: Sequence[str]) -> int:
    normalized = [norm_text(header) for header in headers]
    for name in exact_names:
        needle = norm_text(name)
        if needle in normalized:
            return normalized.index(needle)
    for idx, header in enumerate(normalized):
        if any(norm_text(name) in header for name in contains_names):
            return idx
    return -1


def find_company_column(headers: Sequence[str]) -> int:
    normalized = [norm_text(header) for header in headers]
    for idx, header in enumerate(normalized):
        if header in {"company", "company name", "studio", "employer", "organization", "organisation"}:
            return idx
    for idx, header in enumerate(normalized):
        if (
            ("company" in header or "studio" in header or "employer" in header or "organization" in header or "organisation" in header)
            and not any(part in header for part in ("type", "category", "sector", "industry"))
        ):
            return idx
    return -1


def company_name_candidate_indexes(headers: Sequence[str], primary_idx: int) -> List[int]:
    normalized = [norm_text(header) for header in headers]
    seen = set()
    candidates: List[int] = []

    def push(index: int) -> None:
        if index < 0 or index >= len(headers) or index in seen:
            return
        seen.add(index)
        candidates.append(index)

    push(primary_idx)
    for idx, header in enumerate(normalized):
        name_like = (
            "company name" in header
            or header == "company"
            or "studio" in header
            or "employer" in header
            or "organization" in header
            or "organisation" in header
        )
        type_like = any(part in header for part in ("type", "category", "sector", "industry"))
        if name_like and not type_like:
            push(idx)
    return candidates


def google_sheets_link_candidate_indexes(headers: Sequence[str], primary_idx: int) -> List[int]:
    normalized = [norm_text(header) for header in headers]
    seen = set()
    candidates: List[int] = []

    def push(index: int) -> None:
        if index < 0 or index >= len(headers) or index in seen:
            return
        seen.add(index)
        candidates.append(index)

    push(primary_idx)
    for idx, header in enumerate(normalized):
        if header in {"job link", "url", "apply", "link", "source/contact", "source / contact", "source", "contact"}:
            push(idx)
            continue
        if any(token in header for token in ("job link", "apply", "source/contact", "source / contact")):
            push(idx)
            continue
        if header == "url":
            push(idx)
            continue
        if header == "link":
            push(idx)
            continue
        if header == "source" or header == "contact":
            push(idx)
    return candidates


def resolve_google_sheets_job_link(row: Sequence[str], candidate_indexes: Sequence[int]) -> str:
    for idx in candidate_indexes:
        if idx < 0 or idx >= len(row):
            continue
        raw_value = clean_text(row[idx])
        if not raw_value:
            continue
        if raw_value.lower().startswith("mailto:"):
            continue
        if re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", raw_value):
            continue
        normalized = normalize_url(raw_value)
        if normalized:
            return normalized
    return ""


def is_untrustworthy_company_label(value: str) -> bool:
    return norm_text(value) in UNTRUSTWORTHY_COMPANY_LABELS


def normalize_company_value(value: Any) -> str:
    company = clean_text(value)
    if not company:
        return ""
    if is_untrustworthy_company_label(company):
        return UNKNOWN_COMPANY_LABEL
    return company


def resolve_company_name(row: Sequence[str], primary_idx: int, candidate_indexes: Sequence[int]) -> str:
    values: List[str] = []
    if 0 <= primary_idx < len(row):
        values.append(clean_text(row[primary_idx]))
    for idx in candidate_indexes:
        if 0 <= idx < len(row):
            values.append(clean_text(row[idx]))
    for value in values:
        if value and not is_untrustworthy_company_label(value):
            return value
    for value in values:
        normalized = normalize_company_value(value)
        if normalized:
            return normalized
    return ""


def parse_google_sheets_csv(csv_text: str) -> List[RawJob]:
    rows = list(csv.reader(StringIO(csv_text)))
    if len(rows) < 2:
        return []

    header_idx = -1
    for idx, row in enumerate(rows[:250]):
        normalized = [norm_text(cell) for cell in row if norm_text(cell)]
        if not normalized:
            continue
        has_title = any(
            token in header
            for header in normalized
            for token in ("title", "role", "job", "position")
        )
        has_company = any(
            token in header
            for header in normalized
            for token in ("company", "studio", "employer", "organization", "organisation")
        )
        has_location = "city" in normalized or "country" in normalized or "postal code" in normalized or "location" in normalized
        if has_title and has_company and has_location:
            header_idx = idx
            break
    if header_idx < 0:
        return []

    headers = [clean_text(header) for header in rows[header_idx]]
    company_idx = find_company_column(headers)
    company_candidates = company_name_candidate_indexes(headers, company_idx)
    title_idx = find_column_index(headers, ["title", "role", "job", "position"], ["title", "role", "job", "position"])
    city_idx = find_column_index(headers, ["city"], ["city"])
    country_idx = find_column_index(headers, ["country"], ["country"])
    location_idx = find_column_index(
        headers,
        ["location type", "work type", "fully remote", "remote"],
        ["location", "work type", "remote", "fully remote"],
    )
    contract_idx = find_column_index(
        headers,
        ["employment type", "contract type", "employment", "contract", "job type"],
        ["employment", "contract", "job type"],
    )
    link_idx = find_column_index(headers, ["job link", "url", "apply", "link"], ["job link", "url", "apply", "link"])
    link_candidates = google_sheets_link_candidate_indexes(headers, link_idx)
    sector_idx = find_column_index(
        headers,
        ["sector", "industry", "company type", "company category", "job category"],
        ["sector", "industry", "company type", "company category", "job category"],
    )

    default_country = "Unknown"
    if country_idx < 0 and "german games industry" in norm_text(csv_text[:3000]):
        default_country = "Germany"

    if title_idx < 0 or company_idx < 0:
        return []

    jobs: List[RawJob] = []
    for idx in range(header_idx + 1, len(rows)):
        row = rows[idx]
        title = clean_text(row[title_idx] if title_idx < len(row) else "")
        company = resolve_company_name(row, company_idx, company_candidates)
        if not title or not company:
            continue
        jobs.append(
            {
                "sourceJobId": f"sheet-{idx}",
                "title": title,
                "company": company,
                "city": clean_text(row[city_idx] if 0 <= city_idx < len(row) else ""),
                "country": clean_text(row[country_idx] if 0 <= country_idx < len(row) else default_country),
                "workType": clean_text(row[location_idx] if 0 <= location_idx < len(row) else "On-site"),
                "contractType": clean_text(row[contract_idx] if 0 <= contract_idx < len(row) else ""),
                "jobLink": resolve_google_sheets_job_link(row, link_candidates),
                "sector": clean_text(row[sector_idx] if 0 <= sector_idx < len(row) else ""),
            }
        )
    return jobs

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


def social_extract_urls(text: str) -> List[str]:
    return [normalize_url(url) for url in re.findall(r"https?://[^\s<>()\"']+", clean_text(text)) if normalize_url(url)]


def social_extract_apply_url(*texts: Any) -> str:
    blocked_hosts = {
        "reddit.com",
        "www.reddit.com",
        "x.com",
        "www.x.com",
        "twitter.com",
        "www.twitter.com",
        "t.co",
        "mastodon.gamedev.place",
        "xcancel.com",
        "rss.xcancel.com",
    }
    for text in texts:
        for url in social_extract_urls(clean_text(text)):
            host = clean_text(urlparse(url).netloc).lower()
            if host in blocked_hosts:
                continue
            return url
    return ""


def social_infer_company(*texts: Any, fallback: str = "") -> str:
    corpus = " ".join(clean_text(text) for text in texts if clean_text(text))
    patterns = (
        r"\bat\s+([A-Z][A-Za-z0-9& .'\-]{2,})",
        r"\bjoin\s+([A-Z][A-Za-z0-9& .'\-]{2,})",
        r"\b([A-Z][A-Za-z0-9& .'\-]{2,})\s+is\s+hiring",
    )
    for pattern in patterns:
        match = re.search(pattern, corpus)
        if match:
            candidate = clean_text(match.group(1)).strip(" .,:;")
            candidate = re.split(r"\b(remote|apply|role|position|job)\b", candidate, maxsplit=1, flags=re.IGNORECASE)[0].strip(" .,:;-")
            words = [part for part in candidate.split() if part]
            if len(words) > 6:
                candidate = " ".join(words[:6])
            if candidate:
                return candidate
    return clean_text(fallback) or "Unknown Studio"


def social_compute_confidence(*values: Any, has_apply_url: bool = False, has_remote_hint: bool = False) -> int:
    text = " ".join(norm_text(value) for value in values if value is not None)
    score = 0
    if any(token in text for token in SOCIAL_HIRING_KEYWORDS):
        score += 35
    if looks_like_game_job(text):
        score += 30
    if "job" in text or "role" in text or "position" in text:
        score += 10
    if has_apply_url:
        score += 20
    if has_remote_hint:
        score += 5
    if any(token in text for token in SOCIAL_FOR_HIRE_KEYWORDS):
        score -= 40
    return max(0, min(100, score))


def social_should_keep_post(
    *,
    title: str,
    text: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
    has_apply_url: bool,
) -> Tuple[bool, int]:
    normalized = f"{norm_text(title)} {norm_text(text)}"
    if reject_for_hire_posts and any(token in normalized for token in SOCIAL_FOR_HIRE_KEYWORDS):
        return False, 0
    confidence = social_compute_confidence(title, text, has_apply_url=has_apply_url, has_remote_hint=("remote" in normalized))
    return confidence >= max(0, min(100, int(min_confidence or 0))), confidence


def parse_reddit_json_payload(
    payload: Any,
    *,
    subreddit: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
) -> Tuple[List[RawJob], int]:
    rows: List[Dict[str, Any]] = []
    if isinstance(payload, dict):
        children = (((payload.get("data") or {}).get("children")) if isinstance(payload.get("data"), dict) else [])
        if isinstance(children, list):
            for child in children:
                if isinstance(child, dict) and isinstance(child.get("data"), dict):
                    rows.append(child["data"])
    out: List[RawJob] = []
    low_conf_count = 0
    for item in rows:
        title = clean_text(item.get("title"))
        body = clean_text(item.get("selftext"))
        flair = clean_text(item.get("link_flair_text"))
        post_id = clean_text(item.get("id"))
        permalink = normalize_url(f"https://www.reddit.com{clean_text(item.get('permalink'))}") if clean_text(item.get("permalink")) else ""
        external_url = normalize_url(item.get("url"))
        apply_url = social_extract_apply_url(body, external_url)
        keep, confidence = social_should_keep_post(
            title=title,
            text=f"{body} {flair}",
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            has_apply_url=bool(apply_url),
        )
        if not keep:
            low_conf_count += 1
            continue
        job_link = apply_url or permalink or external_url
        if not title or not job_link:
            continue
        company = social_infer_company(title, body, fallback=clean_text(item.get("author")))
        post_source_id = f"reddit:{clean_text(subreddit)}:{post_id or hashlib.sha1(job_link.encode('utf-8')).hexdigest()[:12]}"
        out.append({
            "sourceJobId": post_source_id,
            "title": title,
            "company": company,
            "city": "Remote" if "remote" in norm_text(f"{title} {body}") else "",
            "country": "Remote" if "remote" in norm_text(f"{title} {body}") else "Unknown",
            "workType": "Remote" if "remote" in norm_text(f"{title} {body}") else "",
            "contractType": clean_text(flair),
            "jobLink": job_link,
            "sector": "Game",
            "postedAt": item.get("created_utc"),
            "adapter": "social",
            "studio": f"reddit/{clean_text(subreddit)}",
            "sourceBundle": [{
                "source": "social_reddit",
                "sourceJobId": post_source_id,
                "jobLink": permalink or job_link,
                "postedAt": item.get("created_utc"),
                "adapter": "social",
                "studio": clean_text(subreddit),
            }],
        })
    return out, low_conf_count


def parse_reddit_rss_payload(
    rss_text: str,
    *,
    subreddit: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
) -> Tuple[List[RawJob], int]:
    try:
        root = ET.fromstring(clean_text(rss_text).lstrip())
    except ET.ParseError:
        return [], 0
    items = root.findall(".//item")
    out: List[RawJob] = []
    low_conf_count = 0
    for item in items:
        title = clean_text(item.findtext("title"))
        link = normalize_url(item.findtext("link"))
        description = strip_html_text(unescape(clean_text(item.findtext("description"))))
        apply_url = social_extract_apply_url(description, link)
        keep, confidence = social_should_keep_post(
            title=title,
            text=description,
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            has_apply_url=bool(apply_url),
        )
        if not keep:
            low_conf_count += 1
            continue
        if not title or not link:
            continue
        company = social_infer_company(title, description, fallback=clean_text(subreddit))
        post_source_id = f"reddit:{clean_text(subreddit)}:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:12]}"
        out.append({
            "sourceJobId": post_source_id,
            "title": title,
            "company": company,
            "city": "Remote" if "remote" in norm_text(f"{title} {description}") else "",
            "country": "Remote" if "remote" in norm_text(f"{title} {description}") else "Unknown",
            "workType": "Remote" if "remote" in norm_text(f"{title} {description}") else "",
            "contractType": "Unknown",
            "jobLink": apply_url or link,
            "sector": "Game",
            "postedAt": clean_text(item.findtext("pubDate")),
            "adapter": "social",
            "studio": f"reddit/{clean_text(subreddit)}",
            "sourceBundle": [{
                "source": "social_reddit",
                "sourceJobId": post_source_id,
                "jobLink": link,
                "postedAt": clean_text(item.findtext("pubDate")),
                "adapter": "social",
                "studio": clean_text(subreddit),
            }],
        })
    return out, low_conf_count


def parse_x_payload(
    payload: Any,
    *,
    query_label: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
) -> Tuple[List[RawJob], int]:
    rows = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), list) else []
    out: List[RawJob] = []
    low_conf_count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        text = clean_text(row.get("text"))
        post_id = clean_text(row.get("id"))
        entities = row.get("entities") if isinstance(row.get("entities"), dict) else {}
        entity_urls = entities.get("urls") if isinstance(entities.get("urls"), list) else []
        expanded_urls = [clean_text(item.get("expanded_url")) for item in entity_urls if isinstance(item, dict)]
        apply_url = social_extract_apply_url(text, " ".join(expanded_urls))
        keep, confidence = social_should_keep_post(
            title=text,
            text=text,
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            has_apply_url=bool(apply_url),
        )
        if not keep:
            low_conf_count += 1
            continue
        permalink = normalize_url(f"https://x.com/i/web/status/{post_id}") if post_id else ""
        company = social_infer_company(text, fallback="Unknown Studio")
        post_source_id = f"x:{post_id or hashlib.sha1(text.encode('utf-8')).hexdigest()[:12]}"
        out.append({
            "sourceJobId": post_source_id,
            "title": clean_text(text[:180]),
            "company": company,
            "city": "Remote" if "remote" in norm_text(text) else "",
            "country": "Remote" if "remote" in norm_text(text) else "Unknown",
            "workType": "Remote" if "remote" in norm_text(text) else "",
            "contractType": clean_text(query_label),
            "jobLink": apply_url or permalink,
            "sector": "Game",
            "postedAt": clean_text(row.get("created_at")),
            "adapter": "social",
            "studio": "x",
            "sourceBundle": [{
                "source": "social_x",
                "sourceJobId": post_source_id,
                "jobLink": permalink or apply_url,
                "postedAt": clean_text(row.get("created_at")),
                "adapter": "social",
                "studio": "x",
            }],
        })
    return out, low_conf_count


def parse_x_rss_payload(
    rss_text: str,
    *,
    query_label: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
) -> Tuple[List[RawJob], int]:
    raw_text = clean_text(rss_text).lstrip()
    safe_text = re.sub(r"&(?!amp;|lt;|gt;|quot;|apos;|#\d+;)", "&amp;", raw_text)
    try:
        root = ET.fromstring(safe_text)
    except ET.ParseError:
        return [], 0
    items = root.findall(".//item")
    out: List[RawJob] = []
    low_conf_count = 0
    for item in items:
        title = clean_text(item.findtext("title"))
        link = normalize_url(item.findtext("link"))
        description = strip_html_text(unescape(clean_text(item.findtext("description"))))
        banner_text = norm_text(f"{title} {description}")
        if "not yet whitelisted" in banner_text or "rss reader" in banner_text:
            low_conf_count += 1
            continue
        text = f"{title} {description}"
        apply_url = social_extract_apply_url(text, link)
        keep, confidence = social_should_keep_post(
            title=title,
            text=text,
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            has_apply_url=bool(apply_url),
        )
        if not keep:
            low_conf_count += 1
            continue
        if not title or not link:
            continue
        post_id = hashlib.sha1(link.encode("utf-8")).hexdigest()[:12]
        company = social_infer_company(title, description, fallback="Unknown Studio")
        source_job_id = f"x:{post_id}"
        out.append({
            "sourceJobId": source_job_id,
            "title": clean_text(title[:180]),
            "company": company,
            "city": "Remote" if "remote" in norm_text(text) else "",
            "country": "Remote" if "remote" in norm_text(text) else "Unknown",
            "workType": "Remote" if "remote" in norm_text(text) else "",
            "contractType": clean_text(query_label),
            "jobLink": apply_url or link,
            "sector": "Game",
            "postedAt": clean_text(item.findtext("pubDate")),
            "adapter": "social",
            "studio": "x",
            "sourceBundle": [{
                "source": "social_x",
                "sourceJobId": source_job_id,
                "jobLink": link,
                "postedAt": clean_text(item.findtext("pubDate")),
                "adapter": "social",
                "studio": "x",
            }],
        })
    return out, low_conf_count


def parse_mastodon_payload(
    payload: Any,
    *,
    instance: str,
    tag: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
) -> Tuple[List[RawJob], int]:
    rows = payload if isinstance(payload, list) else []
    out: List[RawJob] = []
    low_conf_count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        html_text = clean_text(row.get("content"))
        text = strip_html_text(unescape(html_text))
        post_url = normalize_url(row.get("url"))
        card = row.get("card") if isinstance(row.get("card"), dict) else {}
        apply_url = social_extract_apply_url(text, clean_text(card.get("url")))
        keep, confidence = social_should_keep_post(
            title=text,
            text=text,
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            has_apply_url=bool(apply_url),
        )
        if not keep:
            low_conf_count += 1
            continue
        post_id = clean_text(row.get("id"))
        account = row.get("account") if isinstance(row.get("account"), dict) else {}
        account_name = clean_text(account.get("display_name") or account.get("acct"))
        company = social_infer_company(text, fallback=account_name)
        post_source_id = f"mastodon:{clean_text(urlparse(instance).netloc)}:{post_id or hashlib.sha1((post_url or text).encode('utf-8')).hexdigest()[:12]}"
        out.append({
            "sourceJobId": post_source_id,
            "title": clean_text(text[:180]),
            "company": company,
            "city": "Remote" if "remote" in norm_text(text) else "",
            "country": "Remote" if "remote" in norm_text(text) else "Unknown",
            "workType": "Remote" if "remote" in norm_text(text) else "",
            "contractType": clean_text(tag),
            "jobLink": apply_url or post_url,
            "sector": "Game",
            "postedAt": clean_text(row.get("created_at")),
            "adapter": "social",
            "studio": f"mastodon/{clean_text(urlparse(instance).netloc)}",
            "sourceBundle": [{
                "source": "social_mastodon",
                "sourceJobId": post_source_id,
                "jobLink": post_url or apply_url,
                "postedAt": clean_text(row.get("created_at")),
                "adapter": "social",
                "studio": clean_text(urlparse(instance).netloc),
            }],
        })
    return out, low_conf_count


def extract_json_ld_blocks(html_text: str) -> List[str]:
    return re.findall(r"(?is)<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>", html_text)


def strip_html_text(fragment: str) -> str:
    text = re.sub(r"(?is)<[^>]+>", " ", fragment or "")
    return re.sub(r"\s+", " ", text).strip()


def parse_gamesindustry_changed_date(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            continue
    return ""


def iter_job_postings_from_jsonld(value: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(value, list):
        for item in value:
            yield from iter_job_postings_from_jsonld(item)
        return
    if not isinstance(value, dict):
        return
    if clean_text(value.get("@type")) == "JobPosting":
        yield value
    for child in value.values():
        yield from iter_job_postings_from_jsonld(child)


def parse_jobposting_locations(job_location: Any) -> Tuple[str, str]:
    location = job_location
    if isinstance(location, list) and location:
        location = location[0]
    if not isinstance(location, dict):
        return "", "Unknown"

    address = location.get("address")
    if not isinstance(address, dict):
        return "", "Unknown"

    city = clean_text(address.get("addressLocality"))
    country = clean_text(address.get("addressCountry")) or "Unknown"
    return city, country


def parse_jobposting_company(hiring_org: Any, fallback_company: str = "") -> str:
    if isinstance(hiring_org, dict):
        name = clean_text(hiring_org.get("name"))
        if name:
            return name
    return clean_text(fallback_company) or "Unknown"


def parse_jobposting_source_id(identifier: Any, fallback: str = "") -> str:
    if isinstance(identifier, dict):
        value = clean_text(identifier.get("value"))
        if value:
            return value
    return clean_text(fallback)


def parse_jobpostings_from_html(
    html_text: str,
    *,
    base_url: str,
    fallback_company: str = "",
    fallback_source_id_prefix: str = "",
) -> List[RawJob]:
    jobs: List[RawJob] = []
    seen_links = set()
    counter = 0

    for block in extract_json_ld_blocks(html_text):
        decoded = unescape(block.strip())
        if not decoded:
            continue
        try:
            payload = json.loads(decoded)
        except json.JSONDecodeError:
            continue

        for row in iter_job_postings_from_jsonld(payload):
            title = clean_text(row.get("title"))
            if not title:
                continue
            job_link = clean_text(row.get("url"))
            if job_link:
                job_link = urljoin(base_url, job_link)
            else:
                job_link = normalize_url(base_url)
            if not job_link or job_link in seen_links:
                continue
            seen_links.add(job_link)
            counter += 1

            company = parse_jobposting_company(row.get("hiringOrganization"), fallback_company=fallback_company)
            city, country = parse_jobposting_locations(row.get("jobLocation"))
            source_id = parse_jobposting_source_id(
                row.get("identifier"),
                fallback=f"{fallback_source_id_prefix}-{counter}" if fallback_source_id_prefix else "",
            )

            jobs.append(
                {
                    "sourceJobId": source_id,
                    "title": title,
                    "company": company,
                    "city": city,
                    "country": country,
                    "workType": clean_text(row.get("jobLocationType") or ""),
                    "contractType": clean_text(row.get("employmentType") or ""),
                    "jobLink": job_link,
                    "sector": "Game",
                    "postedAt": row.get("datePosted"),
                }
            )
    return jobs


def maybe_fetch_kojima_job_listing_html(
    *,
    page_url: str,
    page_html: str,
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> str:
    """Kojima careers renders the full listing via /kjpviewloader/load POST."""
    if "kojimaproductions.jp" not in (urlparse(page_url).netloc or "").lower():
        return ""
    if "kjp_job_listing" not in page_html and "data-viewref=\"kjp_job_listing\"" not in page_html:
        return ""

    parsed = urlparse(page_url)
    path_parts = [part for part in (parsed.path or "").split("/") if part]
    lang_code = path_parts[0] if path_parts else "en"
    endpoint = f"{parsed.scheme or 'https'}://{parsed.netloc}/kjpviewloader/load"
    payload = {
        "viewName": "kjp_view_job_listing",
        "viewDisplayBase": "kjp_view_job_listing__",
        "langCode": clean_text(lang_code) or "en",
        "inputs": [
            {"name": "jobDiscipline", "value": "All"},
            {"name": "jobLocation", "value": "All"},
        ],
        "page": 0,
    }

    attempt = 0
    last_error: Exception | None = None
    while attempt <= max(0, retries):
        try:
            req = Request(
                endpoint,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0",
                },
                method="POST",
            )
            with urlopen(req, timeout=timeout_s) as response:
                text = response.read().decode("utf-8", errors="ignore")
            return text if clean_text(text) else ""
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= max(0, retries):
                break
            sleep_s = max(0.0, float(backoff_s)) * (attempt + 1)
            if sleep_s > 0:
                time.sleep(sleep_s)
            attempt += 1
            continue
    if last_error:
        raise last_error
    return ""


def parse_teamtailor_listing_links(html_text: str, base_url: str) -> List[str]:
    links = []
    seen = set()
    for href in re.findall(r'(?is)<a[^>]+href=["\']([^"\']+)["\']', html_text):
        absolute = urljoin(base_url, clean_text(href))
        parsed = urlparse(absolute)
        if "/jobs/" not in parsed.path:
            continue
        if "/jobs/show_more" in parsed.path:
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        links.append(absolute)
    return links


def parse_gamesindustry_html(html_text: str, base_url: str = "https://jobs.gamesindustry.biz") -> List[RawJob]:
    jobs: List[RawJob] = []
    seen_links = set()

    def push_job(row: RawJob) -> None:
        job_link = normalize_url(row.get("jobLink"))
        if not job_link:
            return
        if "/job/" not in urlparse(job_link).path:
            return
        if job_link in seen_links:
            return
        seen_links.add(job_link)
        row["jobLink"] = job_link
        jobs.append(row)

    for block in extract_json_ld_blocks(html_text):
        decoded = unescape(block.strip())
        if not decoded:
            continue
        try:
            payload = json.loads(decoded)
        except json.JSONDecodeError:
            continue
        for row in iter_job_postings_from_jsonld(payload):
            title = clean_text(row.get("title"))
            org = row.get("hiringOrganization") if isinstance(row.get("hiringOrganization"), dict) else {}
            company = clean_text(org.get("name"))
            location = row.get("jobLocation")
            if isinstance(location, list) and location:
                location = location[0]
            address = location.get("address") if isinstance(location, dict) and isinstance(location.get("address"), dict) else {}
            city = clean_text(address.get("addressLocality"))
            country = clean_text(address.get("addressCountry"))
            link = clean_text(row.get("url"))
            if link:
                link = urljoin(base_url, link)
            if not title or not company:
                continue
            identifier = row.get("identifier") if isinstance(row.get("identifier"), dict) else {}
            push_job(
                {
                    "sourceJobId": clean_text(identifier.get("value")),
                    "title": title,
                    "company": company,
                    "city": city,
                    "country": country,
                    "workType": clean_text(row.get("jobLocationType") or ""),
                    "contractType": clean_text(row.get("employmentType") or ""),
                    "jobLink": link,
                    "sector": "Game",
                    "postedAt": row.get("datePosted"),
                }
            )

    link_pattern = re.compile(
        r'(?is)<a[^>]+href=["\']([^"\']*/job/[^"\']+)["\'][^>]*class=["\'][^"\']*recruiter-job-link[^"\']*["\'][^>]*>(.*?)</a>'
    )
    for match in link_pattern.finditer(html_text):
        href = clean_text(match.group(1))
        title = strip_html_text(match.group(2))
        if not href or not title:
            continue
        if norm_text(title) in {"read more", "find jobs", "search for jobs"}:
            continue
        context = html_text[max(0, match.start() - 500): min(len(html_text), match.end() + 2500)]
        company_match = re.search(r'(?is)<div class="company-name">(.*?)</div>', context)
        city_match = re.search(r'(?is)<div class="city">(.*?)</div>', context)
        changed_match = re.search(r'(?is)<div class="job-changed-date">(.*?)</div>', context)

        company = strip_html_text(company_match.group(1)) if company_match else ""
        city = strip_html_text(city_match.group(1)) if city_match else ""
        changed_date = strip_html_text(changed_match.group(1)) if changed_match else ""
        source_id_match = re.search(r"/job/[^/?#]*-(\d+)", href)

        push_job(
            {
                "sourceJobId": clean_text(source_id_match.group(1) if source_id_match else ""),
                "title": title,
                "company": company or "Unknown",
                "city": city,
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": urljoin(base_url, href),
                "sector": "Game",
                "postedAt": parse_gamesindustry_changed_date(changed_date),
            }
        )

    if jobs:
        return jobs

    for href, inner in re.findall(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html_text):
        if "/job/" not in href:
            continue
        title = strip_html_text(inner)
        if not title or norm_text(title) == "read more":
            continue
        source_id_match = re.search(r"/job/[^/?#]*-(\d+)", href)
        push_job(
            {
                "sourceJobId": clean_text(source_id_match.group(1) if source_id_match else ""),
                "title": title,
                "company": "Unknown",
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": urljoin(base_url, href),
                "sector": "Game",
                "postedAt": "",
            }
        )
    return jobs


def parse_wellfound_candidate(node: Dict[str, Any], base_url: str) -> Optional[RawJob]:
    title = clean_text(node.get("title") or node.get("jobTitle"))
    company = ""
    if isinstance(node.get("company"), dict):
        company = clean_text(node["company"].get("name"))
    if not company:
        company = clean_text(node.get("companyName") or node.get("company_name") or node.get("company"))
    link = clean_text(node.get("url") or node.get("jobUrl") or node.get("job_url") or node.get("applyUrl") or node.get("canonicalUrl"))
    if link:
        link = urljoin(base_url, link)
    if not title or not company:
        return None
    tags = node.get("tags") or []
    tags_text = " ".join(str(tag) for tag in tags) if isinstance(tags, list) else clean_text(tags)
    description = clean_text(node.get("description") or node.get("snippet"))
    if not looks_like_game_job(title, company, tags_text, description):
        return None

    location_text = clean_text(node.get("location") or node.get("locationName") or "")
    is_remote = bool(node.get("remote")) or "remote" in norm_text(location_text)
    city = ""
    country = "Unknown"
    if location_text:
        parts = [part.strip() for part in location_text.split(",") if part.strip()]
        if parts:
            city = parts[0]
            country = parts[-1] if len(parts) > 1 else parts[0]
    if is_remote:
        city = "Remote"
        country = "Remote"
    return {
        "sourceJobId": clean_text(node.get("id") or node.get("jobId")),
        "title": title,
        "company": company,
        "city": city,
        "country": country,
        "workType": "Remote" if is_remote else location_text,
        "contractType": clean_text(node.get("employmentType") or ""),
        "jobLink": link,
        "sector": clean_text(node.get("industry") or ""),
        "postedAt": node.get("postedAt") or node.get("publishedAt") or node.get("createdAt"),
    }


def parse_wellfound_html(html_text: str, base_url: str = "https://wellfound.com/jobs") -> List[RawJob]:
    jobs: List[RawJob] = []
    match = re.search(r"(?is)<script[^>]+id=[\"']__NEXT_DATA__[\"'][^>]*>(.*?)</script>", html_text)
    if match:
        payload_text = unescape(match.group(1).strip())
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            payload = None
        if payload is not None:
            stack = [payload]
            while stack:
                node = stack.pop()
                if isinstance(node, dict):
                    candidate = parse_wellfound_candidate(node, base_url)
                    if candidate:
                        jobs.append(candidate)
                    stack.extend(node.values())
                elif isinstance(node, list):
                    stack.extend(node)
    if jobs:
        return jobs

    for href, inner in re.findall(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html_text):
        if "/jobs/" not in href:
            continue
        title = re.sub(r"(?is)<[^>]+>", " ", inner)
        title = re.sub(r"\s+", " ", title).strip()
        if not title or not looks_like_game_job(title):
            continue
        jobs.append(
            {
                "sourceJobId": "",
                "title": title,
                "company": "Unknown",
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": urljoin(base_url, href),
                "sector": "",
            }
        )
    return jobs


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
        from scripts.jobs import transport as transport_pkg

        return transport_pkg.AsyncHttpTextFetcher(max_connections=max_connections)


def resolve_fetch_text_impl(
    *,
    fetch_text: Callable[[str, int], str],
    fetch_strategy: str,
    adapter_http_concurrency: int,
) -> Tuple[Callable[[str, int], str], str, Optional[AsyncHttpTextFetcher]]:
    from scripts.jobs import transport as transport_pkg

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
                time.sleep(backoff_s * (2 ** attempt))
    raise RuntimeError(str(last_error) if last_error else f"Unknown fetch error for {url}")


def _community_adapter():
    from scripts.jobs.adapters import community

    return community


def _social_adapter():
    from scripts.jobs.adapters import social

    return social


def _provider_api_adapter():
    from scripts.jobs.adapters import provider_api

    return provider_api


def _static_adapter():
    from scripts.jobs.adapters import static

    return static


def google_sheet_candidate_urls(sheet_id: str, gid: str) -> List[str]:
    return _community_adapter().google_sheet_candidate_urls(sheet_id, gid)


def run_google_sheets_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    sheet_id: str = DEFAULT_GOOGLE_SHEET_ID,
    gid: str = DEFAULT_GOOGLE_SHEET_GID,
    diagnostics_name: str = "",
) -> List[RawJob]:
    return _community_adapter().run_google_sheets_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        sheet_id=sheet_id,
        gid=gid,
        diagnostics_name=diagnostics_name,
    )


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


def looks_like_country_token(value: str) -> bool:
    token = clean_text(value)
    lowered = token.lower()
    if lowered in COUNTRY_NAME_TO_CODE:
        return True
    return len(token) == 2 and token.isalpha()


def parse_greenhouse_location(location_name: Any) -> Tuple[str, str, str]:
    text = clean_text(location_name)
    if not text:
        return "", "Unknown", ""
    lower = norm_text(text)
    if "remote" in lower:
        return "Remote", "Remote", "Remote"

    parts = [clean_text(part) for part in text.split(",") if clean_text(part)]
    if not parts:
        return "", "Unknown", ""
    if len(parts) == 1:
        token = parts[0]
        if looks_like_country_token(token):
            return "", token, ""
        return token, "Unknown", ""

    first = parts[0]
    last = parts[-1]
    if looks_like_country_token(first):
        return parts[1], first, ""
    if looks_like_country_token(last):
        return first, last, ""
    return first, last, ""


def parse_greenhouse_jobs_payload(payload: Any, board_slug: str, fallback_company: str = "") -> List[RawJob]:
    rows = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []

    company_fallback = clean_text(fallback_company) or board_slug.replace("-", " ").title()
    jobs: List[RawJob] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("title"))
        job_link = clean_text(row.get("absolute_url") or row.get("url"))
        if not title or not job_link:
            continue

        company = clean_text(row.get("company_name")) or company_fallback
        location_obj = row.get("location")
        if isinstance(location_obj, dict):
            location_name = clean_text(location_obj.get("name"))
        else:
            location_name = clean_text(location_obj)
        city, country, work_type = parse_greenhouse_location(location_name)

        jobs.append(
            {
                "sourceJobId": f"greenhouse:{board_slug}:{clean_text(row.get('id') or row.get('internal_job_id'))}",
                "title": title,
                "company": company,
                "city": city,
                "country": country,
                "workType": work_type,
                "contractType": "",
                "jobLink": job_link,
                "sector": "Game",
                "postedAt": row.get("first_published") or row.get("updated_at"),
            }
        )
    return jobs


def parse_generic_location_fields(location_value: Any) -> Tuple[str, str, str]:
    text = clean_text(location_value)
    if not text:
        return "", "Unknown", ""
    lower = norm_text(text)
    if "remote" in lower:
        return "Remote", "Remote", "Remote"

    parts = [clean_text(part) for part in re.split(r"[,/|-]", text) if clean_text(part)]
    if not parts:
        return "", "Unknown", ""
    if len(parts) == 1:
        token = parts[0]
        if looks_like_country_token(token):
            return "", normalize_country(token), ""
        return token, "Unknown", ""
    city = parts[0]
    country = normalize_country(parts[-1])
    return city, country, ""


def parse_lever_jobs_payload(payload: Any, account: str, fallback_company: str = "") -> List[RawJob]:
    if not isinstance(payload, list):
        return []
    jobs: List[RawJob] = []
    company = clean_text(fallback_company) or account.replace("-", " ").title()
    for row in payload:
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("text"))
        link = clean_text(row.get("hostedUrl") or row.get("applyUrl") or row.get("url"))
        if not title or not link:
            continue
        categories = row.get("categories") if isinstance(row.get("categories"), dict) else {}
        location_text = clean_text(categories.get("location") or row.get("location"))
        city, country, work_type = parse_generic_location_fields(location_text)
        commitment = clean_text(categories.get("commitment") or row.get("commitment"))
        tags_text = " ".join(
            [
                clean_text(categories.get("team")),
                clean_text(categories.get("department")),
                clean_text(row.get("descriptionPlain")),
            ]
        )
        if not looks_like_game_job(title, company, tags_text):
            continue
        jobs.append(
            {
                "sourceJobId": f"lever:{account}:{clean_text(row.get('id') or row.get('requisitionCode'))}",
                "title": title,
                "company": company,
                "city": city,
                "country": country,
                "workType": work_type or location_text,
                "contractType": commitment,
                "jobLink": link,
                "sector": "Game",
                "postedAt": row.get("createdAt") or row.get("updatedAt"),
            }
        )
    return jobs


def parse_smartrecruiters_jobs_payload(payload: Any, company_id: str, fallback_company: str = "") -> List[RawJob]:
    rows = payload.get("content") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    jobs: List[RawJob] = []
    company = clean_text(fallback_company) or company_id
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("name"))
        posting_id = clean_text(row.get("id") or row.get("ref"))
        link = clean_text(row.get("ref"))
        if link and not link.startswith("http"):
            link = f"https://jobs.smartrecruiters.com/{company_id}/{link}"
        if not title or not (posting_id or link):
            continue
        location_obj = row.get("location") if isinstance(row.get("location"), dict) else {}
        city = clean_text(location_obj.get("city"))
        country = normalize_country(clean_text(location_obj.get("country")) or clean_text(location_obj.get("countryCode")))
        work_type = clean_text(location_obj.get("remote")) or clean_text(location_obj.get("region"))
        tags = " ".join(
            [
                clean_text(row.get("department")),
                clean_text(row.get("function")),
                clean_text(row.get("typeOfEmployment")),
            ]
        )
        if not looks_like_game_job(title, company, tags):
            continue
        jobs.append(
            {
                "sourceJobId": f"smartrecruiters:{company_id}:{posting_id or hashlib.sha1(title.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": city,
                "country": country or "Unknown",
                "workType": work_type,
                "contractType": clean_text(row.get("typeOfEmployment")),
                "jobLink": link or f"https://jobs.smartrecruiters.com/{company_id}/{posting_id}",
                "sector": "Game",
                "postedAt": row.get("releasedDate") or row.get("createdOn"),
            }
        )
    return jobs


def parse_workable_jobs_payload(payload: Any, account: str, fallback_company: str = "") -> List[RawJob]:
    rows = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    jobs: List[RawJob] = []
    company = clean_text(payload.get("name") if isinstance(payload, dict) else "") or clean_text(fallback_company) or account
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("title"))
        link = clean_text(row.get("url") or row.get("shortlink"))
        if link and not link.startswith("http"):
            link = urljoin(f"https://apply.workable.com/{account}/", link)
        location = row.get("location") if isinstance(row.get("location"), dict) else {}
        location_text = " ".join(
            [
                clean_text(location.get("city")),
                clean_text(location.get("country")),
                "Remote" if bool(location.get("telecommuting")) else "",
            ]
        ).strip()
        city, country, work_type = parse_generic_location_fields(location_text)
        if bool(location.get("telecommuting")):
            city, country, work_type = "Remote", "Remote", "Remote"
        tags = " ".join([clean_text(row.get("department")), clean_text(row.get("description"))])
        if not title or not link:
            continue
        if not looks_like_game_job(title, company, tags):
            continue
        jobs.append(
            {
                "sourceJobId": f"workable:{account}:{clean_text(row.get('shortcode') or row.get('id'))}",
                "title": title,
                "company": company,
                "city": city,
                "country": country,
                "workType": work_type or location_text,
                "contractType": clean_text(row.get("employment_type")),
                "jobLink": link,
                "sector": "Game",
                "postedAt": row.get("published") or row.get("created_at"),
            }
        )
    return jobs


def parse_epic_games_jobs_payload(payload: Any, fallback_company: str = "Epic Games") -> List[RawJob]:
    rows = payload.get("hits") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []

    jobs: List[RawJob] = []
    company = clean_text(fallback_company) or "Epic Games"
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("title"))
        posting_id = clean_text(row.get("id") or row.get("internal_job_id") or row.get("requisition_id"))
        link = clean_text(row.get("absolute_url"))
        if not link and posting_id:
            link = f"https://www.epicgames.com/site/en-US/careers/jobs/{posting_id}"
        if not title or not link:
            continue

        company_name = clean_text(row.get("company_name") or row.get("company")) or company
        location_text = clean_text(row.get("location"))
        if not location_text:
            location_text = ", ".join(
                [part for part in [clean_text(row.get("city")), clean_text(row.get("country"))] if part]
            )
        city, country, work_type = parse_generic_location_fields(location_text)
        if bool(row.get("remote")):
            city, country, work_type = "Remote", "Remote", "Remote"
        tags = " ".join(
            [
                clean_text(row.get("department")),
                clean_text(row.get("product")),
                clean_text(row.get("type")),
                clean_text(row.get("filterText")),
            ]
        )
        if not looks_like_game_job(title, company_name, tags):
            continue
        jobs.append(
            {
                "sourceJobId": f"epic:{posting_id or hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company_name,
                "city": city,
                "country": country,
                "workType": work_type or location_text,
                "contractType": clean_text(row.get("type")),
                "jobLink": link,
                "sector": "Game",
                "postedAt": row.get("first_published") or row.get("updated_at"),
            }
        )
    return jobs


def parse_ashby_jobs_from_html(html_text: str, board_url: str, fallback_company: str = "") -> List[RawJob]:
    links = []
    seen = set()
    for href in re.findall(r'(?is)<a[^>]+href=["\']([^"\']+)["\']', html_text):
        absolute = urljoin(board_url, clean_text(href))
        path = urlparse(absolute).path.lower()
        if "/job/" not in path:
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        links.append(absolute)
    jobs: List[RawJob] = []
    for link in links:
        slug = urlparse(link).path.rstrip("/").split("/")[-1]
        title = strip_html_text(re.sub(r"[-_]+", " ", slug)).title()
        if not title:
            continue
        company = clean_text(fallback_company) or "Unknown"
        if not looks_like_game_job(title, company):
            continue
        jobs.append(
            {
                "sourceJobId": f"ashby:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
            }
        )
    return jobs


def parse_personio_feed_xml(xml_text: str, source_name: str = "") -> List[RawJob]:
    jobs: List[RawJob] = []
    root: Optional[ET.Element] = None
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        root = None
    if root is None:
        return jobs

    for posting in root.findall(".//position"):
        title = clean_text(posting.findtext("name"))
        if not title:
            continue
        company = clean_text(posting.findtext("subcompany")) or clean_text(source_name) or "Unknown"
        office = clean_text(posting.findtext("office"))
        department = clean_text(posting.findtext("department"))
        city, country, work_type = parse_generic_location_fields(office)
        job_link = clean_text(posting.findtext("url"))
        posting_id = clean_text(posting.findtext("id")) or clean_text(posting.get("id"))
        tags = " ".join([department, office])
        if not looks_like_game_job(title, company, tags):
            continue
        jobs.append(
            {
                "sourceJobId": f"personio:{source_name}:{posting_id or hashlib.sha1((title + office).encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": city,
                "country": country,
                "workType": work_type or office,
                "contractType": clean_text(posting.findtext("employmentType")),
                "jobLink": job_link,
                "sector": "Game",
                "postedAt": clean_text(posting.findtext("createdAt") or posting.findtext("date")),
            }
        )
    return jobs


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
    )


def run_static_studio_pages_a_i_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[RawJob]:
    return _static_adapter().run_static_studio_pages_a_i_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
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
) -> List[RawJob]:
    return _static_adapter().run_static_studio_pages_j_r_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
    )


def run_static_studio_pages_s_z_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[RawJob]:
    return _static_adapter().run_static_studio_pages_s_z_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
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
    from scripts.jobs import canonicalize as canonicalize_pkg

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
    from scripts.jobs import canonicalize as canonicalize_pkg

    normalized = canonicalize_pkg.canonicalize_job(
        raw,
        source=source,
        fetched_at=fetched_at,
        resolve_redirect_url=resolve_redirect_url,
        resolved_job_link=resolved_job_link,
    )
    return normalized.to_dict() if normalized is not None else None


def canonicalize_google_sheets_rows(
    raw_rows: Sequence[RawJob],
    *,
    source: str,
    fetched_at: str,
    redirect_resolver: Optional[PooledRedirectResolver] = None,
    redirect_concurrency: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
) -> Tuple[List[RawJob], Counter, Dict[str, int]]:
    from scripts.jobs import canonicalize as canonicalize_pkg

    rows, drop_reasons, stats = canonicalize_pkg.canonicalize_google_sheets_rows(
        raw_rows,
        source=source,
        fetched_at=fetched_at,
        redirect_resolver=redirect_resolver,
        redirect_concurrency=redirect_concurrency,
    )
    return [row.to_dict() for row in rows], drop_reasons, stats


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
    from scripts.jobs import dedup as dedup_pkg

    base, other = dedup_pkg.choose_base_record(
        dedup_pkg.CanonicalJob.from_mapping(left),
        dedup_pkg.CanonicalJob.from_mapping(right),
    )
    return base.to_dict(), other.to_dict()


def merge_records(existing: RawJob, candidate: RawJob) -> RawJob:
    from scripts.jobs import dedup as dedup_pkg

    return dedup_pkg.merge_records(
        dedup_pkg.CanonicalJob.from_mapping(existing),
        dedup_pkg.CanonicalJob.from_mapping(candidate),
    ).to_dict()


def deduplicate_jobs(rows: Sequence[RawJob]) -> Tuple[List[RawJob], Dict[str, int]]:
    from scripts.jobs import dedup as dedup_pkg

    merged_rows, stats = dedup_pkg.deduplicate_jobs([dedup_pkg.CanonicalJob.from_mapping(row) for row in rows])
    return [row.to_dict() for row in merged_rows], stats


def default_source_loaders(
    *,
    social_enabled: bool = False,
    social_config: Optional[Dict[str, Any]] = None,
) -> List[Tuple[str, SourceLoader]]:
    from scripts.jobs.adapters import default_source_loaders as package_default_source_loaders

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
    from scripts.jobs import reporting as reporting_pkg

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
    from scripts.jobs import reporting as reporting_pkg

    return reporting_pkg.build_browser_fallback_queue(source_reports, generated_at=generated_at)


def read_previously_successful_sources(report_path: Path) -> set[str]:
    from scripts.jobs import state as state_pkg

    return state_pkg.read_previously_successful_sources(report_path)


def read_success_cache(cache_path: Path) -> set[str]:
    from scripts.jobs import state as state_pkg

    return state_pkg.read_success_cache(cache_path)


def write_success_cache(cache_path: Path, source_reports: Sequence[Dict[str, Any]]) -> None:
    from scripts.jobs import state as state_pkg

    state_pkg.write_success_cache(cache_path, source_reports)


def source_rows_fingerprint(rows: Sequence[RawJob]) -> str:
    from scripts.jobs import state as state_pkg

    return state_pkg.source_rows_fingerprint(rows)


def _clamped_int(value: Any, default: int = 0, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(minimum, parsed)


def normalize_source_state_payload(payload: Dict[str, Any], *, updated_at: str = "") -> Dict[str, Any]:
    from scripts.jobs import state as state_pkg

    return state_pkg.normalize_source_state_payload(payload, updated_at=updated_at)


def read_source_state(state_path: Path) -> Dict[str, Dict[str, Any]]:
    from scripts.jobs import state as state_pkg

    return state_pkg.read_source_state(state_path)


def write_source_state(state_path: Path, rows: Dict[str, Dict[str, Any]]) -> None:
    from scripts.jobs import state as state_pkg

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
    from scripts.jobs import state as state_pkg

    return state_pkg.normalize_job_lifecycle_payload(payload, updated_at=updated_at)


def read_job_lifecycle_state(state_path: Path) -> Dict[str, Dict[str, Any]]:
    from scripts.jobs import state as state_pkg

    return state_pkg.read_job_lifecycle_state(state_path)


def write_job_lifecycle_state(state_path: Path, rows: Dict[str, Dict[str, Any]]) -> None:
    from scripts.jobs import state as state_pkg

    state_pkg.write_job_lifecycle_state(state_path, rows)


def lifecycle_counts(rows: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    from scripts.jobs import state as state_pkg

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
    from scripts.jobs import state as state_pkg

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
    from scripts.jobs import pipeline as pipeline_pkg

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
    from scripts.jobs import pipeline as pipeline_pkg

    return pipeline_pkg.parse_args()


def main() -> int:
    from scripts.jobs import pipeline as pipeline_pkg

    return pipeline_pkg.main()

if __name__ == "__main__":
    raise SystemExit(main())
