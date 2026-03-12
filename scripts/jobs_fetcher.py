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

SHEET_ID = "1ZOJpVS3CcnrkwhpRgkP7tzf3wc4OWQj-uoWFfv4oHZE"
SHEET_GID = "1560329579"
GOOGLE_CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SHEET_GID}"
GOOGLE_ALL_ORIGINS_URL = f"https://api.allorigins.win/raw?url={quote(GOOGLE_CSV_URL, safe='')}"
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
        "remoteFriendly": True,
        "enabledByDefault": True,
    },
    {
        "name": "PlayStation Global",
        "studio": "PlayStation Global",
        "adapter": "greenhouse",
        "slug": "sonyinteractiveentertainmentglobal",
        "nlPriority": True,
        "remoteFriendly": True,
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
        "remoteFriendly": True,
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
        "remoteFriendly": True,
        "enabledByDefault": True,
    },
    {
        "name": "Larian Studios",
        "studio": "Larian Studios",
        "adapter": "greenhouse",
        "slug": "larian-studios",
        "nlPriority": True,
        "remoteFriendly": True,
        "enabledByDefault": False,
    },
    {
        "name": "Jagex (Lever)",
        "studio": "Jagex",
        "adapter": "lever",
        "account": "jagex",
        "api_url": "https://api.lever.co/v0/postings/jagex?mode=json",
        "nlPriority": False,
        "remoteFriendly": True,
        "enabledByDefault": True,
    },
    {
        "name": "Sandbox VR (Lever)",
        "studio": "Sandbox VR",
        "adapter": "lever",
        "account": "sandboxvr",
        "api_url": "https://api.lever.co/v0/postings/sandboxvr?mode=json",
        "nlPriority": False,
        "remoteFriendly": True,
        "enabledByDefault": True,
    },
    {
        "name": "Voodoo (Lever)",
        "studio": "Voodoo",
        "adapter": "lever",
        "account": "voodoo",
        "api_url": "https://api.lever.co/v0/postings/voodoo?mode=json",
        "nlPriority": False,
        "remoteFriendly": True,
        "enabledByDefault": True,
    },
    {
        "name": "CD PROJEKT RED (SmartRecruiters)",
        "studio": "CD PROJEKT RED",
        "adapter": "smartrecruiters",
        "company_id": "CDPROJEKTRED",
        "api_url": "https://api.smartrecruiters.com/v1/companies/CDPROJEKTRED/postings",
        "nlPriority": False,
        "remoteFriendly": True,
        "enabledByDefault": True,
    },
    {
        "name": "Gameloft (SmartRecruiters)",
        "studio": "Gameloft",
        "adapter": "smartrecruiters",
        "company_id": "Gameloft",
        "api_url": "https://api.smartrecruiters.com/v1/companies/Gameloft/postings",
        "nlPriority": False,
        "remoteFriendly": True,
        "enabledByDefault": True,
    },
    {
        "name": "Hutch (Workable)",
        "studio": "Hutch",
        "adapter": "workable",
        "account": "hutch",
        "api_url": "https://apply.workable.com/api/v1/widget/accounts/hutch?details=true",
        "nlPriority": False,
        "remoteFriendly": True,
        "enabledByDefault": True,
    },
    {
        "name": "Wargaming (Workable)",
        "studio": "Wargaming",
        "adapter": "workable",
        "account": "wargaming",
        "api_url": "https://apply.workable.com/api/v1/widget/accounts/wargaming?details=true",
        "nlPriority": False,
        "remoteFriendly": True,
        "enabledByDefault": True,
    },
    {
        "name": "InnoGames (Personio)",
        "studio": "InnoGames",
        "adapter": "personio",
        "feed_url": "https://innogames.jobs.personio.de/xml",
        "nlPriority": True,
        "remoteFriendly": True,
        "enabledByDefault": True,
    },
    {
        "name": "Travian (Personio)",
        "studio": "Travian",
        "adapter": "personio",
        "feed_url": "https://travian.jobs.personio.de/xml",
        "nlPriority": True,
        "remoteFriendly": True,
        "enabledByDefault": True,
    },
    {
        "name": "Jagex (Ashby)",
        "studio": "Jagex",
        "adapter": "ashby",
        "board_url": "https://jobs.ashbyhq.com/jagex/jobs",
        "nlPriority": False,
        "remoteFriendly": True,
        "enabledByDefault": True,
    },
    {
        "name": "Scopely (Ashby)",
        "studio": "Scopely",
        "adapter": "ashby",
        "board_url": "https://jobs.ashbyhq.com/scopely/jobs",
        "nlPriority": False,
        "remoteFriendly": True,
        "enabledByDefault": True,
    },
]

DEFAULT_TIMEOUT_S = 20
DEFAULT_RETRIES = 2
DEFAULT_BACKOFF_S = 1.6
DEFAULT_FETCH_STRATEGY = "auto"
DEFAULT_ADAPTER_HTTP_CONCURRENCY = 24
DEFAULT_HOT_SOURCE_CADENCE_MINUTES = 15
DEFAULT_COLD_SOURCE_CADENCE_MINUTES = 60
_STORAGE_DEFAULTS = get_storage_defaults()
DEFAULT_OUTPUT_DIR = _STORAGE_DEFAULTS["data_dir"]
DEFAULT_SOCIAL_CONFIG_PATH = _STORAGE_DEFAULTS["social_sources_config_path"]
DEFAULT_SOCIAL_LOOKBACK_MINUTES = 30
SOCIAL_SOURCE_NAMES = {"social_reddit", "social_x", "social_mastodon"}
DEFAULT_SOCIAL_MIN_CONFIDENCE = 40
DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE = "balanced"
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


def fingerprint_url(url: Any) -> str:
    normalized = normalize_url(url)
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest() if normalized else ""


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
        if header in {"company", "company name"}:
            return idx
    for idx, header in enumerate(normalized):
        if "company" in header and not any(part in header for part in ("type", "category", "sector", "industry")):
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


def is_generic_company_label(value: str) -> bool:
    return norm_text(value) in {
        "game",
        "tech",
        "game company",
        "tech company",
        "gaming company",
        "technology company",
    }


def resolve_company_name(row: Sequence[str], primary_idx: int, candidate_indexes: Sequence[int]) -> str:
    values: List[str] = []
    if 0 <= primary_idx < len(row):
        values.append(clean_text(row[primary_idx]))
    for idx in candidate_indexes:
        if 0 <= idx < len(row):
            values.append(clean_text(row[idx]))
    for value in values:
        if value and not is_generic_company_label(value):
            return value
    for value in values:
        if value:
            return value
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
        has_title = "title" in normalized or "role" in normalized
        has_company = "company name" in normalized or "company" in normalized
        has_location = "city" in normalized or "country" in normalized
        if has_title and has_company and has_location:
            header_idx = idx
            break
    if header_idx < 0:
        return []

    headers = [clean_text(header) for header in rows[header_idx]]
    company_idx = find_company_column(headers)
    company_candidates = company_name_candidate_indexes(headers, company_idx)
    title_idx = find_column_index(headers, ["title", "role"], ["title", "role"])
    city_idx = find_column_index(headers, ["city"], ["city"])
    country_idx = find_column_index(headers, ["country"], ["country"])
    location_idx = find_column_index(headers, ["location type", "work type"], ["location", "work type", "remote"])
    contract_idx = find_column_index(headers, ["employment type", "contract type", "employment", "contract"], ["employment", "contract"])
    link_idx = find_column_index(headers, ["job link", "url", "apply"], ["job link", "url", "apply"])
    sector_idx = find_column_index(headers, ["sector", "industry", "company type", "company category"], ["sector", "industry", "company type", "company category"])

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
                "country": clean_text(row[country_idx] if 0 <= country_idx < len(row) else "Unknown"),
                "workType": clean_text(row[location_idx] if 0 <= location_idx < len(row) else "On-site"),
                "contractType": clean_text(row[contract_idx] if 0 <= contract_idx < len(row) else ""),
                "jobLink": clean_text(row[link_idx] if 0 <= link_idx < len(row) else ""),
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
        headers={
            "User-Agent": "BaluffoJobsFetcher/1.0 (+https://github.com/)",
            "Accept": "application/json,text/html,text/csv,*/*",
        },
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
    def __init__(self, *, max_connections: int = DEFAULT_ADAPTER_HTTP_CONCURRENCY) -> None:
        if httpx is None:
            raise RuntimeError("httpx is not installed")
        self._max_connections = max(1, int(max_connections or 1))
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._ready = threading.Event()
        self._closed = False
        self._thread.start()
        if not self._ready.wait(timeout=5):
            raise RuntimeError("Async HTTP loop initialization timed out")

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._client = httpx.AsyncClient(
            follow_redirects=True,
            headers={
                "User-Agent": "BaluffoJobsFetcher/1.0 (+https://github.com/)",
                "Accept": "application/json,text/html,text/csv,*/*",
            },
            limits=httpx.Limits(
                max_keepalive_connections=self._max_connections,
                max_connections=max(self._max_connections * 2, self._max_connections),
            ),
        )
        self._ready.set()
        self._loop.run_forever()

    async def _fetch(self, url: str, timeout_s: int) -> str:
        timeout = httpx.Timeout(float(max(1, timeout_s)))
        try:
            response = await self._client.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except httpx.HTTPStatusError as exc:
            code = int(getattr(exc.response, "status_code", 0) or 0)
            raise RuntimeError(f"HTTP {code} for {url}") from exc
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Network error for {url}: {exc}") from exc

    async def _aclose(self) -> None:
        await self._client.aclose()

    def fetch_text(self, url: str, timeout_s: int) -> str:
        if self._closed:
            raise RuntimeError("Async HTTP fetcher is closed")
        future = asyncio.run_coroutine_threadsafe(self._fetch(url, timeout_s), self._loop)
        return str(future.result())

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            future = asyncio.run_coroutine_threadsafe(self._aclose(), self._loop)
            future.result(timeout=5)
        except Exception:  # noqa: BLE001
            pass
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
        except Exception:  # noqa: BLE001
            pass
        self._thread.join(timeout=2)


def resolve_fetch_text_impl(
    *,
    fetch_text: Callable[[str, int], str],
    fetch_strategy: str,
    adapter_http_concurrency: int,
) -> Tuple[Callable[[str, int], str], str, Optional[AsyncHttpTextFetcher]]:
    strategy = norm_text(fetch_strategy)
    chosen = "urllib"
    async_fetcher: Optional[AsyncHttpTextFetcher] = None
    if strategy in {"http", "auto"} and httpx is not None:
        try:
            async_fetcher = AsyncHttpTextFetcher(max_connections=adapter_http_concurrency)
            chosen = "httpx_async"
            return async_fetcher.fetch_text, chosen, async_fetcher
        except Exception:  # noqa: BLE001
            pass
    return fetch_text, chosen, async_fetcher


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


def run_google_sheets_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    errors: List[str] = []
    for url in [GOOGLE_CSV_URL, GOOGLE_ALL_ORIGINS_URL]:
        try:
            text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            jobs = parse_google_sheets_csv(text)
            if jobs:
                return jobs
            errors.append(f"{url}: empty/invalid CSV")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")
    raise RuntimeError("; ".join(errors) if errors else "Google Sheets source failed")


def run_remote_ok_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    errors: List[str] = []
    for url in REMOTE_OK_URLS:
        try:
            text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            parsed = parse_remote_ok_payload(json.loads(text))
            if parsed:
                return parsed
            errors.append(f"{url}: empty/invalid payload")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")
    raise RuntimeError("; ".join(errors) if errors else "Remote OK source failed")


def _request_json_with_headers(url: str, *, timeout_s: int, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    req = Request(url=url, headers=headers or {})
    with urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read().decode(resp.headers.get_content_charset() or "utf-8", errors="replace")
        parsed = json.loads(raw) if raw else {}
        return parsed if isinstance(parsed, dict) else {}


def run_social_reddit_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: Dict[str, Any],
) -> List[RawJob]:
    cfg = social_config.get("reddit") if isinstance(social_config.get("reddit"), dict) else {}
    if not bool(social_config.get("enabled")) or not bool(cfg.get("enabled", True)):
        set_source_diagnostics("social_reddit", adapter="social", studio="reddit", details=[], partial_errors=[])
        return []
    subs = [clean_text(item) for item in (cfg.get("subreddits") or []) if clean_text(item)]
    max_posts = max(1, int(cfg.get("maxPostsPerSubreddit") or 50))
    min_conf = max(0, min(100, int(social_config.get("minConfidence") or DEFAULT_SOCIAL_MIN_CONFIDENCE)))
    reject_for_hire = bool(social_config.get("rejectForHirePosts", True))
    details: List[Dict[str, Any]] = []
    errors: List[str] = []
    jobs: List[RawJob] = []
    low_conf_total = 0

    for sub in subs:
        source_name = f"reddit:r/{sub}"
        json_url = f"https://www.reddit.com/r/{quote(sub, safe='')}/new.json?limit={max_posts}"
        rss_url = f"https://www.reddit.com/r/{quote(sub, safe='')}/new.rss"
        entry = {"adapter": "social", "studio": f"reddit/{sub}", "name": source_name, "status": "ok", "fetchedCount": 0, "keptCount": 0, "error": ""}
        parsed_rows: List[RawJob] = []
        low_conf_sub = 0
        try:
            text = fetch_with_retries(json_url, fetch_text, timeout_s, retries, backoff_s)
            payload = json.loads(text)
            parsed_rows, low_conf_sub = parse_reddit_json_payload(
                payload,
                subreddit=sub,
                min_confidence=min_conf,
                reject_for_hire_posts=reject_for_hire,
            )
            entry["fetchedCount"] = len((((payload.get("data") or {}).get("children")) if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else []) or [])
        except Exception as exc:  # noqa: BLE001
            if bool(cfg.get("rssFallback", True)):
                try:
                    rss_text = fetch_with_retries(rss_url, fetch_text, timeout_s, retries, backoff_s)
                    parsed_rows, low_conf_sub = parse_reddit_rss_payload(
                        rss_text,
                        subreddit=sub,
                        min_confidence=min_conf,
                        reject_for_hire_posts=reject_for_hire,
                    )
                    entry["fetchedCount"] = len(parsed_rows) + int(low_conf_sub)
                except Exception as rss_exc:  # noqa: BLE001
                    entry["status"] = "error"
                    entry["error"] = f"{exc}; {rss_exc}"
                    errors.append(f"reddit:{sub}: {exc}; {rss_exc}")
            else:
                entry["status"] = "error"
                entry["error"] = str(exc)
                errors.append(f"reddit:{sub}: {exc}")
        entry["keptCount"] = len(parsed_rows)
        low_conf_total += int(low_conf_sub)
        jobs.extend(parsed_rows)
        details.append(entry)

    set_source_diagnostics(
        "social_reddit",
        adapter="social",
        studio="reddit",
        details=details,
        partial_errors=errors,
    )
    SOURCE_DIAGNOSTICS["social_reddit"]["lowConfidenceDropped"] = int(low_conf_total)
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def run_social_x_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: Dict[str, Any],
) -> List[RawJob]:
    cfg = social_config.get("x") if isinstance(social_config.get("x"), dict) else {}
    if not bool(social_config.get("enabled")) or not bool(cfg.get("enabled", True)):
        set_source_diagnostics("social_x", adapter="social", studio="x", details=[], partial_errors=[])
        return []
    queries = [clean_text(item) for item in (cfg.get("queries") or []) if clean_text(item)]
    if not queries:
        return []
    max_posts = max(1, int(cfg.get("maxPostsPerQuery") or 25))
    min_conf = max(0, min(100, int(cfg.get("minConfidence") or social_config.get("minConfidence") or DEFAULT_SOCIAL_MIN_CONFIDENCE)))
    reject_for_hire = bool(social_config.get("rejectForHirePosts", True))
    api_cfg = cfg.get("api") if isinstance(cfg.get("api"), dict) else {}
    scraper_cfg = cfg.get("scraperFallback") if isinstance(cfg.get("scraperFallback"), dict) else {}
    rss_cfg = cfg.get("rssFallback") if isinstance(cfg.get("rssFallback"), dict) else {}
    bearer_env = clean_text(api_cfg.get("bearerTokenEnv") or "BALUFFO_X_BEARER_TOKEN")
    bearer = clean_text(__import__("os").environ.get(bearer_env))
    endpoint = clean_text(api_cfg.get("endpoint"))
    scraper_endpoint = clean_text(scraper_cfg.get("endpoint"))
    rss_instances = [clean_text(item).rstrip("/") for item in (rss_cfg.get("instances") or []) if clean_text(item)]

    details: List[Dict[str, Any]] = []
    errors: List[str] = []
    jobs: List[RawJob] = []
    low_conf_total = 0

    for query in queries:
        entry = {"adapter": "social", "studio": "x", "name": f"x:{query}", "status": "ok", "fetchedCount": 0, "keptCount": 0, "error": ""}
        parsed_rows: List[RawJob] = []
        low_conf_query = 0
        try:
            payload: Any = {}
            if bool(api_cfg.get("enabled", True)) and bearer and endpoint:
                url = f"{endpoint}?query={quote(query, safe='')}&max_results={max_posts}&tweet.fields=created_at,entities"
                payload = _request_json_with_headers(
                    url,
                    timeout_s=timeout_s,
                    headers={"Authorization": f"Bearer {bearer}", "Accept": "application/json"},
                )
            elif bool(scraper_cfg.get("enabled")) and scraper_endpoint:
                url = f"{scraper_endpoint}?q={quote(query, safe='')}&limit={max_posts}"
                text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
                payload = json.loads(text)
            elif bool(rss_cfg.get("enabled", True)) and rss_instances:
                rss_errors: List[str] = []
                rss_payload_text = ""
                for instance in rss_instances:
                    rss_url = f"{instance}/search/rss?f=tweets&q={quote(query, safe='')}"
                    try:
                        rss_payload_text = fetch_with_retries(rss_url, fetch_text, timeout_s, retries, backoff_s)
                        break
                    except Exception as rss_exc:  # noqa: BLE001
                        rss_errors.append(f"{instance}: {rss_exc}")
                if not rss_payload_text:
                    raise RuntimeError("; ".join(rss_errors) if rss_errors else "x rss fallback failed")
                parsed_rows, low_conf_query = parse_x_rss_payload(
                    rss_payload_text,
                    query_label=query,
                    min_confidence=min_conf,
                    reject_for_hire_posts=reject_for_hire,
                )
                entry["fetchedCount"] = len(parsed_rows) + int(low_conf_query)
                entry["keptCount"] = len(parsed_rows)
                low_conf_total += int(low_conf_query)
                jobs.extend(parsed_rows)
                details.append(entry)
                continue
            else:
                entry["status"] = "error"
                entry["error"] = "missing x api credentials and fallbacks disabled"
                errors.append(f"x:{query}: {entry['error']}")
                details.append(entry)
                continue

            parsed_rows, low_conf_query = parse_x_payload(
                payload,
                query_label=query,
                min_confidence=min_conf,
                reject_for_hire_posts=reject_for_hire,
            )
            if isinstance(payload, dict) and isinstance(payload.get("data"), list):
                entry["fetchedCount"] = len(payload.get("data") or [])
            else:
                entry["fetchedCount"] = len(parsed_rows) + int(low_conf_query)
        except Exception as exc:  # noqa: BLE001
            entry["status"] = "error"
            entry["error"] = str(exc)
            errors.append(f"x:{query}: {exc}")
        entry["keptCount"] = len(parsed_rows)
        low_conf_total += int(low_conf_query)
        jobs.extend(parsed_rows)
        details.append(entry)

    set_source_diagnostics("social_x", adapter="social", studio="x", details=details, partial_errors=errors)
    SOURCE_DIAGNOSTICS["social_x"]["lowConfidenceDropped"] = int(low_conf_total)
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def run_social_mastodon_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: Dict[str, Any],
) -> List[RawJob]:
    cfg = social_config.get("mastodon") if isinstance(social_config.get("mastodon"), dict) else {}
    if not bool(social_config.get("enabled")) or not bool(cfg.get("enabled", True)):
        set_source_diagnostics("social_mastodon", adapter="social", studio="mastodon", details=[], partial_errors=[])
        return []
    instances = [clean_text(item).rstrip("/") for item in (cfg.get("instances") or []) if clean_text(item)]
    tags = [clean_text(item).lstrip("#") for item in (cfg.get("hashtags") or []) if clean_text(item)]
    max_posts = max(1, int(cfg.get("maxPostsPerTag") or 40))
    min_conf = max(0, min(100, int(social_config.get("minConfidence") or DEFAULT_SOCIAL_MIN_CONFIDENCE)))
    reject_for_hire = bool(social_config.get("rejectForHirePosts", True))
    details: List[Dict[str, Any]] = []
    errors: List[str] = []
    jobs: List[RawJob] = []
    low_conf_total = 0

    for instance in instances:
        for tag in tags:
            entry = {
                "adapter": "social",
                "studio": f"mastodon/{clean_text(urlparse(instance).netloc)}",
                "name": f"mastodon:{clean_text(urlparse(instance).netloc)}:#{tag}",
                "status": "ok",
                "fetchedCount": 0,
                "keptCount": 0,
                "error": "",
            }
            try:
                url = f"{instance}/api/v1/timelines/tag/{quote(tag, safe='')}?limit={max_posts}"
                text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
                payload = json.loads(text)
                parsed_rows, low_conf_tag = parse_mastodon_payload(
                    payload,
                    instance=instance,
                    tag=tag,
                    min_confidence=min_conf,
                    reject_for_hire_posts=reject_for_hire,
                )
                entry["fetchedCount"] = len(payload) if isinstance(payload, list) else len(parsed_rows) + int(low_conf_tag)
                entry["keptCount"] = len(parsed_rows)
                low_conf_total += int(low_conf_tag)
                jobs.extend(parsed_rows)
            except Exception as exc:  # noqa: BLE001
                entry["status"] = "error"
                entry["error"] = str(exc)
                errors.append(f"mastodon:{instance}:#{tag}: {exc}")
            details.append(entry)

    set_source_diagnostics("social_mastodon", adapter="social", studio="mastodon", details=details, partial_errors=errors)
    SOURCE_DIAGNOSTICS["social_mastodon"]["lowConfidenceDropped"] = int(low_conf_total)
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def run_gamesindustry_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    jobs: List[RawJob] = []
    errors: List[str] = []
    for url in GAMES_INDUSTRY_URLS:
        try:
            text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            jobs.extend(parse_gamesindustry_html(text, base_url=url))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def run_epic_games_careers_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    jobs: List[RawJob] = []
    seen_source_ids = set()
    skip = 0
    limit = 20
    max_pages = 40

    for _ in range(max_pages):
        page_url = f"{EPIC_CAREERS_API_URL}?skip={skip}&limit={limit}"
        text = fetch_with_retries(page_url, fetch_text, timeout_s, retries, backoff_s)
        payload = json.loads(text)
        page_jobs = parse_epic_games_jobs_payload(payload, fallback_company="Epic Games")
        if not page_jobs:
            break
        for row in page_jobs:
            source_job_id = clean_text(row.get("sourceJobId"))
            if source_job_id and source_job_id in seen_source_ids:
                continue
            if source_job_id:
                seen_source_ids.add(source_job_id)
            row["adapter"] = "epic_api"
            row["studio"] = "Epic Games"
            jobs.append(row)
        if len(page_jobs) < limit:
            break
        skip += limit

    return jobs


def run_wellfound_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    jobs: List[RawJob] = []
    errors: List[str] = []
    for url in WELLFOUND_URLS:
        try:
            text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            jobs.extend(parse_wellfound_html(text, base_url=url))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


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
    jobs: List[RawJob] = []
    errors: List[str] = []
    details: List[Dict[str, Any]] = []
    for board in registry_entries("greenhouse"):
        slug = clean_text(board.get("slug"))
        if not slug:
            continue
        label = clean_text(board.get("name")) or clean_text(board.get("studio")) or slug
        url = GREENHOUSE_JOBS_URL_TEMPLATE.format(slug=slug)
        entry_report = {
            "adapter": "greenhouse",
            "studio": clean_text(board.get("studio")) or label,
            "name": clean_text(board.get("name")) or slug,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        try:
            text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            payload = json.loads(text)
            parsed = parse_greenhouse_jobs_payload(payload, slug, fallback_company=label)
            for row in parsed:
                row["adapter"] = "greenhouse"
                row["studio"] = clean_text(board.get("studio")) or label
            entry_report["fetchedCount"] = len(parsed)
            entry_report["keptCount"] = len(parsed)
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"greenhouse:{slug}: {exc}")
        details.append(entry_report)
    set_source_diagnostics(
        "greenhouse_boards",
        adapter="greenhouse",
        studio="multiple",
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def run_teamtailor_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    jobs: List[RawJob] = []
    errors: List[str] = []
    seen_links = set()
    details: List[Dict[str, Any]] = []

    for source in registry_entries("teamtailor"):
        source_name = clean_text(source.get("name")) or "teamtailor_source"
        listing_url = clean_text(source.get("listing_url"))
        base_url = clean_text(source.get("base_url")) or listing_url
        fallback_company = clean_text(source.get("company"))
        entry_report = {
            "adapter": "teamtailor",
            "studio": clean_text(source.get("studio")) or fallback_company or source_name,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        if not listing_url:
            entry_report["status"] = "error"
            entry_report["error"] = "missing listing_url"
            details.append(entry_report)
            continue

        try:
            listing_html = fetch_with_retries(listing_url, fetch_text, timeout_s, retries, backoff_s)
            job_links = parse_teamtailor_listing_links(listing_html, base_url=base_url)
            entry_report["fetchedCount"] = len(job_links)
            kept_before = len(jobs)
            for idx, job_link in enumerate(job_links, start=1):
                if job_link in seen_links:
                    continue
                seen_links.add(job_link)
                try:
                    detail_html = fetch_with_retries(job_link, fetch_text, timeout_s, retries, backoff_s)
                    parsed = parse_jobpostings_from_html(
                        detail_html,
                        base_url=job_link,
                        fallback_company=fallback_company,
                        fallback_source_id_prefix=f"teamtailor:{source_name}:{idx}",
                    )
                    if parsed:
                        for row in parsed:
                            row["adapter"] = "teamtailor"
                            row["studio"] = clean_text(source.get("studio")) or fallback_company or source_name
                        jobs.extend(parsed)
                    else:
                        slug = urlparse(job_link).path.rstrip("/").split("/")[-1]
                        title = slug.replace("-", " ").strip()
                        if title:
                            jobs.append(
                                {
                                    "sourceJobId": f"teamtailor:{source_name}:{slug}",
                                    "title": title,
                                    "company": fallback_company or "Unknown",
                                    "city": "",
                                    "country": "Unknown",
                                    "workType": "",
                                    "contractType": "",
                                    "jobLink": job_link,
                                    "sector": "Game",
                                    "postedAt": "",
                                    "adapter": "teamtailor",
                                    "studio": clean_text(source.get("studio")) or fallback_company or source_name,
                                }
                            )
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"teamtailor:{source_name}:{job_link}: {exc}")
            entry_report["keptCount"] = max(0, len(jobs) - kept_before)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"teamtailor:{source_name}:{listing_url}: {exc}")
        details.append(entry_report)

    set_source_diagnostics(
        "teamtailor_sources",
        adapter="teamtailor",
        studio="multiple",
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def run_scrapy_static_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> List[RawJob]:
    del fetch_text  # Scrapy adapter uses subprocess transport and does not consume fetch_text.
    import subprocess

    results_list: List[RawJob] = []
    errors_list: List[str] = []
    details: List[Dict[str, Any]] = []

    def _clean_errors(values: Any) -> List[str]:
        if not isinstance(values, list):
            return []
        cleaned = []
        for item in values:
            text = clean_text(item)
            if text:
                cleaned.append(text)
        return cleaned

    def _base_detail(source_row: Dict[str, Any], *, status: str = "error", error: str = "") -> Dict[str, Any]:
        source_name = clean_text(source_row.get("name")) or "unknown"
        studio_name = clean_text(source_row.get("studio")) or source_name
        pages = source_row.get("pages") if isinstance(source_row.get("pages"), list) else []
        source_id = clean_text(source_row.get("id"))
        if not source_id:
            seed = "|".join([source_name, studio_name, *[clean_text(page) for page in pages]])
            source_id = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
        return {
            "adapter": "scrapy_static",
            "studio": studio_name,
            "name": source_name,
            "status": status,
            "fetchedCount": 0,
            "keptCount": 0,
            "error": clean_text(error),
            "classification": "parse_error" if norm_text(status) == "error" else "ok_no_jobs",
            "top_reject_reasons": [],
            "browserFallbackRecommended": False,
            "sourceId": source_id,
            "pages": [clean_text(page) for page in pages if clean_text(page)],
            "loss": {
                "scrapyRunnerRejectedValidation": 0,
                "scrapyParentInvalidPayload": 0,
            },
        }

    def _coerce_int(value: Any) -> int:
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return 0

    def _normalize_job(raw: Any, source_row: Dict[str, Any]) -> Optional[RawJob]:
        if not isinstance(raw, dict):
            return None
        strict_validation = env_flag("BALUFFO_SCRAPY_VALIDATION_STRICT", DEFAULT_SCRAPY_VALIDATION_STRICT)
        source_name = clean_text(raw.get("source")) or (clean_text(source_row.get("name")) or "scrapy_static")
        studio_name = clean_text(raw.get("studio")) or (clean_text(source_row.get("studio")) or clean_text(source_row.get("name")) or "unknown")
        title = clean_text(raw.get("title"))
        company = clean_text(raw.get("company"))
        job_link = normalize_url(raw.get("jobLink"))
        source_job_id = clean_text(raw.get("sourceJobId"))
        if not title or not company:
            return None
        if not job_link and not strict_validation:
            source_bundle_raw = raw.get("sourceBundle")
            if isinstance(source_bundle_raw, list):
                for item in source_bundle_raw:
                    if not isinstance(item, dict):
                        continue
                    candidate = normalize_url(item.get("jobLink"))
                    if candidate:
                        job_link = candidate
                        break
        if not job_link:
            return None
        if not source_job_id:
            source_job_id = hashlib.sha1(f"{title}|{company}|{job_link}".encode("utf-8")).hexdigest()[:12]
        posted_at = to_iso(raw.get("postedAt"))
        source_bundle = raw.get("sourceBundle")
        if not isinstance(source_bundle, list) or not source_bundle:
            source_bundle = [
                {
                    "source": source_name,
                    "sourceJobId": source_job_id,
                    "jobLink": job_link,
                    "postedAt": posted_at,
                    "adapter": "scrapy_static",
                    "studio": studio_name,
                }
            ]

        return {
            "sourceJobId": source_job_id,
            "title": title,
            "company": company,
            "city": clean_text(raw.get("city")),
            "country": clean_text(raw.get("country")) or "Unknown",
            "workType": clean_text(raw.get("workType")),
            "contractType": clean_text(raw.get("contractType")),
            "jobLink": job_link,
            "sector": clean_text(raw.get("sector")) or "Game",
            "postedAt": posted_at,
            "source": source_name,
            "studio": studio_name,
            "adapter": clean_text(raw.get("adapter")) or "scrapy_static",
            "sourceBundle": source_bundle,
        }

    sources = registry_entries("scrapy_static")
    if not sources:
        set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[],
            partial_errors=["No enabled scrapy_static sources"],
        )
        return []

    runner_path = Path(__file__).resolve().parent / "scrapers" / "runner.py"
    if not runner_path.exists():
        msg = f"scrapy_static runner missing: {runner_path}"
        set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[_base_detail({"name": "scrapy_static"}, error=msg)],
            partial_errors=[msg],
        )
        return []

    for source in sources:
        source_name = clean_text(source.get("name")) or "unknown"
        studio_name = clean_text(source.get("studio")) or source_name
        pages = source.get("pages") if isinstance(source.get("pages"), list) else []
        config = {
            "source": {
                "name": source_name,
                "studio": studio_name,
                "pages": pages,
                "nlPriority": bool(source.get("nlPriority", False)),
                "remoteFriendly": bool(source.get("remoteFriendly", True)),
            },
            "runtime": {
                "timeout_s": int(timeout_s),
                "retries": int(retries),
                "backoff_s": float(backoff_s),
                "download_delay": 1.0,
            },
        }

        source_detail = _base_detail(source)
        try:
            timeout_window = min(300, max(1, int(timeout_s)) * max(1, len(pages)) * 4)
            result = subprocess.run(
                [sys.executable, str(runner_path)],
                input=json.dumps(config).encode("utf-8"),
                capture_output=True,
                timeout=timeout_window,
                check=False,
            )
            stderr_text = clean_text(result.stderr.decode("utf-8", errors="replace"))
            if result.returncode != 0:
                errors_list.append(f"{source_name}: subprocess exit {result.returncode}")
            if stderr_text and result.returncode != 0:
                errors_list.append(f"{source_name}: stderr: {stderr_text[:500]}")

            stdout_text = result.stdout.decode("utf-8", errors="replace")
            try:
                envelope = json.loads(stdout_text)
            except json.JSONDecodeError as exc:
                envelope = {}
                errors_list.append(f"{source_name}: JSON parse error: {exc}")
                if stderr_text:
                    errors_list.append(f"{source_name}: stderr: {stderr_text[:500]}")

            if not isinstance(envelope, dict) or "ok" not in envelope:
                source_detail.update(
                    {
                        "status": "error",
                        "error": "Invalid envelope from scraper runner",
                        "classification": "parse_error",
                        "browserFallbackRecommended": False,
                    }
                )
                if not isinstance(envelope, dict):
                    errors_list.append(f"{source_name}: invalid envelope type")
                else:
                    errors_list.append(f"{source_name}: invalid envelope missing 'ok'")
                details.append(source_detail)
                continue

            envelope_details = envelope.get("details")
            if isinstance(envelope_details, list) and envelope_details:
                detail_0 = envelope_details[0]
                if isinstance(detail_0, dict):
                    source_detail.update(
                        {
                            "status": "ok" if clean_text(detail_0.get("status")).lower() == "ok" else "error",
                            "fetchedCount": _coerce_int(detail_0.get("fetchedCount")),
                            "keptCount": _coerce_int(detail_0.get("keptCount")),
                            "error": clean_text(detail_0.get("error")),
                            "classification": clean_text(detail_0.get("classification")) or source_detail.get("classification"),
                            "browserFallbackRecommended": bool(detail_0.get("browserFallbackRecommended")),
                            "top_reject_reasons": detail_0.get("top_reject_reasons")
                            if isinstance(detail_0.get("top_reject_reasons"), list)
                            else [],
                            "sourceId": clean_text(detail_0.get("sourceId")) or source_detail.get("sourceId"),
                            "pages": detail_0.get("pages") if isinstance(detail_0.get("pages"), list) else source_detail.get("pages"),
                        }
                    )

            partial_errors = _clean_errors(envelope.get("partialErrors"))
            for item in partial_errors:
                errors_list.append(f"{source_name}: {item}")

            jobs = envelope.get("jobs")
            if bool(envelope.get("ok")) and isinstance(jobs, list):
                kept = 0
                parent_invalid_payload = 0
                for item in jobs:
                    normalized = _normalize_job(item, source)
                    if normalized:
                        kept += 1
                        results_list.append(normalized)
                    else:
                        parent_invalid_payload += 1
                        errors_list.append(f"{source_name}: dropped invalid job payload from runner")
                source_detail_loss = source_detail.get("loss") if isinstance(source_detail.get("loss"), dict) else {}
                source_detail_loss["scrapyParentInvalidPayload"] = int(parent_invalid_payload)
                source_detail["loss"] = source_detail_loss
                source_detail["keptCount"] = max(int(source_detail.get("keptCount") or 0), kept)
                source_detail["status"] = "ok"
                if not clean_text(source_detail.get("classification")):
                    source_detail["classification"] = "ok_with_jobs" if kept > 0 else "ok_no_jobs"
                if source_detail.get("classification") == "ok_no_jobs" and int(source_detail.get("fetchedCount") or 0) > 0:
                    source_detail["classification"] = "fetch_ok_extract_zero"
                source_detail["browserFallbackRecommended"] = bool(
                    source_detail.get("browserFallbackRecommended")
                    or source_detail.get("classification") in {"fetch_ok_extract_zero", "blocked_or_challenge"}
                )
            else:
                source_detail["status"] = "error"
                if not clean_text(source_detail.get("error")):
                    source_detail["error"] = "crawl failed"
                source_detail["classification"] = "parse_error"
                errors_list.append(f"{source_name}: crawl failed")

            stats = envelope.get("stats")
            if isinstance(stats, dict):
                source_detail["stats"] = {
                    "downloader/request_count": _coerce_int(stats.get("downloader/request_count")),
                    "downloader/response_count": _coerce_int(stats.get("downloader/response_count")),
                    "downloader/response_status_count/200": _coerce_int(stats.get("downloader/response_status_count/200")),
                    "retry/count": _coerce_int(stats.get("retry/count")),
                    "item_scraped_count": _coerce_int(stats.get("item_scraped_count")),
                    "candidate_links_found": _coerce_int(stats.get("candidate_links_found")),
                    "detail_pages_visited": _coerce_int(stats.get("detail_pages_visited")),
                    "jobs_emitted": _coerce_int(stats.get("jobs_emitted")),
                    "jobs_rejected_validation": _coerce_int(stats.get("jobs_rejected_validation")),
                    "finish_reason": clean_text(stats.get("finish_reason")),
                }
                source_detail_loss = source_detail.get("loss") if isinstance(source_detail.get("loss"), dict) else {}
                source_detail_loss["scrapyRunnerRejectedValidation"] = _coerce_int(stats.get("jobs_rejected_validation"))
                source_detail["loss"] = source_detail_loss
                if int(source_detail.get("fetchedCount") or 0) <= 0:
                    source_detail["fetchedCount"] = int(source_detail["stats"]["downloader/response_count"])

            details.append(source_detail)
        except subprocess.TimeoutExpired:
            source_detail.update(
                {
                    "status": "error",
                    "error": "subprocess timeout",
                    "classification": "timeout",
                    "browserFallbackRecommended": True,
                }
            )
            errors_list.append(f"{source_name}: subprocess timeout")
            details.append(source_detail)
        except Exception as exc:  # noqa: BLE001
            source_detail.update(
                {
                    "status": "error",
                    "error": clean_text(exc)[:500],
                    "classification": "parse_error",
                    "browserFallbackRecommended": False,
                }
            )
            errors_list.append(f"{source_name}: {type(exc).__name__}: {clean_text(exc)[:200]}")
            details.append(source_detail)

    set_source_diagnostics(
        "scrapy_static_sources",
        adapter="scrapy_static",
        studio="multiple",
        details=details,
        partial_errors=errors_list,
    )
    return results_list


def static_source_shard(row: Dict[str, Any]) -> str:
    label = clean_text(row.get("studio")) or clean_text(row.get("name"))
    first_alpha = ""
    for ch in label.lower():
        if "a" <= ch <= "z":
            first_alpha = ch
            break
    if not first_alpha:
        return "s_z"
    if "a" <= first_alpha <= "i":
        return "a_i"
    if "j" <= first_alpha <= "r":
        return "j_r"
    return "s_z"


def run_static_studio_pages_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    sources: Optional[List[Dict[str, Any]]] = None,
    shard: Optional[str] = None,
    diagnostics_name: str = "static_studio_pages",
) -> List[RawJob]:
    jobs: List[RawJob] = []
    errors: List[str] = []
    seen_links = set()
    details: List[Dict[str, Any]] = []
    ignored_link_titles = {
        "apply",
        "apply now",
        "learn more",
        "read more",
        "details",
        "view",
        "view details",
        "view job",
    }

    static_profile = norm_text(os.getenv("BALUFFO_STATIC_DETAIL_HEURISTICS_PROFILE")) or DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE
    default_path_tokens = ["/job/", "/jobs/", "/jobdetail/"]
    default_query_keys = ["job_id"]
    if static_profile == "broad":
        default_path_tokens.extend(["/career/", "/careers/", "/position/", "/positions/"])
        default_query_keys.extend(["gh_jid", "jid", "jobid"])

    def is_probable_job_detail_url(candidate_url: str, source_row: Dict[str, Any]) -> bool:
        parsed = urlparse(candidate_url)
        host = parsed.netloc.lower()
        path = parsed.path.lower()
        query = parsed.query.lower()
        if host.endswith("larian.com") and "/careers/location/" in path:
            return False
        path_tokens = list(default_path_tokens)
        query_keys = list(default_query_keys)
        source_path_tokens = source_row.get("detailPathTokens")
        source_query_keys = source_row.get("detailQueryKeys")
        if isinstance(source_path_tokens, list):
            path_tokens.extend([f"/{norm_text(token).strip('/')}/" for token in source_path_tokens if clean_text(token)])
        if isinstance(source_query_keys, list):
            query_keys.extend([norm_text(token) for token in source_query_keys if clean_text(token)])
        if re.search(r"/careers/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?:/|$)", path):
            return True
        if any(token and token in path for token in path_tokens) or bool(re.search(r"/en/j/\d+", path)):
            return True
        if any(key and f"{key}=" in query for key in query_keys):
            return True
        if "target-req=" in query and ("page=req" in query or "careerportal.aspx" in path):
            return True
        return False

    selected_sources = sources if isinstance(sources, list) else registry_entries("static")
    for source in selected_sources:
        if shard and static_source_shard(source) != shard:
            continue
        source_name = clean_text(source.get("name")) or "static_source"
        company = clean_text(source.get("company")) or source_name
        pages = source.get("pages") if isinstance(source.get("pages"), list) else []
        entry_report = {
            "adapter": "static",
            "studio": clean_text(source.get("studio")) or company or source_name,
            "name": source_name,
            "status": "ok",
            "fetchedCount": len(pages),
            "keptCount": 0,
            "error": "",
            "loss": {
                "staticNonJobUrlRejected": 0,
                "staticDuplicateLinkRejected": 0,
                "staticDetailParseEmpty": 0,
            },
        }
        kept_before = len(jobs)
        link_rejections: Counter[str] = Counter()

        for page in pages:
            page_url = clean_text(page)
            if not page_url:
                continue
            try:
                html = fetch_with_retries(page_url, fetch_text, timeout_s, retries, backoff_s)
                detail_links: List[Tuple[str, str]] = []
                detail_seen = set()
                listing_htmls = [html]
                try:
                    dynamic_listing_html = maybe_fetch_kojima_job_listing_html(
                        page_url=page_url,
                        page_html=html,
                        timeout_s=timeout_s,
                        retries=retries,
                        backoff_s=backoff_s,
                    )
                    if dynamic_listing_html and dynamic_listing_html not in listing_htmls:
                        listing_htmls.append(dynamic_listing_html)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"static:{source_name}:{page_url}: dynamic-listing-fetch failed: {exc}")

                for listing_html in listing_htmls:
                    parsed = parse_jobpostings_from_html(
                        listing_html,
                        base_url=page_url,
                        fallback_company=company,
                        fallback_source_id_prefix=f"static:{source_name}",
                    )
                    for row in parsed:
                        link = normalize_url(row.get("jobLink"))
                        if not link or link in seen_links:
                            continue
                        seen_links.add(link)
                        row["adapter"] = "static"
                        row["studio"] = clean_text(source.get("studio")) or company or source_name
                        jobs.append(row)

                    # Some custom listing tables expose role links in rows, but URLs do not
                    # match generic /job(s)/ path heuristics (e.g. /en/ai-programmer).
                    for row_match in re.finditer(
                        r'(?is)<(?:div|tr)[^>]*class=["\'][^"\']*job-listing-item[^"\']*["\'][^>]*>(.*?)</(?:div|tr)>',
                        listing_html,
                    ):
                        row_html = row_match.group(1) or ""
                        link_match = re.search(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', row_html)
                        if not link_match:
                            continue
                        href = clean_text(link_match.group(1))
                        anchor_text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", link_match.group(2) or ""))
                        absolute = urljoin(page_url, clean_text(href))
                        if absolute in detail_seen:
                            link_rejections["duplicate_link"] += 1
                            continue
                        detail_seen.add(absolute)
                        detail_links.append((absolute, anchor_text))

                    for match in re.finditer(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', listing_html):
                        href = clean_text(match.group(1))
                        anchor_inner = match.group(2) or ""
                        anchor_text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", anchor_inner))
                        absolute = urljoin(page_url, clean_text(href))
                        if not is_probable_job_detail_url(absolute, source):
                            link_rejections["non_job_url"] += 1
                            continue
                        if absolute in detail_seen:
                            link_rejections["duplicate_link"] += 1
                            continue
                        detail_seen.add(absolute)
                        detail_links.append((absolute, anchor_text))
                    # Some career sites embed job links in JSON payloads instead of anchor hrefs.
                    for raw in re.findall(r'https?://[^\s"\'<>]+', listing_html, flags=re.I):
                        absolute = clean_text(raw)
                        if not is_probable_job_detail_url(absolute, source):
                            link_rejections["non_job_url"] += 1
                            continue
                        if absolute in detail_seen:
                            link_rejections["duplicate_link"] += 1
                            continue
                        detail_seen.add(absolute)
                        detail_links.append((absolute, ""))

                for detail, detail_title in detail_links:
                    if detail in seen_links:
                        continue
                    try:
                        detail_html = fetch_with_retries(detail, fetch_text, timeout_s, retries, backoff_s)
                        detail_jobs = parse_jobpostings_from_html(
                            detail_html,
                            base_url=detail,
                            fallback_company=company,
                            fallback_source_id_prefix=f"static:{source_name}",
                        )
                        if detail_jobs:
                            for row in detail_jobs:
                                link = normalize_url(row.get("jobLink"))
                                if not link or link in seen_links:
                                    continue
                                seen_links.add(link)
                                row["adapter"] = "static"
                                row["studio"] = clean_text(source.get("studio")) or company or source_name
                                jobs.append(row)
                            continue
                        link_rejections["detail_parse_empty"] += 1
                        path_parts = [part for part in urlparse(detail).path.rstrip("/").split("/") if part]
                        slug = path_parts[-1] if path_parts else ""
                        if slug.lower() == "apply" and len(path_parts) >= 2:
                            slug = path_parts[-2]
                        slug = re.sub(r"_[Rr]\d+(?:-\d+)?$", "", slug)
                        title = strip_html_text(re.sub(r"[-_]+", " ", slug))
                        parsed_title = clean_text(detail_title)
                        if parsed_title and parsed_title.lower() not in ignored_link_titles:
                            title = parsed_title
                        if title:
                            if re.fullmatch(r"\d+", title):
                                continue
                            seen_links.add(detail)
                            jobs.append(
                                {
                                    "sourceJobId": f"static:{source_name}:{hashlib.sha1(detail.encode('utf-8')).hexdigest()[:10]}",
                                    "title": title.title(),
                                    "company": company,
                                    "city": "",
                                    "country": "Unknown",
                                    "workType": "",
                                    "contractType": "",
                                    "jobLink": detail,
                                    "sector": "Game",
                                    "postedAt": "",
                                    "adapter": "static",
                                    "studio": clean_text(source.get("studio")) or company or source_name,
                                }
                            )
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"static:{source_name}:{detail}: {exc}")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"static:{source_name}:{page_url}: {exc}")
        entry_report["keptCount"] = max(0, len(jobs) - kept_before)
        entry_report["loss"] = {
            "staticNonJobUrlRejected": int(link_rejections.get("non_job_url", 0)),
            "staticDuplicateLinkRejected": int(link_rejections.get("duplicate_link", 0)),
            "staticDetailParseEmpty": int(link_rejections.get("detail_parse_empty", 0)),
        }
        if entry_report["keptCount"] == 0 and pages:
            entry_report["status"] = "error"
            entry_report["error"] = "no jobs extracted from source pages"
        details.append(entry_report)

    diag_studio = "multiple"
    if len(selected_sources) == 1:
        single = selected_sources[0]
        diag_studio = clean_text(single.get("studio")) or clean_text(single.get("company")) or clean_text(single.get("name")) or "multiple"

    set_source_diagnostics(
        diagnostics_name,
        adapter="static",
        studio=diag_studio,
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def run_static_source_entry_source(
    *,
    source_row: Dict[str, Any],
    diagnostics_name: str,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> List[RawJob]:
    return run_static_studio_pages_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        sources=[source_row],
        diagnostics_name=diagnostics_name,
    )


def run_static_studio_pages_a_i_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return run_static_studio_pages_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        shard="a_i",
        diagnostics_name="static_studio_pages_a_i",
    )


def build_static_source_loaders() -> List[Tuple[str, SourceLoader]]:
    loaders: List[Tuple[str, SourceLoader]] = []
    for row in registry_entries("static"):
        source_id = clean_text(row.get("id"))
        if not source_id:
            listing_url = clean_text(row.get("listing_url"))
            digest_seed = listing_url or clean_text(row.get("name")) or json.dumps(row, sort_keys=True, ensure_ascii=False)
            source_id = f"auto:{hashlib.sha1(digest_seed.encode('utf-8')).hexdigest()[:12]}"
        loader_name = f"static_source::{source_id}"

        def _loader(
            *,
            fetch_text: Callable[[str, int], str],
            timeout_s: int,
            retries: int,
            backoff_s: float,
            _row: Dict[str, Any] = row,
            _loader_name: str = loader_name,
        ) -> List[RawJob]:
            return run_static_source_entry_source(
                source_row=_row,
                diagnostics_name=_loader_name,
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
            )

        loaders.append((loader_name, _loader))
    return loaders


def run_static_studio_pages_j_r_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return run_static_studio_pages_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        shard="j_r",
        diagnostics_name="static_studio_pages_j_r",
    )


def run_static_studio_pages_s_z_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    return run_static_studio_pages_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        shard="s_z",
        diagnostics_name="static_studio_pages_s_z",
    )


def run_lever_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    jobs: List[RawJob] = []
    errors: List[str] = []
    details: List[Dict[str, Any]] = []
    for source in registry_entries("lever"):
        source_name = clean_text(source.get("name")) or "lever_source"
        studio = clean_text(source.get("studio")) or source_name
        account = clean_text(source.get("account"))
        api_url = clean_text(source.get("api_url")) or (f"https://api.lever.co/v0/postings/{account}?mode=json" if account else "")
        entry_report = {
            "adapter": "lever",
            "studio": studio,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        if not api_url or not account:
            entry_report["status"] = "error"
            entry_report["error"] = "missing account/api_url"
            details.append(entry_report)
            continue
        try:
            text = fetch_with_retries(api_url, fetch_text, timeout_s, retries, backoff_s)
            payload = json.loads(text)
            parsed = parse_lever_jobs_payload(payload, account, fallback_company=studio)
            entry_report["fetchedCount"] = len(payload) if isinstance(payload, list) else len(parsed)
            entry_report["keptCount"] = len(parsed)
            for row in parsed:
                row["adapter"] = "lever"
                row["studio"] = studio
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"lever:{source_name}: {exc}")
        details.append(entry_report)

    set_source_diagnostics(
        "lever_sources",
        adapter="lever",
        studio="multiple",
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def run_smartrecruiters_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    jobs: List[RawJob] = []
    errors: List[str] = []
    details: List[Dict[str, Any]] = []
    for source in registry_entries("smartrecruiters"):
        source_name = clean_text(source.get("name")) or "smartrecruiters_source"
        studio = clean_text(source.get("studio")) or source_name
        company_id = clean_text(source.get("company_id"))
        api_url = clean_text(source.get("api_url")) or (
            f"https://api.smartrecruiters.com/v1/companies/{company_id}/postings" if company_id else ""
        )
        entry_report = {
            "adapter": "smartrecruiters",
            "studio": studio,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        if not company_id or not api_url:
            entry_report["status"] = "error"
            entry_report["error"] = "missing company_id/api_url"
            details.append(entry_report)
            continue
        try:
            text = fetch_with_retries(api_url, fetch_text, timeout_s, retries, backoff_s)
            payload = json.loads(text)
            parsed = parse_smartrecruiters_jobs_payload(payload, company_id, fallback_company=studio)
            entry_report["fetchedCount"] = len(payload.get("content", [])) if isinstance(payload, dict) else len(parsed)
            entry_report["keptCount"] = len(parsed)
            for row in parsed:
                row["adapter"] = "smartrecruiters"
                row["studio"] = studio
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"smartrecruiters:{source_name}: {exc}")
        details.append(entry_report)

    set_source_diagnostics(
        "smartrecruiters_sources",
        adapter="smartrecruiters",
        studio="multiple",
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def run_workable_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    jobs: List[RawJob] = []
    errors: List[str] = []
    details: List[Dict[str, Any]] = []
    for source in registry_entries("workable"):
        source_name = clean_text(source.get("name")) or "workable_source"
        studio = clean_text(source.get("studio")) or source_name
        account = clean_text(source.get("account"))
        api_url = clean_text(source.get("api_url")) or (
            f"https://apply.workable.com/api/v1/widget/accounts/{account}?details=true" if account else ""
        )
        entry_report = {
            "adapter": "workable",
            "studio": studio,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        if not account or not api_url:
            entry_report["status"] = "error"
            entry_report["error"] = "missing account/api_url"
            details.append(entry_report)
            continue
        try:
            text = fetch_with_retries(api_url, fetch_text, timeout_s, retries, backoff_s)
            payload = json.loads(text)
            parsed = parse_workable_jobs_payload(payload, account, fallback_company=studio)
            entry_report["fetchedCount"] = len(payload.get("jobs", [])) if isinstance(payload, dict) else len(parsed)
            entry_report["keptCount"] = len(parsed)
            for row in parsed:
                row["adapter"] = "workable"
                row["studio"] = studio
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"workable:{source_name}: {exc}")
        details.append(entry_report)

    set_source_diagnostics(
        "workable_sources",
        adapter="workable",
        studio="multiple",
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def run_ashby_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    jobs: List[RawJob] = []
    errors: List[str] = []
    details: List[Dict[str, Any]] = []
    for source in registry_entries("ashby"):
        source_name = clean_text(source.get("name")) or "ashby_source"
        studio = clean_text(source.get("studio")) or source_name
        board_url = clean_text(source.get("board_url"))
        entry_report = {
            "adapter": "ashby",
            "studio": studio,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        if not board_url:
            entry_report["status"] = "error"
            entry_report["error"] = "missing board_url"
            details.append(entry_report)
            continue
        try:
            text = fetch_with_retries(board_url, fetch_text, timeout_s, retries, backoff_s)
            parsed = parse_ashby_jobs_from_html(text, board_url, fallback_company=studio)
            entry_report["fetchedCount"] = len(parsed)
            entry_report["keptCount"] = len(parsed)
            if not parsed:
                entry_report["status"] = "error"
                entry_report["error"] = "no jobs extracted from ashby board html"
            for row in parsed:
                row["adapter"] = "ashby"
                row["studio"] = studio
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"ashby:{source_name}: {exc}")
        details.append(entry_report)

    set_source_diagnostics(
        "ashby_sources",
        adapter="ashby",
        studio="multiple",
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def run_personio_sources_source(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    jobs: List[RawJob] = []
    errors: List[str] = []
    details: List[Dict[str, Any]] = []
    for source in registry_entries("personio"):
        source_name = clean_text(source.get("name")) or "personio_source"
        studio = clean_text(source.get("studio")) or source_name
        feed_url = clean_text(source.get("feed_url"))
        entry_report = {
            "adapter": "personio",
            "studio": studio,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        if not feed_url:
            entry_report["status"] = "error"
            entry_report["error"] = "missing feed_url"
            details.append(entry_report)
            continue
        try:
            text = fetch_with_retries(feed_url, fetch_text, timeout_s, retries, backoff_s)
            parsed = parse_personio_feed_xml(text, source_name=studio)
            entry_report["fetchedCount"] = len(parsed)
            entry_report["keptCount"] = len(parsed)
            if not parsed:
                entry_report["status"] = "error"
                entry_report["error"] = "no jobs parsed from personio feed"
            for row in parsed:
                row["adapter"] = "personio"
                row["studio"] = studio
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"personio:{source_name}: {exc}")
        details.append(entry_report)

    set_source_diagnostics(
        "personio_sources",
        adapter="personio",
        studio="multiple",
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def canonicalize_job_with_reason(raw: Any, *, source: str, fetched_at: str) -> Tuple[Optional[RawJob], str]:
    if not isinstance(raw, dict):
        return None, "invalid_payload"
    title = clean_text(raw.get("title"))
    company = clean_text(raw.get("company"))
    if not title:
        return None, "missing_title"
    if not company:
        return None, "missing_company"
    normalized_link = normalize_url(raw.get("jobLink"))
    raw_link = clean_text(raw.get("jobLink"))
    if env_flag("BALUFFO_CANONICAL_STRICT_URL", DEFAULT_CANONICAL_STRICT_URL) and raw_link and not normalized_link:
        return None, "invalid_url"

    adapter = clean_text(raw.get("adapter"))
    studio = clean_text(raw.get("studio"))

    def normalize_source_bundle(value: Any) -> List[Dict[str, Any]]:
        entries = value
        if isinstance(entries, str):
            try:
                entries = json.loads(entries)
            except json.JSONDecodeError:
                entries = []
        if not isinstance(entries, list):
            entries = []
        normalized_entries: List[Dict[str, Any]] = []
        seen = set()
        for item in entries:
            if not isinstance(item, dict):
                continue
            normalized_item = {
                "source": clean_text(item.get("source")),
                "sourceJobId": clean_text(item.get("sourceJobId")),
                "jobLink": normalize_url(item.get("jobLink")),
                "postedAt": to_iso(item.get("postedAt")),
                "adapter": clean_text(item.get("adapter")),
                "studio": clean_text(item.get("studio")),
            }
            token = "|".join(
                [
                    norm_text(normalized_item.get("source")),
                    norm_text(normalized_item.get("sourceJobId")),
                    norm_text(normalized_item.get("jobLink")),
                ]
            )
            if token in seen:
                continue
            seen.add(token)
            normalized_entries.append(normalized_item)
        return normalized_entries

    source_bundle = normalize_source_bundle(raw.get("sourceBundle"))
    if not source_bundle:
        source_bundle = [
            {
                "source": source,
                "sourceJobId": clean_text(raw.get("sourceJobId") or raw.get("id")),
                "jobLink": normalize_url(raw.get("jobLink")),
                "postedAt": to_iso(raw.get("postedAt")),
                "adapter": adapter,
                "studio": studio,
            }
        ]

    normalized = {
        "id": "",
        "title": title,
        "company": company,
        "city": clean_text(raw.get("city")),
        "country": normalize_country(raw.get("country")),
        "workType": normalize_work_type(raw.get("workType")),
        "contractType": normalize_contract_type(raw.get("contractType"), title),
        "jobLink": normalized_link,
        "sector": normalize_sector(raw.get("sector"), company, title),
        "profession": map_profession(title),
        "companyType": classify_company_type(company, title),
        "description": f"{title} at {company}",
        "source": source,
        "sourceJobId": clean_text(raw.get("sourceJobId") or raw.get("id")),
        "fetchedAt": to_iso(raw.get("fetchedAt")) or fetched_at,
        "postedAt": to_iso(raw.get("postedAt")),
        "status": "active",
        "firstSeenAt": "",
        "lastSeenAt": "",
        "removedAt": "",
        "dedupKey": "",
        "qualityScore": 0,
        "focusScore": 0,
        "sourceBundleCount": len(source_bundle),
        "sourceBundle": source_bundle,
        "adapter": adapter,
        "studio": studio,
    }
    normalized["qualityScore"] = compute_quality_score(normalized)
    normalized["focusScore"] = compute_focus_score(normalized)
    return normalized, ""


def canonicalize_job(raw: RawJob, *, source: str, fetched_at: str) -> Optional[RawJob]:
    normalized, _reason = canonicalize_job_with_reason(raw, source=source, fetched_at=fetched_at)
    return normalized


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


def choose_base_record(left: RawJob, right: RawJob) -> Tuple[RawJob, RawJob]:
    left_rich = record_richness(left)
    right_rich = record_richness(right)
    if right_rich > left_rich:
        return right, left
    if left_rich > right_rich:
        return left, right
    if posted_ts(right.get("postedAt")) > posted_ts(left.get("postedAt")):
        return right, left
    return left, right


def merge_records(existing: RawJob, candidate: RawJob) -> RawJob:
    base, other = choose_base_record(existing, candidate)
    merged = dict(base)
    for field in OUTPUT_FIELDS:
        if not clean_text(merged.get(field)) and clean_text(other.get(field)):
            merged[field] = other[field]
    if posted_ts(other.get("postedAt")) > posted_ts(merged.get("postedAt")):
        merged["postedAt"] = to_iso(other.get("postedAt"))

    bundle: List[Dict[str, Any]] = []
    seen = set()
    for row in [existing, candidate, merged]:
        entries = row.get("sourceBundle")
        if not isinstance(entries, list):
            continue
        for item in entries:
            if not isinstance(item, dict):
                continue
            normalized_item = {
                "source": clean_text(item.get("source")),
                "sourceJobId": clean_text(item.get("sourceJobId")),
                "jobLink": normalize_url(item.get("jobLink")),
                "postedAt": to_iso(item.get("postedAt")),
                "adapter": clean_text(item.get("adapter")),
                "studio": clean_text(item.get("studio")),
            }
            key = "|".join(
                [
                    norm_text(normalized_item.get("source")),
                    norm_text(normalized_item.get("sourceJobId")),
                    norm_text(normalized_item.get("jobLink")),
                ]
            )
            if key in seen:
                continue
            seen.add(key)
            bundle.append(normalized_item)
    merged["sourceBundle"] = bundle
    merged["sourceBundleCount"] = len(bundle)
    merged["qualityScore"] = compute_quality_score(merged)
    merged["focusScore"] = compute_focus_score(merged)
    return merged


def deduplicate_jobs(rows: Sequence[RawJob]) -> Tuple[List[RawJob], Dict[str, int]]:
    merged_rows: List[RawJob] = []
    by_primary: Dict[str, int] = {}
    by_secondary: Dict[str, int] = {}
    by_social: Dict[str, int] = {}
    merges = 0
    merged_by_primary = 0
    merged_by_secondary = 0
    merged_by_social = 0
    merge_samples: List[Dict[str, str]] = []

    for row in rows:
        primary = fingerprint_url(row.get("jobLink"))
        secondary = dedup_secondary_key(row)
        social_key = ""
        if clean_text(row.get("source")) in SOCIAL_SOURCE_NAMES and clean_text(row.get("sourceJobId")):
            social_key = f"{clean_text(row.get('source'))}|{clean_text(row.get('sourceJobId'))}"

        target_idx: Optional[int] = None
        merge_reason = ""
        if primary and primary in by_primary:
            target_idx = by_primary[primary]
            merge_reason = "primary_url"
        elif secondary and secondary in by_secondary:
            target_idx = by_secondary[secondary]
            merge_reason = "secondary_key"
        elif social_key and social_key in by_social:
            target_idx = by_social[social_key]
            merge_reason = "social_key"

        if target_idx is None:
            item = dict(row)
            if primary:
                item["dedupKey"] = f"url:{primary}"
            elif secondary:
                item["dedupKey"] = f"secondary:{hashlib.sha1(secondary.encode('utf-8')).hexdigest()}"
            elif social_key:
                item["dedupKey"] = f"social:{hashlib.sha1(social_key.encode('utf-8')).hexdigest()}"
            else:
                item["dedupKey"] = f"secondary:{hashlib.sha1('|'.join([norm_text(item.get('company')), norm_text(item.get('title'))]).encode('utf-8')).hexdigest()}"
            item["qualityScore"] = compute_quality_score(item)
            item["focusScore"] = compute_focus_score(item)
            merged_rows.append(item)
            idx = len(merged_rows) - 1
            if primary:
                by_primary[primary] = idx
            if secondary:
                by_secondary[secondary] = idx
            if social_key:
                by_social[social_key] = idx
            continue

        merges += 1
        if merge_reason == "primary_url":
            merged_by_primary += 1
        elif merge_reason == "secondary_key":
            merged_by_secondary += 1
        elif merge_reason == "social_key":
            merged_by_social += 1
        if len(merge_samples) < 10:
            merge_samples.append(
                {
                    "reason": merge_reason or "unknown",
                    "existingDedupKey": clean_text(merged_rows[target_idx].get("dedupKey")),
                    "incomingSource": clean_text(row.get("source")),
                    "incomingTitle": clean_text(row.get("title")),
                    "incomingCompany": clean_text(row.get("company")),
                    "incomingJobLink": normalize_url(row.get("jobLink")),
                }
            )
        merged = merge_records(merged_rows[target_idx], row)
        primary = fingerprint_url(merged.get("jobLink"))
        secondary = dedup_secondary_key(merged)
        merged_social_key = ""
        if clean_text(merged.get("source")) in SOCIAL_SOURCE_NAMES and clean_text(merged.get("sourceJobId")):
            merged_social_key = f"{clean_text(merged.get('source'))}|{clean_text(merged.get('sourceJobId'))}"
        if primary:
            merged["dedupKey"] = f"url:{primary}"
        elif secondary:
            merged["dedupKey"] = f"secondary:{hashlib.sha1(secondary.encode('utf-8')).hexdigest()}"
        elif merged_social_key:
            merged["dedupKey"] = f"social:{hashlib.sha1(merged_social_key.encode('utf-8')).hexdigest()}"
        merged_rows[target_idx] = merged
        if primary:
            by_primary[primary] = target_idx
        if secondary:
            by_secondary[secondary] = target_idx
        if merged_social_key:
            by_social[merged_social_key] = target_idx

    merged_rows.sort(
        key=lambda item: (
            int(item.get("focusScore") or 0),
            posted_ts(item.get("postedAt")),
            norm_text(item.get("title")),
        ),
        reverse=True,
    )
    for idx, row in enumerate(merged_rows, start=1):
        row["id"] = idx
    return merged_rows, {
        "inputCount": len(rows),
        "mergedCount": merges,
        "outputCount": len(merged_rows),
        "mergedByPrimaryUrl": merged_by_primary,
        "mergedBySecondaryKey": merged_by_secondary,
        "mergedBySocialKey": merged_by_social,
        "collisionSamplesCount": len(merge_samples),
        "collisionSamples": merge_samples,
    }


def default_source_loaders(
    *,
    social_enabled: bool = False,
    social_config: Optional[Dict[str, Any]] = None,
) -> List[Tuple[str, SourceLoader]]:
    social_cfg = social_config if isinstance(social_config, dict) else load_social_config(
        config_path=DEFAULT_SOCIAL_CONFIG_PATH,
        enabled=bool(social_enabled),
        lookback_minutes=DEFAULT_SOCIAL_LOOKBACK_MINUTES,
    )
    available = {
        "google_sheets": run_google_sheets_source,
        "remote_ok": run_remote_ok_source,
        "gamesindustry": run_gamesindustry_source,
        "epic_games_careers": run_epic_games_careers_source,
        "greenhouse_boards": run_greenhouse_boards_source,
        "teamtailor_sources": run_teamtailor_sources_source,
        "lever_sources": run_lever_sources_source,
        "smartrecruiters_sources": run_smartrecruiters_sources_source,
        "workable_sources": run_workable_sources_source,
        "ashby_sources": run_ashby_sources_source,
        "personio_sources": run_personio_sources_source,
        "scrapy_static_sources": run_scrapy_static_source,
        "social_reddit": lambda **kwargs: run_social_reddit_source(**kwargs, social_config=social_cfg),
        "social_x": lambda **kwargs: run_social_x_source(**kwargs, social_config=social_cfg),
        "social_mastodon": lambda **kwargs: run_social_mastodon_source(**kwargs, social_config=social_cfg),
        "static_studio_pages_a_i": run_static_studio_pages_a_i_source,
        "static_studio_pages_j_r": run_static_studio_pages_j_r_source,
        "static_studio_pages_s_z": run_static_studio_pages_s_z_source,
        "static_studio_pages": run_static_studio_pages_source,
    }
    base_loaders = [(name, available[name]) for name in DEFAULT_SOURCE_LOADER_NAMES if name in available]
    base_loaders = [
        (name, loader)
        for name, loader in base_loaders
        if name not in {"static_studio_pages", "static_studio_pages_a_i", "static_studio_pages_j_r", "static_studio_pages_s_z"}
    ]
    if not bool(social_cfg.get("enabled")):
        base_loaders = [(name, loader) for name, loader in base_loaders if name not in SOCIAL_SOURCE_NAMES]
    return base_loaders + build_static_source_loaders()


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
    lifecycle = lifecycle_counts_map or {}
    raw_fetched = int(sum(int(row.get("fetchedCount") or 0) for row in source_reports if norm_text(row.get("status")) == "ok"))
    canonical_kept = int(canonical_count)
    canonical_dropped = max(0, raw_fetched - canonical_kept)
    dedup_merged = int(dedup_stats.get("mergedCount") or 0)
    final_output = len(deduped_rows)
    return {
        **dedup_stats,
        "rawFetched": raw_fetched,
        "canonicalDropped": canonical_dropped,
        "canonicalKept": canonical_kept,
        "dedupMerged": dedup_merged,
        "finalOutput": final_output,
        "rawFetchedCount": canonical_count,
        "uniqueOutputCount": len(deduped_rows),
        "sourceBundleCollisions": sum(1 for row in deduped_rows if int(row.get("sourceBundleCount") or 0) > 1),
        "targetRoleCount": sum(1 for row in deduped_rows if norm_text(row.get("profession")) in TARGET_PROFESSIONS),
        "netherlandsCount": sum(1 for row in deduped_rows if clean_text(row.get("country")).upper() == "NL"),
        "remoteCount": sum(1 for row in deduped_rows if norm_text(row.get("workType")) == "remote"),
        "targetRoleNetherlandsCount": sum(
            1
            for row in deduped_rows
            if norm_text(row.get("profession")) in TARGET_PROFESSIONS and clean_text(row.get("country")).upper() == "NL"
        ),
        "targetRoleRemoteCount": sum(
            1
            for row in deduped_rows
            if norm_text(row.get("profession")) in TARGET_PROFESSIONS and norm_text(row.get("workType")) == "remote"
        ),
        "preservedPreviousOutput": preserved_previous,
        "sourceCount": len(source_reports),
        "successfulSources": sum(1 for row in source_reports if row["status"] == "ok"),
        "failedSources": sum(1 for row in source_reports if row["status"] == "error"),
        "excludedSources": sum(1 for row in source_reports if row["status"] == "excluded"),
        "activeSourceCount": active_source_count,
        "pendingSourceCount": pending_source_count,
        "newlyApprovedSinceLastRun": newly_approved_since_last_run,
        "jsonBytes": int(json_bytes),
        "csvBytes": int(csv_bytes),
        "lightJsonBytes": int(light_json_bytes),
        "sizeGuardrailExceeded": bool(json_bytes > 50_000_000 or csv_bytes > 50_000_000),
        "recordGuardrailExceeded": bool(len(deduped_rows) > 100_000),
        "lifecycleActiveCount": int(lifecycle.get("active") or 0),
        "lifecycleLikelyRemovedCount": int(lifecycle.get("likelyRemoved") or 0),
        "lifecycleArchivedCount": int(lifecycle.get("archived") or 0),
        "lifecycleTrackedCount": int(lifecycle.get("totalTracked") or 0),
    }


def build_browser_fallback_queue(
    source_reports: Sequence[Dict[str, Any]],
    *,
    generated_at: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen = set()
    for report in source_reports:
        details = report.get("details") if isinstance(report, dict) else None
        if not isinstance(details, list):
            continue
        for item in details:
            if not isinstance(item, dict):
                continue
            classification = norm_text(item.get("classification"))
            recommend = bool(item.get("browserFallbackRecommended"))
            if not recommend or classification not in {"fetch_ok_extract_zero", "blocked_or_challenge", "timeout"}:
                continue
            source_id = clean_text(item.get("sourceId"))
            name = clean_text(item.get("name"))
            studio = clean_text(item.get("studio"))
            pages = item.get("pages") if isinstance(item.get("pages"), list) else []
            clean_pages = [clean_text(page) for page in pages if clean_text(page)] or [""]
            for page in clean_pages:
                dedupe_key = hashlib.sha1(
                    "|".join(["scrapy_static", source_id or name, page]).encode("utf-8")
                ).hexdigest()
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                rows.append(
                    {
                        "dedupeKey": dedupe_key,
                        "adapter": "scrapy_static",
                        "sourceId": source_id,
                        "name": name,
                        "studio": studio,
                        "page": page,
                        "classification": classification,
                        "reason": clean_text(item.get("error")) or classification,
                        "generatedAt": clean_text(generated_at),
                    }
                )
    rows.sort(key=lambda row: (clean_text(row.get("studio")), clean_text(row.get("name")), clean_text(row.get("page"))))
    return rows


def read_previously_successful_sources(report_path: Path) -> set[str]:
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    if not isinstance(payload, dict):
        return set()
    rows = payload.get("sources")
    if not isinstance(rows, list):
        return set()
    successful: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = clean_text(row.get("name"))
        if not name:
            continue
        status = norm_text(row.get("status"))
        kept = int(row.get("keptCount") or 0)
        if status == "ok" and kept > 0:
            successful.add(name)
    return successful


def read_success_cache(cache_path: Path) -> set[str]:
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    if not isinstance(payload, dict):
        return set()
    rows = payload.get("successfulSources")
    if not isinstance(rows, list):
        return set()
    return {clean_text(item) for item in rows if clean_text(item)}


def write_success_cache(cache_path: Path, source_reports: Sequence[Dict[str, Any]]) -> None:
    successful = {
        clean_text(row.get("name"))
        for row in source_reports
        if norm_text(row.get("status")) == "ok" and int(row.get("keptCount") or 0) > 0 and clean_text(row.get("name"))
    }
    if not successful:
        return
    previous = read_success_cache(cache_path)
    merged = sorted(previous | successful)
    payload = {
        "updatedAt": now_iso(),
        "successfulSources": merged,
    }
    write_text_if_changed(cache_path, json.dumps(payload, indent=2, ensure_ascii=False))


def source_rows_fingerprint(rows: Sequence[RawJob]) -> str:
    keys = []
    for row in rows:
        link = normalize_url(row.get("jobLink"))
        source_job_id = clean_text(row.get("sourceJobId"))
        title = norm_text(row.get("title"))
        keys.append(f"{source_job_id}|{link}|{title}")
    keys.sort()
    digest = hashlib.sha1("\n".join(keys).encode("utf-8")).hexdigest()
    return digest


def _clamped_int(value: Any, default: int = 0, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(minimum, parsed)


def normalize_source_state_payload(payload: Dict[str, Any], *, updated_at: str = "") -> Dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    rows = src.get("sources")
    out_rows: Dict[str, Dict[str, Any]] = {}
    if isinstance(rows, dict):
        for raw_name, raw_entry in rows.items():
            name = clean_text(raw_name)
            if not name or not isinstance(raw_entry, dict):
                continue
            entry = {
                "lastRunAt": clean_text(raw_entry.get("lastRunAt")),
                "lastStatus": clean_text(raw_entry.get("lastStatus")),
                "lastDurationMs": _clamped_int(raw_entry.get("lastDurationMs"), 0, 0),
                "lastFetchedCount": _clamped_int(raw_entry.get("lastFetchedCount"), 0, 0),
                "lastKeptCount": _clamped_int(raw_entry.get("lastKeptCount"), 0, 0),
                "lastSuccessAt": clean_text(raw_entry.get("lastSuccessAt")),
                "lastFingerprint": clean_text(raw_entry.get("lastFingerprint")),
                "consecutiveFailures": _clamped_int(raw_entry.get("consecutiveFailures"), 0, 0),
                "quarantinedUntilAt": clean_text(raw_entry.get("quarantinedUntilAt")),
                "lastFailureAt": clean_text(raw_entry.get("lastFailureAt")),
                "lastError": clean_text(raw_entry.get("lastError")),
            }
            out_rows[name] = {key: value for key, value in entry.items() if value not in {"", None}}
    return {
        "schemaVersion": SCHEMA_VERSION,
        "updatedAt": clean_text(src.get("updatedAt")) or clean_text(updated_at) or now_iso(),
        "sources": out_rows,
    }


def read_source_state(state_path: Path) -> Dict[str, Dict[str, Any]]:
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    normalized = normalize_source_state_payload(payload)
    rows = normalized.get("sources")
    if isinstance(rows, dict):
        return rows
    return {}


def write_source_state(state_path: Path, rows: Dict[str, Dict[str, Any]]) -> None:
    payload = normalize_source_state_payload({"sources": rows}, updated_at=now_iso())
    write_text_if_changed(state_path, json.dumps(payload, indent=2, ensure_ascii=False))


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
    src = payload if isinstance(payload, dict) else {}
    raw_jobs = src.get("jobs")
    out_jobs: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw_jobs, dict):
        for raw_key, raw_entry in raw_jobs.items():
            key = clean_text(raw_key)
            if not key or not isinstance(raw_entry, dict):
                continue
            status = norm_text(raw_entry.get("status")) or "active"
            if status not in {"active", "likely_removed", "archived"}:
                status = "active"
            entry = {
                "status": status,
                "firstSeenAt": clean_text(raw_entry.get("firstSeenAt")),
                "lastSeenAt": clean_text(raw_entry.get("lastSeenAt")),
                "removedAt": clean_text(raw_entry.get("removedAt")),
                "archivedAt": clean_text(raw_entry.get("archivedAt")),
                "title": clean_text(raw_entry.get("title")),
                "company": clean_text(raw_entry.get("company")),
                "jobLink": normalize_url(raw_entry.get("jobLink")),
                "source": clean_text(raw_entry.get("source")),
                "sourceJobId": clean_text(raw_entry.get("sourceJobId")),
                "postedAt": to_iso(raw_entry.get("postedAt")),
            }
            out_jobs[key] = {field: value for field, value in entry.items() if value not in {"", None}}
    return {
        "schemaVersion": SCHEMA_VERSION,
        "updatedAt": clean_text(src.get("updatedAt")) or clean_text(updated_at) or now_iso(),
        "jobs": out_jobs,
    }


def read_job_lifecycle_state(state_path: Path) -> Dict[str, Dict[str, Any]]:
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    normalized = normalize_job_lifecycle_payload(payload)
    rows = normalized.get("jobs")
    if isinstance(rows, dict):
        return rows
    return {}


def write_job_lifecycle_state(state_path: Path, rows: Dict[str, Dict[str, Any]]) -> None:
    payload = normalize_job_lifecycle_payload({"jobs": rows}, updated_at=now_iso())
    write_text_if_changed(state_path, json.dumps(payload, indent=2, ensure_ascii=False))


def lifecycle_counts(rows: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    counts = {"active": 0, "likelyRemoved": 0, "archived": 0, "totalTracked": len(rows)}
    for entry in rows.values():
        status = norm_text(entry.get("status"))
        if status == "active":
            counts["active"] += 1
        elif status == "likely_removed":
            counts["likelyRemoved"] += 1
        elif status == "archived":
            counts["archived"] += 1
    return counts


def apply_job_lifecycle_state(
    *,
    deduped_rows: List[RawJob],
    lifecycle_rows: Dict[str, Dict[str, Any]],
    finished_at: str,
    allow_mark_missing: bool,
    remove_to_archive_days: int = LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS,
    archive_retention_days: int = LIFECYCLE_ARCHIVE_RETENTION_DAYS,
) -> Tuple[List[RawJob], Dict[str, Dict[str, Any]], Dict[str, int]]:
    next_rows: Dict[str, Dict[str, Any]] = {clean_text(key): dict(value) for key, value in (lifecycle_rows or {}).items() if clean_text(key)}
    seen_keys: set[str] = set()

    for row in deduped_rows:
        key = _job_identity_key(row)
        if not key:
            continue
        seen_keys.add(key)
        previous = dict(next_rows.get(key) or {})
        first_seen_at = clean_text(previous.get("firstSeenAt")) or finished_at
        row["status"] = "active"
        row["firstSeenAt"] = first_seen_at
        row["lastSeenAt"] = finished_at
        row["removedAt"] = ""

        next_rows[key] = {
            "status": "active",
            "firstSeenAt": first_seen_at,
            "lastSeenAt": finished_at,
            "title": clean_text(row.get("title")),
            "company": clean_text(row.get("company")),
            "jobLink": normalize_url(row.get("jobLink")),
            "source": clean_text(row.get("source")),
            "sourceJobId": clean_text(row.get("sourceJobId")),
            "postedAt": to_iso(row.get("postedAt")),
        }

    if allow_mark_missing:
        now_dt = parse_datetime(finished_at) or datetime.now(timezone.utc)
        for key, entry in list(next_rows.items()):
            if key in seen_keys:
                continue
            status = norm_text(entry.get("status")) or "active"
            removed_at = clean_text(entry.get("removedAt")) or finished_at
            if status == "active":
                entry["status"] = "likely_removed"
                entry["removedAt"] = finished_at
            elif status == "likely_removed":
                removed_dt = parse_datetime(removed_at)
                age_days = int((now_dt - removed_dt).total_seconds() // (24 * 60 * 60)) if removed_dt else 0
                if age_days >= max(1, int(remove_to_archive_days or 1)):
                    entry["status"] = "archived"
                    entry["archivedAt"] = finished_at
                    entry["removedAt"] = removed_at
            next_rows[key] = entry

        retention_days = max(1, int(archive_retention_days or 1))
        for key, entry in list(next_rows.items()):
            if norm_text(entry.get("status")) != "archived":
                continue
            archived_dt = parse_datetime(entry.get("archivedAt") or entry.get("removedAt"))
            if not archived_dt:
                continue
            age_days = int((now_dt - archived_dt).total_seconds() // (24 * 60 * 60))
            if age_days > retention_days:
                next_rows.pop(key, None)

    counts = lifecycle_counts(next_rows)
    return deduped_rows, next_rows, counts


def normalize_runtime_payload(runtime: Dict[str, Any], *, selected_source_count: int) -> Dict[str, Any]:
    src = runtime if isinstance(runtime, dict) else {}
    return {
        "maxWorkers": _clamped_int(src.get("maxWorkers"), 1, 1),
        "maxPerDomain": _clamped_int(src.get("maxPerDomain"), 1, 1),
        "fetchStrategy": clean_text(src.get("fetchStrategy")) or DEFAULT_FETCH_STRATEGY,
        "fetchClient": clean_text(src.get("fetchClient")) or "urllib",
        "adapterHttpConcurrency": _clamped_int(src.get("adapterHttpConcurrency"), DEFAULT_ADAPTER_HTTP_CONCURRENCY, 1),
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
    respect_source_cadence: bool = False,
    hot_source_cadence_minutes: int = DEFAULT_HOT_SOURCE_CADENCE_MINUTES,
    cold_source_cadence_minutes: int = DEFAULT_COLD_SOURCE_CADENCE_MINUTES,
    circuit_breaker_failures: int = 3,
    circuit_breaker_cooldown_minutes: int = 180,
    ignore_circuit_breaker: bool = False,
    social_enabled: bool = False,
    social_config_path: Optional[Path] = None,
    social_lookback_minutes: int = DEFAULT_SOCIAL_LOOKBACK_MINUTES,
    show_progress: bool = True,
    selection_exclusions: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = output_dir / "jobs-unified.json"
    csv_path = output_dir / "jobs-unified.csv"
    light_json_path = output_dir / "jobs-unified-light.json"
    report_path = output_dir / "jobs-fetch-report.json"
    success_cache_path = output_dir / "jobs-success-cache.json"
    source_state_path = output_dir / "jobs-source-state.json"
    lifecycle_state_path = output_dir / "jobs-lifecycle-state.json"
    browser_fallback_queue_path = output_dir / "jobs-browser-fallback-queue.json"
    task_state_path = output_dir / "jobs-fetch-tasks.json"
    pending_registry_path = output_dir / "source-registry-pending.json"
    approval_state_path = output_dir / "source-approval-state.json"
    SOURCE_DIAGNOSTICS.clear()

    started_at = now_iso()
    source_reports: List[Dict[str, Any]] = []
    if isinstance(selection_exclusions, list):
        source_reports.extend([row for row in selection_exclusions if isinstance(row, dict)])
    canonical_rows: List[RawJob] = []
    max_workers = max(1, int(max_workers or 1))
    max_per_domain = max(1, int(max_per_domain or 1))
    adapter_http_concurrency = max(1, int(adapter_http_concurrency or 1))
    hot_source_cadence_minutes = max(1, int(hot_source_cadence_minutes or 1))
    cold_source_cadence_minutes = max(1, int(cold_source_cadence_minutes or 1))
    fetch_text_impl, fetch_client, async_fetcher = resolve_fetch_text_impl(
        fetch_text=fetch_text,
        fetch_strategy=fetch_strategy,
        adapter_http_concurrency=adapter_http_concurrency,
    )
    source_state_rows = read_source_state(source_state_path)
    lifecycle_rows = read_job_lifecycle_state(lifecycle_state_path)
    if seed_from_existing_output:
        canonical_rows.extend(
            read_existing_output_from_file(
                json_path,
                started_at,
                canonicalize_job=canonicalize_job,
                clean_text=clean_text,
            )
        )

    def write_progress_report() -> None:
        deduped_progress_rows, dedup_progress_stats = deduplicate_jobs(canonical_rows)
        dedup_progress_stats["outputCount"] = len(deduped_progress_rows)
        progress_lifecycle_counts = lifecycle_counts(lifecycle_rows)
        progress_payload = normalize_fetch_report_payload({
            "schemaVersion": SCHEMA_VERSION,
            "startedAt": started_at,
            "finishedAt": "",
            "runtime": runtime_payload,
            "summary": build_pipeline_summary(
                dedup_progress_stats,
                deduped_progress_rows,
                source_reports,
                len(canonical_rows),
                False,
                len([row for row in STUDIO_SOURCE_REGISTRY if bool(row.get("enabledByDefault", True))]),
                len(load_registry_from_file(pending_registry_path, [])),
                read_approved_since_last_run(approval_state_path),
                json_bytes=0,
                csv_bytes=0,
                light_json_bytes=0,
                lifecycle_counts_map=progress_lifecycle_counts,
            ),
            "sources": source_reports,
            "outputs": {
                "json": str(json_path),
                "csv": str(csv_path),
                "lightJson": str(light_json_path),
                "report": str(report_path),
                "lifecycleState": str(lifecycle_state_path),
                "changed": {"json": False, "csv": False, "lightJson": False},
            },
        })
        write_text_if_changed(report_path, json.dumps(progress_payload, indent=2, ensure_ascii=False))

    effective_social_config_path = Path(social_config_path) if social_config_path else (output_dir / "social-sources-config.json")
    social_config = load_social_config(
        config_path=effective_social_config_path,
        enabled=bool(social_enabled),
        lookback_minutes=social_lookback_minutes,
    )

    if source_loaders is None:
        try:
            selected_loaders = default_source_loaders(
                social_enabled=bool(social_enabled),
                social_config=social_config,
            )
        except TypeError:
            selected_loaders = default_source_loaders()
    else:
        selected_loaders = list(source_loaders)
    using_default_loaders = source_loaders is None
    runtime_payload = normalize_runtime_payload({
        "maxWorkers": max_workers,
        "maxPerDomain": max_per_domain,
        "fetchStrategy": clean_text(fetch_strategy) or DEFAULT_FETCH_STRATEGY,
        "fetchClient": fetch_client,
        "adapterHttpConcurrency": adapter_http_concurrency,
        "seedFromExistingOutput": bool(seed_from_existing_output),
        "sourceTtlMinutes": int(source_ttl_minutes or 0),
        "respectSourceCadence": bool(respect_source_cadence),
        "hotSourceCadenceMinutes": hot_source_cadence_minutes,
        "coldSourceCadenceMinutes": cold_source_cadence_minutes,
        "circuitBreakerFailures": int(circuit_breaker_failures or 0),
        "circuitBreakerCooldownMinutes": int(circuit_breaker_cooldown_minutes or 0),
        "ignoreCircuitBreaker": bool(ignore_circuit_breaker),
        "socialEnabled": bool(social_enabled),
        "socialConfigPath": str(effective_social_config_path),
        "socialLookbackMinutes": int(social_config.get("lookbackMinutes") or DEFAULT_SOCIAL_LOOKBACK_MINUTES),
        "socialMinConfidence": int(social_config.get("minConfidence") or DEFAULT_SOCIAL_MIN_CONFIDENCE),
        "staticDetailHeuristicsProfile": norm_text(os.getenv("BALUFFO_STATIC_DETAIL_HEURISTICS_PROFILE"))
        or DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE,
        "scrapyValidationStrict": env_flag("BALUFFO_SCRAPY_VALIDATION_STRICT", DEFAULT_SCRAPY_VALIDATION_STRICT),
        "canonicalStrictUrlValidation": env_flag("BALUFFO_CANONICAL_STRICT_URL", DEFAULT_CANONICAL_STRICT_URL),
        "selectedSourceCount": len(selected_loaders),
    }, selected_source_count=len(selected_loaders))

    if respect_source_cadence:
        cadence_skipped: List[Dict[str, Any]] = []
        filtered_loaders: List[Tuple[str, SourceLoader]] = []
        for name, loader in selected_loaders:
            if should_skip_source_by_cadence(
                name,
                source_state_rows,
                hot_minutes=hot_source_cadence_minutes,
                cold_minutes=cold_source_cadence_minutes,
            ):
                cadence_skipped.append(_build_excluded_source_report(name, "skipped_by_source_cadence"))
                continue
            filtered_loaders.append((name, loader))
        selected_loaders = filtered_loaders
        source_reports.extend(cadence_skipped)
        runtime_payload["selectedSourceCount"] = len(selected_loaders)

    selected_loaders, excluded_by_circuit = apply_circuit_breaker_exclusions(
        selected_loaders,
        source_state_rows=source_state_rows,
        circuit_breaker_failures=circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
        ignore_circuit_breaker=ignore_circuit_breaker,
    )
    source_reports.extend(excluded_by_circuit)

    task_rows: Dict[str, Dict[str, Any]] = {
        name: {
            "name": name,
            "status": "queued",
            "startedAt": "",
            "finishedAt": "",
            "durationMs": 0,
            "heartbeatAt": "",
            "error": "",
        }
        for name, _ in selected_loaders
    }
    task_lock = threading.Lock()
    last_task_write_monotonic = 0.0
    last_heartbeat_write: Dict[str, float] = {}

    def write_task_state(finished_at: str = "", *, force: bool = False) -> None:
        nonlocal last_task_write_monotonic
        now_mono = time.perf_counter()
        if not force and (now_mono - last_task_write_monotonic) < 0.9:
            return
        last_task_write_monotonic = now_mono
        with task_lock:
            rows_snapshot = [dict(row) for row in task_rows.values()]
        payload = normalize_task_state_payload({
            "startedAt": started_at,
            "finishedAt": finished_at,
            "summary": {
                "queued": sum(1 for row in rows_snapshot if row.get("status") == "queued"),
                "running": sum(1 for row in rows_snapshot if row.get("status") == "running"),
                "ok": sum(1 for row in rows_snapshot if row.get("status") == "ok"),
                "error": sum(1 for row in rows_snapshot if row.get("status") == "error"),
            },
            "tasks": rows_snapshot,
            "outputs": {"report": str(report_path)},
        }, started_at=started_at, finished_at=finished_at, report_path=str(report_path))
        write_text_if_changed(task_state_path, json.dumps(payload, indent=2, ensure_ascii=False))

    thread_local = threading.local()
    domain_lock = threading.Lock()
    domain_gates: Dict[str, threading.BoundedSemaphore] = {}

    def fetch_text_limited(url: str, timeout: int) -> str:
        host = clean_text(urlparse(url).netloc).lower() or "_unknown"
        with domain_lock:
            gate = domain_gates.get(host)
            if gate is None:
                gate = threading.BoundedSemaphore(max_per_domain)
                domain_gates[host] = gate
        gate.acquire()
        try:
            current = clean_text(getattr(thread_local, "source_name", ""))
            if current and current in task_rows:
                now_mono = time.perf_counter()
                if (now_mono - float(last_heartbeat_write.get(current) or 0.0)) >= 4.0:
                    with task_lock:
                        if task_rows[current].get("status") == "running":
                            task_rows[current]["heartbeatAt"] = now_iso()
                    last_heartbeat_write[current] = now_mono
                    write_task_state()
            return fetch_text_impl(url, timeout)
        finally:
            gate.release()

    def execute_loader(name: str, loader: SourceLoader) -> Tuple[Dict[str, Any], List[RawJob]]:
        source_started = time.perf_counter()
        base_meta = SOURCE_REPORT_META.get(name, {})
        report: Dict[str, Any] = {
            "name": name,
            "status": "ok",
            "adapter": clean_text(base_meta.get("adapter")) or "custom",
            "fetchStrategy": clean_text(base_meta.get("fetchStrategy")) or "auto",
            "studio": clean_text(base_meta.get("studio")) or "",
            "fetchedCount": 0,
            "keptCount": 0,
            "lowConfidenceDropped": 0,
            "error": "",
            "durationMs": 0,
            "loss": {
                "rawFetched": 0,
                "canonicalDropped": 0,
                "canonicalKept": 0,
                "dedupMerged": 0,
                "finalOutput": 0,
                "canonicalDropReasons": {
                    "missing_title": 0,
                    "missing_company": 0,
                    "invalid_url": 0,
                    "invalid_payload": 0,
                },
                "scrapyRunnerRejectedValidation": 0,
                "scrapyParentInvalidPayload": 0,
                "staticNonJobUrlRejected": 0,
                "staticDuplicateLinkRejected": 0,
                "staticDetailParseEmpty": 0,
            },
        }
        canonical_batch: List[RawJob] = []
        try:
            thread_local.source_name = name
            raw_rows = loader(fetch_text=fetch_text_limited, timeout_s=timeout_s, retries=retries, backoff_s=backoff_s)
            report["fetchedCount"] = len(raw_rows)
            report_loss = report["loss"] if isinstance(report.get("loss"), dict) else {}
            report_loss["rawFetched"] = int(len(raw_rows))
            drop_reasons = Counter()
            kept = 0
            for raw in raw_rows:
                normalized, drop_reason = canonicalize_job_with_reason(raw, source=name, fetched_at=started_at)
                if normalized:
                    canonical_batch.append(normalized)
                    kept += 1
                elif drop_reason:
                    drop_reasons[drop_reason] += 1
            report["keptCount"] = kept
            report_loss["canonicalKept"] = int(kept)
            report_loss["canonicalDropped"] = max(0, int(len(raw_rows)) - int(kept))
            report_loss["canonicalDropReasons"] = {
                "missing_title": int(drop_reasons.get("missing_title", 0)),
                "missing_company": int(drop_reasons.get("missing_company", 0)),
                "invalid_url": int(drop_reasons.get("invalid_url", 0)),
                "invalid_payload": int(drop_reasons.get("invalid_payload", 0)),
            }
            current_fingerprint = source_rows_fingerprint(canonical_batch)
            previous_fingerprint = clean_text((source_state_rows.get(name) or {}).get("lastFingerprint"))
            report["sourceFingerprint"] = current_fingerprint
            report["fingerprintChanged"] = bool(current_fingerprint != previous_fingerprint)
            diag = SOURCE_DIAGNOSTICS.get(name) or {}
            if clean_text(diag.get("adapter")):
                report["adapter"] = clean_text(diag.get("adapter"))
            if clean_text(diag.get("studio")):
                report["studio"] = clean_text(diag.get("studio"))
            details = diag.get("details")
            if isinstance(details, list) and details:
                report["details"] = details
            detail_rows = details if isinstance(details, list) else []
            partial_errors = [clean_text(err) for err in (diag.get("partialErrors") or []) if clean_text(err)]
            if partial_errors:
                report["error"] = "; ".join(format_source_error(name, err) for err in partial_errors[:6])
            report["lowConfidenceDropped"] = int(diag.get("lowConfidenceDropped") or 0)
            if name == "scrapy_static_sources":
                runner_rejected = 0
                parent_invalid = 0
                for detail in detail_rows:
                    if not isinstance(detail, dict):
                        continue
                    stats = detail.get("stats") if isinstance(detail.get("stats"), dict) else {}
                    runner_rejected += int(stats.get("jobs_rejected_validation") or 0)
                    loss_detail = detail.get("loss") if isinstance(detail.get("loss"), dict) else {}
                    parent_invalid += int(loss_detail.get("scrapyParentInvalidPayload") or 0)
                report_loss["scrapyRunnerRejectedValidation"] = int(runner_rejected)
                report_loss["scrapyParentInvalidPayload"] = int(parent_invalid)
            if norm_text(report.get("adapter")) == "static":
                static_non_job = 0
                static_dup = 0
                static_empty = 0
                for detail in detail_rows:
                    if not isinstance(detail, dict):
                        continue
                    loss_detail = detail.get("loss") if isinstance(detail.get("loss"), dict) else {}
                    static_non_job += int(loss_detail.get("staticNonJobUrlRejected") or 0)
                    static_dup += int(loss_detail.get("staticDuplicateLinkRejected") or 0)
                    static_empty += int(loss_detail.get("staticDetailParseEmpty") or 0)
                report_loss["staticNonJobUrlRejected"] = int(static_non_job)
                report_loss["staticDuplicateLinkRejected"] = int(static_dup)
                report_loss["staticDetailParseEmpty"] = int(static_empty)
            report["loss"] = report_loss
        except Exception as exc:  # noqa: BLE001
            report["status"] = "error"
            report["error"] = format_source_error(name, exc)
        finally:
            thread_local.source_name = ""

        report["durationMs"] = int((time.perf_counter() - source_started) * 1000)
        return report, canonical_batch

    def mark_task_started(source_name: str) -> None:
        start_time = now_iso()
        with task_lock:
            task_rows[source_name]["status"] = "running"
            task_rows[source_name]["startedAt"] = start_time
            task_rows[source_name]["heartbeatAt"] = start_time
        write_task_state(force=True)
        if show_progress:
            print(f"[jobs_fetcher] START source={source_name}", flush=True)

    def mark_task_finished(source_name: str, report: Dict[str, Any]) -> None:
        end_time = now_iso()
        with task_lock:
            task_rows[source_name]["status"] = "ok" if report.get("status") == "ok" else "error"
            task_rows[source_name]["finishedAt"] = end_time
            task_rows[source_name]["durationMs"] = int(report.get("durationMs") or 0)
            task_rows[source_name]["heartbeatAt"] = end_time
            task_rows[source_name]["error"] = clean_text(report.get("error"))
        write_progress_report()
        write_task_state(force=True)
        if show_progress:
            print(
                f"[jobs_fetcher] DONE source={source_name} status={report['status']} "
                f"fetched={int(report.get('fetchedCount') or 0)} "
                f"kept={int(report.get('keptCount') or 0)} "
                f"durationMs={int(report.get('durationMs') or 0)}",
                flush=True,
            )

    def persist_source_result(source_name: str, report: Dict[str, Any], canonical_batch: List[RawJob]) -> None:
        canonical_rows.extend(canonical_batch)
        source_reports.append(report)
        mark_task_finished(source_name, report)

    def fallback_error_report(source_name: str, exc: Exception) -> Dict[str, Any]:
        return {
            "name": source_name,
            "status": "error",
            "adapter": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("adapter")) or "custom",
            "fetchStrategy": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("fetchStrategy")) or "auto",
            "studio": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("studio")) or "",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": format_source_error(source_name, exc),
            "durationMs": 0,
            "loss": {
                "rawFetched": 0,
                "canonicalDropped": 0,
                "canonicalKept": 0,
                "dedupMerged": 0,
                "finalOutput": 0,
                "canonicalDropReasons": {
                    "missing_title": 0,
                    "missing_company": 0,
                    "invalid_url": 0,
                    "invalid_payload": 0,
                },
            },
        }

    def run_source_execution_stage() -> None:
        if max_workers <= 1 or len(selected_loaders) <= 1:
            for source_name, loader in selected_loaders:
                mark_task_started(source_name)
                report, canonical_batch = execute_loader(source_name, loader)
                persist_source_result(source_name, report, canonical_batch)
            return

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for source_name, loader in selected_loaders:
                mark_task_started(source_name)
                futures[executor.submit(execute_loader, source_name, loader)] = source_name
            for future in as_completed(futures):
                source_name = futures[future]
                try:
                    report, canonical_batch = future.result()
                except Exception as exc:  # noqa: BLE001
                    report = fallback_error_report(source_name, exc)
                    canonical_batch = []
                persist_source_result(source_name, report, canonical_batch)

    write_progress_report()
    write_task_state(force=True)
    try:
        run_source_execution_stage()
    finally:
        if async_fetcher is not None:
            async_fetcher.close()

    if using_default_loaders:
        append_excluded_default_sources(source_reports)

    deduped_rows, dedup_stats = deduplicate_jobs(canonical_rows)

    preserved_previous = False
    if preserve_previous_on_empty and not deduped_rows:
        previous_rows = read_existing_output_from_file(
            json_path,
            started_at,
            canonicalize_job=canonicalize_job,
            clean_text=clean_text,
        )
        if previous_rows:
            deduped_rows = previous_rows
            preserved_previous = True

    selected_loader_names = {name for name, _ in selected_loaders}
    selected_reports = [row for row in source_reports if clean_text(row.get("name")) in selected_loader_names]
    run_is_healthy = all(norm_text(row.get("status")) == "ok" for row in selected_reports) if selected_reports else False
    allow_mark_missing = bool(using_default_loaders and not seed_from_existing_output and run_is_healthy)
    lifecycle_finished_at = now_iso()
    deduped_rows, lifecycle_rows, lifecycle_counts_map = apply_job_lifecycle_state(
        deduped_rows=deduped_rows,
        lifecycle_rows=lifecycle_rows,
        finished_at=lifecycle_finished_at,
        allow_mark_missing=allow_mark_missing,
    )

    dedup_stats["outputCount"] = len(deduped_rows)
    final_output_by_source: Counter[str] = Counter(
        clean_text(row.get("source")) for row in deduped_rows if clean_text(row.get("source"))
    )
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        loss = report.get("loss")
        if not isinstance(loss, dict):
            continue
        source_name = clean_text(report.get("name"))
        canonical_kept = int(loss.get("canonicalKept") or report.get("keptCount") or 0)
        final_output = int(final_output_by_source.get(source_name, 0))
        loss["finalOutput"] = max(0, final_output)
        loss["dedupMerged"] = max(0, canonical_kept - final_output)

    wrote_json = False
    wrote_csv = False
    wrote_light_json = False
    if deduped_rows:
        wrote_json = write_text_if_changed(json_path, serialize_rows_for_json(deduped_rows, OUTPUT_FIELDS))
        wrote_csv = write_text_if_changed(csv_path, serialize_rows_for_csv(deduped_rows, OUTPUT_FIELDS))
        wrote_light_json = write_text_if_changed(
            light_json_path,
            serialize_rows_for_json(deduped_rows, LIGHTWEIGHT_OUTPUT_FIELDS),
        )

    json_bytes = json_path.stat().st_size if json_path.exists() else 0
    csv_bytes = csv_path.stat().st_size if csv_path.exists() else 0
    light_json_bytes = light_json_path.stat().st_size if light_json_path.exists() else 0
    browser_fallback_queue_rows = build_browser_fallback_queue(source_reports, generated_at=lifecycle_finished_at)
    write_text_if_changed(browser_fallback_queue_path, json.dumps(browser_fallback_queue_rows, indent=2, ensure_ascii=False))

    report_payload = normalize_fetch_report_payload({
        "schemaVersion": SCHEMA_VERSION,
        "startedAt": started_at,
        "finishedAt": lifecycle_finished_at,
        "runtime": runtime_payload,
        "summary": build_pipeline_summary(
            dedup_stats,
            deduped_rows,
            source_reports,
            len(canonical_rows),
            preserved_previous,
            len([row for row in STUDIO_SOURCE_REGISTRY if bool(row.get("enabledByDefault", True))]),
            len(load_registry_from_file(pending_registry_path, [])),
            read_approved_since_last_run(approval_state_path),
            json_bytes=json_bytes,
            csv_bytes=csv_bytes,
            light_json_bytes=light_json_bytes,
            lifecycle_counts_map=lifecycle_counts_map,
        ),
        "sources": source_reports,
        "outputs": {
            "json": str(json_path),
            "csv": str(csv_path),
            "lightJson": str(light_json_path),
            "report": str(report_path),
            "lifecycleState": str(lifecycle_state_path),
            "browserFallbackQueue": str(browser_fallback_queue_path),
            "changed": {"json": wrote_json, "csv": wrote_csv, "lightJson": wrote_light_json},
        },
    })
    write_text_if_changed(report_path, json.dumps(report_payload, indent=2, ensure_ascii=False))
    finished_at = clean_text(report_payload.get("finishedAt")) or now_iso()
    write_task_state(finished_at=finished_at, force=True)
    write_success_cache(success_cache_path, source_reports)

    source_state_rows = update_source_state_rows(
        source_state_rows=source_state_rows,
        source_reports=source_reports,
        canonical_rows=canonical_rows,
        finished_at=finished_at,
        circuit_breaker_failures=circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
    )
    write_source_state(source_state_path, source_state_rows)
    write_job_lifecycle_state(lifecycle_state_path, lifecycle_rows)
    return report_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and merge game jobs into unified output feeds.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory to write output files.")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S, help="Per-source request timeout in seconds.")
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help="Retry count per request.")
    parser.add_argument("--backoff", type=float, default=DEFAULT_BACKOFF_S, help="Base retry backoff in seconds.")
    parser.add_argument(
        "--no-preserve-previous-on-empty",
        action="store_true",
        help="Do not preserve previous output if current run yields no jobs.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-source progress logs.",
    )
    parser.add_argument(
        "--skip-successful-sources",
        action="store_true",
        help="Skip sources that were previously successful with non-zero kept jobs in the last report.",
    )
    parser.add_argument(
        "--only-sources",
        default="",
        help="Comma-separated source loader names to run (for targeted/incremental fetches).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=6,
        help="Max concurrent source workers. Use >1 to run source loaders in parallel.",
    )
    parser.add_argument(
        "--max-per-domain",
        type=int,
        default=2,
        help="Max concurrent in-flight requests allowed per domain across workers.",
    )
    parser.add_argument(
        "--fetch-strategy",
        choices=("auto", "http", "browser"),
        default=DEFAULT_FETCH_STRATEGY,
        help="Fetch transport strategy. 'http' prefers async httpx, 'auto' falls back safely, 'browser' keeps HTTP mode in this runtime.",
    )
    parser.add_argument(
        "--adapter-http-concurrency",
        type=int,
        default=DEFAULT_ADAPTER_HTTP_CONCURRENCY,
        help="Connection pool size used by async HTTP fetch transport.",
    )
    parser.add_argument(
        "--source-ttl-minutes",
        type=int,
        default=360,
        help="Freshness window for --skip-successful-sources. Recently successful sources are skipped until TTL expires.",
    )
    parser.add_argument(
        "--respect-source-cadence",
        action="store_true",
        help="Apply source-level hot/cold cadence skipping using source state history.",
    )
    parser.add_argument(
        "--hot-source-cadence-minutes",
        type=int,
        default=DEFAULT_HOT_SOURCE_CADENCE_MINUTES,
        help="Cadence for recently changed sources when --respect-source-cadence is enabled.",
    )
    parser.add_argument(
        "--cold-source-cadence-minutes",
        type=int,
        default=DEFAULT_COLD_SOURCE_CADENCE_MINUTES,
        help="Cadence for stable sources when --respect-source-cadence is enabled.",
    )
    parser.add_argument(
        "--circuit-breaker-failures",
        type=int,
        default=3,
        help="Consecutive failures required before a source is temporarily quarantined.",
    )
    parser.add_argument(
        "--circuit-breaker-cooldown-minutes",
        type=int,
        default=180,
        help="Minutes to quarantine a source after it trips the circuit breaker.",
    )
    parser.add_argument(
        "--ignore-circuit-breaker",
        action="store_true",
        help="Force execution of sources even if currently quarantined.",
    )
    parser.add_argument(
        "--social-enabled",
        action="store_true",
        help="Enable social sources (Reddit, X, Mastodon) in the fetch run.",
    )
    parser.add_argument(
        "--social-config-path",
        default=str(DEFAULT_SOCIAL_CONFIG_PATH),
        help="Path to social source config JSON file.",
    )
    parser.add_argument(
        "--social-lookback-minutes",
        type=int,
        default=DEFAULT_SOCIAL_LOOKBACK_MINUTES,
        help="Lookback window for social source polling.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_loaders: Optional[List[Tuple[str, SourceLoader]]] = None
    seed_from_existing_output = False
    selection_exclusions: List[Dict[str, Any]] = []
    social_config = load_social_config(
        config_path=Path(args.social_config_path),
        enabled=bool(args.social_enabled),
        lookback_minutes=int(args.social_lookback_minutes or DEFAULT_SOCIAL_LOOKBACK_MINUTES),
    )
    try:
        default_loaders = default_source_loaders(
            social_enabled=bool(args.social_enabled),
            social_config=social_config,
        )
    except TypeError:
        default_loaders = default_source_loaders()

    only_sources = [clean_text(part) for part in str(args.only_sources or "").split(",") if clean_text(part)]
    if only_sources:
        wanted = set(only_sources)
        source_loaders = [(name, loader) for name, loader in default_loaders if name in wanted]
        seed_from_existing_output = True
        for name, _loader in default_loaders:
            if name in wanted:
                continue
            selection_exclusions.append(_build_excluded_source_report(name, "only_sources_filter"))
        missing = [name for name in only_sources if name not in {item[0] for item in source_loaders}]
        if missing:
            print(f"[jobs_fetcher] WARN unknown --only-sources entries: {', '.join(missing)}", flush=True)

    if args.skip_successful_sources:
        selected = source_loaders if source_loaders is not None else list(default_loaders)
        source_state_path = Path(args.output_dir) / "jobs-source-state.json"
        state_rows = read_source_state(source_state_path)
        successful = {
            name
            for name, _ in selected
            if should_skip_source_by_ttl(name, state_rows, int(args.source_ttl_minutes or 0))
        }
        if not successful:
            previous_report = Path(args.output_dir) / "jobs-fetch-report.json"
            success_cache_path = Path(args.output_dir) / "jobs-success-cache.json"
            successful = read_success_cache(success_cache_path)
            if not successful:
                successful = read_previously_successful_sources(previous_report)
        if successful:
            selected = [(name, loader) for name, loader in selected if name not in successful]
            for source_name in sorted(successful):
                selection_exclusions.append(_build_excluded_source_report(source_name, "skip_successful_ttl"))
        source_loaders = selected
        seed_from_existing_output = True
        if not args.quiet:
            print(
                f"[jobs_fetcher] Incremental mode: skipping {len(successful)} previously successful sources; running {len(selected)}",
                flush=True,
            )

    forced_only_sources = bool(only_sources)
    deduped_selection_exclusions: List[Dict[str, Any]] = []
    seen_selection_exclusions = set()
    for row in selection_exclusions:
        name = clean_text(row.get("name"))
        reason = clean_text(row.get("exclusionReason") or row.get("error"))
        token = f"{name}|{reason}"
        if not name or token in seen_selection_exclusions:
            continue
        seen_selection_exclusions.add(token)
        deduped_selection_exclusions.append(row)
    report = run_pipeline(
        output_dir=Path(args.output_dir),
        timeout_s=args.timeout,
        retries=args.retries,
        backoff_s=args.backoff,
        preserve_previous_on_empty=not args.no_preserve_previous_on_empty,
        source_loaders=source_loaders,
        seed_from_existing_output=seed_from_existing_output,
        source_ttl_minutes=args.source_ttl_minutes,
        max_workers=args.max_workers,
        max_per_domain=args.max_per_domain,
        fetch_strategy=args.fetch_strategy,
        adapter_http_concurrency=args.adapter_http_concurrency,
        circuit_breaker_failures=args.circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=args.circuit_breaker_cooldown_minutes,
        respect_source_cadence=bool(args.respect_source_cadence),
        hot_source_cadence_minutes=args.hot_source_cadence_minutes,
        cold_source_cadence_minutes=args.cold_source_cadence_minutes,
        ignore_circuit_breaker=bool(args.ignore_circuit_breaker or forced_only_sources),
        social_enabled=bool(args.social_enabled),
        social_config_path=Path(args.social_config_path),
        social_lookback_minutes=int(args.social_lookback_minutes or DEFAULT_SOCIAL_LOOKBACK_MINUTES),
        show_progress=not args.quiet,
        selection_exclusions=deduped_selection_exclusions,
    )
    summary = report.get("summary", {})
    output_count = int(summary.get("outputCount") or 0)
    failed_sources = int(summary.get("failedSources") or 0)
    print(
        f"Jobs fetch completed. Output jobs: {output_count}. "
        f"Failed sources: {failed_sources}. Report: {report['outputs']['report']}"
    )
    return 0 if output_count > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
