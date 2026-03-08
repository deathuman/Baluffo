#!/usr/bin/env python3
"""Aggregate game job listings into unified JSON/CSV feeds."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import hashlib
import json
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

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.contracts import SCHEMA_VERSION
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
REMOTE_OK_URL = "https://remoteok.com/api"
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
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data"
SOURCE_REGISTRY_ACTIVE_PATH = DEFAULT_OUTPUT_DIR / "source-registry-active.json"
SOURCE_REGISTRY_PENDING_PATH = DEFAULT_OUTPUT_DIR / "source-registry-pending.json"
SOURCE_APPROVAL_STATE_PATH = DEFAULT_OUTPUT_DIR / "source-approval-state.json"


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
        rows.append(row)
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
    text = fetch_with_retries(REMOTE_OK_URL, fetch_text, timeout_s, retries, backoff_s)
    return parse_remote_ok_payload(json.loads(text))


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

    def is_probable_job_detail_url(candidate_url: str) -> bool:
        parsed = urlparse(candidate_url)
        path = parsed.path.lower()
        query = parsed.query.lower()
        if (
            "/job/" in path
            or "/jobs/" in path
            or "/jobdetail/" in path
            or bool(re.search(r"/en/j/\d+", path))
        ):
            return True
        if "job_id=" in query:
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
        }
        kept_before = len(jobs)

        for page in pages:
            page_url = clean_text(page)
            if not page_url:
                continue
            try:
                html = fetch_with_retries(page_url, fetch_text, timeout_s, retries, backoff_s)
                parsed = parse_jobpostings_from_html(
                    html,
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

                detail_links: List[Tuple[str, str]] = []
                detail_seen = set()
                for match in re.finditer(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html):
                    href = clean_text(match.group(1))
                    anchor_inner = match.group(2) or ""
                    anchor_text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", anchor_inner))
                    absolute = urljoin(page_url, clean_text(href))
                    if not is_probable_job_detail_url(absolute):
                        continue
                    if absolute in detail_seen:
                        continue
                    detail_seen.add(absolute)
                    detail_links.append((absolute, anchor_text))
                # Some career sites embed job links in JSON payloads instead of anchor hrefs.
                for raw in re.findall(r'https?://[^\s"\'<>]+', html, flags=re.I):
                    absolute = clean_text(raw)
                    if not is_probable_job_detail_url(absolute):
                        continue
                    if absolute in detail_seen:
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


def canonicalize_job(raw: RawJob, *, source: str, fetched_at: str) -> Optional[RawJob]:
    title = clean_text(raw.get("title"))
    company = clean_text(raw.get("company"))
    if not title or not company:
        return None

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
        "jobLink": normalize_url(raw.get("jobLink")),
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
    merges = 0

    for row in rows:
        primary = fingerprint_url(row.get("jobLink"))
        secondary = dedup_secondary_key(row)

        target_idx: Optional[int] = None
        if primary and primary in by_primary:
            target_idx = by_primary[primary]
        elif secondary and secondary in by_secondary:
            target_idx = by_secondary[secondary]

        if target_idx is None:
            item = dict(row)
            item["dedupKey"] = f"url:{primary}" if primary else f"secondary:{hashlib.sha1(secondary.encode('utf-8')).hexdigest()}"
            item["qualityScore"] = compute_quality_score(item)
            item["focusScore"] = compute_focus_score(item)
            merged_rows.append(item)
            idx = len(merged_rows) - 1
            if primary:
                by_primary[primary] = idx
            if secondary:
                by_secondary[secondary] = idx
            continue

        merges += 1
        merged = merge_records(merged_rows[target_idx], row)
        primary = fingerprint_url(merged.get("jobLink"))
        secondary = dedup_secondary_key(merged)
        merged["dedupKey"] = f"url:{primary}" if primary else f"secondary:{hashlib.sha1(secondary.encode('utf-8')).hexdigest()}"
        merged_rows[target_idx] = merged
        if primary:
            by_primary[primary] = target_idx
        if secondary:
            by_secondary[secondary] = target_idx

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
    return merged_rows, {"inputCount": len(rows), "mergedCount": merges, "outputCount": len(merged_rows)}


def default_source_loaders() -> List[Tuple[str, SourceLoader]]:
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
    return {
        **dedup_stats,
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
        "seedFromExistingOutput": bool(src.get("seedFromExistingOutput")),
        "sourceTtlMinutes": _clamped_int(src.get("sourceTtlMinutes"), 0, 0),
        "circuitBreakerFailures": _clamped_int(src.get("circuitBreakerFailures"), 0, 0),
        "circuitBreakerCooldownMinutes": _clamped_int(src.get("circuitBreakerCooldownMinutes"), 0, 0),
        "ignoreCircuitBreaker": bool(src.get("ignoreCircuitBreaker")),
        "selectedSourceCount": _clamped_int(src.get("selectedSourceCount"), selected_source_count, 0),
    }


def normalize_source_report_row(row: Dict[str, Any]) -> Dict[str, Any]:
    src = row if isinstance(row, dict) else {}
    normalized = {
        "name": clean_text(src.get("name")),
        "status": norm_text(src.get("status")) or "error",
        "adapter": clean_text(src.get("adapter")) or "custom",
        "studio": clean_text(src.get("studio")),
        "fetchedCount": _clamped_int(src.get("fetchedCount"), 0, 0),
        "keptCount": _clamped_int(src.get("keptCount"), 0, 0),
        "error": clean_text(src.get("error")),
        "durationMs": _clamped_int(src.get("durationMs"), 0, 0),
    }
    details = src.get("details")
    if isinstance(details, list):
        clean_details = [clean_text(item) for item in details if clean_text(item)]
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
        "studio": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("studio")) or "",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": clean_text(reason),
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
            if entry["lastKeptCount"] > 0:
                entry["lastFingerprint"] = source_rows_fingerprint(
                    [row for row in canonical_rows if clean_text(row.get("source")) == name]
                )
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
    circuit_breaker_failures: int = 3,
    circuit_breaker_cooldown_minutes: int = 180,
    ignore_circuit_breaker: bool = False,
    show_progress: bool = True,
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
    task_state_path = output_dir / "jobs-fetch-tasks.json"
    pending_registry_path = output_dir / "source-registry-pending.json"
    approval_state_path = output_dir / "source-approval-state.json"
    SOURCE_DIAGNOSTICS.clear()

    started_at = now_iso()
    source_reports: List[Dict[str, Any]] = []
    canonical_rows: List[RawJob] = []
    max_workers = max(1, int(max_workers or 1))
    max_per_domain = max(1, int(max_per_domain or 1))
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

    selected_loaders = default_source_loaders() if source_loaders is None else list(source_loaders)
    using_default_loaders = source_loaders is None
    runtime_payload = normalize_runtime_payload({
        "maxWorkers": max_workers,
        "maxPerDomain": max_per_domain,
        "seedFromExistingOutput": bool(seed_from_existing_output),
        "sourceTtlMinutes": int(source_ttl_minutes or 0),
        "circuitBreakerFailures": int(circuit_breaker_failures or 0),
        "circuitBreakerCooldownMinutes": int(circuit_breaker_cooldown_minutes or 0),
        "ignoreCircuitBreaker": bool(ignore_circuit_breaker),
        "selectedSourceCount": len(selected_loaders),
    }, selected_source_count=len(selected_loaders))

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
            return fetch_text(url, timeout)
        finally:
            gate.release()

    def execute_loader(name: str, loader: SourceLoader) -> Tuple[Dict[str, Any], List[RawJob]]:
        source_started = time.perf_counter()
        base_meta = SOURCE_REPORT_META.get(name, {})
        report: Dict[str, Any] = {
            "name": name,
            "status": "ok",
            "adapter": clean_text(base_meta.get("adapter")) or "custom",
            "studio": clean_text(base_meta.get("studio")) or "",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
            "durationMs": 0,
        }
        canonical_batch: List[RawJob] = []
        try:
            thread_local.source_name = name
            raw_rows = loader(fetch_text=fetch_text_limited, timeout_s=timeout_s, retries=retries, backoff_s=backoff_s)
            report["fetchedCount"] = len(raw_rows)
            kept = 0
            for raw in raw_rows:
                normalized = canonicalize_job(raw, source=name, fetched_at=started_at)
                if normalized:
                    canonical_batch.append(normalized)
                    kept += 1
            report["keptCount"] = kept
            diag = SOURCE_DIAGNOSTICS.get(name) or {}
            if clean_text(diag.get("adapter")):
                report["adapter"] = clean_text(diag.get("adapter"))
            if clean_text(diag.get("studio")):
                report["studio"] = clean_text(diag.get("studio"))
            details = diag.get("details")
            if isinstance(details, list) and details:
                report["details"] = details
            partial_errors = [clean_text(err) for err in (diag.get("partialErrors") or []) if clean_text(err)]
            if partial_errors:
                report["error"] = "; ".join(format_source_error(name, err) for err in partial_errors[:6])
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
            "studio": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("studio")) or "",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": format_source_error(source_name, exc),
            "durationMs": 0,
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
    run_source_execution_stage()

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
        "--source-ttl-minutes",
        type=int,
        default=360,
        help="Freshness window for --skip-successful-sources. Recently successful sources are skipped until TTL expires.",
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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_loaders: Optional[List[Tuple[str, SourceLoader]]] = None
    seed_from_existing_output = False
    default_loaders = default_source_loaders()

    only_sources = [clean_text(part) for part in str(args.only_sources or "").split(",") if clean_text(part)]
    if only_sources:
        wanted = set(only_sources)
        source_loaders = [(name, loader) for name, loader in default_loaders if name in wanted]
        seed_from_existing_output = True
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
        source_loaders = selected
        seed_from_existing_output = True
        if not args.quiet:
            print(
                f"[jobs_fetcher] Incremental mode: skipping {len(successful)} previously successful sources; running {len(selected)}",
                flush=True,
            )

    forced_only_sources = bool(only_sources)
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
        circuit_breaker_failures=args.circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=args.circuit_breaker_cooldown_minutes,
        ignore_circuit_breaker=bool(args.ignore_circuit_breaker or forced_only_sources),
        show_progress=not args.quiet,
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
