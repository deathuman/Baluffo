#!/usr/bin/env python3
"""Discover candidate game-dev job sources and queue them for approval."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.source_registry import (
    ACTIVE_PATH,
    DATA_DIR,
    DISCOVERY_CANDIDATES_PATH,
    DISCOVERY_REPORT_PATH,
    PENDING_PATH,
    REJECTED_PATH,
    load_json_array,
    save_json_atomic,
    source_identity,
    unique_sources,
)
from scripts.contracts import SCHEMA_VERSION

SEED_CATALOG_PATH = ROOT / "scripts" / "discovery_seed_catalog.json"
DISCOVERY_STAGES = ("curated_seed", "provider_pattern", "web_provider", "generic_static")
SUPPORTED_PROVIDERS = ("greenhouse", "lever", "smartrecruiters", "workable", "teamtailor", "ashby", "personio")
DISCOVERY_CONFIG_PATH = DATA_DIR / "source-discovery-config.json"
CAREERS_URL_HINTS = ("careers", "career", "jobs", "join-us", "open-positions", "vacancies", "work-with-us")
GENERIC_STATIC_BLOCKED_DOMAINS = (
    "linkedin.com",
    "indeed.com",
    "glassdoor.com",
    "ziprecruiter.com",
    "monster.com",
    "welcome to the jungle.com",
    "welcometothejungle.com",
)
FOCUS_KEYWORDS = ("technical artist", "tech artist", "environment artist", "environment art", "world artist", "terrain artist")
DUCKDUCKGO_HTML_SEARCH = "https://duckduckgo.com/html/?q={query}"
WEB_SEARCH_QUERY_SUFFIX = ("careers", "jobs")
FETCH_MAX_RETRIES = 2
RETRY_BACKOFF_SECONDS = 1.2
RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}
MAX_SEARCH_LINKS_PER_QUERY = 8
MIN_PROVIDER_EVIDENCE_TO_PROBE = 18
MIN_STATIC_EVIDENCE_TO_PROBE = 22
MIN_PROVIDER_EVIDENCE_TO_QUEUE = 26
MIN_STATIC_EVIDENCE_TO_QUEUE = 34
LOW_EVIDENCE_PROBE_LIMIT = 12
PATTERN_PROVIDER_PROBE_THRESHOLD = 30
PATTERN_PROVIDER_QUEUE_THRESHOLD = 40
DOMAIN_QUEUE_CAP_DEFAULT = 2
ADAPTER_QUEUE_CAPS = {
    "greenhouse": 4,
    "lever": 4,
    "smartrecruiters": 4,
    "workable": 4,
    "teamtailor": 4,
    "ashby": 4,
    "personio": 3,
    "static": 6,
}

DISCOVERY_LOG_PATH = str(os.getenv("BALUFFO_DISCOVERY_LOG_PATH") or "").strip()

STATIC_DISCOVERY_CANDIDATES: List[Dict[str, Any]] = [
    {"name": "Sandbox VR (Lever)", "studio": "Sandbox VR", "adapter": "lever", "account": "sandboxvr", "api_url": "https://api.lever.co/v0/postings/sandboxvr?mode=json", "remoteFriendly": True, "nlPriority": False},
    {"name": "Voodoo (Lever)", "studio": "Voodoo", "adapter": "lever", "account": "voodoo", "api_url": "https://api.lever.co/v0/postings/voodoo?mode=json", "remoteFriendly": True, "nlPriority": False},
    {"name": "CD PROJEKT RED (SmartRecruiters)", "studio": "CD PROJEKT RED", "adapter": "smartrecruiters", "company_id": "CDPROJEKTRED", "api_url": "https://api.smartrecruiters.com/v1/companies/CDPROJEKTRED/postings", "remoteFriendly": True, "nlPriority": False},
    {"name": "Gameloft (SmartRecruiters)", "studio": "Gameloft", "adapter": "smartrecruiters", "company_id": "Gameloft", "api_url": "https://api.smartrecruiters.com/v1/companies/Gameloft/postings", "remoteFriendly": True, "nlPriority": False},
    {"name": "Hutch (Workable)", "studio": "Hutch", "adapter": "workable", "account": "hutch", "api_url": "https://apply.workable.com/api/v1/widget/accounts/hutch?details=true", "remoteFriendly": True, "nlPriority": False},
    {"name": "Wargaming (Workable)", "studio": "Wargaming", "adapter": "workable", "account": "wargaming", "api_url": "https://apply.workable.com/api/v1/widget/accounts/wargaming?details=true", "remoteFriendly": True, "nlPriority": False},
    {"name": "InnoGames (Personio)", "studio": "InnoGames", "adapter": "personio", "feed_url": "https://innogames.jobs.personio.de/xml", "remoteFriendly": True, "nlPriority": True},
    {"name": "Travian (Personio)", "studio": "Travian", "adapter": "personio", "feed_url": "https://travian.jobs.personio.de/xml", "remoteFriendly": True, "nlPriority": True},
    {"name": "Jagex (Ashby)", "studio": "Jagex", "adapter": "ashby", "board_url": "https://jobs.ashbyhq.com/jagex/jobs", "remoteFriendly": True, "nlPriority": False},
    {"name": "Scopely (Ashby)", "studio": "Scopely", "adapter": "ashby", "board_url": "https://jobs.ashbyhq.com/scopely/jobs", "remoteFriendly": True, "nlPriority": False},
]

DEFAULT_STUDIO_SEEDS: List[Dict[str, Any]] = [
    {"studio": "Guerrilla Games", "aliases": ["guerrilla-games", "guerrillagames"], "nlPriority": True, "remoteFriendly": True, "likelyProviders": ["greenhouse"], "careersUrl": "https://www.guerrilla-games.com/join"},
    {"studio": "Nixxes", "aliases": ["nixxes"], "nlPriority": True, "remoteFriendly": True, "likelyProviders": ["static"], "careersUrl": "https://www.nixxes.com/careers"},
    {"studio": "Vertigo Games", "aliases": ["vertigo-games", "vertigogames"], "nlPriority": True, "remoteFriendly": True, "likelyProviders": ["workable", "smartrecruiters"], "careersUrl": "https://vertigo-games.com/careers"},
    {"studio": "Triumph Studios", "aliases": ["triumph-studios", "triumphstudios"], "nlPriority": True, "remoteFriendly": True, "likelyProviders": ["static"], "careersUrl": "https://www.triumphstudios.com/careers"},
    {"studio": "Little Chicken", "aliases": ["littlechicken", "little-chicken"], "nlPriority": True, "remoteFriendly": True, "likelyProviders": ["static"], "careersUrl": "https://www.littlechicken.nl/about-us/jobs/"},
]

DEFAULT_DISCOVERY_CONFIG: Dict[str, Any] = {
    "gamesmap": {
        "enabled": False,
        "baseUrl": "https://www.gamesmap.de",
        "indexUrls": [
            "https://www.gamesmap.de/en",
        ],
        "preferEnglish": True,
        "websiteOnlyFallback": False,
        "maxDetailPages": 60,
        "allowedCategoryTokens": [
            "developer",
            "publisher",
            "developer and publisher",
            "pc",
            "console",
            "mobile",
            "browser",
            "online",
            "vr",
            "ar",
            "serious games",
        ],
        "blockedCategoryTokens": [
            "association",
            "university",
            "education",
            "public institution",
            "government",
            "service provider",
        ],
    }
}


def load_studio_seeds() -> List[Dict[str, Any]]:
    try:
        payload = json.loads(SEED_CATALOG_PATH.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)] or list(DEFAULT_STUDIO_SEEDS)
    except (OSError, json.JSONDecodeError):
        pass
    return list(DEFAULT_STUDIO_SEEDS)


STUDIO_SEEDS: List[Dict[str, Any]] = load_studio_seeds()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_log(message: str) -> None:
    line = f"[{now_iso()}] {str(message or '').strip()}"
    print(line, flush=True)


def clean_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def load_discovery_config() -> Dict[str, Any]:
    payload = dict(DEFAULT_DISCOVERY_CONFIG)
    try:
        raw = json.loads(DISCOVERY_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raw = {}
    if not isinstance(raw, dict):
        return payload
    gamesmap = raw.get("gamesmap")
    if isinstance(gamesmap, dict):
        merged = dict(payload.get("gamesmap") or {})
        merged.update(gamesmap)
        payload["gamesmap"] = merged
    return payload


def to_slug(value: str) -> str:
    return re.sub(r"-{2,}", "-", re.sub(r"[^a-z0-9]+", "-", value.lower())).strip("-")


def endpoint_url(candidate: Dict[str, Any]) -> str:
    for key in ("api_url", "feed_url", "board_url", "listing_url"):
        raw = str(candidate.get(key) or "").strip()
        if raw:
            return raw
    return ""


def adapter_domain_fingerprint(candidate: Dict[str, Any]) -> str:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    url = endpoint_url(candidate)
    if not adapter or not url:
        return ""
    try:
        parsed = urlparse(url)
        domain = (parsed.netloc or "").lower().strip()
        path = (parsed.path or "").rstrip("/").lower()
    except ValueError:
        domain = ""
        path = ""
    if not domain:
        return ""
    return f"{adapter}:{domain}:{path}"


def careers_keyword_count(text: str) -> int:
    lowered = str(text or "").lower()
    return sum(1 for token in CAREERS_URL_HINTS if token in lowered)


def root_domain(host: str) -> str:
    token = str(host or "").strip().lower()
    if not token:
        return ""
    parts = [part for part in token.split(".") if part]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return token


def queue_family_key(candidate: Dict[str, Any]) -> str:
    url = endpoint_url(candidate) or str(candidate.get("careersUrl") or "")
    try:
        host = (urlparse(url).netloc or "").lower()
    except ValueError:
        host = ""
    adapter = str(candidate.get("adapter") or "").strip().lower()
    studio = clean_token(str(candidate.get("studio") or candidate.get("name") or ""))
    domain_key = root_domain(host) or studio or "unknown"
    return f"{adapter}:{domain_key}"


def is_blocked_generic_static_url(url: str) -> bool:
    try:
        host = (urlparse(str(url or "")).netloc or "").lower()
    except ValueError:
        return False
    host = host.lstrip(".")
    return any(host == domain or host.endswith(f".{domain}") for domain in GENERIC_STATIC_BLOCKED_DOMAINS)


def fetch_text(url: str, timeout_s: int) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36 BaluffoSourceDiscovery/2.1"
            ),
            "Accept": "application/json,text/html,text/xml,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
        },
    )
    with urlopen(req, timeout=timeout_s) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def _http_code_from_error(exc: Exception) -> Optional[int]:
    if isinstance(exc, HTTPError):
        return int(exc.code)
    match = re.search(r"\bHTTP Error (\d{3})\b", str(exc))
    return int(match.group(1)) if match else None


def _is_retryable_error(exc: Exception) -> bool:
    code = _http_code_from_error(exc)
    if code in RETRYABLE_HTTP_CODES:
        return True
    message = str(exc).lower()
    return "timed out" in message or "temporary failure" in message


def fetch_text_with_retry(url: str, timeout_s: int, *, adapter: str, fetcher=fetch_text) -> str:
    if adapter in {"workable", "personio", "ashby"}:
        time.sleep(0.18)
    attempts = FETCH_MAX_RETRIES + 1
    last_exc: Optional[Exception] = None
    for attempt in range(attempts):
        try:
            return fetcher(url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= FETCH_MAX_RETRIES or not _is_retryable_error(exc):
                break
            time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
    if last_exc:
        raise last_exc
    raise RuntimeError("fetch failed without an explicit error")


def _is_valid_identity_token(token: str) -> bool:
    value = str(token or "").strip()
    return bool(len(value) >= 3 and re.match(r"^[A-Za-z0-9][A-Za-z0-9_-]*$", value) and not value.isdigit())


def validate_candidate_for_probe(candidate: Dict[str, Any]) -> Tuple[bool, str]:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    if adapter in {"lever", "workable"}:
        token = str(candidate.get("account") or "").strip()
        return (_is_valid_identity_token(token), "" if _is_valid_identity_token(token) else "invalid account token")
    if adapter == "greenhouse":
        slug = str(candidate.get("slug") or "").strip()
        return (_is_valid_identity_token(slug), "" if _is_valid_identity_token(slug) else "invalid board slug")
    if adapter == "smartrecruiters":
        company_id = str(candidate.get("company_id") or "").strip()
        valid = len(company_id) >= 3 and bool(re.search(r"[A-Za-z]", company_id))
        return (valid, "" if valid else "invalid company identifier")
    if adapter == "personio":
        host = (urlparse(str(candidate.get("feed_url") or "")).netloc or "").lower()
        return (".jobs.personio.de" in host, "" if ".jobs.personio.de" in host else "invalid personio host")
    if adapter == "teamtailor":
        parsed = urlparse(str(candidate.get("listing_url") or "").strip())
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        valid = ".teamtailor.com" in host or path.startswith("/jobs")
        return (valid, "" if valid else "invalid teamtailor host")
    if adapter == "ashby":
        host = (urlparse(str(candidate.get("board_url") or "").strip()).netloc or "").lower()
        return ("ashbyhq.com" in host, "" if "ashbyhq.com" in host else "invalid ashby host")
    if adapter == "static":
        listing = str(candidate.get("listing_url") or "").strip()
        pages = candidate.get("pages")
        valid = bool(listing) or bool(isinstance(pages, list) and any(str(item or "").strip() for item in pages))
        return (valid, "" if valid else "invalid static source")
    return True, ""


def fallback_probe_urls(candidate: Dict[str, Any]) -> List[str]:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    urls: List[str] = []
    if adapter == "greenhouse":
        slug = str(candidate.get("slug") or "").strip()
        if slug:
            urls.append(f"https://boards.greenhouse.io/{slug}")
    elif adapter == "lever":
        account = str(candidate.get("account") or "").strip()
        if account:
            urls.append(f"https://jobs.lever.co/{account}")
    elif adapter == "smartrecruiters":
        company_id = str(candidate.get("company_id") or "").strip()
        if company_id:
            urls.append(f"https://jobs.smartrecruiters.com/{company_id}")
    elif adapter == "workable":
        account = str(candidate.get("account") or "").strip()
        if account:
            urls.append(f"https://apply.workable.com/{account}")
    elif adapter == "personio":
        host = (urlparse(str(candidate.get("feed_url") or "")).netloc or "").strip()
        if host:
            urls.append(f"https://{host}/")
    return urls


def extract_jobish_links(html: str, base_url: str) -> List[str]:
    matches = re.findall(r'(?is)href=["\']([^"\']+)["\']', str(html or ""))
    out: List[str] = []
    seen = set()
    for raw in matches:
        if not raw or raw.startswith("#") or raw.startswith("mailto:") or raw.startswith("javascript:"):
            continue
        absolute = urljoin(base_url, raw) if base_url else raw
        parsed = urlparse(absolute)
        text = f"{parsed.path} {absolute}".lower()
        if not any(token in text for token in CAREERS_URL_HINTS + ("job", "position", "opening", "vacancy")):
            continue
        normalized = absolute.split("#", 1)[0]
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def parse_probe_count(adapter: str, text: str) -> int:
    if adapter == "lever":
        if text.strip().startswith("{"):
            payload = json.loads(text)
            postings = payload.get("data", []) if isinstance(payload, dict) else []
            return len(postings) if isinstance(postings, list) else 0
        payload = json.loads(text)
        return len(payload) if isinstance(payload, list) else 0
    if adapter == "greenhouse":
        if text.strip().startswith("{"):
            payload = json.loads(text)
            return len(payload.get("jobs", [])) if isinstance(payload, dict) else 0
        return len(set(re.findall(r'(?is)href=["\'][^"\']+/jobs/\d+[^"\']*["\']', text)))
    if adapter == "smartrecruiters":
        if text.strip().startswith("{"):
            payload = json.loads(text)
            return len(payload.get("content", [])) if isinstance(payload, dict) else 0
        return len(set(re.findall(r'(?is)href=["\'][^"\']+/job/[^"\']+["\']', text)))
    if adapter == "workable":
        if text.strip().startswith("{"):
            payload = json.loads(text)
            return len(payload.get("jobs", [])) if isinstance(payload, dict) else 0
        return len(set(re.findall(r'(?is)href=["\'][^"\']+/j/[^"\']+["\']', text)))
    if adapter == "personio":
        if text.lstrip().startswith("<"):
            return len(ET.fromstring(text).findall(".//position"))
        return 0
    if adapter == "ashby":
        return len(set(re.findall(r'(?is)<a[^>]+href=["\']([^"\']+/job/[^"\']+)["\']', text)))
    if adapter == "teamtailor":
        return len(set(re.findall(r'(?is)<a[^>]+href=["\']([^"\']+/jobs/[^"\']+)["\']', text)))
    if adapter == "static":
        return len(extract_jobish_links(text, ""))
    raise ValueError("unsupported adapter")


def probe_candidate(candidate: Dict[str, Any], timeout_s: int, *, fetcher=fetch_text) -> Tuple[bool, int, str]:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    url = endpoint_url(candidate)
    if not adapter or not url:
        return False, 0, "missing adapter or URL"
    valid, reason = validate_candidate_for_probe(candidate)
    if not valid:
        return False, 0, reason
    probe_urls = [url, *fallback_probe_urls(candidate)]
    seen_urls = set()
    last_error = "probe failed"
    for probe_url in probe_urls:
        if not probe_url or probe_url in seen_urls:
            continue
        seen_urls.add(probe_url)
        try:
            text = fetch_text_with_retry(probe_url, timeout_s, adapter=adapter, fetcher=fetcher)
            return True, max(0, int(parse_probe_count(adapter, text))), ""
        except Exception as exc:  # noqa: BLE001
            last_error = f"{probe_url}: {exc}"
    return False, 0, last_error


def classify_probe_failure_stage(error: str) -> str:
    text = str(error or "").lower()
    if "http error 404" in text or "http error 410" in text:
        return "probe_miss"
    if "not well-formed (invalid token)" in text:
        return "probe_miss"
    if "expecting value" in text and "line 1 column 1" in text:
        return "probe_miss"
    return "probe"


def compute_candidate_score(candidate: Dict[str, Any], jobs_found: int) -> Tuple[int, List[str]]:
    score = 0
    reasons: List[str] = []
    label = f"{candidate.get('name', '')} {candidate.get('studio', '')}".lower()
    if any(token in label for token in FOCUS_KEYWORDS):
        score += 35
        reasons.append("target_role_signal")
    if bool(candidate.get("nlPriority")):
        score += 25
        reasons.append("nl_priority")
    if bool(candidate.get("remoteFriendly")):
        score += 15
        reasons.append("remote_friendly")
    evidence = int(candidate.get("evidenceScore") or 0)
    if evidence > 0:
        score += min(25, evidence // 2)
        reasons.append("strong_evidence" if evidence >= 50 else "evidence_signal")
    if jobs_found > 0:
        score += min(25, jobs_found)
        reasons.append("live_jobs_detected")
    return min(100, score), reasons


def compute_confidence(candidate: Dict[str, Any], jobs_found: int) -> str:
    evidence = int(candidate.get("evidenceScore") or 0)
    if jobs_found >= 10 or evidence >= 60:
        return "high"
    if jobs_found >= 1 or evidence >= 35:
        return "medium"
    return "low"


def normalize_candidate(candidate: Dict[str, Any], score: int, reasons: List[str], jobs_found: int, *, probed_at: str) -> Dict[str, Any]:
    row = dict(candidate)
    row["id"] = source_identity(row)
    row["enabledByDefault"] = False
    row["score"] = int(score)
    row["reasons"] = reasons
    row["sampleCount"] = int(jobs_found)
    row["jobsFound"] = int(jobs_found)
    row["confidence"] = compute_confidence(row, jobs_found)
    row["discoveredAt"] = str(row.get("discoveredAt") or probed_at)
    row["lastProbedAt"] = probed_at
    row["discoveryMethod"] = str(row.get("discoveryMethod") or "seed")
    row["discoveryStage"] = str(row.get("discoveryStage") or "provider_pattern")
    row["evidenceScore"] = int(row.get("evidenceScore") or 0)
    row["evidenceTypes"] = unique_string_list(row.get("evidenceTypes") or [])
    row["evidenceSource"] = str(row.get("evidenceSource") or row.get("discoveryMethod") or "unknown")
    row["careersUrl"] = str(row.get("careersUrl") or endpoint_url(row) or "")
    row["weakSignal"] = bool(row.get("weakSignal"))
    row["deferred"] = bool(row.get("deferred"))
    return row


def unique_string_list(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in values:
        token = str(raw or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _strip_html_tags(html: str) -> str:
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", str(html or ""))
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _gamesmap_config_value(config: Optional[Dict[str, Any]], key: str, default: Any) -> Any:
    source = config if isinstance(config, dict) else {}
    return source.get(key, default)


def normalize_gamesmap_category_token(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9+]+", " ", str(value or "").lower())).strip()


def _extract_gamesmap_js_data_container(markup: str) -> Optional[List[Any]]:
    html = str(markup or "")
    token = "window.jsDataContainer"
    start = html.find(token)
    if start < 0:
        return None
    array_start = html.find("[", start)
    if array_start < 0:
        return None
    depth = 0
    for idx in range(array_start, len(html)):
        char = html[idx]
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(html[array_start : idx + 1])
                except json.JSONDecodeError:
                    return None
    return None


def parse_gamesmap_index_entries(html: str, base_url: str, *, prefer_english: bool = True) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen = set()
    payload = _extract_gamesmap_js_data_container(html)
    if isinstance(payload, list):
        for item in payload:
            if not (isinstance(item, list) and len(item) >= 2 and item[0] == "map.coordinates" and isinstance(item[1], dict)):
                continue
            points = item[1].get("points")
            if not isinstance(points, dict):
                continue
            for point in points.get("industry") or []:
                if not isinstance(point, dict):
                    continue
                slug = str(point.get("slug") or "").strip().strip("/")
                studio = str(point.get("name") or "").strip()
                if not slug or not studio:
                    continue
                path = f"/en/detail/industry/{slug}" if prefer_english else f"/detail/industry/{slug}"
                detail_url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
                province = point.get("province") if isinstance(point.get("province"), dict) else {}
                location = str((province.get("nameEn") if prefer_english else province.get("name")) or province.get("nameEn") or province.get("name") or "").strip()
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                out.append({
                    "detailUrl": detail_url,
                    "studio": studio,
                    "location": location,
                })
            break
    for detail_url in parse_gamesmap_index_links(html, base_url):
        if detail_url in seen:
            continue
        seen.add(detail_url)
        out.append({
            "detailUrl": detail_url,
            "studio": "",
            "location": "",
        })
    return out


def parse_gamesmap_index_links(html: str, base_url: str) -> List[str]:
    links = re.findall(r'(?is)href=["\']([^"\']+)["\']', str(html or ""))
    out: List[str] = []
    seen = set()
    for raw in links:
        absolute = urljoin(base_url, raw)
        try:
            parsed = urlparse(absolute)
        except ValueError:
            continue
        path = (parsed.path or "").lower()
        if "/detail/industry/" not in path:
            continue
        normalized = absolute.split("#", 1)[0]
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def parse_gamesmap_detail_page(page_url: str, html: str) -> Optional[Dict[str, Any]]:
    markup = str(html or "")
    name_match = re.search(r"(?is)<h1[^>]*>(.*?)</h1>", markup)
    if not name_match:
        title_match = re.search(r"(?is)<title[^>]*>(.*?)</title>", markup)
        name = _strip_html_tags(title_match.group(1)) if title_match else ""
    else:
        name = _strip_html_tags(name_match.group(1))
    if not name:
        return None

    categories: List[str] = []
    for match in re.finditer(r'(?is)<[^>]+class=["\'][^"\']*(?:tag|badge|category|chip)[^"\']*["\'][^>]*>(.*?)</[^>]+>', markup):
        token = _strip_html_tags(match.group(1))
        if token:
            categories.append(token)
    for match in re.finditer(r'(?is)(?:Category|Categories|Branche|Branchen)\s*</[^>]+>\s*<[^>]+>(.*?)</[^>]+>', markup):
        chunk = _strip_html_tags(match.group(1))
        for part in re.split(r"[|,/]| • |\s{2,}", chunk):
            token = part.strip()
            if token:
                categories.append(token)
    categories = unique_string_list(categories)

    location = ""
    for match in re.finditer(r'(?is)(?:Location|Standort|City)\s*</[^>]+>\s*<[^>]+>(.*?)</[^>]+>', markup):
        token = _strip_html_tags(match.group(1))
        if token:
            location = token
            break

    website_url = ""
    careers_url = ""
    ignored_hosts = (
        "game.de",
        "facebook.com",
        "instagram.com",
        "twitter.com",
        "x.com",
        "twitch.tv",
        "linkedin.com",
        "youtube.com",
        "youtu.be",
        "discord.gg",
        "discord.com",
        "list-manage.com",
    )
    for match in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', markup):
        href = str(match.group(1) or "").strip()
        if not href or href.startswith(("mailto:", "javascript:", "#")):
            continue
        absolute = urljoin(page_url, href).split("#", 1)[0]
        try:
            parsed = urlparse(absolute)
            page_host = (urlparse(page_url).netloc or "").lower()
        except ValueError:
            continue
        host = (parsed.netloc or "").lower()
        if not host or host.endswith("gamesmap.de") or host == page_host:
            continue
        if any(host == blocked or host.endswith(f".{blocked}") for blocked in ignored_hosts):
            continue
        label = _strip_html_tags(match.group(2))
        context_start = max(0, match.start() - 140)
        context = _strip_html_tags(markup[context_start:match.end()])
        hint_blob = f"{label} {absolute} {context}".lower()
        if any(token in hint_blob for token in CAREERS_URL_HINTS + ("job page", "job pages", "stellen", "karriere")):
            if not careers_url:
                careers_url = absolute
                continue
        if not website_url:
            website_url = absolute
    if not website_url and careers_url:
        try:
            parsed = urlparse(careers_url)
            website_url = f"{parsed.scheme}://{parsed.netloc}"
        except ValueError:
            website_url = ""

    return {
        "studio": name,
        "detailUrl": page_url,
        "websiteUrl": website_url,
        "careersUrl": careers_url,
        "categories": categories,
        "location": location,
    }


def gamesmap_matches_category(categories: Iterable[str], allowed: Iterable[str], blocked: Iterable[str]) -> bool:
    normalized_categories = [normalize_gamesmap_category_token(item) for item in categories if str(item or "").strip()]
    if not normalized_categories:
        return False
    blocked_tokens = [normalize_gamesmap_category_token(item) for item in blocked if str(item or "").strip()]
    if any(any(token and token in category for token in blocked_tokens) for category in normalized_categories):
        return False
    allowed_tokens = [normalize_gamesmap_category_token(item) for item in allowed if str(item or "").strip()]
    if not allowed_tokens:
        return True
    return any(any(token and token in category for token in allowed_tokens) for category in normalized_categories)


def build_gamesmap_static_candidate(
    *,
    studio: str,
    target_url: str,
    nl_priority: bool,
    remote_friendly: bool,
    website_only: bool,
    detail_url: str,
    categories: List[str],
    location: str,
    manual_only: bool = False,
) -> Dict[str, Any]:
    evidence_types = ["gamesmap_directory", "gamesmap_category_match"]
    evidence_score = 24
    if website_only:
        evidence_types.append("gamesmap_website")
        evidence_types.append("gamesmap_website_only")
        if manual_only:
            evidence_types.append("gamesmap_manual_website_only")
    else:
        evidence_types.append("gamesmap_careers_url")
        evidence_score = 40
    if location:
        evidence_types.append("gamesmap_location")
    return {
        "name": f"{studio} (Gamesmap)",
        "studio": studio,
        "company": studio,
        "adapter": "static",
        "pages": [target_url],
        "listing_url": target_url,
        "remoteFriendly": remote_friendly,
        "nlPriority": nl_priority,
        "enabledByDefault": False,
        "discoveryMethod": "gamesmap",
        "discoveryStage": "generic_static",
        "careersUrl": "" if website_only else target_url,
        "evidenceSource": "gamesmap",
        "evidenceTypes": evidence_types,
        "evidenceScore": evidence_score,
        "weakSignal": bool(website_only),
        "sourceDirectory": "gamesmap",
        "sourceDirectoryUrl": "https://www.gamesmap.de/",
        "sourceDirectoryEntryUrl": detail_url,
        "sourceDirectoryCategories": unique_string_list(categories),
        "sourceDirectoryLocation": str(location or "").strip(),
        "manualOnly": bool(manual_only),
    }


def discover_gamesmap_candidates(timeout_s: int, *, config: Optional[Dict[str, Any]] = None, fetcher=fetch_text) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    cfg = dict(_gamesmap_config_value(config, "gamesmap", DEFAULT_DISCOVERY_CONFIG["gamesmap"]))
    if not bool(cfg.get("enabled")):
        return [], [], []
    base_url = str(cfg.get("baseUrl") or "https://www.gamesmap.de").strip()
    index_urls = [str(item).strip() for item in (cfg.get("indexUrls") or []) if str(item).strip()]
    prefer_english = bool(cfg.get("preferEnglish", True))
    allowed_tokens = list(cfg.get("allowedCategoryTokens") or [])
    blocked_tokens = list(cfg.get("blockedCategoryTokens") or [])
    website_only_fallback = bool(cfg.get("websiteOnlyFallback", True))
    website_only_manual_only = bool(cfg.get("websiteOnlyManualOnly", False))
    max_detail_pages = max(0, int(cfg.get("maxDetailPages") or 0))

    provider_candidates: List[Dict[str, Any]] = []
    static_candidates: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    detail_entries: List[Dict[str, str]] = []
    seen_details = set()

    for index_url in index_urls:
        try:
            index_html = fetcher(index_url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            failures.append({"name": index_url, "adapter": "gamesmap", "error": str(exc), "stage": "directory_index_fetch"})
            continue
        for entry in parse_gamesmap_index_entries(index_html, base_url, prefer_english=prefer_english):
            detail_url = str(entry.get("detailUrl") or "").strip()
            if not detail_url:
                continue
            if detail_url in seen_details:
                continue
            seen_details.add(detail_url)
            detail_entries.append(entry)
            if max_detail_pages and len(detail_entries) >= max_detail_pages:
                break
        if max_detail_pages and len(detail_entries) >= max_detail_pages:
            break

    for entry in detail_entries:
        detail_url = str(entry.get("detailUrl") or "").strip()
        try:
            detail_html = fetcher(detail_url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            failures.append({"name": detail_url, "adapter": "gamesmap", "error": str(exc), "stage": "directory_detail_fetch"})
            continue
        parsed = parse_gamesmap_detail_page(detail_url, detail_html)
        if not parsed:
            failures.append({"name": detail_url, "adapter": "gamesmap", "error": "detail parse failed", "stage": "directory_detail_parse"})
            continue
        studio = str(parsed.get("studio") or "").strip()
        categories = list(parsed.get("categories") or [])
        if not studio or not gamesmap_matches_category(categories, allowed_tokens, blocked_tokens):
            continue
        location = str(parsed.get("location") or entry.get("location") or "").strip()
        careers_url = str(parsed.get("careersUrl") or "").strip()
        website_url = str(parsed.get("websiteUrl") or "").strip()
        nl_priority = False
        remote_friendly = True

        if careers_url:
            inferred = infer_web_candidate(careers_url, studio, nl_priority=nl_priority, remote_friendly=remote_friendly, discovery_method="gamesmap")
            if inferred:
                inferred["evidenceSource"] = "gamesmap"
                inferred["evidenceTypes"] = unique_string_list([*(inferred.get("evidenceTypes") or []), "gamesmap_directory", "gamesmap_careers_url", "gamesmap_category_match"])
                inferred["evidenceScore"] = max(int(inferred.get("evidenceScore") or 0), 44)
                inferred["careersUrl"] = careers_url
                inferred["sourceDirectory"] = "gamesmap"
                inferred["sourceDirectoryUrl"] = "https://www.gamesmap.de/"
                inferred["sourceDirectoryEntryUrl"] = detail_url
                inferred["sourceDirectoryCategories"] = unique_string_list(categories)
                inferred["sourceDirectoryLocation"] = location
                provider_candidates.append(inferred)
            else:
                static_candidates.append(
                    build_gamesmap_static_candidate(
                        studio=studio,
                        target_url=careers_url,
                        nl_priority=nl_priority,
                        remote_friendly=remote_friendly,
                        website_only=False,
                        detail_url=detail_url,
                        categories=categories,
                        location=location,
                        manual_only=False,
                    )
                )
            continue

        if website_only_fallback and website_url:
            static_candidates.append(
                build_gamesmap_static_candidate(
                    studio=studio,
                    target_url=website_url,
                    nl_priority=nl_priority,
                    remote_friendly=remote_friendly,
                    website_only=True,
                    detail_url=detail_url,
                    categories=categories,
                    location=location,
                    manual_only=website_only_manual_only,
                )
            )

    return unique_sources(provider_candidates), unique_sources(static_candidates), failures


def expand_aliases(seed: Dict[str, Any]) -> List[str]:
    aliases = [str(seed.get("studio") or "")]
    aliases.extend(str(item) for item in (seed.get("aliases") or []) if item)
    normalized: List[str] = []
    seen = set()
    for raw in aliases:
        slug = to_slug(raw)
        token = clean_token(raw)
        for value in (slug, token):
            if not value or value in seen:
                continue
            seen.add(value)
            normalized.append(value)
    return normalized


def likely_providers_for_seed(seed: Dict[str, Any]) -> List[str]:
    explicit = [str(item).strip().lower() for item in (seed.get("likelyProviders") or []) if str(item).strip()]
    if explicit:
        return [item for item in explicit if item in SUPPORTED_PROVIDERS or item == "static"]
    providers = {"greenhouse", "workable", "teamtailor"}
    if not bool(seed.get("nlPriority")):
        providers.update({"lever", "smartrecruiters", "ashby"})
    if bool(seed.get("remoteFriendly")):
        providers.add("personio")
    return [item for item in SUPPORTED_PROVIDERS if item in providers]


def provider_reinforcement_score(seed: Dict[str, Any], provider: str) -> int:
    careers_url = str(seed.get("careersUrl") or "").strip().lower()
    if not careers_url:
        return 0
    parsed = urlparse(careers_url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    if provider == "greenhouse":
        return 18 if "greenhouse" in host or "greenhouse" in path else 0
    if provider == "lever":
        return 18 if "lever.co" in host else 0
    if provider == "smartrecruiters":
        return 18 if "smartrecruiters" in host else 0
    if provider == "workable":
        return 18 if "workable" in host else 0
    if provider == "ashby":
        return 18 if "ashbyhq" in host else 0
    if provider == "personio":
        return 18 if ".jobs.personio.de" in host else 0
    if provider == "teamtailor":
        if ".teamtailor.com" in host:
            return 18
        if path.startswith("/jobs") and careers_keyword_count(careers_url):
            return 8
        return 0
    return 0


def _pattern_aliases_for_provider(seed: Dict[str, Any], provider: str) -> List[str]:
    aliases = expand_aliases(seed)
    return aliases[:2] if provider in {"greenhouse", "lever", "workable", "teamtailor"} else aliases[:1]


def build_pattern_candidates() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for seed in STUDIO_SEEDS:
        studio = str(seed.get("studio") or "").strip()
        if not studio:
            continue
        nl_priority = bool(seed.get("nlPriority"))
        remote_friendly = bool(seed.get("remoteFriendly"))
        careers_url = str(seed.get("careersUrl") or "").strip()
        evidence_types = ["provider_hint", "seed_catalog"]
        explicit = [str(item).strip().lower() for item in (seed.get("likelyProviders") or []) if str(item).strip()]
        for provider in likely_providers_for_seed(seed):
            reinforcement = provider_reinforcement_score(seed, provider)
            for alias in _pattern_aliases_for_provider(seed, provider):
                base: Dict[str, Any] = {
                    "studio": studio,
                    "remoteFriendly": remote_friendly,
                    "nlPriority": nl_priority,
                    "discoveryMethod": "pattern",
                    "discoveryStage": "provider_pattern",
                    "careersUrl": careers_url,
                    "evidenceScore": 14 + (10 if provider in explicit else 0) + reinforcement,
                    "evidenceTypes": unique_string_list([*evidence_types, "provider_reinforced"] if reinforcement else evidence_types),
                    "evidenceSource": "pattern",
                }
                if provider == "lever":
                    rows.append({**base, "name": f"{studio} (Lever)", "adapter": "lever", "account": alias, "api_url": f"https://api.lever.co/v0/postings/{alias}?mode=json"})
                elif provider == "greenhouse":
                    rows.append({**base, "name": f"{studio} (Greenhouse)", "adapter": "greenhouse", "slug": alias, "api_url": f"https://boards-api.greenhouse.io/v1/boards/{alias}/jobs?content=true"})
                elif provider == "smartrecruiters":
                    rows.append({**base, "name": f"{studio} (SmartRecruiters)", "adapter": "smartrecruiters", "company_id": alias.upper(), "api_url": f"https://api.smartrecruiters.com/v1/companies/{alias.upper()}/postings"})
                elif provider == "workable":
                    rows.append({**base, "name": f"{studio} (Workable)", "adapter": "workable", "account": alias, "api_url": f"https://apply.workable.com/api/v1/widget/accounts/{alias}?details=true"})
                elif provider == "teamtailor":
                    rows.append({**base, "name": f"{studio} (Teamtailor)", "adapter": "teamtailor", "company": studio, "listing_url": f"https://{alias}.teamtailor.com/jobs", "base_url": f"https://{alias}.teamtailor.com"})
                elif provider == "ashby":
                    rows.append({**base, "name": f"{studio} (Ashby)", "adapter": "ashby", "board_url": f"https://jobs.ashbyhq.com/{alias}/jobs"})
                elif provider == "personio":
                    rows.append({**base, "name": f"{studio} (Personio)", "adapter": "personio", "feed_url": f"https://{alias}.jobs.personio.de/xml"})
    return unique_sources(rows)


def stage_curated_seed_candidates() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for raw in STATIC_DISCOVERY_CANDIDATES:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        row["discoveryMethod"] = str(row.get("discoveryMethod") or "seed")
        row["discoveryStage"] = "curated_seed"
        row["evidenceScore"] = int(row.get("evidenceScore") or 52)
        row["evidenceTypes"] = list(row.get("evidenceTypes") or ["curated_seed"])
        row["evidenceSource"] = str(row.get("evidenceSource") or "seed")
        row["careersUrl"] = str(row.get("careersUrl") or endpoint_url(row) or "")
        rows.append(row)
    return unique_sources(rows)


def extract_links_from_html(html: str) -> List[str]:
    links = re.findall(r'(?is)href=["\']([^"\']+)["\']', html)
    out: List[str] = []
    for raw in links:
        if not raw:
            continue
        if "uddg=" in raw:
            query = parse_qs(urlparse(raw).query)
            target = query.get("uddg", [""])[0]
            if target:
                out.append(unquote(target))
        elif raw.startswith("http://") or raw.startswith("https://"):
            out.append(raw)
    return out


def studio_domain_match(studio: str, url: str) -> bool:
    token = clean_token(studio)
    if not token:
        return False
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    return bool(token[:8] and token[:8] in clean_token(f"{parsed.netloc} {parsed.path}"))


def _provider_candidate(*, studio: str, adapter: str, url: str, nl_priority: bool, remote_friendly: bool, discovery_method: str, evidence_types: List[str], evidence_source: str, evidence_score: int) -> Optional[Dict[str, Any]]:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    if adapter == "greenhouse":
        slug = clean_token(path.split("/boards/", 1)[1].split("/", 1)[0]) if "boards-api.greenhouse.io" in host and "/boards/" in path else clean_token(([p for p in path.split("/") if p] or [""])[0])
        if not slug:
            return None
        return {"name": f"{studio} (Greenhouse)", "studio": studio, "adapter": "greenhouse", "slug": slug, "api_url": f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true", "remoteFriendly": remote_friendly, "nlPriority": nl_priority, "discoveryMethod": discovery_method, "discoveryStage": "web_provider", "careersUrl": url, "evidenceScore": evidence_score, "evidenceTypes": evidence_types, "evidenceSource": evidence_source}
    if adapter == "lever":
        if "api.lever.co" in host and "/v0/postings/" in path:
            account = clean_token(path.split("/v0/postings/", 1)[1].split("/", 1)[0])
        else:
            account = clean_token(([p for p in path.split("/") if p] or [""])[0])
        if not account:
            return None
        return {"name": f"{studio} (Lever)", "studio": studio, "adapter": "lever", "account": account, "api_url": f"https://api.lever.co/v0/postings/{account}?mode=json", "remoteFriendly": remote_friendly, "nlPriority": nl_priority, "discoveryMethod": discovery_method, "discoveryStage": "web_provider", "careersUrl": url, "evidenceScore": evidence_score, "evidenceTypes": evidence_types, "evidenceSource": evidence_source}
    if adapter == "smartrecruiters":
        company_id = ""
        if "api.smartrecruiters.com" in host and "/companies/" in path:
            pieces = [p for p in path.split("/") if p]
            if "companies" in pieces:
                idx = pieces.index("companies")
                if idx + 1 < len(pieces):
                    company_id = pieces[idx + 1].strip()
        elif "jobs.smartrecruiters.com" in host:
            company_id = ([p for p in path.split("/") if p] or [""])[0].strip()
        if not company_id:
            return None
        return {"name": f"{studio} (SmartRecruiters)", "studio": studio, "adapter": "smartrecruiters", "company_id": company_id, "api_url": f"https://api.smartrecruiters.com/v1/companies/{company_id}/postings", "remoteFriendly": remote_friendly, "nlPriority": nl_priority, "discoveryMethod": discovery_method, "discoveryStage": "web_provider", "careersUrl": url, "evidenceScore": evidence_score, "evidenceTypes": evidence_types, "evidenceSource": evidence_source}
    if adapter == "workable":
        account = clean_token(([p for p in path.split("/") if p] or [""])[-1])
        if not account:
            return None
        return {"name": f"{studio} (Workable)", "studio": studio, "adapter": "workable", "account": account, "api_url": f"https://apply.workable.com/api/v1/widget/accounts/{account}?details=true", "remoteFriendly": remote_friendly, "nlPriority": nl_priority, "discoveryMethod": discovery_method, "discoveryStage": "web_provider", "careersUrl": url, "evidenceScore": evidence_score, "evidenceTypes": evidence_types, "evidenceSource": evidence_source}
    if adapter == "teamtailor":
        base_url = f"{parsed.scheme}://{host}" if parsed.scheme else f"https://{host}"
        return {"name": f"{studio} (Teamtailor)", "studio": studio, "adapter": "teamtailor", "listing_url": f"{base_url}/jobs", "base_url": base_url, "company": studio, "remoteFriendly": remote_friendly, "nlPriority": nl_priority, "discoveryMethod": discovery_method, "discoveryStage": "web_provider", "careersUrl": url, "evidenceScore": evidence_score, "evidenceTypes": evidence_types, "evidenceSource": evidence_source}
    if adapter == "ashby":
        slug = clean_token(([p for p in path.split("/") if p] or [""])[0])
        if not slug:
            return None
        return {"name": f"{studio} (Ashby)", "studio": studio, "adapter": "ashby", "board_url": f"https://jobs.ashbyhq.com/{slug}/jobs", "remoteFriendly": remote_friendly, "nlPriority": nl_priority, "discoveryMethod": discovery_method, "discoveryStage": "web_provider", "careersUrl": url, "evidenceScore": evidence_score, "evidenceTypes": evidence_types, "evidenceSource": evidence_source}
    if adapter == "personio":
        token = host.split(".jobs.personio.de", 1)[0]
        if not token:
            return None
        return {"name": f"{studio} (Personio)", "studio": studio, "adapter": "personio", "feed_url": f"https://{token}.jobs.personio.de/xml", "remoteFriendly": remote_friendly, "nlPriority": nl_priority, "discoveryMethod": discovery_method, "discoveryStage": "web_provider", "careersUrl": url, "evidenceScore": evidence_score, "evidenceTypes": evidence_types, "evidenceSource": evidence_source}
    return None


def candidate_variant_key(candidate: Dict[str, Any]) -> str:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    studio = clean_token(str(candidate.get("studio") or candidate.get("name") or ""))
    careers_url = str(candidate.get("careersUrl") or "").strip().lower()
    if not adapter:
        return ""
    return f"{adapter}:{studio}:{careers_url}"


def collapse_competing_candidates(candidates: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    preferred: Dict[str, Dict[str, Any]] = {}
    passthrough: List[Dict[str, Any]] = []
    for raw in candidates:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        if str(row.get("discoveryMethod") or "") != "seed_careers_page":
            passthrough.append(row)
            continue
        key = candidate_variant_key(row)
        if not key:
            passthrough.append(row)
            continue
        current = preferred.get(key)
        if current is None:
            preferred[key] = row
            continue
        current_score = (
            int(current.get("evidenceScore") or 0),
            careers_keyword_count(endpoint_url(current)),
            int(bool(studio_domain_match(str(current.get("studio") or ""), endpoint_url(current)))),
            len(endpoint_url(current)),
        )
        row_score = (
            int(row.get("evidenceScore") or 0),
            careers_keyword_count(endpoint_url(row)),
            int(bool(studio_domain_match(str(row.get("studio") or ""), endpoint_url(row)))),
            len(endpoint_url(row)),
        )
        if row_score > current_score:
            preferred[key] = row
    return unique_sources([*passthrough, *preferred.values()])


def infer_web_candidate(url: str, studio: str, *, nl_priority: bool, remote_friendly: bool, discovery_method: str = "web_search") -> Optional[Dict[str, Any]]:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    evidence_types = ["provider_url"]
    evidence_score = 28 + (12 if studio_domain_match(studio, url) else 0) + (4 if careers_keyword_count(url) else 0)
    if "boards.greenhouse.io" in host or "jobs.greenhouse.io" in host or "boards-api.greenhouse.io" in host:
        return _provider_candidate(studio=studio, adapter="greenhouse", url=url, nl_priority=nl_priority, remote_friendly=remote_friendly, discovery_method=discovery_method, evidence_types=evidence_types, evidence_source="url", evidence_score=evidence_score)
    if "jobs.ashbyhq.com" in host:
        return _provider_candidate(studio=studio, adapter="ashby", url=url, nl_priority=nl_priority, remote_friendly=remote_friendly, discovery_method=discovery_method, evidence_types=evidence_types, evidence_source="url", evidence_score=evidence_score)
    if "apply.workable.com" in host:
        return _provider_candidate(studio=studio, adapter="workable", url=url, nl_priority=nl_priority, remote_friendly=remote_friendly, discovery_method=discovery_method, evidence_types=evidence_types, evidence_source="url", evidence_score=evidence_score)
    if ".teamtailor.com" in host:
        return _provider_candidate(studio=studio, adapter="teamtailor", url=url, nl_priority=nl_priority, remote_friendly=remote_friendly, discovery_method=discovery_method, evidence_types=evidence_types, evidence_source="url", evidence_score=evidence_score)
    if ".jobs.personio.de" in host:
        return _provider_candidate(studio=studio, adapter="personio", url=url, nl_priority=nl_priority, remote_friendly=remote_friendly, discovery_method=discovery_method, evidence_types=evidence_types, evidence_source="url", evidence_score=evidence_score)
    if ("api.lever.co" in host and "/v0/postings/" in path) or ("lever.co" in host and host != "api.lever.co"):
        return _provider_candidate(studio=studio, adapter="lever", url=url, nl_priority=nl_priority, remote_friendly=remote_friendly, discovery_method=discovery_method, evidence_types=evidence_types, evidence_source="url", evidence_score=evidence_score)
    if ("api.smartrecruiters.com" in host and "/companies/" in path) or "jobs.smartrecruiters.com" in host:
        return _provider_candidate(studio=studio, adapter="smartrecruiters", url=url, nl_priority=nl_priority, remote_friendly=remote_friendly, discovery_method=discovery_method, evidence_types=evidence_types, evidence_source="url", evidence_score=evidence_score)
    return None


def infer_provider_candidates_from_html(page_url: str, html: str, *, studio: str, nl_priority: bool, remote_friendly: bool, discovery_method: str = "web_search") -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    page_candidate = infer_web_candidate(page_url, studio, nl_priority=nl_priority, remote_friendly=remote_friendly, discovery_method=discovery_method)
    if page_candidate:
        page_candidate["evidenceSource"] = "page_url"
        page_candidate["evidenceTypes"] = unique_string_list([*(page_candidate.get("evidenceTypes") or []), "careers_page"])
        page_candidate["evidenceScore"] = int(page_candidate.get("evidenceScore") or 0) + 10
        page_candidate["careersUrl"] = page_url
        candidates.append(page_candidate)
    embedded_urls = extract_links_from_html(html)
    embedded_urls.extend(re.findall(r'https?://[^"\')\s]+', str(html or "")))
    text = str(html or "").lower()
    if "teamtailor" in text and careers_keyword_count(page_url):
        embedded_urls.append(page_url)
    seen = set()
    for raw_url in embedded_urls:
        url = str(raw_url or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        inferred = infer_web_candidate(url, studio, nl_priority=nl_priority, remote_friendly=remote_friendly, discovery_method=discovery_method)
        if not inferred:
            continue
        inferred["evidenceSource"] = "html_embed"
        inferred["evidenceTypes"] = unique_string_list([*(inferred.get("evidenceTypes") or []), "html_embed", "careers_page"])
        inferred["evidenceScore"] = int(inferred.get("evidenceScore") or 0) + 12
        inferred["careersUrl"] = page_url
        candidates.append(inferred)
    return collapse_competing_candidates(candidates)


def build_static_candidate_from_page(page_url: str, html: str, *, studio: str, nl_priority: bool, remote_friendly: bool, discovery_method: str) -> Optional[Dict[str, Any]]:
    if is_blocked_generic_static_url(page_url):
        return None
    if not careers_keyword_count(page_url) and careers_keyword_count(html) == 0:
        return None
    detail_links = extract_jobish_links(html, page_url)
    jsonld_hits = re.findall(r'"@type"\s*:\s*"JobPosting"', str(html or ""), flags=re.I)
    if not detail_links and not jsonld_hits:
        return None
    evidence_types = ["careers_keyword"]
    evidence_score = 18
    if detail_links:
        evidence_types.append("structured_job_links")
        evidence_score += min(24, len(detail_links) * 6)
    if jsonld_hits:
        evidence_types.append("jobposting_jsonld")
        evidence_score += 18
    if studio_domain_match(studio, page_url):
        evidence_types.append("studio_domain_match")
        evidence_score += 10
    detail_sample = detail_links[:6]
    return {
        "name": f"{studio} (Manual Website)",
        "studio": studio,
        "company": studio,
        "adapter": "static",
        "pages": [page_url, *detail_sample],
        "listing_url": page_url,
        "remoteFriendly": remote_friendly,
        "nlPriority": nl_priority,
        "enabledByDefault": False,
        "discoveryMethod": discovery_method,
        "discoveryStage": "generic_static",
        "careersUrl": page_url,
        "evidenceSource": "careers_page",
        "evidenceTypes": evidence_types,
        "evidenceScore": evidence_score,
        "weakSignal": len(detail_sample) < 2 and not jsonld_hits,
        "detailPageCount": len(detail_links),
        "detailPagesSample": detail_sample,
    }


def build_web_search_queries(max_queries: int = 18) -> List[Tuple[str, Dict[str, Any]]]:
    queries: List[Tuple[str, Dict[str, Any]]] = []
    for seed in STUDIO_SEEDS:
        studio = str(seed.get("studio") or "").strip()
        if not studio:
            continue
        for suffix in WEB_SEARCH_QUERY_SUFFIX:
            queries.append((f"{studio} {suffix} game studio", seed))
        careers_url = str(seed.get("careersUrl") or "").strip()
        if careers_url:
            host = (urlparse(careers_url).netloc or "").strip()
            if host:
                queries.append((f"{studio} site:{host} jobs", seed))
        if len(queries) >= max_queries:
            break
    return queries[:max_queries]


def discover_seed_careers_page_candidates(timeout_s: int, *, fetcher=fetch_text) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    provider_candidates: List[Dict[str, Any]] = []
    static_candidates: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for seed in STUDIO_SEEDS:
        careers_url = str(seed.get("careersUrl") or "").strip()
        studio = str(seed.get("studio") or "").strip()
        if not careers_url or not studio:
            continue
        nl_priority = bool(seed.get("nlPriority"))
        remote_friendly = bool(seed.get("remoteFriendly"))
        try:
            page_html = fetcher(careers_url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            failures.append({"name": careers_url, "adapter": "seed_careers_page", "error": str(exc), "stage": "page_fetch"})
            continue
        page_provider_candidates = infer_provider_candidates_from_html(
            careers_url,
            page_html,
            studio=studio,
            nl_priority=nl_priority,
            remote_friendly=remote_friendly,
            discovery_method="seed_careers_page",
        )
        provider_candidates.extend(page_provider_candidates)
        if page_provider_candidates:
            continue
        static_candidate = build_static_candidate_from_page(
            careers_url,
            page_html,
            studio=studio,
            nl_priority=nl_priority,
            remote_friendly=remote_friendly,
            discovery_method="seed_careers_page",
        )
        if static_candidate:
            static_candidates.append(static_candidate)
    return collapse_competing_candidates(provider_candidates), unique_sources(static_candidates), failures


def discover_web_search_candidates(timeout_s: int, *, fetcher=fetch_text, max_queries: int = 18) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    provider_candidates: List[Dict[str, Any]] = []
    static_candidates: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for query, seed in build_web_search_queries(max_queries=max_queries):
        url = DUCKDUCKGO_HTML_SEARCH.format(query=quote_plus(query))
        try:
            html = fetcher(url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            failures.append({"name": query, "adapter": "web_search", "error": str(exc), "stage": "search"})
            continue
        links = extract_links_from_html(html)[:MAX_SEARCH_LINKS_PER_QUERY]
        studio = str(seed.get("studio") or "")
        nl_priority = bool(seed.get("nlPriority"))
        remote_friendly = bool(seed.get("remoteFriendly"))
        for link in links:
            inferred = infer_web_candidate(link, studio, nl_priority=nl_priority, remote_friendly=remote_friendly, discovery_method="web_search")
            if inferred:
                provider_candidates.append(inferred)
                continue
            if not careers_keyword_count(link):
                continue
            try:
                page_html = fetcher(link, timeout_s)
            except Exception as exc:  # noqa: BLE001
                failures.append({"name": link, "adapter": "web_search", "error": str(exc), "stage": "page_fetch"})
                continue
            provider_candidates.extend(
                infer_provider_candidates_from_html(
                    link,
                    page_html,
                    studio=studio,
                    nl_priority=nl_priority,
                    remote_friendly=remote_friendly,
                    discovery_method="web_search",
                )
            )
            static_candidate = build_static_candidate_from_page(link, page_html, studio=studio, nl_priority=nl_priority, remote_friendly=remote_friendly, discovery_method="web_search")
            if static_candidate:
                static_candidates.append(static_candidate)
    return collapse_competing_candidates(provider_candidates), unique_sources(static_candidates), failures


def merge_candidate_streams(streams: Iterable[Tuple[str, List[Dict[str, Any]]]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for stage, items in streams:
        for raw in items:
            if not isinstance(raw, dict):
                continue
            row = dict(raw)
            row["discoveryStage"] = str(row.get("discoveryStage") or stage)
            row["discoveryMethod"] = str(row.get("discoveryMethod") or ("seed" if stage == "curated_seed" else "pattern"))
            row["discoveredAt"] = str(row.get("discoveredAt") or now_iso())
            row["evidenceTypes"] = unique_string_list(row.get("evidenceTypes") or [])
            row["evidenceScore"] = int(row.get("evidenceScore") or 0)
            rows.append(row)
    return rows


def estimate_probe_priority(candidate: Dict[str, Any]) -> int:
    return int(candidate.get("evidenceScore") or 0) + (20 if str(candidate.get("discoveryStage") or "") == "curated_seed" else 0)


def _evidence_threshold_for_probe(candidate: Dict[str, Any]) -> int:
    if str(candidate.get("discoveryStage") or "") == "provider_pattern":
        return PATTERN_PROVIDER_PROBE_THRESHOLD
    return MIN_STATIC_EVIDENCE_TO_PROBE if str(candidate.get("adapter") or "") == "static" else MIN_PROVIDER_EVIDENCE_TO_PROBE


def _evidence_threshold_for_queue(candidate: Dict[str, Any]) -> int:
    if str(candidate.get("discoveryStage") or "") == "provider_pattern":
        return PATTERN_PROVIDER_QUEUE_THRESHOLD
    return MIN_STATIC_EVIDENCE_TO_QUEUE if str(candidate.get("adapter") or "") == "static" else MIN_PROVIDER_EVIDENCE_TO_QUEUE


def _should_queue_candidate(candidate: Dict[str, Any], jobs_found: int) -> bool:
    return jobs_found > 0 or int(candidate.get("evidenceScore") or 0) >= _evidence_threshold_for_queue(candidate)


def _sort_candidate_key(row: Dict[str, Any]) -> Tuple[int, int, int, str]:
    return (int(row.get("score") or 0), int(row.get("evidenceScore") or 0), int(row.get("jobsFound") or 0), str(row.get("name") or ""))


def apply_queue_balancing(candidates: List[Dict[str, Any]], top_n: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    queued: List[Dict[str, Any]] = []
    all_rows: List[Dict[str, Any]] = []
    deferred_counts: Counter[str] = Counter()
    adapter_counts: Counter[str] = Counter()
    family_counts: Counter[str] = Counter()
    for row in sorted(candidates, key=_sort_candidate_key, reverse=True):
        adapter = str(row.get("adapter") or "unknown")
        family = queue_family_key(row)
        defer_reason = ""
        if top_n > 0 and len(queued) >= top_n:
            defer_reason = "top_n_cap"
        elif adapter_counts[adapter] >= ADAPTER_QUEUE_CAPS.get(adapter, 3):
            defer_reason = "adapter_cap"
        elif family and family_counts[family] >= DOMAIN_QUEUE_CAP_DEFAULT:
            defer_reason = "domain_cap"
        normalized = dict(row)
        if defer_reason:
            normalized["deferred"] = True
            normalized["deferReason"] = defer_reason
            deferred_counts[defer_reason] += 1
        else:
            normalized["deferred"] = False
            queued.append(normalized)
            adapter_counts[adapter] += 1
            if family:
                family_counts[family] += 1
        all_rows.append(normalized)
    return queued, all_rows, dict(deferred_counts)


def _init_stage_counter() -> Dict[str, int]:
    return {stage: 0 for stage in DISCOVERY_STAGES}


def run_discovery(timeout_s: int, top_n: int, *, mode: str = "dynamic", include_web_search: bool = True, discovery_config: Optional[Dict[str, Any]] = None, fetcher=fetch_text) -> Dict[str, Any]:
    started_at = now_iso()
    effective_config = discovery_config if isinstance(discovery_config, dict) else load_discovery_config()
    active = load_json_array(ACTIVE_PATH, [])
    pending_existing = load_json_array(PENDING_PATH, [])
    rejected = load_json_array(REJECTED_PATH, [])
    emit_log(f"Starting source discovery: mode={mode}, top_n={top_n}, web_search={'on' if include_web_search else 'off'}.")
    emit_log(f"Loaded registries: active={len(active)}, pending={len(pending_existing)}, rejected={len(rejected)}.")

    existing_rows = [*active, *pending_existing, *rejected]
    seen_ids = {source_identity(row) for row in existing_rows if isinstance(row, dict)}
    seen_domains = {fp for fp in (adapter_domain_fingerprint(row) for row in existing_rows if isinstance(row, dict)) if fp}

    provider_web_candidates: List[Dict[str, Any]] = []
    static_web_candidates: List[Dict[str, Any]] = []
    web_failures: List[Dict[str, Any]] = []

    streams: List[Tuple[str, List[Dict[str, Any]]]] = [("curated_seed", stage_curated_seed_candidates())]
    if mode == "dynamic":
        streams.append(("provider_pattern", build_pattern_candidates()))
        provider_web_candidates, static_web_candidates, web_failures = discover_seed_careers_page_candidates(timeout_s, fetcher=fetcher)
        streams.append(("web_provider", provider_web_candidates))
        streams.append(("generic_static", static_web_candidates))
        provider_gamesmap_candidates, static_gamesmap_candidates, gamesmap_failures = discover_gamesmap_candidates(
            timeout_s,
            config=effective_config,
            fetcher=fetcher,
        )
        web_failures.extend(gamesmap_failures)
        streams.append(("web_provider", provider_gamesmap_candidates))
        streams.append(("generic_static", static_gamesmap_candidates))
        if include_web_search:
            provider_search_candidates, static_search_candidates, search_failures = discover_web_search_candidates(timeout_s, fetcher=fetcher)
            provider_web_candidates.extend(provider_search_candidates)
            static_web_candidates.extend(static_search_candidates)
            web_failures.extend(search_failures)
            streams.append(("web_provider", provider_search_candidates))
            streams.append(("generic_static", static_search_candidates))

    generated_count_by_stage = _init_stage_counter()
    survived_dedupe_count_by_stage = _init_stage_counter()
    probed_count_by_stage = _init_stage_counter()
    queued_count_by_stage = _init_stage_counter()
    duplicate_reasons: Counter[str] = Counter()

    discovered = merge_candidate_streams(streams)
    for row in discovered:
        generated_count_by_stage[str(row.get("discoveryStage") or "provider_pattern")] += 1
    found_endpoint_count = len(discovered)
    emit_log(
        "Generated candidates by stage: "
        + ", ".join(f"{stage}={generated_count_by_stage.get(stage, 0)}" for stage in DISCOVERY_STAGES)
        + f" (total={found_endpoint_count})."
    )

    filtered: List[Dict[str, Any]] = []
    skipped_duplicate_count = 0
    local_seen_ids = set(seen_ids)
    local_seen_domains = set(seen_domains)
    for row in discovered:
        stage = str(row.get("discoveryStage") or "provider_pattern")
        row_id = source_identity(row)
        row_domain = adapter_domain_fingerprint(row)
        if row_id in seen_ids:
            skipped_duplicate_count += 1
            duplicate_reasons["existing_id"] += 1
            continue
        if row_domain and row_domain in seen_domains:
            skipped_duplicate_count += 1
            duplicate_reasons["existing_domain"] += 1
            continue
        if row_id in local_seen_ids:
            skipped_duplicate_count += 1
            duplicate_reasons["run_id"] += 1
            continue
        if row_domain and row_domain in local_seen_domains:
            skipped_duplicate_count += 1
            duplicate_reasons["run_domain"] += 1
            continue
        local_seen_ids.add(row_id)
        if row_domain:
            local_seen_domains.add(row_domain)
        survived_dedupe_count_by_stage[stage] += 1
        filtered.append(row)

    filtered.sort(key=estimate_probe_priority, reverse=True)
    emit_log(
        "After dedupe: "
        + ", ".join(f"{stage}={survived_dedupe_count_by_stage.get(stage, 0)}" for stage in DISCOVERY_STAGES)
        + f"; skipped_duplicates={skipped_duplicate_count}."
    )

    queueable_candidates: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = list(web_failures)
    healthy = 0
    probed = 0
    adapter_counter: Counter[str] = Counter()
    method_counter: Counter[str] = Counter()
    skipped_invalid = 0
    skipped_low_evidence_probe_count = 0
    processed_count = 0
    low_evidence_probes_used = 0

    def build_summary(current_candidates: List[Dict[str, Any]], deferred_candidates: int = 0, deferred_counts: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
        return {
            "probedCount": probed,
            "healthyCount": healthy,
            "newCandidateCount": len(current_candidates),
            "taEnvCandidateCount": sum(1 for row in current_candidates if "target_role_signal" in row.get("reasons", [])),
            "nlCandidateCount": sum(1 for row in current_candidates if bool(row.get("nlPriority"))),
            "remoteCandidateCount": sum(1 for row in current_candidates if bool(row.get("remoteFriendly"))),
            "failedProbeCount": len([row for row in failures if str(row.get("stage")) == "probe"]),
            "probeMissCount": len([row for row in failures if str(row.get("stage")) == "probe_miss"]),
            "foundEndpointCount": found_endpoint_count,
            "probedCandidateCount": probed,
            "queuedCandidateCount": len([row for row in current_candidates if not bool(row.get("deferred"))]),
            "discoverableButDeferredCount": int(deferred_candidates),
            "skippedDuplicateCount": skipped_duplicate_count,
            "skippedInvalidCount": skipped_invalid,
            "skippedLowEvidenceProbeCount": skipped_low_evidence_probe_count,
            "adapterCounts": dict(adapter_counter),
            "methodCounts": dict(method_counter),
            "generatedCountByStage": dict(generated_count_by_stage),
            "survivedDedupeCountByStage": dict(survived_dedupe_count_by_stage),
            "probedCountByStage": dict(probed_count_by_stage),
            "queuedCountByStage": dict(queued_count_by_stage),
            "duplicateReasons": dict(duplicate_reasons),
            "deferredReasons": dict(deferred_counts or {}),
        }

    def write_progress_report(current_candidates: List[Dict[str, Any]]) -> None:
        save_json_atomic(
            DISCOVERY_REPORT_PATH,
            {
                "schemaVersion": SCHEMA_VERSION,
                "mode": mode,
                "startedAt": started_at,
                "finishedAt": "",
                "summary": build_summary(current_candidates),
                "candidates": current_candidates,
                "failures": failures,
                "topFailures": [],
                "outputs": {
                    "report": str(DISCOVERY_REPORT_PATH),
                    "candidates": str(DISCOVERY_CANDIDATES_PATH),
                    "pending": str(PENDING_PATH),
                },
            },
        )

    write_progress_report([])
    emit_log(f"Starting probe phase for {len(filtered)} candidate(s).")
    for raw in filtered:
        processed_count += 1
        stage = str(raw.get("discoveryStage") or "provider_pattern")
        valid, invalid_reason = validate_candidate_for_probe(raw)
        if not valid:
            skipped_invalid += 1
            failures.append({"name": raw.get("name"), "adapter": raw.get("adapter"), "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(), "error": invalid_reason, "stage": "validation"})
            if processed_count % 5 == 0:
                write_progress_report(queueable_candidates)
            continue
        evidence_score = int(raw.get("evidenceScore") or 0)
        threshold = _evidence_threshold_for_probe(raw)
        if evidence_score < threshold:
            if stage == "provider_pattern":
                skipped_low_evidence_probe_count += 1
                failures.append({"name": raw.get("name"), "adapter": raw.get("adapter"), "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(), "error": f"pattern evidence score {evidence_score} below probe threshold {threshold}", "stage": "probe_skipped"})
                if processed_count % 5 == 0:
                    write_progress_report(queueable_candidates)
                continue
            if low_evidence_probes_used >= LOW_EVIDENCE_PROBE_LIMIT:
                skipped_low_evidence_probe_count += 1
                failures.append({"name": raw.get("name"), "adapter": raw.get("adapter"), "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(), "error": f"evidence score {evidence_score} below probe threshold {threshold}", "stage": "probe_skipped"})
                if processed_count % 5 == 0:
                    write_progress_report(queueable_candidates)
                continue
            low_evidence_probes_used += 1
        probed += 1
        probed_count_by_stage[stage] += 1
        ok, jobs_found, error = probe_candidate(raw, timeout_s, fetcher=fetcher)
        if not ok:
            failures.append({"name": raw.get("name"), "adapter": raw.get("adapter"), "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(), "error": error, "stage": classify_probe_failure_stage(error)})
            if processed_count % 5 == 0:
                write_progress_report(queueable_candidates)
            continue
        if not _should_queue_candidate(raw, jobs_found):
            failures.append({"name": raw.get("name"), "adapter": raw.get("adapter"), "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(), "error": f"candidate passed probe but evidence {evidence_score} is below queue threshold", "stage": "queue_filtered"})
            if processed_count % 5 == 0:
                write_progress_report(queueable_candidates)
            continue
        healthy += 1
        score, reasons = compute_candidate_score(raw, jobs_found)
        normalized = normalize_candidate(raw, score, reasons, jobs_found, probed_at=now_iso())
        queueable_candidates.append(normalized)
        adapter_counter[str(normalized.get("adapter") or "unknown")] += 1
        method_counter[str(normalized.get("discoveryMethod") or "unknown")] += 1
        if processed_count % 5 == 0:
            emit_log(
                f"Progress: processed={processed_count}/{len(filtered)}, probed={probed}, queued={len(queueable_candidates)}, "
                f"probe_misses={len([row for row in failures if str(row.get('stage')) == 'probe_miss'])}, "
                f"skipped_low_evidence={skipped_low_evidence_probe_count}."
            )
            write_progress_report(queueable_candidates)

    queued_candidates, report_candidates, deferred_reason_counts = apply_queue_balancing(queueable_candidates, top_n)
    for row in queued_candidates:
        queued_count_by_stage[str(row.get("discoveryStage") or "provider_pattern")] += 1
    emit_log(
        f"Probe phase finished: healthy={healthy}, queued={len(queued_candidates)}, "
        f"deferred={len([row for row in report_candidates if bool(row.get('deferred'))])}, probe_misses={len([row for row in failures if str(row.get('stage')) == 'probe_miss'])}."
    )

    save_json_atomic(PENDING_PATH, unique_sources([*pending_existing, *queued_candidates]))
    save_json_atomic(DISCOVERY_CANDIDATES_PATH, queued_candidates)

    summary = build_summary(report_candidates, deferred_candidates=len([row for row in report_candidates if bool(row.get("deferred"))]), deferred_counts=deferred_reason_counts)
    failure_counter: Counter[str] = Counter()
    for row in failures:
        adapter = str(row.get("adapter") or "unknown")
        domain = str(row.get("domain") or "").strip()
        failure_counter[f"{adapter}:{domain}" if domain else adapter] += 1

    report = {
        "schemaVersion": SCHEMA_VERSION,
        "mode": mode,
        "startedAt": started_at,
        "finishedAt": now_iso(),
        "summary": summary,
        "candidates": report_candidates,
        "failures": failures,
        "topFailures": [{"key": key, "count": count} for key, count in failure_counter.most_common(5)],
        "outputs": {
            "report": str(DISCOVERY_REPORT_PATH),
            "candidates": str(DISCOVERY_CANDIDATES_PATH),
            "pending": str(PENDING_PATH),
        },
    }
    save_json_atomic(DISCOVERY_REPORT_PATH, report)
    emit_log(f"Discovery report written to {DISCOVERY_REPORT_PATH}.")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover new job source candidates.")
    parser.add_argument("--timeout", type=int, default=12)
    parser.add_argument("--top", type=int, default=0, help="Limit new candidates written this run; 0 = no limit.")
    parser.add_argument("--mode", choices=("dynamic", "static"), default="dynamic")
    parser.add_argument("--no-web-search", action="store_true", help="Disable lightweight web search phase.")
    parser.add_argument("--gamesmap-website-only-fallback", action="store_true", help="Manual-only mode: include Gamesmap homepage-only candidates in this run.")
    parser.add_argument("--gamesmap-max-detail-pages", type=int, default=0, help="Optional Gamesmap crawl cap override for this run; 0 = config default.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    discovery_config = load_discovery_config()
    if bool(args.gamesmap_website_only_fallback):
        gamesmap_cfg = dict(discovery_config.get("gamesmap") or {})
        gamesmap_cfg["websiteOnlyFallback"] = True
        gamesmap_cfg["websiteOnlyManualOnly"] = True
        discovery_config["gamesmap"] = gamesmap_cfg
    if int(args.gamesmap_max_detail_pages or 0) > 0:
        gamesmap_cfg = dict(discovery_config.get("gamesmap") or {})
        gamesmap_cfg["maxDetailPages"] = int(args.gamesmap_max_detail_pages)
        discovery_config["gamesmap"] = gamesmap_cfg
    report = run_discovery(
        timeout_s=args.timeout,
        top_n=args.top,
        mode=args.mode,
        include_web_search=not bool(args.no_web_search),
        discovery_config=discovery_config,
    )
    emit_log(
        "Source discovery completed. "
        f"Found endpoints: {report['summary']['foundEndpointCount']}. "
        f"Queued candidates: {report['summary']['queuedCandidateCount']}. "
        f"Deferred candidates: {report['summary'].get('discoverableButDeferredCount', 0)}. "
        f"Failed probes: {report['summary'].get('failedProbeCount', 0)}. "
        f"Probe misses: {report['summary'].get('probeMissCount', 0)}. "
        f"Report: {report['outputs']['report']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
