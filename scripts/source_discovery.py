#!/usr/bin/env python3
"""Discover candidate game-dev job sources and queue them for approval."""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError
from urllib.parse import parse_qs, quote_plus, unquote, urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.source_registry import (
    ACTIVE_PATH,
    DISCOVERY_CANDIDATES_PATH,
    DISCOVERY_REPORT_PATH,
    PENDING_PATH,
    REJECTED_PATH,
    load_json_array,
    save_json_atomic,
    source_identity,
    unique_sources,
)

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

STUDIO_SEEDS: List[Dict[str, Any]] = [
    {"studio": "Guerrilla Games", "aliases": ["guerrilla-games", "guerrillagames"], "nlPriority": True, "remoteFriendly": True},
    {"studio": "Nixxes", "aliases": ["nixxes"], "nlPriority": True, "remoteFriendly": True},
    {"studio": "Vertigo Games", "aliases": ["vertigo-games", "vertigogames"], "nlPriority": True, "remoteFriendly": True},
    {"studio": "Triumph Studios", "aliases": ["triumph-studios", "triumphstudios"], "nlPriority": True, "remoteFriendly": True},
    {"studio": "Little Chicken", "aliases": ["littlechicken", "little-chicken"], "nlPriority": True, "remoteFriendly": True},
    {"studio": "PlayStation Global", "aliases": ["sonyinteractiveentertainmentglobal", "playstation", "sony-interactive-entertainment"], "nlPriority": True, "remoteFriendly": True},
    {"studio": "Larian Studios", "aliases": ["larian-studios", "larianstudios"], "nlPriority": True, "remoteFriendly": True},
    {"studio": "Jagex", "aliases": ["jagex"], "nlPriority": False, "remoteFriendly": True},
    {"studio": "Remedy Entertainment", "aliases": ["remedy-entertainment", "remedyentertainment"], "nlPriority": False, "remoteFriendly": True},
    {"studio": "Supercell", "aliases": ["supercell"], "nlPriority": False, "remoteFriendly": True},
]

FOCUS_KEYWORDS = ("technical artist", "tech artist", "environment artist", "environment art", "world artist", "terrain artist")
DUCKDUCKGO_HTML_SEARCH = "https://duckduckgo.com/html/?q={query}"
WEB_SEARCH_QUERY_SUFFIX = ("careers", "jobs")
FETCH_MAX_RETRIES = 2
RETRY_BACKOFF_SECONDS = 1.2
RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


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
    message = str(exc)
    match = re.search(r"\bHTTP Error (\d{3})\b", message)
    if match:
        return int(match.group(1))
    return None


def _is_retryable_error(exc: Exception) -> bool:
    code = _http_code_from_error(exc)
    if code in RETRYABLE_HTTP_CODES:
        return True
    message = str(exc).lower()
    return "timed out" in message or "temporary failure" in message


def fetch_text_with_retry(
    url: str,
    timeout_s: int,
    *,
    adapter: str,
    fetcher=fetch_text,
) -> str:
    # Pace 429-prone providers to reduce burst failures during dynamic scans.
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
    if len(value) < 3:
        return False
    if not re.match(r"^[A-Za-z0-9][A-Za-z0-9_-]*$", value):
        return False
    if value.isdigit():
        return False
    return True


def validate_candidate_for_probe(candidate: Dict[str, Any]) -> Tuple[bool, str]:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    if adapter in {"lever", "workable"}:
        token = str(candidate.get("account") or "").strip()
        if not _is_valid_identity_token(token):
            return False, "invalid account token"
        return True, ""
    if adapter == "greenhouse":
        slug = str(candidate.get("slug") or "").strip()
        if not _is_valid_identity_token(slug):
            return False, "invalid board slug"
        return True, ""
    if adapter == "smartrecruiters":
        company_id = str(candidate.get("company_id") or "").strip()
        if len(company_id) < 3 or not re.search(r"[A-Za-z]", company_id):
            return False, "invalid company identifier"
        return True, ""
    if adapter == "personio":
        url = str(candidate.get("feed_url") or "").strip()
        host = (urlparse(url).netloc or "").lower()
        if ".jobs.personio.de" not in host:
            return False, "invalid personio host"
        return True, ""
    if adapter == "teamtailor":
        url = str(candidate.get("listing_url") or "").strip()
        host = (urlparse(url).netloc or "").lower()
        if ".teamtailor.com" not in host:
            return False, "invalid teamtailor host"
        return True, ""
    if adapter == "ashby":
        url = str(candidate.get("board_url") or "").strip()
        host = (urlparse(url).netloc or "").lower()
        if "ashbyhq.com" not in host:
            return False, "invalid ashby host"
        return True, ""
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
        links = re.findall(r'(?is)href=["\'][^"\']+/jobs/\d+[^"\']*["\']', text)
        return len(set(links))
    if adapter == "smartrecruiters":
        if text.strip().startswith("{"):
            payload = json.loads(text)
            return len(payload.get("content", [])) if isinstance(payload, dict) else 0
        links = re.findall(r'(?is)href=["\'][^"\']+/job/[^"\']+["\']', text)
        return len(set(links))
    if adapter == "workable":
        if text.strip().startswith("{"):
            payload = json.loads(text)
            return len(payload.get("jobs", [])) if isinstance(payload, dict) else 0
        links = re.findall(r'(?is)href=["\'][^"\']+/j/[^"\']+["\']', text)
        return len(set(links))
    if adapter == "personio":
        if text.lstrip().startswith("<"):
            root = ET.fromstring(text)
            return len(root.findall(".//position"))
        return 0
    if adapter == "ashby":
        links = re.findall(r'(?is)<a[^>]+href=["\']([^"\']+/job/[^"\']+)["\']', text)
        return len(set(links))
    if adapter == "teamtailor":
        links = re.findall(r'(?is)<a[^>]+href=["\']([^"\']+/jobs/[^"\']+)["\']', text)
        return len(set(links))
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
            count = parse_probe_count(adapter, text)
            return True, max(0, int(count)), ""
        except Exception as exc:  # noqa: BLE001
            last_error = f"{probe_url}: {exc}"
            continue
    return False, 0, last_error


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
    if jobs_found > 0:
        score += min(25, jobs_found)
        reasons.append("live_jobs_detected")
    return min(100, score), reasons


def compute_confidence(candidate: Dict[str, Any], jobs_found: int) -> str:
    method = str(candidate.get("discoveryMethod") or "seed").lower()
    if jobs_found >= 10:
        return "high"
    if jobs_found >= 1:
        return "medium" if method == "web_search" else "high"
    return "low"


def normalize_candidate(
    candidate: Dict[str, Any],
    score: int,
    reasons: List[str],
    jobs_found: int,
    *,
    probed_at: str,
) -> Dict[str, Any]:
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
    return row


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


def build_pattern_candidates() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for seed in STUDIO_SEEDS:
        studio = str(seed.get("studio") or "").strip()
        if not studio:
            continue
        nl_priority = bool(seed.get("nlPriority"))
        remote_friendly = bool(seed.get("remoteFriendly"))
        for alias in expand_aliases(seed):
            display = studio
            rows.extend(
                [
                    {
                        "name": f"{display} (Lever)",
                        "studio": display,
                        "adapter": "lever",
                        "account": alias,
                        "api_url": f"https://api.lever.co/v0/postings/{alias}?mode=json",
                        "remoteFriendly": remote_friendly,
                        "nlPriority": nl_priority,
                        "discoveryMethod": "pattern",
                    },
                    {
                        "name": f"{display} (Greenhouse)",
                        "studio": display,
                        "adapter": "greenhouse",
                        "slug": alias,
                        "api_url": f"https://boards-api.greenhouse.io/v1/boards/{alias}/jobs?content=true",
                        "remoteFriendly": remote_friendly,
                        "nlPriority": nl_priority,
                        "discoveryMethod": "pattern",
                    },
                    {
                        "name": f"{display} (SmartRecruiters)",
                        "studio": display,
                        "adapter": "smartrecruiters",
                        "company_id": alias.upper(),
                        "api_url": f"https://api.smartrecruiters.com/v1/companies/{alias.upper()}/postings",
                        "remoteFriendly": remote_friendly,
                        "nlPriority": nl_priority,
                        "discoveryMethod": "pattern",
                    },
                    {
                        "name": f"{display} (Workable)",
                        "studio": display,
                        "adapter": "workable",
                        "account": alias,
                        "api_url": f"https://apply.workable.com/api/v1/widget/accounts/{alias}?details=true",
                        "remoteFriendly": remote_friendly,
                        "nlPriority": nl_priority,
                        "discoveryMethod": "pattern",
                    },
                    {
                        "name": f"{display} (Teamtailor)",
                        "studio": display,
                        "adapter": "teamtailor",
                        "company": display,
                        "listing_url": f"https://{alias}.teamtailor.com/jobs",
                        "base_url": f"https://{alias}.teamtailor.com",
                        "remoteFriendly": remote_friendly,
                        "nlPriority": nl_priority,
                        "discoveryMethod": "pattern",
                    },
                    {
                        "name": f"{display} (Ashby)",
                        "studio": display,
                        "adapter": "ashby",
                        "board_url": f"https://jobs.ashbyhq.com/{alias}/jobs",
                        "remoteFriendly": remote_friendly,
                        "nlPriority": nl_priority,
                        "discoveryMethod": "pattern",
                    },
                    {
                        "name": f"{display} (Personio)",
                        "studio": display,
                        "adapter": "personio",
                        "feed_url": f"https://{alias}.jobs.personio.de/xml",
                        "remoteFriendly": remote_friendly,
                        "nlPriority": nl_priority,
                        "discoveryMethod": "pattern",
                    },
                ]
            )
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


def infer_web_candidate(url: str, studio: str, *, nl_priority: bool, remote_friendly: bool) -> Optional[Dict[str, Any]]:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    if "boards.greenhouse.io" in host or "jobs.greenhouse.io" in host:
        parts = [p for p in path.split("/") if p]
        if not parts:
            return None
        slug = clean_token(parts[0])
        if not slug:
            return None
        return {
            "name": f"{studio} (Greenhouse)",
            "studio": studio,
            "adapter": "greenhouse",
            "slug": slug,
            "api_url": f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
            "remoteFriendly": remote_friendly,
            "nlPriority": nl_priority,
            "discoveryMethod": "web_search",
        }
    if "jobs.ashbyhq.com" in host:
        parts = [p for p in path.split("/") if p]
        if not parts:
            return None
        slug = clean_token(parts[0])
        if not slug:
            return None
        return {
            "name": f"{studio} (Ashby)",
            "studio": studio,
            "adapter": "ashby",
            "board_url": f"https://jobs.ashbyhq.com/{slug}/jobs",
            "remoteFriendly": remote_friendly,
            "nlPriority": nl_priority,
            "discoveryMethod": "web_search",
        }
    if "apply.workable.com" in host:
        parts = [p for p in path.split("/") if p]
        account = clean_token(parts[-1]) if parts else ""
        if not account:
            return None
        return {
            "name": f"{studio} (Workable)",
            "studio": studio,
            "adapter": "workable",
            "account": account,
            "api_url": f"https://apply.workable.com/api/v1/widget/accounts/{account}?details=true",
            "remoteFriendly": remote_friendly,
            "nlPriority": nl_priority,
            "discoveryMethod": "web_search",
        }
    if ".teamtailor.com" in host:
        base_url = f"{parsed.scheme}://{host}" if parsed.scheme else f"https://{host}"
        return {
            "name": f"{studio} (Teamtailor)",
            "studio": studio,
            "adapter": "teamtailor",
            "listing_url": f"{base_url}/jobs",
            "base_url": base_url,
            "company": studio,
            "remoteFriendly": remote_friendly,
            "nlPriority": nl_priority,
            "discoveryMethod": "web_search",
        }
    if ".jobs.personio.de" in host:
        token = host.split(".jobs.personio.de", 1)[0]
        if not token:
            return None
        return {
            "name": f"{studio} (Personio)",
            "studio": studio,
            "adapter": "personio",
            "feed_url": f"https://{token}.jobs.personio.de/xml",
            "remoteFriendly": remote_friendly,
            "nlPriority": nl_priority,
            "discoveryMethod": "web_search",
        }
    if "api.lever.co" in host and "/v0/postings/" in path:
        account = clean_token(path.split("/v0/postings/", 1)[1].split("/", 1)[0])
        if not account:
            return None
        return {
            "name": f"{studio} (Lever)",
            "studio": studio,
            "adapter": "lever",
            "account": account,
            "api_url": f"https://api.lever.co/v0/postings/{account}?mode=json",
            "remoteFriendly": remote_friendly,
            "nlPriority": nl_priority,
            "discoveryMethod": "web_search",
        }
    if "api.smartrecruiters.com" in host and "/companies/" in path:
        pieces = [p for p in path.split("/") if p]
        if "companies" not in pieces:
            return None
        idx = pieces.index("companies")
        if idx + 1 >= len(pieces):
            return None
        company_id = pieces[idx + 1].strip()
        if not company_id:
            return None
        return {
            "name": f"{studio} (SmartRecruiters)",
            "studio": studio,
            "adapter": "smartrecruiters",
            "company_id": company_id,
            "api_url": f"https://api.smartrecruiters.com/v1/companies/{company_id}/postings",
            "remoteFriendly": remote_friendly,
            "nlPriority": nl_priority,
            "discoveryMethod": "web_search",
        }
    return None


def build_web_search_queries(max_queries: int = 12) -> List[Tuple[str, Dict[str, Any]]]:
    queries: List[Tuple[str, Dict[str, Any]]] = []
    for seed in STUDIO_SEEDS:
        studio = str(seed.get("studio") or "").strip()
        if not studio:
            continue
        for suffix in WEB_SEARCH_QUERY_SUFFIX:
            query = f"{studio} {suffix} game studio"
            queries.append((query, seed))
        if len(queries) >= max_queries:
            break
    return queries[:max_queries]


def discover_web_search_candidates(timeout_s: int, *, fetcher=fetch_text, max_queries: int = 12) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    candidates: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for query, seed in build_web_search_queries(max_queries=max_queries):
        url = DUCKDUCKGO_HTML_SEARCH.format(query=quote_plus(query))
        try:
            html = fetcher(url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            failures.append({"name": query, "adapter": "web_search", "error": str(exc), "stage": "search"})
            continue
        links = extract_links_from_html(html)
        studio = str(seed.get("studio") or "")
        for link in links:
            inferred = infer_web_candidate(
                link,
                studio,
                nl_priority=bool(seed.get("nlPriority")),
                remote_friendly=bool(seed.get("remoteFriendly")),
            )
            if inferred:
                candidates.append(inferred)
    return unique_sources(candidates), failures


def merge_candidate_streams(streams: Iterable[Tuple[str, List[Dict[str, Any]]]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for method, items in streams:
        for raw in items:
            if not isinstance(raw, dict):
                continue
            row = dict(raw)
            row["discoveryMethod"] = str(row.get("discoveryMethod") or method)
            row["discoveredAt"] = str(row.get("discoveredAt") or now_iso())
            rows.append(row)
    return rows


def run_discovery(
    timeout_s: int,
    top_n: int,
    *,
    mode: str = "dynamic",
    include_web_search: bool = True,
    fetcher=fetch_text,
) -> Dict[str, Any]:
    started_at = now_iso()
    active = load_json_array(ACTIVE_PATH, [])
    pending_existing = load_json_array(PENDING_PATH, [])
    rejected = load_json_array(REJECTED_PATH, [])

    existing_rows = [*active, *pending_existing, *rejected]
    seen_ids = {source_identity(row) for row in existing_rows if isinstance(row, dict)}
    seen_domains = {fp for fp in (adapter_domain_fingerprint(row) for row in existing_rows if isinstance(row, dict)) if fp}

    web_candidates: List[Dict[str, Any]] = []
    web_failures: List[Dict[str, Any]] = []

    streams: List[Tuple[str, List[Dict[str, Any]]]] = [("seed", STATIC_DISCOVERY_CANDIDATES)]
    if mode == "dynamic":
        streams.append(("pattern", build_pattern_candidates()))
        if include_web_search:
            web_candidates, web_failures = discover_web_search_candidates(timeout_s, fetcher=fetcher)
            streams.append(("web_search", web_candidates))

    discovered = merge_candidate_streams(streams)
    found_endpoint_count = len(discovered)

    filtered: List[Dict[str, Any]] = []
    skipped_duplicate_count = 0
    local_seen_ids = set(seen_ids)
    local_seen_domains = set(seen_domains)
    for row in discovered:
        row_id = source_identity(row)
        row_domain = adapter_domain_fingerprint(row)
        if row_id in local_seen_ids or (row_domain and row_domain in local_seen_domains):
            skipped_duplicate_count += 1
            continue
        local_seen_ids.add(row_id)
        if row_domain:
            local_seen_domains.add(row_domain)
        filtered.append(row)

    candidates: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = list(web_failures)
    healthy = 0
    probed = 0
    adapter_counter: Counter[str] = Counter()
    method_counter: Counter[str] = Counter()
    skipped_invalid = 0

    for raw in filtered:
        valid, invalid_reason = validate_candidate_for_probe(raw)
        if not valid:
            skipped_invalid += 1
            failures.append(
                {
                    "name": raw.get("name"),
                    "adapter": raw.get("adapter"),
                    "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(),
                    "error": invalid_reason,
                    "stage": "validation",
                }
            )
            continue
        probed += 1
        ok, jobs_found, error = probe_candidate(raw, timeout_s, fetcher=fetcher)
        if not ok:
            failures.append(
                {
                    "name": raw.get("name"),
                    "adapter": raw.get("adapter"),
                    "domain": (urlparse(endpoint_url(raw)).netloc or "").lower(),
                    "error": error,
                    "stage": "probe",
                }
            )
            continue

        healthy += 1
        score, reasons = compute_candidate_score(raw, jobs_found)
        normalized = normalize_candidate(raw, score, reasons, jobs_found, probed_at=now_iso())
        candidates.append(normalized)
        adapter_counter[str(normalized.get("adapter") or "unknown")] += 1
        method_counter[str(normalized.get("discoveryMethod") or "unknown")] += 1

    candidates.sort(
        key=lambda row: (
            int(row.get("score") or 0),
            int(row.get("jobsFound") or 0),
            str(row.get("name") or ""),
        ),
        reverse=True,
    )
    if top_n > 0:
        candidates = candidates[:top_n]

    merged_pending = unique_sources([*pending_existing, *candidates])
    save_json_atomic(PENDING_PATH, merged_pending)
    save_json_atomic(DISCOVERY_CANDIDATES_PATH, candidates)

    summary = {
        "probedCount": probed,
        "healthyCount": healthy,
        "newCandidateCount": len(candidates),
        "taEnvCandidateCount": sum(1 for row in candidates if "target_role_signal" in row.get("reasons", [])),
        "nlCandidateCount": sum(1 for row in candidates if bool(row.get("nlPriority"))),
        "remoteCandidateCount": sum(1 for row in candidates if bool(row.get("remoteFriendly"))),
        "failedProbeCount": len([row for row in failures if str(row.get("stage")) == "probe"]),
        "foundEndpointCount": found_endpoint_count,
        "probedCandidateCount": probed,
        "queuedCandidateCount": len(candidates),
        "skippedDuplicateCount": skipped_duplicate_count,
        "skippedInvalidCount": skipped_invalid,
        "adapterCounts": dict(adapter_counter),
        "methodCounts": dict(method_counter),
    }

    failure_counter: Counter[str] = Counter()
    for row in failures:
        adapter = str(row.get("adapter") or "unknown")
        domain = str(row.get("domain") or "").strip()
        key = f"{adapter}:{domain}" if domain else adapter
        failure_counter[key] += 1

    top_failures = [
        {"key": key, "count": count}
        for key, count in failure_counter.most_common(5)
    ]

    report = {
        "mode": mode,
        "startedAt": started_at,
        "finishedAt": now_iso(),
        "summary": summary,
        "candidates": candidates,
        "failures": failures,
        "topFailures": top_failures,
        "outputs": {
            "report": str(DISCOVERY_REPORT_PATH),
            "candidates": str(DISCOVERY_CANDIDATES_PATH),
            "pending": str(PENDING_PATH),
        },
    }
    save_json_atomic(DISCOVERY_REPORT_PATH, report)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover new job source candidates.")
    parser.add_argument("--timeout", type=int, default=12)
    parser.add_argument("--top", type=int, default=0, help="Limit new candidates written this run; 0 = no limit.")
    parser.add_argument("--mode", choices=("dynamic", "static"), default="dynamic")
    parser.add_argument("--no-web-search", action="store_true", help="Disable lightweight web search phase.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = run_discovery(
        timeout_s=args.timeout,
        top_n=args.top,
        mode=args.mode,
        include_web_search=not bool(args.no_web_search),
    )
    print(
        "Source discovery completed. "
        f"Found endpoints: {report['summary']['foundEndpointCount']}. "
        f"Queued candidates: {report['summary']['queuedCandidateCount']}. "
        f"Failures: {len(report.get('failures', []))}. "
        f"Report: {report['outputs']['report']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
