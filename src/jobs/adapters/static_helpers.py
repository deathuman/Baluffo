from __future__ import annotations

import hashlib
import os
import re
import threading
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Tuple
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import parse_jobpostings_from_html, strip_html_text
from src.jobs.common import config as common_config
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, norm_text, normalize_url


@dataclass(frozen=True)
class StaticSourceRuntimeConfig:
    static_profile: str
    static_detail_concurrency: int
    static_source_time_budget_s: int
    default_path_tokens: List[str]
    default_query_keys: List[str]


def build_static_source_runtime_config(static_detail_concurrency: int) -> StaticSourceRuntimeConfig:
    static_profile = norm_text(os.getenv("BALUFFO_STATIC_DETAIL_HEURISTICS_PROFILE")) or common_config.DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE
    detail_concurrency = max(1, int(static_detail_concurrency or common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY))
    default_path_tokens = ["/job/", "/jobs/", "/jobdetail/"]
    default_query_keys = ["job_id"]
    if static_profile == "broad":
        default_path_tokens.extend(["/career/", "/careers/", "/position/", "/positions/"])
        default_query_keys.extend(["gh_jid", "jid", "jobid"])
    return StaticSourceRuntimeConfig(
        static_profile=static_profile,
        static_detail_concurrency=detail_concurrency,
        static_source_time_budget_s=max(5, int(os.getenv("BALUFFO_STATIC_SOURCE_TIME_BUDGET_S") or 25)),
        default_path_tokens=default_path_tokens,
        default_query_keys=default_query_keys,
    )


def build_static_entry_report(*, source: Dict[str, Any], source_name: str, pages: List[str], company: str) -> Dict[str, Any]:
    return {
        "adapter": "static",
        "studio": clean_text(source.get("studio")) or company or source_name,
        "name": source_name,
        "sourceId": clean_text(source.get("id")),
        "pages": list(pages),
        "status": "ok",
        "fetchedCount": len(pages),
        "keptCount": 0,
        "error": "",
        "loss": {
            "staticNonJobUrlRejected": 0,
            "staticDuplicateLinkRejected": 0,
            "staticDetailParseEmpty": 0,
        },
        "stats": {
            "candidate_links_found": 0,
            "detail_pages_visited": 0,
            "jobs_emitted": 0,
            "fetch_cache_hits": 0,
            "detail_yield_percent": 0,
            "listing_fetch_ms": 0,
            "candidate_extraction_ms": 0,
            "detail_fetch_ms": 0,
        },
    }


def source_detail_concurrency_for(
    source_key: str,
    *,
    source_state_rows: Dict[str, Dict[str, Any]] | None,
    static_detail_concurrency: int,
) -> int:
    entry = (source_state_rows or {}).get(source_key) if isinstance(source_state_rows, dict) else {}
    if not isinstance(entry, dict):
        return static_detail_concurrency
    pages_visited = int(entry.get("lastDetailPagesVisited") or 0)
    duration_ms = int(entry.get("lastDurationMs") or 0)
    if pages_visited >= 40 or duration_ms >= 15_000:
        return max(static_detail_concurrency, 8)
    return static_detail_concurrency


def create_fetch_html_cached(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> Callable[[str], tuple[str, bool]]:
    fetch_cache: Dict[str, str] = {}
    fetch_cache_lock = threading.Lock()

    def fetch_html_cached(url: str, *, remaining_budget_s: float | None = None) -> Tuple[str, bool]:
        normalized = normalize_url(url) or clean_text(url)
        if not normalized:
            return "", False
        with fetch_cache_lock:
            cached = fetch_cache.get(normalized)
        if cached is not None:
            return cached, True
        fetch_url = clean_text(url) or normalized
        effective_timeout_s = int(timeout_s or 1)
        effective_retries = int(retries or 0)
        if remaining_budget_s is not None:
            remaining = float(max(0.0, remaining_budget_s))
            effective_timeout_s = max(3, min(effective_timeout_s, int(remaining)))
            if remaining <= float(effective_timeout_s) * float(max(1, effective_retries + 1)):
                effective_retries = 0
        text = fetch_with_retries(fetch_url, fetch_text, effective_timeout_s, effective_retries, backoff_s)
        with fetch_cache_lock:
            fetch_cache[normalized] = text
        return text, False

    return fetch_html_cached


def is_probable_job_detail_url(
    candidate_url: str,
    source_row: Dict[str, Any],
    *,
    default_path_tokens: List[str],
    default_query_keys: List[str],
) -> bool:
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


def add_detail_link(
    detail_links: List[Tuple[str, str]],
    detail_seen: set[str],
    seen_links: set[str],
    link_rejections: Counter[str],
    *,
    candidate_url: str,
    anchor_text: str,
    enforce_heuristics: bool,
    page_url: str,
    source: Dict[str, Any],
    default_path_tokens: List[str],
    default_query_keys: List[str],
) -> None:
    absolute = normalize_url(urljoin(page_url, clean_text(candidate_url)))
    if not absolute:
        link_rejections["non_job_url"] += 1
        return
    if enforce_heuristics and not is_probable_job_detail_url(
        absolute,
        source,
        default_path_tokens=default_path_tokens,
        default_query_keys=default_query_keys,
    ):
        link_rejections["non_job_url"] += 1
        return
    if absolute in detail_seen or absolute in seen_links:
        link_rejections["duplicate_link"] += 1
        return
    detail_seen.add(absolute)
    detail_links.append((absolute, clean_text(anchor_text)))


def process_detail_link(
    *,
    detail: str,
    detail_title: str,
    source_started: float,
    static_source_time_budget_s: int,
    fetch_html_cached: Callable[..., Tuple[str, bool]],
    timeout_s: int,
    company: str,
    source_name: str,
    source: Dict[str, Any],
    ignored_link_titles: set[str],
) -> Dict[str, Any]:
    fetch_started = time.perf_counter()
    remaining_budget_s = float(static_source_time_budget_s) - float(time.perf_counter() - source_started)
    detail_html, cache_hit = fetch_html_cached(detail, remaining_budget_s=remaining_budget_s)
    fetch_ms = int((time.perf_counter() - fetch_started) * 1000)
    parse_started = time.perf_counter()
    detail_jobs = parse_jobpostings_from_html(
        detail_html,
        base_url=detail,
        fallback_company=company,
        fallback_source_id_prefix=f"static:{source_name}",
    )
    parse_ms = int((time.perf_counter() - parse_started) * 1000)

    rows: List[RawJob] = []
    parse_empty = False
    if detail_jobs:
        for row in detail_jobs:
            row["adapter"] = "static"
            row["studio"] = clean_text(source.get("studio")) or company or source_name
            rows.append(row)
    else:
        parse_empty = True
        path_parts = [part for part in urlparse(detail).path.rstrip("/").split("/") if part]
        slug = path_parts[-1] if path_parts else ""
        if slug.lower() == "apply" and len(path_parts) >= 2:
            slug = path_parts[-2]
        slug = re.sub(r"_[Rr]\d+(?:-\d+)?$", "", slug)
        title = strip_html_text(re.sub(r"[-_]+", " ", slug))
        parsed_title = clean_text(detail_title)
        if parsed_title and parsed_title.lower() not in ignored_link_titles:
            title = parsed_title
        if title and not re.fullmatch(r"\d+", title):
            rows.append(
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
    return {
        "rows": rows,
        "parseEmpty": parse_empty,
        "fetchMs": fetch_ms,
        "parseMs": parse_ms,
        "cacheHit": cache_hit,
    }
