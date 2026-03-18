"""Static and scrapy adapters."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from src.exceptions import AdapterValidationError
from src.jobs.common import config as common_config
from src.jobs.adapters.html_parsers import (
    maybe_fetch_kojima_job_listing_html,
    parse_jobpostings_from_html,
    strip_html_text,
)
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.errors import NoPluginFoundError
from src.jobs.adapters.plugins.static import register_static_plugins
from src.jobs.adapters.plugins.static._heuristics import detect_js_shell
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.static_scrapy import run_scrapy_static_source
from src.jobs.common import registry_entries, set_source_diagnostics
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.interfaces import SourceLoader
from src.shared.regex import find_urls_in_text
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, norm_text, normalize_url

register_static_plugins()


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
    static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: Optional[Dict[str, Dict[str, Any]]] = None,
    try_playwright: Optional[Callable[[str, int], Tuple[str, str]]] = None,
) -> List[RawJob]:
    jobs: List[RawJob] = []
    errors: List[str] = []
    warnings: List[str] = []
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

    static_profile = norm_text(os.getenv("BALUFFO_STATIC_DETAIL_HEURISTICS_PROFILE")) or common_config.DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE
    static_detail_concurrency = max(1, int(static_detail_concurrency or common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY))
    default_path_tokens = ["/job/", "/jobs/", "/jobdetail/"]
    default_query_keys = ["job_id"]
    if static_profile == "broad":
        default_path_tokens.extend(["/career/", "/careers/", "/position/", "/positions/"])
        default_query_keys.extend(["gh_jid", "jid", "jobid"])

    def source_detail_concurrency_for(source_key: str) -> int:
        entry = (source_state_rows or {}).get(source_key) if isinstance(source_state_rows, dict) else {}
        if not isinstance(entry, dict):
            return static_detail_concurrency
        pages_visited = int(entry.get("lastDetailPagesVisited") or 0)
        duration_ms = int(entry.get("lastDurationMs") or 0)
        if pages_visited >= 40 or duration_ms >= 15_000:
            return max(static_detail_concurrency, 8)
        return static_detail_concurrency

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
            # Clamp request timeout so we don't exceed the per-source budget on one call.
            remaining = float(max(0.0, remaining_budget_s))
            effective_timeout_s = max(3, min(effective_timeout_s, int(remaining)))
            # If retries could exceed the remaining budget, disable them.
            if remaining <= float(effective_timeout_s) * float(max(1, effective_retries + 1)):
                effective_retries = 0
        text = fetch_with_retries(fetch_url, fetch_text, effective_timeout_s, effective_retries, backoff_s)
        with fetch_cache_lock:
            fetch_cache[normalized] = text
        return text, False

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

    if isinstance(sources, list):
        selected_sources = sources
    else:
        # Use the jobs_fetcher facade so tests (and admin ops) can override
        # `STUDIO_SOURCE_REGISTRY` via `src.jobs_fetcher.STUDIO_SOURCE_REGISTRY`.
        try:
            from src import jobs_fetcher as jobs_fetcher_pkg

            selected_sources = jobs_fetcher_pkg.registry_entries("static", enabled_only=True)
        except Exception:  # noqa: BLE001
            selected_sources = registry_entries("static")
    static_source_time_budget_s = max(5, int(os.getenv("BALUFFO_STATIC_SOURCE_TIME_BUDGET_S") or 25))
    for source in selected_sources:
        source_started = time.perf_counter()
        if shard and static_source_shard(source) != shard:
            continue
        source_name = clean_text(source.get("name")) or "static_source"
        company = clean_text(source.get("company")) or source_name
        pages = source.get("pages") if isinstance(source.get("pages"), list) else []
        entry_report = {
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
        kept_before = len(jobs)
        link_rejections: Counter[str] = Counter()
        stats = entry_report["stats"]

        host = ""
        if pages:
            try:
                parsed = urlparse(clean_text(pages[0]) or "")
                host = (parsed.netloc or "").strip().lower()
            except Exception:  # noqa: BLE001
                pass
        ctx = AdapterPluginContext(family="static", adapter_key="static", source_identity=host or source_name)
        try:
            plugin, _ = default_registry.select(ctx)
            plugin_jobs = plugin.run(
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
                pages=pages,
                source_row=source,
                parse_jobpostings_from_html=parse_jobpostings_from_html,
                maybe_fetch_kojima_job_listing_html=maybe_fetch_kojima_job_listing_html,
                try_playwright=try_playwright,
            )
            jobs.extend(plugin_jobs)
            entry_report["fetchedCount"] = len(pages)
            entry_report["keptCount"] = len(plugin_jobs)
            plugin_meta = source.get("_staticPluginMeta") if isinstance(source, dict) else None
            if isinstance(plugin_meta, dict):
                entry_report["classification"] = clean_text(plugin_meta.get("classification"))
                entry_report["browserFallbackRecommended"] = bool(plugin_meta.get("browserFallbackRecommended"))
                entry_report["extractorHint"] = clean_text(plugin_meta.get("extractorHint"))
                if plugin_meta.get("emptyConfirmed"):
                    entry_report["emptyConfirmed"] = True
                ats_links = plugin_meta.get("atsLinks")
                if isinstance(ats_links, list):
                    entry_report["atsLinks"] = [clean_text(v) for v in ats_links if clean_text(v)][:5]
                meta_error = clean_text(plugin_meta.get("error"))
                if meta_error and not entry_report.get("error"):
                    entry_report["error"] = meta_error

            # If plugin extracted nothing, treat as error unless it proved an explicit empty state
            # or a non-fatal browser escalation classification.
            if not plugin_jobs:
                classification = clean_text(entry_report.get("classification"))
                empty_confirmed = bool(entry_report.get("emptyConfirmed")) or classification == "empty_confirmed"
                browser_recommended = bool(entry_report.get("browserFallbackRecommended"))
                if not empty_confirmed:
                    entry_report["status"] = "error"
                    if not entry_report.get("error"):
                        entry_report["error"] = "no jobs extracted from source pages"
                    if not classification:
                        entry_report["classification"] = "fetch_ok_extract_zero"
                    if browser_recommended:
                        warn_page = clean_text(pages[0]) if pages else ""
                        warnings.append(f"static:{source_name}:{warn_page}: {entry_report.get('error')}")
                    else:
                        errors.append(f"static:{source_name}: {entry_report.get('error')}")
                else:
                    entry_report["status"] = "ok"
                    entry_report["error"] = ""
            else:
                entry_report["status"] = "ok"
            details.append(entry_report)
            continue
        except NoPluginFoundError:
            pass

        def add_detail_link(
            detail_links: List[Tuple[str, str]],
            detail_seen: set[str],
            candidate_url: str,
            anchor_text: str,
            *,
            enforce_heuristics: bool,
            page_url: str,
        ) -> None:
            absolute = normalize_url(urljoin(page_url, clean_text(candidate_url)))
            if not absolute:
                link_rejections["non_job_url"] += 1
                return
            if enforce_heuristics and not is_probable_job_detail_url(absolute, source):
                link_rejections["non_job_url"] += 1
                return
            if absolute in detail_seen or absolute in seen_links:
                link_rejections["duplicate_link"] += 1
                return
            detail_seen.add(absolute)
            detail_links.append((absolute, clean_text(anchor_text)))

        def process_detail_link(detail: str, detail_title: str) -> Dict[str, Any]:
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

        for page in pages:
            page_url = clean_text(page)
            if not page_url:
                continue
            if (time.perf_counter() - source_started) > float(static_source_time_budget_s):
                entry_report["status"] = "error"
                entry_report["classification"] = "timeout"
                entry_report["browserFallbackRecommended"] = True
                entry_report["error"] = f"time budget exceeded ({static_source_time_budget_s}s)"
                warnings.append(f"static:{source_name}:{page_url}: time_budget_exceeded")
                break
            try:
                listing_fetch_started = time.perf_counter()
                remaining_budget_s = float(static_source_time_budget_s) - float(time.perf_counter() - source_started)
                effective_timeout_s = max(3, min(int(timeout_s or 1), int(remaining_budget_s or timeout_s)))
                html = ""
                cache_hit = False
                try:
                    html, cache_hit = fetch_html_cached(page_url, remaining_budget_s=remaining_budget_s)
                except Exception as exc:  # noqa: BLE001
                    err_str = str(exc)
                    err_lower = err_str.lower()
                    if try_playwright and ("403" in err_str or "timeout" in err_lower or "timed out" in err_lower):
                        reason = "403" if "403" in err_str else "timeout"
                        html, _ = try_playwright(page_url, effective_timeout_s)
                        print(
                            f"[static] playwright_fallback_used url={page_url!r} reason={reason} got_html={bool(html)}",
                            file=sys.stderr,
                            flush=True,
                        )
                    if not html:
                        raise
                stats["listing_fetch_ms"] += int((time.perf_counter() - listing_fetch_started) * 1000)
                if cache_hit:
                    stats["fetch_cache_hits"] += 1
                if try_playwright and html and detect_js_shell(html):
                    parsed_pre = parse_jobpostings_from_html(
                        html,
                        base_url=page_url,
                        fallback_company=company,
                        fallback_source_id_prefix=f"static:{source_name}",
                    )
                    link_count = len(re.findall(r'(?is)<a[^>]+href=["\']([^"\']+)["\']', html))
                    if not parsed_pre and link_count < 3:
                        html2, _ = try_playwright(page_url, effective_timeout_s)
                        print(
                            f"[static] playwright_fallback_used url={page_url!r} reason=js_shell got_html={bool(html2)}",
                            file=sys.stderr,
                            flush=True,
                        )
                        if html2:
                            html = html2
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

                extraction_started = time.perf_counter()
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
                        add_detail_link(detail_links, detail_seen, href, anchor_text, enforce_heuristics=False, page_url=page_url)

                    for match in re.finditer(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', listing_html):
                        href = clean_text(match.group(1))
                        anchor_inner = match.group(2) or ""
                        anchor_text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", anchor_inner))
                        add_detail_link(detail_links, detail_seen, href, anchor_text, enforce_heuristics=True, page_url=page_url)
                    for raw in find_urls_in_text(listing_html):
                        add_detail_link(detail_links, detail_seen, clean_text(raw), "", enforce_heuristics=True, page_url=page_url)
                stats["candidate_links_found"] += len(detail_links)
                stats["candidate_extraction_ms"] += int((time.perf_counter() - extraction_started) * 1000)

                if not detail_links:
                    continue
                source_key = diagnostics_name if len(selected_sources) == 1 else source_name
                detail_fetch_started = time.perf_counter()
                with ThreadPoolExecutor(max_workers=source_detail_concurrency_for(source_key)) as executor:
                    future_map = {
                        executor.submit(process_detail_link, detail, detail_title): (detail, detail_title)
                        for detail, detail_title in detail_links
                    }
                    for future in as_completed(future_map):
                        detail, _detail_title = future_map[future]
                        stats["detail_pages_visited"] += 1
                        try:
                            detail_result = future.result()
                        except Exception as exc:  # noqa: BLE001
                            msg = str(exc)
                            if "HTTP 403" in msg:
                                entry_report["classification"] = "blocked_or_challenge"
                                entry_report["browserFallbackRecommended"] = True
                                entry_report["error"] = msg
                                warnings.append(f"static:{source_name}:{detail}: {msg}")
                            else:
                                errors.append(f"static:{source_name}:{detail}: {exc}")
                            continue
                        stats["fetch_cache_hits"] += 1 if detail_result.get("cacheHit") else 0
                        stats["detail_fetch_ms"] += int(detail_result.get("fetchMs") or 0)
                        if detail_result.get("parseEmpty"):
                            link_rejections["detail_parse_empty"] += 1
                        for row in detail_result.get("rows") or []:
                            link = normalize_url(row.get("jobLink"))
                            if not link or link in seen_links:
                                continue
                            seen_links.add(link)
                            jobs.append(row)
                stats["detail_fetch_ms"] += max(0, int((time.perf_counter() - detail_fetch_started) * 1000) - int(stats["detail_fetch_ms"] or 0))
            except Exception as exc:  # noqa: BLE001
                msg = str(exc)
                if "HTTP 403" in msg:
                    entry_report["status"] = "error"
                    entry_report["classification"] = "blocked_or_challenge"
                    entry_report["browserFallbackRecommended"] = True
                    entry_report["error"] = msg
                    warnings.append(f"static:{source_name}:{page_url}: {msg}")
                elif "Network error" in msg or "timed out" in msg or "Timeout" in msg:
                    entry_report["status"] = "error"
                    entry_report["classification"] = "timeout"
                    entry_report["browserFallbackRecommended"] = True
                    entry_report["error"] = msg
                    warnings.append(f"static:{source_name}:{page_url}: {msg}")
                else:
                    errors.append(f"static:{source_name}:{page_url}: {exc}")
        entry_report["keptCount"] = max(0, len(jobs) - kept_before)
        stats["jobs_emitted"] = int(entry_report["keptCount"])
        if int(stats["detail_pages_visited"] or 0) > 0:
            stats["detail_yield_percent"] = int(round((entry_report["keptCount"] / stats["detail_pages_visited"]) * 100))
        entry_report["loss"] = {
            "staticNonJobUrlRejected": int(link_rejections.get("non_job_url", 0)),
            "staticDuplicateLinkRejected": int(link_rejections.get("duplicate_link", 0)),
            "staticDetailParseEmpty": int(link_rejections.get("detail_parse_empty", 0)),
        }
        if entry_report["keptCount"] == 0 and pages and not clean_text(entry_report.get("classification")):
            entry_report["status"] = "error"
            entry_report["classification"] = "fetch_ok_extract_zero"
            entry_report["browserFallbackRecommended"] = True
            entry_report["error"] = "no jobs extracted from source pages"
            # If we're running a single static source loader, treat this as a hard error
            # so it isn't silently reported as ok-with-zero.
            if len(selected_sources) == 1:
                errors.append(f"static:{source_name}: no jobs extracted from source pages")
        details.append(entry_report)

    diag_studio = "multiple"
    if len(selected_sources) == 1:
        single = selected_sources[0]
        diag_studio = (
            clean_text(single.get("studio"))
            or clean_text(single.get("company"))
            or clean_text(single.get("name"))
            or "multiple"
        )

    set_source_diagnostics(
        diagnostics_name,
        adapter="static",
        studio=diag_studio,
        details=details,
        partial_errors=(warnings + errors),
    )
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def run_static_source_entry_source(
    *,
    source_row: Dict[str, Any],
    diagnostics_name: str,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: Optional[Dict[str, Dict[str, Any]]] = None,
    try_playwright: Optional[Callable[[str, int], Tuple[str, str]]] = None,
) -> List[RawJob]:
    return run_static_studio_pages_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        sources=[source_row],
        diagnostics_name=diagnostics_name,
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
    static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: Optional[Dict[str, Dict[str, Any]]] = None,
    try_playwright: Optional[Callable[[str, int], Tuple[str, str]]] = None,
) -> List[RawJob]:
    return run_static_studio_pages_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        shard="a_i",
        diagnostics_name="static_studio_pages_a_i",
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        try_playwright=try_playwright,
    )


def static_source_name_for_registry_row(row: Dict[str, Any]) -> str:
    """Return the pipeline source name for a static registry row (same as build_static_source_loaders)."""
    source_id = clean_text(row.get("id"))
    if not source_id:
        listing_url = clean_text(row.get("listing_url"))
        digest_seed = listing_url or clean_text(row.get("name")) or json.dumps(row, sort_keys=True, ensure_ascii=False)
        source_id = f"auto:{hashlib.sha1(digest_seed.encode('utf-8')).hexdigest()[:12]}"
    return f"static_source::{source_id}"


def build_static_source_loaders() -> List[Tuple[str, SourceLoader]]:
    loaders: List[Tuple[str, SourceLoader]] = []
    for row in registry_entries("static"):
        loader_name = static_source_name_for_registry_row(row)

        def _loader(
            *,
            fetch_text: Callable[[str, int], str],
            timeout_s: int,
            retries: int,
            backoff_s: float,
            _row: Dict[str, Any] = row,
            _loader_name: str = loader_name,
            static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
            source_state_rows: Optional[Dict[str, Dict[str, Any]]] = None,
            try_playwright: Optional[Callable[[str, int], Tuple[str, str]]] = None,
        ) -> List[RawJob]:
            return run_static_source_entry_source(
                source_row=_row,
                diagnostics_name=_loader_name,
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
                static_detail_concurrency=static_detail_concurrency,
                source_state_rows=source_state_rows,
                try_playwright=try_playwright,
            )

        loaders.append((loader_name, _loader))
    return loaders


def run_static_studio_pages_j_r_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: Optional[Dict[str, Dict[str, Any]]] = None,
    try_playwright: Optional[Callable[[str, int], Tuple[str, str]]] = None,
) -> List[RawJob]:
    return run_static_studio_pages_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        shard="j_r",
        diagnostics_name="static_studio_pages_j_r",
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
    static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: Optional[Dict[str, Dict[str, Any]]] = None,
    try_playwright: Optional[Callable[[str, int], Tuple[str, str]]] = None,
) -> List[RawJob]:
    return run_static_studio_pages_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        shard="s_z",
        diagnostics_name="static_studio_pages_s_z",
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        try_playwright=try_playwright,
    )


