"""Static and scrapy adapters."""

from __future__ import annotations

import hashlib
import json
import re
import sys
import time
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import urlparse

from src.exceptions import AdapterValidationError
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
from src.jobs.adapters.static_helpers import (
    add_detail_link,
    build_static_entry_report,
    build_static_source_runtime_config,
    choose_detail_traversal_mode,
    create_fetch_html_cached,
    process_detail_link,
    source_detail_concurrency_for,
    source_detail_limit_for,
)
from src.jobs.common import config as common_config
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.interfaces import SourceLoader
from src.jobs.models import RawJob
from src.jobs.registry import registry_entries
from src.jobs.state import get_incremental_cache_decision
from src.jobs.text_utils import clean_text, normalize_url
from src.jobs.transport import conditional_revalidate_url
from src.scrapers.domain_profiles import domain_profile_for_url
from src.shared.regex import find_urls_in_text
from src.shared.utils import now_iso

register_static_plugins()


def static_source_shard(row: dict[str, Any]) -> str:
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
    sources: list[dict[str, Any]] | None = None,
    shard: str | None = None,
    diagnostics_name: str = "static_studio_pages",
    static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    errors: list[str] = []
    warnings: list[str] = []
    seen_links = set()
    details: list[dict[str, Any]] = []
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

    static_runtime = build_static_source_runtime_config(static_detail_concurrency)
    static_detail_concurrency = static_runtime.static_detail_concurrency
    fetch_html_cached = create_fetch_html_cached(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )

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
    static_source_time_budget_s = static_runtime.static_source_time_budget_s
    for source in selected_sources:
        source_started = time.perf_counter()
        if shard and static_source_shard(source) != shard:
            continue
        source_name = clean_text(source.get("name")) or "static_source"
        company = clean_text(source.get("company")) or source_name
        pages = source.get("pages") if isinstance(source.get("pages"), list) else []
        entry_report = build_static_entry_report(
            source=source,
            source_name=source_name,
            pages=pages,
            company=company,
        )
        cache_decision = get_incremental_cache_decision(
            source_name,
            source_state_rows or {},
            adapter="static",
            force_refresh_all=force_refresh_all,
        )
        entry_report["cacheDecision"] = clean_text(cache_decision.get("cacheDecision")) or "run_now"
        entry_report["cacheDecisionReason"] = clean_text(cache_decision.get("cacheDecisionReason")) or "run_now"
        kept_before = len(jobs)
        link_rejections: Counter[str] = Counter()
        stats = entry_report["stats"]
        if entry_report["cacheDecision"] in {"skip_fresh", "cooldown_skip"}:
            entry_report["status"] = "excluded"
            entry_report["error"] = entry_report["cacheDecisionReason"]
            entry_report["exclusionReason"] = f"cache_{entry_report['cacheDecisionReason']}"
            details.append(entry_report)
            continue
        state_entry = (source_state_rows or {}).get(source_name) if isinstance(source_state_rows, dict) else {}
        if entry_report["cacheDecision"] == "revalidate_only" and pages:
            revalidate = conditional_revalidate_url(
                clean_text(pages[0]),
                timeout_s,
                etag=clean_text((state_entry or {}).get("lastHttpEtag")),
                last_modified=clean_text((state_entry or {}).get("lastHttpLastModified")),
            )
            entry_report["httpStatus"] = int(revalidate.get("statusCode") or 0)
            if clean_text(revalidate.get("etag")):
                entry_report["httpEtag"] = clean_text(revalidate.get("etag"))
            if clean_text(revalidate.get("lastModified")):
                entry_report["httpLastModified"] = clean_text(revalidate.get("lastModified"))
            if bool(revalidate.get("notModified")):
                entry_report["status"] = "excluded"
                entry_report["error"] = "not_modified_304"
                entry_report["exclusionReason"] = "cache_not_modified_304"
                entry_report["cacheDecisionReason"] = "not_modified_304"
                details.append(entry_report)
                continue

        host = ""
        if pages:
            try:
                parsed = urlparse(clean_text(pages[0]) or "")
                host = (parsed.netloc or "").strip().lower()
            except Exception:  # noqa: BLE001
                pass
        plugin_identity = host or source_name
        if host == "jobs.jobvite.com" and pages:
            plugin_identity = clean_text(pages[0]) or plugin_identity
        ctx = AdapterPluginContext(family="static", adapter_key="static", source_identity=plugin_identity)
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

        for page in pages:
            page_url = clean_text(page)
            if not page_url:
                continue
            domain_profile = domain_profile_for_url(page_url)
            source_budget_s = int(domain_profile.get("static_source_time_budget_s") or static_source_time_budget_s)
            if (time.perf_counter() - source_started) > float(source_budget_s):
                entry_report["status"] = "error"
                entry_report["classification"] = "timeout"
                entry_report["browserFallbackRecommended"] = True
                entry_report["error"] = f"time budget exceeded ({source_budget_s}s)"
                warnings.append(f"static:{source_name}:{page_url}: time_budget_exceeded")
                break
            try:
                listing_fetch_started = time.perf_counter()
                remaining_budget_s = float(source_budget_s) - float(time.perf_counter() - source_started)
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
                detail_links: list[tuple[str, str]] = []
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
                listing_jobs_found = 0
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
                        listing_jobs_found += 1

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
                        add_detail_link(
                            detail_links,
                            detail_seen,
                            seen_links,
                            link_rejections,
                            candidate_url=href,
                            anchor_text=anchor_text,
                            enforce_heuristics=False,
                            page_url=page_url,
                            source=source,
                            default_path_tokens=static_runtime.default_path_tokens,
                            default_query_keys=static_runtime.default_query_keys,
                        )

                    for match in re.finditer(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', listing_html):
                        href = clean_text(match.group(1))
                        anchor_inner = match.group(2) or ""
                        anchor_text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", anchor_inner))
                        add_detail_link(
                            detail_links,
                            detail_seen,
                            seen_links,
                            link_rejections,
                            candidate_url=href,
                            anchor_text=anchor_text,
                            enforce_heuristics=True,
                            page_url=page_url,
                            source=source,
                            default_path_tokens=static_runtime.default_path_tokens,
                            default_query_keys=static_runtime.default_query_keys,
                        )
                    for raw in find_urls_in_text(listing_html):
                        add_detail_link(
                            detail_links,
                            detail_seen,
                            seen_links,
                            link_rejections,
                            candidate_url=clean_text(raw),
                            anchor_text="",
                            enforce_heuristics=True,
                            page_url=page_url,
                            source=source,
                            default_path_tokens=static_runtime.default_path_tokens,
                            default_query_keys=static_runtime.default_query_keys,
                        )
                stats["candidate_links_found"] += len(detail_links)
                stats["candidate_extraction_ms"] += int((time.perf_counter() - extraction_started) * 1000)
                listing_fingerprint = hashlib.sha1("\n".join(listing_htmls).encode("utf-8")).hexdigest()
                previous_listing_fingerprint = clean_text((state_entry or {}).get("lastListingFingerprint"))
                entry_report["listingFingerprint"] = listing_fingerprint
                entry_report["listingCheckedAt"] = now_iso()
                entry_report["listingChanged"] = bool(listing_fingerprint != previous_listing_fingerprint)
                if previous_listing_fingerprint and listing_fingerprint == previous_listing_fingerprint:
                    entry_report["cacheDecision"] = "listing_only"
                    entry_report["cacheDecisionReason"] = "listing_fingerprint_unchanged"
                    entry_report["detailSkippedByListingFingerprint"] = True
                    stats["detail_skipped_by_listing_fingerprint"] += 1
                    detail_links = []

                if not detail_links:
                    continue
                source_key = diagnostics_name if len(selected_sources) == 1 else source_name
                plugin_meta = source.get("_staticPluginMeta") if isinstance(source, dict) else None
                detail_traversal_mode = choose_detail_traversal_mode(
                    page_url,
                    runtime_config=static_runtime,
                    profile=domain_profile,
                    plugin_meta=plugin_meta,
                    listing_jobs_found=listing_jobs_found,
                    discovered_links=len(detail_links),
                    source_key=source_key,
                    source_state_rows=source_state_rows,
                )
                entry_report["detailTraversalMode"] = detail_traversal_mode
                if detail_traversal_mode == "listing_only":
                    detail_links = []
                    continue
                detail_limit = source_detail_limit_for(
                    source_key,
                    source_state_rows=source_state_rows,
                    discovered_links=len(detail_links),
                    listing_jobs_found=listing_jobs_found,
                    low_yield_detail_cap=static_runtime.low_yield_detail_cap,
                    very_low_yield_detail_cap=static_runtime.very_low_yield_detail_cap,
                )
                profile_max_detail_links = max(0, int(domain_profile.get("max_detail_links") or 0))
                if profile_max_detail_links > 0:
                    detail_limit = min(detail_limit, profile_max_detail_links) if detail_limit else profile_max_detail_links
                if detail_limit and detail_limit < len(detail_links):
                    detail_links = detail_links[:detail_limit]
                detail_fetch_started = time.perf_counter()
                with ThreadPoolExecutor(max_workers=source_detail_concurrency_for(
                    source_key,
                    source_state_rows=source_state_rows,
                    static_detail_concurrency=static_detail_concurrency,
                )) as executor:
                    future_map = {
                        executor.submit(
                            process_detail_link,
                            detail=detail,
                            detail_title=detail_title,
                            source_started=source_started,
                            static_source_time_budget_s=source_budget_s,
                            fetch_html_cached=fetch_html_cached,
                            timeout_s=timeout_s,
                            company=company,
                            source_name=source_name,
                            source=source,
                            ignored_link_titles=ignored_link_titles,
                        ): (detail, detail_title)
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
    source_row: dict[str, Any],
    diagnostics_name: str,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
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
        force_refresh_all=force_refresh_all,
    )


def run_static_studio_pages_a_i_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
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
        force_refresh_all=force_refresh_all,
    )


def static_source_name_for_registry_row(row: dict[str, Any]) -> str:
    """Return the pipeline source name for a static registry row (same as build_static_source_loaders)."""
    source_id = clean_text(row.get("id"))
    if not source_id:
        listing_url = clean_text(row.get("listing_url"))
        digest_seed = listing_url or clean_text(row.get("name")) or json.dumps(row, sort_keys=True, ensure_ascii=False)
        source_id = f"auto:{hashlib.sha1(digest_seed.encode('utf-8')).hexdigest()[:12]}"
    return f"static_source::{source_id}"


def build_static_source_loaders() -> list[tuple[str, SourceLoader]]:
    loaders: list[tuple[str, SourceLoader]] = []
    for row in registry_entries("static"):
        loader_name = static_source_name_for_registry_row(row)

        def _loader(
            *,
            fetch_text: Callable[[str, int], str],
            timeout_s: int,
            retries: int,
            backoff_s: float,
            _row: dict[str, Any] = row,
            _loader_name: str = loader_name,
            static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
            source_state_rows: dict[str, dict[str, Any]] | None = None,
            try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
            force_refresh_all: bool = False,
        ) -> list[RawJob]:
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
                force_refresh_all=force_refresh_all,
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
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
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
        force_refresh_all=force_refresh_all,
    )


def run_static_studio_pages_s_z_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
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
        force_refresh_all=force_refresh_all,
    )


