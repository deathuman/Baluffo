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
from src.jobs.adapters import static_scrapy as _static_scrapy
from src.jobs.adapters.html_parsers import (
    maybe_fetch_kojima_job_listing_html,
    parse_jobpostings_from_html,
    strip_html_text,
)
from src.jobs.adapters.location_rules import classify_city_garbage
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.errors import NoPluginFoundError
from src.jobs.adapters.plugins.static import register_static_plugins
from src.jobs.adapters.plugins.static._heuristics import detect_js_shell
from src.jobs.adapters.plugins.static._rendered_cards import extract_rendered_card_jobs
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.static_helpers import (
    _is_one_man_studio_noise_city,
    add_detail_link,
    build_static_entry_report,
    build_static_source_runtime_config,
    choose_detail_traversal_mode,
    create_fetch_html_cached,
    process_detail_link,
    source_detail_concurrency_for,
    source_detail_limit_for,
    source_detail_retries_for,
    update_source_detail_taxonomy,
)
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.interfaces import SourceLoader
from src.jobs.models import RawJob
from src.jobs.page_gating import classify_job_page, looks_like_job_title_candidate
from src.jobs.registry import registry_entries
from src.jobs.state import (
    get_incremental_cache_decision,
    should_skip_static_source_for_structured_migration,
)
from src.jobs.text_utils import clean_text, normalize_url, sanitize_location_text
from src.jobs.transport import conditional_revalidate_url
from src.scrapers.domain_profiles import domain_profile_for_url
from src.shared.regex import find_urls_in_text
from src.shared.utils import now_iso

from ..common import config as common_config

register_static_plugins()
run_scrapy_static_source = _static_scrapy.run_scrapy_static_source


def _needs_detail_location_resolution(
    row: dict[str, Any], link: str = "", location_hint: str = ""
) -> bool:
    city = clean_text(row.get("city"))
    if city:
        sanitized_city, city_reason = sanitize_location_text(city, field_name="city")
        if city_reason or classify_city_garbage(city) or not sanitized_city:
            return True
    hint = clean_text(location_hint)
    return not city and bool(hint)


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
    heartbeat_callback: Callable[[], None] | None = None,
    progress_callback: Callable[..., None] | None = None,
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
    dead_listing_page_examples: list[str] = []

    static_runtime = build_static_source_runtime_config(static_detail_concurrency)
    static_detail_concurrency = static_runtime.static_detail_concurrency
    fetch_html_cached = create_fetch_html_cached(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )
    emit_heartbeat = heartbeat_callback or (lambda: None)

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
        emit_heartbeat()
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
        entry_report["browserEscalationEnabled"] = bool(try_playwright)
        cache_decision = get_incremental_cache_decision(
            source_name,
            source_state_rows or {},
            adapter="static",
            force_refresh_all=force_refresh_all,
        )
        entry_report["cacheDecision"] = clean_text(cache_decision.get("cacheDecision")) or "run_now"
        entry_report["cacheDecisionReason"] = (
            clean_text(cache_decision.get("cacheDecisionReason")) or "run_now"
        )
        kept_before = len(jobs)
        link_rejections: Counter[str] = Counter()
        stats = entry_report["stats"]
        progress_state = {
            "listingPagesVisited": 0,
            "lastProgressSignature": "",
        }
        source_pages = pages
        source_jobs = jobs
        source_stats = stats
        source_kept_before = kept_before

        def emit_source_progress(
            *,
            phase_key: str,
            phase_label: str,
            counts: dict[str, Any] | None = None,
            target_label: str = "",
            target_url: str = "",
            event_level: str = "muted",
            message: str = "",
            progress_state_ref: dict[str, Any] = progress_state,
            source_pages_ref: list[str] = source_pages,
            source_jobs_ref: list[dict[str, Any]] = source_jobs,
            source_stats_ref: dict[str, Any] = source_stats,
            source_kept_before_ref: int = source_kept_before,
        ) -> None:
            if progress_callback is None:
                return
            payload_counts = {
                "listingPages": len(source_pages_ref),
                "listingPagesVisited": max(0, int(progress_state_ref["listingPagesVisited"])),
                "candidateLinksFound": int(source_stats_ref.get("candidate_links_found") or 0),
                "detailPagesVisited": int(source_stats_ref.get("detail_pages_visited") or 0),
                "jobsEmitted": max(0, int(len(source_jobs_ref) - source_kept_before_ref)),
            }
            if isinstance(counts, dict):
                payload_counts.update(counts)
            signature = json.dumps(
                {
                    "phaseKey": str(phase_key or "").strip(),
                    "phaseLabel": str(phase_label or "").strip(),
                    "counts": payload_counts,
                    "targetLabel": str(target_label or "").strip(),
                    "targetUrl": str(target_url or "").strip(),
                    "message": str(message or "").strip(),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            if signature == progress_state_ref["lastProgressSignature"]:
                return
            progress_state_ref["lastProgressSignature"] = signature
            progress_callback(
                phase_key=phase_key,
                phase_label=phase_label,
                counts=payload_counts,
                target_label=target_label,
                target_url=target_url,
                event_level=event_level,
                message=message,
            )

        emit_source_progress(
            phase_key="static_prepare",
            phase_label="Preparing static source",
            counts={"cacheDecision": entry_report["cacheDecision"]},
            target_label=source_name,
            event_level="info",
            message=f"Preparing static source {source_name}.",
        )
        if entry_report["cacheDecision"] in {"skip_fresh", "cooldown_skip"}:
            entry_report["status"] = "excluded"
            entry_report["error"] = entry_report["cacheDecisionReason"]
            entry_report["exclusionReason"] = f"cache_{entry_report['cacheDecisionReason']}"
            update_source_detail_taxonomy(entry_report)
            details.append(entry_report)
            continue
        state_entry = (
            (source_state_rows or {}).get(source_name)
            if isinstance(source_state_rows, dict)
            else {}
        )
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
                update_source_detail_taxonomy(entry_report)
                details.append(entry_report)
                continue
        if should_skip_static_source_for_structured_migration(
            source_name, source, source_state_rows
        ):
            entry_report["status"] = "excluded"
            entry_report["error"] = "structured_migration_promoted"
            entry_report["exclusionReason"] = "structured_migration_promoted"
            entry_report["structuredMigrationSkipped"] = True
            update_source_detail_taxonomy(entry_report)
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
        ctx = AdapterPluginContext(
            family="static", adapter_key="static", source_identity=plugin_identity
        )
        try:
            plugin, _ = default_registry.select(ctx)
            plugin_jobs = plugin.run(
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
                heartbeat_callback=heartbeat_callback,
                pages=pages,
                source_row=source,
                parse_jobpostings_from_html=parse_jobpostings_from_html,
                maybe_fetch_kojima_job_listing_html=maybe_fetch_kojima_job_listing_html,
                try_playwright=try_playwright,
            )
            if "theonemanstudio" in source_name.lower() or "one man studio" in company.lower():
                for row in plugin_jobs:
                    if not isinstance(row, dict):
                        continue
                    row_city, _ = sanitize_location_text(row.get("city"), field_name="city")
                    row_country, _ = sanitize_location_text(
                        row.get("country"), field_name="country"
                    )
                    if row_country == "Remote":
                        row_country = ""
                    if row_city == "Remote":
                        row_city = ""
                    if row_country in {"", "Unknown"} and _is_one_man_studio_noise_city(
                        row_city,
                        source_name=source_name,
                        source=source,
                    ):
                        row_city = ""
                    row["city"] = row_city
                    row["country"] = row_country
                    if row_city or row_country not in {"", "Unknown"}:
                        row["locations"] = [
                            {
                                "city": row_city,
                                "country": row_country if row_country != "Unknown" else "",
                            }
                        ]
                        row["locationSummary"] = ", ".join(
                            part
                            for part in [row_city, row_country if row_country != "Unknown" else ""]
                            if part
                        )
                    else:
                        row["locations"] = []
                        row["locationSummary"] = ""
            emit_heartbeat()
            jobs.extend(plugin_jobs)
            entry_report["fetchedCount"] = len(pages)
            entry_report["keptCount"] = len(plugin_jobs)
            plugin_meta = source.get("_staticPluginMeta") if isinstance(source, dict) else None
            if isinstance(plugin_meta, dict):
                entry_report["classification"] = clean_text(plugin_meta.get("classification"))
                entry_report["browserFallbackRecommended"] = bool(
                    plugin_meta.get("browserFallbackRecommended")
                )
                entry_report["extractorHint"] = clean_text(plugin_meta.get("extractorHint"))
                if plugin_meta.get("emptyConfirmed"):
                    entry_report["emptyConfirmed"] = True
                ats_links = plugin_meta.get("atsLinks")
                if isinstance(ats_links, list):
                    entry_report["atsLinks"] = [clean_text(v) for v in ats_links if clean_text(v)][
                        :5
                    ]
                dead_listing_count = int(plugin_meta.get("deadListingPageCount") or 0)
                if dead_listing_count > 0:
                    entry_report["deadListingPageCount"] = dead_listing_count
                dead_listing_examples = plugin_meta.get("deadListingPageExamples")
                if isinstance(dead_listing_examples, list) and dead_listing_examples:
                    entry_report["deadListingPageExamples"] = [
                        clean_text(v) for v in dead_listing_examples if clean_text(v)
                    ][:5]
                meta_error = clean_text(plugin_meta.get("error"))
                if meta_error and not entry_report.get("error"):
                    entry_report["error"] = meta_error

                # If plugin extracted nothing, treat as error unless it proved an explicit empty state
                # or a non-fatal browser escalation classification.
                if not plugin_jobs:
                    emit_heartbeat()
                    classification = clean_text(entry_report.get("classification"))
                    empty_confirmed = (
                        bool(entry_report.get("emptyConfirmed"))
                        or classification == "empty_confirmed"
                    )
                    browser_recommended = bool(entry_report.get("browserFallbackRecommended"))
                    if classification not in {"dead_listing_page", "empty_confirmed"} and pages:
                        probe_page = clean_text(pages[0])
                        if probe_page:
                            try:
                                probe_html, _ = fetch_html_cached(
                                    probe_page, remaining_budget_s=float(timeout_s or 1)
                                )
                            except Exception:  # noqa: BLE001
                                probe_html = ""
                            if probe_html:
                                job_like, gate_reason = classify_job_page(
                                    probe_html,
                                    probe_page,
                                    profile=source if isinstance(source, dict) else None,
                                )
                                if not job_like and gate_reason == "dead_listing_page":
                                    classification = "dead_listing_page"
                                    entry_report["classification"] = classification
                                    entry_report["browserFallbackRecommended"] = False
                                    entry_report["browserEscalationEligible"] = False
                                    entry_report.pop("browserEscalationEligibilityReason", None)
                                    entry_report["deadListingPageCount"] = max(
                                        1, int(entry_report.get("deadListingPageCount") or 0)
                                    )
                                    if len(dead_listing_page_examples) < 5:
                                        dead_listing_page_examples.append(
                                            f"{probe_page} | {company}"
                                        )
                                    entry_report["deadListingPageExamples"] = (
                                        dead_listing_page_examples
                                    )
                                    empty_confirmed = True
                    if int(entry_report.get("deadListingPageCount") or 0) > 0:
                        classification = "dead_listing_page"
                        entry_report["classification"] = classification
                    if classification == "dead_listing_page":
                        entry_report["status"] = "ok"
                        entry_report["error"] = ""
                    elif not empty_confirmed:
                        entry_report["status"] = "error"
                        if not entry_report.get("error"):
                            entry_report["error"] = "no jobs extracted from source pages"
                        if browser_recommended:
                            warn_page = clean_text(pages[0]) if pages else ""
                            warnings.append(
                                f"static:{source_name}:{warn_page}: {entry_report.get('error')}"
                            )
                        else:
                            errors.append(f"static:{source_name}: {entry_report.get('error')}")
                else:
                    entry_report["status"] = "ok"
                    entry_report["error"] = ""
            else:
                entry_report["status"] = "ok"
            update_source_detail_taxonomy(entry_report)
            details.append(entry_report)
            continue
        except NoPluginFoundError:
            pass

        for page in pages:
            emit_heartbeat()
            page_url = clean_text(page)
            if not page_url:
                continue
            progress_state["listingPagesVisited"] += 1
            domain_profile = domain_profile_for_url(page_url)
            source_budget_s = int(
                domain_profile.get("static_source_time_budget_s") or static_source_time_budget_s
            )
            if (time.perf_counter() - source_started) > float(source_budget_s):
                entry_report["status"] = "error"
                entry_report["classification"] = "timeout"
                entry_report["browserFallbackRecommended"] = True
                entry_report["error"] = f"time budget exceeded ({source_budget_s}s)"
                warnings.append(f"static:{source_name}:{page_url}: time_budget_exceeded")
                break
            try:
                listing_fetch_started = time.perf_counter()
                emit_source_progress(
                    phase_key="static_listing_fetch",
                    phase_label="Fetching listing pages",
                    target_label=(
                        f"Listing {progress_state['listingPagesVisited']}/{max(1, len(pages))}"
                    ),
                    target_url=page_url,
                    event_level="muted",
                    message=(
                        f"Fetching listing page {progress_state['listingPagesVisited']}/{max(1, len(pages))} "
                        f"for {source_name}."
                    ),
                )
                remaining_budget_s = float(source_budget_s) - float(
                    time.perf_counter() - source_started
                )
                effective_timeout_s = max(
                    3, min(int(timeout_s or 1), int(remaining_budget_s or timeout_s))
                )
                html = ""
                cache_hit = False
                try:
                    html, cache_hit = fetch_html_cached(
                        page_url, remaining_budget_s=remaining_budget_s
                    )
                except Exception as exc:  # noqa: BLE001
                    err_str = str(exc)
                    err_lower = err_str.lower()
                    linked_in_throttle = "linkedin" in f"{page_url} {err_str}".lower()
                    if try_playwright and (
                        "403" in err_str
                        or (linked_in_throttle and "429" in err_str)
                        or "timeout" in err_lower
                        or "timed out" in err_lower
                    ):
                        if "403" in err_str:
                            reason = "403"
                        elif linked_in_throttle and "429" in err_str:
                            reason = "429"
                        else:
                            reason = "timeout"
                        html, _ = try_playwright(page_url, effective_timeout_s)
                        print(
                            f"[static] playwright_fallback_used url={page_url!r} reason={reason} got_html={bool(html)}",
                            file=sys.stderr,
                            flush=True,
                        )
                    if not html:
                        raise
                stats["listing_fetch_ms"] += int(
                    (time.perf_counter() - listing_fetch_started) * 1000
                )
                if cache_hit:
                    stats["fetch_cache_hits"] += 1
                emit_heartbeat()
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
                    errors.append(
                        f"static:{source_name}:{page_url}: dynamic-listing-fetch failed: {exc}"
                    )

                extraction_started = time.perf_counter()
                listing_jobs_found = 0
                emit_source_progress(
                    phase_key="static_candidate_extraction",
                    phase_label="Extracting candidates",
                    target_label=(
                        f"Listing {progress_state['listingPagesVisited']}/{max(1, len(pages))}"
                    ),
                    target_url=page_url,
                    event_level="muted",
                    message=f"Extracting listing candidates for {source_name}.",
                )
                for listing_html in listing_htmls:
                    emit_heartbeat()
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

                    if listing_jobs_found == 0:
                        rendered_rows = extract_rendered_card_jobs(
                            listing_html,
                            page_url=page_url,
                            company=company,
                            source_id=clean_text(source.get("id")) or source_name,
                            allow_any_anchor=True,
                        )
                        if rendered_rows:
                            has_job_like_rendered_title = False
                            for row in rendered_rows:
                                row = dict(row)
                                mode = clean_text(row.pop("_renderedCardMode", ""))
                                if mode == "fallback":
                                    continue
                                row["adapter"] = "static"
                                row["studio"] = (
                                    clean_text(source.get("studio")) or company or source_name
                                )
                                row["source"] = (
                                    clean_text(source.get("name")) or company or source_name
                                )
                                title = clean_text(row.get("title"))
                                link = normalize_url(row.get("jobLink"))
                                location_hint = clean_text(row.pop("_locationHint", ""))
                                needs_detail_lookup = _needs_detail_location_resolution(
                                    row,
                                    link,
                                    location_hint,
                                )
                                if looks_like_job_title_candidate(title):
                                    if not link or link in seen_links:
                                        continue
                                    if needs_detail_lookup:
                                        detail_result = process_detail_link(
                                            detail=link,
                                            detail_title=title,
                                            source_started=source_started,
                                            static_source_time_budget_s=source_budget_s,
                                            fetch_html_cached=fetch_html_cached,
                                            timeout_s=timeout_s,
                                            detail_retries=retries,
                                            company=company,
                                            source_name=source_name,
                                            source=source,
                                            ignored_link_titles=ignored_link_titles,
                                        )
                                        stats["detail_pages_visited"] += 1
                                        emit_source_progress(
                                            phase_key="static_detail_traversal",
                                            phase_label="Traversing detail pages",
                                            target_label=title or link or source_name,
                                            target_url=link,
                                        )
                                        stats["detail_fetch_ms"] += int(
                                            detail_result.get("fetchMs") or 0
                                        )
                                        emitted_detail_rows = detail_result.get("rows") or []
                                        if emitted_detail_rows:
                                            seen_links.add(link)
                                            for emitted_row in emitted_detail_rows:
                                                if not isinstance(emitted_row, dict):
                                                    continue
                                                emitted_row["source"] = (
                                                    clean_text(source.get("name"))
                                                    or company
                                                    or source_name
                                                )
                                                emitted_row["studio"] = (
                                                    clean_text(source.get("studio"))
                                                    or company
                                                    or source_name
                                                )
                                                jobs.append(emitted_row)
                                                listing_jobs_found += 1
                                            has_job_like_rendered_title = True
                                            continue
                                    seen_links.add(link)
                                    jobs.append(row)
                                    listing_jobs_found += 1
                                    has_job_like_rendered_title = True
                                    continue
                                if not link or link in seen_links:
                                    continue
                                detail_result = process_detail_link(
                                    detail=link,
                                    detail_title=title,
                                    source_started=source_started,
                                    static_source_time_budget_s=source_budget_s,
                                    fetch_html_cached=fetch_html_cached,
                                    timeout_s=timeout_s,
                                    detail_retries=retries,
                                    company=company,
                                    source_name=source_name,
                                    source=source,
                                    ignored_link_titles=ignored_link_titles,
                                )
                                stats["detail_pages_visited"] += 1
                                emit_source_progress(
                                    phase_key="static_detail_traversal",
                                    phase_label="Traversing detail pages",
                                    target_label=title or link or source_name,
                                    target_url=link,
                                )
                                stats["detail_fetch_ms"] += int(detail_result.get("fetchMs") or 0)
                                emitted_detail_rows = detail_result.get("rows") or []
                                if emitted_detail_rows:
                                    seen_links.add(link)
                                    for emitted_row in emitted_detail_rows:
                                        if not isinstance(emitted_row, dict):
                                            continue
                                        emitted_row["source"] = (
                                            clean_text(source.get("name")) or company or source_name
                                        )
                                        emitted_row["studio"] = (
                                            clean_text(source.get("studio"))
                                            or company
                                            or source_name
                                        )
                                        jobs.append(emitted_row)
                                        listing_jobs_found += 1
                                elif row:
                                    jobs.append(row)
                                    listing_jobs_found += 1
                            if listing_jobs_found > 0 and has_job_like_rendered_title:
                                source["_staticPluginMeta"] = {
                                    "detailFetchRequired": False,
                                    "detailTraversalMode": "listing_only",
                                }
                                emit_heartbeat()
                                continue

                    for row_match in re.finditer(
                        r'(?is)<(?:div|tr)[^>]*class=["\'][^"\']*job-listing-item[^"\']*["\'][^>]*>(.*?)</(?:div|tr)>',
                        listing_html,
                    ):
                        row_html = row_match.group(1) or ""
                        link_match = re.search(
                            r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', row_html
                        )
                        if not link_match:
                            continue
                        href = clean_text(link_match.group(1))
                        anchor_text = strip_html_text(
                            re.sub(r"(?is)<[^>]+>", " ", link_match.group(2) or "")
                        )
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

                    for match in re.finditer(
                        r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', listing_html
                    ):
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
                stats["candidate_extraction_ms"] += int(
                    (time.perf_counter() - extraction_started) * 1000
                )
                emit_source_progress(
                    phase_key="static_candidate_extraction",
                    phase_label="Candidates extracted",
                    counts={
                        "detailCandidates": len(detail_links),
                        "listingJobsFound": listing_jobs_found,
                    },
                    target_label=(
                        f"Listing {progress_state['listingPagesVisited']}/{max(1, len(pages))}"
                    ),
                    target_url=page_url,
                    event_level="muted",
                    message=(
                        f"Found {len(detail_links)} detail candidate"
                        f"{'' if len(detail_links) == 1 else 's'} for {source_name}."
                    ),
                )
                listing_fingerprint = hashlib.sha1(
                    "\n".join(listing_htmls).encode("utf-8")
                ).hexdigest()
                previous_listing_fingerprint = clean_text(
                    (state_entry or {}).get("lastListingFingerprint")
                )
                entry_report["listingFingerprint"] = listing_fingerprint
                entry_report["listingCheckedAt"] = now_iso()
                entry_report["listingChanged"] = bool(
                    listing_fingerprint != previous_listing_fingerprint
                )
                if (
                    previous_listing_fingerprint
                    and listing_fingerprint == previous_listing_fingerprint
                    and not force_refresh_all
                ):
                    entry_report["cacheDecision"] = "listing_only"
                    entry_report["cacheDecisionReason"] = "listing_fingerprint_unchanged"
                    entry_report["detailSkippedByListingFingerprint"] = True
                    stats["detail_skipped_by_listing_fingerprint"] += 1
                    detail_links = []

                if not detail_links:
                    emit_heartbeat()
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
                    emit_heartbeat()
                    continue
                detail_limit = source_detail_limit_for(
                    source_key,
                    source_state_rows=source_state_rows,
                    discovered_links=len(detail_links),
                    listing_jobs_found=listing_jobs_found,
                    low_yield_detail_cap=static_runtime.low_yield_detail_cap,
                    very_low_yield_detail_cap=static_runtime.very_low_yield_detail_cap,
                )
                detail_retries = source_detail_retries_for(
                    source_key,
                    source_state_rows=source_state_rows,
                    base_retries=retries,
                )
                profile_max_detail_links = max(0, int(domain_profile.get("max_detail_links") or 0))
                if profile_max_detail_links > 0:
                    detail_limit = (
                        min(detail_limit, profile_max_detail_links)
                        if detail_limit
                        else profile_max_detail_links
                    )
                if detail_limit and detail_limit < len(detail_links):
                    detail_links = detail_links[:detail_limit]
                detail_fetch_started = time.perf_counter()
                emit_source_progress(
                    phase_key="static_detail_traversal",
                    phase_label="Traversing detail pages",
                    counts={"detailCandidates": len(detail_links)},
                    target_label=f"{len(detail_links)} detail page(s)",
                    target_url=page_url,
                    event_level="muted",
                    message=(
                        f"Traversing {len(detail_links)} detail page"
                        f"{'' if len(detail_links) == 1 else 's'} for {source_name}."
                    ),
                )
                emit_heartbeat()
                with ThreadPoolExecutor(
                    max_workers=source_detail_concurrency_for(
                        source_key,
                        source_state_rows=source_state_rows,
                        static_detail_concurrency=static_detail_concurrency,
                    )
                ) as executor:
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
                            detail_retries=detail_retries,
                            ignored_link_titles=ignored_link_titles,
                        ): (detail, detail_title)
                        for detail, detail_title in detail_links
                    }
                    for future in as_completed(future_map):
                        detail, _detail_title = future_map[future]
                        stats["detail_pages_visited"] += 1
                        emit_source_progress(
                            phase_key="static_detail_traversal",
                            phase_label="Traversing detail pages",
                            target_label=_detail_title or detail or source_name,
                            target_url=detail,
                        )
                        emit_heartbeat()
                        try:
                            detail_result = future.result()
                        except Exception as exc:  # noqa: BLE001
                            msg = str(exc)
                            linked_in_throttle = "linkedin" in f"{page_url} {msg}".lower()
                            if "HTTP 403" in msg or (
                                linked_in_throttle
                                and ("HTTP 429" in msg or "Too Many Requests" in msg)
                            ):
                                entry_report["classification"] = "blocked_or_challenge"
                                entry_report["browserFallbackRecommended"] = True
                                entry_report["error"] = msg
                                warnings.append(f"static:{source_name}:{detail}: {msg}")
                            else:
                                errors.append(f"static:{source_name}:{detail}: {exc}")
                            continue
                        stats["fetch_cache_hits"] += 1 if detail_result.get("cacheHit") else 0
                        stats["detail_fetch_ms"] += int(detail_result.get("fetchMs") or 0)
                        rejected_classification = clean_text(
                            detail_result.get("rejectedClassification")
                        )
                        if rejected_classification == "dead_listing_page":
                            link_rejections["dead_listing_page"] += 1
                            stats["dead_listing_pages_rejected"] += 1
                            if len(dead_listing_page_examples) < 5:
                                example = clean_text(detail_result.get("rejectedExample"))
                                if example:
                                    dead_listing_page_examples.append(example)
                        elif detail_result.get("parseEmpty"):
                            link_rejections["detail_parse_empty"] += 1
                        for row in detail_result.get("rows") or []:
                            link = normalize_url(row.get("jobLink"))
                            if not link or link in seen_links:
                                continue
                            seen_links.add(link)
                            jobs.append(row)
                        emit_heartbeat()
                stats["detail_fetch_ms"] += max(
                    0,
                    int((time.perf_counter() - detail_fetch_started) * 1000)
                    - int(stats["detail_fetch_ms"] or 0),
                )
            except Exception as exc:  # noqa: BLE001
                msg = str(exc)
                linked_in_throttle = "linkedin" in f"{page_url} {msg}".lower()
                if "HTTP 403" in msg or (
                    linked_in_throttle and ("HTTP 429" in msg or "Too Many Requests" in msg)
                ):
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
                emit_heartbeat()
        entry_report["keptCount"] = max(0, len(jobs) - kept_before)
        stats["jobs_emitted"] = int(entry_report["keptCount"])
        if int(stats["detail_pages_visited"] or 0) > 0:
            stats["detail_yield_percent"] = int(
                round((entry_report["keptCount"] / stats["detail_pages_visited"]) * 100)
            )
        entry_report["loss"] = {
            "staticNonJobUrlRejected": int(link_rejections.get("non_job_url", 0)),
            "staticDuplicateLinkRejected": int(link_rejections.get("duplicate_link", 0)),
            "staticDetailParseEmpty": int(link_rejections.get("detail_parse_empty", 0)),
            "staticDeadListingPageRejected": int(link_rejections.get("dead_listing_page", 0)),
        }
        entry_report["deadListingPageCount"] = int(link_rejections.get("dead_listing_page", 0))
        entry_report["deadListingPageExamples"] = dead_listing_page_examples
        if (
            entry_report["keptCount"] == 0
            and pages
            and not clean_text(entry_report.get("classification"))
            and int(link_rejections.get("dead_listing_page", 0)) <= 0
        ):
            entry_report["status"] = "error"
            entry_report["error"] = "no jobs extracted from source pages"
            # If we're running a single static source loader, treat this as a hard error
            # so it isn't silently reported as ok-with-zero.
            if len(selected_sources) == 1:
                errors.append(f"static:{source_name}: no jobs extracted from source pages")
        emit_heartbeat()
        update_source_detail_taxonomy(entry_report)
        if (
            entry_report["keptCount"] == 0
            and int(entry_report.get("deadListingPageCount") or 0) > 0
        ):
            entry_report["classification"] = "dead_listing_page"
            entry_report["browserFallbackRecommended"] = False
            entry_report["browserEscalationEligible"] = False
            entry_report.pop("browserEscalationEligibilityReason", None)
        emit_source_progress(
            phase_key="static_completed",
            phase_label="Static source completed",
            counts={
                "keptCount": int(entry_report.get("keptCount") or 0),
                "fetchedCount": int(entry_report.get("fetchedCount") or 0),
                "detailCandidates": int(stats.get("candidate_links_found") or 0),
                "detailPagesVisited": int(stats.get("detail_pages_visited") or 0),
            },
            target_label=source_name,
            event_level="success" if int(entry_report.get("keptCount") or 0) > 0 else "warn",
            message=(
                f"Completed static source {source_name}: kept "
                f"{int(entry_report.get('keptCount') or 0)} job"
                f"{'' if int(entry_report.get('keptCount') or 0) == 1 else 's'}."
            ),
        )
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
    heartbeat_callback: Callable[[], None] | None = None,
    progress_callback: Callable[..., None] | None = None,
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
        heartbeat_callback=heartbeat_callback,
        progress_callback=progress_callback,
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
    heartbeat_callback: Callable[[], None] | None = None,
    progress_callback: Callable[..., None] | None = None,
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
        heartbeat_callback=heartbeat_callback,
        progress_callback=progress_callback,
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
        digest_seed = (
            listing_url
            or clean_text(row.get("name"))
            or json.dumps(row, sort_keys=True, ensure_ascii=False)
        )
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
            heartbeat_callback: Callable[[], None] | None = None,
            progress_callback: Callable[..., None] | None = None,
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
                heartbeat_callback=heartbeat_callback,
                progress_callback=progress_callback,
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
    heartbeat_callback: Callable[[], None] | None = None,
    progress_callback: Callable[..., None] | None = None,
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
        heartbeat_callback=heartbeat_callback,
        progress_callback=progress_callback,
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
    heartbeat_callback: Callable[[], None] | None = None,
    progress_callback: Callable[..., None] | None = None,
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
        heartbeat_callback=heartbeat_callback,
        progress_callback=progress_callback,
        shard="s_z",
        diagnostics_name="static_studio_pages_s_z",
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        try_playwright=try_playwright,
        force_refresh_all=force_refresh_all,
    )
