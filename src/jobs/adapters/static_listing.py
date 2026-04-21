from __future__ import annotations

import asyncio
import hashlib
import re
import sys
import time
from typing import Any

from src.jobs.adapters.html_parsers import (
    maybe_fetch_kojima_job_listing_html,
    parse_jobpostings_from_html,
)
from src.jobs.adapters.plugins.static._heuristics import detect_js_shell
from src.jobs.adapters.static_helpers import (
    choose_detail_traversal_mode,
    effective_timeout_for_remaining_budget,
    is_probable_job_detail_url,
    source_detail_concurrency_for,
    source_detail_limit_for,
    source_detail_retries_for,
    static_source_budget_exhausted,
)
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.text_utils import clean_text
from src.scrapers.domain_profiles import domain_profile_for_url
from src.shared.http_batch import fetch_pages_batched
from src.shared.utils import now_iso

from ..common import config as common_config
from . import static_listing_flow as _static_listing_flow
from .static_detail import StaticDetailTraversalPlan, run_detail_traversal
from .static_runtime import StaticSourceContext

root = None

StaticListingStageState = _static_listing_flow.StaticListingStageState
_complete_source_without_generic_flow = _static_listing_flow._complete_source_without_generic_flow
_finish_generic_source = _static_listing_flow._finish_generic_source
_handle_skip_and_revalidation = _static_listing_flow._handle_skip_and_revalidation
_run_plugin_fast_path = _static_listing_flow._run_plugin_fast_path
_should_try_listing_browser_fallback = _static_listing_flow._should_try_listing_browser_fallback
_extract_listing_candidates = _static_listing_flow._extract_listing_candidates


def process_static_source(ctx: StaticSourceContext) -> None:
    _static_listing_flow.root = root
    if _handle_skip_and_revalidation(ctx):
        return
    if _run_plugin_fast_path(ctx):
        return

    cleaned_pages = [clean_text(page) for page in ctx.pages if clean_text(page)]
    listing_batch_size = max(
        1,
        min(
            ctx.runtime_config.static_detail_concurrency, len(cleaned_pages) if cleaned_pages else 1
        ),
    )
    stage_state = StaticListingStageState()
    stop_source = False

    def _fetch_listing_html_sync(url: str, *, effective_timeout_s: int) -> str:
        return fetch_with_retries(
            url,
            ctx.run_deps.fetch_text,
            timeout_s=effective_timeout_s,
            retries=ctx.run_deps.retries,
            backoff_s=ctx.run_deps.backoff_s,
        )

    for batch_start in range(0, len(cleaned_pages), listing_batch_size):
        if stop_source:
            break
        if static_source_budget_exhausted(
            deadline_monotonic=float(ctx.source_deadline),
            reserve_s=1.0,
        ):
            ctx.stop_for_budget_exhaustion(
                target_url=clean_text(cleaned_pages[batch_start]) or ctx.source_name,
                source_budget_s=ctx.runtime_config.static_source_time_budget_s,
            )
            break
        page_batch = cleaned_pages[batch_start : batch_start + listing_batch_size]
        listing_batch_jobs: list[dict[str, Any]] = []
        for page_url in page_batch:
            domain_profile = domain_profile_for_url(page_url)
            source_budget_s = int(
                domain_profile.get("static_source_time_budget_s")
                or ctx.runtime_config.static_source_time_budget_s
            )
            ctx.sync_source_deadline(source_budget_s)
            if static_source_budget_exhausted(
                deadline_monotonic=float(ctx.source_deadline),
                reserve_s=1.0,
            ):
                ctx.stop_for_budget_exhaustion(
                    target_url=page_url,
                    source_budget_s=source_budget_s,
                )
                stop_source = True
                break
            listing_batch_jobs.append(
                {
                    "url": page_url,
                    "payload": {
                        "domainProfile": domain_profile,
                        "sourceBudgetS": source_budget_s,
                    },
                }
            )
        if stop_source or not listing_batch_jobs:
            break

        stage_state.clear_batch_meta()

        def _fetch_listing_job(batch_job: dict[str, Any], url: str, _timeout_s: int) -> str:
            del _timeout_s
            fetch_started = time.perf_counter()
            payload = batch_job.get("payload") if isinstance(batch_job, dict) else {}
            source_budget_s = int(
                (payload or {}).get("sourceBudgetS")
                or ctx.runtime_config.static_source_time_budget_s
            )
            ctx.sync_source_deadline(source_budget_s)
            remaining_budget_s = ctx.remaining_budget_s()
            effective_timeout_s = effective_timeout_for_remaining_budget(
                timeout_s=ctx.run_deps.timeout_s,
                remaining_budget_s=remaining_budget_s,
            )
            if effective_timeout_s <= 0:
                raise TimeoutError(f"time budget exceeded ({source_budget_s}s)")
            html = ""
            browser_fallback_attempted = False
            browser_fallback_error = ""
            try:
                html = _fetch_listing_html_sync(url, effective_timeout_s=effective_timeout_s)
            except Exception as exc:  # noqa: BLE001
                err_str = str(exc)
                should_fallback, reason = _should_try_listing_browser_fallback(url, err_str)
                if ctx.run_deps.try_playwright and should_fallback:
                    browser_budget_s = effective_timeout_for_remaining_budget(
                        timeout_s=ctx.run_deps.timeout_s,
                        remaining_budget_s=ctx.remaining_budget_s(),
                    )
                    if browser_budget_s > 0:
                        browser_fallback_attempted = True
                        stage_state.increment_browser_fallbacks()
                        html, browser_fallback_error = ctx.run_deps.try_playwright(
                            url, browser_budget_s
                        )
                    print(
                        f"[static] playwright_fallback_used url={url!r} reason={reason} got_html={bool(html)}",
                        file=sys.stderr,
                        flush=True,
                    )
                if not html:
                    if browser_fallback_attempted:
                        if "403" in err_str:
                            stage_state.note_terminal_reason("blocked_after_browser_fallback", ctx)
                        else:
                            stage_state.note_terminal_reason("browser_fallback_empty", ctx)
                    elif reason == "timeout":
                        stage_state.note_terminal_reason("listing_timeout", ctx)
                    raise
            stage_state.record_batch_meta(
                url,
                durationMs=int((time.perf_counter() - fetch_started) * 1000),
                timeoutS=effective_timeout_s,
                cacheHit=False,
                browserFallbackUsed=browser_fallback_attempted,
                browserFallbackError=browser_fallback_error,
            )
            return html

        async def _fetch_listing_job_async(
            client: Any,
            batch_job: dict[str, Any],
            url: str,
            _timeout_s: int,
        ) -> str:
            del _timeout_s
            fetch_started = time.perf_counter()
            payload = batch_job.get("payload") if isinstance(batch_job, dict) else {}
            source_budget_s = int(
                (payload or {}).get("sourceBudgetS")
                or ctx.runtime_config.static_source_time_budget_s
            )
            ctx.sync_source_deadline(source_budget_s)
            remaining_budget_s = ctx.remaining_budget_s()
            effective_timeout_s = effective_timeout_for_remaining_budget(
                timeout_s=ctx.run_deps.timeout_s,
                remaining_budget_s=remaining_budget_s,
            )
            if effective_timeout_s <= 0:
                raise TimeoutError(f"time budget exceeded ({source_budget_s}s)")
            html = ""
            browser_fallback_attempted = False
            browser_fallback_error = ""
            try:
                if ctx.run_deps.listing_async_fetch is None:
                    html = await asyncio.to_thread(
                        _fetch_listing_html_sync,
                        url,
                        effective_timeout_s=effective_timeout_s,
                    )
                else:
                    html = await ctx.run_deps.listing_async_fetch(
                        client,
                        batch_job,
                        url,
                        effective_timeout_s,
                    )
            except Exception as exc:  # noqa: BLE001
                err_str = str(exc)
                should_fallback, reason = _should_try_listing_browser_fallback(url, err_str)
                if ctx.run_deps.try_playwright and should_fallback:
                    browser_budget_s = effective_timeout_for_remaining_budget(
                        timeout_s=ctx.run_deps.timeout_s,
                        remaining_budget_s=ctx.remaining_budget_s(),
                    )
                    if browser_budget_s > 0:
                        browser_fallback_attempted = True
                        stage_state.increment_browser_fallbacks()
                        html, browser_fallback_error = await asyncio.to_thread(
                            ctx.run_deps.try_playwright,
                            url,
                            browser_budget_s,
                        )
                        print(
                            f"[static] playwright_fallback_used url={url!r} reason={reason} got_html={bool(html)}",
                            file=sys.stderr,
                            flush=True,
                        )
                if not html:
                    if browser_fallback_attempted:
                        if "403" in err_str:
                            stage_state.note_terminal_reason("blocked_after_browser_fallback", ctx)
                        else:
                            stage_state.note_terminal_reason("browser_fallback_empty", ctx)
                    elif reason == "timeout":
                        stage_state.note_terminal_reason("listing_timeout", ctx)
                    raise
            stage_state.record_batch_meta(
                url,
                durationMs=int((time.perf_counter() - fetch_started) * 1000),
                timeoutS=effective_timeout_s,
                cacheHit=False,
                browserFallbackUsed=browser_fallback_attempted,
                browserFallbackError=browser_fallback_error,
            )
            return html

        def _on_listing_batch_progress(completed: int, total: int) -> None:
            completed_count = max(0, int(completed or 0))
            total_count = max(1, int(total or 0))
            ctx.emit_heartbeat()
            ctx.emit_source_progress(
                phase_key="static_listing_fetch",
                phase_label="Fetching listing pages",
                counts={"listingPagesFetched": completed_count},
                target_label=f"Listing fetch {completed_count}/{total_count}",
                wait_reason="listing_batch",
                event_level="muted",
                message=(
                    f"Fetched {completed_count}/{total_count} listing page"
                    f"{'' if total_count == 1 else 's'} for {ctx.source_name}."
                ),
            )

        ctx.emit_source_progress(
            phase_key="static_listing_fetch",
            phase_label="Fetching listing pages",
            counts={"listingPagesFetched": 0},
            target_label=f"Listing fetch 0/{max(1, len(listing_batch_jobs))}",
            target_url=clean_text((listing_batch_jobs[0] or {}).get("url")),
            wait_reason="listing_batch",
            event_level="muted",
            message=(
                f"Fetching {max(1, len(listing_batch_jobs))} listing page"
                f"{'' if len(listing_batch_jobs) == 1 else 's'} for {ctx.source_name}."
            ),
        )
        listing_results = fetch_pages_batched(
            ctx.run_deps.timeout_s,
            listing_batch_jobs,
            sync_fetch=_fetch_listing_job,
            async_fetch=_fetch_listing_job_async
            if ctx.run_deps.listing_async_fetch is not None
            else None,
            total_concurrency=max(1, len(listing_batch_jobs)),
            per_host_concurrency=common_config.DEFAULT_STATIC_FETCH_MAX_PER_DOMAIN,
            progress_callback=_on_listing_batch_progress,
        )
        ctx.stats["listing_batch_count"] = int(ctx.stats.get("listing_batch_count") or 0) + 1

        for result in listing_results:
            ctx.emit_heartbeat()
            if static_source_budget_exhausted(
                deadline_monotonic=float(ctx.source_deadline),
                reserve_s=0.0,
            ):
                stop_source = True
                break
            page_url = clean_text(result.get("url"))
            if not page_url:
                continue
            ctx.progress_state["listingPagesVisited"] += 1
            ctx.emit_source_progress(
                phase_key="static_listing_fetch",
                phase_label="Fetching listing pages",
                target_label=(
                    f"Listing {ctx.progress_state['listingPagesVisited']}/{max(1, len(ctx.pages))}"
                ),
                target_url=page_url,
                wait_reason="listing_batch",
                event_level="muted",
                message=(
                    f"Fetching listing page {ctx.progress_state['listingPagesVisited']}/{max(1, len(ctx.pages))} "
                    f"for {ctx.source_name}."
                ),
            )
            payload = result.get("payload") if isinstance(result.get("payload"), dict) else {}
            domain_profile = (
                payload.get("domainProfile")
                if isinstance(payload.get("domainProfile"), dict)
                else domain_profile_for_url(page_url)
            )
            source_budget_s = int(
                payload.get("sourceBudgetS") or ctx.runtime_config.static_source_time_budget_s
            )
            ctx.sync_source_deadline(source_budget_s)
            try:
                if not bool(result.get("ok")):
                    ctx.record_static_fetch_failure(
                        target_url=page_url,
                        exc=str(result.get("error") or ""),
                    )
                    ctx.emit_heartbeat()
                    continue
                if static_source_budget_exhausted(
                    deadline_monotonic=float(ctx.source_deadline),
                    reserve_s=0.0,
                ):
                    ctx.stop_for_budget_exhaustion(
                        target_url=page_url,
                        source_budget_s=source_budget_s,
                    )
                    stop_source = True
                    continue

                listing_meta = stage_state.batch_meta.get(page_url) or {}
                ctx.stats["listing_fetch_ms"] += int(listing_meta.get("durationMs") or 0)
                ctx.stats["listing_browser_fallbacks"] = int(
                    ctx.stats.get("listing_browser_fallbacks") or 0
                ) + int(bool(listing_meta.get("browserFallbackUsed")))
                if bool(listing_meta.get("cacheHit")):
                    ctx.stats["fetch_cache_hits"] += 1
                effective_timeout_s = int(listing_meta.get("timeoutS") or ctx.run_deps.timeout_s)
                html = str(result.get("text") or "")
                if ctx.run_deps.try_playwright and html and detect_js_shell(html):
                    dynamic_listing_timeout_s = effective_timeout_for_remaining_budget(
                        timeout_s=max(1, effective_timeout_s),
                        remaining_budget_s=ctx.remaining_budget_s(),
                    )
                    parsed_pre = parse_jobpostings_from_html(
                        html,
                        base_url=page_url,
                        fallback_company=ctx.company,
                        fallback_source_id_prefix=f"static:{ctx.source_name}",
                    )
                    link_count = len(re.findall(r'(?is)<a[^>]+href=["\']([^"\']+)["\']', html))
                    if not parsed_pre and link_count < 3 and dynamic_listing_timeout_s > 0:
                        html2, _ = ctx.run_deps.try_playwright(page_url, dynamic_listing_timeout_s)
                        print(
                            f"[static] playwright_fallback_used url={page_url!r} reason=js_shell got_html={bool(html2)}",
                            file=sys.stderr,
                            flush=True,
                        )
                        if html2:
                            stage_state.increment_browser_fallbacks()
                            html = html2
                listing_htmls = [html]
                try:
                    dynamic_listing_timeout_s = effective_timeout_for_remaining_budget(
                        timeout_s=ctx.run_deps.timeout_s,
                        remaining_budget_s=ctx.remaining_budget_s(),
                    )
                    if dynamic_listing_timeout_s > 0:
                        dynamic_listing_html = maybe_fetch_kojima_job_listing_html(
                            page_url=page_url,
                            page_html=html,
                            timeout_s=dynamic_listing_timeout_s,
                            retries=ctx.run_deps.retries,
                            backoff_s=ctx.run_deps.backoff_s,
                        )
                        if dynamic_listing_html and dynamic_listing_html not in listing_htmls:
                            listing_htmls.append(dynamic_listing_html)
                except Exception as exc:  # noqa: BLE001
                    ctx.errors.append(
                        f"static:{ctx.source_name}:{page_url}: dynamic-listing-fetch failed: {exc}"
                    )

                extraction_started = time.perf_counter()
                ctx.emit_source_progress(
                    phase_key="static_candidate_extraction",
                    phase_label="Extracting candidates",
                    target_label=(
                        f"Listing {ctx.progress_state['listingPagesVisited']}/{max(1, len(ctx.pages))}"
                    ),
                    target_url=page_url,
                    wait_reason="parsing",
                    event_level="muted",
                    message=f"Extracting listing candidates for {ctx.source_name}.",
                )
                listing_jobs_found, detail_links = _extract_listing_candidates(
                    ctx,
                    page_url=page_url,
                    source_budget_s=source_budget_s,
                    listing_htmls=listing_htmls,
                )
                ctx.stats["candidate_links_found"] += len(detail_links)
                ctx.stats["candidate_extraction_ms"] += int(
                    (time.perf_counter() - extraction_started) * 1000
                )
                ctx.emit_source_progress(
                    phase_key="static_candidate_extraction",
                    phase_label="Candidates extracted",
                    counts={
                        "detailCandidates": len(detail_links),
                        "listingJobsFound": listing_jobs_found,
                    },
                    target_label=(
                        f"Listing {ctx.progress_state['listingPagesVisited']}/{max(1, len(ctx.pages))}"
                    ),
                    target_url=page_url,
                    event_level="muted",
                    message=(
                        f"Found {len(detail_links)} detail candidate"
                        f"{'' if len(detail_links) == 1 else 's'} for {ctx.source_name}."
                    ),
                )
                listing_fingerprint = hashlib.sha1(
                    "\n".join(listing_htmls).encode("utf-8")
                ).hexdigest()
                previous_listing_fingerprint = clean_text(
                    (ctx.state_entry or {}).get("lastListingFingerprint")
                )
                ctx.entry_report["listingFingerprint"] = listing_fingerprint
                ctx.entry_report["listingCheckedAt"] = now_iso()
                ctx.entry_report["listingChanged"] = bool(
                    listing_fingerprint != previous_listing_fingerprint
                )
                if (
                    previous_listing_fingerprint
                    and listing_fingerprint == previous_listing_fingerprint
                    and not ctx.run_deps.force_refresh_all
                ):
                    ctx.entry_report["cacheDecision"] = "listing_only"
                    ctx.entry_report["cacheDecisionReason"] = "listing_fingerprint_unchanged"
                    ctx.entry_report["detailSkippedByListingFingerprint"] = True
                    ctx.stats["detail_skipped_by_listing_fingerprint"] += 1
                    detail_links = []

                if not detail_links:
                    ctx.emit_heartbeat()
                    continue
                if static_source_budget_exhausted(
                    deadline_monotonic=float(ctx.source_deadline),
                    reserve_s=1.0,
                ):
                    ctx.stop_for_budget_exhaustion(
                        target_url=page_url,
                        source_budget_s=source_budget_s,
                    )
                    stop_source = True
                    continue
                source_key = (
                    ctx.run_deps.diagnostics_name
                    if ctx.selected_source_count == 1
                    else ctx.source_name
                )
                plugin_meta = (
                    ctx.source.get("_staticPluginMeta") if isinstance(ctx.source, dict) else None
                )
                probable_detail_links = [
                    (detail, detail_title)
                    for detail, detail_title in detail_links
                    if is_probable_job_detail_url(
                        detail,
                        ctx.source,
                        default_path_tokens=ctx.runtime_config.default_path_tokens,
                        default_query_keys=ctx.runtime_config.default_query_keys,
                    )
                ]
                detail_traversal_mode = choose_detail_traversal_mode(
                    page_url,
                    runtime_config=ctx.runtime_config,
                    profile=domain_profile,
                    plugin_meta=plugin_meta,
                    listing_jobs_found=listing_jobs_found,
                    discovered_links=len(detail_links),
                    probable_detail_candidates=len(probable_detail_links),
                    source_key=source_key,
                    source_state_rows=ctx.run_deps.source_state_rows,
                )
                ctx.entry_report["detailTraversalMode"] = detail_traversal_mode
                if detail_traversal_mode == "listing_only":
                    ctx.emit_heartbeat()
                    continue
                source_has_listing_rows = ctx.current_source_kept_count() > 0
                if (
                    detail_links
                    and probable_detail_links
                    and (source_has_listing_rows or ctx.runtime_config.uncapped_deep_static)
                ):
                    detail_links = probable_detail_links
                if not detail_links:
                    ctx.emit_heartbeat()
                    continue
                detail_limit = source_detail_limit_for(
                    source_key,
                    source_state_rows=ctx.run_deps.source_state_rows,
                    discovered_links=len(detail_links),
                    listing_jobs_found=listing_jobs_found,
                    low_yield_detail_cap=ctx.runtime_config.low_yield_detail_cap,
                    very_low_yield_detail_cap=ctx.runtime_config.very_low_yield_detail_cap,
                    uncapped_deep_static=ctx.runtime_config.uncapped_deep_static,
                )
                detail_retries = source_detail_retries_for(
                    source_key,
                    source_state_rows=ctx.run_deps.source_state_rows,
                    base_retries=ctx.run_deps.retries,
                    uncapped_deep_static=ctx.runtime_config.uncapped_deep_static,
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
                detail_concurrency = source_detail_concurrency_for(
                    source_key,
                    source_state_rows=ctx.run_deps.source_state_rows,
                    static_detail_concurrency=ctx.run_deps.static_detail_concurrency,
                )
                ctx.emit_source_progress(
                    phase_key="static_detail_traversal",
                    phase_label="Traversing detail pages",
                    counts={"detailCandidates": len(detail_links)},
                    target_label=f"{len(detail_links)} detail page(s)",
                    target_url=page_url,
                    wait_reason="detail_batch",
                    event_level="muted",
                    message=(
                        f"Traversing {len(detail_links)} detail page"
                        f"{'' if len(detail_links) == 1 else 's'} for {ctx.source_name}."
                    ),
                )
                ctx.emit_heartbeat()
                stop_source = run_detail_traversal(
                    ctx,
                    StaticDetailTraversalPlan(
                        page_url=page_url,
                        detail_links=detail_links,
                        detail_concurrency=detail_concurrency,
                        detail_retries=detail_retries,
                        source_budget_s=source_budget_s,
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                ctx.record_static_fetch_failure(target_url=page_url, exc=exc)
                if ctx.current_source_kept_count() <= 0 and clean_text(
                    stage_state.terminal_reason
                ) in {
                    "blocked_after_browser_fallback",
                    "browser_fallback_empty",
                    "listing_timeout",
                    "listing_timeout_after_browser_fallback",
                }:
                    stop_source = True
                ctx.emit_heartbeat()

    _finish_generic_source(ctx, stage_state)
