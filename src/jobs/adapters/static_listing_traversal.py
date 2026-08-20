"""Static listing detail traversal engine.

AI boundary owns: adaptive detail traversal (plan/state, batch scheduling, fetch error
accounting, nested candidate fanout) and the `_run_static_detail_traversal` entrypoint used by
the fetch runner.
AI boundary implement in: this leaf for detail traversal; fanout caps come from
``static_listing_common.py`` and nested limits from ``static_listing_state.py``. ``process_detail_html``
is imported directly from ``static_detail_heuristics.py`` (no coordinator round-trip); tests that
fake detail parsing patch ``static_listing_traversal.process_detail_html``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from src.jobs.adapters.static_detail_heuristics import process_detail_html
from src.jobs.adapters.static_listing_common import (
    _EXTERNAL_DETAIL_FANOUT_LINK_CAP,
    StaticDetailCandidate,
    _cap_external_detail_fanout,
    _effective_timeout_or_raise,
)
from src.jobs.adapters.static_listing_state import (
    _is_provisional_static_artifact_row,
    _nested_detail_limit_for,
)
from src.jobs.adapters.static_runtime_support import (
    remaining_static_source_budget_s,
    static_source_budget_exhausted,
)
from src.jobs.text_utils import clean_text, normalize_url
from src.scrapers.domain_profiles import domain_profile_for_url
from src.shared.http_batch import fetch_pages_batched
from src.shared.json_shapes import as_json_object as _as_dict

from .static_runtime import StaticSourceContext


@dataclass(frozen=True)
class StaticDetailTraversalPlan:
    page_url: str
    detail_links: list[StaticDetailCandidate]
    detail_concurrency: int
    detail_retries: int
    source_budget_s: int


@dataclass
class _StaticDetailTraversalState:
    index: int = 0
    stop: bool = False
    stop_source: bool = False
    off_domain_failure_count: int = 0
    redirect_loop_count: int = 0
    scheduled_urls: set[str] = field(default_factory=set)


# mutation — modifies in-place state
def _stop_detail_traversal_adaptively(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    state: _StaticDetailTraversalState,
) -> None:
    state.stop = True
    remaining_candidates = max(0, len(plan.detail_links) - int(state.index or 0))
    if remaining_candidates > 0:
        ctx.stats["detail_pages_skipped_by_adaptive_stop"] = (
            int(ctx.stats.get("detail_pages_skipped_by_adaptive_stop") or 0) + remaining_candidates
        )


# mutation — modifies in-place state
def _next_detail_batch_size(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    state: _StaticDetailTraversalState,
    *,
    remaining_budget_s: float,
) -> int:
    batch_size = max(1, int(plan.detail_concurrency))
    current_gate_wait_ms, current_gate_wait_count = ctx.current_domain_gate_wait_stats()
    ctx.stats["domain_gate_wait_ms"] = int(current_gate_wait_ms)
    ctx.stats["domain_gate_wait_count"] = int(current_gate_wait_count)
    if (
        current_gate_wait_ms > 0
        and int(state.index or 0) > 0
        and current_gate_wait_ms >= int(ctx.stats.get("detail_fetch_ms") or 0)
    ):
        batch_size = 1
    if remaining_budget_s < 8.0:
        batch_size = 1
    batch_budget_cap = max(1, int(max(0.0, remaining_budget_s) // 3.0))
    return max(1, min(batch_size, batch_budget_cap))


# pure — builds fetch job dicts
def _build_detail_batch_jobs(detail_batch: list[StaticDetailCandidate]) -> list[dict[str, Any]]:
    return [
        {
            "url": candidate.url,
            "payload": {
                "detailTitle": candidate.title,
                "detailDepth": candidate.depth,
                "parentUrl": candidate.parent_url,
            },
        }
        for candidate in detail_batch
    ]


# network — makes HTTP requests
def _fetch_detail_job(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    detail_batch_meta: dict[str, dict[str, Any]],
    batch_job: dict[str, Any],
    url: str,
    _timeout_s: int,
) -> str:
    del batch_job, _timeout_s
    fetch_started = time.perf_counter()
    ctx.sync_source_deadline(plan.source_budget_s)
    current_remaining_budget_s = remaining_static_source_budget_s(
        deadline_monotonic=float(ctx.source_deadline)
    )
    effective_timeout_s = _effective_timeout_or_raise(
        timeout_s=ctx.run_deps.timeout_s,
        remaining_budget_s=current_remaining_budget_s,
        source_budget_s=plan.source_budget_s,
    )
    html, cache_hit = ctx.html_fetcher.fetch_html_cached(
        url,
        remaining_budget_s=current_remaining_budget_s,
        retries_override=plan.detail_retries,
    )
    detail_batch_meta[url] = {
        "cacheHit": cache_hit,
        "fetchMs": int((time.perf_counter() - fetch_started) * 1000),
        "timeoutS": effective_timeout_s,
    }
    return html


# mutation — modifies in-place state
def _emit_detail_batch_progress(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    detail_batch_start: int,
    completed: int,
    total: int,
) -> None:
    completed_count = max(0, int(completed or 0))
    total_count = max(1, int(total or 0))
    detail_candidate_count = len(plan.detail_links)
    fetched_count = min(detail_candidate_count, detail_batch_start + completed_count)
    ctx.emit_heartbeat()
    ctx.emit_source_progress(
        phase_key="static_detail_traversal",
        phase_label="Traversing detail pages",
        counts={
            "detailCandidates": detail_candidate_count,
            "detailPagesFetched": fetched_count,
        },
        target_label=f"Detail fetch {fetched_count}/{detail_candidate_count}",
        target_url=plan.page_url,
        wait_reason="detail_batch",
        event_level="muted",
        message=(
            f"Fetched {completed_count}/{total_count} detail page"
            f"{'' if total_count == 1 else 's'} for {ctx.source_name}."
        ),
    )


# mutation — modifies in-place state
def _record_detail_fetch_error(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    state: _StaticDetailTraversalState,
    *,
    detail: str,
    msg: str,
) -> None:
    linked_in_throttle = "linkedin" in f"{plan.page_url} {msg}".lower()
    detail_host = (urlparse(detail).netloc or "").strip().lower()
    page_host = (urlparse(plan.page_url).netloc or "").strip().lower()
    if "Exceeded maximum allowed redirects" in msg:
        state.redirect_loop_count += 1
    if detail_host and page_host and detail_host != page_host:
        state.off_domain_failure_count += 1
    if "HTTP 403" in msg or (
        linked_in_throttle and ("HTTP 429" in msg or "Too Many Requests" in msg)
    ):
        ctx.entry_report["classification"] = "blocked_or_challenge"
        ctx.entry_report["browserFallbackRecommended"] = True
        ctx.entry_report["error"] = msg
        ctx.warnings.append(f"static:{ctx.source_name}:{detail}: {msg}")
    else:
        ctx.errors.append(f"static:{ctx.source_name}:{detail}: {msg}")
    if state.redirect_loop_count >= 2 or state.off_domain_failure_count >= 2:
        _stop_detail_traversal_adaptively(ctx, plan, state)


# mutation — modifies in-place state
def _record_detail_rejection(ctx: StaticSourceContext, detail_result: dict[str, Any]) -> None:
    rejected_classification = clean_text(detail_result.get("rejectedClassification"))
    if rejected_classification == "dead_listing_page":
        ctx.link_rejections["dead_listing_page"] += 1
        ctx.stats["dead_listing_pages_rejected"] += 1
        if len(ctx.dead_listing_page_examples) < 5:
            example = clean_text(detail_result.get("rejectedExample"))
            if example:
                ctx.dead_listing_page_examples.append(example)
    elif detail_result.get("parseEmpty"):
        ctx.link_rejections["detail_parse_empty"] += 1


def _append_detail_result_rows(ctx: StaticSourceContext, rows: list[dict[str, Any]]) -> int:
    appended = 0
    for row in rows:
        if not isinstance(row, dict) or _is_provisional_static_artifact_row(row):
            continue
        link = normalize_url(row.get("jobLink"))
        if not link or link in ctx.seen_links:
            continue
        ctx.seen_links.add(link)
        ctx.jobs.append(row)
        appended += 1
    return appended


def _nested_detail_candidates(
    ctx: StaticSourceContext,
    state: _StaticDetailTraversalState,
    *,
    parent_url: str,
    parent_depth: int,
    nested_links: list[dict[str, Any]],
) -> list[StaticDetailCandidate]:
    child_candidates: list[StaticDetailCandidate] = []
    for item in nested_links:
        if not isinstance(item, dict):
            continue
        child_url = normalize_url(item.get("url"))
        if not child_url or child_url in ctx.seen_links or child_url in state.scheduled_urls:
            continue
        child_candidates.append(
            StaticDetailCandidate(
                url=child_url,
                title=clean_text(item.get("title")),
                depth=parent_depth + 1,
                parent_url=parent_url,
            )
        )
    return child_candidates


def _cap_nested_detail_candidates(
    ctx: StaticSourceContext,
    *,
    parent_url: str,
    child_candidates: list[StaticDetailCandidate],
) -> list[StaticDetailCandidate]:
    if not child_candidates:
        return []
    nested_profile = domain_profile_for_url(parent_url)
    profile_external_cap = max(0, int(nested_profile.get("max_external_detail_links") or 0))
    child_candidates = _cap_external_detail_fanout(
        ctx,
        page_url=parent_url,
        detail_links=child_candidates,
        cap=profile_external_cap or _EXTERNAL_DETAIL_FANOUT_LINK_CAP,
    )
    nested_limit = _nested_detail_limit_for(ctx, len(child_candidates))
    profile_max_detail_links = max(0, int(nested_profile.get("max_detail_links") or 0))
    if profile_max_detail_links > 0:
        nested_limit = (
            min(nested_limit, profile_max_detail_links)
            if nested_limit
            else profile_max_detail_links
        )
    if nested_limit and nested_limit < len(child_candidates):
        return child_candidates[:nested_limit]
    return child_candidates


def _schedule_nested_detail_candidates(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    state: _StaticDetailTraversalState,
    child_candidates: list[StaticDetailCandidate],
) -> int:
    added = 0
    for candidate in child_candidates:
        if candidate.url in ctx.seen_links or candidate.url in state.scheduled_urls:
            continue
        state.scheduled_urls.add(candidate.url)
        plan.detail_links.append(candidate)
        added += 1
    if added:
        ctx.stats["candidate_links_found"] = (
            int(ctx.stats.get("candidate_links_found") or 0) + added
        )
    return added


def _enqueue_nested_detail_links(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    state: _StaticDetailTraversalState,
    *,
    parent_url: str,
    parent_depth: int,
    nested_links: list[dict[str, Any]],
) -> int:
    if parent_depth >= 1:
        return 0
    child_candidates = _nested_detail_candidates(
        ctx,
        state,
        parent_url=parent_url,
        parent_depth=parent_depth,
        nested_links=nested_links,
    )
    if not child_candidates:
        return 0
    child_candidates = _cap_nested_detail_candidates(
        ctx,
        parent_url=parent_url,
        child_candidates=child_candidates,
    )
    return _schedule_nested_detail_candidates(ctx, plan, state, child_candidates)


# mutation — modifies in-place state
def _process_detail_result_row(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    state: _StaticDetailTraversalState,
    detail_batch_meta: dict[str, dict[str, Any]],
    detail_result_row: dict[str, Any],
) -> None:
    detail = clean_text(detail_result_row.get("url"))
    if not detail:
        return
    detail_payload = _as_dict(detail_result_row.get("payload"))
    detail_title = clean_text(detail_payload.get("detailTitle"))
    detail_depth = max(0, int(detail_payload.get("detailDepth") or 0))
    ctx.stats["detail_pages_visited"] += 1
    ctx.emit_source_progress(
        phase_key="static_detail_traversal",
        phase_label="Traversing detail pages",
        target_label=detail_title or detail or ctx.source_name,
        target_url=detail,
        wait_reason="parsing",
    )
    ctx.emit_heartbeat()
    if not bool(detail_result_row.get("ok")):
        _record_detail_fetch_error(
            ctx,
            plan,
            state,
            detail=detail,
            msg=str(detail_result_row.get("error") or ""),
        )
        return
    detail_meta = detail_batch_meta.get(detail) or {}
    detail_result = process_detail_html(
        detail=detail,
        detail_title=detail_title,
        detail_html=str(detail_result_row.get("text") or ""),
        fetch_ms=int(detail_meta.get("fetchMs") or 0),
        cache_hit=bool(detail_meta.get("cacheHit")),
        company=ctx.company,
        source_name=ctx.source_name,
        source=ctx.source,
        ignored_link_titles=ctx.ignored_link_titles,
        default_path_tokens=ctx.runtime_config.default_path_tokens,
        default_query_keys=ctx.runtime_config.default_query_keys,
    )
    ctx.stats["fetch_cache_hits"] += 1 if detail_result.get("cacheHit") else 0
    ctx.stats["detail_fetch_ms"] += int(detail_result.get("fetchMs") or 0)
    appended_rows = _append_detail_result_rows(ctx, detail_result.get("rows") or [])
    nested_scheduled = 0
    if appended_rows == 0:
        nested_scheduled = _enqueue_nested_detail_links(
            ctx,
            plan,
            state,
            parent_url=detail,
            parent_depth=detail_depth,
            nested_links=detail_result.get("nestedDetailLinks") or [],
        )
    if appended_rows == 0 and nested_scheduled == 0:
        _record_detail_rejection(ctx, detail_result)
    ctx.emit_heartbeat()


# pure — budget check with optional source stop
def _detail_budget_exhausted(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    *,
    reserve_s: float,
) -> bool:
    if not static_source_budget_exhausted(
        deadline_monotonic=float(ctx.source_deadline),
        reserve_s=reserve_s,
    ):
        return False
    ctx.stop_for_budget_exhaustion(
        target_url=plan.page_url,
        source_budget_s=plan.source_budget_s,
    )
    return True


# network — makes HTTP requests
def _run_detail_batch(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    state: _StaticDetailTraversalState,
) -> None:
    remaining_budget_s = remaining_static_source_budget_s(
        deadline_monotonic=float(ctx.source_deadline)
    )
    detail_batch_size = _next_detail_batch_size(
        ctx,
        plan,
        state,
        remaining_budget_s=remaining_budget_s,
    )
    detail_batch_start = int(state.index or 0)
    detail_batch = plan.detail_links[
        detail_batch_start : detail_batch_start + min(detail_batch_size, len(plan.detail_links))
    ]
    detail_batch_meta: dict[str, dict[str, Any]] = {}
    ctx.stats["detail_batch_count"] = int(ctx.stats.get("detail_batch_count") or 0) + 1
    detail_results = fetch_pages_batched(
        ctx.run_deps.timeout_s,
        _build_detail_batch_jobs(detail_batch),
        sync_fetch=lambda batch_job, url, timeout_s: _fetch_detail_job(
            ctx,
            plan,
            detail_batch_meta,
            batch_job,
            url,
            timeout_s,
        ),
        total_concurrency=plan.detail_concurrency,
        per_host_concurrency=plan.detail_concurrency,
        progress_callback=lambda completed, total: _emit_detail_batch_progress(
            ctx,
            plan,
            detail_batch_start,
            completed,
            total,
        ),
    )
    for detail_result_row in detail_results:
        if _detail_budget_exhausted(ctx, plan, reserve_s=0.0):
            state.stop_source = True
            break
        _process_detail_result_row(ctx, plan, state, detail_batch_meta, detail_result_row)
    if not state.stop_source:
        state.index = detail_batch_start + len(detail_batch)


# mutation — modifies in-place state
def _finish_detail_traversal(
    ctx: StaticSourceContext,
    *,
    detail_fetch_started: float,
    detail_fetch_base_ms: int,
) -> None:
    ctx.stats["detail_fetch_ms"] += max(
        0,
        int((time.perf_counter() - detail_fetch_started) * 1000)
        - max(0, int(ctx.stats.get("detail_fetch_ms") or 0) - detail_fetch_base_ms),
    )
    current_gate_wait_ms, current_gate_wait_count = ctx.current_domain_gate_wait_stats()
    ctx.stats["domain_gate_wait_ms"] = int(current_gate_wait_ms)
    ctx.stats["domain_gate_wait_count"] = int(current_gate_wait_count)


# orchestration — coordinates network + mutation
def _run_static_detail_traversal(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
) -> bool:
    detail_fetch_started = time.perf_counter()
    detail_fetch_base_ms = int(ctx.stats.get("detail_fetch_ms") or 0)
    state = _StaticDetailTraversalState()
    state.scheduled_urls = {candidate.url for candidate in plan.detail_links if candidate.url}
    while int(state.index or 0) < len(plan.detail_links):
        if state.stop or state.stop_source:
            break
        if _detail_budget_exhausted(ctx, plan, reserve_s=1.0):
            state.stop_source = True
            break
        _run_detail_batch(ctx, plan, state)
    _finish_detail_traversal(
        ctx,
        detail_fetch_started=detail_fetch_started,
        detail_fetch_base_ms=detail_fetch_base_ms,
    )
    return state.stop_source
