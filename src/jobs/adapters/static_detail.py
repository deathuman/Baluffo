from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from src.jobs.adapters.static_runtime_support import (
    effective_timeout_for_remaining_budget,
    remaining_static_source_budget_s,
    static_source_budget_exhausted,
)
from src.jobs.text_utils import clean_text, normalize_url
from src.shared.http_batch import fetch_pages_batched

from .static_runtime import StaticSourceContext

root: Any | None = None


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


@dataclass(frozen=True)
class StaticDetailTraversalPlan:
    page_url: str
    detail_links: list[tuple[str, str]]
    detail_concurrency: int
    detail_retries: int
    source_budget_s: int


def _root_module():
    if root is not None:
        return root
    from src.jobs.adapters import static as static_root

    return static_root


def run_detail_traversal(ctx: StaticSourceContext, plan: StaticDetailTraversalPlan) -> bool:
    detail_fetch_started = time.perf_counter()
    detail_fetch_base_ms = int(ctx.stats.get("detail_fetch_ms") or 0)
    detail_candidate_count = len(plan.detail_links)
    detail_off_domain_failure_count = 0
    detail_redirect_loop_count = 0
    detail_state = {
        "stop": False,
        "index": 0,
    }
    stop_source = False

    def stop_detail_traversal_adaptively() -> None:
        detail_state["stop"] = True
        remaining_candidates = max(
            0,
            len(plan.detail_links) - int(detail_state.get("index") or 0),
        )
        if remaining_candidates > 0:
            ctx.stats["detail_pages_skipped_by_adaptive_stop"] = (
                int(ctx.stats.get("detail_pages_skipped_by_adaptive_stop") or 0)
                + remaining_candidates
            )

    def next_detail_batch_size(*, remaining_budget_s: float) -> int:
        batch_size = max(1, int(plan.detail_concurrency))
        current_gate_wait_ms, current_gate_wait_count = ctx.current_domain_gate_wait_stats()
        ctx.stats["domain_gate_wait_ms"] = int(current_gate_wait_ms)
        ctx.stats["domain_gate_wait_count"] = int(current_gate_wait_count)
        if (
            current_gate_wait_ms > 0
            and int(detail_state.get("index") or 0) > 0
            and current_gate_wait_ms >= int(ctx.stats.get("detail_fetch_ms") or 0)
        ):
            batch_size = 1
        if remaining_budget_s < 8.0:
            batch_size = 1
        batch_budget_cap = max(1, int(max(0.0, remaining_budget_s) // 3.0))
        batch_size = min(batch_size, batch_budget_cap)
        return max(1, batch_size)

    while int(detail_state.get("index") or 0) < len(plan.detail_links):
        if bool(detail_state.get("stop")) or stop_source:
            break
        if static_source_budget_exhausted(
            deadline_monotonic=float(ctx.source_deadline),
            reserve_s=1.0,
        ):
            ctx.stop_for_budget_exhaustion(
                target_url=plan.page_url,
                source_budget_s=plan.source_budget_s,
            )
            stop_source = True
            break
        remaining_budget_s = remaining_static_source_budget_s(
            deadline_monotonic=float(ctx.source_deadline)
        )
        detail_batch_size = next_detail_batch_size(remaining_budget_s=remaining_budget_s)
        detail_batch_start = int(detail_state.get("index") or 0)
        detail_batch = plan.detail_links[
            detail_batch_start : detail_batch_start + min(detail_batch_size, len(plan.detail_links))
        ]
        detail_batch_meta: dict[str, dict[str, Any]] = {}
        ctx.stats["detail_batch_count"] = int(ctx.stats.get("detail_batch_count") or 0) + 1

        def _fetch_detail_job(
            batch_job: dict[str, Any],
            url: str,
            _timeout_s: int,
            _detail_batch_meta: dict[str, dict[str, Any]] = detail_batch_meta,
        ) -> str:
            del batch_job, _timeout_s
            fetch_started = time.perf_counter()
            ctx.sync_source_deadline(plan.source_budget_s)
            current_remaining_budget_s = remaining_static_source_budget_s(
                deadline_monotonic=float(ctx.source_deadline)
            )
            effective_timeout_s = effective_timeout_for_remaining_budget(
                timeout_s=ctx.run_deps.timeout_s,
                remaining_budget_s=current_remaining_budget_s,
            )
            if effective_timeout_s <= 0:
                raise TimeoutError(f"time budget exceeded ({plan.source_budget_s}s)")
            html, cache_hit = ctx.html_fetcher.fetch_html_cached(
                url,
                remaining_budget_s=current_remaining_budget_s,
                retries_override=plan.detail_retries,
            )
            _detail_batch_meta[url] = {
                "cacheHit": cache_hit,
                "fetchMs": int((time.perf_counter() - fetch_started) * 1000),
                "timeoutS": effective_timeout_s,
            }
            return html

        def _on_detail_batch_progress(
            completed: int,
            total: int,
            _detail_batch_start: int = detail_batch_start,
        ) -> None:
            completed_count = max(0, int(completed or 0))
            total_count = max(1, int(total or 0))
            ctx.emit_heartbeat()
            ctx.emit_source_progress(
                phase_key="static_detail_traversal",
                phase_label="Traversing detail pages",
                counts={
                    "detailCandidates": detail_candidate_count,
                    "detailPagesFetched": min(
                        detail_candidate_count,
                        _detail_batch_start + completed_count,
                    ),
                },
                target_label=(
                    f"Detail fetch "
                    f"{min(detail_candidate_count, _detail_batch_start + completed_count)}/"
                    f"{detail_candidate_count}"
                ),
                target_url=plan.page_url,
                wait_reason="detail_batch",
                event_level="muted",
                message=(
                    f"Fetched {completed_count}/{total_count} detail page"
                    f"{'' if total_count == 1 else 's'} for {ctx.source_name}."
                ),
            )

        detail_results = fetch_pages_batched(
            ctx.run_deps.timeout_s,
            [
                {
                    "url": detail,
                    "payload": {"detailTitle": detail_title},
                }
                for detail, detail_title in detail_batch
            ],
            sync_fetch=_fetch_detail_job,
            total_concurrency=plan.detail_concurrency,
            per_host_concurrency=plan.detail_concurrency,
            progress_callback=_on_detail_batch_progress,
        )

        for detail_result_row in detail_results:
            if static_source_budget_exhausted(
                deadline_monotonic=float(ctx.source_deadline),
                reserve_s=0.0,
            ):
                stop_source = True
                ctx.stop_for_budget_exhaustion(
                    target_url=plan.page_url,
                    source_budget_s=plan.source_budget_s,
                )
                break
            detail = clean_text(detail_result_row.get("url"))
            if not detail:
                continue
            detail_payload = _as_dict(detail_result_row.get("payload"))
            detail_title = clean_text(detail_payload.get("detailTitle"))
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
                msg = str(detail_result_row.get("error") or "")
                linked_in_throttle = "linkedin" in f"{plan.page_url} {msg}".lower()
                detail_host = (urlparse(detail).netloc or "").strip().lower()
                page_host = (urlparse(plan.page_url).netloc or "").strip().lower()
                if "Exceeded maximum allowed redirects" in msg:
                    detail_redirect_loop_count += 1
                if detail_host and page_host and detail_host != page_host:
                    detail_off_domain_failure_count += 1
                if "HTTP 403" in msg or (
                    linked_in_throttle and ("HTTP 429" in msg or "Too Many Requests" in msg)
                ):
                    ctx.entry_report["classification"] = "blocked_or_challenge"
                    ctx.entry_report["browserFallbackRecommended"] = True
                    ctx.entry_report["error"] = msg
                    ctx.warnings.append(f"static:{ctx.source_name}:{detail}: {msg}")
                else:
                    ctx.errors.append(f"static:{ctx.source_name}:{detail}: {msg}")
                if detail_redirect_loop_count >= 2 or detail_off_domain_failure_count >= 2:
                    stop_detail_traversal_adaptively()
                continue
            detail_meta = detail_batch_meta.get(detail) or {}
            detail_result = _root_module().process_detail_html(
                detail=detail,
                detail_title=detail_title,
                detail_html=str(detail_result_row.get("text") or ""),
                fetch_ms=int(detail_meta.get("fetchMs") or 0),
                cache_hit=bool(detail_meta.get("cacheHit")),
                company=ctx.company,
                source_name=ctx.source_name,
                source=ctx.source,
                ignored_link_titles=ctx.ignored_link_titles,
            )
            ctx.stats["fetch_cache_hits"] += 1 if detail_result.get("cacheHit") else 0
            ctx.stats["detail_fetch_ms"] += int(detail_result.get("fetchMs") or 0)
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
            for row in detail_result.get("rows") or []:
                link = normalize_url(row.get("jobLink"))
                if not link or link in ctx.seen_links:
                    continue
                ctx.seen_links.add(link)
                ctx.jobs.append(row)
            ctx.emit_heartbeat()

        if stop_source:
            break
        detail_state["index"] = detail_batch_start + len(detail_batch)

    ctx.stats["detail_fetch_ms"] += max(
        0,
        int((time.perf_counter() - detail_fetch_started) * 1000)
        - max(0, int(ctx.stats.get("detail_fetch_ms") or 0) - detail_fetch_base_ms),
    )
    current_gate_wait_ms, current_gate_wait_count = ctx.current_domain_gate_wait_stats()
    ctx.stats["domain_gate_wait_ms"] = int(current_gate_wait_ms)
    ctx.stats["domain_gate_wait_count"] = int(current_gate_wait_count)
    return stop_source
