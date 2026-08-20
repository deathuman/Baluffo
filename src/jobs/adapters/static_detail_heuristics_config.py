"""Static detail-page heuristics - traversal limits and concurrency.

AI boundary owns: per-source detail concurrency/limit/retry caps, tail metrics, and the traversal-mode decision.
AI boundary implement in: this static_detail_heuristics_config.py leaf.
AI boundary search before contracts: static listing/runtime, page gating, and detail heuristic tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused static detail tests."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from src.jobs.text_utils import clean_text
from src.shared.json_shapes import as_json_object as _as_dict

from .static_runtime_support import StaticSourceRuntimeConfig


def source_detail_concurrency_for(
    source_key: str,
    *,
    source_state_rows: dict[str, dict[str, Any]] | None,
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


def _source_tail_metrics(
    source_key: str,
    *,
    source_state_rows: dict[str, dict[str, Any]] | None,
) -> dict[str, int]:
    entry = (source_state_rows or {}).get(source_key) if isinstance(source_state_rows, dict) else {}
    if not isinstance(entry, dict):
        return {}
    stage_timings = _as_dict(entry.get("lastStageTimingsMs"))
    return {
        "last_detail_pages": int(entry.get("lastDetailPagesVisited") or 0),
        "last_kept": int(entry.get("lastKeptCount") or 0),
        "last_duration_ms": int(entry.get("lastDurationMs") or 0),
        "last_detail_yield_pct": int(entry.get("lastDetailYieldPct") or 0),
        "last_detail_fetch_ms": int(stage_timings.get("detailFetch") or 0),
        "last_listing_fetch_ms": int(stage_timings.get("listingFetch") or 0),
    }


def _detail_limit_cap(
    metrics: dict[str, int],
    *,
    listing_jobs_found: int,
    low_yield_detail_cap: int,
    very_low_yield_detail_cap: int,
) -> int:
    pages = metrics["last_detail_pages"]
    kept = metrics["last_kept"]
    duration_ms = metrics["last_duration_ms"]
    yield_pct = metrics["last_detail_yield_pct"]
    fetch_ms = metrics["last_detail_fetch_ms"]
    if fetch_ms >= 120_000 or duration_ms >= 120_000 or pages >= 60:
        if listing_jobs_found > 0 and (yield_pct <= 20 or kept <= 1):
            return max(1, min(very_low_yield_detail_cap, 4))
        return very_low_yield_detail_cap
    if fetch_ms >= 60_000 or duration_ms >= 90_000 or pages >= 30:
        return low_yield_detail_cap if yield_pct <= 20 or kept <= 1 else very_low_yield_detail_cap
    if fetch_ms >= 30_000 or duration_ms >= 45_000 or pages >= 20:
        return low_yield_detail_cap if yield_pct <= 15 else very_low_yield_detail_cap
    if pages >= 30 and kept <= 1 and duration_ms >= 45_000:
        return very_low_yield_detail_cap if listing_jobs_found > 0 else low_yield_detail_cap
    if pages >= 20 and duration_ms >= 20_000 and yield_pct <= 5:
        return low_yield_detail_cap if listing_jobs_found <= 0 else very_low_yield_detail_cap
    if listing_jobs_found > 0 and pages >= 10 and yield_pct <= 10:
        return very_low_yield_detail_cap
    return 0


def source_detail_limit_for(
    source_key: str,
    *,
    source_state_rows: dict[str, dict[str, Any]] | None,
    discovered_links: int,
    listing_jobs_found: int,
    low_yield_detail_cap: int,
    very_low_yield_detail_cap: int,
    uncapped_deep_static: bool = False,
) -> int:
    if discovered_links <= 0:
        return 0
    if (
        uncapped_deep_static
        and int(low_yield_detail_cap or 0) <= 0
        and int(very_low_yield_detail_cap or 0) <= 0
    ):
        return discovered_links
    metrics = _source_tail_metrics(source_key, source_state_rows=source_state_rows)
    if not metrics:
        return discovered_links
    cap = _detail_limit_cap(
        metrics,
        listing_jobs_found=listing_jobs_found,
        low_yield_detail_cap=low_yield_detail_cap,
        very_low_yield_detail_cap=very_low_yield_detail_cap,
    )
    return min(discovered_links, max(1, cap)) if cap else discovered_links


def source_detail_retries_for(
    source_key: str,
    *,
    source_state_rows: dict[str, dict[str, Any]] | None,
    base_retries: int,
    listing_jobs_found: int = 0,
    uncapped_deep_static: bool = False,
) -> int:
    metrics = _source_tail_metrics(source_key, source_state_rows=source_state_rows)
    retries = max(0, int(base_retries or 0))
    if uncapped_deep_static:
        return retries
    if not metrics:
        if listing_jobs_found > 0:
            return 0
        return retries
    last_duration_ms = metrics["last_duration_ms"]
    last_detail_fetch_ms = metrics["last_detail_fetch_ms"]
    last_detail_pages = metrics["last_detail_pages"]

    if last_detail_fetch_ms >= 120_000 or last_duration_ms >= 120_000 or last_detail_pages >= 60:
        return 0
    if last_detail_fetch_ms >= 60_000 or last_duration_ms >= 90_000 or last_detail_pages >= 30:
        return min(retries, 1)
    if last_detail_fetch_ms >= 30_000 or last_duration_ms >= 45_000 or last_detail_pages >= 20:
        return min(retries, 1)
    return retries


def _should_skip_detail_for_tail(metrics: dict[str, int], *, listing_jobs_found: int) -> bool:
    if not metrics or listing_jobs_found <= 0:
        return False
    slow_or_deep = (
        metrics["last_detail_fetch_ms"] >= 120_000
        or metrics["last_duration_ms"] >= 120_000
        or metrics["last_detail_pages"] >= 40
    )
    low_yield = metrics["last_detail_yield_pct"] <= 20 or metrics["last_kept"] <= 1
    return slow_or_deep and low_yield


def choose_detail_traversal_mode(
    page_url: str,
    *,
    runtime_config: StaticSourceRuntimeConfig,
    profile: dict[str, Any] | None,
    plugin_meta: dict[str, Any] | None,
    listing_jobs_found: int,
    discovered_links: int,
    source_key: str,
    source_state_rows: dict[str, dict[str, Any]] | None,
    probable_detail_candidates: int = 0,
) -> str:
    plugin_meta = plugin_meta if isinstance(plugin_meta, dict) else {}
    profile = profile if isinstance(profile, dict) else {}
    allow_override = bool(
        runtime_config.uncapped_deep_static and int(probable_detail_candidates or 0) > 0
    )
    explicit_mode = clean_text(plugin_meta.get("detailTraversalMode")) or clean_text(
        profile.get("detail_traversal_mode")
    )
    if explicit_mode in {"listing_only", "capped_detail", "full_detail"}:
        if explicit_mode != "listing_only" or not allow_override:
            return explicit_mode
    detail_fetch_required = plugin_meta.get("detailFetchRequired")
    if detail_fetch_required is None:
        detail_fetch_required = profile.get("detail_fetch_required")
    if detail_fetch_required is False and listing_jobs_found > 0 and not allow_override:
        return "listing_only"
    metrics = _source_tail_metrics(source_key, source_state_rows=source_state_rows)
    if _should_skip_detail_for_tail(metrics, listing_jobs_found=listing_jobs_found):
        if not allow_override:
            return "listing_only"
    host = (urlparse(clean_text(page_url) or "").hostname or "").lower()
    if host in runtime_config.listing_only_hosts and listing_jobs_found > 0 and not allow_override:
        return "listing_only"
    detail_limit = source_detail_limit_for(
        source_key,
        source_state_rows=source_state_rows,
        discovered_links=discovered_links,
        listing_jobs_found=listing_jobs_found,
        low_yield_detail_cap=runtime_config.low_yield_detail_cap,
        very_low_yield_detail_cap=runtime_config.very_low_yield_detail_cap,
        uncapped_deep_static=runtime_config.uncapped_deep_static,
    )
    return "capped_detail" if detail_limit < discovered_links else "full_detail"
