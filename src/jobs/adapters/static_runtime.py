from __future__ import annotations

import json
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from src.jobs.models import RawJob
from src.jobs.state_incremental import get_incremental_cache_decision
from src.jobs.text_utils import clean_text

from ..common import config as common_config
from .static_runtime_support import (
    StaticHtmlFetcher,
    StaticSourceRuntimeConfig,
    _as_dict,
    build_static_entry_report,
    build_static_source_deadline,
    classify_static_fetch_exception,
    remaining_static_source_budget_s,
)


def _default_ignored_link_titles() -> set[str]:
    return {
        "apply",
        "apply now",
        "learn more",
        "read more",
        "details",
        "view",
        "view details",
        "view job",
    }


def _as_pages(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [clean_text(item) for item in value]


@dataclass(frozen=True)
class StaticRunDeps:
    fetch_text: Callable[[str, int], str]
    timeout_s: int
    retries: int
    backoff_s: float
    diagnostics_name: str = "static_studio_pages"
    heartbeat_callback: Callable[[], None] | None = None
    progress_callback: Callable[..., None] | None = None
    static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY
    source_state_rows: dict[str, dict[str, Any]] | None = None
    listing_async_fetch: Callable[[Any, dict[str, Any], str, int], Awaitable[str]] | None = None
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None
    force_refresh_all: bool = False


@dataclass
class StaticSourceContext:
    run_deps: StaticRunDeps
    runtime_config: StaticSourceRuntimeConfig
    html_fetcher: StaticHtmlFetcher
    source: dict[str, Any]
    source_name: str
    company: str
    pages: list[str]
    entry_report: dict[str, Any]
    state_entry: dict[str, Any]
    selected_source_count: int
    jobs: list[RawJob]
    warnings: list[str]
    errors: list[str]
    details: list[dict[str, Any]]
    source_started: float = field(default_factory=time.perf_counter)
    source_deadline: float = 0.0
    kept_before: int = 0
    progress_state: dict[str, Any] = field(
        default_factory=lambda: {
            "listingPagesVisited": 0,
            "lastProgressSignature": "",
        }
    )
    seen_links: set[str] = field(default_factory=set)
    dead_listing_page_examples: list[str] = field(default_factory=list)
    link_rejections: Counter[str] = field(default_factory=Counter)
    ignored_link_titles: set[str] = field(default_factory=_default_ignored_link_titles)

    def __post_init__(self) -> None:
        if not self.source_deadline:
            self.source_deadline = build_static_source_deadline(
                source_started=self.source_started,
                source_budget_s=self.runtime_config.static_source_time_budget_s,
            )
        if not self.kept_before:
            self.kept_before = len(self.jobs)

    @property
    def stats(self) -> dict[str, Any]:
        stats_value = self.entry_report.get("stats")
        if isinstance(stats_value, dict):
            return stats_value
        stats: dict[str, Any] = {}
        self.entry_report["stats"] = stats
        return stats

    @property
    def source_state_rows(self) -> dict[str, dict[str, Any]]:
        return (
            self.run_deps.source_state_rows
            if isinstance(self.run_deps.source_state_rows, dict)
            else {}
        )

    def emit_heartbeat(self) -> None:
        callback = self.run_deps.heartbeat_callback
        if callback is not None:
            callback()

    def emit_source_progress(
        self,
        *,
        phase_key: str,
        phase_label: str,
        counts: dict[str, Any] | None = None,
        target_label: str = "",
        target_url: str = "",
        wait_reason: str = "",
        event_level: str = "muted",
        message: str = "",
    ) -> None:
        if self.run_deps.progress_callback is None:
            return
        payload_counts = {
            "listingPages": len(self.pages),
            "listingPagesVisited": max(0, int(self.progress_state["listingPagesVisited"])),
            "candidateLinksFound": int(self.stats.get("candidate_links_found") or 0),
            "detailPagesVisited": int(self.stats.get("detail_pages_visited") or 0),
            "jobsEmitted": self.current_source_kept_count(),
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
                "waitReason": str(wait_reason or "").strip(),
                "message": str(message or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        if signature == self.progress_state["lastProgressSignature"]:
            return
        self.progress_state["lastProgressSignature"] = signature
        self.run_deps.progress_callback(
            phase_key=phase_key,
            phase_label=phase_label,
            counts=payload_counts,
            target_label=target_label,
            target_url=target_url,
            wait_reason=wait_reason,
            event_level=event_level,
            message=message,
        )

    def sync_source_deadline(self, source_budget_s: int) -> float:
        self.source_deadline = min(
            float(self.source_deadline),
            build_static_source_deadline(
                source_started=self.source_started,
                source_budget_s=source_budget_s,
            ),
        )
        return float(self.source_deadline)

    def remaining_budget_s(self) -> float:
        return remaining_static_source_budget_s(deadline_monotonic=float(self.source_deadline))

    def current_source_kept_count(self) -> int:
        return max(0, len(self.jobs) - self.kept_before)

    def current_domain_gate_wait_stats(self) -> tuple[int, int]:
        reader = getattr(self.run_deps.fetch_text, "_baluffo_gate_wait_stats", None)
        if callable(reader):
            payload = reader(self.source_name)
            if isinstance(payload, dict):
                return (
                    int(payload.get("domainGateWaitMs") or 0),
                    int(payload.get("domainGateWaitCount") or 0),
                )
        return (
            int(self.stats.get("domain_gate_wait_ms") or 0),
            int(self.stats.get("domain_gate_wait_count") or 0),
        )

    def record_static_fetch_failure(self, *, target_url: str, exc: Exception | str) -> None:
        msg = str(exc)
        classification, browser_recommended = classify_static_fetch_exception(
            exc,
            anti_bot_retry=bool(self.source.get("antiBotBrowserRetry")),
            target_url=target_url,
        )
        if classification in {"anti_bot_or_challenge", "blocked_or_challenge", "timeout"}:
            self.entry_report["status"] = "error"
            self.entry_report["classification"] = classification
            self.entry_report["browserFallbackRecommended"] = browser_recommended
            self.entry_report["error"] = msg
            self.warnings.append(f"static:{self.source_name}:{target_url}: {msg}")
            return
        self.errors.append(f"static:{self.source_name}:{target_url}: {exc}")

    def stop_for_budget_exhaustion(self, *, target_url: str, source_budget_s: int) -> None:
        self.entry_report["classification"] = "timeout"
        self.entry_report["browserFallbackRecommended"] = True
        self.entry_report["error"] = f"time budget exceeded ({source_budget_s}s)"
        if self.current_source_kept_count() <= 0:
            self.entry_report["status"] = "error"
        self.warnings.append(f"static:{self.source_name}:{target_url}: time_budget_exceeded")


def build_static_source_context(
    *,
    run_deps: StaticRunDeps,
    runtime_config: StaticSourceRuntimeConfig,
    html_fetcher: StaticHtmlFetcher,
    source: dict[str, Any],
    selected_source_count: int,
    jobs: list[RawJob],
    warnings: list[str],
    errors: list[str],
    details: list[dict[str, Any]],
) -> StaticSourceContext:
    source_name = clean_text(source.get("name")) or "static_source"
    company = clean_text(source.get("company")) or source_name
    pages = _as_pages(source.get("pages"))
    entry_report = build_static_entry_report(
        source=source,
        source_name=source_name,
        pages=pages,
        company=company,
    )
    entry_report["browserEscalationEnabled"] = bool(run_deps.try_playwright)
    cache_decision = get_incremental_cache_decision(
        source_name,
        run_deps.source_state_rows or {},
        adapter="static",
        force_refresh_all=run_deps.force_refresh_all,
    )
    entry_report["cacheDecision"] = clean_text(cache_decision.get("cacheDecision")) or "run_now"
    entry_report["cacheDecisionReason"] = (
        clean_text(cache_decision.get("cacheDecisionReason")) or "run_now"
    )
    state_entry = _as_dict((run_deps.source_state_rows or {}).get(source_name))
    return StaticSourceContext(
        run_deps=run_deps,
        runtime_config=runtime_config,
        html_fetcher=html_fetcher,
        source=source,
        source_name=source_name,
        company=company,
        pages=pages,
        entry_report=entry_report,
        state_entry=state_entry,
        selected_source_count=max(0, int(selected_source_count)),
        jobs=jobs,
        warnings=warnings,
        errors=errors,
        details=details,
    )
