#!/usr/bin/env python3
"""Run an isolated two-pass fetch incremental benchmark under _out/."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

if TYPE_CHECKING:
    from src.jobs.interfaces import SourceLoader
else:
    SourceLoader = Any

DEFAULT_BENCHMARK_SOURCES = [
    "greenhouse_boards",
    "lever_sources",
    "ashby_sources",
    "teamtailor_sources",
    "smartrecruiters_sources",
    "workable_sources",
    "recruitee_sources",
    "pinpoint_sources",
    "breezy_sources",
    "jazzhr_sources",
    "personio_sources",
    "static_source::static:name:little chicken",
]
STATIC_DETAIL_TARGET_MARKER = "__static_detail_targets__"
STATIC_DETAIL_TARGET_SOURCE_NAMES = (
    "PlayStation (Sheet)",
    "Electronic Arts (Manual Website)",
    "Warner Bros. Games (Sheet)",
)
STATIC_OUTLIER_TARGET_MARKER = "__static_outlier_targets__"
STATIC_OUTLIER_TARGET_SOURCE_NAMES = (
    "Maliyo Games (Sheet)",
    "Million Victories (GameDevMap)",
    "Atari (GameDevMap)",
    "Netflix Games Studios (Sheet)",
    "Super Lucky Casino (GameDevMap)",
    "Koei Tecmo Vietnam (GameDevMap)",
    "Lightbulb Crew (GameDevMap)",
    "Atvis (GameDevMap)",
)
FETCH_BENCHMARK_GROUPS = {
    "smoke": ["greenhouse_boards", "lever_sources"],
    "provider-api": [
        "greenhouse_boards",
        "lever_sources",
        "ashby_sources",
        "smartrecruiters_sources",
        "teamtailor_sources",
    ],
    "static-detail": [
        STATIC_DETAIL_TARGET_MARKER,
    ],
    "static-outliers": [
        STATIC_OUTLIER_TARGET_MARKER,
    ],
    "mixed": [
        "greenhouse_boards",
        "lever_sources",
        "ashby_sources",
        "__static_first_2__",
    ],
}


def _ensure_repo_on_path() -> Path:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run an isolated two-pass fetch incremental benchmark."
    )
    parser.add_argument(
        "--output-dir",
        default="_out/perf-sanity-fetch-incremental",
        help="Isolated output dir for benchmark artifacts.",
    )
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--backoff", type=float, default=1.2)
    parser.add_argument("--max-workers", type=int, default=12)
    parser.add_argument("--max-per-domain", type=int, default=3)
    parser.add_argument("--adapter-http-concurrency", type=int, default=48)
    parser.add_argument("--static-detail-concurrency", type=int, default=10)
    parser.add_argument("--group", choices=sorted(FETCH_BENCHMARK_GROUPS), default="")
    parser.add_argument("--sources", nargs="*", default=None)
    parser.add_argument(
        "--keep-existing-output",
        action="store_true",
        help="Reuse the output dir instead of removing it before pass one.",
    )
    return parser.parse_args(argv)


def _as_list(value: object) -> list[Any]:
    return value if isinstance(value, list) else []


def _timing_summary(report: dict[str, object]) -> dict[str, Any]:
    runtime = dict(report.get("runtime") or {})
    return dict(runtime.get("timingSummary") or {})


def _runtime_duration_ms(report: dict[str, object]) -> int:
    runtime = dict(report.get("runtime") or {})
    timing_summary = _timing_summary(report)
    return int(
        runtime.get("totalDurationMs")
        or timing_summary.get("totalDurationMs")
        or runtime.get("wallClockDurationMs")
        or timing_summary.get("wallClockDurationMs")
        or 0
    )


def _stage_durations_ms(*reports: dict[str, object]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for report in reports:
        stage_totals = dict(_timing_summary(report).get("stageTotalsMs") or {})
        for stage, value in stage_totals.items():
            try:
                duration = int(float(value))
            except (TypeError, ValueError):
                continue
            if duration > 0:
                totals[str(stage)] = totals.get(str(stage), 0) + duration
    return totals


def _network_wait_counters(*reports: dict[str, object]) -> dict[str, Any]:
    counters = {
        "cacheSkippedCount": 0,
        "revalidatedCount": 0,
        "notModifiedCount": 0,
        "boardRefreshedCount": 0,
        "boardSkippedCount": 0,
        "failedSources": 0,
        "timeoutOrErrorCount": 0,
    }
    adapter_durations: dict[str, int] = {}
    for report in reports:
        summary = dict(report.get("summary") or {})
        for key in ("cacheSkippedCount", "revalidatedCount", "notModifiedCount", "failedSources"):
            counters[key] += int(summary.get(key) or 0)
        for row in _as_list(_timing_summary(report).get("adapterTimings")):
            if not isinstance(row, dict):
                continue
            adapter = str(row.get("adapter") or "").strip()
            if not adapter:
                continue
            adapter_durations[adapter] = adapter_durations.get(adapter, 0) + int(
                row.get("durationMs") or 0
            )
            counters["timeoutOrErrorCount"] += int(row.get("errorCount") or 0)
        for row in _as_list(report.get("sources")):
            if not isinstance(row, dict):
                continue
            board_counts = dict(row.get("boardCacheDecisionCounts") or {})
            counters["boardRefreshedCount"] += int(board_counts.get("run_now") or 0)
            counters["boardSkippedCount"] += int(board_counts.get("skip_fresh") or 0)
    return {**counters, "adapterDurationsMs": adapter_durations}


def _slowest_sources(report: dict[str, object], *, limit: int = 5) -> list[dict[str, object]]:
    runtime = dict(report.get("runtime") or {})
    rows: list[dict[str, object]] = []
    for row in _as_list(runtime.get("slowestSources")):
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "name": str(row.get("name") or ""),
                "adapter": str(row.get("adapter") or ""),
                "durationMs": int(row.get("durationMs") or 0),
                "keptCount": int(row.get("keptCount") or 0),
                "detailPagesVisited": int(row.get("detailPagesVisited") or 0),
                "detailYieldPct": int(row.get("detailYieldPct") or 0),
            }
        )
    rows.sort(key=lambda row: int(row.get("durationMs") or 0), reverse=True)
    return rows[: max(0, int(limit))]


def _slowest_provider_boards(
    report: dict[str, object], *, limit: int = 10
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for source in _as_list(report.get("sources")):
        if not isinstance(source, dict):
            continue
        source_name = str(source.get("name") or "")
        adapter = str(source.get("adapter") or "")
        for detail in _as_list(source.get("details")):
            if not isinstance(detail, dict):
                continue
            try:
                duration_ms = int(detail.get("durationMs") or 0)
            except (TypeError, ValueError):
                duration_ms = 0
            if duration_ms <= 0:
                continue
            rows.append(
                {
                    "source": source_name,
                    "adapter": str(detail.get("adapter") or adapter),
                    "name": str(detail.get("name") or ""),
                    "studio": str(detail.get("studio") or ""),
                    "slug": str(detail.get("slug") or ""),
                    "status": str(detail.get("status") or ""),
                    "cacheDecision": str(detail.get("cacheDecision") or ""),
                    "durationMs": duration_ms,
                    "fetchMs": int(detail.get("fetchMs") or 0),
                    "parseMs": int(detail.get("parseMs") or 0),
                    "keptCount": int(detail.get("keptCount") or 0),
                    "providerUrl": str(detail.get("providerUrl") or ""),
                    "error": str(detail.get("error") or ""),
                }
            )
    rows.sort(key=lambda row: int(row.get("durationMs") or 0), reverse=True)
    return rows[: max(0, int(limit))]


def source_names_for_args(args: argparse.Namespace) -> list[str]:
    group = str(args.group or "").strip()
    if group:
        return list(FETCH_BENCHMARK_GROUPS[group])
    return [str(name) for name in (args.sources or DEFAULT_BENCHMARK_SOURCES)]


def _select_loaders(source_names: list[str]) -> tuple[list[tuple[str, SourceLoader]], list[str]]:
    from src.jobs import adapters as adapters_pkg
    from src.jobs.adapters import static as static_adapter
    from src.jobs.registry import registry_entries
    from src.jobs.text_utils import clean_text

    available: dict[str, SourceLoader] = {
        name: loader for name, loader in adapters_pkg.default_source_loaders(social_enabled=False)
    }
    for static_name, static_loader in static_adapter.build_static_source_loaders():
        available.setdefault(static_name, static_loader)
    for extracted_name, extracted_loader in adapters_pkg.EXTRACTED_ADAPTERS.items():
        available.setdefault(extracted_name, cast(SourceLoader, extracted_loader))
    expanded_source_names: list[str] = []
    static_names = [name for name in available if str(name).startswith("static_source::")]
    for name in source_names:
        if name in {STATIC_DETAIL_TARGET_MARKER, STATIC_OUTLIER_TARGET_MARKER}:
            expected_names = (
                STATIC_DETAIL_TARGET_SOURCE_NAMES
                if name == STATIC_DETAIL_TARGET_MARKER
                else STATIC_OUTLIER_TARGET_SOURCE_NAMES
            )
            target_names: list[str] = []
            seen_target_names: set[str] = set()
            for row in registry_entries("static"):
                source_name = clean_text(row.get("name"))
                if source_name not in expected_names:
                    continue
                if source_name in seen_target_names:
                    continue
                seen_target_names.add(source_name)
                target_names.append(static_adapter.static_source_name_for_registry_row(row))
            expanded_source_names.extend(target_names)
            continue
        if str(name).startswith("__static_first_") and str(name).endswith("__"):
            raw_limit = str(name).removeprefix("__static_first_").removesuffix("__")
            try:
                limit = max(0, int(raw_limit))
            except ValueError:
                limit = 0
            expanded_source_names.extend(static_names[:limit])
            continue
        expanded_source_names.append(name)
    selected: list[tuple[str, SourceLoader]] = []
    missing: list[str] = []
    for name in expanded_source_names:
        normalized_name = clean_text(name)
        loader: SourceLoader | None = available.get(name)
        if loader is None and normalized_name:
            loader = next(
                (
                    candidate_loader
                    for candidate_name, candidate_loader in available.items()
                    if clean_text(candidate_name) == normalized_name
                ),
                None,
            )
        if loader is None:
            missing.append(name)
            continue
        selected.append((normalized_name or name, loader))
    return selected, missing


def _normalized_host(value: object) -> str:
    host = urlparse(str(value or "")).hostname or ""
    return host.lower().removeprefix("www.")


def _registry_page_signal_for_row(source_name: str, row: dict[str, object]) -> dict[str, object]:
    pages = [str(page) for page in _as_list(row.get("pages")) if str(page or "").strip()]
    listing_url = str(
        row.get("listing_url") or row.get("careersUrl") or (pages[0] if pages else "")
    )
    listing_host = _normalized_host(listing_url)
    off_listing_pages: list[str] = []
    off_listing_hosts: list[str] = []
    seen_hosts: set[str] = set()
    for page in pages:
        host = _normalized_host(page)
        if not host or host == listing_host:
            continue
        off_listing_pages.append(page)
        if host not in seen_hosts:
            seen_hosts.add(host)
            off_listing_hosts.append(host)
    return {
        "name": source_name,
        "listingHost": listing_host,
        "pageCount": len(pages),
        "offListingHostPageCount": len(off_listing_pages),
        "offListingHosts": off_listing_hosts,
        "offListingHostPages": off_listing_pages[:5],
    }


def _registry_page_signals(source_names: list[str]) -> dict[str, dict[str, object]]:
    from src.jobs.adapters import static as static_adapter
    from src.jobs.registry import registry_entries

    source_set = set(source_names)
    signals: dict[str, dict[str, object]] = {}
    for row in registry_entries("static"):
        if not isinstance(row, dict):
            continue
        source_name = static_adapter.static_source_name_for_registry_row(row)
        if source_name not in source_set:
            continue
        signal = _registry_page_signal_for_row(source_name, row)
        if int(signal.get("offListingHostPageCount") or 0) > 0:
            signals[source_name] = signal
    return signals


def _registry_scope_summary(
    registry_page_signals: dict[str, dict[str, object]],
) -> dict[str, object]:
    rows = list(registry_page_signals.values())
    rows.sort(
        key=lambda row: (
            int(row.get("offListingHostPageCount") or 0),
            int(row.get("pageCount") or 0),
        ),
        reverse=True,
    )
    return {
        "sourceCount": len(rows),
        "offListingHostPageCount": sum(
            int(row.get("offListingHostPageCount") or 0) for row in rows
        ),
        "sources": rows,
    }


def _family_summary(report: dict[str, object], source_names: list[str]) -> dict[str, object]:
    rows = [
        row
        for row in _as_list(report.get("sources"))
        if isinstance(row, dict) and str(row.get("name") or "") in source_names
    ]
    family: dict[str, object] = {}
    for row in rows:
        name = str(row.get("name") or "")
        family[name] = {
            "status": row.get("status"),
            "durationMs": int(row.get("durationMs") or 0),
            "keptCount": int(row.get("keptCount") or 0),
            "boardCount": int(row.get("boardCount") or 0)
            if row.get("boardCount") is not None
            else None,
            "boardCacheDecisionCounts": dict(row.get("boardCacheDecisionCounts") or {}),
            "boardSkippedCount": int(row.get("boardSkippedCount") or 0)
            if row.get("boardSkippedCount") is not None
            else None,
            "boardRevalidatedCount": int(row.get("boardRevalidatedCount") or 0)
            if row.get("boardRevalidatedCount") is not None
            else None,
            "boardNotModifiedCount": int(row.get("boardNotModifiedCount") or 0)
            if row.get("boardNotModifiedCount") is not None
            else None,
            "boardRefreshedCount": int(row.get("boardRefreshedCount") or 0)
            if row.get("boardRefreshedCount") is not None
            else None,
            "error": str(row.get("error") or ""),
            "failureBucket": str(row.get("failureBucket") or ""),
            "zeroKeptClassification": str(row.get("zeroKeptClassification") or ""),
            "stats": dict(row.get("stats") or {}),
            "loss": dict(row.get("loss") or {}),
        }
    return family


def _source_policy_signals(
    report: dict[str, object],
    source_names: list[str],
    *,
    limit: int = 10,
) -> list[dict[str, object]]:
    source_set = set(source_names)
    rows: list[dict[str, object]] = []
    for row in _as_list(report.get("sources")):
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "")
        if name not in source_set:
            continue
        loss = dict(row.get("loss") or {})
        raw_fetched = int(loss.get("rawFetched") or row.get("fetchedCount") or 0)
        dedup_merged = int(loss.get("dedupMerged") or 0)
        final_output = int(loss.get("finalOutput") or row.get("keptCount") or 0)
        merge_ratio = float(dedup_merged) / float(raw_fetched) if raw_fetched > 0 else 0.0
        failure_bucket = str(row.get("failureBucket") or "")
        zero_kept = str(row.get("zeroKeptClassification") or "")
        error = str(row.get("error") or "")
        flags: list[str] = []
        if failure_bucket:
            flags.append(f"failure:{failure_bucket}")
        if zero_kept:
            flags.append(f"zero_kept:{zero_kept}")
        if raw_fetched >= 10 and merge_ratio >= 0.5:
            flags.append("high_merge_ratio")
        if "time budget exceeded" in error or "time_budget_exceeded" in error:
            flags.append("time_budget")
        if "Network error" in error or "Server disconnected" in error:
            flags.append("network_wait")
        if not flags:
            continue
        rows.append(
            {
                "name": name,
                "adapter": str(row.get("adapter") or ""),
                "durationMs": int(row.get("durationMs") or 0),
                "keptCount": int(row.get("keptCount") or 0),
                "rawFetched": raw_fetched,
                "dedupMerged": dedup_merged,
                "finalOutput": final_output,
                "mergeRatioPct": int(round(merge_ratio * 100)),
                "failureBucket": failure_bucket,
                "zeroKeptClassification": zero_kept,
                "flags": flags,
            }
        )
    rows.sort(
        key=lambda item: (
            int("failure:site_changed" in item.get("flags", [])),
            int("high_merge_ratio" in item.get("flags", [])),
            int(item.get("durationMs") or 0),
        ),
        reverse=True,
    )
    return rows[: max(0, int(limit))]


def _next_optimization_targets(
    source_policy_signals: list[dict[str, object]],
    *,
    registry_page_signals: dict[str, dict[str, object]] | None = None,
    limit: int = 5,
) -> list[dict[str, object]]:
    registry_page_signals = registry_page_signals or {}
    targets: list[dict[str, object]] = []
    for signal in source_policy_signals:
        name = str(signal.get("name") or "")
        flags = [str(flag) for flag in _as_list(signal.get("flags"))]
        if not flags:
            continue
        action = "timeout_or_network_budget"
        priority = 30
        reasons: list[str] = []
        if "failure:site_changed" in flags:
            action = "source_policy_review"
            priority = 100
            reasons.append("site_changed")
        if "failure:needs_review" in flags or "zero_kept:needs_review" in flags:
            action = "source_policy_review"
            priority = max(priority, 90)
            reasons.append("needs_review")
        if "high_merge_ratio" in flags:
            if action != "source_policy_review":
                action = "source_scope_review"
                priority = max(priority, 70)
            reasons.append("high_merge_ratio")
        if "time_budget" in flags:
            reasons.append("time_budget")
        if "network_wait" in flags:
            reasons.append("network_wait")
        if not reasons:
            reasons = flags
        kept_count = int(signal.get("keptCount") or 0)
        registry_page_evidence = registry_page_signals.get(name, {})
        has_registry_scope_evidence = (
            int(registry_page_evidence.get("offListingHostPageCount") or 0) > 0
        )
        if action == "timeout_or_network_budget" and has_registry_scope_evidence:
            action = "source_scope_and_timeout_review"
            priority = max(priority, 65)
            reasons.append("cross_host_registry_pages")
        output_contract_risk = (
            action
            in {
                "source_policy_review",
                "source_scope_review",
                "source_scope_and_timeout_review",
            }
            and kept_count > 0
        )
        targets.append(
            {
                "name": name,
                "action": action,
                "priority": priority,
                "durationMs": int(signal.get("durationMs") or 0),
                "keptCount": kept_count,
                "outputContractRisk": output_contract_risk,
                "requiresExplicitDecision": output_contract_risk,
                "registryPageEvidence": registry_page_evidence,
                "reasons": reasons,
            }
        )
    targets.sort(
        key=lambda item: (
            int(item.get("priority") or 0),
            int(item.get("durationMs") or 0),
        ),
        reverse=True,
    )
    return targets[: max(0, int(limit))]


def _error_samples(value: object, *, limit: int = 3) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    samples = [part.strip() for part in raw.split(";") if part.strip()]
    return samples[: max(0, int(limit))]


def _urls_in_text(value: object) -> list[str]:
    urls: list[str] = []
    for match in re.finditer(r"https?://[^\s;,\]\[(){}<>\"']+", str(value or "")):
        cleaned = match.group(0).rstrip(":.")
        if cleaned and cleaned not in urls:
            urls.append(cleaned)
    return urls


def _timeout_url_role(source_name: str, url: str) -> str:
    normalized_source = source_name.lower().rstrip("/")
    normalized_url = url.lower().rstrip("/")
    if normalized_url and normalized_url in normalized_source:
        return "listing"
    return "detail_or_registry_page"


def _timeout_url_bucket(urls: list[str]) -> dict[str, object]:
    return {
        "timeoutUrlCount": len(urls),
        "timeoutUrls": urls[:5],
        "firstTimeoutUrl": urls[0] if urls else "",
        "lastTimeoutUrl": urls[-1] if urls else "",
    }


def _timeout_diagnostics(
    source_name: str,
    error: object,
    detail_timing: dict[str, object],
) -> dict[str, object]:
    parts = [part.strip() for part in str(error or "").split(";") if part.strip()]
    timeout_parts = [
        part
        for part in parts
        if "time budget exceeded" in part.lower() or "time_budget_exceeded" in part.lower()
    ]
    network_parts = [
        part
        for part in parts
        if "network error" in part.lower() or "server disconnected" in part.lower()
    ]
    timeout_urls: list[str] = []
    for part in timeout_parts:
        for url in _urls_in_text(part):
            if url not in timeout_urls:
                timeout_urls.append(url)
    role_counts: dict[str, int] = {}
    listing_timeout_urls: list[str] = []
    detail_timeout_urls: list[str] = []
    for url in timeout_urls:
        role = _timeout_url_role(source_name, url)
        role_counts[role] = role_counts.get(role, 0) + 1
        if role == "listing":
            listing_timeout_urls.append(url)
        else:
            detail_timeout_urls.append(url)
    return {
        "timeoutErrorCount": len(timeout_parts),
        "networkErrorCount": len(network_parts),
        "timeoutUrlCount": len(timeout_urls),
        "timeoutUrls": timeout_urls[:5],
        "firstTimeoutUrl": timeout_urls[0] if timeout_urls else "",
        "lastTimeoutUrl": timeout_urls[-1] if timeout_urls else "",
        "timeoutUrlRoleCounts": role_counts,
        "listingTimeouts": _timeout_url_bucket(listing_timeout_urls),
        "detailTimeouts": _timeout_url_bucket(detail_timeout_urls),
        "detailPagesVisited": int(detail_timing.get("detailPagesVisited") or 0),
        "detailYieldPct": int(detail_timing.get("detailYieldPct") or 0),
    }


def _kept_output_host_breakdown(report: dict[str, object], source_name: str) -> dict[str, object]:
    host_counts: dict[str, int] = {}
    for row in _as_list(report.get("jobs")):
        if not isinstance(row, dict):
            continue
        if str(row.get("source") or "") != source_name:
            bundle_matches = False
            for bundle_row in _as_list(row.get("sourceBundle")):
                if (
                    isinstance(bundle_row, dict)
                    and str(bundle_row.get("source") or "") == source_name
                ):
                    bundle_matches = True
                    break
            if not bundle_matches:
                continue
        host = _normalized_host(row.get("jobLink"))
        if not host:
            host = "unknown"
        host_counts[host] = host_counts.get(host, 0) + 1
    rows = [
        {"host": host, "keptCount": count}
        for host, count in sorted(host_counts.items(), key=lambda item: item[1], reverse=True)
    ]
    return {
        "totalKeptCount": sum(host_counts.values()),
        "hostCount": len(host_counts),
        "hosts": rows,
    }


def _load_output_jobs(output_dir: Path) -> list[dict[str, object]]:
    from src.shared.json_io import read_json

    payload = read_json(output_dir / "jobs-unified.json", [])
    if isinstance(payload, dict):
        payload = payload.get("jobs")
    return [row for row in _as_list(payload) if isinstance(row, dict)]


def _source_policy_decision_evidence(
    target: dict[str, object],
    report: dict[str, object],
) -> dict[str, object]:
    name = str(target.get("name") or "")
    registry_page_evidence = dict(target.get("registryPageEvidence") or {})
    listing_host = str(registry_page_evidence.get("listingHost") or "")
    off_listing_hosts = [
        str(host) for host in _as_list(registry_page_evidence.get("offListingHosts")) if str(host)
    ]
    kept_output_host_breakdown = _kept_output_host_breakdown(report, name)
    kept_hosts = [
        str(row.get("host") or "")
        for row in _as_list(kept_output_host_breakdown.get("hosts"))
        if isinstance(row, dict)
    ]
    cross_host_kept_hosts = [
        host for host in kept_hosts if host and host != listing_host and host != "unknown"
    ]
    suggested_decision = ""
    if bool(target.get("requiresExplicitDecision")) and off_listing_hosts:
        suggested_decision = "keep_parent_scope" if not cross_host_kept_hosts else "split_source"
    elif bool(target.get("requiresExplicitDecision")):
        suggested_decision = "hide_source_after_review"
    return {
        "policyDecisionNeeded": bool(target.get("requiresExplicitDecision")),
        "sourceScopeIdentity": {
            "listingHost": listing_host,
            "offListingHosts": off_listing_hosts,
        },
        "keptOutputHostBreakdown": kept_output_host_breakdown,
        "suggestedDecision": suggested_decision,
    }


def _source_detail_timing_signals(report: dict[str, object]) -> dict[str, dict[str, object]]:
    runtime = dict(report.get("runtime") or {})
    signals: dict[str, dict[str, object]] = {}
    for row in _as_list(runtime.get("slowestSources")):
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "")
        if not name:
            continue
        signals[name] = {
            "detailPagesVisited": int(row.get("detailPagesVisited") or 0),
            "detailYieldPct": int(row.get("detailYieldPct") or 0),
        }
    return signals


def _decision_type_for_target(target: dict[str, object]) -> str:
    action = str(target.get("action") or "")
    kept_count = int(target.get("keptCount") or 0)
    if action == "source_policy_review" and kept_count <= 0:
        return "follow_up_review"
    if action == "source_policy_review":
        return "explicit_source_policy"
    if action in {"source_scope_review", "source_scope_and_timeout_review"}:
        return "explicit_source_scope"
    return "timeout_diagnostics"


def _next_decision_for_type(decision_type: str) -> str:
    if decision_type == "slow_productive_static":
        return "Treat as slow but productive; review diagnostics before any budget change."
    if decision_type == "explicit_source_policy":
        return "Decide whether this kept-output source should keep its current policy scope before any behavior change."
    if decision_type == "explicit_source_scope":
        return "Decide whether cross-host registry pages belong in this source before any timeout or scope change."
    if decision_type == "follow_up_review":
        return "Review zero-kept needs-review evidence after kept-output source decisions are resolved."
    return "Inspect timeout and network evidence before changing budgets or retry behavior."


def _is_slow_productive_static(
    target: dict[str, object],
    signal: dict[str, object],
    registry_page_evidence: dict[str, object],
    detail_timing: dict[str, object],
) -> bool:
    return (
        str(target.get("action") or "") == "timeout_or_network_budget"
        and int(target.get("keptCount") or 0) > 0
        and not bool(target.get("requiresExplicitDecision"))
        and int(registry_page_evidence.get("offListingHostPageCount") or 0) == 0
        and int(detail_timing.get("detailYieldPct") or 0) >= 80
        and "time_budget" in [str(flag) for flag in _as_list(signal.get("flags"))]
    )


def _source_decision_matrix(
    targets: list[dict[str, object]],
    source_policy_signals: list[dict[str, object]],
    report: dict[str, object],
) -> list[dict[str, object]]:
    signal_by_name = {str(row.get("name") or ""): row for row in source_policy_signals}
    family_summary = _family_summary(report, [str(row.get("name") or "") for row in targets])
    detail_signals = _source_detail_timing_signals(report)
    rows: list[dict[str, object]] = []
    for target in targets:
        name = str(target.get("name") or "")
        if not name:
            continue
        signal = signal_by_name.get(name, {})
        family = dict(family_summary.get(name) or {})
        registry_page_evidence = dict(target.get("registryPageEvidence") or {})
        decision_type = _decision_type_for_target(target)
        detail_timing = detail_signals.get(name, {})
        if _is_slow_productive_static(target, signal, registry_page_evidence, detail_timing):
            decision_type = "slow_productive_static"
        rows.append(
            {
                "name": name,
                "action": str(target.get("action") or ""),
                "priority": int(target.get("priority") or 0),
                "keptCount": int(target.get("keptCount") or 0),
                "durationMs": int(target.get("durationMs") or 0),
                "decisionType": decision_type,
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": bool(target.get("requiresExplicitDecision")),
                "evidence": {
                    "flags": [str(flag) for flag in _as_list(signal.get("flags"))],
                    "reasons": [str(reason) for reason in _as_list(target.get("reasons"))],
                    "mergeRatioPct": int(signal.get("mergeRatioPct") or 0),
                    "failureBucket": str(signal.get("failureBucket") or ""),
                    "zeroKeptClassification": str(signal.get("zeroKeptClassification") or ""),
                    "registryPageEvidence": registry_page_evidence,
                    "detailTiming": detail_timing,
                    "timeoutDiagnostics": _timeout_diagnostics(
                        name,
                        family.get("error"),
                        detail_timing,
                    ),
                    "sourcePolicyDecision": _source_policy_decision_evidence(target, report),
                    "errorSamples": _error_samples(family.get("error")),
                },
                "nextDecision": _next_decision_for_type(decision_type),
            }
        )
    return rows


def _format_markdown_list(values: object) -> str:
    items = [str(value).strip() for value in _as_list(values) if str(value).strip()]
    return ", ".join(items) if items else "-"


def _render_source_decision_matrix_markdown(rows: list[dict[str, object]]) -> str:
    lines = [
        "# Source Decision Matrix",
        "",
        "Generated from `sourceDecisionMatrix` in `benchmark-summary.json`. This report is diagnostics-only and does not authorize source, timeout, registry, or output behavior changes.",
        "",
    ]
    if not rows:
        lines.extend(["No source decision rows were generated.", ""])
        return "\n".join(lines)
    for row in rows:
        evidence = dict(row.get("evidence") or {})
        registry_evidence = dict(evidence.get("registryPageEvidence") or {})
        detail_timing = dict(evidence.get("detailTiming") or {})
        timeout_diagnostics = dict(evidence.get("timeoutDiagnostics") or {})
        timeout_role_counts = dict(timeout_diagnostics.get("timeoutUrlRoleCounts") or {})
        source_policy_decision = dict(evidence.get("sourcePolicyDecision") or {})
        host_breakdown = dict(source_policy_decision.get("keptOutputHostBreakdown") or {})
        kept_host_rows = [
            f"{row.get('host')}={row.get('keptCount')}"
            for row in _as_list(host_breakdown.get("hosts"))
            if isinstance(row, dict)
        ]
        lines.extend(
            [
                f"## {row.get('name') or '-'}",
                "",
                f"- Decision type: `{row.get('decisionType') or '-'}`",
                f"- Current action: `{row.get('action') or '-'}`",
                f"- Priority: `{int(row.get('priority') or 0)}`",
                f"- Kept count: `{int(row.get('keptCount') or 0)}`",
                f"- Duration: `{int(row.get('durationMs') or 0)}ms`",
                f"- Requires explicit decision: `{str(bool(row.get('requiresExplicitDecision'))).lower()}`",
                f"- Recommended first pass: `{row.get('recommendedFirstPass') or '-'}`",
                f"- Behavior change allowed: `{str(bool(row.get('behaviorChangeAllowed'))).lower()}`",
                f"- Flags: {_format_markdown_list(evidence.get('flags'))}",
                f"- Reasons: {_format_markdown_list(evidence.get('reasons'))}",
                f"- Failure bucket: `{evidence.get('failureBucket') or '-'}`",
                f"- Zero-kept classification: `{evidence.get('zeroKeptClassification') or '-'}`",
                f"- Merge ratio: `{int(evidence.get('mergeRatioPct') or 0)}%`",
                f"- Listing host: `{registry_evidence.get('listingHost') or '-'}`",
                f"- Off-listing hosts: {_format_markdown_list(registry_evidence.get('offListingHosts'))}",
                f"- Off-listing pages: {_format_markdown_list(registry_evidence.get('offListingHostPages'))}",
                f"- Detail timing: `pages={int(detail_timing.get('detailPagesVisited') or 0)}, yield={int(detail_timing.get('detailYieldPct') or 0)}%`",
                f"- Timeout diagnostics: `timeouts={int(timeout_diagnostics.get('timeoutErrorCount') or 0)}, network={int(timeout_diagnostics.get('networkErrorCount') or 0)}, timeoutUrls={int(timeout_diagnostics.get('timeoutUrlCount') or 0)}`",
                f"- Timeout URL roles: {_format_markdown_list([f'{key}={value}' for key, value in timeout_role_counts.items()])}",
                f"- Timeout URLs: {_format_markdown_list(timeout_diagnostics.get('timeoutUrls'))}",
                f"- Listing timeout URLs: {_format_markdown_list(dict(timeout_diagnostics.get('listingTimeouts') or {}).get('timeoutUrls'))}",
                f"- Detail timeout URLs: {_format_markdown_list(dict(timeout_diagnostics.get('detailTimeouts') or {}).get('timeoutUrls'))}",
                f"- Policy decision needed: `{str(bool(source_policy_decision.get('policyDecisionNeeded'))).lower()}`",
                f"- Suggested source-policy decision: `{source_policy_decision.get('suggestedDecision') or '-'}`",
                f"- Kept output hosts: {_format_markdown_list(kept_host_rows)}",
                f"- Error samples: {_format_markdown_list(evidence.get('errorSamples'))}",
                f"- Next decision: {row.get('nextDecision') or '-'}",
                "",
            ]
        )
    return "\n".join(lines)


def _render_source_decision_log_template_markdown(rows: list[dict[str, object]]) -> str:
    lines = [
        "# Source Decision Log Template",
        "",
        "Generated from `sourceDecisionMatrix` in `benchmark-summary.json`. This file is local review evidence only and does not authorize registry, timeout, suppression, source-output, or persisted-job behavior changes.",
        "",
    ]
    if not rows:
        lines.extend(["No source decision rows were generated.", ""])
        return "\n".join(lines)
    for row in rows:
        evidence = dict(row.get("evidence") or {})
        registry_evidence = dict(evidence.get("registryPageEvidence") or {})
        timeout_diagnostics = dict(evidence.get("timeoutDiagnostics") or {})
        source_policy_decision = dict(evidence.get("sourcePolicyDecision") or {})
        lines.extend(
            [
                f"## {row.get('name') or '-'}",
                "",
                f"- Decision type: `{row.get('decisionType') or '-'}`",
                f"- Requires explicit decision: `{str(bool(row.get('requiresExplicitDecision'))).lower()}`",
                f"- Current recommendation: `{row.get('recommendedFirstPass') or '-'}`",
                f"- Behavior change allowed: `{str(bool(row.get('behaviorChangeAllowed'))).lower()}`",
                f"- Current action: `{row.get('action') or '-'}`",
                f"- Kept count: `{int(row.get('keptCount') or 0)}`",
                f"- Duration: `{int(row.get('durationMs') or 0)}ms`",
                f"- Evidence flags: {_format_markdown_list(evidence.get('flags'))}",
                f"- Evidence reasons: {_format_markdown_list(evidence.get('reasons'))}",
                f"- Cross-host evidence: {_format_markdown_list(registry_evidence.get('offListingHosts'))}",
                f"- Timeout diagnostics: `timeouts={int(timeout_diagnostics.get('timeoutErrorCount') or 0)}, network={int(timeout_diagnostics.get('networkErrorCount') or 0)}, timeoutUrls={int(timeout_diagnostics.get('timeoutUrlCount') or 0)}`",
                f"- First timeout URL: `{timeout_diagnostics.get('firstTimeoutUrl') or '-'}`",
                f"- Last timeout URL: `{timeout_diagnostics.get('lastTimeoutUrl') or '-'}`",
                f"- Listing timeout URLs: {_format_markdown_list(dict(timeout_diagnostics.get('listingTimeouts') or {}).get('timeoutUrls'))}",
                f"- Detail timeout URLs: {_format_markdown_list(dict(timeout_diagnostics.get('detailTimeouts') or {}).get('timeoutUrls'))}",
                f"- Policy decision needed: `{str(bool(source_policy_decision.get('policyDecisionNeeded'))).lower()}`",
                f"- Suggested source-policy decision: `{source_policy_decision.get('suggestedDecision') or '-'}`",
                f"- Failure bucket: `{evidence.get('failureBucket') or '-'}`",
                f"- Zero-kept classification: `{evidence.get('zeroKeptClassification') or '-'}`",
                f"- Error samples: {_format_markdown_list(evidence.get('errorSamples'))}",
                "",
                "Decision: preserve / investigate / change_later",
                "Chosen action:",
                "Reason:",
                "Risk accepted: yes/no",
                "Follow-up owner/date:",
                "",
            ]
        )
    return "\n".join(lines)


def _source_decision_trend(
    current_rows: list[dict[str, object]],
    previous_payload: dict[str, object] | None,
) -> dict[str, object]:
    previous_rows = (
        _as_list(previous_payload.get("sourceDecisionMatrix"))
        if isinstance(previous_payload, dict)
        else []
    )
    previous_by_name = {
        str(row.get("name") or ""): row for row in previous_rows if isinstance(row, dict)
    }
    rows: list[dict[str, object]] = []
    stable_slow_productive = 0
    changed_decision_type = 0
    new_count = 0
    missing_previous_count = 0
    for row in current_rows:
        name = str(row.get("name") or "")
        if not name:
            continue
        previous = previous_by_name.get(name)
        current_decision_type = str(row.get("decisionType") or "")
        previous_decision_type = str(previous.get("decisionType") or "") if previous else ""
        decision_type_changed = bool(previous) and current_decision_type != previous_decision_type
        if (
            current_decision_type == "slow_productive_static"
            and previous_decision_type == "slow_productive_static"
        ):
            stable_slow_productive += 1
        if decision_type_changed:
            changed_decision_type += 1
        if previous is None:
            new_count += 1
        rows.append(
            {
                "name": name,
                "decisionType": current_decision_type,
                "previousDecisionType": previous_decision_type,
                "decisionTypeChanged": decision_type_changed,
                "keptCount": int(row.get("keptCount") or 0),
                "previousKeptCount": int(previous.get("keptCount") or 0) if previous else 0,
                "durationMs": int(row.get("durationMs") or 0),
                "previousDurationMs": int(previous.get("durationMs") or 0) if previous else 0,
                "requiresExplicitDecision": bool(row.get("requiresExplicitDecision")),
                "previousRequiresExplicitDecision": bool(previous.get("requiresExplicitDecision"))
                if previous
                else False,
            }
        )
    current_names = {str(row.get("name") or "") for row in current_rows if isinstance(row, dict)}
    missing_previous_count = len(
        [name for name in previous_by_name if name and name not in current_names]
    )
    return {
        "status": "compared" if previous_by_name else "no_previous",
        "rowCount": len(rows),
        "newCount": new_count,
        "missingPreviousCount": missing_previous_count,
        "changedDecisionTypeCount": changed_decision_type,
        "stableSlowProductiveCount": stable_slow_productive,
        "rows": rows,
    }


def _render_source_decision_trend_markdown(trend: dict[str, object]) -> str:
    lines = [
        "# Source Decision Trend",
        "",
        "Generated from the current and previous `sourceDecisionMatrix` payloads when a previous benchmark summary exists. This report is diagnostics-only.",
        "",
        f"- Status: `{trend.get('status') or 'no_previous'}`",
        f"- Rows: `{int(trend.get('rowCount') or 0)}`",
        f"- New rows: `{int(trend.get('newCount') or 0)}`",
        f"- Missing previous rows: `{int(trend.get('missingPreviousCount') or 0)}`",
        f"- Changed decision types: `{int(trend.get('changedDecisionTypeCount') or 0)}`",
        f"- Stable slow productive rows: `{int(trend.get('stableSlowProductiveCount') or 0)}`",
        "",
    ]
    rows = [row for row in _as_list(trend.get("rows")) if isinstance(row, dict)]
    if not rows:
        lines.extend(["No source decision trend rows were generated.", ""])
        return "\n".join(lines)
    for row in rows:
        lines.extend(
            [
                f"## {row.get('name') or '-'}",
                "",
                f"- Decision type: `{row.get('decisionType') or '-'}`",
                f"- Previous decision type: `{row.get('previousDecisionType') or '-'}`",
                f"- Decision type changed: `{str(bool(row.get('decisionTypeChanged'))).lower()}`",
                f"- Kept count: `{int(row.get('keptCount') or 0)}` (previous `{int(row.get('previousKeptCount') or 0)}`)",
                f"- Duration: `{int(row.get('durationMs') or 0)}ms` (previous `{int(row.get('previousDurationMs') or 0)}ms`)",
                f"- Requires explicit decision: `{str(bool(row.get('requiresExplicitDecision'))).lower()}`",
                "",
            ]
        )
    return "\n".join(lines)


def _run_pass(
    output_dir: Path,
    selected_loaders: list[tuple[str, SourceLoader]],
    args: argparse.Namespace,
):
    from src.jobs.pipeline import run_pipeline

    return run_pipeline(
        output_dir=output_dir,
        source_loaders=selected_loaders,
        timeout_s=int(args.timeout),
        retries=int(args.retries),
        backoff_s=float(args.backoff),
        max_workers=int(args.max_workers),
        max_per_domain=int(args.max_per_domain),
        adapter_http_concurrency=int(args.adapter_http_concurrency),
        static_detail_concurrency=int(args.static_detail_concurrency),
        show_progress=False,
    )


def main(argv: list[str] | None = None) -> int:
    root = _ensure_repo_on_path()
    args = parse_args(argv)
    from src.baluffo_config import get_storage_defaults
    from src.shared.json_io import (
        copy_json_file_to_storage,
        existing_json_candidate,
        gzip_backed_json_storage_path,
    )

    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = (root / output_dir).resolve()
    previous_payload: dict[str, object] | None = None
    previous_summary_path = output_dir / "benchmark-summary.json"
    if previous_summary_path.exists():
        try:
            previous_payload = json.loads(previous_summary_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            previous_payload = None
    if output_dir.exists() and not bool(args.keep_existing_output):
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    live_data_dir = Path(get_storage_defaults()["data_dir"])
    for name in (
        "jobs-source-state.json",
        "source-registry-active.json",
        "source-registry-pending.json",
        "source-registry-rejected.json",
    ):
        source_path = existing_json_candidate(live_data_dir / name) or live_data_dir / name
        target_path = output_dir / name
        if source_path.exists() and not gzip_backed_json_storage_path(target_path).exists():
            copy_json_file_to_storage(source_path, target_path)

    selected_loaders, missing = _select_loaders(source_names_for_args(args))
    if missing:
        raise SystemExit(f"Unknown sources for benchmark: {', '.join(missing)}")
    os.environ["BALUFFO_DATA_DIR"] = str(output_dir)
    from src.storage_metrics import snapshot_storage_metrics

    first = _run_pass(output_dir, selected_loaders, args)
    second = _run_pass(output_dir, selected_loaders, args)
    first_duration_ms = _runtime_duration_ms(first)
    second_duration_ms = _runtime_duration_ms(second)
    selected_names = [name for name, _loader in selected_loaders]
    source_policy_signals = _source_policy_signals(first, selected_names)
    output_jobs = _load_output_jobs(output_dir)
    decision_report = {**first, "jobs": output_jobs}
    registry_page_signals = _registry_page_signals(selected_names)
    next_optimization_targets = _next_optimization_targets(
        source_policy_signals,
        registry_page_signals=registry_page_signals,
    )
    source_decision_matrix = _source_decision_matrix(
        next_optimization_targets,
        source_policy_signals,
        decision_report,
    )
    source_decision_trend = _source_decision_trend(source_decision_matrix, previous_payload)

    payload = {
        "outputDir": str(output_dir),
        "sources": selected_names,
        "benchmarkGroup": str(args.group or "custom"),
        "totalDurationMs": first_duration_ms + second_duration_ms,
        "firstRunDurationMs": first_duration_ms,
        "secondRunDurationMs": second_duration_ms,
        "stageDurationsMs": _stage_durations_ms(first, second),
        "networkWaitCounters": _network_wait_counters(first, second),
        "sourceTimingSignals": {
            "firstRunSlowestSources": _slowest_sources(first),
            "secondRunSlowestSources": _slowest_sources(second),
            "firstRunSlowestProviderBoards": _slowest_provider_boards(first),
            "secondRunSlowestProviderBoards": _slowest_provider_boards(second),
        },
        "sourcePolicySignals": source_policy_signals,
        "sourceRegistrySignals": registry_page_signals,
        "registryScopeSummary": _registry_scope_summary(registry_page_signals),
        "nextOptimizationTargets": next_optimization_targets,
        "sourceDecisionMatrix": source_decision_matrix,
        "sourceDecisionTrend": source_decision_trend,
        "firstRun": {
            "summary": dict(first.get("summary") or {}),
            "runtime": dict(first.get("runtime") or {}),
            "familySummary": _family_summary(first, selected_names),
        },
        "secondRun": {
            "summary": dict(second.get("summary") or {}),
            "runtime": dict(second.get("runtime") or {}),
            "familySummary": _family_summary(second, selected_names),
        },
        "storageMetrics": snapshot_storage_metrics(output_dir),
    }
    (output_dir / "benchmark-summary.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (output_dir / "source-decision-matrix.md").write_text(
        _render_source_decision_matrix_markdown(
            [dict(row) for row in _as_list(payload.get("sourceDecisionMatrix"))]
        ),
        encoding="utf-8",
    )
    (output_dir / "source-decision-log-template.md").write_text(
        _render_source_decision_log_template_markdown(
            [dict(row) for row in _as_list(payload.get("sourceDecisionMatrix"))]
        ),
        encoding="utf-8",
    )
    (output_dir / "source-decision-trend.md").write_text(
        _render_source_decision_trend_markdown(dict(payload.get("sourceDecisionTrend") or {})),
        encoding="utf-8",
    )
    print(json.dumps(payload, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
