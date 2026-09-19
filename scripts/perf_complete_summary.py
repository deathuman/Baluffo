#!/usr/bin/env python3
"""Console summary and complete-row recording.

Leaf of ``scripts/perf_complete.py``; every unit body is byte-identical to the pre-split
module. The coordinator imports and re-exports these names.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
del _repo_root

from scripts.perf_complete_process import _format_top_contributors, _record_row
from scripts.perf_complete_profiles import _read_stat_value

__all__ = [
    "Any",
    "_format_top_contributors",
    "_print_console_summary",
    "_read_stat_value",
    "_record_complete_rows",
    "_record_row",
]


def _print_console_summary(summary: dict[str, Any]) -> None:
    print("\nComplete benchmark summary", flush=True)
    sync = summary["benchmarks"]["sync"]
    startup = summary["benchmarks"]["startup"]
    rows = [
        (
            "discovery",
            summary["benchmarks"]["discovery"].get("medianDurationMs", 0),
            summary["benchmarks"]["discovery"],
            None,
        ),
        (
            "fetch",
            summary["benchmarks"]["fetch"].get("medianDurationMs", 0),
            summary["benchmarks"]["fetch"],
            None,
        ),
        (
            "frontendBoot",
            summary["benchmarks"]["frontendBoot"].get("durationMs", 0),
            summary["benchmarks"]["frontendBoot"],
            None,
        ),
        (
            "startup.jobs.cold",
            startup["jobs"]["cold"].get("durationMs", 0),
            startup["jobs"]["cold"],
            None,
        ),
        (
            "startup.jobs.warm",
            startup["jobs"]["warm"].get("durationMs", 0),
            startup["jobs"]["warm"],
            None,
        ),
        (
            "startup.admin.cold",
            startup["admin"]["cold"].get("durationMs", 0),
            startup["admin"]["cold"],
            None,
        ),
        (
            "startup.admin.warm",
            startup["admin"]["warm"].get("durationMs", 0),
            startup["admin"]["warm"],
            None,
        ),
        (
            "sync.push",
            sync.get("pushTiming", {}).get("totalDurationMs", 0),
            sync,
            sync.get("comparisons", {}).get("push", {}).get("status"),
        ),
        (
            "sync.pull",
            sync.get("pullTiming", {}).get("totalDurationMs", 0),
            sync,
            sync.get("comparisons", {}).get("pull", {}).get("status"),
        ),
    ]
    print("name,durationMs,peakRamBytes,artifactBytes,status,topContributors", flush=True)
    for name, duration, section, status_override in rows:
        memory = (
            section.get("memoryMetrics") if isinstance(section.get("memoryMetrics"), dict) else {}
        )
        artifacts = (
            section.get("artifactSizes") if isinstance(section.get("artifactSizes"), dict) else {}
        )
        status = (
            status_override
            or section.get("status")
            or section.get("comparison", {}).get("status")
            or ""
        )
        peak_ram = max(
            int(memory.get("peakWorkingSetBytes") or 0),
            int(memory.get("peakRssBytes") or 0),
        )
        print(
            f"{name},{int(duration or 0)},{peak_ram},{int(artifacts.get('totalBytes') or 0)},{status},{_format_top_contributors(memory)}",
            flush=True,
        )
    bridge = summary["benchmarks"].get("bridgeProfile")
    if isinstance(bridge, dict):
        top_routes = (
            bridge.get("topRoutesByP95") if isinstance(bridge.get("topRoutesByP95"), list) else []
        )
        top_operations = (
            bridge.get("topOperationsByP95")
            if isinstance(bridge.get("topOperationsByP95"), list)
            else []
        )
        if top_routes or top_operations:
            print("\nBridge profile top timings", flush=True)
            print("kind,source,label,p95Ms,avgMs,count,errorCount", flush=True)
            for kind, rows in (("route", top_routes[:5]), ("operation", top_operations[:5])):
                for row in rows:
                    line = (
                        f"{kind},{row.get('source')},{row.get('label')},"
                        f"{int(row.get('p95Ms') or 0)},{int(row.get('avgMs') or 0)},"
                        f"{int(row.get('count') or 0)},{int(row.get('errorCount') or 0)}"
                    )
                    print(
                        line,
                        flush=True,
                    )
    fetch_source_timing = summary["benchmarks"].get("fetch", {}).get("sourceTiming")
    if isinstance(fetch_source_timing, dict):
        top_sources = (
            fetch_source_timing.get("topSourcesByDuration")
            if isinstance(fetch_source_timing.get("topSourcesByDuration"), list)
            else []
        )
        top_boards = (
            fetch_source_timing.get("topProviderBoardsByDuration")
            if isinstance(fetch_source_timing.get("topProviderBoardsByDuration"), list)
            else []
        )
        source_breakdown = (
            fetch_source_timing.get("providerSourceBreakdown")
            if isinstance(fetch_source_timing.get("providerSourceBreakdown"), list)
            else []
        )
        host_breakdown = (
            fetch_source_timing.get("providerHostBreakdown")
            if isinstance(fetch_source_timing.get("providerHostBreakdown"), list)
            else []
        )
        if top_sources or top_boards:
            print("\nFetch source timing", flush=True)
            print("kind,run,source,label,durationMs,status", flush=True)
            for row in top_sources[:5]:
                print(
                    f"source,{row.get('run')},{row.get('name')},{row.get('adapter')},"
                    f"{int(row.get('durationMs') or 0)},",
                    flush=True,
                )
            for row in top_boards[:5]:
                print(
                    f"provider-board,{row.get('run')},{row.get('source')},{row.get('name')},"
                    f"{int(row.get('durationMs') or 0)},{row.get('status')}",
                    flush=True,
                )
            for row in source_breakdown[:5]:
                print(
                    f"provider-source-summary,,{row.get('source')},{row.get('adapter')},"
                    f"{int(row.get('totalDurationMs') or 0)},"
                    f"{row.get('statuses')}",
                    flush=True,
                )
            for row in host_breakdown[:5]:
                print(
                    f"provider-host-summary,,{row.get('source')},{row.get('providerHost')},"
                    f"{int(row.get('totalDurationMs') or 0)},"
                    f"{row.get('statuses')}",
                    flush=True,
                )
    sync_detail = summary["benchmarks"].get("syncDetail")
    if isinstance(sync_detail, dict):
        stage_rows = (
            sync_detail.get("stageTop") if isinstance(sync_detail.get("stageTop"), list) else []
        )
        if stage_rows:
            print("\nSync push detail timing", flush=True)
            print("stage,durationMs", flush=True)
            for row in stage_rows[:8]:
                print(f"{row.get('stage')},{int(row.get('durationMs') or 0)}", flush=True)
        remote_rows = (
            sync_detail.get("remoteOperationTop")
            if isinstance(sync_detail.get("remoteOperationTop"), list)
            else []
        )
        if remote_rows:
            print("\nSync remote timing", flush=True)
            print("operation,durationMs", flush=True)
            for row in remote_rows[:8]:
                print(
                    f"{row.get('operation')},{int(row.get('durationMs') or 0)}",
                    flush=True,
                )
        remote_wall_rows = (
            sync_detail.get("remoteStageWallTop")
            if isinstance(sync_detail.get("remoteStageWallTop"), list)
            else []
        )
        if remote_wall_rows:
            print("\nSync remote wall timing", flush=True)
            print("stage,wallMs", flush=True)
            for row in remote_wall_rows[:8]:
                print(f"{row.get('stage')},{int(row.get('wallMs') or 0)}", flush=True)
    storage_reads = summary["benchmarks"].get("storageReadProfile")
    if isinstance(storage_reads, dict):
        duration_rows = (
            storage_reads.get("topReadsByDuration")
            if isinstance(storage_reads.get("topReadsByDuration"), list)
            else []
        )
        byte_rows = (
            storage_reads.get("topReadsByBytes")
            if isinstance(storage_reads.get("topReadsByBytes"), list)
            else []
        )
        if duration_rows or byte_rows:
            print("\nStorage read profile", flush=True)
            print(
                "kind,source,surface,artifact,maxMs,maxBytes,readCount,failedReadCount", flush=True
            )
            seen: set[tuple[str, str, str]] = set()
            for kind, rows in (("duration", duration_rows[:5]), ("bytes", byte_rows[:5])):
                for row in rows:
                    identity = (
                        str(row.get("source") or ""),
                        str(row.get("surface") or ""),
                        str(row.get("artifact") or ""),
                    )
                    if identity in seen:
                        continue
                    seen.add(identity)
                    print(
                        f"{kind},{row.get('source')},{row.get('surface')},{row.get('artifact')},"
                        f"{_read_stat_value(row, 'durationMs', 'max')},"
                        f"{_read_stat_value(row, 'bytesRead', 'max')},"
                        f"{int(row.get('readCount') or 0)},"
                        f"{int(row.get('failedReadCount') or 0)}",
                        flush=True,
                    )
    memory_profile = summary["benchmarks"].get("memoryProfile")
    if isinstance(memory_profile, dict):
        memory_rows = (
            memory_profile.get("topSamplesByPeakRam")
            if isinstance(memory_profile.get("topSamplesByPeakRam"), list)
            else []
        )
        if memory_rows:
            print("\nMemory profile peaks", flush=True)
            print("source,peakRamBytes,lastSampleBytes,topCategory,topProcess", flush=True)
            for row in memory_rows[:5]:
                category_peaks = (
                    row.get("categoryPeaks") if isinstance(row.get("categoryPeaks"), dict) else {}
                )
                top_category = (
                    max(category_peaks, key=lambda key: int(category_peaks.get(key) or 0))
                    if category_peaks
                    else ""
                )
                top_processes = (
                    row.get("topProcesses") if isinstance(row.get("topProcesses"), list) else []
                )
                top_process = (
                    top_processes[0] if top_processes and isinstance(top_processes[0], dict) else {}
                )
                print(
                    f"{row.get('source')},{int(row.get('peakBytes') or 0)},"
                    f"{int(row.get('lastSampleBytes') or 0)},"
                    f"{top_category},{top_process.get('name') or ''}",
                    flush=True,
                )
    targets = summary.get("optimizationTargets")
    if isinstance(targets, list) and targets:
        print("\nOptimization targets", flush=True)
        print("kind,source,label,durationMs,rankValue,rankUnit", flush=True)
        for row in targets[:8]:
            if isinstance(row, dict):
                line = (
                    f"{row.get('kind')},{row.get('source')},{row.get('label')},"
                    f"{int(row.get('durationMs') or 0)},"
                    f"{int(row.get('rankValue') or row.get('durationMs') or 0)},"
                    f"{row.get('rankUnit') or 'ms'}"
                )
                print(
                    line,
                    flush=True,
                )


def _record_complete_rows(
    summary: dict[str, Any],
    *,
    baseline_dir: Path,
    trend_path: Path,
    record_baseline: bool,
    record_trend: bool,
) -> None:
    artifact = str(summary.get("summaryPath") or "")
    discovery = summary["benchmarks"]["discovery"]
    fetch = summary["benchmarks"]["fetch"]
    frontend = summary["benchmarks"]["frontendBoot"]
    startup = summary["benchmarks"]["startup"]
    sync = summary["benchmarks"]["sync"]
    rows = [
        (
            "discovery",
            discovery.get("medianDurationMs"),
            discovery.get("stageMedianDurationsMs"),
            discovery.get("comparison", {}).get("status"),
        ),
        (
            "fetch",
            fetch.get("medianDurationMs"),
            fetch.get("stageMedianDurationsMs"),
            fetch.get("comparison", {}).get("status"),
        ),
        (
            "frontend-boot",
            frontend.get("durationMs"),
            {row["page"]: row["durationMs"] for row in frontend.get("pages", [])},
            frontend.get("comparison", {}).get("status"),
        ),
        (
            "startup-cold",
            startup["jobs"]["cold"].get("durationMs"),
            startup["jobs"]["cold"].get("stageDurationsMs"),
            startup["jobs"]["cold"].get("comparison", {}).get("status"),
        ),
        (
            "startup-warm",
            startup["jobs"]["warm"].get("durationMs"),
            startup["jobs"]["warm"].get("stageDurationsMs"),
            startup["jobs"]["warm"].get("comparison", {}).get("status"),
        ),
        (
            "startup-admin-cold",
            startup["admin"]["cold"].get("durationMs"),
            startup["admin"]["cold"].get("stageDurationsMs"),
            startup["admin"]["cold"].get("comparison", {}).get("status"),
        ),
        (
            "startup-admin-warm",
            startup["admin"]["warm"].get("durationMs"),
            startup["admin"]["warm"].get("stageDurationsMs"),
            startup["admin"]["warm"].get("comparison", {}).get("status"),
        ),
        (
            "sync-push",
            sync.get("pushTiming", {}).get("totalDurationMs"),
            sync.get("pushTiming", {}).get("stageTotalsMs"),
            sync.get("comparisons", {}).get("push", {}).get("status"),
        ),
        (
            "sync-pull",
            sync.get("pullTiming", {}).get("totalDurationMs"),
            sync.get("pullTiming", {}).get("stageTotalsMs"),
            sync.get("comparisons", {}).get("pull", {}).get("status"),
        ),
    ]
    for mode, duration_ms, stages, status in rows:
        _record_row(
            mode=mode,
            duration_ms=int(duration_ms or 0),
            status=str(status or "baseline_missing"),
            stage_durations_ms=stages if isinstance(stages, dict) else {},
            artifact=artifact,
            baseline_dir=baseline_dir,
            trend_path=trend_path,
            record_baseline=record_baseline,
            record_trend=record_trend,
        )
