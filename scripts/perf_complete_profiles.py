#!/usr/bin/env python3
"""Storage, memory, sync, chrome-trace and optimization summaries.

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

from scripts.perf_complete_process import _artifact_path_from_summary, _read_json
from scripts.perf_complete_spec import summarize_trace_file

__all__ = [
    "Any",
    "_artifact_path_from_summary",
    "_memory_sample",
    "_read_json",
    "_read_stat_value",
    "_storage_metrics_payload",
    "_storage_read_sample",
    "build_chrome_trace_summary",
    "build_memory_profile_summary",
    "build_optimization_targets",
    "build_storage_read_profile_summary",
    "build_sync_detail_summary",
    "summarize_trace_file",
]


def _storage_metrics_payload(path: Path | None) -> dict[str, Any]:
    payload = _read_json(path) if path is not None else {}
    storage_metrics = (
        payload.get("storageMetrics") if isinstance(payload.get("storageMetrics"), dict) else {}
    )
    return storage_metrics if storage_metrics else payload


def _read_stat_value(row: dict[str, Any], field: str, stat: str) -> int:
    stats = row.get(field) if isinstance(row.get(field), dict) else {}
    return int(stats.get(stat) or 0)


def _storage_read_sample(
    *,
    source: str,
    storage_path: Path | None,
    page: str = "",
    mode: str = "",
) -> dict[str, Any]:
    metrics = _storage_metrics_payload(storage_path)
    reads = metrics.get("reads") if isinstance(metrics.get("reads"), dict) else {}
    surfaces = reads.get("surfaces") if isinstance(reads.get("surfaces"), list) else []
    return {
        "source": source,
        "page": page,
        "mode": mode,
        "storageMetricsPath": str(storage_path or ""),
        "ok": bool(metrics),
        "readCount": int(reads.get("readCount") or 0),
        "failedReadCount": int(reads.get("failedReadCount") or 0),
        "surfaceCount": int(reads.get("surfaceCount") or 0),
        "error": str(metrics.get("error") or ""),
        "surfaces": [dict(row) for row in surfaces if isinstance(row, dict)],
    }


def build_storage_read_profile_summary(
    *,
    startup: dict[str, Any],
    sync: dict[str, Any],
) -> dict[str, Any]:
    samples: list[dict[str, Any]] = []
    for page in ("jobs", "admin"):
        page_summary = startup.get(page) if isinstance(startup.get(page), dict) else {}
        for mode in ("cold", "warm"):
            section = page_summary.get(mode) if isinstance(page_summary.get(mode), dict) else {}
            samples.append(
                _storage_read_sample(
                    source=f"startup.{page}.{mode}",
                    page=page,
                    mode=mode,
                    storage_path=_artifact_path_from_summary(section, "storageMetricsSnapshot"),
                )
            )
    samples.append(
        _storage_read_sample(
            source="sync",
            storage_path=_artifact_path_from_summary(sync, "storageMetricsSnapshot"),
        )
    )

    rows: list[dict[str, Any]] = []
    for sample in samples:
        for row in sample.get("surfaces") if isinstance(sample.get("surfaces"), list) else []:
            if not isinstance(row, dict):
                continue
            rows.append(
                {
                    "source": str(sample.get("source") or ""),
                    "page": str(sample.get("page") or ""),
                    "mode": str(sample.get("mode") or ""),
                    "storageMetricsPath": str(sample.get("storageMetricsPath") or ""),
                    "surface": str(row.get("surface") or ""),
                    "artifact": str(row.get("artifact") or ""),
                    "storageKind": str(row.get("storageKind") or ""),
                    "readCount": int(row.get("readCount") or 0),
                    "failedReadCount": int(row.get("failedReadCount") or 0),
                    "durationMs": dict(row.get("durationMs") or {}),
                    "bytesRead": dict(row.get("bytesRead") or {}),
                    "rowCount": dict(row.get("rowCount") or {}),
                    "memoryDeltaBytes": dict(row.get("memoryDeltaBytes") or {}),
                }
            )
    rows_by_duration = sorted(
        rows,
        key=lambda row: (
            _read_stat_value(row, "durationMs", "max"),
            _read_stat_value(row, "durationMs", "total"),
            int(row.get("readCount") or 0),
        ),
        reverse=True,
    )
    rows_by_bytes = sorted(
        rows,
        key=lambda row: (
            _read_stat_value(row, "bytesRead", "max"),
            _read_stat_value(row, "rowCount", "max"),
            int(row.get("readCount") or 0),
        ),
        reverse=True,
    )
    failed_rows = [row for row in rows_by_duration if int(row.get("failedReadCount") or 0) > 0]
    public_samples = [
        {key: value for key, value in sample.items() if key != "surfaces"} for sample in samples
    ]
    return {
        "samples": public_samples,
        "topReadsByDuration": rows_by_duration[:15],
        "topReadsByBytes": rows_by_bytes[:15],
        "failedReads": failed_rows[:15],
    }


def _memory_sample(source: str, metrics: dict[str, Any]) -> dict[str, Any]:
    peak_sample = metrics.get("peakSample") if isinstance(metrics.get("peakSample"), dict) else {}
    first_sample = (
        metrics.get("firstSample") if isinstance(metrics.get("firstSample"), dict) else {}
    )
    last_sample = metrics.get("lastSample") if isinstance(metrics.get("lastSample"), dict) else {}
    top_processes = (
        metrics.get("topProcesses") if isinstance(metrics.get("topProcesses"), list) else []
    )
    if not top_processes:
        top_processes = (
            peak_sample.get("processes") if isinstance(peak_sample.get("processes"), list) else []
        )
    category_peaks = (
        metrics.get("categoryPeaks") if isinstance(metrics.get("categoryPeaks"), dict) else {}
    )
    peak_bytes = max(
        int(metrics.get("peakWorkingSetBytes") or 0),
        int(metrics.get("peakRssBytes") or 0),
    )
    first_bytes = int(first_sample.get("memoryBytes") or 0)
    last_bytes = int(last_sample.get("memoryBytes") or 0)
    return {
        "source": source,
        "sampleCount": int(metrics.get("sampleCount") or 0),
        "peakWorkingSetBytes": int(metrics.get("peakWorkingSetBytes") or 0),
        "peakRssBytes": int(metrics.get("peakRssBytes") or 0),
        "peakBytes": peak_bytes,
        "firstSampleBytes": first_bytes,
        "lastSampleBytes": last_bytes,
        "peakToLastDeltaBytes": max(0, peak_bytes - last_bytes),
        "firstToLastDeltaBytes": last_bytes - first_bytes,
        "maxProcessCount": int(metrics.get("maxProcessCount") or 0),
        "unsupportedReason": str(metrics.get("unsupportedReason") or ""),
        "categoryPeaks": {str(key): int(value or 0) for key, value in category_peaks.items()},
        "topProcesses": [
            {
                "pid": int(row.get("pid") or 0),
                "name": str(row.get("name") or ""),
                "category": str(row.get("category") or "other"),
                "peakBytes": max(
                    int(row.get("peakBytes") or row.get("memoryBytes") or 0),
                    int(row.get("peakWorkingSetBytes") or row.get("workingSetBytes") or 0),
                    int(row.get("peakRssBytes") or row.get("rssBytes") or 0),
                ),
            }
            for row in top_processes
            if isinstance(row, dict)
        ][:10],
    }


def build_memory_profile_summary(benchmarks: dict[str, Any]) -> dict[str, Any]:
    raw_samples: list[tuple[str, dict[str, Any]]] = []
    for source in ("discovery", "fetch", "frontendBoot", "sync"):
        section = benchmarks.get(source) if isinstance(benchmarks.get(source), dict) else {}
        metrics = (
            section.get("memoryMetrics") if isinstance(section.get("memoryMetrics"), dict) else {}
        )
        if metrics:
            raw_samples.append((source, metrics))
    startup = benchmarks.get("startup") if isinstance(benchmarks.get("startup"), dict) else {}
    for page in ("jobs", "admin"):
        page_summary = startup.get(page) if isinstance(startup.get(page), dict) else {}
        for mode in ("cold", "warm"):
            section = page_summary.get(mode) if isinstance(page_summary.get(mode), dict) else {}
            metrics = (
                section.get("memoryMetrics")
                if isinstance(section.get("memoryMetrics"), dict)
                else {}
            )
            if metrics:
                raw_samples.append((f"startup.{page}.{mode}", metrics))
    samples = [_memory_sample(source, metrics) for source, metrics in raw_samples]
    top_samples = sorted(samples, key=lambda row: int(row.get("peakBytes") or 0), reverse=True)
    category_rows: list[dict[str, Any]] = []
    for sample in samples:
        for category, peak_bytes in dict(sample.get("categoryPeaks") or {}).items():
            category_rows.append(
                {
                    "source": str(sample.get("source") or ""),
                    "category": str(category or "other"),
                    "peakBytes": int(peak_bytes or 0),
                }
            )
    category_rows.sort(key=lambda row: int(row.get("peakBytes") or 0), reverse=True)
    process_rows: list[dict[str, Any]] = []
    for sample in samples:
        for row in (
            sample.get("topProcesses") if isinstance(sample.get("topProcesses"), list) else []
        ):
            if isinstance(row, dict):
                process_rows.append({"source": str(sample.get("source") or ""), **row})
    process_rows.sort(key=lambda row: int(row.get("peakBytes") or 0), reverse=True)
    steady_rows = sorted(
        samples, key=lambda row: int(row.get("lastSampleBytes") or 0), reverse=True
    )
    retained_peak_rows = sorted(
        samples,
        key=lambda row: (
            int(row.get("lastSampleBytes") or 0),
            -int(row.get("peakToLastDeltaBytes") or 0),
        ),
        reverse=True,
    )
    return {
        "samples": samples,
        "topSamplesByPeakRam": top_samples[:10],
        "topSamplesBySteadyStateRam": steady_rows[:10],
        "topSamplesByRetainedPeakRam": retained_peak_rows[:10],
        "topCategoryPeaks": category_rows[:15],
        "topProcesses": process_rows[:15],
    }


def build_sync_detail_summary(sync: dict[str, Any]) -> dict[str, Any]:
    push_timing = sync.get("pushTiming") if isinstance(sync.get("pushTiming"), dict) else {}
    detail = (
        push_timing.get("detailTiming") if isinstance(push_timing.get("detailTiming"), dict) else {}
    )
    stage_totals = (
        detail.get("stageTotalsMs") if isinstance(detail.get("stageTotalsMs"), dict) else {}
    )
    stage_rows = [
        {"stage": str(stage), "durationMs": int(duration or 0)}
        for stage, duration in stage_totals.items()
        if int(duration or 0) > 0
    ]
    stage_rows.sort(key=lambda row: int(row.get("durationMs") or 0), reverse=True)
    remote_timing = (
        push_timing.get("remoteTiming") if isinstance(push_timing.get("remoteTiming"), dict) else {}
    )
    operation_totals = (
        remote_timing.get("operationTotalsMs")
        if isinstance(remote_timing.get("operationTotalsMs"), dict)
        else {}
    )
    operation_rows = [
        {"operation": str(operation), "durationMs": int(duration or 0)}
        for operation, duration in operation_totals.items()
        if int(duration or 0) > 0
    ]
    operation_rows.sort(key=lambda row: int(row.get("durationMs") or 0), reverse=True)
    stage_wall = (
        remote_timing.get("stageWallMs")
        if isinstance(remote_timing.get("stageWallMs"), dict)
        else {}
    )
    stage_wall_rows = [
        {"stage": str(stage), "wallMs": int(duration or 0)}
        for stage, duration in stage_wall.items()
        if int(duration or 0) > 0
    ]
    stage_wall_rows.sort(key=lambda row: int(row.get("wallMs") or 0), reverse=True)
    slowest_requests = (
        remote_timing.get("slowestRequests")
        if isinstance(remote_timing.get("slowestRequests"), list)
        else []
    )
    return {
        "available": bool(stage_rows),
        "source": "sync.push",
        "stageTotalsMs": {row["stage"]: row["durationMs"] for row in stage_rows},
        "stageTop": stage_rows[:15],
        "remoteTimingAvailable": bool(remote_timing),
        "remoteRequestCount": int(remote_timing.get("requestCount") or 0),
        "remoteTotalRequestDurationMs": int(remote_timing.get("totalRequestDurationMs") or 0),
        "remoteWallDurationMs": int(remote_timing.get("wallDurationMs") or 0),
        "remoteStageWallMs": {row["stage"]: row["wallMs"] for row in stage_wall_rows},
        "remoteStageWallTop": stage_wall_rows[:12],
        "remoteOperationTotalsMs": {row["operation"]: row["durationMs"] for row in operation_rows},
        "remoteOperationTop": operation_rows[:12],
        "remoteSlowestRequests": [row for row in slowest_requests[:20] if isinstance(row, dict)],
        "reportPath": str(sync.get("reportPath") or ""),
    }


def build_chrome_trace_summary(trace_paths: list[str] | None) -> dict[str, Any]:
    samples: list[dict[str, Any]] = []
    for raw_path in trace_paths or []:
        trace_path = str(raw_path or "").strip()
        if not trace_path:
            continue
        try:
            samples.append(summarize_trace_file(trace_path))
        except Exception as exc:  # noqa: BLE001 - diagnostic input should not fail perf:complete.
            samples.append(
                {
                    "ok": False,
                    "tracePath": str(Path(trace_path).expanduser()),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

    latest_lcp: list[dict[str, Any]] = []
    slow_resources: list[dict[str, Any]] = []
    slow_user_timings: list[dict[str, Any]] = []
    for sample in samples:
        trace_path = str(sample.get("tracePath") or "")
        lcp = sample.get("latestLcp") if isinstance(sample.get("latestLcp"), dict) else {}
        if lcp:
            latest_lcp.append({**lcp, "tracePath": trace_path})
        rows = sample.get("slowResources") if isinstance(sample.get("slowResources"), list) else []
        for row in rows:
            if isinstance(row, dict):
                slow_resources.append({**row, "tracePath": trace_path})
        rows = (
            sample.get("slowUserTimings") if isinstance(sample.get("slowUserTimings"), list) else []
        )
        for row in rows:
            if isinstance(row, dict):
                slow_user_timings.append({**row, "tracePath": trace_path})

    return {
        "samples": samples,
        "latestLcp": sorted(
            latest_lcp,
            key=lambda row: float(row.get("startMs") or 0),
            reverse=True,
        )[:10],
        "topSlowResources": sorted(
            slow_resources,
            key=lambda row: float(row.get("durationMs") or 0),
            reverse=True,
        )[:20],
        "topSlowUserTimings": sorted(
            slow_user_timings,
            key=lambda row: float(row.get("durationMs") or 0),
            reverse=True,
        )[:20],
    }


def build_optimization_targets(benchmarks: dict[str, Any]) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []

    def add(
        kind: str,
        source: str,
        label: str,
        duration_ms: int,
        evidence: str = "",
        *,
        rank_value: int | None = None,
        rank_unit: str = "ms",
    ) -> None:
        resolved_rank = int(duration_ms if rank_value is None else rank_value)
        if resolved_rank <= 0:
            return
        targets.append(
            {
                "kind": kind,
                "source": source,
                "label": label,
                "durationMs": int(duration_ms or 0),
                "rankValue": resolved_rank,
                "rankUnit": str(rank_unit or "ms"),
                "evidence": evidence,
            }
        )

    discovery = benchmarks.get("discovery") if isinstance(benchmarks.get("discovery"), dict) else {}
    fetch = benchmarks.get("fetch") if isinstance(benchmarks.get("fetch"), dict) else {}
    frontend = (
        benchmarks.get("frontendBoot") if isinstance(benchmarks.get("frontendBoot"), dict) else {}
    )
    startup = benchmarks.get("startup") if isinstance(benchmarks.get("startup"), dict) else {}
    sync = benchmarks.get("sync") if isinstance(benchmarks.get("sync"), dict) else {}
    sync_detail = (
        benchmarks.get("syncDetail") if isinstance(benchmarks.get("syncDetail"), dict) else {}
    )
    bridge = (
        benchmarks.get("bridgeProfile") if isinstance(benchmarks.get("bridgeProfile"), dict) else {}
    )
    storage_reads = (
        benchmarks.get("storageReadProfile")
        if isinstance(benchmarks.get("storageReadProfile"), dict)
        else {}
    )
    memory_profile = (
        benchmarks.get("memoryProfile") if isinstance(benchmarks.get("memoryProfile"), dict) else {}
    )
    chrome_traces = (
        benchmarks.get("chromeTraces") if isinstance(benchmarks.get("chromeTraces"), dict) else {}
    )

    add(
        "benchmark",
        "discovery",
        "median duration",
        int(discovery.get("medianDurationMs") or 0),
        str(discovery.get("payloadPath") or ""),
    )
    add(
        "benchmark",
        "fetch",
        "median duration",
        int(fetch.get("medianDurationMs") or 0),
        str(fetch.get("payloadPath") or ""),
    )
    frontend_pages = frontend.get("pages") if isinstance(frontend.get("pages"), list) else []
    for page in frontend_pages:
        if isinstance(page, dict):
            add(
                "frontend-boot",
                str(page.get("page") or ""),
                "navigation duration",
                int(page.get("durationMs") or 0),
                str(page.get("summaryPath") or ""),
            )
    for page in ("jobs", "admin"):
        page_summary = startup.get(page) if isinstance(startup.get(page), dict) else {}
        for mode in ("cold", "warm"):
            section = page_summary.get(mode) if isinstance(page_summary.get(mode), dict) else {}
            stage_rows = section.get("stageDurationsMs")
            if not isinstance(stage_rows, dict):
                continue
            for label, duration_ms in stage_rows.items():
                add(
                    "startup-stage",
                    f"startup.{page}.{mode}",
                    str(label),
                    int(duration_ms or 0),
                    str(section.get("reportPath") or ""),
                )
    source_timing = fetch.get("sourceTiming") if isinstance(fetch.get("sourceTiming"), dict) else {}
    source_rows = (
        source_timing.get("topSourcesByDuration")
        if isinstance(source_timing.get("topSourcesByDuration"), list)
        else []
    )
    for row in source_rows:
        if isinstance(row, dict):
            add(
                "fetch-source",
                str(row.get("run") or ""),
                str(row.get("name") or row.get("adapter") or ""),
                int(row.get("durationMs") or 0),
                str(fetch.get("payloadPath") or ""),
            )
    provider_rows = (
        source_timing.get("topProviderBoardsByDuration")
        if isinstance(source_timing.get("topProviderBoardsByDuration"), list)
        else []
    )
    for row in provider_rows:
        if isinstance(row, dict):
            add(
                "fetch-provider-board",
                str(row.get("source") or row.get("adapter") or ""),
                str(row.get("name") or row.get("studio") or ""),
                int(row.get("durationMs") or 0),
                str(row.get("providerUrl") or ""),
            )
    provider_host_rows = (
        source_timing.get("providerHostBreakdown")
        if isinstance(source_timing.get("providerHostBreakdown"), list)
        else []
    )
    for row in provider_host_rows:
        if isinstance(row, dict):
            add(
                "fetch-provider-host",
                str(row.get("source") or row.get("adapter") or ""),
                str(row.get("providerHost") or row.get("adapter") or ""),
                int(row.get("totalDurationMs") or 0),
                str(row.get("statuses") or ""),
            )
    fetch_targets = (
        source_timing.get("nextOptimizationTargets")
        if isinstance(source_timing.get("nextOptimizationTargets"), list)
        else []
    )
    for row in fetch_targets:
        if isinstance(row, dict):
            add(
                "fetch-policy-target",
                str(row.get("action") or ""),
                str(row.get("name") or ""),
                int(row.get("durationMs") or 0),
                str(row.get("reasons") or ""),
            )
    bridge_routes = (
        bridge.get("topRoutesByP95") if isinstance(bridge.get("topRoutesByP95"), list) else []
    )
    for row in bridge_routes:
        if isinstance(row, dict):
            add(
                "bridge-route",
                str(row.get("source") or ""),
                str(row.get("label") or ""),
                int(row.get("p95Ms") or 0),
                str(row.get("profilePath") or ""),
            )
    bridge_operations = (
        bridge.get("topOperationsByP95")
        if isinstance(bridge.get("topOperationsByP95"), list)
        else []
    )
    for row in bridge_operations:
        if isinstance(row, dict):
            add(
                "bridge-operation",
                str(row.get("source") or ""),
                str(row.get("label") or ""),
                int(row.get("p95Ms") or 0),
                str(row.get("profilePath") or ""),
            )
    storage_duration_rows = (
        storage_reads.get("topReadsByDuration")
        if isinstance(storage_reads.get("topReadsByDuration"), list)
        else []
    )
    for row in storage_duration_rows:
        if isinstance(row, dict):
            add(
                "storage-read-duration",
                str(row.get("source") or ""),
                str(row.get("surface") or row.get("artifact") or ""),
                _read_stat_value(row, "durationMs", "max"),
                str(row.get("storageMetricsPath") or ""),
            )
    storage_byte_rows = (
        storage_reads.get("topReadsByBytes")
        if isinstance(storage_reads.get("topReadsByBytes"), list)
        else []
    )
    for row in storage_byte_rows:
        if isinstance(row, dict):
            byte_count = _read_stat_value(row, "bytesRead", "max")
            if byte_count <= 0:
                continue
            add(
                "storage-read-bytes",
                str(row.get("source") or ""),
                str(row.get("surface") or row.get("artifact") or ""),
                0,
                f"{byte_count} bytes; {row.get('storageMetricsPath') or ''}",
                rank_value=byte_count,
                rank_unit="bytes",
            )
    failed_read_rows = (
        storage_reads.get("failedReads")
        if isinstance(storage_reads.get("failedReads"), list)
        else []
    )
    for row in failed_read_rows:
        if isinstance(row, dict):
            add(
                "storage-read-failure",
                str(row.get("source") or ""),
                str(row.get("surface") or row.get("artifact") or ""),
                0,
                str(row.get("storageMetricsPath") or ""),
                rank_value=int(row.get("failedReadCount") or 0),
                rank_unit="count",
            )
    memory_rows = (
        memory_profile.get("topSamplesByPeakRam")
        if isinstance(memory_profile.get("topSamplesByPeakRam"), list)
        else []
    )
    for row in memory_rows:
        if isinstance(row, dict):
            peak_bytes = int(row.get("peakBytes") or 0)
            if peak_bytes <= 0:
                continue
            top_category = ""
            category_peaks = (
                row.get("categoryPeaks") if isinstance(row.get("categoryPeaks"), dict) else {}
            )
            if category_peaks:
                top_category = max(
                    category_peaks,
                    key=lambda key: int(category_peaks.get(key) or 0),
                )
            add(
                "memory-peak",
                str(row.get("source") or ""),
                top_category or "process tree peak",
                0,
                f"{peak_bytes} bytes",
                rank_value=peak_bytes,
                rank_unit="bytes",
            )
    steady_memory_rows = (
        memory_profile.get("topSamplesBySteadyStateRam")
        if isinstance(memory_profile.get("topSamplesBySteadyStateRam"), list)
        else []
    )
    for row in steady_memory_rows:
        if isinstance(row, dict):
            last_bytes = int(row.get("lastSampleBytes") or 0)
            if last_bytes <= 0:
                continue
            add(
                "memory-steady-state",
                str(row.get("source") or ""),
                "last sample process tree",
                0,
                f"{last_bytes} bytes",
                rank_value=last_bytes,
                rank_unit="bytes",
            )
    add(
        "sync",
        "sync.push",
        "total duration",
        int(sync.get("pushTiming", {}).get("totalDurationMs") or 0),
        str(sync.get("reportPath") or ""),
    )
    add(
        "sync",
        "sync.pull",
        "total duration",
        int(sync.get("pullTiming", {}).get("totalDurationMs") or 0),
        str(sync.get("reportPath") or ""),
    )
    sync_detail_rows = (
        sync_detail.get("stageTop") if isinstance(sync_detail.get("stageTop"), list) else []
    )
    for row in sync_detail_rows:
        if isinstance(row, dict):
            add(
                "sync-detail",
                "sync.push",
                str(row.get("stage") or ""),
                int(row.get("durationMs") or 0),
                str(sync_detail.get("reportPath") or ""),
            )
    sync_remote_operations = (
        sync_detail.get("remoteOperationTop")
        if isinstance(sync_detail.get("remoteOperationTop"), list)
        else []
    )
    for row in sync_remote_operations:
        if isinstance(row, dict):
            add(
                "sync-remote-operation",
                "sync.push",
                str(row.get("operation") or ""),
                int(row.get("durationMs") or 0),
                str(sync_detail.get("reportPath") or ""),
            )
    sync_remote_stage_wall = (
        sync_detail.get("remoteStageWallTop")
        if isinstance(sync_detail.get("remoteStageWallTop"), list)
        else []
    )
    for row in sync_remote_stage_wall:
        if isinstance(row, dict):
            add(
                "sync-remote-wall",
                "sync.push",
                str(row.get("stage") or ""),
                int(row.get("wallMs") or 0),
                str(sync_detail.get("reportPath") or ""),
            )
    sync_remote_requests = (
        sync_detail.get("remoteSlowestRequests")
        if isinstance(sync_detail.get("remoteSlowestRequests"), list)
        else []
    )
    for row in sync_remote_requests:
        if isinstance(row, dict):
            add(
                "sync-remote-request",
                "sync.push",
                f"{row.get('method') or ''} {row.get('operation') or ''}",
                int(row.get("durationMs") or 0),
                str(row.get("path") or sync_detail.get("reportPath") or ""),
            )
    lcp_rows = (
        chrome_traces.get("latestLcp") if isinstance(chrome_traces.get("latestLcp"), list) else []
    )
    for row in lcp_rows:
        if isinstance(row, dict):
            add(
                "chrome-lcp",
                "chrome-trace",
                str(row.get("nodeName") or "latest LCP"),
                int(float(row.get("startMs") or 0)),
                str(row.get("tracePath") or ""),
            )
    resource_rows = (
        chrome_traces.get("topSlowResources")
        if isinstance(chrome_traces.get("topSlowResources"), list)
        else []
    )
    for row in resource_rows:
        if isinstance(row, dict):
            add(
                "chrome-resource",
                "chrome-trace",
                str(row.get("url") or row.get("requestId") or "resource"),
                int(float(row.get("durationMs") or 0)),
                str(row.get("tracePath") or ""),
            )
    targets.sort(
        key=lambda row: int(row.get("rankValue") or row.get("durationMs") or 0), reverse=True
    )
    return targets[:20]
