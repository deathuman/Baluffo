#!/usr/bin/env python3
"""Payload benchmark, fetch timing and startup summary builders.

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

from scripts.perf_complete_process import (
    _bounded_dict,
    _comparison,
    _read_json,
    summarize_artifacts,
)
from scripts.perf_complete_spec import SUSPECT_ROUTE_LABELS

__all__ = [
    "Any",
    "SUSPECT_ROUTE_LABELS",
    "_annotated_timing_rows",
    "_benchmark_steps",
    "_bounded_dict",
    "_comparison",
    "_error_timing_rows",
    "_frontend_page_duration_ms",
    "_profile_sample",
    "_read_json",
    "_startup_stage_durations",
    "_startup_summary",
    "_summarize_memory_runs",
    "_suspect_route_rows",
    "_timing_rows",
    "build_fetch_source_timing_summary",
    "summarize_artifacts",
]


def _summarize_memory_runs(rows: list[dict[str, Any]]) -> dict[str, Any]:
    metrics = [dict(row.get("memoryMetrics") or {}) for row in rows]
    unsupported = [
        str(row.get("unsupportedReason") or "")
        for row in metrics
        if str(row.get("unsupportedReason") or "")
    ]
    peak_metric = max(
        metrics,
        key=lambda row: max(
            int(row.get("peakWorkingSetBytes") or 0),
            int(row.get("peakRssBytes") or 0),
        ),
        default={},
    )
    category_peaks: dict[str, int] = {}
    top_by_identity: dict[tuple[int, str, str, str], dict[str, Any]] = {}
    for row in metrics:
        category_totals = (
            row.get("categoryPeaks") if isinstance(row.get("categoryPeaks"), dict) else {}
        )
        for category, total in category_totals.items():
            key = str(category or "other")
            category_peaks[key] = max(int(category_peaks.get(key) or 0), int(total or 0))
        candidates = row.get("topProcesses") if isinstance(row.get("topProcesses"), list) else []
        if not candidates:
            peak_sample = row.get("peakSample") if isinstance(row.get("peakSample"), dict) else {}
            candidates = (
                peak_sample.get("processes")
                if isinstance(peak_sample.get("processes"), list)
                else []
            )
        for process in candidates:
            if not isinstance(process, dict):
                continue
            peak_bytes = max(
                int(process.get("peakBytes") or process.get("memoryBytes") or 0),
                int(process.get("peakWorkingSetBytes") or process.get("workingSetBytes") or 0),
                int(process.get("peakRssBytes") or process.get("rssBytes") or 0),
            )
            identity = (
                int(process.get("pid") or 0),
                str(process.get("name") or ""),
                str(process.get("imagePath") or ""),
                str(process.get("commandLine") or ""),
            )
            existing = top_by_identity.get(identity)
            if existing is None or peak_bytes > int(existing.get("peakBytes") or 0):
                top_by_identity[identity] = {
                    "pid": int(process.get("pid") or 0),
                    "parentPid": int(process.get("parentPid") or 0),
                    "name": str(process.get("name") or ""),
                    "imagePath": str(process.get("imagePath") or ""),
                    "commandLine": str(process.get("commandLine") or ""),
                    "category": str(process.get("category") or "other"),
                    "peakWorkingSetBytes": int(
                        process.get("peakWorkingSetBytes") or process.get("workingSetBytes") or 0
                    ),
                    "peakRssBytes": int(
                        process.get("peakRssBytes") or process.get("rssBytes") or 0
                    ),
                    "peakBytes": peak_bytes,
                    "sampleCount": int(process.get("sampleCount") or 0),
                }
            elif existing is not None:
                existing["sampleCount"] = int(existing.get("sampleCount") or 0) + int(
                    process.get("sampleCount") or 0
                )
    top_processes = sorted(
        top_by_identity.values(),
        key=lambda row: int(row.get("peakBytes") or 0),
        reverse=True,
    )[:10]
    return {
        "sampleCount": sum(int(row.get("sampleCount") or 0) for row in metrics),
        "peakWorkingSetBytes": max(
            [int(row.get("peakWorkingSetBytes") or 0) for row in metrics] or [0]
        ),
        "peakRssBytes": max([int(row.get("peakRssBytes") or 0) for row in metrics] or [0]),
        "maxProcessCount": max([int(row.get("maxProcessCount") or 0) for row in metrics] or [0]),
        "skippedProcessCount": sum(int(row.get("skippedProcessCount") or 0) for row in metrics),
        "unsupportedReason": ""
        if any(int(row.get("sampleCount") or 0) for row in metrics)
        else (unsupported[0] if unsupported else ""),
        "peakSample": dict(peak_metric.get("peakSample") or {}),
        "topProcesses": top_processes,
        "categoryPeaks": category_peaks,
    }


def _benchmark_steps(output_dir: Path) -> dict[str, list[tuple[str, list[str], Path, Path]]]:
    return {
        "discovery": [
            (
                f"run-{index}",
                [
                    sys.executable,
                    "src/discovery_sanity_benchmark.py",
                    "--preset",
                    "quick",
                    "--timeout",
                    "10",
                    "--top",
                    "5",
                    "--output-dir",
                    str(output_dir / "discovery" / f"run-{index}" / "data"),
                ],
                output_dir / "discovery" / f"run-{index}" / "payload.txt",
                output_dir / "discovery" / f"run-{index}" / "stderr.log",
            )
            for index in range(1, 4)
        ],
        "fetch": [
            (
                f"run-{index}",
                [
                    sys.executable,
                    "src/fetch_incremental_sanity_benchmark.py",
                    "--group",
                    "smoke",
                    "--timeout",
                    "30",
                    "--output-dir",
                    str(output_dir / "fetch" / f"run-{index}" / "data"),
                ],
                output_dir / "fetch" / f"run-{index}" / "payload.txt",
                output_dir / "fetch" / f"run-{index}" / "stderr.log",
            )
            for index in range(1, 4)
        ],
    }


def build_fetch_source_timing_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    source_rows: list[dict[str, Any]] = []
    board_rows: list[dict[str, Any]] = []
    targets: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    source_keys = (
        "name",
        "adapter",
        "status",
        "cacheDecision",
        "durationMs",
        "fetchMs",
        "parseMs",
        "keptCount",
        "detailPagesVisited",
        "detailYieldPct",
        "error",
    )
    board_keys = (
        "source",
        "adapter",
        "name",
        "studio",
        "slug",
        "status",
        "cacheDecision",
        "durationMs",
        "fetchMs",
        "parseMs",
        "keptCount",
        "providerUrl",
        "providerHost",
        "error",
    )
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        run_label = str(row.get("label") or "")
        signals = (
            payload.get("sourceTimingSignals")
            if isinstance(payload.get("sourceTimingSignals"), dict)
            else {}
        )
        for field, phase in (
            ("firstRunSlowestSources", "first"),
            ("secondRunSlowestSources", "second"),
        ):
            items = signals.get(field) if isinstance(signals.get(field), list) else []
            for item in items:
                if isinstance(item, dict):
                    source_rows.append(
                        {
                            "run": run_label,
                            "phase": phase,
                            **_bounded_dict(item, source_keys),
                        }
                    )
        for field, phase in (
            ("firstRunSlowestProviderBoards", "first"),
            ("secondRunSlowestProviderBoards", "second"),
        ):
            items = signals.get(field) if isinstance(signals.get(field), list) else []
            for item in items:
                if isinstance(item, dict):
                    board_rows.append(
                        {
                            "run": run_label,
                            "phase": phase,
                            **_bounded_dict(item, board_keys),
                        }
                    )
        target_items = (
            payload.get("nextOptimizationTargets")
            if isinstance(payload.get("nextOptimizationTargets"), list)
            else []
        )
        for item in target_items:
            if isinstance(item, dict):
                targets.append(
                    {
                        "run": run_label,
                        **_bounded_dict(
                            item,
                            (
                                "name",
                                "action",
                                "priority",
                                "durationMs",
                                "keptCount",
                                "outputContractRisk",
                                "requiresExplicitDecision",
                                "reasons",
                            ),
                        ),
                    }
                )
        decision_items = (
            payload.get("sourceDecisionMatrix")
            if isinstance(payload.get("sourceDecisionMatrix"), list)
            else []
        )
        for item in decision_items:
            if isinstance(item, dict):
                decisions.append(
                    {
                        "run": run_label,
                        **_bounded_dict(
                            item,
                            (
                                "name",
                                "action",
                                "priority",
                                "keptCount",
                                "durationMs",
                                "decisionType",
                                "recommendedFirstPass",
                                "behaviorChangeAllowed",
                                "requiresExplicitDecision",
                                "nextDecision",
                            ),
                        ),
                    }
                )
    source_rows.sort(key=lambda item: int(item.get("durationMs") or 0), reverse=True)
    board_rows.sort(key=lambda item: int(item.get("durationMs") or 0), reverse=True)
    targets.sort(
        key=lambda item: (int(item.get("priority") or 0), int(item.get("durationMs") or 0)),
        reverse=True,
    )
    decisions.sort(
        key=lambda item: (int(item.get("priority") or 0), int(item.get("durationMs") or 0)),
        reverse=True,
    )

    def _increment_bucket(target: dict[str, int], key: Any) -> None:
        token = str(key or "").strip() or "unknown"
        target[token] = int(target.get(token) or 0) + 1

    def _group_rows(
        rows_to_group: list[dict[str, Any]], key_fields: tuple[str, ...]
    ) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, ...], dict[str, Any]] = {}
        for item in rows_to_group:
            key = tuple(str(item.get(field) or "").strip() for field in key_fields)
            row = grouped.setdefault(
                key,
                {
                    **{field: key[index] for index, field in enumerate(key_fields)},
                    "sampleCount": 0,
                    "totalDurationMs": 0,
                    "maxDurationMs": 0,
                    "totalFetchMs": 0,
                    "totalParseMs": 0,
                    "statuses": {},
                    "cacheDecisions": {},
                },
            )
            duration_ms = int(item.get("durationMs") or 0)
            fetch_ms = int(item.get("fetchMs") or 0)
            parse_ms = int(item.get("parseMs") or 0)
            row["sampleCount"] = int(row.get("sampleCount") or 0) + 1
            row["totalDurationMs"] = int(row.get("totalDurationMs") or 0) + duration_ms
            row["maxDurationMs"] = max(int(row.get("maxDurationMs") or 0), duration_ms)
            row["totalFetchMs"] = int(row.get("totalFetchMs") or 0) + fetch_ms
            row["totalParseMs"] = int(row.get("totalParseMs") or 0) + parse_ms
            _increment_bucket(row["statuses"], item.get("status"))
            _increment_bucket(row["cacheDecisions"], item.get("cacheDecision"))
        rows = list(grouped.values())
        rows.sort(
            key=lambda row: (
                int(row.get("totalDurationMs") or 0),
                int(row.get("maxDurationMs") or 0),
                int(row.get("sampleCount") or 0),
            ),
            reverse=True,
        )
        return rows

    status_breakdown: dict[str, int] = {}
    cache_decision_breakdown: dict[str, int] = {}
    for item in [*source_rows, *board_rows]:
        _increment_bucket(status_breakdown, item.get("status"))
        _increment_bucket(cache_decision_breakdown, item.get("cacheDecision"))
    return {
        "sampleCount": len(rows),
        "topSourcesByDuration": source_rows[:15],
        "topProviderBoardsByDuration": board_rows[:15],
        "providerSourceBreakdown": _group_rows(board_rows, ("source", "adapter"))[:15],
        "providerHostBreakdown": _group_rows(board_rows, ("source", "adapter", "providerHost"))[
            :15
        ],
        "adapterBreakdown": _group_rows([*source_rows, *board_rows], ("adapter",))[:15],
        "statusBreakdown": status_breakdown,
        "cacheDecisionBreakdown": cache_decision_breakdown,
        "nextOptimizationTargets": targets[:15],
        "sourceDecisionMatrix": decisions[:15],
    }


def _frontend_page_duration_ms(path: Path) -> int:
    payload = _read_json(path)
    performance = payload.get("performance") if isinstance(payload.get("performance"), dict) else {}
    navigation = performance.get("navigation") if isinstance(performance, dict) else []
    if not isinstance(navigation, list) or not navigation:
        return 0
    first = navigation[0] if isinstance(navigation[0], dict) else {}
    return int(float(first.get("duration") or 0))


def _startup_stage_durations(report: dict[str, Any]) -> dict[str, int]:
    profile = report.get("startupProfile") if isinstance(report.get("startupProfile"), dict) else {}
    stages = profile.get("stages") if isinstance(profile.get("stages"), list) else []
    return {
        str(row.get("key") or ""): int(row.get("durationMs") or 0)
        for row in stages
        if isinstance(row, dict) and str(row.get("key") or "")
    }


def _timing_rows(profile: dict[str, Any], section: str) -> list[dict[str, Any]]:
    container = profile.get(section) if isinstance(profile.get(section), dict) else {}
    key = "routes" if section == "routeTimings" else "operations"
    rows = container.get(key) if isinstance(container.get(key), list) else []
    return [dict(row) for row in rows if isinstance(row, dict)]


def _profile_sample(
    *,
    source: str,
    profile_path: Path | None,
    page: str = "",
    mode: str = "",
) -> dict[str, Any]:
    profile = _read_json(profile_path) if profile_path is not None else {}
    routes = _timing_rows(profile, "routeTimings")
    operations = _timing_rows(profile, "operationTimings")
    return {
        "source": source,
        "page": page,
        "mode": mode,
        "profilePath": str(profile_path or ""),
        "ok": bool(profile.get("ok")),
        "generatedAt": str(profile.get("generatedAt") or ""),
        "routeCount": len(routes),
        "operationCount": len(operations),
        "error": str(profile.get("error") or ""),
        "profile": profile,
    }


def _annotated_timing_rows(
    samples: list[dict[str, Any]],
    *,
    section: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sample in samples:
        profile = sample.get("profile") if isinstance(sample.get("profile"), dict) else {}
        for row in _timing_rows(profile, section):
            rows.append(
                {
                    "source": str(sample.get("source") or ""),
                    "page": str(sample.get("page") or ""),
                    "mode": str(sample.get("mode") or ""),
                    "profilePath": str(sample.get("profilePath") or ""),
                    "label": str(row.get("label") or ""),
                    "count": int(row.get("count") or 0),
                    "p95Ms": int(row.get("p95Ms") or 0),
                    "p50Ms": int(row.get("p50Ms") or 0),
                    "avgMs": int(row.get("avgMs") or 0),
                    "maxMs": int(row.get("maxMs") or 0),
                    "lastMs": int(row.get("lastMs") or 0),
                    "lastStatus": int(row.get("lastStatus") or 0),
                    "errorCount": int(row.get("errorCount") or 0),
                }
            )
    rows.sort(
        key=lambda row: (
            int(row.get("p95Ms") or 0),
            int(row.get("avgMs") or 0),
            int(row.get("count") or 0),
        ),
        reverse=True,
    )
    return rows


def _error_timing_rows(rows: list[dict[str, Any]], *, limit: int = 20) -> list[dict[str, Any]]:
    filtered = [
        dict(row)
        for row in rows
        if int(row.get("errorCount") or 0) > 0 or int(row.get("lastStatus") or 0) >= 400
    ]
    return filtered[: max(0, int(limit or 0))]


def _suspect_route_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    suspects = []
    for label in SUSPECT_ROUTE_LABELS:
        matches = [row for row in rows if str(row.get("label") or "") == label]
        if not matches:
            suspects.append({"label": label, "samples": [], "maxP95Ms": 0})
            continue
        suspects.append(
            {
                "label": label,
                "maxP95Ms": max(int(row.get("p95Ms") or 0) for row in matches),
                "samples": matches,
            }
        )
    suspects.sort(key=lambda row: int(row.get("maxP95Ms") or 0), reverse=True)
    return suspects


def _startup_summary(
    *,
    mode: str,
    page: str,
    report_path: Path,
    artifacts_dir: Path,
    command_result: dict[str, Any],
    baseline_dir: Path,
) -> dict[str, Any]:
    page_key = str(page or "jobs").strip().lower() or "jobs"
    report = _read_json(report_path)
    profile = report.get("startupProfile") if isinstance(report.get("startupProfile"), dict) else {}
    duration_ms = int(profile.get("firstUsableMs") or 0)
    key_paths = [report_path]
    artifacts = report.get("artifacts") if isinstance(report.get("artifacts"), dict) else {}
    for value in artifacts.values():
        token = str(value or "").strip()
        if token:
            key_paths.append(Path(token))
    comparison_mode = f"startup-{mode}" if page_key == "jobs" else f"startup-{page_key}-{mode}"
    comparison = _comparison(
        mode=comparison_mode, duration_ms=duration_ms, baseline_dir=baseline_dir
    )
    return {
        "mode": f"startup-{page_key}-{mode}",
        "page": page_key,
        "durationMs": duration_ms,
        "status": str(comparison.get("status") or ""),
        "startupProfileStatus": str(
            profile.get("status") or ("passed" if report.get("ok") else "failed")
        ),
        "classification": str(profile.get("classification") or ""),
        "firstUsableEvent": str(profile.get("firstUsableEvent") or ""),
        "stageDurationsMs": _startup_stage_durations(report),
        "reportPath": str(report_path),
        "artifactsDir": str(artifacts_dir),
        "report": report,
        "command": command_result,
        "memoryMetrics": dict(report.get("memoryMetrics") or {}),
        "artifactSizes": summarize_artifacts(roots=[artifacts_dir], key_paths=key_paths),
        "comparison": comparison,
    }
