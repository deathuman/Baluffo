#!/usr/bin/env python3
"""Run the broad local benchmark suite and write a consolidated report."""

from __future__ import annotations

import argparse
import contextlib
import http.client
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.perf_baseline import (
    append_trend_record,
    build_baseline_record,
    write_baseline_record,
)
from scripts.perf_ci import _summarize_runs
from scripts.perf_compare import (
    benchmark_duration_ms,
    compare_duration,
    load_benchmark_payload,
)
from scripts.run_startup_probe_pair import (
    cold_startup_probe_args,
    packaged_probe_command,
    resolve_built_exe,
    startup_pair_paths,
    warm_startup_probe_args,
)
from src.shared.process_memory import ProcessMemorySampler

DEFAULT_OUTPUT_ROOT = REPO_ROOT / "_out" / "perf-complete"
DEFAULT_BASELINE_DIR = REPO_ROOT / "_out" / "perf-baseline"
DEFAULT_TREND_PATH = REPO_ROOT / "_out" / "perf-trend.ndjson"
DEFAULT_RUNTIME_TIMEOUT_S = 60.0
SUSPECT_ROUTE_LABELS = (
    "GET /ops/dashboard-health",
    "GET /ops/health",
    "GET /ops/task-state",
)
LIVE_BRIDGE_ENDPOINTS = (
    "/ops/performance-profile",
    "/ops/health",
    "/ops/task-state?view=summary",
    "/ops/dashboard-health",
    "/sync/status",
    "/registry/summary",
    "/jobs.html",
    "/admin.html",
)


def parse_timeout_sequence(
    value: str | float | int | None, *, fallback: float = 3.0
) -> list[float]:
    text = str(value if value is not None else "").strip()
    if not text:
        return [float(fallback)]
    values: list[float] = []
    for part in text.split(","):
        token = part.strip()
        if not token:
            continue
        try:
            timeout_s = float(token)
        except ValueError:
            continue
        if timeout_s > 0:
            values.append(timeout_s)
    return values or [float(fallback)]


def generate_run_token(*, now: datetime | None = None) -> str:
    resolved_now = now if isinstance(now, datetime) else datetime.now(UTC)
    return resolved_now.strftime("%Y%m%d-%H%M%S-%f")


def _npm_command() -> str:
    return "npm.cmd" if os.name == "nt" else "npm"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _median(values: list[int]) -> int:
    numbers = sorted(int(value) for value in values if int(value) >= 0)
    if not numbers:
        return 0
    return numbers[len(numbers) // 2]


def _duration_ms(started_at: float, finished_at: float) -> int:
    return max(0, int(round((finished_at - started_at) * 1000)))


def _file_size(path: Path) -> int:
    try:
        return int(path.stat().st_size)
    except OSError:
        return 0


def summarize_artifacts(
    *,
    roots: list[Path] | None = None,
    key_paths: list[Path] | None = None,
    largest_limit: int = 10,
) -> dict[str, Any]:
    files: dict[Path, int] = {}
    for root in roots or []:
        resolved_root = Path(root).expanduser().resolve()
        if resolved_root.is_file():
            files[resolved_root] = _file_size(resolved_root)
        elif resolved_root.is_dir():
            for path in resolved_root.rglob("*"):
                if path.is_file():
                    resolved = path.resolve()
                    files[resolved] = _file_size(resolved)
    key_artifacts = []
    for path in key_paths or []:
        resolved = Path(path).expanduser().resolve()
        size = _file_size(resolved) if resolved.is_file() else 0
        key_artifacts.append(
            {
                "path": str(resolved),
                "exists": resolved.exists(),
                "sizeBytes": size,
            }
        )
        if resolved.is_file():
            files[resolved] = size
    largest = sorted(files.items(), key=lambda item: item[1], reverse=True)[:largest_limit]
    return {
        "totalBytes": sum(files.values()),
        "fileCount": len(files),
        "keyArtifacts": key_artifacts,
        "largestFiles": [{"path": str(path), "sizeBytes": size} for path, size in largest],
    }


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


def run_monitored_command(
    command: list[str],
    *,
    stdout_path: Path,
    stderr_path: Path,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"$ {' '.join(command)}", flush=True)
    started = datetime.now(UTC).isoformat()
    with (
        stdout_path.open("w", encoding="utf-8") as stdout_handle,
        stderr_path.open("w", encoding="utf-8") as stderr_handle,
    ):
        process = subprocess.Popen(
            command,
            cwd=REPO_ROOT,
            stdout=stdout_handle,
            stderr=stderr_handle,
            env=env,
        )
        sampler = ProcessMemorySampler(int(process.pid or 0))
        sampler.start()
        exit_code = int(process.wait())
        memory_metrics = sampler.stop()
    return {
        "command": command,
        "exitCode": exit_code,
        "startedAt": started,
        "finishedAt": datetime.now(UTC).isoformat(),
        "stdoutPath": str(stdout_path),
        "stderrPath": str(stderr_path),
        "memoryMetrics": memory_metrics,
    }


def _comparison(
    *,
    mode: str,
    duration_ms: int,
    baseline_dir: Path,
) -> dict[str, Any]:
    baseline = load_benchmark_payload(baseline_dir / f"{mode}-baseline.json")
    result = compare_duration(
        current_duration_ms=int(duration_ms or 0),
        baseline_duration_ms=benchmark_duration_ms(baseline, mode=mode),
    )
    result["mode"] = mode
    return result


def _record_row(
    *,
    mode: str,
    duration_ms: int,
    status: str,
    stage_durations_ms: dict[str, Any] | None,
    artifact: str,
    baseline_dir: Path,
    trend_path: Path,
    record_baseline: bool,
    record_trend: bool,
) -> None:
    if not (record_baseline or record_trend):
        return
    record = build_baseline_record(
        mode=mode,
        total_duration_ms=int(duration_ms or 0),
        status="pass" if record_baseline else status,
        stage_durations_ms=stage_durations_ms or {},
        artifact=artifact,
    )
    if record_baseline:
        path = write_baseline_record(record, baseline_dir=baseline_dir, trend_path=trend_path)
        print(f"Recorded {mode} complete baseline: {path}", flush=True)
    elif record_trend:
        path = append_trend_record(record, trend_path=trend_path)
        print(f"Recorded {mode} complete trend row: {path}", flush=True)


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


def run_repeated_payload_benchmark(
    *,
    mode: str,
    steps: list[tuple[str, list[str], Path, Path]],
    output_dir: Path,
    baseline_dir: Path,
) -> tuple[dict[str, Any], int]:
    rows: list[dict[str, Any]] = []
    payloads: list[dict[str, Any]] = []
    exit_code = 0
    for label, command, stdout_path, stderr_path in steps:
        command_result = run_monitored_command(
            command,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
        payload = load_benchmark_payload(stdout_path)
        payloads.append(payload)
        rows.append(
            {
                "label": label,
                "payloadPath": str(stdout_path),
                "payload": payload,
                **command_result,
            }
        )
        if int(command_result.get("exitCode") or 0) != 0:
            exit_code = int(command_result.get("exitCode") or 1)
    summary = _summarize_runs(mode, payloads)
    summary["runsDetail"] = rows
    summary["memoryMetrics"] = _summarize_memory_runs(rows)
    summary["artifactSizes"] = summarize_artifacts(
        roots=[output_dir / mode],
        key_paths=[Path(str(row["payloadPath"])) for row in rows],
    )
    summary["comparison"] = _comparison(
        mode=mode,
        duration_ms=int(summary.get("medianDurationMs") or 0),
        baseline_dir=baseline_dir,
    )
    if mode == "fetch":
        summary["sourceTiming"] = build_fetch_source_timing_summary(rows)
    return summary, exit_code


def _bounded_dict(row: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {key: row.get(key) for key in keys if key in row}


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


def run_frontend_boot(
    *,
    output_dir: Path,
    baseline_dir: Path,
) -> tuple[dict[str, Any], int]:
    trace_dir = output_dir / "frontend-boot" / "traces"
    env = os.environ.copy()
    env["BALUFFO_PERF_TRACE_DIR"] = str(trace_dir)
    result = run_monitored_command(
        [_npm_command(), "run", "test:frontend:perf"],
        stdout_path=output_dir / "frontend-boot" / "stdout.log",
        stderr_path=output_dir / "frontend-boot" / "stderr.log",
        env=env,
    )
    summary_paths = sorted(trace_dir.glob("*-boot-summary.json"))
    pages = []
    for path in summary_paths:
        payload = _read_json(path)
        duration_ms = _frontend_page_duration_ms(path)
        pages.append(
            {
                "page": str(payload.get("page") or path.name.removesuffix("-boot-summary.json")),
                "durationMs": duration_ms,
                "summaryPath": str(path),
            }
        )
    duration_ms = max([int(row.get("durationMs") or 0) for row in pages] or [0])
    summary = {
        "mode": "frontend-boot",
        "durationMs": duration_ms,
        "pages": pages,
        "command": result,
        "memoryMetrics": dict(result.get("memoryMetrics") or {}),
        "artifactSizes": summarize_artifacts(roots=[trace_dir], key_paths=summary_paths),
        "comparison": _comparison(
            mode="frontend-boot",
            duration_ms=duration_ms,
            baseline_dir=baseline_dir,
        ),
    }
    return summary, int(result.get("exitCode") or 0)


def _startup_stage_durations(report: dict[str, Any]) -> dict[str, int]:
    profile = report.get("startupProfile") if isinstance(report.get("startupProfile"), dict) else {}
    stages = profile.get("stages") if isinstance(profile.get("stages"), list) else []
    return {
        str(row.get("key") or ""): int(row.get("durationMs") or 0)
        for row in stages
        if isinstance(row, dict) and str(row.get("key") or "")
    }


def _artifact_path_from_summary(section: dict[str, Any], key: str) -> Path | None:
    report = section.get("report") if isinstance(section.get("report"), dict) else {}
    artifacts = report.get("artifacts") if isinstance(report.get("artifacts"), dict) else {}
    token = str(artifacts.get(key) or "").strip()
    if not token and isinstance(section.get("scenario"), dict):
        details = (
            section["scenario"].get("details")
            if isinstance(section["scenario"].get("details"), dict)
            else {}
        )
        token = str(details.get(key) or "").strip()
    if not token:
        token = str(section.get(key) or "").strip()
    if not token:
        return None
    return Path(token).expanduser().resolve()


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


def _fetch_live_bridge_request(
    *,
    base_url: str,
    endpoint: str,
    timeout_s: float,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    endpoint_path = str(endpoint or "").strip()
    url = f"{base_url}{endpoint_path}"
    started_at = time.perf_counter()
    timeout_value = max(1.0, float(timeout_s or 0))
    parsed_url = urllib.parse.urlsplit(url)
    request_path = parsed_url.path or "/"
    if parsed_url.query:
        request_path = f"{request_path}?{parsed_url.query}"
    host = str(parsed_url.hostname or "")
    scheme = str(parsed_url.scheme or "http").lower()
    if scheme not in {"http", "https"} or not host:
        return (
            {
                "ok": False,
                "endpoint": endpoint_path,
                "status": 0,
                "durationMs": _duration_ms(started_at, time.perf_counter()),
                "timeoutS": timeout_value,
                "tcpConnectMs": 0,
                "firstByteMs": 0,
                "contentType": "",
                "sizeBytes": 0,
                "topLevelKeys": [],
                "phase": "url_parse",
                "error": f"unsupported live bridge URL: {url}",
            },
            None,
        )
    port = int(parsed_url.port or (443 if scheme == "https" else 80))
    connection_cls = (
        http.client.HTTPSConnection if scheme == "https" else http.client.HTTPConnection
    )
    connection = connection_cls(host, port=port, timeout=timeout_value)
    tcp_connect_ms = 0
    first_byte_ms = 0
    try:
        connect_started_at = time.perf_counter()
        connection.connect()
        tcp_connect_ms = _duration_ms(connect_started_at, time.perf_counter())
        request_started_at = time.perf_counter()
        connection.putrequest("GET", request_path)
        connection.putheader("Host", parsed_url.netloc)
        connection.putheader("Accept", "application/json,text/html;q=0.9,*/*;q=0.1")
        connection.putheader("User-Agent", "BaluffoPerfSampler/1")
        connection.putheader("Connection", "close")
        connection.endheaders()
        response = connection.getresponse()
        first_byte_ms = _duration_ms(request_started_at, time.perf_counter())
        raw_payload = response.read()
        status = int(response.status or 0)
        content_type = str(response.headers.get("content-type") or "")
        duration_ms = _duration_ms(started_at, time.perf_counter())
        parsed_payload: dict[str, Any] | None = None
        top_level_keys: list[str] = []
        if endpoint_path != "/jobs.html" and endpoint_path != "/admin.html":
            try:
                decoded = raw_payload.decode("utf-8")
                parsed = json.loads(decoded)
                if isinstance(parsed, dict):
                    parsed_payload = parsed
                    top_level_keys = sorted(str(key) for key in parsed.keys())[:20]
            except (UnicodeDecodeError, json.JSONDecodeError):
                parsed_payload = None
        return (
            {
                "ok": 200 <= status < 400,
                "endpoint": endpoint_path,
                "status": status,
                "durationMs": duration_ms,
                "timeoutS": timeout_value,
                "tcpConnectMs": tcp_connect_ms,
                "firstByteMs": first_byte_ms,
                "contentType": content_type,
                "sizeBytes": len(raw_payload),
                "topLevelKeys": top_level_keys,
                "phase": "complete",
            },
            parsed_payload,
        )
    except (OSError, TimeoutError, http.client.HTTPException, urllib.error.URLError) as exc:
        phase = (
            "tcp_connect"
            if tcp_connect_ms <= 0
            else "first_byte"
            if first_byte_ms <= 0
            else "response_body"
        )
        return (
            {
                "ok": False,
                "endpoint": endpoint_path,
                "status": 0,
                "durationMs": _duration_ms(started_at, time.perf_counter()),
                "timeoutS": timeout_value,
                "tcpConnectMs": tcp_connect_ms,
                "firstByteMs": first_byte_ms,
                "contentType": "",
                "sizeBytes": 0,
                "topLevelKeys": [],
                "phase": phase,
                "error": str(exc),
            },
            None,
        )
    finally:
        with contextlib.suppress(Exception):
            connection.close()


def capture_live_bridge_profile(
    *,
    bridge_base_url: str,
    output_dir: Path,
    timeout_s: float = 3.0,
    timeout_sequence: list[float] | None = None,
) -> dict[str, Any]:
    profile_dir = output_dir / "bridge-profile" / "live"
    profile_dir.mkdir(parents=True, exist_ok=True)
    profile_path = profile_dir / "performance-profile.json"
    report_path = profile_dir / "live-bridge-sample.json"
    base = str(bridge_base_url or "").strip().rstrip("/")
    if not base:
        payload = {"ok": False, "error": "bridge base URL was empty"}
        profile_path.write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
        report_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        return {"ok": False, "error": "bridge base URL was empty", "profilePath": str(profile_path)}
    requests = []
    performance_profile: dict[str, Any] | None = None
    timeouts = timeout_sequence if isinstance(timeout_sequence, list) else [float(timeout_s)]
    timeouts = [float(value) for value in timeouts if float(value or 0) > 0] or [float(timeout_s)]
    for active_timeout_s in timeouts:
        for endpoint in LIVE_BRIDGE_ENDPOINTS:
            row, parsed = _fetch_live_bridge_request(
                base_url=base,
                endpoint=endpoint,
                timeout_s=active_timeout_s,
            )
            requests.append(row)
            if endpoint == "/ops/performance-profile" and performance_profile is None:
                performance_profile = parsed if isinstance(parsed, dict) else None
    if performance_profile is None:
        performance_profile = {"ok": False, "error": "performance profile unavailable"}
    profile_path.write_text(
        json.dumps(performance_profile, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    report = {
        "ok": any(bool(row.get("ok")) for row in requests),
        "baseUrl": base,
        "generatedAt": datetime.now(UTC).isoformat(),
        "profilePath": str(profile_path),
        "timeoutsS": timeouts,
        "requests": requests,
        "slowestRequests": sorted(
            requests,
            key=lambda row: int(row.get("durationMs") or 0),
            reverse=True,
        )[:8],
    }
    report_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return {**report, "reportPath": str(report_path)}


def build_bridge_profile_summary(
    *,
    startup: dict[str, Any],
    sync: dict[str, Any],
    output_dir: Path,
    bridge_base_url: str = "",
    live_bridge_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    samples: list[dict[str, Any]] = []
    for page in ("jobs", "admin"):
        page_summary = startup.get(page) if isinstance(startup.get(page), dict) else {}
        for mode in ("cold", "warm"):
            section = page_summary.get(mode) if isinstance(page_summary.get(mode), dict) else {}
            samples.append(
                _profile_sample(
                    source=f"startup.{page}.{mode}",
                    page=page,
                    mode=mode,
                    profile_path=_artifact_path_from_summary(section, "performanceProfileSnapshot"),
                )
            )
    samples.append(
        _profile_sample(
            source="sync",
            profile_path=_artifact_path_from_summary(sync, "performanceProfileSnapshot"),
        )
    )
    external_base = str(bridge_base_url or "").strip()
    if external_base:
        live_profile = (
            live_bridge_profile if isinstance(live_bridge_profile, dict) else None
        ) or capture_live_bridge_profile(
            bridge_base_url=external_base,
            output_dir=output_dir,
        )
        samples.append(
            _profile_sample(
                source="live.bridge",
                profile_path=Path(str(live_profile.get("profilePath") or "")),
            )
        )
    route_rows = _annotated_timing_rows(samples, section="routeTimings")
    operation_rows = _annotated_timing_rows(samples, section="operationTimings")
    public_samples = [
        {key: value for key, value in sample.items() if key not in {"profile"}}
        for sample in samples
    ]
    return {
        "samples": public_samples,
        "topRoutesByP95": route_rows[:12],
        "topOperationsByP95": operation_rows[:12],
        "errorRoutes": _error_timing_rows(route_rows),
        "errorOperations": _error_timing_rows(operation_rows),
        "suspectRoutes": _suspect_route_rows(route_rows),
    }


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
    targets.sort(
        key=lambda row: int(row.get("rankValue") or row.get("durationMs") or 0), reverse=True
    )
    return targets[:20]


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


def run_startup_pair(
    *,
    output_dir: Path,
    runtime_timeout_s: float,
    baseline_dir: Path,
    page: str = "jobs",
    open_path: str = "jobs.html",
    exe_path: Path | None = None,
    profile_record_only: bool = False,
) -> tuple[dict[str, Any], int, Path | None]:
    page_key = str(page or "jobs").strip().lower() or "jobs"
    paths = startup_pair_paths(artifact_root=output_dir / "startup" / page_key, open_path=open_path)
    paths["runRoot"].mkdir(parents=True, exist_ok=True)
    cold_command = packaged_probe_command(
        cold_startup_probe_args(
            paths,
            runtime_timeout_s=runtime_timeout_s,
            open_path=open_path,
            exe_path=exe_path,
            profile_record_only=profile_record_only,
        )
    )
    cold_result = run_monitored_command(
        cold_command,
        stdout_path=paths["runRoot"] / "cold-stdout.log",
        stderr_path=paths["runRoot"] / "cold-stderr.log",
    )
    reused_exe: Path | None = None
    startup_exit = int(cold_result.get("exitCode") or 0)
    try:
        reused_exe = resolve_built_exe(paths["coldReportPath"], paths["coldArtifactsDir"])
    except RuntimeError:
        reused_exe = None
    warm_result: dict[str, Any] = {
        "exitCode": 1,
        "memoryMetrics": {},
        "stdoutPath": "",
        "stderrPath": "",
    }
    if reused_exe is not None:
        warm_command = packaged_probe_command(
            warm_startup_probe_args(
                paths,
                reused_exe=reused_exe,
                runtime_timeout_s=runtime_timeout_s,
                open_path=open_path,
                profile_record_only=profile_record_only,
            )
        )
        warm_result = run_monitored_command(
            warm_command,
            stdout_path=paths["runRoot"] / "warm-stdout.log",
            stderr_path=paths["runRoot"] / "warm-stderr.log",
        )
        if int(warm_result.get("exitCode") or 0) != 0:
            startup_exit = int(warm_result.get("exitCode") or 1)
    cold_summary = _startup_summary(
        mode="cold",
        page=page_key,
        report_path=paths["coldReportPath"],
        artifacts_dir=paths["coldArtifactsDir"],
        command_result=cold_result,
        baseline_dir=baseline_dir,
    )
    warm_summary = _startup_summary(
        mode="warm",
        page=page_key,
        report_path=paths["warmReportPath"],
        artifacts_dir=paths["warmArtifactsDir"],
        command_result=warm_result,
        baseline_dir=baseline_dir,
    )
    if (
        int(cold_summary.get("durationMs") or 0) > 0
        and int(warm_summary.get("durationMs") or 0) > 0
    ):
        startup_exit = 0
    summary = {
        "page": page_key,
        "cold": cold_summary,
        "warm": warm_summary,
        "reusedExe": str(reused_exe or ""),
        "runRoot": str(paths["runRoot"]),
    }
    return summary, startup_exit, reused_exe


def _scenario_by_slug(report: dict[str, Any], slug: str) -> dict[str, Any]:
    scenarios = report.get("scenarios") if isinstance(report.get("scenarios"), list) else []
    for row in scenarios:
        if isinstance(row, dict) and str(row.get("slug") or "") == slug:
            return row
    return {}


def run_sync_rehearsal(
    *,
    output_dir: Path,
    runtime_timeout_s: float,
    baseline_dir: Path,
    exe_path: Path | None,
) -> tuple[dict[str, Any], int]:
    artifacts_dir = output_dir / "sync" / "artifacts"
    report_path = output_dir / "sync" / "report.json"
    command = [
        sys.executable,
        "src/packaged_desktop_smoke.py",
        "--sync-rehearsal",
        "--runtime-timeout",
        str(runtime_timeout_s),
        "--artifacts-dir",
        str(artifacts_dir),
        "--report-path",
        str(report_path),
    ]
    if exe_path is not None:
        command.extend(["--exe-path", str(exe_path)])
    command_result = run_monitored_command(
        command,
        stdout_path=output_dir / "sync" / "stdout.log",
        stderr_path=output_dir / "sync" / "stderr.log",
    )
    report = _read_json(report_path)
    scenario = _scenario_by_slug(report, "packaged-sync-rehearsal")
    details = scenario.get("details") if isinstance(scenario.get("details"), dict) else {}
    push_timing = details.get("pushTiming") if isinstance(details.get("pushTiming"), dict) else {}
    pull_timing = details.get("pullTiming") if isinstance(details.get("pullTiming"), dict) else {}
    push_duration = int(push_timing.get("totalDurationMs") or 0)
    pull_duration = int(pull_timing.get("totalDurationMs") or 0)
    key_paths = [report_path]
    for key in ("runtimeStdout", "runtimeStderr"):
        token = str(details.get(key) or "").strip()
        if token:
            key_paths.append(Path(token))
    profile_token = str(details.get("performanceProfileSnapshot") or "").strip()
    if profile_token:
        key_paths.append(Path(profile_token))
    storage_token = str(details.get("storageMetricsSnapshot") or "").strip()
    if storage_token:
        key_paths.append(Path(storage_token))
    summary = {
        "mode": "sync",
        "durationMs": int(scenario.get("durationMs") or 0),
        "status": str(scenario.get("status") or ("passed" if report.get("ok") else "failed")),
        "reportPath": str(report_path),
        "artifactsDir": str(artifacts_dir),
        "tokenRequests": int(details.get("tokenRequests") or 0),
        "contentRequests": int(details.get("contentRequests") or 0),
        "putRequests": int(details.get("putRequests") or 0),
        "deleteRequests": int(details.get("deleteRequests") or 0),
        "bytesWritten": int(details.get("bytesWritten") or 0),
        "pushTiming": push_timing,
        "pullTiming": pull_timing,
        "report": report,
        "scenario": scenario,
        "command": command_result,
        "memoryMetrics": dict(scenario.get("memoryMetrics") or {}),
        "artifactSizes": summarize_artifacts(roots=[artifacts_dir], key_paths=key_paths),
        "comparisons": {
            "push": _comparison(
                mode="sync-push",
                duration_ms=push_duration,
                baseline_dir=baseline_dir,
            ),
            "pull": _comparison(
                mode="sync-pull",
                duration_ms=pull_duration,
                baseline_dir=baseline_dir,
            ),
        },
    }
    if not summary["memoryMetrics"]:
        summary["memoryMetrics"] = dict(command_result.get("memoryMetrics") or {})
    return summary, int(command_result.get("exitCode") or 0)


def _process_peak_bytes(process: dict[str, Any]) -> int:
    return max(
        int(process.get("memoryBytes") or 0),
        int(process.get("peakBytes") or 0),
        int(process.get("workingSetBytes") or 0),
        int(process.get("rssBytes") or 0),
        int(process.get("peakWorkingSetBytes") or 0),
        int(process.get("peakRssBytes") or 0),
    )


def _format_mib(bytes_value: int) -> str:
    return f"{max(0, int(bytes_value or 0)) / (1024 * 1024):.1f}MiB"


def _format_top_contributors(memory: dict[str, Any], *, limit: int = 3) -> str:
    peak_sample = memory.get("peakSample") if isinstance(memory.get("peakSample"), dict) else {}
    processes = (
        peak_sample.get("processes") if isinstance(peak_sample.get("processes"), list) else []
    )
    if not processes:
        processes = (
            memory.get("topProcesses") if isinstance(memory.get("topProcesses"), list) else []
        )
    rows = [dict(row) for row in processes if isinstance(row, dict)]
    rows.sort(key=_process_peak_bytes, reverse=True)
    parts = []
    for row in rows[: max(0, int(limit or 0))]:
        name = str(row.get("name") or Path(str(row.get("imagePath") or "")).name or "process")
        category = str(row.get("category") or "other")
        parts.append(f"{name}[{category}]={_format_mib(_process_peak_bytes(row))}")
    return "|".join(parts)


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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the complete Baluffo benchmark report.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--baseline-dir", default=str(DEFAULT_BASELINE_DIR))
    parser.add_argument("--trend-path", default=str(DEFAULT_TREND_PATH))
    parser.add_argument("--runtime-timeout", type=float, default=DEFAULT_RUNTIME_TIMEOUT_S)
    parser.add_argument(
        "--bridge-base-url",
        default="",
        help=(
            "Optional running bridge base URL to sample with /ops/performance-profile. "
            "Failures are recorded as evidence and do not fail the benchmark."
        ),
    )
    parser.add_argument(
        "--bridge-timeouts",
        default="3",
        help="Comma-separated timeout seconds for optional live bridge sampling.",
    )
    parser.add_argument("--record-trend", action="store_true")
    parser.add_argument("--record-baseline", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_root = Path(str(args.output_root)).expanduser().resolve()
    baseline_dir = Path(str(args.baseline_dir)).expanduser().resolve()
    trend_path = Path(str(args.trend_path)).expanduser().resolve()
    run_dir = output_root / generate_run_token()
    run_dir.mkdir(parents=True, exist_ok=True)

    exit_code = 0
    steps = _benchmark_steps(run_dir)
    discovery, discovery_exit = run_repeated_payload_benchmark(
        mode="discovery",
        steps=steps["discovery"],
        output_dir=run_dir,
        baseline_dir=baseline_dir,
    )
    fetch, fetch_exit = run_repeated_payload_benchmark(
        mode="fetch",
        steps=steps["fetch"],
        output_dir=run_dir,
        baseline_dir=baseline_dir,
    )
    frontend, frontend_exit = run_frontend_boot(output_dir=run_dir, baseline_dir=baseline_dir)
    startup_jobs, startup_jobs_exit, reused_exe = run_startup_pair(
        output_dir=run_dir,
        runtime_timeout_s=float(args.runtime_timeout),
        baseline_dir=baseline_dir,
        page="jobs",
        open_path="jobs.html",
    )
    startup_admin, startup_admin_exit, admin_reused_exe = run_startup_pair(
        output_dir=run_dir,
        runtime_timeout_s=float(args.runtime_timeout),
        baseline_dir=baseline_dir,
        page="admin",
        open_path="admin.html",
        exe_path=reused_exe,
        profile_record_only=True,
    )
    reused_exe = reused_exe or admin_reused_exe
    sync, sync_exit = run_sync_rehearsal(
        output_dir=run_dir,
        runtime_timeout_s=float(args.runtime_timeout),
        baseline_dir=baseline_dir,
        exe_path=reused_exe,
    )
    for value in (
        discovery_exit,
        fetch_exit,
        frontend_exit,
        startup_jobs_exit,
        startup_admin_exit,
        sync_exit,
    ):
        if int(value or 0) != 0 and exit_code == 0:
            exit_code = int(value or 1)

    summary_path = run_dir / "summary.json"
    latest_summary_path = output_root / "summary.json"
    benchmarks = {
        "discovery": discovery,
        "fetch": fetch,
        "frontendBoot": frontend,
        "startup": {
            "jobs": startup_jobs,
            "admin": startup_admin,
        },
        "sync": sync,
    }
    live_bridge_profile = (
        capture_live_bridge_profile(
            bridge_base_url=str(args.bridge_base_url or ""),
            output_dir=run_dir,
            timeout_sequence=parse_timeout_sequence(str(args.bridge_timeouts or "3")),
        )
        if str(args.bridge_base_url or "").strip()
        else {}
    )
    benchmarks["syncDetail"] = build_sync_detail_summary(sync)
    if live_bridge_profile:
        benchmarks["liveBridgeProfile"] = live_bridge_profile
    benchmarks["bridgeProfile"] = build_bridge_profile_summary(
        startup=benchmarks["startup"],
        sync=sync,
        output_dir=run_dir,
        bridge_base_url=str(args.bridge_base_url or ""),
        live_bridge_profile=live_bridge_profile,
    )
    benchmarks["storageReadProfile"] = build_storage_read_profile_summary(
        startup=benchmarks["startup"],
        sync=sync,
    )
    benchmarks["memoryProfile"] = build_memory_profile_summary(benchmarks)
    summary = {
        "schemaVersion": 1,
        "generatedAt": datetime.now(UTC).isoformat(),
        "runDir": str(run_dir),
        "summaryPath": str(summary_path),
        "latestSummaryPath": str(latest_summary_path),
        "benchmarks": benchmarks,
        "optimizationTargets": build_optimization_targets(benchmarks),
        "overallArtifactSizes": summarize_artifacts(roots=[run_dir], key_paths=[summary_path]),
    }
    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    latest_summary_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(summary_path, latest_summary_path)
    _record_complete_rows(
        summary,
        baseline_dir=baseline_dir,
        trend_path=trend_path,
        record_baseline=bool(args.record_baseline),
        record_trend=bool(args.record_trend),
    )
    _print_console_summary(summary)
    print(f"Summary: {summary_path}", flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
