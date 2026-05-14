#!/usr/bin/env python3
"""Run the broad local benchmark suite and write a consolidated report."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
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
    return summary, exit_code


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


def _startup_summary(
    *,
    mode: str,
    report_path: Path,
    artifacts_dir: Path,
    command_result: dict[str, Any],
    baseline_dir: Path,
) -> dict[str, Any]:
    report = _read_json(report_path)
    profile = report.get("startupProfile") if isinstance(report.get("startupProfile"), dict) else {}
    duration_ms = int(profile.get("firstUsableMs") or 0)
    key_paths = [report_path]
    artifacts = report.get("artifacts") if isinstance(report.get("artifacts"), dict) else {}
    for value in artifacts.values():
        token = str(value or "").strip()
        if token:
            key_paths.append(Path(token))
    comparison = _comparison(
        mode=f"startup-{mode}",
        duration_ms=duration_ms,
        baseline_dir=baseline_dir,
    )
    return {
        "mode": f"startup-{mode}",
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
) -> tuple[dict[str, Any], int, Path | None]:
    paths = startup_pair_paths(artifact_root=output_dir / "startup")
    paths["runRoot"].mkdir(parents=True, exist_ok=True)
    cold_command = packaged_probe_command(
        cold_startup_probe_args(paths, runtime_timeout_s=runtime_timeout_s)
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
        report_path=paths["coldReportPath"],
        artifacts_dir=paths["coldArtifactsDir"],
        command_result=cold_result,
        baseline_dir=baseline_dir,
    )
    warm_summary = _startup_summary(
        mode="warm",
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
    else:
        command.append("--rebuild")
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
            "startup.cold",
            summary["benchmarks"]["startup"]["cold"].get("durationMs", 0),
            summary["benchmarks"]["startup"]["cold"],
            None,
        ),
        (
            "startup.warm",
            summary["benchmarks"]["startup"]["warm"].get("durationMs", 0),
            summary["benchmarks"]["startup"]["warm"],
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
            startup["cold"].get("durationMs"),
            startup["cold"].get("stageDurationsMs"),
            startup["cold"].get("comparison", {}).get("status"),
        ),
        (
            "startup-warm",
            startup["warm"].get("durationMs"),
            startup["warm"].get("stageDurationsMs"),
            startup["warm"].get("comparison", {}).get("status"),
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
    startup, startup_exit, reused_exe = run_startup_pair(
        output_dir=run_dir,
        runtime_timeout_s=float(args.runtime_timeout),
        baseline_dir=baseline_dir,
    )
    sync, sync_exit = run_sync_rehearsal(
        output_dir=run_dir,
        runtime_timeout_s=float(args.runtime_timeout),
        baseline_dir=baseline_dir,
        exe_path=reused_exe,
    )
    for value in (discovery_exit, fetch_exit, frontend_exit, startup_exit, sync_exit):
        if int(value or 0) != 0 and exit_code == 0:
            exit_code = int(value or 1)

    summary_path = run_dir / "summary.json"
    latest_summary_path = output_root / "summary.json"
    summary = {
        "schemaVersion": 1,
        "generatedAt": datetime.now(UTC).isoformat(),
        "runDir": str(run_dir),
        "summaryPath": str(summary_path),
        "latestSummaryPath": str(latest_summary_path),
        "benchmarks": {
            "discovery": discovery,
            "fetch": fetch,
            "frontendBoot": frontend,
            "startup": startup,
            "sync": sync,
        },
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
