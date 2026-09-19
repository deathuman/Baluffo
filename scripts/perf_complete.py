#!/usr/bin/env python3
"""Run the broad local benchmark suite and write a consolidated report.

Thin coordinator: the implementation lives in sibling leaf modules and every
original module attribute is re-exported here, so importers and tests keep
working unchanged."""

from __future__ import annotations

import argparse
import http.client
import json
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
del _repo_root

from scripts.perf_complete_bench import (
    _annotated_timing_rows,
    _benchmark_steps,
    _error_timing_rows,
    _frontend_page_duration_ms,
    _profile_sample,
    _startup_stage_durations,
    _startup_summary,
    _summarize_memory_runs,
    _suspect_route_rows,
    _timing_rows,
    build_fetch_source_timing_summary,
)
from scripts.perf_complete_bridge import _fetch_live_bridge_request, contextlib, time, urllib
from scripts.perf_complete_process import (
    _artifact_path_from_summary,
    _bounded_dict,
    _comparison,
    _duration_ms,
    _file_size,
    _format_mib,
    _format_top_contributors,
    _median,
    _npm_command,
    _process_peak_bytes,
    _read_json,
    _record_row,
    parse_timeout_sequence,
    summarize_artifacts,
)
from scripts.perf_complete_profiles import (
    _memory_sample,
    _read_stat_value,
    _storage_metrics_payload,
    _storage_read_sample,
    build_chrome_trace_summary,
    build_memory_profile_summary,
    build_optimization_targets,
    build_storage_read_profile_summary,
    build_sync_detail_summary,
)
from scripts.perf_complete_runs import _scenario_by_slug
from scripts.perf_complete_spec import (
    DEFAULT_BASELINE_DIR,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_RUNTIME_TIMEOUT_S,
    DEFAULT_TREND_PATH,
    LIVE_BRIDGE_ENDPOINTS,
    REPO_ROOT,
    SUSPECT_ROUTE_LABELS,
    ProcessMemorySampler,
    _summarize_runs,
    append_trend_record,
    benchmark_duration_ms,
    build_baseline_record,
    cold_startup_probe_args,
    compare_duration,
    load_benchmark_payload,
    packaged_probe_command,
    resolve_built_exe,
    startup_pair_paths,
    summarize_trace_file,
    warm_startup_probe_args,
    write_baseline_record,
)
from scripts.perf_complete_summary import _print_console_summary, _record_complete_rows

__all__ = [
    "Any",
    "DEFAULT_BASELINE_DIR",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_RUNTIME_TIMEOUT_S",
    "DEFAULT_TREND_PATH",
    "LIVE_BRIDGE_ENDPOINTS",
    "ProcessMemorySampler",
    "REPO_ROOT",
    "SUSPECT_ROUTE_LABELS",
    "UTC",
    "_annotated_timing_rows",
    "_artifact_path_from_summary",
    "_benchmark_steps",
    "_bounded_dict",
    "_comparison",
    "_duration_ms",
    "_error_timing_rows",
    "_fetch_live_bridge_request",
    "_file_size",
    "_format_mib",
    "_format_top_contributors",
    "_frontend_page_duration_ms",
    "_median",
    "_memory_sample",
    "_npm_command",
    "_print_console_summary",
    "_process_peak_bytes",
    "_profile_sample",
    "_read_json",
    "_read_stat_value",
    "_record_complete_rows",
    "_record_row",
    "_scenario_by_slug",
    "_startup_stage_durations",
    "_startup_summary",
    "_storage_metrics_payload",
    "_storage_read_sample",
    "_summarize_memory_runs",
    "_summarize_runs",
    "_suspect_route_rows",
    "_timing_rows",
    "append_trend_record",
    "argparse",
    "benchmark_duration_ms",
    "build_baseline_record",
    "build_bridge_profile_summary",
    "build_chrome_trace_summary",
    "build_fetch_source_timing_summary",
    "build_memory_profile_summary",
    "build_optimization_targets",
    "build_storage_read_profile_summary",
    "build_sync_detail_summary",
    "capture_live_bridge_profile",
    "cold_startup_probe_args",
    "compare_duration",
    "contextlib",
    "datetime",
    "generate_run_token",
    "http",
    "json",
    "load_benchmark_payload",
    "main",
    "os",
    "packaged_probe_command",
    "parse_args",
    "parse_timeout_sequence",
    "resolve_built_exe",
    "run_frontend_boot",
    "run_monitored_command",
    "run_repeated_payload_benchmark",
    "run_startup_pair",
    "run_sync_rehearsal",
    "shutil",
    "startup_pair_paths",
    "subprocess",
    "summarize_artifacts",
    "summarize_trace_file",
    "time",
    "urllib",
    "warm_startup_probe_args",
    "write_baseline_record",
]


def generate_run_token(*, now: datetime | None = None) -> str:
    resolved_now = now if isinstance(now, datetime) else datetime.now(UTC)
    return resolved_now.strftime("%Y%m%d-%H%M%S-%f")


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
    parser.add_argument(
        "--chrome-trace",
        action="append",
        default=[],
        help=(
            "Optional Chrome DevTools Performance trace .json or .json.gz to fold into "
            "the report as user-perceived performance evidence. Can be passed multiple times."
        ),
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
    if list(args.chrome_trace or []):
        benchmarks["chromeTraces"] = build_chrome_trace_summary(list(args.chrome_trace or []))
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
