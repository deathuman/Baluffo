#!/usr/bin/env python3
"""Run the local performance smoke benchmark suite."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.perf_baseline import append_trend_record, build_baseline_record, write_baseline_record
from scripts.perf_compare import benchmark_duration_ms, compare_duration, load_benchmark_payload

DEFAULT_OUTPUT_DIR = REPO_ROOT / "_out" / "perf-ci"
DEFAULT_BASELINE_DIR = REPO_ROOT / "_out" / "perf-baseline"


def perf_ci_steps(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    baseline_dir: Path = DEFAULT_BASELINE_DIR,
) -> list[tuple[list[str], Path | None]]:
    discovery_output = output_dir / "discovery.json"
    fetch_output = output_dir / "fetch.json"
    return [
        (
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
                str(output_dir / "discovery-data"),
            ],
            discovery_output,
        ),
        (
            [
                sys.executable,
                "scripts/perf_compare.py",
                "--mode",
                "discovery",
                "--current",
                str(discovery_output),
                "--baseline",
                str(baseline_dir / "discovery-baseline.json"),
            ],
            None,
        ),
        (
            [
                sys.executable,
                "src/fetch_incremental_sanity_benchmark.py",
                "--timeout",
                "30",
                "--group",
                "smoke",
                "--output-dir",
                str(output_dir / "fetch-data"),
            ],
            fetch_output,
        ),
        (
            [
                sys.executable,
                "scripts/perf_compare.py",
                "--mode",
                "fetch",
                "--current",
                str(fetch_output),
                "--baseline",
                str(baseline_dir / "fetch-baseline.json"),
            ],
            None,
        ),
    ]


def perf_ci_benchmark_steps(
    *, output_dir: Path = DEFAULT_OUTPUT_DIR
) -> list[tuple[str, list[str], Path]]:
    discovery_output = output_dir / "discovery.json"
    fetch_output = output_dir / "fetch.json"
    return [
        (
            "discovery",
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
                str(output_dir / "discovery-data"),
            ],
            discovery_output,
        ),
        (
            "fetch",
            [
                sys.executable,
                "src/fetch_incremental_sanity_benchmark.py",
                "--group",
                "smoke",
                "--timeout",
                "30",
                "--output-dir",
                str(output_dir / "fetch-data"),
            ],
            fetch_output,
        ),
    ]


def run_step(command: list[str], output_path: Path | None) -> int:
    print(f"$ {' '.join(command)}", flush=True)
    if output_path is None:
        return subprocess.run(command, cwd=REPO_ROOT, check=False).returncode
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        return subprocess.run(command, cwd=REPO_ROOT, stdout=handle, check=False).returncode


def _median(values: list[int]) -> int:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return int(round((ordered[middle - 1] + ordered[middle]) / 2))


def _stage_durations(payload: dict[str, Any]) -> dict[str, int]:
    stages = payload.get("stageDurationsMs")
    if not isinstance(stages, dict):
        return {}
    result: dict[str, int] = {}
    for key, value in stages.items():
        try:
            duration = int(float(value))
        except (TypeError, ValueError):
            continue
        if duration > 0:
            result[str(key)] = duration
    return result


def _nested_int(payload: dict[str, Any], path: tuple[str, ...]) -> int:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return 0
        current = current.get(key)
    try:
        return max(0, int(float(current)))
    except (TypeError, ValueError):
        return 0


def _storage_metric_points(payload: dict[str, Any]) -> dict[str, int]:
    metrics = payload.get("storageMetrics")
    if not isinstance(metrics, dict):
        return {}
    points = {
        "writeCount": _nested_int(metrics, ("writes", "writeCount")),
        "serializationDurationTotalMs": _nested_int(
            metrics, ("writes", "totals", "serializationDurationMs", "total")
        ),
        "atomicReplaceDurationTotalMs": _nested_int(
            metrics, ("writes", "totals", "atomicReplaceDurationMs", "total")
        ),
        "compressedBytesTotal": _nested_int(
            metrics, ("writes", "totals", "compressedSizeBytes", "total")
        ),
        "uncompressedBytesTotal": _nested_int(
            metrics, ("writes", "totals", "uncompressedSizeBytes", "total")
        ),
        "registryJsonlJournalBytes": _nested_int(
            metrics, ("registryJournals", "registryJsonlJournalBytes")
        ),
        "sourceSyncLatestSizeBytes": _nested_int(
            metrics, ("sourceSyncSnapshots", "latestSizeBytes")
        ),
    }
    return {key: value for key, value in points.items() if value > 0}


def _summarize_storage_metrics(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[int]] = {}
    for payload in payloads:
        for key, value in _storage_metric_points(payload).items():
            grouped.setdefault(key, []).append(value)
    return {
        key: {
            "min": min(values),
            "median": _median(values),
            "max": max(values),
        }
        for key, values in sorted(grouped.items())
        if values
    }


def _summarize_runs(mode: str, payloads: list[dict[str, Any]]) -> dict[str, Any]:
    durations = [benchmark_duration_ms(payload, mode=mode) for payload in payloads]
    durations = [duration for duration in durations if duration > 0]
    stage_totals: dict[str, list[int]] = {}
    for payload in payloads:
        for stage, duration in _stage_durations(payload).items():
            stage_totals.setdefault(stage, []).append(duration)
    summary: dict[str, Any] = {
        "mode": mode,
        "runs": len(payloads),
        "durationsMs": durations,
        "medianDurationMs": _median(durations) if durations else 0,
        "minDurationMs": min(durations) if durations else 0,
        "maxDurationMs": max(durations) if durations else 0,
    }
    if stage_totals:
        summary["stageMedianDurationsMs"] = {
            stage: _median(values) for stage, values in sorted(stage_totals.items())
        }
    storage_metrics = _summarize_storage_metrics(payloads)
    if storage_metrics:
        summary["storageMetrics"] = storage_metrics
    return summary


def _record_median_summary(
    *,
    mode: str,
    benchmark_summary: dict[str, Any],
    status: str,
    output_dir: Path,
    baseline_dir: Path,
    trend_path: Path,
    record_baseline: bool,
    record_trend: bool,
) -> None:
    record = build_baseline_record(
        mode=mode,
        total_duration_ms=int(benchmark_summary.get("medianDurationMs") or 0),
        status="pass" if record_baseline else status,
        stage_durations_ms=dict(benchmark_summary.get("stageMedianDurationsMs") or {}),
        storage_metrics=dict(benchmark_summary.get("storageMetrics") or {}),
        artifact=str(output_dir / "summary.json"),
    )
    if record_baseline:
        path = write_baseline_record(record, baseline_dir=baseline_dir, trend_path=trend_path)
        print(f"Recorded median {mode} baseline: {path}", flush=True)
    elif record_trend:
        path = append_trend_record(record, trend_path=trend_path)
        print(f"Recorded median {mode} trend row: {path}", flush=True)


def run_repeated_perf_ci(
    runs: int,
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    baseline_dir: Path = DEFAULT_BASELINE_DIR,
    trend_path: Path = REPO_ROOT / "_out" / "perf-trend.ndjson",
    record_baseline: bool = False,
    record_trend: bool = False,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    payloads_by_mode: dict[str, list[dict[str, Any]]] = {"discovery": [], "fetch": []}
    for run_index in range(1, runs + 1):
        run_dir = output_dir / "runs" / f"run-{run_index}"
        for mode, command, output_path in perf_ci_benchmark_steps(output_dir=run_dir):
            exit_code = run_step(command, output_path)
            if exit_code != 0:
                return exit_code
            payloads_by_mode.setdefault(mode, []).append(load_benchmark_payload(output_path))

    summary = {
        "runs": runs,
        "benchmarks": {
            mode: _summarize_runs(mode, payloads)
            for mode, payloads in sorted(payloads_by_mode.items())
            if payloads
        },
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    exit_code = 0
    for mode, benchmark_summary in summary["benchmarks"].items():
        baseline = load_benchmark_payload(baseline_dir / f"{mode}-baseline.json")
        result = compare_duration(
            current_duration_ms=int(benchmark_summary.get("medianDurationMs") or 0),
            baseline_duration_ms=benchmark_duration_ms(baseline, mode=mode),
        )
        result.update({"mode": mode, "runs": runs})
        print(json.dumps(result, indent=2, ensure_ascii=False))
        if record_baseline or record_trend:
            _record_median_summary(
                mode=mode,
                benchmark_summary=benchmark_summary,
                status=str(result.get("status") or "pass"),
                output_dir=output_dir,
                baseline_dir=baseline_dir,
                trend_path=trend_path,
                record_baseline=record_baseline,
                record_trend=record_trend,
            )
        if result["status"] == "warn":
            print(f"::warning title=Benchmark regression::{result['message']}", file=sys.stderr)
        if result["status"] == "failed":
            exit_code = 1
    return exit_code


def parse_args(argv: list[str] | None = None) -> Any:
    import argparse

    parser = argparse.ArgumentParser(description="Run local performance smoke benchmarks.")
    parser.add_argument("--runs", type=int, default=1, help="Number of runs to execute.")
    parser.add_argument(
        "--record-trend",
        action="store_true",
        help="Append repeated-run median rows to _out/perf-trend.ndjson.",
    )
    parser.add_argument(
        "--record-baseline",
        action="store_true",
        help="Write repeated-run median baselines and append trend rows.",
    )
    parser.add_argument("--baseline-dir", default=str(DEFAULT_BASELINE_DIR))
    parser.add_argument("--trend-path", default=str(REPO_ROOT / "_out" / "perf-trend.ndjson"))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    runs = max(1, int(args.runs))
    if runs > 1:
        return run_repeated_perf_ci(
            runs,
            baseline_dir=Path(str(args.baseline_dir)),
            trend_path=Path(str(args.trend_path)),
            record_baseline=bool(args.record_baseline),
            record_trend=bool(args.record_trend),
        )
    DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for command, output_path in perf_ci_steps():
        exit_code = run_step(command, output_path)
        if exit_code != 0:
            return exit_code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
