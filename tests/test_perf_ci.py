from __future__ import annotations

import sys
from pathlib import Path

from scripts import perf_ci


def test_perf_ci_steps_match_smoke_workflow(tmp_path: Path) -> None:
    output_dir = tmp_path / "perf-ci"
    baseline_dir = tmp_path / "perf-baseline"

    steps = perf_ci.perf_ci_steps(output_dir=output_dir, baseline_dir=baseline_dir)

    assert steps[0] == (
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
        output_dir / "discovery.json",
    )
    assert steps[1][0] == [
        sys.executable,
        "scripts/perf_compare.py",
        "--mode",
        "discovery",
        "--current",
        str(output_dir / "discovery.json"),
        "--baseline",
        str(baseline_dir / "discovery-baseline.json"),
    ]
    assert steps[2] == (
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
        output_dir / "fetch.json",
    )
    assert steps[3][0] == [
        sys.executable,
        "scripts/perf_compare.py",
        "--mode",
        "fetch",
        "--current",
        str(output_dir / "fetch.json"),
        "--baseline",
        str(baseline_dir / "fetch-baseline.json"),
    ]


def test_repeated_benchmark_steps_use_group_smoke(tmp_path: Path) -> None:
    output_dir = tmp_path / "perf-ci"

    steps = perf_ci.perf_ci_benchmark_steps(output_dir=output_dir)

    assert steps[1] == (
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
        output_dir / "fetch.json",
    )


def test_summarize_runs_uses_median_and_stage_medians() -> None:
    summary = perf_ci._summarize_runs(
        "fetch",
        [
            {"totalDurationMs": 300, "stageDurationsMs": {"fetchAndParse": 200}},
            {"totalDurationMs": 100, "stageDurationsMs": {"fetchAndParse": 50}},
            {"totalDurationMs": 200, "stageDurationsMs": {"fetchAndParse": 100}},
        ],
    )

    assert summary["medianDurationMs"] == 200
    assert summary["minDurationMs"] == 100
    assert summary["maxDurationMs"] == 300
    assert summary["stageMedianDurationsMs"] == {"fetchAndParse": 100}


def test_parse_args_accepts_median_recording_options(tmp_path: Path) -> None:
    args = perf_ci.parse_args(
        [
            "--runs",
            "3",
            "--record-trend",
            "--record-baseline",
            "--baseline-dir",
            str(tmp_path / "baseline"),
            "--trend-path",
            str(tmp_path / "trend.ndjson"),
        ]
    )

    assert args.runs == 3
    assert args.record_trend is True
    assert args.record_baseline is True
    assert args.baseline_dir == str(tmp_path / "baseline")
    assert args.trend_path == str(tmp_path / "trend.ndjson")
