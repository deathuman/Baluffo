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
            "--sources",
            "greenhouse_boards",
            "lever_sources",
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
