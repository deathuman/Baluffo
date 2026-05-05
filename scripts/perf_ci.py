#!/usr/bin/env python3
"""Run the local performance smoke benchmark suite."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
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
                "--sources",
                "greenhouse_boards",
                "lever_sources",
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


def run_step(command: list[str], output_path: Path | None) -> int:
    print(f"$ {' '.join(command)}", flush=True)
    if output_path is None:
        return subprocess.run(command, cwd=REPO_ROOT, check=False).returncode
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        return subprocess.run(command, cwd=REPO_ROOT, stdout=handle, check=False).returncode


def main() -> int:
    DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for command, output_path in perf_ci_steps():
        exit_code = run_step(command, output_path)
        if exit_code != 0:
            return exit_code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
