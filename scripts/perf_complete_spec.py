#!/usr/bin/env python3
"""Defaults, endpoint tables and import surface for the benchmark run.

The pre-split module defined its repo root and its constant block before any
function, and the fidelity checker treats a unit's body as running up to the
next unit. The import preamble therefore travels with those constants in this
leaf instead of sitting in the coordinator header.
"""

from __future__ import annotations

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
del _repo_root


__all__ = [
    "DEFAULT_BASELINE_DIR",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_RUNTIME_TIMEOUT_S",
    "DEFAULT_TREND_PATH",
    "LIVE_BRIDGE_ENDPOINTS",
    "ProcessMemorySampler",
    "REPO_ROOT",
    "SUSPECT_ROUTE_LABELS",
    "_summarize_runs",
    "append_trend_record",
    "benchmark_duration_ms",
    "build_baseline_record",
    "cold_startup_probe_args",
    "compare_duration",
    "load_benchmark_payload",
    "packaged_probe_command",
    "resolve_built_exe",
    "startup_pair_paths",
    "summarize_trace_file",
    "warm_startup_probe_args",
    "write_baseline_record",
]


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.chrome_trace_summary import summarize_trace_file
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
