#!/usr/bin/env python3
"""Audit every hot fetch-report consumer and its read cost.

The fetch report is the largest routine artifact the bridge touches (tens of
megabytes after a full run). This tool times each consumer class on a real
report so a "summary" path that secretly parses the whole file is visible:

* pipeline launch baseline count: pre-fix normalize pass vs the memoized read
* desktop startup success gate: full report parse vs the summary sidecar plus its
  bounded run-identity prefix
* polled summary/live route payloads: sidecar reads (should be sub-millisecond)
* explicit full-report consumers: normalization is expected, so the number is a
  budget for how expensive those views are allowed to be
* the retry-failed fetcher preset: it needs failed source names, so it pays a
  full normalize once per launch (listed for completeness, not as a defect)

No production code is exercised for its side effects: the startup gate runs
against a temporary data dir, and nothing is written under ``data/``.

Usage::

    python tools/measurements/pipeline/fetch_report_read_audit.py
    python tools/measurements/pipeline/fetch_report_read_audit.py --report PATH --runs 5
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.bridge import admin_task_runtime
from src.bridge.fetch_report_review_state import load_fetch_report_with_dedup_review_state
from src.bridge.fetch_report_summary import FETCH_REPORT_SUMMARY_FILE_NAME
from src.bridge.ops_health import summarize_fetch_report
from src.bridge.report_normalizer import (
    failed_source_names_from_report,
    normalize_fetch_report_contract,
)
from src.bridge.routes.get_fetch_report import (
    _fetch_report_live_payload_from_file,
    _fetch_report_summary_payload_from_file,
)
from src.ship import jobs_first_run_state as feed_state
from src.source_registry_io import load_runtime_evidence


def _resolve_report(explicit: str) -> Path:
    if explicit:
        return Path(explicit)
    candidates = [
        path for path in (REPO_ROOT / "_out").glob("*/*/jobs-fetch-report.json") if path.is_file()
    ]
    if candidates:
        return max(candidates, key=lambda path: path.stat().st_size)
    return REPO_ROOT / "data" / "jobs-fetch-report.json"


def _timed(fn: Any, runs: int) -> tuple[Any, float]:
    values: list[Any] = []
    timings: list[float] = []
    for _ in range(max(1, runs)):
        started = time.perf_counter()
        value = fn()
        timings.append((time.perf_counter() - started) * 1000)
        values.append(value)
    return values[0], statistics.median(timings)


def _pre_change_count(report_path: Path) -> int:
    normalized = normalize_fetch_report_contract(load_runtime_evidence(report_path, {}))
    return int(summarize_fetch_report(normalized).get("outputCount") or 0)


def _startup_gate_dir(report_path: Path) -> tuple[Path, tempfile.TemporaryDirectory[str]]:
    holder = tempfile.TemporaryDirectory(prefix="fetch-report-audit-")
    data_path = Path(holder.name)
    try:
        os.link(report_path, data_path / feed_state.FETCH_REPORT_NAME)
    except OSError:
        shutil.copyfile(report_path, data_path / feed_state.FETCH_REPORT_NAME)
    sidecar = report_path.with_name(FETCH_REPORT_SUMMARY_FILE_NAME)
    if sidecar.is_file():
        shutil.copyfile(sidecar, data_path / FETCH_REPORT_SUMMARY_FILE_NAME)
    for name in feed_state.RUNTIME_FEED_ARTIFACTS:
        (data_path / name).write_text(json.dumps([{"id": 1}]), encoding="utf-8")
    return data_path, holder


def _pre_change_startup_gate(data_path: Path) -> bool:
    report = feed_state.read_json(data_path / feed_state.FETCH_REPORT_NAME, {})
    if not feed_state._runtime_report_has_success_metadata(report):  # noqa: SLF001
        return False
    return feed_state.has_loadable_runtime_feed_artifacts(data_path)


def _render(rows: list[tuple[str, Any, float, str, str]], report_path: Path) -> str:
    lines = [
        f"report: {report_path}",
        f"size:   {report_path.stat().st_size / 1_000_000:.1f} MB",
        "",
        f"{'consumer':<42}{'median ms':>12}{'result':>10}  {'runs':<22}note",
    ]
    for label, result, median_ms, runs, note in rows:
        rendered = "-" if result is None else str(result)
        lines.append(f"{label:<42}{median_ms:>12.3f}{rendered:>10}  {runs:<22}{note}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--report", default="", help="Fetch report to measure; default: largest under _out"
    )
    parser.add_argument("--runs", type=int, default=3, help="Iterations per arm (default: 3)")
    args = parser.parse_args(argv)

    report_path = _resolve_report(args.report)
    if not report_path.is_file():
        print(f"report not found: {report_path}", file=sys.stderr)
        return 2
    sidecar_path = report_path.with_name(FETCH_REPORT_SUMMARY_FILE_NAME)
    root_mod = SimpleNamespace(
        JOBS_FETCH_REPORT_PATH=report_path,
        normalize_fetch_report_contract=normalize_fetch_report_contract,
        summarize_fetch_report=summarize_fetch_report,
    )
    dedup_state = report_path.parent / "dedup-review-state.audit-missing.json"

    data_path, holder = _startup_gate_dir(report_path)
    rows: list[tuple[str, Any, float, str, str]] = []
    try:
        rows.append(
            (
                "pipeline launch count, pre-change",
                *_timed(lambda: _pre_change_count(report_path), args.runs),
                "pipeline launch",
                "normalize + summarize (removed)",
            )
        )
        admin_task_runtime.clear_fetch_output_count_cache()
        cold_value, cold_ms = _timed(
            lambda: admin_task_runtime.current_fetch_output_count(root_mod=root_mod), 1
        )
        rows.append(
            (
                "pipeline launch count, current cold",
                cold_value,
                cold_ms,
                "pipeline launch",
                "sidecar + memo",
            )
        )
        warm_value, warm_ms = _timed(
            lambda: admin_task_runtime.current_fetch_output_count(root_mod=root_mod), args.runs
        )
        rows.append(
            (
                "pipeline launch count, current warm",
                warm_value,
                warm_ms,
                "pipeline launch",
                "memo hit",
            )
        )
        rows.append(
            (
                "desktop success gate, pre-change",
                *_timed(lambda: _pre_change_startup_gate(data_path), args.runs),
                "every desktop start",
                "full report parse (removed)",
            )
        )
        rows.append(
            (
                "desktop success gate, current",
                *_timed(
                    lambda: feed_state.has_successful_runtime_jobs_report(data_path), args.runs
                ),
                "every desktop start",
                "sidecar + identity prefix",
            )
        )
    finally:
        holder.cleanup()

    summary_value, summary_ms = _timed(
        lambda: _fetch_report_summary_payload_from_file(report_path), args.runs
    )
    rows.append(
        (
            "ops fetch-report summary view",
            summary_value.get("detailLevel"),
            summary_ms,
            "Admin poll",
            "sidecar/prefix only",
        )
    )
    live_value, live_ms = _timed(
        lambda: _fetch_report_live_payload_from_file(report_path), args.runs
    )
    rows.append(
        (
            "ops fetch-report live view",
            live_value.get("detailLevel"),
            live_ms,
            "Admin fetcher poll",
            "sidecar only",
        )
    )
    full_value, full_ms = _timed(
        lambda: load_fetch_report_with_dedup_review_state(
            normalize_fetch_report_contract=normalize_fetch_report_contract,
            jobs_fetch_report_path=report_path,
            dedup_review_state_path=dedup_state,
        ),
        args.runs,
    )
    rows.append(
        (
            "full report normalize + dedup merge",
            len(full_value[0].get("sources") or []),
            full_ms,
            "explicit full view",
            "expected: full payload",
        )
    )
    failed_names_value, failed_names_ms = _timed(
        lambda: failed_source_names_from_report(
            normalize_fetch_report_contract(load_runtime_evidence(report_path, {}))
        ),
        args.runs,
    )
    rows.append(
        (
            "failed source names (retry preset)",
            len(failed_names_value),
            failed_names_ms,
            "jobs fetcher launch",
            "needs source rows",
        )
    )

    print(_render(rows, report_path))
    if not sidecar_path.is_file():
        print("\nwarning: no summary sidecar beside the report; the gate will fall back")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
