#!/usr/bin/env python3
"""Measure what the pipeline launch spends reading the last fetch report's count.

``PipelineService.start_task`` reads ``summary.outputCount`` from the previous
fetch report to record the baseline. The count is the only thing it needs, but
the read used to normalize the whole report (every source row, detail row,
availability block), so a multi-megabyte report cost the launch hundreds of
milliseconds for a single integer.

This tool answers two questions with the real launch path, not a model of it:

1. Which share of the service-side launch is ``baselineCountMs``? It starts the
   real ``PipelineService`` with a worker that returns immediately, captures the
   ``jobs_pipeline_launch_finished`` row, and repeats for three count
   implementations: the pre-change normalize pass, the memoized cold read, and
   the memoized warm read.
2. What does the journal say in production? It sums recorded
   ``phaseDurationsMs.baselineCountMs`` against ``durationMs`` for finished
   launches. Launch rows emit the phases, so an empty journal means the app has
   not run a launch since that instrumentation landed, not that the share is
   zero.

Nothing is fetched, written to ``data/``, or persisted: the launch's control
status goes to a temporary directory, and the worker thread does no work.

Usage::

    python tools/measurements/pipeline/launch_baseline_read.py
    python tools/measurements/pipeline/launch_baseline_read.py --report PATH --runs 5
    python tools/measurements/pipeline/launch_baseline_read.py --journal data/admin-bridge-events.jsonl
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.bridge import admin_task_runtime
from src.bridge.fetch_report_summary import FETCH_REPORT_SUMMARY_FILE_NAME
from src.bridge.ops_health import summarize_fetch_report
from src.bridge.pipeline_service import PipelineRuntime, PipelineService
from src.bridge.report_normalizer import normalize_fetch_report_contract
from src.shared.fetch_report_identity import fetch_report_identity
from src.source_registry_io import load_runtime_evidence

LAUNCH_EVENT = "jobs_pipeline_launch_finished"
DEFAULT_JOURNAL = REPO_ROOT / "data" / "admin-bridge-events.jsonl"


def _resolve_report(explicit: str) -> Path:
    if explicit:
        return Path(explicit)
    candidates = [
        path for path in (REPO_ROOT / "_out").glob("*/*/jobs-fetch-report.json") if path.is_file()
    ]
    if candidates:
        return max(candidates, key=lambda path: path.stat().st_size)
    return REPO_ROOT / "data" / "jobs-fetch-report.json"


def _root_for(report_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        JOBS_FETCH_REPORT_PATH=report_path,
        normalize_fetch_report_contract=normalize_fetch_report_contract,
        summarize_fetch_report=summarize_fetch_report,
    )


def _pre_change_count(report_path: Path) -> int:
    """Exactly what ``current_fetch_output_count`` did before the memoized read."""
    normalized = normalize_fetch_report_contract(load_runtime_evidence(report_path, {}))
    return int(summarize_fetch_report(normalized).get("outputCount") or 0)


def _report_summary_count(report_path: Path) -> int:
    payload = load_runtime_evidence(report_path, {})
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    return int(summary.get("outputCount") or summary.get("uniqueOutputCount") or 0)


def _sidecar_count(report_path: Path) -> int | None:
    payload = load_runtime_evidence(report_path.with_name(FETCH_REPORT_SUMMARY_FILE_NAME), {})
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    if "outputCount" not in summary and "uniqueOutputCount" not in summary:
        return None
    return int(summary.get("outputCount") or summary.get("uniqueOutputCount") or 0)


def _timed(fn: Any, runs: int) -> tuple[int | None, float]:
    values: list[int | None] = []
    timings: list[float] = []
    for _ in range(max(1, runs)):
        started = time.perf_counter()
        value = fn()
        timings.append((time.perf_counter() - started) * 1000)
        values.append(value)
    return (values[0] if values else None), statistics.median(timings)


def _launch_once(
    *,
    report_path: Path,
    count_fn: Any,
    control_dir: Path,
) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    def bridge_log(level: str, event: str, **fields: Any) -> None:
        if event == LAUNCH_EVENT:
            captured.update(fields)
            captured["_level"] = level

    status: dict[str, Any] = {
        "active": False,
        "runId": "",
        "stage": "idle",
        "progress": {},
        "startedAt": "",
        "finishedAt": "",
        "error": "",
        "updatesFound": False,
        "refreshRecommended": False,
        "baselineOutputCount": 0,
        "finalOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }
    service = PipelineService(
        pipeline_state_lock=threading.Lock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=bridge_log,
        now_iso=lambda: "2026-01-01T00:00:00Z",
        parse_iso=lambda _value: None,
        sync_task_running=lambda: False,
        current_fetch_output_count=count_fn,
        load_json_object=lambda _path, default: default,
        wait_for_sync_completion=lambda _action, _timeout: {"status": "ok"},
        discovery_report_path=report_path.parent / "source-discovery-report.json",
        fetch_report_path=report_path,
        trigger_discovery_task=lambda **_kw: (200, {"started": True}),
        start_fetcher_task=lambda _payload: {"started": True},
        start_sync_task=lambda *_args, **_kw: {"started": True},
        get_app_version=lambda: "measurement",
        control_data_dir=control_dir,
    )
    # The launch is what is being measured; the worker's stages are not run.
    service._run_worker = lambda _run_id: None  # type: ignore[method-assign]
    result = service.start_task({"jobsPageLoadedCount": 25})
    captured.setdefault("durationMs", 0)
    captured["baseline"] = status.get("baselineOutputCount")
    captured["started"] = bool(result.get("started"))
    return captured


def _measure_launch_arm(
    *,
    report_path: Path,
    label: str,
    count_fn: Any,
    runs: int,
    clear_cache_each_run: bool,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for _ in range(runs):
        if clear_cache_each_run:
            admin_task_runtime.clear_fetch_output_count_cache()
        with tempfile.TemporaryDirectory(prefix="baseline-launch-") as control_dir:
            rows.append(
                _launch_once(
                    report_path=report_path,
                    count_fn=count_fn,
                    control_dir=Path(control_dir),
                )
            )
    totals = [int(row.get("durationMs") or 0) for row in rows]
    phases = [dict(row.get("phaseDurationsMs") or {}) for row in rows]
    baselines = [int(phase.get("baselineCountMs") or 0) for phase in phases]
    totals_median = statistics.median(totals)
    baselines_median = statistics.median(baselines)
    slowest = [str(row.get("slowestPhase") or "") for row in rows]
    return {
        "label": label,
        "launchMs": totals_median,
        "baselineMs": baselines_median,
        "sharePct": (100.0 * baselines_median / totals_median) if totals_median else 0.0,
        "slowest": statistics.mode(slowest) if slowest else "",
        "baselineCount": rows[0].get("baseline") if rows else None,
        "level": rows[0].get("_level") if rows else "",
    }


def _journal_share(journal_path: Path) -> tuple[int, list[tuple[int, int, str]]]:
    rows: list[tuple[int, int, str]] = []
    try:
        lines = journal_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return 0, []
    for line in lines:
        if LAUNCH_EVENT not in line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("event") != LAUNCH_EVENT:
            continue
        fields = payload.get("fields")
        if not isinstance(fields, dict):
            continue
        phases = fields.get("phaseDurationsMs")
        if not isinstance(phases, dict) or "baselineCountMs" not in phases:
            continue
        rows.append(
            (
                int(fields.get("durationMs") or 0),
                int(phases.get("baselineCountMs") or 0),
                str(fields.get("slowestPhase") or ""),
            )
        )
    return len(rows), rows


def _sidecar_identity_state(report_path: Path, sidecar: Path) -> str:
    """Whether the sidecar's run identity matches the report it sits beside."""
    try:
        payload = json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "unreadable"
    identity = fetch_report_identity(report_path)
    if identity is None:
        return "report identity unavailable"
    run_id = str(payload.get("runId") or "").strip()
    finished_at = str(payload.get("finishedAt") or "").strip()
    return "match" if identity == (run_id, finished_at) else "mismatch"


def _format_age_delta(delta_ms: float) -> str:
    magnitude = abs(delta_ms)
    if magnitude < 1_000:
        return f"{delta_ms:+.0f} ms"
    if magnitude < 60_000:
        return f"{delta_ms / 1_000:+.1f} s"
    if magnitude < 3_600_000:
        return f"{delta_ms / 60_000:+.1f} min"
    if magnitude < 86_400_000:
        return f"{delta_ms / 3_600_000:+.1f} h"
    return f"{delta_ms / 86_400_000:+.1f} d"


def _render(
    report_path: Path,
    sidecar: Path,
    *,
    direct: list[tuple[str, int | None, float]],
    launches: list[dict[str, Any]],
    journal_path: Path,
) -> str:
    size_mb = report_path.stat().st_size / 1_000_000
    lines = [
        f"report: {report_path}",
        f"size:   {size_mb:.1f} MB",
    ]
    if sidecar.exists():
        delta_ms = (sidecar.stat().st_mtime_ns - report_path.stat().st_mtime_ns) / 1_000_000
        lines.append(
            f"sidecar: {sidecar.name} ({sidecar.stat().st_size / 1000:.1f} kB, "
            f"identity {_sidecar_identity_state(report_path, sidecar)}, "
            f"mtime {_format_age_delta(delta_ms)} vs report - informational)"
        )
    else:
        lines.append("sidecar: absent (launch falls back to the report)")

    lines.extend(["", "count read, direct"])
    lines.append(f"{'arm':<34}{'median ms':>12}{'count':>10}")
    for label, value, median_ms in direct:
        rendered = "n/a" if value is None else str(value)
        lines.append(f"{label:<34}{median_ms:>12.3f}{rendered:>10}")

    lines.extend(["", "service-side launch (real PipelineService.start_task, stub worker)"])
    lines.append(f"{'arm':<34}{'launch ms':>12}{'baseline ms':>14}{'share':>9}{'slowest':>18}")
    for row in launches:
        lines.append(
            f"{row['label']:<34}{row['launchMs']:>12.0f}{row['baselineMs']:>14.0f}"
            f"{row['sharePct']:>8.1f}%{row['slowest']:>18}"
        )

    lines.extend(["", f"journal: {journal_path}"])
    count, rows = _journal_share(journal_path)
    if not rows:
        lines.append("  no jobs_pipeline_launch_finished rows with phaseDurationsMs")
        lines.append(
            "  run a real update in the app, then re-run this tool for the production share"
        )
        return "\n".join(lines)
    totals = [row[0] for row in rows]
    baselines = [row[1] for row in rows]
    slowest_baseline = sum(1 for row in rows if row[2] == "baselineCountMs")
    total_median = statistics.median(totals)
    baseline_median = statistics.median(baselines)
    share = (100.0 * baseline_median / total_median) if total_median else 0.0
    last_total, last_baseline, _ = rows[-1]
    lines.append(
        f"  rows: {count}  median launch: {total_median:.0f} ms  "
        f"median baselineCountMs: {baseline_median:.0f} ms  share: {share:.1f}%"
    )
    lines.append(
        f"  slowest phase was baselineCountMs in {slowest_baseline}/{count} launches; "
        f"last launch: {last_total} ms total, {last_baseline} ms baseline"
    )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--report", default="", help="Fetch report to measure; default: largest under _out"
    )
    parser.add_argument("--runs", type=int, default=3, help="Iterations per arm (default: 3)")
    parser.add_argument(
        "--journal", default=str(DEFAULT_JOURNAL), help="Bridge journal for the production share"
    )
    parser.add_argument("--skip-launch", action="store_true", help="Only time the count reads")
    args = parser.parse_args(argv)

    report_path = _resolve_report(args.report)
    if not report_path.is_file():
        print(f"report not found: {report_path}", file=sys.stderr)
        return 2
    sidecar = report_path.with_name(FETCH_REPORT_SUMMARY_FILE_NAME)
    root_mod = _root_for(report_path)

    direct: list[tuple[str, int | None, float]] = []
    for label, fn in (
        ("pre-change normalize+summarize", lambda: _pre_change_count(report_path)),
        ("report summary only", lambda: _report_summary_count(report_path)),
        ("summary sidecar only", lambda: _sidecar_count(report_path)),
    ):
        value, median_ms = _timed(fn, args.runs)
        direct.append((label, value, median_ms))

    admin_task_runtime.clear_fetch_output_count_cache()
    memo_value, memo_cold_ms = _timed(
        lambda: admin_task_runtime.current_fetch_output_count(root_mod=root_mod), 1
    )
    admin_task_runtime.clear_fetch_output_count_cache()
    admin_task_runtime.current_fetch_output_count(root_mod=root_mod)
    memo_warm_value, memo_warm_ms = _timed(
        lambda: admin_task_runtime.current_fetch_output_count(root_mod=root_mod), 1
    )
    direct.append(("current, cold cache", memo_value, memo_cold_ms))
    direct.append(("current, warm cache", memo_warm_value, memo_warm_ms))

    launches: list[dict[str, Any]] = []
    if not args.skip_launch:
        launches.append(
            _measure_launch_arm(
                report_path=report_path,
                label="pre-change count in launch",
                count_fn=lambda: _pre_change_count(report_path),
                runs=args.runs,
                clear_cache_each_run=False,
            )
        )
        launches.append(
            _measure_launch_arm(
                report_path=report_path,
                label="current count, cold cache",
                count_fn=lambda: admin_task_runtime.current_fetch_output_count(root_mod=root_mod),
                runs=args.runs,
                clear_cache_each_run=True,
            )
        )
        launches.append(
            _measure_launch_arm(
                report_path=report_path,
                label="current count, warm cache",
                count_fn=lambda: admin_task_runtime.current_fetch_output_count(root_mod=root_mod),
                runs=args.runs,
                clear_cache_each_run=False,
            )
        )

    print(
        _render(
            report_path,
            sidecar,
            direct=direct,
            launches=launches,
            journal_path=Path(args.journal),
        )
    )

    expected = direct[0][1]
    # ``None`` means the arm could not supply a count (for example a stale
    # sidecar the launch would reject), not that it disagreed.
    mismatches = [label for label, value, _ms in direct if value is not None and value != expected]
    if mismatches:
        print(
            "\nwarning: count mismatch between arms: " + ", ".join(mismatches),
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
