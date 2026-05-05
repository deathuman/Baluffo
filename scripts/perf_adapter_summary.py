#!/usr/bin/env python3
"""Print adapter-duration summaries from benchmark JSON artifacts."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.perf_compare import load_benchmark_payload

DEFAULT_BENCHMARK_PATHS = [
    Path("_out/perf-ci/fetch.json"),
    Path("_out/perf-ci/discovery.json"),
]


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _add_duration(summary: dict[str, int], adapter: Any, duration: Any) -> None:
    adapter_name = str(adapter or "").strip().lower()
    if not adapter_name:
        return
    try:
        duration_ms = int(float(duration))
    except (TypeError, ValueError):
        return
    if duration_ms <= 0:
        return
    summary[adapter_name] = summary.get(adapter_name, 0) + duration_ms


def _collect_timing_summary_adapters(summary: dict[str, int], payload: dict[str, Any]) -> None:
    runtime = _as_dict(payload.get("runtime"))
    timing_summary = _as_dict(runtime.get("timingSummary"))
    for row in _as_list(timing_summary.get("adapterTimings")):
        if isinstance(row, dict):
            _add_duration(summary, row.get("adapter"), row.get("durationMs"))
    for row in _as_list(runtime.get("adapterTimings")):
        if isinstance(row, dict):
            _add_duration(summary, row.get("adapter"), row.get("durationMs"))


def summarize_adapter_durations(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary: dict[str, int] = {}
    for payload in payloads:
        network_counters = _as_dict(payload.get("networkWaitCounters"))
        for adapter, duration in _as_dict(network_counters.get("adapterDurationsMs")).items():
            _add_duration(summary, adapter, duration)
        _collect_timing_summary_adapters(summary, payload)
        _collect_timing_summary_adapters(summary, _as_dict(payload.get("firstRun")))
        _collect_timing_summary_adapters(summary, _as_dict(payload.get("secondRun")))
    return [
        {"adapter": adapter, "durationMs": duration_ms}
        for adapter, duration_ms in sorted(summary.items(), key=lambda item: item[1], reverse=True)
    ]


def format_adapter_summary(rows: list[dict[str, Any]], *, limit: int = 10) -> str:
    visible_rows = rows[: max(0, int(limit))]
    if not visible_rows:
        return "No adapter duration data found."
    lines = ["adapter              duration"]
    for row in visible_rows:
        adapter = str(row.get("adapter") or "unknown")[:20]
        duration_s = int(row.get("durationMs") or 0) / 1000
        lines.append(f"{adapter:<20} {duration_s:>7.1f}s")
    return "\n".join(lines)


def load_payloads(paths: list[Path]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            continue
        payload = load_benchmark_payload(path)
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize adapter durations from benchmarks.")
    parser.add_argument("paths", nargs="*", default=[str(path) for path in DEFAULT_BENCHMARK_PATHS])
    parser.add_argument("--limit", type=int, default=10)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    paths = [Path(str(path)) for path in args.paths]
    print(
        format_adapter_summary(summarize_adapter_durations(load_payloads(paths)), limit=args.limit)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
