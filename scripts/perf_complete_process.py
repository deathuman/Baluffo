#!/usr/bin/env python3
"""Monitored-command execution, artifact reading and timing primitives.

Leaf of ``scripts/perf_complete.py``; every unit body is byte-identical to the pre-split
module. The coordinator imports and re-exports these names.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
del _repo_root

from scripts.perf_complete_spec import (
    append_trend_record,
    benchmark_duration_ms,
    build_baseline_record,
    compare_duration,
    load_benchmark_payload,
    write_baseline_record,
)

__all__ = [
    "Any",
    "_artifact_path_from_summary",
    "_bounded_dict",
    "_comparison",
    "_duration_ms",
    "_file_size",
    "_format_mib",
    "_format_top_contributors",
    "_median",
    "_npm_command",
    "_process_peak_bytes",
    "_read_json",
    "_record_row",
    "append_trend_record",
    "benchmark_duration_ms",
    "build_baseline_record",
    "compare_duration",
    "json",
    "load_benchmark_payload",
    "os",
    "parse_timeout_sequence",
    "summarize_artifacts",
    "write_baseline_record",
]


def parse_timeout_sequence(
    value: str | float | int | None, *, fallback: float = 3.0
) -> list[float]:
    text = str(value if value is not None else "").strip()
    if not text:
        return [float(fallback)]
    values: list[float] = []
    for part in text.split(","):
        token = part.strip()
        if not token:
            continue
        try:
            timeout_s = float(token)
        except ValueError:
            continue
        if timeout_s > 0:
            values.append(timeout_s)
    return values or [float(fallback)]


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


def _duration_ms(started_at: float, finished_at: float) -> int:
    return max(0, int(round((finished_at - started_at) * 1000)))


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


def _bounded_dict(row: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {key: row.get(key) for key in keys if key in row}


def _artifact_path_from_summary(section: dict[str, Any], key: str) -> Path | None:
    report = section.get("report") if isinstance(section.get("report"), dict) else {}
    artifacts = report.get("artifacts") if isinstance(report.get("artifacts"), dict) else {}
    token = str(artifacts.get(key) or "").strip()
    if not token and isinstance(section.get("scenario"), dict):
        details = (
            section["scenario"].get("details")
            if isinstance(section["scenario"].get("details"), dict)
            else {}
        )
        token = str(details.get(key) or "").strip()
    if not token:
        token = str(section.get(key) or "").strip()
    if not token:
        return None
    return Path(token).expanduser().resolve()


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
