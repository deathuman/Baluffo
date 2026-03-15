#!/usr/bin/env python3
"""Compute fetcher performance metrics from report/history artifacts."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse


def parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def read_json(path: Path, fallback: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback


def sanitize_source_label(value: Any, *, max_len: int = 64) -> str:
    text = str(value or "")
    lower = text.lower()
    if lower.startswith("static_source::static:listing_url:"):
        raw_url = text.split("static_source::static:listing_url:", 1)[1].strip()
        parsed = urlparse(raw_url)
        host = (parsed.netloc or "").strip().lower()
        path = (parsed.path or "").strip()
        if host:
            text = f"static:{host}{path}"
        else:
            text = "static-source"
    elif lower.startswith("static_source::auto:"):
        text = "static-source"
    elif lower.startswith("static_source::"):
        text = text.split("static_source::", 1)[1].strip() or "static-source"
    clean = "".join(ch for ch in text if ch.isprintable())
    clean = " ".join(clean.split())
    if not clean:
        return "unknown-source"
    if len(clean) > max_len:
        return f"{clean[: max_len - 3].rstrip()}..."
    return clean


def summarize_source_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = max(1, len(rows))
    failed = [row for row in rows if str(row.get("status") or "").strip().lower() == "error"]
    excluded = [row for row in rows if str(row.get("status") or "").strip().lower() == "excluded"]
    durations: List[Tuple[str, int]] = []
    for row in rows:
        try:
            ms = int(row.get("durationMs") or 0)
        except (TypeError, ValueError):
            ms = 0
        durations.append((sanitize_source_label(row.get("name")), max(0, ms)))
    durations.sort(key=lambda item: item[1], reverse=True)
    return {
        "sourceCount": len(rows),
        "successfulSources": max(0, len(rows) - len(failed) - len(excluded)),
        "failedSources": len(failed),
        "excludedSources": len(excluded),
        "sourceFailureRate": round(len(failed) / total, 4),
        "slowestSources": [{"name": name, "durationMs": ms} for name, ms in durations[:5] if name],
    }


def summarize_run_history(rows: List[Dict[str, Any]], window: int) -> Dict[str, Any]:
    clean_rows = [row for row in rows if isinstance(row, dict) and str(row.get("type") or "").lower() == "fetch"]
    clean_rows.sort(
        key=lambda row: parse_iso(row.get("finishedAt") or row.get("startedAt")) or datetime.min,
        reverse=True,
    )
    sampled = clean_rows[: max(1, int(window or 1))]
    durations: List[int] = []
    for row in sampled:
        try:
            durations.append(max(0, int(row.get("durationMs") or 0)))
        except (TypeError, ValueError):
            continue
    if not durations:
        return {
            "windowRuns": 0,
            "medianDurationMs": 0,
            "averageDurationMs": 0,
            "latestDurationMs": 0,
        }
    return {
        "windowRuns": len(durations),
        "medianDurationMs": int(median(durations)),
        "averageDurationMs": int(sum(durations) / max(1, len(durations))),
        "latestDurationMs": int(durations[0]),
    }


def build_metrics(report: Dict[str, Any], history: List[Dict[str, Any]], window: int) -> Dict[str, Any]:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    sources = report.get("sources") if isinstance(report.get("sources"), list) else []
    input_count = int(summary.get("inputCount") or 0)
    output_count = int(summary.get("outputCount") or 0)
    merged = int(summary.get("mergedCount") or 0)
    duplicate_rate = round((merged / input_count), 4) if input_count > 0 else 0.0
    output_yield_rate = round((output_count / input_count), 4) if input_count > 0 else 0.0
    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "latestRun": {
            "startedAt": str(report.get("startedAt") or ""),
            "finishedAt": str(report.get("finishedAt") or ""),
            "inputCount": input_count,
            "outputCount": output_count,
            "mergedCount": merged,
            "duplicateRate": duplicate_rate,
            "outputYieldRate": output_yield_rate,
            **summarize_source_rows([row for row in sources if isinstance(row, dict)]),
        },
        "history": summarize_run_history(history, window=window),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize fetcher performance metrics.")
    parser.add_argument("--data-dir", default="data", help="Directory containing fetcher artifacts.")
    parser.add_argument("--window-runs", type=int, default=20, help="Number of recent fetch runs to include.")
    parser.add_argument("--output", default="", help="Optional output JSON file path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir)
    report_path = data_dir / "jobs-fetch-report.json"
    history_path = data_dir / "admin-run-history.json"
    report = read_json(report_path, {})
    history = read_json(history_path, [])
    if not isinstance(report, dict):
        report = {}
    if not isinstance(history, list):
        history = []
    metrics = build_metrics(report, history, window=max(1, int(args.window_runs or 1)))
    payload = json.dumps(metrics, indent=2, ensure_ascii=False)
    output = str(args.output or "").strip()
    if output:
        Path(output).write_text(payload, encoding="utf-8")
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
