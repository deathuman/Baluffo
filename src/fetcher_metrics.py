#!/usr/bin/env python3
"""Compute fetcher performance metrics from report/history artifacts."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any
from urllib.parse import urlparse

from src.jobs.common.contracts_provider_coverage import normalize_provider_coverage_payload
from src.jobs.common.contracts_provider_static_overlap import (
    normalize_provider_static_overlap_payload,
)
from src.jobs.common.contracts_redundant_static_proposals import (
    normalize_redundant_static_proposals_payload,
)
from src.jobs.common.contracts_source_health import normalize_source_health_payload
from src.jobs.common.contracts_static_suppression_policy import (
    normalize_static_suppression_policy_payload,
)
from src.shared.json_io import read_json
from src.shared.json_shapes import as_json_list, as_json_object, copy_json_object, json_object_rows
from src.shared.utils import parse_iso


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


def summarize_source_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = max(1, len(rows))
    failed = [row for row in rows if str(row.get("status") or "").strip().lower() == "error"]
    excluded = [row for row in rows if str(row.get("status") or "").strip().lower() == "excluded"]
    durations: list[tuple[str, int]] = []
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


def percentile(values: list[int], pct: float) -> int:
    if not values:
        return 0
    ordered = sorted(int(max(0, value)) for value in values)
    if len(ordered) == 1:
        return int(ordered[0])
    index = int(round((len(ordered) - 1) * max(0.0, min(1.0, float(pct)))))
    return int(ordered[index])


def summarize_run_history(rows: list[dict[str, Any]], window: int) -> dict[str, Any]:
    clean_rows = [
        row
        for row in rows
        if isinstance(row, dict) and str(row.get("type") or "").lower() == "fetch"
    ]
    clean_rows.sort(
        key=lambda row: parse_iso(row.get("finishedAt") or row.get("startedAt")) or datetime.min,
        reverse=True,
    )
    sampled = clean_rows[: max(1, int(window or 1))]
    durations: list[int] = []
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


def build_metrics(
    report: dict[str, Any], history: list[dict[str, Any]], window: int
) -> dict[str, Any]:
    summary = as_json_object(report.get("summary"))
    sources = json_object_rows(report.get("sources"))
    input_count = int(summary.get("inputCount") or 0)
    output_count = int(summary.get("outputCount") or 0)
    merged = int(summary.get("mergedCount") or 0)
    duplicate_rate = round((merged / input_count), 4) if input_count > 0 else 0.0
    output_yield_rate = round((output_count / input_count), 4) if input_count > 0 else 0.0
    runtime = as_json_object(report.get("runtime"))
    timing_summary = as_json_object(runtime.get("timingSummary"))
    durations = [max(0, int(row.get("durationMs") or 0)) for row in sources]
    source_health = normalize_source_health_payload(report.get("sourceHealth"), sources)
    provider_coverage = normalize_provider_coverage_payload(report.get("providerCoverage"))
    provider_static_overlap = normalize_provider_static_overlap_payload(
        report.get("providerStaticOverlap"), source_rows=sources
    )
    static_suppression_policy = normalize_static_suppression_policy_payload(
        report.get("staticSuppressionPolicy")
    )
    redundant_static_proposals = normalize_redundant_static_proposals_payload(
        report.get("redundantStaticProposals")
    )
    source_policy_recommendation_export = as_json_object(
        report.get("sourcePolicyRecommendationExport")
    )
    return {
        "generatedAt": datetime.now(UTC).isoformat(),
        "latestRun": {
            "startedAt": str(report.get("startedAt") or ""),
            "finishedAt": str(report.get("finishedAt") or ""),
            "durationMs": int(timing_summary.get("totalDurationMs") or sum(durations)),
            "inputCount": input_count,
            "outputCount": output_count,
            "mergedCount": merged,
            "duplicateRate": duplicate_rate,
            "outputYieldRate": output_yield_rate,
            "medianSourceDurationMs": int(
                timing_summary.get("medianSourceDurationMs") or percentile(durations, 0.5)
            ),
            "p95SourceDurationMs": int(
                timing_summary.get("p95SourceDurationMs") or percentile(durations, 0.95)
            ),
            "stageTotalsMs": copy_json_object(timing_summary.get("stageTotalsMs")),
            "stageTop": list(as_json_list(timing_summary.get("stageTop"))),
            "highCostLowYieldSources": list(
                as_json_list(timing_summary.get("highCostLowYieldSources"))
            ),
            "sourceHealth": source_health,
            "providerCoverage": provider_coverage,
            "providerStaticOverlap": provider_static_overlap,
            "staticSuppressionPolicy": static_suppression_policy,
            "redundantStaticProposals": redundant_static_proposals,
            "sourcePolicyRecommendationExport": source_policy_recommendation_export,
            **summarize_source_rows(sources),
        },
        "history": summarize_run_history(history, window=window),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize fetcher performance metrics.")
    parser.add_argument(
        "--data-dir", default="data", help="Directory containing fetcher artifacts."
    )
    parser.add_argument(
        "--window-runs", type=int, default=20, help="Number of recent fetch runs to include."
    )
    parser.add_argument("--output", default="", help="Optional output JSON file path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir)
    report_path = data_dir / "jobs-fetch-report.json"
    history_path = data_dir / "admin-run-history.json"
    report = read_json(report_path, {})
    history = read_json(history_path, [])
    metrics = build_metrics(
        as_json_object(report),
        json_object_rows(history),
        window=max(1, int(args.window_runs or 1)),
    )
    payload = json.dumps(metrics, indent=2, ensure_ascii=False)
    output = str(args.output or "").strip()
    if output:
        Path(output).write_text(payload, encoding="utf-8")
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
