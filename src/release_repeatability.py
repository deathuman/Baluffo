#!/usr/bin/env python3
"""Build a repeatability report from multiple fetch report snapshots."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from pathlib import Path
from statistics import median
from typing import Any

from src.shared.json_io import read_json
from src.shared.utils import int_or_default, now_iso


def safe_int(value: Any, default: int = 0) -> int:
    return int_or_default(value, default)


def safe_text(value: Any) -> str:
    return str(value or "").strip()


def summarize_run(path: Path) -> dict[str, Any]:
    report = read_json(path, {})
    if not isinstance(report, dict):
        report = {}
    runtime = report.get("runtime") if isinstance(report.get("runtime"), dict) else {}
    timing_summary = (
        runtime.get("timingSummary") if isinstance(runtime.get("timingSummary"), dict) else {}
    )
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    sources = [row for row in (report.get("sources") or []) if isinstance(row, dict)]
    failed_sources = [
        {
            "name": safe_text(row.get("name")) or "unknown",
            "adapter": safe_text(row.get("adapter")) or "custom",
            "error": safe_text(row.get("error")),
            "keptCount": safe_int(row.get("keptCount")),
        }
        for row in sources
        if safe_text(row.get("status")).lower() == "error"
    ]
    source_map = {
        safe_text(row.get("name")) or f"source-{index}": {
            "adapter": safe_text(row.get("adapter")) or "custom",
            "status": safe_text(row.get("status")).lower(),
            "keptCount": safe_int(row.get("keptCount")),
            "fetchedCount": safe_int(row.get("fetchedCount")),
            "durationMs": safe_int(row.get("durationMs")),
            "error": safe_text(row.get("error")),
        }
        for index, row in enumerate(sources)
    }
    return {
        "path": str(path),
        "label": path.stem,
        "startedAt": safe_text(report.get("startedAt")),
        "finishedAt": safe_text(report.get("finishedAt")),
        "outputCount": safe_int(summary.get("outputCount")),
        "failedSources": safe_int(summary.get("failedSources")),
        "wallClockDurationMs": safe_int(timing_summary.get("wallClockDurationMs")),
        "totalDurationMs": safe_int(timing_summary.get("totalDurationMs")),
        "selectedSourceCount": safe_int(runtime.get("selectedSourceCount")),
        "incrementalCacheEnabled": bool(runtime.get("incrementalCacheEnabled")),
        "forceRefreshAll": bool(runtime.get("forceRefreshAll")),
        "socialEnabled": bool(runtime.get("socialEnabled")),
        "failedSourceNames": [row["name"] for row in failed_sources],
        "failedSourcesDetail": failed_sources,
        "sources": source_map,
    }


def build_source_volatility(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    all_names = sorted({name for run in runs for name in run.get("sources", {})})
    rows: list[dict[str, Any]] = []
    for name in all_names:
        kept_counts: list[int] = []
        error_runs = 0
        nonzero_runs = 0
        adapters = set()
        statuses: list[str] = []
        errors: list[str] = []
        for run in runs:
            source = (run.get("sources") or {}).get(name) or {}
            kept = safe_int(source.get("keptCount"))
            status = safe_text(source.get("status")).lower() or "missing"
            error = safe_text(source.get("error"))
            kept_counts.append(kept)
            statuses.append(status)
            if kept > 0:
                nonzero_runs += 1
            if status == "error":
                error_runs += 1
            if error:
                errors.append(error)
            adapter = safe_text(source.get("adapter"))
            if adapter:
                adapters.add(adapter)
        min_kept = min(kept_counts) if kept_counts else 0
        max_kept = max(kept_counts) if kept_counts else 0
        swing = max_kept - min_kept
        zero_flip = min_kept == 0 and max_kept > 0
        if swing <= 0 and error_runs <= 0 and not zero_flip:
            continue
        rows.append(
            {
                "name": name,
                "adapter": sorted(adapters)[0] if adapters else "unknown",
                "minKeptCount": min_kept,
                "maxKeptCount": max_kept,
                "keptCountSwing": swing,
                "errorRuns": error_runs,
                "nonZeroRuns": nonzero_runs,
                "zeroToNonZeroFlip": zero_flip,
                "statuses": statuses,
                "errors": sorted(set(error for error in errors if error))[:5],
            }
        )
    rows.sort(
        key=lambda row: (
            safe_int(row.get("keptCountSwing")),
            safe_int(row.get("errorRuns")),
            1 if row.get("zeroToNonZeroFlip") else 0,
        ),
        reverse=True,
    )
    return rows


def build_gate(runs: list[dict[str, Any]], release_floor: int) -> dict[str, Any]:
    output_counts = [safe_int(run.get("outputCount")) for run in runs]
    failed_counts = [safe_int(run.get("failedSources")) for run in runs]
    social_nonzero = any(
        safe_int(((run.get("sources") or {}).get("social_mastodon") or {}).get("keptCount")) > 0
        or safe_int(((run.get("sources") or {}).get("social_reddit") or {}).get("keptCount")) > 0
        for run in runs
    )
    return {
        "releaseFloor": release_floor,
        "minOutputCount": min(output_counts) if output_counts else 0,
        "maxOutputCount": max(output_counts) if output_counts else 0,
        "medianOutputCount": int(median(output_counts)) if output_counts else 0,
        "outputSwing": (max(output_counts) - min(output_counts)) if output_counts else 0,
        "maxFailedSources": max(failed_counts) if failed_counts else 0,
        "allRunsForcedFullRefresh": all(bool(run.get("forceRefreshAll")) for run in runs),
        "allRunsSocialEnabled": all(bool(run.get("socialEnabled")) for run in runs),
        "socialContributedInAtLeastOneRun": social_nonzero,
        "passesReleaseFloor": bool(output_counts) and min(output_counts) >= release_floor,
        "passesNoTopLevelFailures": bool(failed_counts) and max(failed_counts) == 0,
    }


def build_report(report_paths: Iterable[Path], release_floor: int) -> dict[str, Any]:
    runs = [summarize_run(path) for path in report_paths]
    output_counts = [safe_int(run.get("outputCount")) for run in runs]
    failed_counts = [safe_int(run.get("failedSources")) for run in runs]
    wall_clock = [safe_int(run.get("wallClockDurationMs")) for run in runs]
    volatility = build_source_volatility(runs)
    return {
        "generatedAt": now_iso(),
        "runCount": len(runs),
        "runs": runs,
        "totals": {
            "minOutputCount": min(output_counts) if output_counts else 0,
            "maxOutputCount": max(output_counts) if output_counts else 0,
            "medianOutputCount": int(median(output_counts)) if output_counts else 0,
            "outputSwing": (max(output_counts) - min(output_counts)) if output_counts else 0,
            "minFailedSources": min(failed_counts) if failed_counts else 0,
            "maxFailedSources": max(failed_counts) if failed_counts else 0,
            "minWallClockDurationMs": min(wall_clock) if wall_clock else 0,
            "maxWallClockDurationMs": max(wall_clock) if wall_clock else 0,
        },
        "gate": build_gate(runs, release_floor),
        "volatileSources": volatility[:25],
        "recommendations": build_recommendations(runs, volatility, release_floor),
    }


def build_recommendations(
    runs: list[dict[str, Any]], volatility: list[dict[str, Any]], release_floor: int
) -> list[str]:
    items: list[str] = []
    gate = build_gate(runs, release_floor)
    if not gate.get("passesReleaseFloor"):
        items.append(
            f"Do not use a single high-water run as the release gate; the minimum repeated output ({safe_int(gate.get('minOutputCount'))}) is below the release floor ({release_floor})."
        )
    if not gate.get("passesNoTopLevelFailures"):
        items.append(
            "Treat top-level source failures as a repeatability blocker until the maximum repeated run failure count returns to zero."
        )
    if volatility:
        top = volatility[0]
        items.append(
            f"Stabilize `{safe_text(top.get('name'))}` first; it has the largest kept-job swing ({safe_int(top.get('keptCountSwing'))}) across repeated full-refresh runs."
        )
    if any("gracklehq" == safe_text(row.get("name")) for row in volatility):
        items.append(
            "Add a preflight or monitored-volatility rule for `gracklehq` so one transient network miss does not dominate release acceptance."
        )
    items.append(
        "Use the repeatability report together with the single-run audit: one shows correctness of a run, the other shows whether the baseline is stable enough to release."
    )
    return items[:5]


def render_markdown(report: dict[str, Any]) -> str:
    totals = report.get("totals") if isinstance(report.get("totals"), dict) else {}
    gate = report.get("gate") if isinstance(report.get("gate"), dict) else {}
    lines = [
        "# Release Repeatability Report",
        "",
        f"Generated: {safe_text(report.get('generatedAt'))}",
        "",
        "## Executive summary",
        f"- Runs analyzed: {safe_int(report.get('runCount'))}",
        f"- Output range: {safe_int(totals.get('minOutputCount'))} to {safe_int(totals.get('maxOutputCount'))}",
        f"- Output swing: {safe_int(totals.get('outputSwing'))}",
        f"- Failed source range: {safe_int(totals.get('minFailedSources'))} to {safe_int(totals.get('maxFailedSources'))}",
        f"- Release floor: {safe_int(gate.get('releaseFloor'))}",
        f"- Passes release floor on every run: {bool(gate.get('passesReleaseFloor'))}",
        f"- Passes zero top-level failures on every run: {bool(gate.get('passesNoTopLevelFailures'))}",
        "",
        "## Runs",
    ]
    for run in report.get("runs") or []:
        lines.append(
            f"- `{safe_text(run.get('label'))}`"
            f": output={safe_int(run.get('outputCount'))}, failed={safe_int(run.get('failedSources'))},"
            f" wallClock={safe_int(run.get('wallClockDurationMs'))} ms, social={bool(run.get('socialEnabled'))}"
        )
    lines.append("")
    lines.append("## Volatile sources")
    volatile = report.get("volatileSources") or []
    if not volatile:
        lines.append("- None")
    else:
        for row in volatile[:10]:
            lines.append(
                f"- `{safe_text(row.get('name'))}` ({safe_text(row.get('adapter'))})"
                f": kept swing={safe_int(row.get('keptCountSwing'))},"
                f" min/max={safe_int(row.get('minKeptCount'))}/{safe_int(row.get('maxKeptCount'))},"
                f" errorRuns={safe_int(row.get('errorRuns'))},"
                f" zeroFlip={bool(row.get('zeroToNonZeroFlip'))}"
            )
    lines.append("")
    lines.append("## Recommendations")
    for item in report.get("recommendations") or []:
        lines.append(f"- {safe_text(item)}")
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a release repeatability report from multiple jobs-fetch reports."
    )
    parser.add_argument(
        "--reports", nargs="+", required=True, help="One or more jobs-fetch-report.json paths."
    )
    parser.add_argument(
        "--release-floor",
        type=int,
        default=34131,
        help="Release floor for repeated full-refresh runs.",
    )
    parser.add_argument(
        "--output-json", default="", help="Optional output path for the JSON repeatability report."
    )
    parser.add_argument(
        "--output-md",
        default="",
        help="Optional output path for the Markdown repeatability report.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report_paths = [Path(item).resolve() for item in args.reports]
    report = build_report(report_paths, args.release_floor)
    default_root = report_paths[0].parent if report_paths else Path("data").resolve()
    json_output = (
        Path(args.output_json).resolve()
        if safe_text(args.output_json)
        else default_root / "release-repeatability-report.json"
    )
    md_output = (
        Path(args.output_md).resolve()
        if safe_text(args.output_md)
        else default_root / "release-repeatability-report.md"
    )
    json_output.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    md_output.write_text(render_markdown(report), encoding="utf-8")
    print(str(json_output))
    print(str(md_output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
