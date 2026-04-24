#!/usr/bin/env python3
"""Build a combined discovery/fetch audit report from live data artifacts."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from src.shared.json_io import read_json
from src.shared.json_shapes import as_json_object, json_object_rows
from src.shared.utils import int_or_default, now_iso

HIGH_COST_LOW_YIELD_MS = 20_000
LOW_YIELD_FETCHED_MIN = 20
LOW_YIELD_RATIO_MAX = 0.1
SOFT_FAILURE_CLASSIFICATIONS = {
    "blocked_or_challenge",
    "anti_bot_or_challenge",
    "js_required",
    "site_changed",
    "empty_confirmed",
    "needs_review",
    "fetch_ok_extract_zero",
    "ok_no_jobs",
    "parse_error",
    "parser_stale",
    "dead_listing_page",
    "browser_timeout",
    "browser_retry_not_recommended",
    "rate_limited",
}


def safe_int(value: Any, default: int = 0) -> int:
    return int_or_default(value, default)


def safe_text(value: Any) -> str:
    return str(value or "").strip()


def ratio_pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((float(numerator) / float(denominator)) * 100.0, 2)


def summarize_discovery(report: dict[str, Any]) -> dict[str, Any]:
    summary = as_json_object(report.get("summary"))
    runtime = as_json_object(report.get("runtime"))
    loss_accounting = as_json_object(summary.get("lossAccounting"))
    failures = json_object_rows(report.get("failures"))
    candidates = json_object_rows(report.get("candidates"))
    adapter_timings = json_object_rows(runtime.get("adapterTimings"))
    stage_top = json_object_rows(runtime.get("stageTop"))
    top_failures = json_object_rows(report.get("topFailures"))
    failed_sources = [
        {
            "name": safe_text(row.get("name")) or safe_text(row.get("domain")) or "unknown",
            "adapter": safe_text(row.get("adapter")) or "unknown",
            "stage": safe_text(row.get("stage")),
            "error": safe_text(row.get("error")),
        }
        for row in failures[:25]
    ]
    return {
        "startedAt": safe_text(report.get("startedAt")),
        "finishedAt": safe_text(report.get("finishedAt")),
        "totalDurationMs": safe_int(runtime.get("totalDurationMs")),
        "queuedCandidateCount": safe_int(summary.get("queuedCandidateCount")),
        "failedProbeCount": safe_int(summary.get("failedProbeCount")),
        "probeMissCount": safe_int(summary.get("probeMissCount")),
        "discoverableButDeferredCount": safe_int(summary.get("discoverableButDeferredCount")),
        "queueFilteredCount": safe_int(loss_accounting.get("queueFiltered")),
        "candidateCount": len(candidates),
        "failureCount": len(failures),
        "stageTop": stage_top[:5],
        "slowestAdapters": adapter_timings[:5],
        "topFailures": top_failures[:5],
        "failedSources": failed_sources,
    }


def infer_detail_duration_ms(detail: dict[str, Any]) -> int:
    if safe_int(detail.get("durationMs")) > 0:
        return safe_int(detail.get("durationMs"))
    stats = as_json_object(detail.get("stats"))
    timing_keys = (
        "listing_fetch_ms",
        "candidate_extraction_ms",
        "detail_fetch_ms",
        "parse_csv_ms",
        "redirect_resolve_ms",
        "canonicalize_ms",
    )
    return sum(max(0, safe_int(stats.get(key))) for key in timing_keys)


def flatten_fetch_detail_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source_row in json_object_rows(report.get("sources")):
        for detail in json_object_rows(source_row.get("details")):
            fetched = max(0, safe_int(detail.get("fetchedCount")))
            kept = max(0, safe_int(detail.get("keptCount")))
            rows.append(
                {
                    "sourceName": safe_text(source_row.get("name")) or "unknown",
                    "name": safe_text(detail.get("name"))
                    or safe_text(detail.get("studio"))
                    or "unknown",
                    "adapter": safe_text(detail.get("adapter"))
                    or safe_text(source_row.get("adapter"))
                    or "custom",
                    "studio": safe_text(detail.get("studio")),
                    "status": safe_text(detail.get("status")).lower(),
                    "classification": safe_text(detail.get("classification")).lower(),
                    "durationMs": infer_detail_duration_ms(detail),
                    "fetchedCount": fetched,
                    "keptCount": kept,
                    "yieldPct": ratio_pct(kept, fetched),
                    "error": safe_text(detail.get("error")),
                }
            )
    rows.sort(key=lambda row: int(row.get("durationMs") or 0), reverse=True)
    return rows


def summarize_fetch(report: dict[str, Any], jobs: list[dict[str, Any]]) -> dict[str, Any]:
    summary = as_json_object(report.get("summary"))
    runtime = as_json_object(report.get("runtime"))
    timing_summary = as_json_object(runtime.get("timingSummary"))
    outputs = as_json_object(report.get("outputs"))
    report_output_path = Path(str(outputs.get("report") or ""))
    sources = json_object_rows(report.get("sources"))
    detail_rows = flatten_fetch_detail_rows(report)
    failed_sources = [
        {
            "name": safe_text(row.get("name")) or "unknown",
            "adapter": safe_text(row.get("adapter")) or "custom",
            "durationMs": safe_int(row.get("durationMs")),
            "error": safe_text(row.get("error")),
        }
        for row in sources
        if safe_text(row.get("status")).lower() == "error"
    ]
    productive_expensive = [
        {
            "name": safe_text(row.get("name")) or "unknown",
            "adapter": safe_text(row.get("adapter")) or "custom",
            "durationMs": safe_int(row.get("durationMs")),
            "keptCount": safe_int(row.get("keptCount")),
            "fetchedCount": safe_int(row.get("fetchedCount")),
        }
        for row in sources
        if safe_int(row.get("durationMs")) >= HIGH_COST_LOW_YIELD_MS
        and safe_int(row.get("keptCount")) > 1
    ]
    productive_expensive.sort(key=lambda row: safe_int(row.get("durationMs")), reverse=True)
    return {
        "startedAt": safe_text(report.get("startedAt")),
        "finishedAt": safe_text(report.get("finishedAt")),
        "totalDurationMs": safe_int(timing_summary.get("totalDurationMs")),
        "wallClockDurationMs": safe_int(timing_summary.get("wallClockDurationMs")),
        "totalJobs": len(jobs),
        "sourceCount": len(sources),
        "successfulSources": safe_int(summary.get("successfulSources")),
        "failedSourcesCount": safe_int(summary.get("failedSources")),
        "outputCount": safe_int(summary.get("outputCount")),
        "browserFallbackQueueCount": len(
            read_json(
                report_output_path.parent / "jobs-browser-fallback-queue.json",
                [],
            )
        ),
        "siteChangedDiagnosedCount": sum(
            1 for row in sources if safe_text(row.get("failureBucket")).lower() == "site_changed"
        ),
        "siteChangedMissingOldUrlCount": sum(
            1
            for row in sources
            if safe_text(row.get("failureBucket")).lower() == "site_changed"
            and not (
                safe_text(row.get("listingUrl"))
                or (
                    isinstance(row.get("pages"), list)
                    and any(safe_text(page) for page in row.get("pages") or [])
                )
                or (
                    isinstance(row.get("details"), list)
                    and any(
                        isinstance(item.get("pages"), list)
                        and any(safe_text(page) for page in item.get("pages") or [])
                        for item in json_object_rows(row.get("details"))
                    )
                )
                or safe_text(row.get("providerUrl"))
            )
        ),
        "parserRegressionQueueCount": len(
            read_json(
                report_output_path.parent / "jobs-parser-regression-queue.json",
                [],
            )
        ),
        "stageTop": json_object_rows(timing_summary.get("stageTop"))[:5],
        "slowestAdapters": json_object_rows(timing_summary.get("slowestAdapters"))[:5],
        "slowestSourceLoaders": json_object_rows(runtime.get("slowestSources"))[:10],
        "slowestSourceEntries": detail_rows[:10],
        "highCostLowYieldSources": json_object_rows(timing_summary.get("highCostLowYieldSources"))[
            :10
        ],
        "detailHeavySources": json_object_rows(timing_summary.get("detailHeavySources"))[:10],
        "productiveExpensiveSources": productive_expensive[:10],
        "failedSources": failed_sources[:25],
        "detailRows": detail_rows,
    }


def issue_key(row: dict[str, Any]) -> tuple[str, str]:
    return (
        safe_text(row.get("sourceName") or row.get("name")),
        safe_text(row.get("classification") or row.get("error") or row.get("category")),
    )


def unique_issue_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    unique_rows: list[dict[str, Any]] = []
    for row in rows:
        key = issue_key(row)
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(row)
    return unique_rows


def build_issues(
    discovery: dict[str, Any],
    fetch: dict[str, Any],
    raw_fetch_report: dict[str, Any],
    raw_discovery_report: dict[str, Any],
) -> dict[str, Any]:
    hard_failures: list[dict[str, Any]] = []
    soft_failures: list[dict[str, Any]] = []
    high_cost_low_yield: list[dict[str, Any]] = []
    coverage_risks: list[dict[str, Any]] = []

    for row in json_object_rows(raw_fetch_report.get("sources")):
        source_name = safe_text(row.get("name")) or "unknown"
        adapter = safe_text(row.get("adapter")) or "custom"
        status = safe_text(row.get("status")).lower()
        fetched = max(0, safe_int(row.get("fetchedCount")))
        kept = max(0, safe_int(row.get("keptCount")))
        duration_ms = max(0, safe_int(row.get("durationMs")))
        classification = safe_text(row.get("classification")).lower()
        base = {
            "sourceName": source_name,
            "name": source_name,
            "adapter": adapter,
            "durationMs": duration_ms,
            "fetchedCount": fetched,
            "keptCount": kept,
            "yieldPct": ratio_pct(kept, fetched),
            "classification": classification,
            "error": safe_text(row.get("error")),
        }
        if status == "error":
            hard_failures.append({**base, "category": "fetch_source_error"})
        if classification in SOFT_FAILURE_CLASSIFICATIONS:
            soft_failures.append({**base, "category": classification})
        if duration_ms >= HIGH_COST_LOW_YIELD_MS and kept <= 1:
            high_cost_low_yield.append({**base, "category": "slow_low_yield"})
        if (
            fetched >= LOW_YIELD_FETCHED_MIN
            and fetched > 0
            and (float(kept) / float(fetched)) <= LOW_YIELD_RATIO_MAX
        ):
            coverage_risks.append({**base, "category": "low_yield"})

    for row in json_object_rows(fetch.get("detailRows")):
        status = safe_text(row.get("status")).lower()
        classification = safe_text(row.get("classification")).lower()
        fetched = max(0, safe_int(row.get("fetchedCount")))
        kept = max(0, safe_int(row.get("keptCount")))
        duration_ms = max(0, safe_int(row.get("durationMs")))
        base = dict(row)
        if status == "error":
            hard_failures.append({**base, "category": "fetch_detail_error"})
        if classification in SOFT_FAILURE_CLASSIFICATIONS:
            soft_failures.append({**base, "category": classification})
        if duration_ms >= HIGH_COST_LOW_YIELD_MS and kept <= 1:
            high_cost_low_yield.append({**base, "category": "slow_low_yield"})
        if (
            fetched >= LOW_YIELD_FETCHED_MIN
            and fetched > 0
            and (float(kept) / float(fetched)) <= LOW_YIELD_RATIO_MAX
        ):
            coverage_risks.append({**base, "category": "low_yield"})

    for row in json_object_rows(raw_discovery_report.get("failures")):
        stage = safe_text(row.get("dropStage") or row.get("stage"))
        item = {
            "sourceName": safe_text(row.get("name")) or safe_text(row.get("domain")) or "unknown",
            "name": safe_text(row.get("name")) or safe_text(row.get("domain")) or "unknown",
            "adapter": safe_text(row.get("adapter")) or "unknown",
            "stage": stage,
            "error": safe_text(row.get("error")),
            "category": stage or "discovery_failure",
        }
        if stage == "probe_failed":
            hard_failures.append(item)
        elif stage in {"queue_filtered", "deferred_by_cap", "low_evidence_skipped"}:
            coverage_risks.append(item)
        else:
            soft_failures.append(item)

    if safe_int(discovery.get("queueFilteredCount")) > 0:
        coverage_risks.append(
            {
                "sourceName": "discovery",
                "name": "discovery queue filtered candidates",
                "adapter": "discovery",
                "count": safe_int(discovery.get("queueFilteredCount")),
                "category": "queue_filtered",
            }
        )
    if safe_int(discovery.get("discoverableButDeferredCount")) > 0:
        coverage_risks.append(
            {
                "sourceName": "discovery",
                "name": "discovery deferred candidates",
                "adapter": "discovery",
                "count": safe_int(discovery.get("discoverableButDeferredCount")),
                "category": "deferred_by_cap",
            }
        )

    return {
        "hard_failures": unique_issue_rows(hard_failures)[:25],
        "soft_failures": unique_issue_rows(soft_failures)[:25],
        "high_cost_low_yield": unique_issue_rows(high_cost_low_yield)[:25],
        "coverage_risks": unique_issue_rows(coverage_risks)[:25],
    }


def build_recommendations(report: dict[str, Any]) -> list[str]:
    recommendations: list[str] = []
    fetch = as_json_object(report.get("fetch"))
    discovery = as_json_object(report.get("discovery"))
    issues = as_json_object(report.get("issues"))
    slow_adapters = json_object_rows(fetch.get("slowestAdapters"))
    slow_adapter = slow_adapters[0] if slow_adapters else {}
    if safe_text(slow_adapter.get("adapter")):
        recommendations.append(
            f"Investigate {safe_text(slow_adapter.get('adapter'))} first for fetch-time wins; it is the slowest adapter family in the baseline."
        )
    if (
        safe_int(discovery.get("discoverableButDeferredCount")) > 0
        or safe_int(discovery.get("queueFilteredCount")) > 0
    ):
        recommendations.append(
            "Review discovery deferred and queue-filtered candidates next; they are immediate coverage expansion opportunities."
        )
    if issues.get("high_cost_low_yield") or []:
        recommendations.append(
            "Prioritize the high-cost/low-yield sources for parser fixes, time-budget tightening, or source deactivation."
        )
    if issues.get("hard_failures") or []:
        recommendations.append(
            "Triage hard failures before adding new sources so the baseline reliability does not regress further."
        )
    return recommendations[:5]


def build_report(
    discovery_report: dict[str, Any], fetch_report: dict[str, Any], jobs: list[dict[str, Any]]
) -> dict[str, Any]:
    discovery = summarize_discovery(discovery_report)
    fetch = summarize_fetch(fetch_report, jobs)
    issues = build_issues(discovery, fetch, fetch_report, discovery_report)
    top_level_failed_sources = len(fetch.get("failedSources") or [])
    top_level_high_cost_low_yield = len(fetch.get("highCostLowYieldSources") or [])
    report = {
        "generatedAt": now_iso(),
        "discovery": discovery,
        "fetch": {key: value for key, value in fetch.items() if key != "detailRows"},
        "totals": {
            "totalJobs": safe_int(fetch.get("totalJobs")),
            "discoveryDurationMs": safe_int(discovery.get("totalDurationMs")),
            "fetchDurationMs": safe_int(fetch.get("totalDurationMs")),
            "fetchWallClockDurationMs": safe_int(fetch.get("wallClockDurationMs")),
            "failedSources": top_level_failed_sources,
            "topLevelFailedSources": top_level_failed_sources,
            "topLevelHighCostLowYieldSources": top_level_high_cost_low_yield,
            "issueInventoryHardFailures": len(issues.get("hard_failures") or []),
            "issueInventorySoftFailures": len(issues.get("soft_failures") or []),
            "issueInventoryHighCostLowYield": len(issues.get("high_cost_low_yield") or []),
            "issueInventoryCoverageRisks": len(issues.get("coverage_risks") or []),
            "notProperlyActingSources": sum(
                len(issues.get(key) or [])
                for key in ("hard_failures", "soft_failures", "high_cost_low_yield")
            ),
        },
        "issues": issues,
    }
    report["recommendations"] = build_recommendations(report)
    return report


def render_markdown(report: dict[str, Any]) -> str:
    discovery = as_json_object(report.get("discovery"))
    fetch = as_json_object(report.get("fetch"))
    issues = as_json_object(report.get("issues"))
    totals = as_json_object(report.get("totals"))
    lines = [
        "# Pipeline Audit Report",
        "",
        f"Generated: {safe_text(report.get('generatedAt'))}",
        "",
        "## Executive summary",
        f"- Discovery duration: {safe_int(discovery.get('totalDurationMs'))} ms",
        f"- Fetch duration: {safe_int(fetch.get('totalDurationMs'))} ms",
        f"- Fetch wall-clock: {safe_int(fetch.get('wallClockDurationMs'))} ms",
        f"- Total jobs: {safe_int(totals.get('totalJobs'))}",
        f"- Top-level failed fetch sources: {safe_int(totals.get('topLevelFailedSources'))}",
        f"- Top-level high-cost/low-yield sources: {safe_int(totals.get('topLevelHighCostLowYieldSources'))}",
        f"- Issue inventory hard failures: {safe_int(totals.get('issueInventoryHardFailures'))}",
        f"- Issue inventory soft failures: {safe_int(totals.get('issueInventorySoftFailures'))}",
        f"- Issue inventory high-cost/low-yield: {safe_int(totals.get('issueInventoryHighCostLowYield'))}",
        f"- Issue inventory coverage risks: {safe_int(totals.get('issueInventoryCoverageRisks'))}",
        "",
        "## Slowest areas",
    ]
    for row in json_object_rows(discovery.get("stageTop"))[:5]:
        lines.append(
            f"- Discovery stage `{safe_text(row.get('stage'))}`: {safe_int(row.get('durationMs'))} ms"
        )
    for row in json_object_rows(fetch.get("slowestAdapters"))[:5]:
        lines.append(
            f"- Fetch adapter `{safe_text(row.get('adapter'))}`: {safe_int(row.get('durationMs'))} ms across {safe_int(row.get('sourceCount'))} source(s)"
        )
    for row in json_object_rows(fetch.get("slowestSourceLoaders"))[:5]:
        lines.append(
            f"- Loader `{safe_text(row.get('name'))}` ({safe_text(row.get('adapter'))}): {safe_int(row.get('durationMs'))} ms, kept {safe_int(row.get('keptCount'))}"
        )
    for row in json_object_rows(fetch.get("slowestSourceEntries"))[:5]:
        lines.append(
            f"- Source entry `{safe_text(row.get('name'))}` via `{safe_text(row.get('sourceName'))}`: {safe_int(row.get('durationMs'))} ms, kept {safe_int(row.get('keptCount'))}/{safe_int(row.get('fetchedCount'))}"
        )
    if fetch.get("productiveExpensiveSources"):
        lines.append("")
        lines.append("## Productive but expensive sources")
        for row in json_object_rows(fetch.get("productiveExpensiveSources"))[:5]:
            lines.append(
                f"- `{safe_text(row.get('name'))}` ({safe_text(row.get('adapter'))})"
                f": {safe_int(row.get('durationMs'))} ms, kept {safe_int(row.get('keptCount'))}/{safe_int(row.get('fetchedCount'))}"
            )
    if fetch.get("detailHeavySources"):
        lines.append("")
        lines.append("## Detail-fetch dominated sources")
        for row in json_object_rows(fetch.get("detailHeavySources"))[:5]:
            lines.append(
                f"- `{safe_text(row.get('name'))}` ({safe_text(row.get('adapter'))})"
                f": detailFetch={safe_int(row.get('detailFetchMs'))} ms, total={safe_int(row.get('durationMs'))} ms, kept {safe_int(row.get('keptCount'))}"
            )
    lines.append("")
    lines.append("## Failed or not properly acting sources")
    for section_name in ("hard_failures", "soft_failures", "high_cost_low_yield", "coverage_risks"):
        lines.append(f"### {section_name.replace('_', ' ').title()}")
        rows = json_object_rows(issues.get(section_name))
        if not rows:
            lines.append("- None")
            continue
        for row in rows[:10]:
            lines.append(
                f"- `{safe_text(row.get('name') or row.get('sourceName'))}` ({safe_text(row.get('adapter'))})"
                f" | duration={safe_int(row.get('durationMs'))} ms"
                f" | kept={safe_int(row.get('keptCount'))}/{safe_int(row.get('fetchedCount'))}"
                f" | category={safe_text(row.get('category'))}"
                f" | error={safe_text(row.get('error'))}"
            )
    lines.append("")
    lines.append("## Recommendations")
    for item in report.get("recommendations") or []:
        lines.append(f"- {safe_text(item)}")
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a combined discovery + fetch audit report.")
    parser.add_argument(
        "--data-dir", default="data", help="Directory containing discovery/fetch artifacts."
    )
    parser.add_argument(
        "--output-json", default="", help="Optional output path for the JSON audit report."
    )
    parser.add_argument(
        "--output-md", default="", help="Optional output path for the Markdown audit report."
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir).resolve()
    discovery_report = read_json(data_dir / "source-discovery-report.json", {})
    fetch_report = read_json(data_dir / "jobs-fetch-report.json", {})
    jobs = read_json(data_dir / "jobs-unified.json", [])
    report = build_report(
        as_json_object(discovery_report),
        as_json_object(fetch_report),
        json_object_rows(jobs),
    )
    json_output = (
        Path(args.output_json).resolve()
        if safe_text(args.output_json)
        else data_dir / "pipeline-audit-report.json"
    )
    md_output = (
        Path(args.output_md).resolve()
        if safe_text(args.output_md)
        else data_dir / "pipeline-audit-report.md"
    )
    json_output.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    md_output.write_text(render_markdown(report), encoding="utf-8")
    print(str(json_output))
    print(str(md_output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
