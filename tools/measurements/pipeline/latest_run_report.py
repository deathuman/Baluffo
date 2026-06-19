#!/usr/bin/env python3
"""Summarize the latest discovery and fetch run artifacts.

The tool is intentionally thin: it resolves the latest local report files,
loads them, and reuses :mod:`src.pipeline_audit` for the combined summary
contract. The rendered output is concise so engineers and AI coders can triage
the latest run without opening the raw JSON artifacts.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src import pipeline_audit as audit

DISCOVERY_REPORT_NAME = "source-discovery-report.json"
FETCH_REPORT_NAME = "jobs-fetch-report.json"
JOBS_UNIFIED_NAME = "jobs-unified.json"
PARSER_REGRESSION_QUEUE_NAME = "jobs-parser-regression-queue.json"
BROWSER_FALLBACK_QUEUE_NAME = "jobs-browser-fallback-queue.json"
_EXPECTED_CLI_FAILURES = (OSError, TypeError, ValueError)


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise FileNotFoundError(f"Missing report file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in report file: {path}") from exc


def _read_json_list(path: Path) -> list[dict[str, Any]]:
    try:
        payload = _read_json(path)
    except FileNotFoundError:
        return []
    except ValueError:
        return []
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def _candidate_roots(repo_root: Path) -> list[Path]:
    roots: list[Path] = []
    for candidate in (repo_root / "data", repo_root / "_out" / "latest"):
        if candidate.is_dir():
            roots.append(candidate)
    runs_root = repo_root / "_out" / "runs"
    if runs_root.is_dir():
        roots.extend(path for path in runs_root.iterdir() if path.is_dir())
    return roots


def _resolve_latest_artifact_path(repo_root: Path, filename: str) -> Path:
    candidates: list[Path] = []
    for root in _candidate_roots(repo_root):
        candidate = root / filename
        if candidate.is_file():
            candidates.append(candidate)
    if not candidates:
        candidate_roots = ", ".join(str(path) for path in _candidate_roots(repo_root))
        raise FileNotFoundError(
            f"Could not find {filename} under the latest report roots: {candidate_roots or repo_root}"
        )

    def _sort_key(path: Path) -> tuple[int, int, str]:
        stat = path.stat()
        return (int(stat.st_mtime_ns), len(str(path)), str(path))

    return max(candidates, key=_sort_key)


def _report_label(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _display_report_path(path_text: Any) -> str:
    text = str(path_text or "").strip()
    if not text:
        return "(missing)"
    return _report_label(Path(text))


def _preview_rows(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    preview: list[dict[str, Any]] = []
    for row in rows[: max(0, int(limit or 0))]:
        preview.append(
            {
                "source": str(row.get("source") or ""),
                "oldUrl": str(row.get("oldUrl") or ""),
                "currentUrl": str(row.get("currentUrl") or ""),
                "lastStatus": str(row.get("lastStatus") or ""),
                "listingFingerprintChanged": bool(row.get("listingFingerprintChanged")),
                "adapter": str(row.get("adapter") or ""),
            }
        )
    return preview


def _empty_discovery_report() -> dict[str, Any]:
    return {
        "startedAt": "",
        "finishedAt": "",
        "summary": {},
        "runtime": {},
        "failures": [],
        "topFailures": [],
        "candidates": [],
    }


def build_latest_run_summary(*, repo_root: Path | None = None, limit: int = 5) -> dict[str, Any]:
    root = Path(repo_root or REPO_ROOT).resolve()
    fetch_report_path = _resolve_latest_artifact_path(root, FETCH_REPORT_NAME)
    discovery_report_path: Path | None = None
    try:
        discovery_report_path = _resolve_latest_artifact_path(root, DISCOVERY_REPORT_NAME)
    except FileNotFoundError:
        discovery_report_path = None
    jobs_unified_path = fetch_report_path.parent / JOBS_UNIFIED_NAME
    if not jobs_unified_path.is_file():
        try:
            jobs_unified_path = _resolve_latest_artifact_path(root, JOBS_UNIFIED_NAME)
        except FileNotFoundError:
            jobs_unified_path = jobs_unified_path

    fetch_report = _read_json(fetch_report_path)
    discovery_report = (
        _read_json(discovery_report_path)
        if discovery_report_path is not None
        else _empty_discovery_report()
    )
    if not isinstance(discovery_report, dict):
        raise TypeError(
            "Discovery report must be a JSON object: "
            f"{discovery_report_path or DISCOVERY_REPORT_NAME}"
        )
    if not isinstance(fetch_report, dict):
        raise TypeError(f"Fetch report must be a JSON object: {fetch_report_path}")

    # Ensure the audit helper resolves queue artifacts from the selected fetch report root.
    fetch_report = dict(fetch_report)
    fetch_outputs = (
        fetch_report.get("outputs") if isinstance(fetch_report.get("outputs"), dict) else {}
    )
    fetch_outputs = dict(fetch_outputs)
    fetch_outputs["report"] = str(fetch_report_path)
    fetch_report["outputs"] = fetch_outputs

    jobs_payload = _read_json_list(jobs_unified_path)
    audit_report = audit.build_report(discovery_report, fetch_report, jobs_payload)

    parser_regression_queue_path = fetch_report_path.parent / PARSER_REGRESSION_QUEUE_NAME
    browser_fallback_queue_path = fetch_report_path.parent / BROWSER_FALLBACK_QUEUE_NAME
    parser_regression_rows = _read_json_list(parser_regression_queue_path)
    fetch_runtime = (
        fetch_report.get("runtime") if isinstance(fetch_report.get("runtime"), dict) else {}
    )

    return {
        "generatedAt": audit.now_iso(),
        "repoRoot": str(root),
        "paths": {
            "discoveryReport": str(discovery_report_path) if discovery_report_path else "",
            "fetchReport": str(fetch_report_path),
            "jobsUnified": str(jobs_unified_path) if jobs_unified_path.is_file() else "",
            "parserRegressionQueue": str(parser_regression_queue_path),
            "browserFallbackQueue": str(browser_fallback_queue_path),
        },
        "report": audit_report,
        "fetchRuntimeStatic": {
            "staticDomainGateWaitMs": int(fetch_runtime.get("staticDomainGateWaitMs") or 0),
            "staticDetailBatchCount": int(fetch_runtime.get("staticDetailBatchCount") or 0),
            "staticAdaptiveStops": int(fetch_runtime.get("staticAdaptiveStops") or 0),
            "staticListingTimeoutStops": int(fetch_runtime.get("staticListingTimeoutStops") or 0),
            "staticListingBrowserFallbacks": int(
                fetch_runtime.get("staticListingBrowserFallbacks") or 0
            ),
        },
        "parserRegressionQueuePreview": _preview_rows(parser_regression_rows, limit),
    }


def render_text_summary(summary: dict[str, Any]) -> str:
    report = summary.get("report") if isinstance(summary.get("report"), dict) else {}
    discovery = report.get("discovery") if isinstance(report.get("discovery"), dict) else {}
    fetch = report.get("fetch") if isinstance(report.get("fetch"), dict) else {}
    totals = report.get("totals") if isinstance(report.get("totals"), dict) else {}
    issues = report.get("issues") if isinstance(report.get("issues"), dict) else {}
    paths = summary.get("paths") if isinstance(summary.get("paths"), dict) else {}
    preview = [
        row for row in (summary.get("parserRegressionQueuePreview") or []) if isinstance(row, dict)
    ]
    fetch_runtime_static = (
        summary.get("fetchRuntimeStatic")
        if isinstance(summary.get("fetchRuntimeStatic"), dict)
        else {}
    )
    lines = [
        "# Latest Run Report",
        "",
        f"- Repo root: {str(summary.get('repoRoot') or '')}",
        f"- Discovery report: {_display_report_path(paths.get('discoveryReport'))}",
        f"- Fetch report: {_display_report_path(paths.get('fetchReport'))}",
        f"- Parser regression queue: {_display_report_path(paths.get('parserRegressionQueue'))}",
        "",
        "## Discovery",
        f"- Started: {str(discovery.get('startedAt') or '')}",
        f"- Finished: {str(discovery.get('finishedAt') or '')}",
        f"- Queued candidates: {int(discovery.get('queuedCandidateCount') or 0)}",
        f"- Failed probes: {int(discovery.get('failedProbeCount') or 0)}",
        f"- Probe misses: {int(discovery.get('probeMissCount') or 0)}",
        f"- Discoverable but deferred: {int(discovery.get('discoverableButDeferredCount') or 0)}",
        f"- Queue filtered: {int(discovery.get('queueFilteredCount') or 0)}",
        f"- Failures: {int(discovery.get('failureCount') or 0)}",
        "",
        "## Fetch",
        f"- Started: {str(fetch.get('startedAt') or '')}",
        f"- Finished: {str(fetch.get('finishedAt') or '')}",
        f"- Sources: {int(fetch.get('sourceCount') or 0)}",
        f"- Successful sources: {int(fetch.get('successfulSources') or 0)}",
        f"- Failed sources: {int(fetch.get('failedSourcesCount') or 0)}",
        f"- Output rows: {int(fetch.get('outputCount') or 0)}",
        f"- Browser fallback queue: {int(fetch.get('browserFallbackQueueCount') or 0)}",
        f"- Static domain-gate wait: {int(fetch_runtime_static.get('staticDomainGateWaitMs') or 0)} ms",
        f"- Static detail batches: {int(fetch_runtime_static.get('staticDetailBatchCount') or 0)}",
        f"- Static adaptive stops: {int(fetch_runtime_static.get('staticAdaptiveStops') or 0)}",
        f"- Static listing timeout stops: {int(fetch_runtime_static.get('staticListingTimeoutStops') or 0)}",
        f"- Static listing browser fallbacks: {int(fetch_runtime_static.get('staticListingBrowserFallbacks') or 0)}",
        "",
        "## Triage",
        f"- site_changed diagnosed: {int(fetch.get('siteChangedDiagnosedCount') or 0)}",
        f"- parser regression queue: {int(fetch.get('parserRegressionQueueCount') or 0)}",
        f"- missing old URL: {int(fetch.get('siteChangedMissingOldUrlCount') or 0)}",
        f"- issue inventory hard failures: {len(issues.get('hard_failures') or [])}",
        f"- issue inventory soft failures: {len(issues.get('soft_failures') or [])}",
        f"- issue inventory high-cost/low-yield: {len(issues.get('high_cost_low_yield') or [])}",
        f"- issue inventory coverage risks: {len(issues.get('coverage_risks') or [])}",
        f"- total jobs: {int(totals.get('totalJobs') or 0)}",
    ]
    if preview:
        lines.extend(
            [
                "",
                "## Parser Regression Queue Preview",
            ]
        )
        for row in preview:
            queue_bits = [
                f"source={str(row.get('source') or '')}",
                f"oldUrl={str(row.get('oldUrl') or '')}",
            ]
            if str(row.get("currentUrl") or ""):
                queue_bits.append(f"currentUrl={str(row.get('currentUrl') or '')}")
            queue_bits.append(f"lastStatus={str(row.get('lastStatus') or '')}")
            queue_bits.append(
                f"listingFingerprintChanged={str(bool(row.get('listingFingerprintChanged'))).lower()}"
            )
            lines.append("- " + " | ".join(queue_bits))
    else:
        lines.extend(
            [
                "",
                "## Parser Regression Queue Preview",
                "- None",
            ]
        )
    lines.extend(
        [
            "",
            "## Top Fetch Failures",
        ]
    )
    failed_fetch = [row for row in (fetch.get("failedSources") or []) if isinstance(row, dict)]
    if failed_fetch:
        for row in failed_fetch[:5]:
            lines.append(
                f"- {str(row.get('name') or '')} ({str(row.get('adapter') or '')}) | duration={int(row.get('durationMs') or 0)} ms | error={str(row.get('error') or '')}"
            )
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Top Discovery Failures",
        ]
    )
    failed_discovery = [
        row for row in (discovery.get("failedSources") or []) if isinstance(row, dict)
    ]
    if failed_discovery:
        for row in failed_discovery[:5]:
            lines.append(
                f"- {str(row.get('name') or '')} ({str(row.get('adapter') or '')}) | stage={str(row.get('stage') or '')} | error={str(row.get('error') or '')}"
            )
    else:
        lines.append("- None")
    recommendations = [
        str(item) for item in (report.get("recommendations") or []) if str(item).strip()
    ]
    lines.extend(
        [
            "",
            "## Recommendations",
        ]
    )
    if recommendations:
        for item in recommendations[:5]:
            lines.append(f"- {item}")
    else:
        lines.append("- None")
    lines.append("")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize the latest discovery and fetch runs.")
    parser.add_argument(
        "--repo-root",
        default=str(REPO_ROOT),
        help="Repository root containing data/ and _out/ run locations.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum number of queue and failure rows to show in the preview.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of text.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        summary = build_latest_run_summary(
            repo_root=Path(str(args.repo_root)), limit=int(args.limit)
        )
    except _EXPECTED_CLI_FAILURES as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        print(render_text_summary(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
