#!/usr/bin/env python3
"""Compare targeted adapter-audit results against the latest full fetch report."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.jobs.text_utils import clean_text

DEFAULT_FULL_REPORT = ROOT / "data" / "jobs-fetch-report.json"
DEFAULT_AUDIT_REPORT = ROOT / "data" / "adapter-audit-report.json"
_AUDIT_SECTION_RE = re.compile(r"^## (?P<bucket>[a-z-]+)$")
_AUDIT_LINE_RE = re.compile(
    r"^- `(?P<adapter>[^`]+)` jobs=`(?P<jobs>\d+)` durationMs=`(?P<duration>\d+)`.*$"
)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _load_audit_report(path: Path) -> dict[str, Any]:
    if path.exists() and path.suffix.lower() == ".json":
        return _load_json(path)

    candidate = path
    if not candidate.exists():
        fallback = path.with_suffix(".md")
        if fallback.exists():
            candidate = fallback
    if candidate.suffix.lower() == ".md" and candidate.exists():
        return _load_audit_markdown(candidate)
    if candidate.exists():
        return _load_json(candidate)
    raise FileNotFoundError(candidate)


def _load_audit_markdown(path: Path) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    current_bucket = ""
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        section_match = _AUDIT_SECTION_RE.match(line)
        if section_match:
            current_bucket = section_match.group("bucket")
            continue
        line_match = _AUDIT_LINE_RE.match(line)
        if not line_match:
            continue
        rest = line.split(" sources=", 1)
        error_text = ""
        if len(rest) == 2 and "; error=" in rest[1]:
            error_text = rest[1].split("; error=", 1)[1].strip()
        results.append(
            {
                "adapter": line_match.group("adapter"),
                "jobsCount": int(line_match.group("jobs")),
                "durationMs": int(line_match.group("duration")),
                "bucket": current_bucket,
                "error": error_text,
            }
        )
    return {"results": results}


def _adapter_name_from_full_row(row: dict[str, Any]) -> str:
    adapter = clean_text(row.get("adapter"))
    if adapter:
        return adapter
    name = clean_text(row.get("name"))
    if name.endswith("_sources"):
        return name[:-8]
    return name


def _format_status_counts(counts: Counter[str]) -> str:
    if not counts:
        return ""
    return ", ".join(f"{status}:{counts[status]}" for status in sorted(counts))


def summarize_full_report(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = report.get("sources") if isinstance(report.get("sources"), list) else []
    summaries: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        adapter = _adapter_name_from_full_row(row)
        if not adapter:
            continue
        entry = summaries.setdefault(
            adapter,
            {
                "adapter": adapter,
                "sourceCount": 0,
                "keptCount": 0,
                "fetchedCount": 0,
                "errorCount": 0,
                "excludedCount": 0,
                "statusCounts": Counter(),
            },
        )
        entry["sourceCount"] += 1
        entry["keptCount"] += int(row.get("keptCount") or 0)
        entry["fetchedCount"] += int(row.get("fetchedCount") or 0)
        status = clean_text(row.get("status")).lower() or "unknown"
        entry["statusCounts"][status] += 1
        if status == "error":
            entry["errorCount"] += 1
        if status == "excluded":
            entry["excludedCount"] += 1
    return summaries


def summarize_audit_report(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = report.get("results") if isinstance(report.get("results"), list) else []
    summaries: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        adapter = clean_text(row.get("adapter"))
        if not adapter:
            continue
        bucket = clean_text(row.get("bucket")).lower() or "unknown"
        entry = summaries.setdefault(
            adapter,
            {
                "adapter": adapter,
                "jobsCount": 0,
                "bucket": bucket,
                "durationMs": 0,
                "error": "",
            },
        )
        entry["jobsCount"] += int(row.get("jobsCount") or 0)
        entry["durationMs"] += int(row.get("durationMs") or 0)
        if not entry["error"]:
            entry["error"] = clean_text(row.get("error"))
        if bucket not in {"unknown", entry["bucket"]} and entry["bucket"] == "unknown":
            entry["bucket"] = bucket
    return summaries


def compare_reports(
    *,
    full_report: dict[str, Any],
    audit_report: dict[str, Any],
) -> dict[str, Any]:
    full_summary = summarize_full_report(full_report)
    audit_summary = summarize_audit_report(audit_report)
    adapters = sorted(set(full_summary) | set(audit_summary))

    rows: list[dict[str, Any]] = []
    mismatch_count = 0
    for adapter in adapters:
        full_row = full_summary.get(adapter, {})
        audit_row = audit_summary.get(adapter, {})
        full_kept = int(full_row.get("keptCount") or 0)
        audit_jobs = int(audit_row.get("jobsCount") or 0)
        reasons: list[str] = []
        if audit_jobs > 0 and full_kept == 0:
            reasons.append("audit_positive_full_zero")
        if audit_jobs == 0 and full_kept > 0:
            reasons.append("audit_zero_full_positive")
        if int(full_row.get("errorCount") or 0) > 0 and audit_jobs > 0:
            reasons.append("full_error_audit_positive")
        if reasons:
            mismatch_count += 1
        rows.append(
            {
                "adapter": adapter,
                "auditJobsCount": audit_jobs,
                "auditBucket": clean_text(audit_row.get("bucket")),
                "fullKeptCount": full_kept,
                "fullFetchedCount": int(full_row.get("fetchedCount") or 0),
                "fullSourceCount": int(full_row.get("sourceCount") or 0),
                "fullStatusCounts": dict(full_row.get("statusCounts") or {}),
                "fullStatusSummary": _format_status_counts(
                    Counter(full_row.get("statusCounts") or {})
                ),
                "mismatch": bool(reasons),
                "mismatchReasons": reasons,
            }
        )
    return {
        "summary": {
            "adapterCount": len(adapters),
            "mismatchCount": mismatch_count,
            "fullAdapterCount": len(full_summary),
            "auditAdapterCount": len(audit_summary),
        },
        "rows": rows,
    }


def render_table(report: dict[str, Any]) -> str:
    lines = [
        "Adapter parity diff",
        f"Adapters: {report['summary']['adapterCount']}  Mismatches: {report['summary']['mismatchCount']}",
        "",
        "adapter | audit_jobs | full_kept | full_status | mismatch",
        "--- | ---: | ---: | --- | ---",
    ]
    for row in report["rows"]:
        mismatch = "yes" if row["mismatch"] else "no"
        status = row["fullStatusSummary"] or "-"
        lines.append(
            f"{row['adapter']} | {row['auditJobsCount']} | {row['fullKeptCount']} | {status} | {mismatch}"
        )
        if row["mismatchReasons"]:
            lines.append(f"  reasons: {', '.join(row['mismatchReasons'])}")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--full-report",
        type=Path,
        default=DEFAULT_FULL_REPORT,
        help="Path to jobs-fetch-report.json (default: data/jobs-fetch-report.json)",
    )
    parser.add_argument(
        "--audit-report",
        type=Path,
        default=DEFAULT_AUDIT_REPORT,
        help="Path to adapter-audit-report.json (default: data/adapter-audit-report.json)",
    )
    parser.add_argument(
        "--format",
        choices=("table", "json"),
        default="table",
        help="Output format",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    full_report = _load_json(args.full_report)
    audit_report = _load_audit_report(args.audit_report)
    report = compare_reports(full_report=full_report, audit_report=audit_report)
    if args.format == "json":
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print(render_table(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
