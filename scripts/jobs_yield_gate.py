#!/usr/bin/env python3
"""Read-only helpers for jobs adapter yield gates."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.jobs.adapters.api import default_source_loaders
from src.jobs.text_utils import clean_text


def _resolve_report_path(value: str) -> Path:
    path = Path(value)
    if path.is_dir():
        return path / "jobs-fetch-report.json"
    return path


def _load_report(value: str) -> dict[str, Any]:
    path = _resolve_report_path(value)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _source_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = report.get("sources")
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _source_key(row: dict[str, Any]) -> str:
    return clean_text(row.get("name") or row.get("source") or row.get("adapter"))


def _summary_by_source(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    for row in _source_rows(report):
        key = _source_key(row)
        if not key:
            continue
        entry = summaries.setdefault(
            key,
            {
                "source": key,
                "keptCount": 0,
                "fetchedCount": 0,
                "statusCounts": Counter(),
                "failureBuckets": Counter(),
            },
        )
        entry["keptCount"] += max(0, int(row.get("keptCount") or 0))
        entry["fetchedCount"] += max(0, int(row.get("fetchedCount") or 0))
        status = clean_text(row.get("status")).lower() or "unknown"
        entry["statusCounts"][status] += 1
        bucket = clean_text(row.get("failureBucket") or row.get("zeroKeptClassification"))
        if bucket:
            entry["failureBuckets"][bucket] += 1
    return summaries


def _format_counter(counter: Counter[str]) -> str:
    return ",".join(f"{key}:{counter[key]}" for key in sorted(counter))


def list_static_sources(args: argparse.Namespace) -> int:
    rows = [
        name
        for name, _loader in default_source_loaders(social_enabled=False)
        if clean_text(name).startswith("static_source::")
    ]
    if args.contains:
        needle = clean_text(args.contains).lower()
        rows = [name for name in rows if needle in name.lower()]
    rows = rows[: max(0, int(args.limit or 0))] if int(args.limit or 0) > 0 else rows
    if args.json:
        print(json.dumps(rows, indent=2, ensure_ascii=False))
    else:
        for name in rows:
            print(name)
    return 0


def compare_reports(args: argparse.Namespace) -> int:
    before = _summary_by_source(_load_report(args.before))
    after = _summary_by_source(_load_report(args.after))
    keys = sorted(set(before) | set(after))
    drops: list[str] = []
    print(
        "source\tbefore_kept\tafter_kept\tbefore_status\tafter_status\t"
        "before_failures\tafter_failures"
    )
    for key in keys:
        before_row = before.get(key, {})
        after_row = after.get(key, {})
        before_kept = int(before_row.get("keptCount") or 0)
        after_kept = int(after_row.get("keptCount") or 0)
        if after_kept < before_kept:
            drops.append(key)
        print(
            "\t".join(
                [
                    key,
                    str(before_kept),
                    str(after_kept),
                    _format_counter(before_row.get("statusCounts") or Counter()),
                    _format_counter(after_row.get("statusCounts") or Counter()),
                    _format_counter(before_row.get("failureBuckets") or Counter()),
                    _format_counter(after_row.get("failureBuckets") or Counter()),
                ]
            )
        )
    if drops and not args.allow_drops:
        print("Yield drops detected: " + ", ".join(drops), file=sys.stderr)
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only jobs adapter yield gate helpers.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser(
        "list-static-sources",
        help="Print valid generated static source IDs for --only-sources gates.",
    )
    list_parser.add_argument("--limit", type=int, default=20)
    list_parser.add_argument("--contains", default="")
    list_parser.add_argument("--json", action="store_true")
    list_parser.set_defaults(func=list_static_sources)

    compare_parser = subparsers.add_parser(
        "compare",
        help="Compare before/after jobs-fetch-report.json source kept counts.",
    )
    compare_parser.add_argument("before", help="Before output dir or jobs-fetch-report.json path.")
    compare_parser.add_argument("after", help="After output dir or jobs-fetch-report.json path.")
    compare_parser.add_argument("--allow-drops", action="store_true")
    compare_parser.set_defaults(func=compare_reports)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
