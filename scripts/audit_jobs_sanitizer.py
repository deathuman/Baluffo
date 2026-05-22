#!/usr/bin/env python3
"""Audit unified jobs output for sanitizer follow-up checks."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.jobs.canonicalize import (
    _derive_google_sheets_title_from_url,
    _google_sheets_provider_title_target,
    _is_google_sheets_category_label,
)

DEFAULT_SUSPICIOUS_TITLES = ("Account-management", "Administartive")


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _canonical_drop_reasons(path: Path | None) -> Counter[str]:
    if path is None or not path.exists():
        return Counter()
    report = json.loads(path.read_text(encoding="utf-8"))
    reasons: Counter[str] = Counter()
    for source in report.get("sources") or []:
        loss = source.get("loss") if isinstance(source, dict) else {}
        drop_reasons = (loss or {}).get("canonicalDropReasons") or {}
        reasons.update({str(key): int(value or 0) for key, value in drop_reasons.items()})
    return reasons


def _host(row: dict[str, str]) -> str:
    return urlparse(row.get("jobLink") or "").netloc.lower()


def _category_style_title(title: str) -> bool:
    return _is_google_sheets_category_label(title)


def _print_counter(title: str, counter: Counter[str], limit: int) -> None:
    print(title)
    for value, count in counter.most_common(limit):
        print(f"  {count:>5}  {value}")
    if not counter:
        print("      0  <none>")


def _print_rows(title: str, rows: list[dict[str, str]], limit: int) -> None:
    print(title)
    for row in rows[:limit]:
        print(
            "  "
            f"{row.get('title', '')} | "
            f"{row.get('company', '')} | "
            f"{row.get('sector', '')}/{row.get('companyType', '')} | "
            f"{_host(row)} | "
            f"{row.get('jobLink', '')}"
        )
    if not rows:
        print("  <none>")


def _print_repair_rows(title: str, rows: list[tuple[dict[str, str], str]], limit: int) -> None:
    print(title)
    for row, repaired_title in rows[:limit]:
        print(
            "  "
            f"{row.get('title', '')} -> {repaired_title} | "
            f"{row.get('company', '')} | "
            f"{row.get('sector', '')}/{row.get('companyType', '')} | "
            f"{_host(row)} | "
            f"{row.get('jobLink', '')}"
        )
    if not rows:
        print("  <none>")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize sanitizer-sensitive rows in jobs-unified.csv."
    )
    parser.add_argument(
        "--input-csv",
        default="_out/latest/build/portable/ship/data/jobs-unified.csv",
        help="Path to jobs-unified.csv.",
    )
    parser.add_argument(
        "--report-json",
        default="_out/latest/build/portable/ship/data/jobs-fetch-report.json",
        help="Optional jobs-fetch-report.json path for canonical drop counts.",
    )
    parser.add_argument(
        "--suspicious-title",
        action="append",
        dest="suspicious_titles",
        help="Suspicious exact title to count. May be repeated.",
    )
    parser.add_argument("--limit", type=int, default=25, help="Rows/counter entries to show.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_csv = Path(args.input_csv)
    report_json = Path(args.report_json) if args.report_json else None
    suspicious_titles = tuple(args.suspicious_titles or DEFAULT_SUSPICIOUS_TITLES)

    rows = _read_rows(input_csv)
    suspicious = [row for row in rows if row.get("title") in suspicious_titles]
    google_rows = [row for row in rows if (row.get("source") or "").startswith("google_sheets")]
    category_style = [row for row in google_rows if _category_style_title(row.get("title") or "")]
    repair_candidates = [
        (row, repaired_title)
        for row in category_style
        if (
            repaired_title := _derive_google_sheets_title_from_url(
                source=row.get("source") or "",
                title=row.get("title") or "",
                company=row.get("company") or "",
                job_link=row.get("jobLink") or "",
            )
        )
    ]
    provider_hydration_targets = [
        row
        for row in category_style
        if _google_sheets_provider_title_target(row.get("jobLink") or "") is not None
    ]

    print(f"input_csv: {input_csv}")
    print(f"total_rows: {len(rows)}")
    print(f"google_sheets_rows: {len(google_rows)}")
    print(f"google_sheets_category_style_titles: {len(category_style)}")
    print(f"google_sheets_title_repair_candidates: {len(repair_candidates)}")
    print(f"google_sheets_provider_hydration_targets: {len(provider_hydration_targets)}")
    _print_counter("canonical_drop_reasons:", _canonical_drop_reasons(report_json), args.limit)
    _print_counter(
        "suspicious_titles:",
        Counter(row.get("title") or "" for row in suspicious),
        args.limit,
    )
    _print_counter("suspicious_domains:", Counter(_host(row) for row in suspicious), args.limit)
    _print_counter(
        "top_google_category_style_titles:",
        Counter(row.get("title") or "" for row in category_style),
        args.limit,
    )
    _print_counter(
        "top_google_title_repair_candidate_titles:",
        Counter(row.get("title") or "" for row, _ in repair_candidates),
        args.limit,
    )
    _print_counter(
        "top_google_provider_hydration_target_domains:",
        Counter(_host(row) for row in provider_hydration_targets),
        args.limit,
    )
    _print_counter(
        "top_google_provider_hydration_target_titles:",
        Counter(row.get("title") or "" for row in provider_hydration_targets),
        args.limit,
    )
    _print_rows("suspicious_samples:", suspicious, args.limit)
    _print_repair_rows("title_repair_candidate_samples:", repair_candidates, args.limit)
    _print_rows("provider_hydration_target_samples:", provider_hydration_targets, args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
