#!/usr/bin/env python3
"""Audit unified jobs output for HTML-like contamination in public text fields."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

from src.jobs.text_utils import clean_text, has_html_like_fragment, invalid_location_reason

PUBLIC_TEXT_FIELDS = (
    "title",
    "company",
    "city",
    "country",
    "profession",
    "sector",
    "contractType",
    "workType",
)


def read_json(path: Path, fallback: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback


def build_contamination_report(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    field_counts: Counter[str] = Counter()
    examples: List[Dict[str, Any]] = []
    contaminated_rows = 0
    total_rows = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        total_rows += 1
        contaminated_fields = {
            field: clean_text(row.get(field))
            for field in PUBLIC_TEXT_FIELDS
            if has_html_like_fragment(row.get(field))
        }
        if not contaminated_fields:
            continue
        contaminated_rows += 1
        field_counts.update(contaminated_fields.keys())
        if len(examples) < 20:
            examples.append(
                {
                    "company": clean_text(row.get("company")),
                    "title": clean_text(row.get("title")),
                    "source": clean_text(row.get("source")),
                    "jobLink": clean_text(row.get("jobLink")),
                    "fields": contaminated_fields,
                }
            )
    return {
        "totalRows": total_rows,
        "contaminatedRows": contaminated_rows,
        "fieldCounts": dict(field_counts),
        "examples": examples,
    }


def build_location_quality_report(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    field_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    examples: List[Dict[str, Any]] = []
    invalid_count = 0
    total_rows = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        total_rows += 1
        invalid_fields: Dict[str, Dict[str, str]] = {}
        for field in ("city", "country"):
            reason = invalid_location_reason(row.get(field), field_name=field)
            if not reason:
                continue
            invalid_fields[field] = {
                "reason": reason,
                "value": clean_text(row.get(field)),
            }
            field_counts[field] += 1
            reason_counts[reason] += 1
            invalid_count += 1
        if invalid_fields and len(examples) < 20:
            examples.append(
                {
                    "company": clean_text(row.get("company")),
                    "title": clean_text(row.get("title")),
                    "source": clean_text(row.get("source")),
                    "jobLink": clean_text(row.get("jobLink")),
                    "fields": invalid_fields,
                }
            )
    return {
        "totalRows": total_rows,
        "invalidLocationFieldCount": invalid_count,
        "fieldCounts": dict(field_counts),
        "reasonCounts": dict(reason_counts),
        "examples": examples,
    }


def build_public_text_quality_report(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    report = build_contamination_report(rows)
    report["locationQualityAudit"] = build_location_quality_report(rows)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit unified jobs output for HTML-like contamination.")
    parser.add_argument("--input", default="data/jobs-unified.json", help="Unified jobs JSON to scan.")
    parser.add_argument("--output", default="", help="Optional JSON output path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).resolve()
    rows = read_json(input_path, [])
    if not isinstance(rows, list):
        rows = []
    report = build_public_text_quality_report(rows)
    output_path = Path(args.output).resolve() if clean_text(args.output) else input_path.parent / "jobs-contamination-audit.json"
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(str(output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
