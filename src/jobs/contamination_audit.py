#!/usr/bin/env python3
"""Audit unified jobs output for HTML-like contamination in public text fields."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from src.jobs.adapters.location_rules import classify_city_garbage
from src.jobs.text_utils import clean_text, has_html_like_fragment, invalid_location_reason
from src.shared.json_io import read_json

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


def build_contamination_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    field_counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
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


def build_location_quality_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    field_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    invalid_count = 0
    total_rows = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        total_rows += 1
        invalid_fields: dict[str, dict[str, str]] = {}
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


def _city_garbage_hit(value: Any) -> dict[str, str] | None:
    text = clean_text(value)
    category = classify_city_garbage(text)
    if not category:
        return None
    return {"category": category, "value": text}


def _record_city_garbage_hit(
    *,
    field: str,
    hit: dict[str, str],
    field_counts: Counter[str],
    category_counts: Counter[str],
) -> None:
    field_counts[field] += 1
    category_counts[hit["category"]] += 1


def _top_level_city_garbage_fields(
    row: dict[str, Any],
    *,
    field_counts: Counter[str],
    category_counts: Counter[str],
) -> dict[str, Any]:
    invalid_fields: dict[str, Any] = {}
    for field in ("city", "locationSummary"):
        hit = _city_garbage_hit(row.get(field))
        if not hit:
            continue
        invalid_fields[field] = hit
        _record_city_garbage_hit(
            field=field,
            hit=hit,
            field_counts=field_counts,
            category_counts=category_counts,
        )
    return invalid_fields


def _location_city_garbage_hits(
    row: dict[str, Any],
    *,
    field_counts: Counter[str],
    category_counts: Counter[str],
) -> list[dict[str, str]]:
    locations = row.get("locations")
    if not isinstance(locations, list):
        return []
    location_hits: list[dict[str, str]] = []
    for item in locations:
        if not isinstance(item, dict):
            continue
        hit = _city_garbage_hit(item.get("city"))
        if not hit:
            continue
        location_hits.append(hit)
        _record_city_garbage_hit(
            field="locations.city",
            hit=hit,
            field_counts=field_counts,
            category_counts=category_counts,
        )
    return location_hits


def _city_garbage_example(
    row: dict[str, Any],
    invalid_fields: dict[str, Any],
) -> dict[str, Any]:
    return {
        "company": clean_text(row.get("company")),
        "title": clean_text(row.get("title")),
        "source": clean_text(row.get("source")),
        "jobLink": clean_text(row.get("jobLink")),
        "fields": invalid_fields,
    }


def build_city_garbage_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    field_counts: Counter[str] = Counter()
    category_counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    garbage_rows = 0
    total_rows = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        total_rows += 1
        invalid_fields = _top_level_city_garbage_fields(
            row,
            field_counts=field_counts,
            category_counts=category_counts,
        )
        location_hits = _location_city_garbage_hits(
            row,
            field_counts=field_counts,
            category_counts=category_counts,
        )
        if location_hits:
            invalid_fields["locations.city"] = location_hits

        if not invalid_fields:
            continue
        garbage_rows += 1
        if len(examples) < 20:
            examples.append(_city_garbage_example(row, invalid_fields))
    return {
        "totalRows": total_rows,
        "garbageRows": garbage_rows,
        "fieldCounts": dict(field_counts),
        "categoryCounts": dict(category_counts),
        "examples": examples,
    }


def build_public_text_quality_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    report = build_contamination_report(rows)
    report["locationQualityAudit"] = build_location_quality_report(rows)
    report["cityGarbageAudit"] = build_city_garbage_report(rows)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit unified jobs output for HTML-like contamination."
    )
    parser.add_argument(
        "--input", default="data/jobs-unified.json", help="Unified jobs JSON to scan."
    )
    parser.add_argument("--output", default="", help="Optional JSON output path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).resolve()
    rows = read_json(input_path, [])
    if not isinstance(rows, list):
        rows = []
    report = build_public_text_quality_report(rows)
    output_path = (
        Path(args.output).resolve()
        if clean_text(args.output)
        else input_path.parent / "jobs-contamination-audit.json"
    )
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(str(output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
