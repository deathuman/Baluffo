#!/usr/bin/env python3
"""Classify residual static/provider/browser failures from a fetch report."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src import source_registry as sr

REDIRECT_STATUS_MARKERS = ("HTTP 301", "HTTP 302", "HTTP 303", "HTTP 307", "HTTP 308")
STALE_STATUS_MARKERS = ("HTTP 404", "HTTP 500", "HTTP 522")
RATE_LIMIT_MARKERS = ("HTTP 403", "HTTP 429", "too many requests", "rate limit")
SUPPORTED_PROVIDER_HOST_TOKENS = (
    "greenhouse.io",
    "lever.co",
    "teamtailor.com",
    "smartrecruiters.com",
    "workable.com",
    "recruitee.com",
    "breezy.hr",
    "jazz.co",
    "bamboohr.com",
    "myworkdayjobs.com",
    "personio.com",
)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _source_id_from_report(row: dict[str, Any]) -> str:
    source_id = _clean_text(row.get("sourceId")).lower()
    if source_id:
        return source_id
    name = _clean_text(row.get("name"))
    if name.startswith("static_source::"):
        return name.split("static_source::", 1)[1].lower()
    return name.lower()


def _endpoint_url(row: dict[str, Any]) -> str:
    for key in ("api_url", "feed_url", "board_url", "listing_url", "careersUrl"):
        value = _clean_text(row.get(key))
        if value:
            return value
    pages = row.get("pages")
    if isinstance(pages, list):
        for value in pages:
            text = _clean_text(value)
            if text:
                return text
    return ""


def _combined_text(row: dict[str, Any]) -> str:
    return " ".join(
        _clean_text(row.get(key))
        for key in ("name", "error", "warning", "classification", "failureBucket")
    )


def _has_marker(text: str, markers: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(marker.lower() in lowered for marker in markers)


def _active_report_by_source_id(fetch_report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    reports: dict[str, dict[str, Any]] = {}
    for row in fetch_report.get("sources") or []:
        if not isinstance(row, dict):
            continue
        source_id = _source_id_from_report(row)
        if source_id:
            reports[source_id] = row
    return reports


def _stronger_peer_exists(
    source: dict[str, Any],
    *,
    active_rows: list[dict[str, Any]],
    report_by_source_id: dict[str, dict[str, Any]],
) -> bool:
    source_id = sr.source_identity(source)
    source_family = sr.source_family_key(source)
    for peer in active_rows:
        if sr.source_identity(peer) == source_id:
            continue
        if sr.source_family_key(peer) != source_family:
            continue
        if _clean_text(peer.get("adapter")).lower() != "static":
            return True
        peer_report = report_by_source_id.get(sr.source_identity(peer))
        if peer_report and int(peer_report.get("keptCount") or 0) > 0:
            return True
    return False


def classify_residual_failure(
    report_row: dict[str, Any],
    *,
    source_row: dict[str, Any] | None = None,
    active_rows: list[dict[str, Any]] | None = None,
    report_by_source_id: dict[str, dict[str, Any]] | None = None,
) -> str:
    """Return a narrow triage class for a failed source report row."""

    active_rows = active_rows or []
    report_by_source_id = report_by_source_id or {}
    text = _combined_text(report_row)
    bucket = _clean_text(report_row.get("failureBucket")).lower()
    adapter = _clean_text(report_row.get("adapter")).lower()
    endpoint = _endpoint_url(source_row or {})
    host = urlparse(endpoint).netloc.lower()

    if _has_marker(text, STALE_STATUS_MARKERS):
        return "stale_or_dead_url"
    if _has_marker(text, RATE_LIMIT_MARKERS) or bucket == "anti_bot_or_challenge":
        return "anti_bot_or_rate_limited"
    if source_row and _stronger_peer_exists(
        source_row,
        active_rows=active_rows,
        report_by_source_id=report_by_source_id,
    ):
        return "redundant_provider_coverage"
    if host and any(token in host for token in SUPPORTED_PROVIDER_HOST_TOKENS):
        return "existing_provider_migration"
    if bucket == "site_changed" or _has_marker(text, REDIRECT_STATUS_MARKERS):
        return "site_changed"
    if bucket == "js_required" or "js_required" in text.lower():
        return "browser_required" if adapter == "static" else "manual_review"
    if bucket == "needs_review":
        return "manual_review"
    return "manual_review"


def build_residual_failure_summary(
    *,
    fetch_report: dict[str, Any],
    active_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    active_by_id = {sr.source_identity(row): row for row in active_rows}
    report_by_source_id = _active_report_by_source_id(fetch_report)
    rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for report_row in fetch_report.get("sources") or []:
        if not isinstance(report_row, dict):
            continue
        if _clean_text(report_row.get("status")).lower() not in {"error", "failed"}:
            continue
        source_id = _source_id_from_report(report_row)
        source_row = active_by_id.get(source_id)
        triage_class = classify_residual_failure(
            report_row,
            source_row=source_row,
            active_rows=active_rows,
            report_by_source_id=report_by_source_id,
        )
        counts[triage_class] += 1
        rows.append(
            {
                "sourceId": source_id,
                "sourceName": _clean_text(report_row.get("name")),
                "adapter": _clean_text(report_row.get("adapter")),
                "failureBucket": _clean_text(report_row.get("failureBucket")),
                "triageClass": triage_class,
                "endpoint": _endpoint_url(source_row or {}),
            }
        )
    return {
        "failedSourceCount": len(rows),
        "byClass": dict(sorted(counts.items())),
        "rows": rows,
    }


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch-report", type=Path, required=True)
    parser.add_argument("--active-registry", type=Path, required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    summary = build_residual_failure_summary(
        fetch_report=_read_json(args.fetch_report),
        active_rows=_read_json(args.active_registry),
    )
    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        print(f"Failed sources: {summary['failedSourceCount']}")
        for triage_class, count in summary["byClass"].items():
            print(f"- {triage_class}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
