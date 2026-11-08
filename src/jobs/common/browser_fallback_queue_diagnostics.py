"""Legacy browser-fallback queue diagnostics.

The queue reader silently collapses repeated canonical pages and skips
``parse_error`` rows the static allowlist does not name, so a legacy queue with
either problem looks clean from the reader's output. This leaf reports the raw
queue shape instead of the filtered one, and never rewrites the queue.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.jobs.common.browser_fallback_policy import is_browser_fallback_queue_eligible
from src.jobs.common.config import STATIC_CLASSIFICATIONS_FOR_BROWSER_QUEUE
from src.jobs.text_utils import clean_text

LEGACY_BROWSER_FALLBACK_QUEUE_FILENAME = "jobs-browser-fallback-queue.json"
# Ops polls must stay compact: the counts are complete, the row lists are capped.
MAX_DIAGNOSTIC_ROWS = 50
_MAX_REASON_CHARS = 200


def _iso_from_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=UTC).isoformat().replace("+00:00", "Z")


def _path_display(path: Path) -> str:
    """Container-relative label, matching the other ops artifact diagnostics.

    The real data dir is absolute on the desktop and varies per deployment, so the
    display path is a stable artifact label rather than a host path.
    """
    return f"/data/{path.name}"


def _row_eligible(row: dict[str, Any], classification: str) -> bool:
    return is_browser_fallback_queue_eligible(
        classification=classification,
        error=row.get("reason"),
    )


def _reader_source_id(row: dict[str, Any], page: str) -> str:
    """The identity the queue reader uses for a kept row.

    Mirrors ``common.registry._scrapy_queue_source_id``: a legacy row with no
    ``sourceId`` is still identified by its page, so reporting the raw blank
    would leave the answer this diagnostic exists to give unreadable.
    """
    source_id = clean_text(row.get("sourceId"))
    if source_id or not page:
        return source_id
    if clean_text(row.get("adapter")) != "scrapy_static":
        return ""
    digest = hashlib.sha1(page.encode("utf-8")).hexdigest()[:12]
    return f"scrapy_static:{digest}"


def _duplicate_page_entry(page: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """One repeated canonical page, with the copy the reader keeps.

    The raw duplicate count is the artifact's shape; ``readerCollapsedRowCount`` is
    the part that changes what the next run does. They differ when every extra row
    is ineligible anyway -- a ``parse_error`` + ``timeout`` pair repeats a page
    without ever double-running it.
    """
    source_ids: list[str] = []
    classifications: list[str] = []
    kept_source_id = ""
    eligible_row_count = 0
    kept_source_taken = False
    for row in rows:
        source_id = clean_text(row.get("sourceId"))
        if source_id and source_id not in source_ids:
            source_ids.append(source_id)
        classification = clean_text(row.get("classification")).lower()
        if classification and classification not in classifications:
            classifications.append(classification)
        if _row_eligible(row, classification):
            eligible_row_count += 1
            if not kept_source_taken:
                kept_source_taken = True
                kept_source_id = _reader_source_id(row, page)
    return {
        "page": page,
        "rowCount": len(rows),
        "eligibleRowCount": eligible_row_count,
        "readerCollapsedRowCount": max(0, eligible_row_count - 1),
        "sourceIds": source_ids,
        "classifications": classifications,
        # The reader keeps the first eligible row for a page, so naming it
        # separates a harmless repeat from one that silently drops a source.
        "readerKeptSourceId": kept_source_id,
    }


def _parse_error_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "page": clean_text(row.get("page")),
        "sourceId": clean_text(row.get("sourceId")),
        "name": clean_text(row.get("name")) or clean_text(row.get("studio")),
        "classification": clean_text(row.get("classification")).lower(),
        "reason": clean_text(row.get("reason"))[:_MAX_REASON_CHARS],
    }


def _scan_rows(
    rows: list[Any],
) -> tuple[
    int,
    int,
    set[str],
    dict[str, list[dict[str, Any]]],
    list[dict[str, Any]],
]:
    """Collect the raw counts and page groups the reader would otherwise hide."""
    invalid_row_count = 0
    eligible_row_count = 0
    eligible_pages: set[str] = set()
    page_rows: dict[str, list[dict[str, Any]]] = {}
    parse_error_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            invalid_row_count += 1
            continue
        page = clean_text(row.get("page"))
        classification = clean_text(row.get("classification")).lower()
        eligible = _row_eligible(row, classification)
        if eligible:
            eligible_row_count += 1
            if page:
                eligible_pages.add(page)
        if page:
            page_rows.setdefault(page, []).append(row)
        if classification == "parse_error" and not eligible:
            parse_error_rows.append(_parse_error_row(row))
    return invalid_row_count, eligible_row_count, eligible_pages, page_rows, parse_error_rows


def build_browser_fallback_queue_diagnostics(path: str | Path) -> dict[str, Any]:
    """Report what the queue reader will silently drop from a legacy queue file.

    Read-only and bounded: a missing, unreadable or malformed file is reported as
    such instead of raising, because this backs an ops route. ``clean`` describes
    the artifact, not the endpoint -- a queue with issues is still a successful
    diagnostic.
    """
    queue_path = Path(path)
    warnings: list[str] = []
    payload: dict[str, Any] = {
        "ok": True,
        "queueFile": queue_path.name,
        "pathDisplay": _path_display(queue_path),
        "exists": queue_path.is_file(),
        "readable": False,
        "sizeBytes": 0,
        "modifiedAt": "",
        "rowCount": 0,
        "invalidRowCount": 0,
        "eligibleRowCount": 0,
        "eligibleUniquePageCount": 0,
        "duplicatePageCount": 0,
        "duplicateRowCount": 0,
        "duplicateExcessRowCount": 0,
        "eligibleDuplicatePageCount": 0,
        "readerCollapsedRowCount": 0,
        "parseErrorRowCount": 0,
        "issueCount": 0,
        "clean": True,
        "allowlistedClassifications": sorted(STATIC_CLASSIFICATIONS_FOR_BROWSER_QUEUE),
        "duplicatePages": [],
        "parseErrorRows": [],
        "truncated": {"duplicatePages": False, "parseErrorRows": False},
        "warnings": warnings,
    }
    if not queue_path.is_file():
        warnings.append("missing")
        return payload
    try:
        stat = queue_path.stat()
        payload["sizeBytes"] = int(stat.st_size)
        payload["modifiedAt"] = _iso_from_timestamp(stat.st_mtime)
    except OSError as exc:
        warnings.append(f"stat_failed:{type(exc).__name__}")
        return payload
    try:
        rows = json.loads(queue_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        warnings.append("invalid_json")
        return payload
    except OSError as exc:
        warnings.append(f"read_failed:{type(exc).__name__}")
        return payload
    if not isinstance(rows, list):
        warnings.append("invalid_payload")
        return payload

    payload["readable"] = True
    payload["rowCount"] = len(rows)
    (
        invalid_row_count,
        eligible_row_count,
        eligible_pages,
        page_rows,
        parse_error_rows,
    ) = _scan_rows(rows)

    duplicate_pages = [
        _duplicate_page_entry(page, group) for page, group in page_rows.items() if len(group) > 1
    ]
    duplicate_pages.sort(key=lambda entry: (-entry["rowCount"], entry["page"]))
    parse_error_rows.sort(key=lambda entry: (entry["page"], entry["sourceId"]))
    duplicate_row_count = sum(int(entry["rowCount"]) for entry in duplicate_pages)
    eligible_duplicate_pages = [
        entry for entry in duplicate_pages if int(entry["readerCollapsedRowCount"]) > 0
    ]
    # A parser row on a page already reported as repeated is the same finding, so
    # counting it again would report one page as two issues.
    duplicate_page_keys = {entry["page"] for entry in duplicate_pages}
    standalone_parse_error_count = sum(
        1
        for entry in parse_error_rows
        if not entry["page"] or entry["page"] not in duplicate_page_keys
    )
    payload.update(
        {
            "invalidRowCount": invalid_row_count,
            "eligibleRowCount": eligible_row_count,
            "eligibleUniquePageCount": len(eligible_pages),
            "duplicatePageCount": len(duplicate_pages),
            "duplicateRowCount": duplicate_row_count,
            "duplicateExcessRowCount": duplicate_row_count - len(duplicate_pages),
            "eligibleDuplicatePageCount": len(eligible_duplicate_pages),
            "readerCollapsedRowCount": sum(
                int(entry["readerCollapsedRowCount"]) for entry in eligible_duplicate_pages
            ),
            "parseErrorRowCount": len(parse_error_rows),
            "issueCount": len(duplicate_pages) + standalone_parse_error_count,
            "clean": not duplicate_pages and not parse_error_rows,
            "duplicatePages": duplicate_pages[:MAX_DIAGNOSTIC_ROWS],
            "parseErrorRows": parse_error_rows[:MAX_DIAGNOSTIC_ROWS],
            "truncated": {
                "duplicatePages": len(duplicate_pages) > MAX_DIAGNOSTIC_ROWS,
                "parseErrorRows": len(parse_error_rows) > MAX_DIAGNOSTIC_ROWS,
            },
        }
    )
    return payload


__all__ = [
    "LEGACY_BROWSER_FALLBACK_QUEUE_FILENAME",
    "MAX_DIAGNOSTIC_ROWS",
    "build_browser_fallback_queue_diagnostics",
]
