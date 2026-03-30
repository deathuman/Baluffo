#!/usr/bin/env python3
"""IO helpers for jobs pipeline outputs."""

from __future__ import annotations

import csv
import json
from collections.abc import Callable, Sequence
from io import StringIO
from pathlib import Path
from typing import Any

RawJob = dict[str, Any]


def read_existing_output(
    json_path: Path,
    fetched_at: str,
    *,
    canonicalize_job: Callable[..., dict[str, Any] | None],
    clean_text: Callable[[Any], str],
) -> list[RawJob]:
    if not json_path.exists():
        return []
    try:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    if isinstance(payload, list):
        rows = [row for row in payload if isinstance(row, dict)]
    elif isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
        rows = [row for row in payload["jobs"] if isinstance(row, dict)]
    else:
        return []

    restored: list[RawJob] = []
    for row in rows:
        normalized = canonicalize_job(
            row,
            source=clean_text(row.get("source")) or "previous_output",
            fetched_at=fetched_at,
        )
        if normalized:
            if clean_text(row.get("dedupKey")):
                normalized["dedupKey"] = clean_text(row.get("dedupKey"))
            restored.append(normalized)
    return restored


def serialize_rows_for_json(rows: Sequence[RawJob], fields: Sequence[str]) -> str:
    payload = [{field: row.get(field, "") for field in fields} for row in rows]
    return json.dumps(payload, indent=2, ensure_ascii=False)


def serialize_rows_for_csv(rows: Sequence[RawJob], fields: Sequence[str]) -> str:
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(fields))
    writer.writeheader()
    for row in rows:
        payload: dict[str, Any] = {}
        for field in fields:
            value = row.get(field, "")
            if isinstance(value, (list, dict)):
                value = json.dumps(value, ensure_ascii=False)
            payload[field] = value
        writer.writerow(payload)
    return buffer.getvalue()


def write_text_if_changed(path: Path, text: str) -> bool:
    try:
        existing = path.read_text(encoding="utf-8")
        if existing == text:
            return False
    except OSError:
        pass
    path.write_text(text, encoding="utf-8")
    return True


def write_atomic_if_changed(path: Path, text: str) -> bool:
    """Write text to path atomically (via .tmp + rename) so readers never see partial content."""
    try:
        existing = path.read_text(encoding="utf-8")
        if existing == text:
            return False
    except OSError:
        pass
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp_path.write_text(text, encoding="utf-8")
        tmp_path.replace(path)
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
    return True
