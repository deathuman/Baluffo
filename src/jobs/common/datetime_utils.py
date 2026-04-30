"""Datetime parsing/formatting helpers for jobs pipeline payloads."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from src.jobs.text_utils import clean_text


def _parse_epoch_timestamp(value: int | float) -> datetime | None:
    num = float(value)
    if num <= 0:
        return None
    if num > 10_000_000_000:
        num /= 1000.0
    try:
        return datetime.fromtimestamp(num, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _parse_iso_datetime_text(text: str) -> datetime | None:
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return _parse_epoch_timestamp(value)
    text = clean_text(value)
    if not text:
        return None
    if re.fullmatch(r"\d{10,13}", text):
        return parse_datetime(int(text))
    return _parse_iso_datetime_text(text)


def to_iso(value: Any) -> str:
    dt = parse_datetime(value)
    return dt.isoformat() if dt else ""


def posted_ts(value: Any) -> float:
    dt = parse_datetime(value)
    return dt.timestamp() if dt else 0.0
