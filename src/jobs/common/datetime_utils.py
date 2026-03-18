"""Datetime parsing/formatting helpers for jobs pipeline payloads."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Optional

from src.jobs.text_utils import clean_text


def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        num = float(value)
        if num <= 0:
            return None
        if num > 10_000_000_000:
            num /= 1000.0
        try:
            return datetime.fromtimestamp(num, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    text = clean_text(value)
    if not text:
        return None
    if re.fullmatch(r"\d{10,13}", text):
        return parse_datetime(int(text))
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_iso(value: Any) -> str:
    dt = parse_datetime(value)
    return dt.isoformat() if dt else ""


def posted_ts(value: Any) -> float:
    dt = parse_datetime(value)
    return dt.timestamp() if dt else 0.0

