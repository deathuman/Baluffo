"""Shared utilities used across Baluffo (no imports from jobs/bridge/admin_bridge)."""

from __future__ import annotations

import os
import re
from datetime import UTC, datetime
from typing import Any


def now_utc() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(UTC)


def now_iso() -> str:
    """Return current UTC datetime as ISO 8601 string."""
    return now_utc().isoformat()


def utc_now_iso() -> str:
    """Alias for now_iso(); same behavior for scripts and packaged probes."""
    return now_iso()


def parse_iso(value: Any) -> datetime | None:
    """Parse an ISO timestamp and normalize it to UTC when possible."""
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def env_flag(name: str, default: bool) -> bool:
    """Parse env var as boolean: 1/true/yes/on -> True, 0/false/no/off -> False, else default."""
    raw = str(os.getenv(name) or "").strip()
    raw = re.sub(r"\s+", " ", raw).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def coerce_bool(value: Any, default: bool) -> bool:
    """Coerce value to bool; 1/true/yes/on -> True, 0/false/no/off -> False, else default."""
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def coerce_int(
    value: Any,
    default: int,
    *,
    minimum: int = 1,
    maximum: int = 65535,
) -> int:
    """Coerce value to int and clamp to [minimum, maximum]."""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(minimum, min(maximum, parsed))


def int_or_default(value: Any, default: int = 0) -> int:
    """Return int(value) when possible, otherwise the provided default."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def coerce_str(value: Any, default: str) -> str:
    """Coerce value to non-empty str; return default if stripped empty."""
    text = str(value or "").strip()
    return text or str(default)


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_dict_rows(value: Any) -> list[dict[str, Any]]:
    return [item for item in _as_list(value) if isinstance(item, dict)]
