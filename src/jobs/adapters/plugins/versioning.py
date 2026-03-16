from __future__ import annotations

from typing import Any


def normalize_schema_version(value: Any, *, default: int = 1) -> int:
    """Normalize schema version values like 1, 1.0, or '1.0' to an int."""
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(str(value)))
        except (TypeError, ValueError):
            return int(default)

