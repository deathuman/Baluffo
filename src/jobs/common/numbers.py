"""Small numeric coercion helpers shared across jobs modules."""

from __future__ import annotations

from typing import Any


def _clamped_int(value: Any, default: int = 0, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(int(minimum), int(parsed))

