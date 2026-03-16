"""
Validation helpers for data contracts (CanonicalJob, etc.).
Used at pipeline output and bridge boundaries to catch shape errors early.
See docs/DATA_CONTRACT.md for field definitions.
"""
from __future__ import annotations

from typing import Any, Dict, List


def validate_canonical_jobs_payload(rows: List[Any]) -> List[Dict[str, Any]]:
    """
    Validate that each item is a dict (CanonicalJob-like). Raises ValueError if any row is not a dict.
    Call before writing jobs-unified.json to catch shape errors early.
    See docs/DATA_CONTRACT.md for full CanonicalJob schema.
    """
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"jobs payload index {i}: expected dict, got {type(row).__name__}")
        out.append(row)
    return out
