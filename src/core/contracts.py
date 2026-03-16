"""
Validation helpers for data contracts (CanonicalJob, etc.).
Used at pipeline output and bridge boundaries to catch shape errors early.
See docs/DATA_CONTRACT.md for field definitions.
Pydantic schemas live in src/core/schemas.py.
"""
from __future__ import annotations

from typing import Any, Dict, List

from pydantic import ValidationError as PydanticValidationError  # noqa: PLC2701

from src.core.schemas import CanonicalJobSchema


def validate_canonical_jobs_payload(rows: List[Any]) -> List[Dict[str, Any]]:
    """
    Validate that each item is a dict and conforms to CanonicalJob-like shape (Pydantic).
    Raises ValueError if any row is not a dict or fails schema validation.
    Call before writing jobs-unified.json to catch shape errors early.
    See docs/DATA_CONTRACT.md for full CanonicalJob schema.
    """
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"jobs payload index {i}: expected dict, got {type(row).__name__}")
        try:
            CanonicalJobSchema.model_validate(row)
        except PydanticValidationError as e:
            raise ValueError(f"jobs payload index {i}: invalid CanonicalJob shape: {e!s}") from e
        out.append(row)
    return out
