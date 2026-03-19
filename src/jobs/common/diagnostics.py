"""Diagnostics helpers shared across jobs adapters."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from src.jobs.common.config import SOURCE_DIAGNOSTICS
from src.jobs.text_utils import clean_text


def set_source_diagnostics(
    source_name: str,
    *,
    adapter: str,
    studio: str,
    details: Optional[List[Dict[str, Any]]] = None,
    partial_errors: Optional[List[str]] = None,
) -> None:
    SOURCE_DIAGNOSTICS[source_name] = {
        "adapter": clean_text(adapter) or "unknown",
        "studio": clean_text(studio) or "multiple",
        "details": details or [],
        "partialErrors": partial_errors or [],
    }


__all__ = ["SOURCE_DIAGNOSTICS", "set_source_diagnostics"]
