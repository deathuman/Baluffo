"""Diagnostics helpers shared across jobs adapters."""

from __future__ import annotations

from typing import Any

from src.jobs.common.config import SOURCE_DIAGNOSTICS
from src.jobs.text_utils import clean_text


def set_source_diagnostics(
    source_name: str,
    *,
    adapter: str,
    studio: str,
    provider_url: str = "",
    details: list[dict[str, Any]] | None = None,
    partial_errors: list[str] | None = None,
) -> None:
    SOURCE_DIAGNOSTICS[source_name] = {
        "adapter": clean_text(adapter) or "unknown",
        "studio": clean_text(studio) or "multiple",
        "providerUrl": clean_text(provider_url),
        "details": details or [],
        "partialErrors": partial_errors or [],
    }


__all__ = ["SOURCE_DIAGNOSTICS", "set_source_diagnostics"]
