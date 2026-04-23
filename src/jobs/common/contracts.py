"""Stable jobs contract compatibility surface."""

from __future__ import annotations

from .contracts_fetch_report import normalize_fetch_report_payload
from .contracts_runtime import normalize_runtime_payload
from .contracts_source_reports import normalize_source_report_row
from .contracts_task_state import normalize_task_state_payload

__all__ = [
    "normalize_fetch_report_payload",
    "normalize_runtime_payload",
    "normalize_source_report_row",
    "normalize_task_state_payload",
]
