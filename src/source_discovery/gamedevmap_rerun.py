"""Rerun-selection helpers extracted from ``gamedevmap_active_dry_run.py``.

All functions delegate to ``active_audit_runtime`` — no coordinator import.
"""

from __future__ import annotations

from typing import Any

from . import active_audit_runtime
from .gamedevmap_rejection import _rejection_row_key, _row_url

GAMEDEVMAP_RERUN_REASONS = {
    "homepage_fetch_failed",
    "no_careers_evidence",
    "probe_failed",
    "zero_jobs",
}


# pure helper
def _parse_rerun_reasons(value: str | list[str] | tuple[str, ...] | None) -> set[str]:
    if not value:
        return set()
    raw_items: list[str] = []
    if isinstance(value, str):
        raw_items = value.split(",")
    else:
        raw_items = [str(item) for item in value]
    return {
        item.strip()
        for item in raw_items
        if item.strip() and item.strip() in GAMEDEVMAP_RERUN_REASONS
    }


# pure helper
def _row_keys(row: dict[str, Any]) -> set[str]:
    return active_audit_runtime.row_identity_keys(
        row,
        url=_row_url(row),
        entry_url=str(row.get("sourceDirectoryEntryUrl") or "").strip(),
    )


# pure helper
def _select_rerun_rows(
    artifact: dict[str, Any],
    representative_rows: list[dict[str, Any]],
    rerun_reasons: set[str],
) -> tuple[list[dict[str, Any]], set[str]]:
    return active_audit_runtime.select_rerun_rows(
        artifact,
        representative_rows,
        rerun_reasons,
        rejected_key="rejectedForActivation",
        rejection_key_fn=_rejection_row_key,
        row_keys_fn=_row_keys,
    )


# pure helper
def _prune_rerun_rejections(
    artifact: dict[str, Any],
    *,
    rerun_reasons: set[str],
    rerun_row_keys: set[str],
) -> None:
    active_audit_runtime.prune_rerun_rejections(
        artifact,
        rejected_key="rejectedForActivation",
        rerun_reasons=rerun_reasons,
        rerun_row_keys=rerun_row_keys,
        rejection_key_fn=_rejection_row_key,
    )


__all__ = [
    "GAMEDEVMAP_RERUN_REASONS",
    "_parse_rerun_reasons",
    "_row_keys",
    "_select_rerun_rows",
    "_prune_rerun_rejections",
]
