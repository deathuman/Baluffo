"""Canonicalization helpers for source registry rows."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from src.source_registry_identity import ensure_source_id, source_identity
from src.source_registry_state import (
    REGISTRY_MIGRATION_V2,
    REGISTRY_STATE_ACTIVE,
    _apply_registry_legacy_fields,
    _infer_pending_reason,
    _infer_registry_state,
    _infer_state_changed_at,
    _infer_state_changed_by,
)


def canonicalize_registry_row(row: dict[str, Any], *, bucket: str = "") -> dict[str, Any]:
    normalized = dict(row)
    normalized = ensure_source_id(normalized)
    registry_state = _infer_registry_state(normalized, bucket=bucket)
    state_changed_at = _infer_state_changed_at(normalized, registry_state=registry_state)
    state_changed_by = _infer_state_changed_by(normalized)
    if state_changed_at and not state_changed_by:
        state_changed_by = REGISTRY_MIGRATION_V2
    reason = _infer_pending_reason(normalized, registry_state=registry_state, bucket=bucket)
    normalized = _apply_registry_legacy_fields(
        normalized,
        registry_state=registry_state,
        state_changed_at=state_changed_at,
        state_changed_by=state_changed_by,
        reason=reason,
    )
    normalized["registryState"] = registry_state
    normalized["pendingReason"] = reason if registry_state != REGISTRY_STATE_ACTIVE else ""
    normalized["stateChangedAt"] = state_changed_at
    normalized["stateChangedBy"] = state_changed_by
    if (
        registry_state == REGISTRY_STATE_ACTIVE
        and not str(normalized.get("lastPromotedAt") or "").strip()
    ):
        normalized["lastPromotedAt"] = state_changed_at
    if (
        registry_state != REGISTRY_STATE_ACTIVE
        and not str(normalized.get("lastDemotedAt") or "").strip()
    ):
        normalized["lastDemotedAt"] = state_changed_at
    return normalized


def sort_sources_by_identity(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        (ensure_source_id(dict(row)) for row in rows if isinstance(row, dict)),
        key=lambda row: (
            source_identity(row),
            str(row.get("stateChangedAt") or ""),
            str(row.get("lastPromotedAt") or ""),
            str(row.get("lastDemotedAt") or ""),
        ),
    )
