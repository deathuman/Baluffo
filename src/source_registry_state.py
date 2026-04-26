"""Registry state constants and transition helpers."""

from __future__ import annotations

from typing import Any

from src.shared.utils import now_iso
from src.source_registry_identity import ensure_source_id

REGISTRY_STATE_ACTIVE = "active"
REGISTRY_STATE_PENDING = "pending"
REGISTRY_STATE_REJECTED = "rejected"
REGISTRY_STATES = frozenset(
    {REGISTRY_STATE_ACTIVE, REGISTRY_STATE_PENDING, REGISTRY_STATE_REJECTED}
)
REGISTRY_REASON_MANUAL_SOURCE = "manual_source"
REGISTRY_REASON_MANUAL_SOURCE_VARIANT = "manual_source_variant_added"
REGISTRY_REASON_DISCOVERY_AUTO_APPROVE = "discovery_auto_approve"
REGISTRY_REASON_ROLLBACK = "registry_rollback"
REGISTRY_REASON_RESTORE_REJECTED = "registry_restore_rejected"
REGISTRY_REASON_REJECT = "registry_reject"
REGISTRY_REASON_APPROVE = "registry_approve"
REGISTRY_REASON_DELETE = "registry_delete"
REGISTRY_REASON_RESTORE_DELETED = "registry_restore_deleted"
REGISTRY_REASON_FETCH_EMPTY_DEMOTE = "fetch_empty_demote"
REGISTRY_REASON_FETCH_FAILURE_DEMOTE = "fetch_failure_demote"
REGISTRY_REASON_DUPLICATE_FAMILY = "duplicate_family_weaker_variant"
REGISTRY_REASON_REPEATED_ZERO_JOBS = "repeated_zero_jobs"
REGISTRY_MIGRATION_V2 = "registry_migration_v2"
ZERO_JOB_HIDDEN_DEFER_THRESHOLD = 3


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _coerce_state(value: Any, default: str = REGISTRY_STATE_PENDING) -> str:
    token = str(value or "").strip().lower()
    if token in REGISTRY_STATES:
        return token
    return default


def _infer_registry_state(row: dict[str, Any], *, bucket: str = "") -> str:
    bucket_token = str(bucket or "").strip().lower()
    if bucket_token in REGISTRY_STATES:
        return bucket_token
    registry_state = _coerce_state(row.get("registryState"), "")
    if registry_state:
        return registry_state
    candidate_state = str(row.get("candidateState") or "").strip().lower()
    if candidate_state == "live" or bool(row.get("enabledByDefault")):
        return REGISTRY_STATE_ACTIVE
    if candidate_state == "quarantined":
        return REGISTRY_STATE_REJECTED
    if candidate_state == "validated":
        return REGISTRY_STATE_PENDING
    pending_reason = str(row.get("pendingReason") or "").strip().lower()
    if pending_reason:
        return REGISTRY_STATE_PENDING
    quarantine_reason = str(row.get("quarantineReason") or "").strip().lower()
    if quarantine_reason:
        return REGISTRY_STATE_REJECTED
    return REGISTRY_STATE_PENDING


def _infer_pending_reason(row: dict[str, Any], *, registry_state: str, bucket: str = "") -> str:
    current = str(row.get("pendingReason") or "").strip()
    if current:
        return current
    if registry_state == REGISTRY_STATE_ACTIVE:
        return ""
    if registry_state == REGISTRY_STATE_REJECTED:
        return _first_text(
            row.get("quarantineReason"),
            row.get("pendingReason"),
            row.get("reason"),
            REGISTRY_REASON_REJECT,
        )
    bucket_token = str(bucket or "").strip().lower()
    if bucket_token == REGISTRY_STATE_PENDING:
        return _first_text(
            row.get("pendingReason"),
            row.get("discoveryMethod"),
            row.get("manualFallback"),
            REGISTRY_REASON_PENDING_DEFAULT,
        )
    return ""


def _infer_state_changed_at(row: dict[str, Any], *, registry_state: str) -> str:
    return _first_text(
        row.get("stateChangedAt"),
        row.get("approvedAt") if registry_state == REGISTRY_STATE_ACTIVE else "",
        row.get("quarantinedAt") if registry_state == REGISTRY_STATE_REJECTED else "",
        row.get("lastPromotedAt") if registry_state == REGISTRY_STATE_ACTIVE else "",
        row.get("lastDemotedAt") if registry_state != REGISTRY_STATE_ACTIVE else "",
        row.get("manualAddedAt"),
        row.get("discoveredAt"),
        row.get("firstDeferredAt"),
        row.get("lastProbedAt"),
        row.get("updatedAt"),
        row.get("createdAt"),
    )


def _infer_state_changed_by(row: dict[str, Any]) -> str:
    return _first_text(
        row.get("stateChangedBy"),
        row.get("approvedBy"),
        row.get("quarantinedBy"),
        row.get("manualAddedBy"),
        row.get("discoveredBy"),
    )


def _apply_registry_legacy_fields(
    updated: dict[str, Any],
    *,
    registry_state: str,
    state_changed_at: str,
    state_changed_by: str,
    reason: str,
) -> dict[str, Any]:
    updated["registryState"] = registry_state
    updated["pendingReason"] = reason if registry_state != REGISTRY_STATE_ACTIVE else ""
    updated["stateChangedAt"] = state_changed_at
    updated["stateChangedBy"] = state_changed_by
    updated["lastPromotedAt"] = str(updated.get("lastPromotedAt") or "")
    updated["lastDemotedAt"] = str(updated.get("lastDemotedAt") or "")
    if registry_state == REGISTRY_STATE_ACTIVE:
        updated["candidateState"] = "live"
        updated["enabledByDefault"] = True
        updated["approvedAt"] = str(updated.get("approvedAt") or state_changed_at)
        updated["approvedBy"] = str(updated.get("approvedBy") or state_changed_by or "")
        updated["liveAt"] = str(updated.get("liveAt") or state_changed_at)
        updated["quarantinedAt"] = ""
        updated["quarantineReason"] = ""
        updated["lastPromotedAt"] = str(updated.get("lastPromotedAt") or state_changed_at)
    elif registry_state == REGISTRY_STATE_PENDING:
        prior_candidate_state = str(updated.get("candidateState") or "").strip().lower()
        updated["candidateState"] = (
            "hidden"
            if prior_candidate_state == "hidden" or bool(updated.get("hiddenFromDefault"))
            else "validated"
        )
        updated["enabledByDefault"] = False
        updated["approvedAt"] = ""
        updated["approvedBy"] = ""
        updated["liveAt"] = ""
        updated["quarantinedAt"] = ""
        updated["quarantineReason"] = ""
        updated["lastDemotedAt"] = str(updated.get("lastDemotedAt") or state_changed_at)
    else:
        updated["candidateState"] = "quarantined"
        updated["enabledByDefault"] = False
        updated["approvedAt"] = ""
        updated["approvedBy"] = ""
        updated["liveAt"] = ""
        updated["quarantinedAt"] = str(updated.get("quarantinedAt") or state_changed_at)
        updated["quarantineReason"] = str(
            updated.get("quarantineReason") or reason or REGISTRY_REASON_REJECT
        )
        updated["lastDemotedAt"] = str(updated.get("lastDemotedAt") or state_changed_at)
    return updated


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


def _transition_state_metadata(
    row: dict[str, Any],
    *,
    registry_state: str,
    reason: str,
    actor: str,
    at: str,
) -> dict[str, Any]:
    from src.source_registry_canonicalize import canonicalize_registry_row

    updated = canonicalize_registry_row(row, bucket=registry_state)
    updated["registryState"] = registry_state
    updated["pendingReason"] = reason if registry_state != REGISTRY_STATE_ACTIVE else ""
    updated["stateChangedAt"] = at
    updated["stateChangedBy"] = str(actor or "").strip()
    if registry_state == REGISTRY_STATE_ACTIVE:
        updated["lastPromotedAt"] = at
        updated["lastDemotedAt"] = str(updated.get("lastDemotedAt") or "")
        updated["candidateState"] = "live"
        updated["enabledByDefault"] = True
        updated["approvedAt"] = str(updated.get("approvedAt") or at)
        updated["approvedBy"] = str(updated.get("approvedBy") or actor or "")
        updated["liveAt"] = str(updated.get("liveAt") or at)
        updated["quarantinedAt"] = ""
        updated["quarantineReason"] = ""
    elif registry_state == REGISTRY_STATE_PENDING:
        updated["lastDemotedAt"] = at
        prior_candidate_state = str(updated.get("candidateState") or "").strip().lower()
        updated["candidateState"] = (
            "hidden"
            if prior_candidate_state == "hidden" or bool(updated.get("hiddenFromDefault"))
            else "validated"
        )
        updated["enabledByDefault"] = False
        updated["approvedAt"] = ""
        updated["approvedBy"] = ""
        updated["liveAt"] = ""
        updated["quarantinedAt"] = ""
        updated["quarantineReason"] = ""
    else:
        updated["lastDemotedAt"] = at
        updated["candidateState"] = "quarantined"
        updated["enabledByDefault"] = False
        updated["approvedAt"] = ""
        updated["approvedBy"] = ""
        updated["liveAt"] = ""
        updated["quarantinedAt"] = at
        updated["quarantineReason"] = reason or REGISTRY_REASON_REJECT
    return ensure_source_id(updated)


def transition_registry_to_active(
    row: dict[str, Any], *, reason: str, actor: str, at: str | None = None
) -> dict[str, Any]:
    return _transition_state_metadata(
        row,
        registry_state=REGISTRY_STATE_ACTIVE,
        reason=reason,
        actor=actor,
        at=str(at or now_iso()),
    )


def transition_registry_to_pending(
    row: dict[str, Any], *, reason: str, actor: str, at: str | None = None
) -> dict[str, Any]:
    return _transition_state_metadata(
        row,
        registry_state=REGISTRY_STATE_PENDING,
        reason=reason,
        actor=actor,
        at=str(at or now_iso()),
    )


def transition_registry_to_rejected(
    row: dict[str, Any], *, reason: str, actor: str, at: str | None = None
) -> dict[str, Any]:
    return _transition_state_metadata(
        row,
        registry_state=REGISTRY_STATE_REJECTED,
        reason=reason,
        actor=actor,
        at=str(at or now_iso()),
    )


REGISTRY_REASON_PENDING_DEFAULT = REGISTRY_REASON_MANUAL_SOURCE


def is_hidden_from_default(row: dict[str, Any]) -> bool:
    return (
        bool(row.get("hiddenFromDefault"))
        or str(row.get("candidateState") or "").strip().lower() == "hidden"
    )


def hide_repeated_zero_job_pending(
    row: dict[str, Any],
    *,
    threshold: int = ZERO_JOB_HIDDEN_DEFER_THRESHOLD,
    actor: str = "discovery_zero_job_policy",
    at: str | None = None,
) -> dict[str, Any]:
    updated = ensure_source_id(dict(row))
    registry_state = _infer_registry_state(updated, bucket=REGISTRY_STATE_PENDING)
    if registry_state == REGISTRY_STATE_ACTIVE:
        return updated
    jobs_found = max(
        0,
        _coerce_int(updated.get("jobsFound"), 0),
        _coerce_int(updated.get("sampleCount"), 0),
    )
    if jobs_found > 0 or _coerce_int(updated.get("deferCount"), 0) < int(threshold):
        return updated
    timestamp = str(at or now_iso())
    updated["registryState"] = REGISTRY_STATE_PENDING
    updated["candidateState"] = "hidden"
    updated["hiddenFromDefault"] = True
    updated["pendingReason"] = REGISTRY_REASON_REPEATED_ZERO_JOBS
    updated["stateChangedAt"] = str(updated.get("stateChangedAt") or timestamp)
    updated["stateChangedBy"] = str(updated.get("stateChangedBy") or actor)
    updated["lastDemotedAt"] = str(updated.get("lastDemotedAt") or timestamp)
    return updated
