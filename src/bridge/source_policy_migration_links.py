from __future__ import annotations

from typing import Any

from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import as_json_list, as_json_object
from src.source_discovery.config import SUPPORTED_PROVIDERS

ADMIN_MIGRATION_LINK_ACTOR = "admin_provider_link_backfill"
MIGRATION_LINK_SOURCE_DEFAULT = "provider_coverage_link_backfill"
APPLY_MIGRATION_IDENTITY_LINK = "apply_migration_identity_link"
CLEAR_MIGRATION_IDENTITY_LINK = "clear_migration_identity_link"
SUPPORTED_ACTIONS = frozenset({APPLY_MIGRATION_IDENTITY_LINK, CLEAR_MIGRATION_IDENTITY_LINK})
ALLOWED_APPLY_RECOMMENDATIONS = frozenset(
    {"", "backfill_migration_identity_candidate", "needs_review"}
)
REJECTED_RECOMMENDATIONS = frozenset({"ambiguous_static_match", "insufficient_evidence"})
STATIC_LIKE_ADAPTERS = frozenset(
    {"static", "scrapy_static", "generic_static", "seed_careers_page", "sheet_directory"}
)
STATIC_LIKE_STAGES = frozenset({"generic_static", "seed_careers_page", "sheet_directory"})
MIGRATION_LINK_FIELDS = (
    "migrationSourceIdentity",
    "migrationSourceName",
    "migrationConfidence",
    "migrationReasons",
    "migrationLinkedAt",
    "migrationLinkedBy",
    "migrationLinkSource",
)


def _source_id(api: Any, row: dict[str, Any]) -> str:
    for key in ("id", "sourceId", "sourceIdentity"):
        value = clean_text(row.get(key))
        if value:
            return value
    try:
        return clean_text(api.source_identity(row))
    except Exception:  # noqa: BLE001
        return ""


def _row_matches(api: Any, row: dict[str, Any], source_id: str) -> bool:
    target = norm_text(source_id)
    if not target:
        return False
    tokens = {
        norm_text(row.get("id")),
        norm_text(row.get("sourceId")),
        norm_text(row.get("sourceIdentity")),
        norm_text(_source_id(api, row)),
    }
    return target in tokens


def _find_row(
    api: Any, state: dict[str, list[dict[str, Any]]], source_id: str, *, buckets: tuple[str, ...]
) -> tuple[str, int, dict[str, Any]] | None:
    for bucket in buckets:
        rows = state.get(bucket) or []
        for index, row in enumerate(rows):
            if isinstance(row, dict) and _row_matches(api, row, source_id):
                return bucket, index, row
    return None


def _is_static_like(row: dict[str, Any]) -> bool:
    adapter = norm_text(row.get("adapter") or row.get("currentAdapter"))
    stage = norm_text(row.get("discoveryStage") or row.get("discoveryMethod"))
    return adapter in STATIC_LIKE_ADAPTERS or stage in STATIC_LIKE_STAGES


def _confidence(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _reasons(value: Any) -> list[str]:
    return [clean_text(item) for item in as_json_list(value) if clean_text(item)]


def _response(
    *,
    action: str,
    provider_bucket: str,
    provider_source_id: str,
    static_source_id: str,
    provider_row: dict[str, Any],
    changed: bool,
    warning: str = "",
) -> dict[str, Any]:
    return {
        "ok": True,
        "changed": changed,
        "action": action,
        "providerBucket": provider_bucket,
        "providerSourceId": provider_source_id,
        "staticSourceId": static_source_id,
        "providerRow": provider_row,
        **({"warning": warning} if warning else {}),
    }


def _validate_apply_payload(data: dict[str, Any]) -> tuple[str, str, float, str, list[str]]:
    provider_source_id = clean_text(data.get("providerSourceId"))
    static_source_id = clean_text(data.get("staticSourceId"))
    if not provider_source_id:
        raise ValueError("providerSourceId required")
    if not static_source_id:
        raise ValueError("staticSourceId required")
    recommendation = norm_text(data.get("recommendedAction"))
    blockers = {norm_text(item) for item in as_json_list(data.get("blockers")) if norm_text(item)}
    if recommendation in REJECTED_RECOMMENDATIONS or blockers & REJECTED_RECOMMENDATIONS:
        raise ValueError(recommendation or sorted(blockers & REJECTED_RECOMMENDATIONS)[0])
    if recommendation not in ALLOWED_APPLY_RECOMMENDATIONS:
        raise ValueError("unsupported_recommended_action")
    confidence = _confidence(data.get("confidence"))
    if confidence < 0.75:
        raise ValueError("migration_identity_confidence_below_threshold")
    return (
        provider_source_id,
        static_source_id,
        confidence,
        recommendation,
        _reasons(data.get("reasons")),
    )


def _apply_link(
    api: Any, state: dict[str, list[dict[str, Any]]], data: dict[str, Any]
) -> dict[str, Any]:
    provider_source_id, static_source_id, confidence, _recommendation, reasons = (
        _validate_apply_payload(data)
    )
    provider_match = _find_row(api, state, provider_source_id, buckets=("active", "pending"))
    if provider_match is None:
        raise ValueError("provider_source_not_found")
    provider_bucket, provider_index, provider_row = provider_match
    if norm_text(provider_row.get("adapter")) not in {
        norm_text(item) for item in SUPPORTED_PROVIDERS
    }:
        raise ValueError("unsupported_provider_adapter")
    static_match = _find_row(api, state, static_source_id, buckets=("active", "pending"))
    if static_match is None:
        raise ValueError("static_source_not_found")
    _static_bucket, _static_index, static_row = static_match
    if not _is_static_like(static_row):
        raise ValueError("static_source_not_static_like")
    current_static = clean_text(provider_row.get("migrationSourceIdentity"))
    if current_static and norm_text(current_static) != norm_text(static_source_id):
        raise ValueError("migration_identity_already_linked_to_different_static_source")
    if current_static:
        return _response(
            action=APPLY_MIGRATION_IDENTITY_LINK,
            provider_bucket=provider_bucket,
            provider_source_id=_source_id(api, provider_row),
            static_source_id=static_source_id,
            provider_row=dict(provider_row),
            changed=False,
            warning="migration_identity_already_linked",
        )

    updated = dict(provider_row)
    linked_at = clean_text(getattr(api, "now_iso", lambda: "")() or "")
    static_name = clean_text(data.get("staticSourceName")) or clean_text(static_row.get("name"))
    updated["migrationSourceIdentity"] = static_source_id
    updated["migrationSourceName"] = static_name
    updated["migrationConfidence"] = round(confidence, 2)
    updated["migrationReasons"] = reasons
    updated["migrationLinkedAt"] = linked_at
    updated["migrationLinkedBy"] = ADMIN_MIGRATION_LINK_ACTOR
    updated["migrationLinkSource"] = (
        clean_text(data.get("recommendationSource")) or MIGRATION_LINK_SOURCE_DEFAULT
    )
    updated["stateChangedAt"] = linked_at or clean_text(updated.get("stateChangedAt"))
    updated["stateChangedBy"] = ADMIN_MIGRATION_LINK_ACTOR
    state[provider_bucket][provider_index] = updated
    persisted = api.persist_state_and_auto_sync(
        state, reason="source_policy_migration_identity_link"
    )
    persisted_match = _find_row(api, persisted, provider_source_id, buckets=(provider_bucket,))
    persisted_row = dict(persisted_match[2]) if persisted_match else updated
    return _response(
        action=APPLY_MIGRATION_IDENTITY_LINK,
        provider_bucket=provider_bucket,
        provider_source_id=_source_id(api, persisted_row),
        static_source_id=static_source_id,
        provider_row=persisted_row,
        changed=True,
    )


def _clear_link(
    api: Any, state: dict[str, list[dict[str, Any]]], data: dict[str, Any]
) -> dict[str, Any]:
    provider_source_id = clean_text(data.get("providerSourceId"))
    static_source_id = clean_text(data.get("staticSourceId"))
    if not provider_source_id:
        raise ValueError("providerSourceId required")
    if not static_source_id:
        raise ValueError("staticSourceId required")
    provider_match = _find_row(api, state, provider_source_id, buckets=("active", "pending"))
    if provider_match is None:
        raise ValueError("provider_source_not_found")
    provider_bucket, provider_index, provider_row = provider_match
    current_static = clean_text(provider_row.get("migrationSourceIdentity"))
    if not current_static:
        raise ValueError("migration_identity_not_linked")
    if norm_text(current_static) != norm_text(static_source_id):
        raise ValueError("migration_identity_static_source_mismatch")
    if clean_text(provider_row.get("migrationLinkedBy")) != ADMIN_MIGRATION_LINK_ACTOR:
        raise ValueError("migration_identity_not_owned_by_backfill_action")
    updated = dict(provider_row)
    for field in MIGRATION_LINK_FIELDS:
        updated.pop(field, None)
    linked_at = clean_text(getattr(api, "now_iso", lambda: "")() or "")
    updated["stateChangedAt"] = linked_at or clean_text(updated.get("stateChangedAt"))
    updated["stateChangedBy"] = ADMIN_MIGRATION_LINK_ACTOR
    state[provider_bucket][provider_index] = updated
    persisted = api.persist_state_and_auto_sync(
        state, reason="source_policy_migration_identity_clear"
    )
    persisted_match = _find_row(api, persisted, provider_source_id, buckets=(provider_bucket,))
    persisted_row = dict(persisted_match[2]) if persisted_match else updated
    return _response(
        action=CLEAR_MIGRATION_IDENTITY_LINK,
        provider_bucket=provider_bucket,
        provider_source_id=_source_id(api, persisted_row),
        static_source_id=static_source_id,
        provider_row=persisted_row,
        changed=True,
    )


def apply_source_policy_migration_link_action(api: Any, payload: Any) -> dict[str, Any]:
    data = as_json_object(payload)
    action = norm_text(data.get("action"))
    if action not in SUPPORTED_ACTIONS:
        raise ValueError("unsupported_migration_identity_action")
    state = {
        bucket: [dict(row) for row in rows if isinstance(row, dict)]
        for bucket, rows in (api.load_state() or {}).items()
    }
    state.setdefault("active", [])
    state.setdefault("pending", [])
    state.setdefault("rejected", [])
    if action == APPLY_MIGRATION_IDENTITY_LINK:
        return _apply_link(api, state, data)
    return _clear_link(api, state, data)


__all__ = [
    "ADMIN_MIGRATION_LINK_ACTOR",
    "APPLY_MIGRATION_IDENTITY_LINK",
    "CLEAR_MIGRATION_IDENTITY_LINK",
    "apply_source_policy_migration_link_action",
]
