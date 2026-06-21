"""Local source-policy review/override state helpers.

AI boundary owns: source-policy review-state contract loading, merging, and override rows.
AI boundary implement in: this file for review-state semantics; recommendation generation and bridge actions stay elsewhere.
AI boundary search before contracts: source-policy routes, migration links, recommendation contracts, and review-state tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused source-policy review-state tests.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import as_json_object
from src.shared.utils import parse_iso

SOURCE_POLICY_REVIEW_STATE_SCHEMA_VERSION = "1.0"
SOURCE_POLICY_REVIEW_STATES = frozenset({"new", "acknowledged", "reviewed", "snoozed"})
SOURCE_POLICY_MANUAL_SUPPRESSION_OVERRIDES = frozenset({"none", "force_pause"})
SOURCE_POLICY_REVIEW_ACTIONS = frozenset(
    {"acknowledge", "reviewed", "snooze", "force_pause", "clear_override"}
)
_PAIR_KEY_SEPARATOR = "||"
_NOTES_LIMIT = 500
_ACTOR_LIMIT = 80


def source_policy_review_pair_key(*, static_source_id: str, provider_source_id: str) -> str:
    static_key = norm_text(static_source_id)
    provider_key = norm_text(provider_source_id)
    if not static_key or not provider_key:
        return ""
    return f"{static_key}{_PAIR_KEY_SEPARATOR}{provider_key}"


def _review_state(value: Any) -> str:
    state = norm_text(value)
    return state if state in SOURCE_POLICY_REVIEW_STATES else "new"


def _manual_override(value: Any) -> str:
    override = norm_text(value)
    return override if override in SOURCE_POLICY_MANUAL_SUPPRESSION_OVERRIDES else "none"


def _bounded_text(value: Any, limit: int) -> str:
    return clean_text(value)[:limit]


def _parseable_iso(value: str) -> bool:
    text = clean_text(value)
    return parse_iso(text) is not None


def normalize_source_policy_review_pair(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    static_source_id = clean_text(src.get("staticSourceId"))
    provider_source_id = clean_text(src.get("providerSourceId"))
    review_state = _review_state(src.get("reviewState"))
    snoozed_until = clean_text(src.get("snoozedUntil"))
    if review_state != "snoozed":
        snoozed_until = ""
    return {
        "staticSourceId": static_source_id,
        "staticSourceName": clean_text(src.get("staticSourceName")),
        "providerSourceId": provider_source_id,
        "providerSourceName": clean_text(src.get("providerSourceName")),
        "reviewState": review_state,
        "manualSuppressionOverride": _manual_override(src.get("manualSuppressionOverride")),
        "snoozedUntil": snoozed_until,
        "notes": _bounded_text(src.get("notes"), _NOTES_LIMIT),
        "updatedAt": clean_text(src.get("updatedAt")),
        "updatedBy": _bounded_text(src.get("updatedBy"), _ACTOR_LIMIT),
    }


def _summary_from_pairs(pairs: dict[str, dict[str, Any]]) -> dict[str, int]:
    rows = list(pairs.values())
    review_counts = Counter(row["reviewState"] for row in rows)
    return {
        "totalPairs": len(rows),
        "acknowledgedCount": review_counts.get("acknowledged", 0),
        "reviewedCount": review_counts.get("reviewed", 0),
        "snoozedCount": review_counts.get("snoozed", 0),
        "forcePausedCount": sum(
            1 for row in rows if row["manualSuppressionOverride"] == "force_pause"
        ),
    }


def normalize_source_policy_review_state_artifact(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    raw_pairs = as_json_object(src.get("pairs"))
    pairs: dict[str, dict[str, Any]] = {}
    for raw_key, raw_row in raw_pairs.items():
        row = normalize_source_policy_review_pair(raw_row)
        key = source_policy_review_pair_key(
            static_source_id=row["staticSourceId"],
            provider_source_id=row["providerSourceId"],
        )
        if not key:
            key = clean_text(raw_key)
        if key:
            pairs[key] = row
    return {
        "schemaVersion": clean_text(src.get("schemaVersion"))
        or SOURCE_POLICY_REVIEW_STATE_SCHEMA_VERSION,
        "updatedAt": clean_text(src.get("updatedAt")),
        "summary": _summary_from_pairs(pairs),
        "pairs": pairs,
    }


def read_source_policy_review_state_artifact(path: Path) -> tuple[dict[str, Any], str]:
    artifact_path = Path(path)
    if not artifact_path.exists():
        return normalize_source_policy_review_state_artifact({}), "missing_review_state_artifact"
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return normalize_source_policy_review_state_artifact({}), "malformed_review_state_artifact"
    if not isinstance(payload, dict):
        return normalize_source_policy_review_state_artifact({}), "malformed_review_state_artifact"
    return normalize_source_policy_review_state_artifact(payload), ""


def find_source_policy_review_pair(
    review_state: Any,
    *,
    static_source_id: str,
    static_source_name: str = "",
    provider_source_id: str,
    provider_source_name: str = "",
) -> dict[str, Any]:
    src = normalize_source_policy_review_state_artifact(review_state)
    static_tokens = {norm_text(static_source_id), norm_text(static_source_name)} - {""}
    provider_tokens = {norm_text(provider_source_id), norm_text(provider_source_name)} - {""}
    for row in src["pairs"].values():
        pair_static = {
            norm_text(row.get("staticSourceId")),
            norm_text(row.get("staticSourceName")),
        } - {""}
        pair_provider = {
            norm_text(row.get("providerSourceId")),
            norm_text(row.get("providerSourceName")),
        } - {""}
        if static_tokens & pair_static and provider_tokens & pair_provider:
            return normalize_source_policy_review_pair(row)
    return {}


def source_policy_review_pair_public_fields(row: Any) -> dict[str, Any]:
    pair = normalize_source_policy_review_pair(row)
    return {
        "reviewState": pair["reviewState"],
        "manualSuppressionOverride": pair["manualSuppressionOverride"],
        "snoozedUntil": pair["snoozedUntil"],
        "notes": pair["notes"],
        "reviewUpdatedAt": pair["updatedAt"],
        "reviewUpdatedBy": pair["updatedBy"],
    }


def apply_source_policy_review_action(
    *,
    prior_artifact: Any,
    action_payload: Any,
    updated_at: str,
    default_updated_by: str = "admin",
) -> tuple[dict[str, Any], dict[str, Any]]:
    artifact = normalize_source_policy_review_state_artifact(prior_artifact)
    payload = as_json_object(action_payload)
    action = norm_text(payload.get("action"))
    if action not in SOURCE_POLICY_REVIEW_ACTIONS:
        raise ValueError("invalid source policy action")
    static_source_id = clean_text(payload.get("staticSourceId"))
    provider_source_id = clean_text(payload.get("providerSourceId"))
    if not static_source_id or not provider_source_id:
        raise ValueError("staticSourceId and providerSourceId are required")
    snoozed_until = clean_text(payload.get("snoozedUntil"))
    if action == "snooze" and not _parseable_iso(snoozed_until):
        raise ValueError("snoozedUntil must be a parseable ISO timestamp")
    key = source_policy_review_pair_key(
        static_source_id=static_source_id,
        provider_source_id=provider_source_id,
    )
    prior_pair = normalize_source_policy_review_pair(artifact["pairs"].get(key, {}))
    pair = {
        **prior_pair,
        "staticSourceId": static_source_id,
        "staticSourceName": clean_text(payload.get("staticSourceName"))
        or prior_pair["staticSourceName"],
        "providerSourceId": provider_source_id,
        "providerSourceName": clean_text(payload.get("providerSourceName"))
        or prior_pair["providerSourceName"],
        "updatedAt": clean_text(updated_at),
        "updatedBy": _bounded_text(payload.get("updatedBy") or default_updated_by, _ACTOR_LIMIT),
        "notes": _bounded_text(payload.get("notes"), _NOTES_LIMIT)
        if "notes" in payload
        else prior_pair["notes"],
    }
    if action == "acknowledge":
        pair["reviewState"] = "acknowledged"
    elif action == "reviewed":
        pair["reviewState"] = "reviewed"
    elif action == "snooze":
        pair["reviewState"] = "snoozed"
        pair["snoozedUntil"] = snoozed_until
    elif action == "force_pause":
        pair["manualSuppressionOverride"] = "force_pause"
    elif action == "clear_override":
        pair["manualSuppressionOverride"] = "none"
    if action != "snooze" and pair.get("reviewState") != "snoozed":
        pair["snoozedUntil"] = ""
    normalized_pair = normalize_source_policy_review_pair(pair)
    artifact["pairs"][key] = normalized_pair
    next_artifact = {
        "schemaVersion": SOURCE_POLICY_REVIEW_STATE_SCHEMA_VERSION,
        "updatedAt": clean_text(updated_at),
        "pairs": artifact["pairs"],
    }
    return normalize_source_policy_review_state_artifact(next_artifact), normalized_pair
