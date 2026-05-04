from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.source_registry import source_identity
from src.source_registry_policy import duplicate_family_conflict_cards

SOURCE_HEALTH_FIELD_NAMES = (
    "healthScore",
    "lastStatus",
    "lastRunAt",
    "lastCheckedAt",
    "lastSuccessAt",
    "lastSuccessfulFetchAt",
    "lastSeenInFetchAt",
    "lastKeptCount",
    "lastJobsKept",
    "consecutiveFailures",
    "failureCount",
    "consecutiveZeroKept",
    "zeroJobStreak",
    "health",
    "healthReason",
)

CONFLICT_DIFF_FIELDS = (
    "name",
    "sourceId",
    "id",
    "registryState",
    "candidateState",
    "transitionReason",
    "pendingReason",
    "quarantineReason",
    "stateChangedAt",
    "stateChangedBy",
    "lastPromotedAt",
    "lastDemotedAt",
    "duplicateFamilyKey",
    "duplicateOfSourceId",
    "duplicateOfSourceName",
    "adapter",
    "jobsFound",
    "rankScore",
    "score",
    "lastStatus",
    "lastRunAt",
    "lastCheckedAt",
    "lastSuccessAt",
    "lastSuccessfulFetchAt",
    "lastSeenInFetchAt",
    "lastKeptCount",
    "lastJobsKept",
    "consecutiveFailures",
    "failureCount",
    "consecutiveZeroKept",
    "zeroJobStreak",
    "health",
    "healthReason",
)

CONFLICT_ACTIONS_BY_STATE = {
    "active": ({"action": "demote-active", "label": "Demote", "route": "/registry/demote-active"},),
    "pending": (
        {"action": "approve", "label": "Promote", "route": "/registry/approve"},
        {"action": "reject", "label": "Reject", "route": "/registry/reject"},
    ),
    "rejected": (
        {"action": "restore-rejected", "label": "Restore", "route": "/registry/restore-rejected"},
    ),
}

_FIELD_LABELS = {
    "name": "Name",
    "sourceId": "Source ID",
    "id": "ID",
    "registryState": "Registry state",
    "candidateState": "Candidate state",
    "transitionReason": "Transition reason",
    "pendingReason": "Pending reason",
    "quarantineReason": "Quarantine reason",
    "stateChangedAt": "State changed at",
    "stateChangedBy": "State changed by",
    "lastPromotedAt": "Last promoted at",
    "lastDemotedAt": "Last demoted at",
    "duplicateFamilyKey": "Duplicate family",
    "duplicateOfSourceId": "Duplicate of source ID",
    "duplicateOfSourceName": "Duplicate of source name",
    "adapter": "Adapter",
    "jobsFound": "Jobs found",
    "rankScore": "Rank score",
    "score": "Score",
    "lastStatus": "Last status",
    "lastRunAt": "Last run at",
    "lastCheckedAt": "Last checked at",
    "lastSuccessAt": "Last success at",
    "lastSuccessfulFetchAt": "Last successful fetch at",
    "lastSeenInFetchAt": "Last seen in fetch at",
    "lastKeptCount": "Last kept count",
    "lastJobsKept": "Last jobs kept",
    "consecutiveFailures": "Consecutive failures",
    "failureCount": "Failure count",
    "consecutiveZeroKept": "Consecutive zero-kept",
    "zeroJobStreak": "Zero-job streak",
    "health": "Health",
    "healthReason": "Health reason",
}


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _json_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, sort_keys=True, ensure_ascii=False)
        except TypeError:
            return str(value)
    return str(value or "").strip()


def _source_state_rows_by_name(source_state_payload: Any) -> dict[str, dict[str, Any]]:
    rows = _as_dict(_as_dict(source_state_payload).get("sources"))
    return {str(key).strip().lower(): row for key, row in rows.items() if isinstance(row, dict)}


def _source_state_row_for_registry_row(
    row: dict[str, Any], source_state_rows: dict[str, dict[str, Any]]
) -> tuple[dict[str, Any], str]:
    for key in (
        _clean_text(row.get("name")),
        _clean_text(row.get("sourceId")),
        _clean_text(row.get("id")),
        source_identity(row),
    ):
        lookup = key.strip().lower()
        if lookup and lookup in source_state_rows:
            return source_state_rows[lookup], lookup
    return {}, ""


def _row_actions(row: dict[str, Any]) -> list[dict[str, Any]]:
    state = _clean_text(row.get("registryState") or row.get("candidateState")).lower()
    row_id = _clean_text(row.get("id") or row.get("sourceId") or source_identity(row))
    actions = [dict(action) for action in CONFLICT_ACTIONS_BY_STATE.get(state, ())]
    if row_id:
        for action in actions:
            action["ids"] = [row_id]
    return actions


def _join_source_health_aliases(
    row: dict[str, Any], source_state_rows: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    merged = dict(row)
    source_state_row, source_state_name = _source_state_row_for_registry_row(row, source_state_rows)
    if source_state_name:
        merged["sourceStateName"] = source_state_name
    for key in SOURCE_HEALTH_FIELD_NAMES:
        value = source_state_row.get(key)
        if value not in {"", None}:
            merged[key] = value
    transition_reason = _clean_text(
        merged.get("pendingReason")
        or merged.get("quarantineReason")
        or merged.get("reason")
        or merged.get("registryReason")
    )
    merged["transitionReason"] = transition_reason
    merged["actions"] = _row_actions(merged)
    return merged


def _compare_registry_rows(winner: dict[str, Any], loser: dict[str, Any]) -> list[dict[str, Any]]:
    diffs: list[dict[str, Any]] = []
    for key in CONFLICT_DIFF_FIELDS:
        winner_value = winner.get(key)
        loser_value = loser.get(key)
        if _json_value(winner_value) == _json_value(loser_value):
            continue
        diffs.append(
            {
                "key": key,
                "label": _FIELD_LABELS.get(key, key.replace("_", " ").title()),
                "winnerValue": winner_value,
                "loserValue": loser_value,
            }
        )
    return diffs


def derive_registry_conflict_queue(
    registry_state: Any, source_state_payload: Any = None
) -> dict[str, Any]:
    registry = _as_dict(registry_state)
    registry_rows = [
        dict(row)
        for bucket in ("active", "pending", "rejected")
        for row in _as_list(registry.get(bucket))
        if isinstance(row, dict)
    ]
    source_state_rows = _source_state_rows_by_name(source_state_payload)
    family_cards = duplicate_family_conflict_cards(
        registry_rows,
        source_state=source_state_payload,
    )
    conflicts: list[dict[str, Any]] = []
    for card in family_cards:
        winner = _join_source_health_aliases(_as_dict(card.get("winner")), source_state_rows)
        losers = [
            _join_source_health_aliases(_as_dict(row), source_state_rows)
            for row in _as_list(card.get("losers"))
            if isinstance(row, dict)
        ]
        rows = [winner, *losers]
        conflicts.append(
            {
                "familyKey": _clean_text(card.get("familyKey")),
                "rowCount": max(0, int(card.get("rowCount") or len(rows))),
                "winner": winner,
                "winnerScore": _as_dict(card.get("winnerScore")),
                "winnerRationale": _as_list(card.get("winnerRationale")),
                "losers": losers,
                "rows": rows,
                "diffs": [
                    {
                        "loserId": _clean_text(
                            row.get("id") or row.get("sourceId") or source_identity(row)
                        ),
                        "loserName": _clean_text(row.get("name")),
                        "fields": _compare_registry_rows(winner, row),
                    }
                    for row in losers
                ],
            }
        )
    return {
        "summary": {
            "conflictCount": len(conflicts),
            "familyCount": len(conflicts),
            "rowCount": sum(int(card.get("rowCount") or 0) for card in conflicts),
            "winnerCount": len(conflicts),
            "loserCount": sum(len(card.get("losers") or []) for card in conflicts),
        },
        "conflicts": conflicts,
    }


def load_registry_conflicts_payload(
    *,
    load_state: Callable[[], Any],
    load_json_object: Callable[..., Any],
    source_state_path: Path,
) -> dict[str, Any]:
    registry_state = load_state()
    source_state_payload = load_json_object(source_state_path, {})
    payload = derive_registry_conflict_queue(registry_state, source_state_payload)
    warnings: list[str] = []
    if not Path(source_state_path).exists():
        warnings.append("missing_jobs_source_state_artifact")
    if warnings:
        payload["warnings"] = warnings
    return payload
