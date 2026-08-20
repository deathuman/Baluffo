"""Registry conflict row helpers — row assembly and audit join.

AI boundary owns: row actions, pending-audit sections, conflict diff comparison, and source-health alias joining.
AI boundary implement in: this registry_conflicts_row_audit.py leaf.
AI boundary search before contracts: registry conflict routes, registry_conflicts coordinator, and frontend registry conflict callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry conflict row tests."""

from __future__ import annotations

from collections import Counter
from typing import Any

from src.bridge.registry_conflicts_row_core import (
    _as_list,
    _clean_text,
    _count_from_key,
    _has_fresh_or_healthy_signal,
    _int_value,
    _row_identity,
    _row_state,
)
from src.bridge.registry_conflicts_row_identity import _json_value
from src.bridge.registry_conflicts_row_source_state import (
    SOURCE_HEALTH_FIELD_NAMES,
    _source_state_row_for_registry_row,
)
from src.source_registry import source_identity

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
    "lastFetchedCount",
    "lastJobsFound",
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


def _row_actions(row: dict[str, Any]) -> list[dict[str, Any]]:
    state = _row_state(row)
    row_id = _clean_text(row.get("id") or row.get("sourceId") or source_identity(row))
    actions: list[dict[str, Any]] = [
        dict(action) for action in CONFLICT_ACTIONS_BY_STATE.get(state, ())
    ]
    if row_id:
        for action in actions:
            action["ids"] = [row_id]
    return actions


def _source_identity_counts(rows: list[dict[str, Any]]) -> Counter[str]:
    identities: Counter[str] = Counter()
    for row in rows:
        row_id = source_identity(row)
        if row_id:
            identities[row_id] += 1
    return identities


def _safe_auto_demoted_pending_audit_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _row_identity(row),
        "name": _clean_text(row.get("name")),
        "registryState": _row_state(row),
        "pendingReason": _clean_text(row.get("pendingReason")),
        "stateChangedAt": _clean_text(row.get("stateChangedAt")),
        "stateChangedBy": _clean_text(row.get("stateChangedBy")),
    }


def _build_pending_audit_section(cards: list[dict[str, Any]]) -> dict[str, Any]:
    row_count = sum(len(_as_list(card.get("rows"))) for card in cards)
    return {
        "summary": {
            "familyCount": len(cards),
            "rowCount": row_count,
        },
        "families": cards,
    }


def _build_independent_provider_board_audit(cards: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "summary": {
            "familyCount": len(cards),
            "rowCount": sum(_int_value(card.get("rowCount")) for card in cards),
        },
        "families": cards,
    }


def _build_pending_conflict_audit(
    *,
    safe_auto_demoted_cards: list[dict[str, Any]],
    safe_static_alias_cards: list[dict[str, Any]],
    safe_pending_provider_cards: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "safeAutoDemotedPending": _build_pending_audit_section(safe_auto_demoted_cards),
        "safePendingStaticAlias": _build_pending_audit_section(safe_static_alias_cards),
        "safePendingProviderLowerJobs": _build_pending_audit_section(safe_pending_provider_cards),
    }


def _unique_registry_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for row in rows:
        row_id = source_identity(row)
        if row_id and row_id in seen:
            continue
        if row_id:
            seen.add(row_id)
        unique.append(row)
    return unique


def _row_has_fresh_count_evidence(row: dict[str, Any]) -> bool:
    if any(
        _count_from_key(row, key) is not None
        for key in ("liveJobsFound", "lastJobsKept", "lastKeptCount", "lastReliableJobsFound")
    ):
        return True
    return _has_fresh_or_healthy_signal(row)


def _join_source_health_aliases(
    row: dict[str, Any],
    source_state_rows: dict[str, dict[str, Any]],
    ambiguous_names: set[str] | None = None,
) -> dict[str, Any]:
    merged = dict(row)
    source_state_row, source_state_name = _source_state_row_for_registry_row(
        row, source_state_rows, ambiguous_names
    )
    if source_state_name:
        merged["sourceStateName"] = source_state_name
    for key in SOURCE_HEALTH_FIELD_NAMES:
        value = source_state_row.get(key)
        if value not in {"", None}:
            merged[key] = value
    if not merged.get("lastSuccessfulFetchAt") and merged.get("lastSuccessAt"):
        merged["lastSuccessfulFetchAt"] = merged.get("lastSuccessAt")
    if not merged.get("lastSeenInFetchAt"):
        merged["lastSeenInFetchAt"] = merged.get("lastCheckedAt") or merged.get("lastRunAt") or ""
    if merged.get("lastJobsKept") in {"", None} and merged.get("lastKeptCount") not in {"", None}:
        merged["lastJobsKept"] = merged.get("lastKeptCount")
    if merged.get("failureCount") in {"", None} and merged.get("consecutiveFailures") not in {
        "",
        None,
    }:
        merged["failureCount"] = merged.get("consecutiveFailures")
    if merged.get("zeroJobStreak") in {"", None} and merged.get("consecutiveZeroKept") not in {
        "",
        None,
    }:
        merged["zeroJobStreak"] = merged.get("consecutiveZeroKept")
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
