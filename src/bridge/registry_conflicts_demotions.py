"""Safe demotion application helpers for registry conflicts.

Extracted from registry_conflicts.py as part of the conflict split.

AI boundary owns: registry conflict demotion application and safe state mutation helpers.
AI boundary implement in: this file for demotion mutation mechanics; eligibility stays in automation/adjudication leaves.
AI boundary search before contracts: registry_conflicts coordinator, source registry IO, and demotion tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry demotion tests.
"""

from __future__ import annotations

from typing import Any

from src.bridge.registry_conflicts_automation import (
    _pending_provider_replacement_rows,
    _pending_static_fragment_alias_pair_for_target,
)
from src.bridge.registry_conflicts_row import (
    SAFE_AUTO_DEMOTE_ACTIONS,
    SAFE_AUTO_DEMOTE_REASON,
    _active_same_adapter_provider_rows,
    _adjudicated_independent_provider_loser_ids,
    _adjudication_proves_independent_provider_boards,
    _as_dict,
    _as_list,
    _clean_text,
    _current_jobs_prove_independent_provider_boards,
    _independent_provider_board_audit_row,
    _row_identity,
)
from src.source_registry import source_identity
from src.source_registry_state import (
    transition_registry_to_active,
    transition_registry_to_pending,
    transition_registry_to_rejected,
)


def _independent_provider_board_suppression(
    *,
    family_key: str,
    rows: list[dict[str, Any]],
    family_adjudication: dict[str, Any] | None,
    job_index: dict[str, set[str]],
) -> dict[str, Any] | None:
    provider_rows = _active_same_adapter_provider_rows(rows)
    if not provider_rows:
        return None
    if _adjudication_proves_independent_provider_boards(provider_rows, family_adjudication):
        return _independent_provider_board_audit_row(
            family_key=family_key,
            rows=provider_rows,
            evidence_reason="live_adjudication_keep_both_job_sets_differ",
        )
    if _current_jobs_prove_independent_provider_boards(provider_rows, job_index):
        return _independent_provider_board_audit_row(
            family_key=family_key,
            rows=provider_rows,
            evidence_reason="current_fetch_job_identity_overlap_below_threshold",
        )
    return None


def _apply_independent_provider_board_suppression(
    *,
    family_key: str,
    candidate_rows: list[dict[str, Any]],
    losers: list[dict[str, Any]],
    family_adjudication: dict[str, Any] | None,
    job_index: dict[str, set[str]],
    audit_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], bool]:
    suppression = _independent_provider_board_suppression(
        family_key=family_key,
        rows=candidate_rows,
        family_adjudication=family_adjudication,
        job_index=job_index,
    )
    if suppression:
        audit_rows.append(suppression)
        return losers, True

    adjudicated_independent_ids = _adjudicated_independent_provider_loser_ids(
        candidate_rows,
        family_adjudication or {},
    )
    if not adjudicated_independent_ids:
        return losers, False

    audit_rows.append(
        _independent_provider_board_audit_row(
            family_key=family_key,
            rows=[
                row for row in candidate_rows if _row_identity(row) in adjudicated_independent_ids
            ],
            evidence_reason="live_adjudication_keep_both_job_sets_differ",
        )
    )
    return [row for row in losers if _row_identity(row) not in adjudicated_independent_ids], False


def _empty_safe_demotion_result(state: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    return {
        "ok": True,
        "demoted": 0,
        "skipped": 0,
        "applied": [],
        "skippedRows": [],
        "state": state,
    }


def _safe_demotion_state(registry_state: Any) -> dict[str, list[dict[str, Any]]]:
    registry = _as_dict(registry_state)
    return {
        bucket: [dict(row) for row in _as_list(registry.get(bucket)) if isinstance(row, dict)]
        for bucket in ("active", "pending", "rejected")
    }


def _eligible_safe_demotion_cards(
    conflict_payload: dict[str, Any], action_filter: str
) -> dict[str, dict[str, Any]]:
    eligible_by_id: dict[str, dict[str, Any]] = {}
    for card in _as_list(conflict_payload.get("conflicts")):
        if not isinstance(card, dict):
            continue
        safe_automation = _as_dict(card.get("safeAutomation"))
        safe_action = _clean_text(safe_automation.get("action"))
        if not safe_automation.get("eligible"):
            continue
        if action_filter and safe_action != action_filter:
            continue
        if not action_filter and safe_action not in SAFE_AUTO_DEMOTE_ACTIONS:
            continue
        for target_id in _as_list(safe_automation.get("targetIds")):
            target = _clean_text(target_id)
            if target:
                eligible_by_id[target] = card
    return eligible_by_id


def _safe_demotion_applied_entry(row_id: str, card: dict[str, Any]) -> dict[str, str]:
    return {
        "id": row_id,
        "familyKey": _clean_text(card.get("familyKey")),
        "action": _clean_text(_as_dict(card.get("safeAutomation")).get("action")),
    }


def _apply_safe_demotion_targets(
    state: dict[str, list[dict[str, Any]]],
    *,
    target_ids: set[str],
    eligible_by_id: dict[str, dict[str, Any]],
    now: str,
    actor: str,
) -> tuple[list[dict[str, Any]], list[dict[str, str]], list[dict[str, Any]]]:
    moved: list[dict[str, Any]] = []
    active_remaining: list[dict[str, Any]] = []
    applied: list[dict[str, str]] = []
    for row in state["active"]:
        row_id = source_identity(row)
        if row_id not in target_ids:
            active_remaining.append(row)
            continue
        moved.append(
            transition_registry_to_pending(
                row,
                reason=SAFE_AUTO_DEMOTE_REASON,
                actor=str(actor or SAFE_AUTO_DEMOTE_REASON),
                at=now or None,
            )
        )
        applied.append(_safe_demotion_applied_entry(row_id, eligible_by_id.get(row_id) or {}))
    return active_remaining, applied, moved


def _apply_pending_provider_replacement_targets(
    state: dict[str, list[dict[str, Any]]],
    *,
    target_ids: set[str],
    eligible_by_id: dict[str, dict[str, Any]],
    now: str,
    actor: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]]]:
    promotions_by_id: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for target_id in sorted(target_ids):
        card = eligible_by_id.get(target_id) or {}
        rows = [row for row in _as_list(card.get("rows")) if isinstance(row, dict)]
        active, pending_provider, blocked = _pending_provider_replacement_rows(rows)
        if blocked or _row_identity(pending_provider) != target_id:
            continue
        promotions_by_id[target_id] = (active, pending_provider)

    active_to_demote = {_row_identity(pair[0]) for pair in promotions_by_id.values()}
    pending_to_promote = set(promotions_by_id)
    next_active = [row for row in state["active"] if source_identity(row) not in active_to_demote]
    next_pending = [
        row for row in state["pending"] if source_identity(row) not in pending_to_promote
    ]
    applied: list[dict[str, str]] = []

    for target_id, (active_row, pending_row) in promotions_by_id.items():
        promoted = transition_registry_to_active(
            pending_row,
            reason=SAFE_AUTO_DEMOTE_REASON,
            actor=str(actor or SAFE_AUTO_DEMOTE_REASON),
            at=now or None,
        )
        demoted = transition_registry_to_pending(
            active_row,
            reason=SAFE_AUTO_DEMOTE_REASON,
            actor=str(actor or SAFE_AUTO_DEMOTE_REASON),
            at=now or None,
        )
        next_active.append(promoted)
        next_pending.append(demoted)
        applied.append(_safe_demotion_applied_entry(target_id, eligible_by_id.get(target_id) or {}))

    return next_active, next_pending, applied


def _apply_pending_static_fragment_alias_targets(
    state: dict[str, list[dict[str, Any]]],
    *,
    target_ids: set[str],
    eligible_by_id: dict[str, dict[str, Any]],
    now: str,
    actor: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]]]:
    promotions_by_id: dict[str, tuple[list[dict[str, Any]], dict[str, Any]]] = {}
    for target_id in sorted(target_ids):
        card = eligible_by_id.get(target_id) or {}
        rows = [row for row in _as_list(card.get("rows")) if isinstance(row, dict)]
        active_bares, pending_fragment, blocked = _pending_static_fragment_alias_pair_for_target(
            rows,
            target_id,
        )
        if blocked or _row_identity(pending_fragment) != target_id:
            continue
        promotions_by_id[target_id] = (active_bares, pending_fragment)

    active_to_demote = {
        _row_identity(active_row)
        for active_rows, _pending_row in promotions_by_id.values()
        for active_row in active_rows
    }
    pending_to_promote = set(promotions_by_id)
    next_active = [row for row in state["active"] if source_identity(row) not in active_to_demote]
    next_pending = [
        row for row in state["pending"] if source_identity(row) not in pending_to_promote
    ]
    applied: list[dict[str, str]] = []

    for target_id, (active_rows, pending_row) in promotions_by_id.items():
        promoted = transition_registry_to_active(
            pending_row,
            reason=SAFE_AUTO_DEMOTE_REASON,
            actor=str(actor or SAFE_AUTO_DEMOTE_REASON),
            at=now or None,
        )
        next_active.append(promoted)
        for active_row in active_rows:
            next_pending.append(
                transition_registry_to_pending(
                    active_row,
                    reason=SAFE_AUTO_DEMOTE_REASON,
                    actor=str(actor or SAFE_AUTO_DEMOTE_REASON),
                    at=now or None,
                )
            )
        applied.append(_safe_demotion_applied_entry(target_id, eligible_by_id.get(target_id) or {}))

    return next_active, next_pending, applied


def _apply_pending_rejection_targets(
    state: dict[str, list[dict[str, Any]]],
    *,
    target_ids: set[str],
    eligible_by_id: dict[str, dict[str, Any]],
    now: str,
    actor: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]]]:
    next_pending: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    applied: list[dict[str, str]] = []
    for row in state["pending"]:
        row_id = source_identity(row)
        if row_id not in target_ids:
            next_pending.append(row)
            continue
        rejected.append(
            transition_registry_to_rejected(
                row,
                reason=SAFE_AUTO_DEMOTE_REASON,
                actor=str(actor or SAFE_AUTO_DEMOTE_REASON),
                at=now or None,
            )
        )
        applied.append(_safe_demotion_applied_entry(row_id, eligible_by_id.get(row_id) or {}))
    return next_pending, rejected, applied
