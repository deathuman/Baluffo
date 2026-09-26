"""Safe demotion application helpers for registry conflicts.

Extracted from registry_conflicts.py as part of the conflict split.

AI boundary owns: registry conflict demotion application and safe state mutation helpers.
AI boundary implement in: this file for demotion mutation mechanics; eligibility stays in automation/adjudication leaves.
AI boundary search before contracts: registry_conflicts coordinator, source registry IO, and demotion tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry demotion tests.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.bridge.registry_conflicts_automation import (
    _pending_provider_replacement_rows,
    _pending_static_fragment_alias_pair_for_target,
)
from src.bridge.registry_conflicts_row import (
    SAFE_AUTO_DEMOTE_ACTIONS,
    SAFE_AUTO_DEMOTE_REASON,
    SAFE_AUTO_DEMOTE_RESTORE_REASON,
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


def _card_winner_id(card: dict[str, Any]) -> str:
    winner = _as_dict(card.get("winner"))
    if not winner:
        return ""
    return _clean_text(winner.get("id")) or _row_identity(winner)


def _safe_demotion_applied_entry(row_id: str, card: dict[str, Any]) -> dict[str, str]:
    return {
        "id": row_id,
        "familyKey": _clean_text(card.get("familyKey")),
        "action": _clean_text(_as_dict(card.get("safeAutomation")).get("action")),
        # The row this one lost to. Without it the `applied` record says what moved but not what
        # it moved behind, which is the one fact an operator reviewing a run cannot otherwise get.
        "winnerId": _card_winner_id(card),
    }


def _stamp_conflict_provenance(row: dict[str, Any], card: dict[str, Any]) -> dict[str, Any]:
    """Record which family and which winner this row was demoted behind.

    `duplicateOfSourceId` would be the obvious field, but it is not provenance-only: the admin
    summaries and the soak report read it as "this row is a duplicate", so stamping it here would
    move the documented `duplicatePendingCount` KPI from its real duplicate count to include every
    conflict-demoted row, and would add a `duplicate_static_row` score penalty to rows that were
    not duplicates. `hiddenFromDefault` is worse still: it is a live gate, so a stamped row would
    vanish from admin pending listings and from the pending provider-migration fetch lane.

    `conflictFamilyKey` and `supersededBySourceId` are read by nothing, so the row gains durable,
    offline-joinable provenance -- surviving the journal, the sqlite `payload_json` mirror, and
    remote sync -- without disturbing a single consumer.
    """
    winner_id = _card_winner_id(card)
    family_key = _clean_text(card.get("familyKey"))
    stamped = dict(row)
    if family_key:
        stamped["conflictFamilyKey"] = family_key
    # A demoted row that is its own family's winner (the restore path re-promotes) keeps no
    # superseded pointer; a stale one would misreport the row as a loser.
    if winner_id and winner_id != source_identity(stamped):
        stamped["supersededBySourceId"] = winner_id
    else:
        stamped.pop("supersededBySourceId", None)
    return stamped


def _apply_state_transition_targets(
    state: dict[str, list[dict[str, Any]]],
    *,
    source_key: str,
    target_ids: set[str],
    eligible_by_id: dict[str, dict[str, Any]],
    now: str,
    actor: str,
    transition: Callable[..., dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]]]:
    """Move matching ``state[source_key]`` rows through ``transition``.

    Returns ``(remaining_rows, transitioned_rows, applied)``: rows whose
    identity is not targeted are kept, targeted rows are passed through the
    caller's transition, and each transition records its applied entry.
    """
    remaining: list[dict[str, Any]] = []
    transitioned: list[dict[str, Any]] = []
    applied: list[dict[str, str]] = []
    for row in state[source_key]:
        row_id = source_identity(row)
        if row_id not in target_ids:
            remaining.append(row)
            continue
        card = eligible_by_id.get(row_id) or {}
        transitioned.append(
            _stamp_conflict_provenance(
                transition(
                    row,
                    reason=SAFE_AUTO_DEMOTE_REASON,
                    actor=str(actor or SAFE_AUTO_DEMOTE_REASON),
                    at=now or None,
                ),
                card,
            )
        )
        applied.append(_safe_demotion_applied_entry(row_id, card))
    return remaining, transitioned, applied


def _apply_safe_demotion_targets(
    state: dict[str, list[dict[str, Any]]],
    *,
    target_ids: set[str],
    eligible_by_id: dict[str, dict[str, Any]],
    now: str,
    actor: str,
) -> tuple[list[dict[str, Any]], list[dict[str, str]], list[dict[str, Any]]]:
    active_remaining, moved, applied = _apply_state_transition_targets(
        state,
        source_key="active",
        target_ids=target_ids,
        eligible_by_id=eligible_by_id,
        now=now,
        actor=actor,
        transition=transition_registry_to_pending,
    )
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
    return _apply_state_transition_targets(
        state,
        source_key="pending",
        target_ids=target_ids,
        eligible_by_id=eligible_by_id,
        now=now,
        actor=actor,
        transition=transition_registry_to_rejected,
    )


def _restore_sort_number(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _best_restore_candidate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Pick the registration most worth keeping active: rank first, then yield, then stable id."""
    return sorted(
        rows,
        key=lambda row: (
            -_restore_sort_number(row.get("rankScore")),
            -_restore_sort_number(row.get("jobsFound")),
            source_identity(row),
        ),
    )[0]


def _families_demoted_from(
    moved_ids: set[str], eligible_by_id: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """The conflict cards this call demoted from, keyed by family."""
    families: dict[str, dict[str, Any]] = {}
    for row_id in moved_ids:
        card = eligible_by_id.get(row_id) or {}
        family_key = _clean_text(card.get("familyKey"))
        if family_key:
            families.setdefault(family_key, card)
    return families


def _restore_families_left_without_active_rows(
    state: dict[str, list[dict[str, Any]]],
    *,
    eligible_by_id: dict[str, dict[str, Any]],
    moved_ids: set[str],
    now: str,
    actor: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Put one registration back for any family this call left with no active row.

    The demotion helpers above move rows without checking the result, so a target set covering every
    active row of a board strips that board of coverage with nothing in the response saying so --
    the same shape as the duplicate-URL policy's "no stale baseline entry" rule. Only families this
    call demoted from are considered, so a deliberately parked board is never resurrected.
    """
    families = _families_demoted_from(moved_ids, eligible_by_id)
    next_active = [dict(row) for row in state["active"]]
    if not families:
        return next_active, []

    next_pending = [dict(row) for row in state["pending"]]
    active_ids = {source_identity(row) for row in next_active}
    pending_ids = {source_identity(row) for row in next_pending}
    restored: list[dict[str, Any]] = []

    for family_key in sorted(families):
        card_rows = [
            row for row in _as_list(families[family_key].get("rows")) if isinstance(row, dict)
        ]
        if any(source_identity(row) in active_ids for row in card_rows):
            continue
        candidates = [row for row in card_rows if source_identity(row) in pending_ids]
        if not candidates:
            continue
        winner = _best_restore_candidate(candidates)
        winner_id = source_identity(winner)
        promoted = transition_registry_to_active(
            dict(winner),
            reason=SAFE_AUTO_DEMOTE_RESTORE_REASON,
            actor=str(actor or SAFE_AUTO_DEMOTE_REASON),
            at=now or None,
        )
        # The row is the family's survivor again, so the pointer recorded when it was demoted is
        # stale and would misreport an active row as a loser.
        promoted.pop("supersededBySourceId", None)
        next_active.append(promoted)
        next_pending = [row for row in next_pending if source_identity(row) != winner_id]
        active_ids.add(winner_id)
        pending_ids.discard(winner_id)
        restored.append(
            {
                "id": winner_id,
                "familyKey": family_key,
                "reason": "restored_family_left_without_active_row",
            }
        )

    return next_active, restored
