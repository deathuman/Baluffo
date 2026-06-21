"""Registry conflict queue derivation and safe demotion application.

Ownership split 2026-05-26: row helpers → registry_conflicts_row.py,
automation → registry_conflicts_automation.py, demotions → registry_conflicts_demotions.py,
summary → registry_conflicts_summary.py.

This file is the coordinator — it imports leaf modules and re-exports the public
API surface for external callers.

AI boundary owns: public registry conflict coordinator and compatibility surface over conflict leaves.
AI boundary implement in: leaf modules for row, summary, automation, and demotion behavior; keep this coordinator stable.
AI boundary search before contracts: registry conflict routes, post admin routes, source registry policy, and conflict tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry conflict tests.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.bridge.registry_conflicts_automation import (
    _analyze_safe_automation,
    _build_automation_summary,
    _build_review_summary,
    _build_triage_summary,
    _classify_conflict_review,
    _classify_conflict_triage,
    _drop_safe_pending_homepage_static_losers,
    _is_safe_auto_demoted_pending,
    _is_safe_pending_static_weaker_alias,
    _safe_pending_provider_lower_jobs_rows,
)
from src.bridge.registry_conflicts_demotions import (
    _apply_independent_provider_board_suppression,
    _apply_pending_provider_replacement_targets,
    _apply_pending_rejection_targets,
    _apply_pending_static_fragment_alias_targets,
    _apply_safe_demotion_targets,
    _eligible_safe_demotion_cards,
    _empty_safe_demotion_result,
    _safe_demotion_state,
)

# ── Re-export public constants and API ──
from src.bridge.registry_conflicts_row import (
    SAFE_AUTO_DEMOTE_ACTION,
    SAFE_AUTO_DEMOTE_ACTIONS,
    SAFE_AUTO_DEMOTE_LABEL,
    SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_ACTION,
    SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_LABEL,
    SAFE_AUTO_DEMOTE_PROVIDER_STATIC_ACTION,
    SAFE_AUTO_DEMOTE_PROVIDER_STATIC_LABEL,
    SAFE_AUTO_DEMOTE_REASON,
    SAFE_AUTO_DEMOTE_ROUTE,
    SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_LABEL,
    SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_LABEL,
    SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_LABEL,
    SAFE_AUTO_PROMOTE_PENDING_PROVIDER_ACTION,
    SAFE_AUTO_PROMOTE_PENDING_PROVIDER_LABEL,
    SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_ACTION,
    SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_LABEL,
    SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_ACTION,
    SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_LABEL,
    _adjudication_families_by_key,
    _ambiguous_registry_row_names,
    _as_dict,
    _as_list,
    _build_independent_provider_board_audit,
    _build_pending_conflict_audit,
    _clean_text,
    _compare_registry_rows,
    _join_source_health_aliases,
    _merge_fetch_report_source_details,
    _row_identity,
    _row_state,
    _safe_auto_demoted_pending_audit_row,
    _source_job_identity_index,
    _source_state_rows_by_name,
    _unique_registry_rows,
    _with_live_adjudication_card,
)

# ── Re-export public API from summary module ──
from src.bridge.registry_conflicts_summary import (
    build_registry_conflicts_summary_cache_key,
    load_cached_registry_conflicts_summary,
    load_registry_conflicts_summary_payload,
    summarize_registry_conflicts_payload,
    write_registry_conflicts_summary_cache,
)
from src.shared.json_io import read_json
from src.shared.json_io import read_json_object as read_pipeline_json_object
from src.source_registry import source_identity
from src.source_registry_policy import duplicate_family_conflict_cards

__all__ = [
    "SAFE_AUTO_DEMOTE_ACTION",
    "SAFE_AUTO_DEMOTE_ACTIONS",
    "SAFE_AUTO_DEMOTE_LABEL",
    "SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_ACTION",
    "SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_LABEL",
    "SAFE_AUTO_DEMOTE_PROVIDER_STATIC_ACTION",
    "SAFE_AUTO_DEMOTE_PROVIDER_STATIC_LABEL",
    "SAFE_AUTO_DEMOTE_REASON",
    "SAFE_AUTO_DEMOTE_ROUTE",
    "SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_ACTION",
    "SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_LABEL",
    "SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_ACTION",
    "SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_LABEL",
    "SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_ACTION",
    "SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_LABEL",
    "SAFE_AUTO_PROMOTE_PENDING_PROVIDER_ACTION",
    "SAFE_AUTO_PROMOTE_PENDING_PROVIDER_LABEL",
    "SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_ACTION",
    "SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_LABEL",
    "SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_ACTION",
    "SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_LABEL",
    "apply_registry_conflict_safe_demotions",
    "build_registry_conflicts_summary_cache_key",
    "derive_registry_conflict_queue",
    "load_cached_registry_conflicts_summary",
    "load_registry_conflicts_payload",
    "load_registry_conflicts_summary_payload",
    "summarize_registry_conflicts_payload",
    "write_registry_conflicts_summary_cache",
]


def apply_registry_conflict_safe_demotions(
    registry_state: Any,
    source_state_payload: Any = None,
    *,
    action: str = "",
    ids: list[str] | None = None,
    now: str = "",
    actor: str = SAFE_AUTO_DEMOTE_REASON,
    protected_ids: set[str] | None = None,
) -> dict[str, Any]:
    state = _safe_demotion_state(registry_state)
    action_filter = _clean_text(action)
    if action_filter and action_filter not in SAFE_AUTO_DEMOTE_ACTIONS:
        return {
            **_empty_safe_demotion_result(state),
            "ok": False,
            "error": "Unsupported safe automation action.",
        }

    requested_ids = {_clean_text(item) for item in (ids or []) if _clean_text(item)}
    conflict_payload = derive_registry_conflict_queue(state, source_state_payload)
    eligible_by_id = _eligible_safe_demotion_cards(conflict_payload, action_filter)
    protected = {_clean_text(item) for item in (protected_ids or set()) if _clean_text(item)}
    selected_ids = (requested_ids or set(eligible_by_id)) - protected
    target_ids = selected_ids & set(eligible_by_id)
    provider_promotion_ids = {
        row_id
        for row_id in target_ids
        if _clean_text(
            _as_dict((eligible_by_id.get(row_id) or {}).get("safeAutomation")).get("action")
        )
        == SAFE_AUTO_PROMOTE_PENDING_PROVIDER_ACTION
    }
    static_fragment_promotion_ids = {
        row_id
        for row_id in target_ids
        if _clean_text(
            _as_dict((eligible_by_id.get(row_id) or {}).get("safeAutomation")).get("action")
        )
        == SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_ACTION
    }
    pending_rejection_ids = {
        row_id
        for row_id in target_ids
        if _clean_text(
            _as_dict((eligible_by_id.get(row_id) or {}).get("safeAutomation")).get("action")
        )
        == SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_ACTION
    }
    promotion_ids = provider_promotion_ids | static_fragment_promotion_ids
    demotion_ids = target_ids - promotion_ids - pending_rejection_ids
    skipped_rows = [
        {
            "id": row_id,
            "reason": "protected_from_load_time_safe_auto_demote",
        }
        for row_id in sorted((requested_ids or set(eligible_by_id)) & protected)
    ]
    skipped_rows.extend(
        {
            "id": row_id,
            "reason": "not_currently_safe_auto_demote_eligible",
        }
        for row_id in sorted(selected_ids - target_ids)
    )
    promoted_active, promoted_pending, promoted_applied = (
        _apply_pending_provider_replacement_targets(
            state,
            target_ids=provider_promotion_ids,
            eligible_by_id=eligible_by_id,
            now=now,
            actor=actor,
        )
    )
    state["active"] = promoted_active
    state["pending"] = promoted_pending
    fragment_active, fragment_pending, fragment_applied = (
        _apply_pending_static_fragment_alias_targets(
            state,
            target_ids=static_fragment_promotion_ids,
            eligible_by_id=eligible_by_id,
            now=now,
            actor=actor,
        )
    )
    state["active"] = fragment_active
    state["pending"] = fragment_pending
    rejected_pending, rejected_rows, rejection_applied = _apply_pending_rejection_targets(
        state,
        target_ids=pending_rejection_ids,
        eligible_by_id=eligible_by_id,
        now=now,
        actor=actor,
    )
    state["pending"] = rejected_pending
    state["rejected"] = _unique_registry_rows([*state["rejected"], *rejected_rows])
    active_remaining, applied, moved = _apply_safe_demotion_targets(
        state,
        target_ids=demotion_ids,
        eligible_by_id=eligible_by_id,
        now=now,
        actor=actor,
    )
    moved_ids = {source_identity(row) for row in moved}
    for row_id in sorted(demotion_ids - moved_ids):
        skipped_rows.append({"id": row_id, "reason": "eligible_target_not_active"})
    promoted_ids = {_clean_text(row.get("id")) for row in [*promoted_applied, *fragment_applied]}
    for row_id in sorted(promotion_ids - promoted_ids):
        skipped_rows.append({"id": row_id, "reason": "eligible_target_not_pending"})
    rejected_ids = {_clean_text(row.get("id")) for row in rejection_applied}
    for row_id in sorted(pending_rejection_ids - rejected_ids):
        skipped_rows.append({"id": row_id, "reason": "eligible_target_not_pending"})

    state["active"] = active_remaining
    state["pending"] = _unique_registry_rows([*state["pending"], *moved])
    return {
        "ok": True,
        "demoted": (
            len(moved) + len(promoted_applied) + len(fragment_applied) + len(rejection_applied)
        ),
        "skipped": len(skipped_rows),
        "applied": [*promoted_applied, *fragment_applied, *rejection_applied, *applied],
        "skippedRows": skipped_rows,
        "state": state,
    }


def derive_registry_conflict_queue(
    registry_state: Any,
    source_state_payload: Any = None,
    adjudication_payload: Any = None,
    job_rows: Any = None,
) -> dict[str, Any]:
    registry = _as_dict(registry_state)
    registry_rows = [
        dict(row)
        for bucket in ("active", "pending", "rejected")
        for row in _as_list(registry.get(bucket))
        if isinstance(row, dict)
    ]
    source_state_rows = _source_state_rows_by_name(source_state_payload)
    job_index = _source_job_identity_index(job_rows)
    ambiguous_names = _ambiguous_registry_row_names(registry_rows)
    adjudication_by_family = _adjudication_families_by_key(adjudication_payload)
    family_cards = duplicate_family_conflict_cards(
        registry_rows,
        source_state=source_state_payload,
    )
    conflicts: list[dict[str, Any]] = []
    safe_auto_demoted_pending_audit: list[dict[str, Any]] = []
    safe_pending_static_alias_audit: list[dict[str, Any]] = []
    safe_pending_provider_lower_jobs_audit: list[dict[str, Any]] = []
    independent_provider_board_audit: list[dict[str, Any]] = []
    for card in family_cards:
        family_key = _clean_text(card.get("familyKey"))
        family_adjudication = adjudication_by_family.get(family_key)
        if family_adjudication:
            card = _with_live_adjudication_card(
                card,
                family=family_adjudication,
                source_state_payload=source_state_payload,
            )
        family_key = _clean_text(card.get("familyKey"))
        winner = _join_source_health_aliases(
            _as_dict(card.get("winner")), source_state_rows, ambiguous_names
        )
        losers = [
            _join_source_health_aliases(_as_dict(row), source_state_rows, ambiguous_names)
            for row in _as_list(card.get("losers"))
            if isinstance(row, dict)
        ]
        suppressed_losers = [row for row in losers if _is_safe_auto_demoted_pending(row)]
        if suppressed_losers:
            safe_auto_demoted_pending_audit.append(
                {
                    "familyKey": _clean_text(card.get("familyKey")),
                    "rowCount": len(suppressed_losers),
                    "rows": [
                        _safe_auto_demoted_pending_audit_row(row) for row in suppressed_losers
                    ],
                }
            )
            losers = [row for row in losers if not _is_safe_auto_demoted_pending(row)]
        candidate_rows = [winner, *losers]
        losers, skip_conflict = _apply_independent_provider_board_suppression(
            family_key=family_key,
            candidate_rows=candidate_rows,
            losers=losers,
            family_adjudication=family_adjudication,
            job_index=job_index,
            audit_rows=independent_provider_board_audit,
        )
        if skip_conflict:
            continue
        candidate_rows = [winner, *losers]
        suppressed_pending_provider_losers = _safe_pending_provider_lower_jobs_rows(candidate_rows)
        if suppressed_pending_provider_losers:
            safe_pending_provider_lower_jobs_audit.append(
                {
                    "familyKey": family_key,
                    "rowCount": len(suppressed_pending_provider_losers),
                    "rows": [
                        _safe_auto_demoted_pending_audit_row(row)
                        for row in suppressed_pending_provider_losers
                    ],
                }
            )
            suppressed_ids = {_row_identity(row) for row in suppressed_pending_provider_losers}
            candidate_rows = [
                row for row in candidate_rows if _row_identity(row) not in suppressed_ids
            ]
            active_remaining = [row for row in candidate_rows if _row_state(row) == "active"]
            if active_remaining:
                winner = active_remaining[0]
                winner_id = _row_identity(winner)
                losers = [row for row in candidate_rows if _row_identity(row) != winner_id]
            elif candidate_rows:
                winner = candidate_rows[0]
                winner_id = _row_identity(winner)
                losers = [row for row in candidate_rows if _row_identity(row) != winner_id]
            else:
                losers = []
        losers = _drop_safe_pending_homepage_static_losers(
            winner,
            losers,
            family_key,
            safe_pending_static_alias_audit,
        )
        suppressed_static_alias_losers = [
            row for row in losers if _is_safe_pending_static_weaker_alias(winner, row, family_key)
        ]
        if suppressed_static_alias_losers:
            safe_pending_static_alias_audit.append(
                {
                    "familyKey": family_key,
                    "rowCount": len(suppressed_static_alias_losers),
                    "rows": [
                        _safe_auto_demoted_pending_audit_row(row)
                        for row in suppressed_static_alias_losers
                    ],
                }
            )
            losers = [
                row
                for row in losers
                if not _is_safe_pending_static_weaker_alias(winner, row, family_key)
            ]
        rows = [winner, *losers]
        if len(rows) < 2:
            continue
        triage = _classify_conflict_triage(rows)
        review = _classify_conflict_review(rows, triage["bucket"])
        safe_automation = _analyze_safe_automation(
            family_key=family_key,
            winner=winner,
            losers=losers,
            rows=rows,
        )
        conflicts.append(
            {
                "familyKey": family_key,
                "rowCount": len(rows),
                "triageBucket": triage["bucket"],
                "triageLabel": triage["label"],
                "triageReason": triage["reason"],
                "triageRisk": triage["risk"],
                "reviewPriority": review["priority"],
                "reviewQueue": review["queue"],
                "reviewLabel": review["label"],
                "reviewReason": review["reason"],
                "suggestedDisposition": review["suggestedDisposition"],
                "suggestedConfidence": review["suggestedConfidence"],
                "evidenceFlags": review["evidenceFlags"],
                "safeAutomation": safe_automation,
                "effectiveWinnerSource": _clean_text(card.get("effectiveWinnerSource"))
                or "registry",
                "liveAdjudicationComplete": bool(card.get("liveAdjudicationComplete")),
                "adjudication": _as_dict(card.get("adjudication")),
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
    conflicts.sort(
        key=lambda card: (
            int(card.get("reviewPriority", 3)),
            _clean_text(card.get("reviewQueue")),
            _clean_text(card.get("familyKey")),
        )
    )
    automation = _build_automation_summary(conflicts)
    automation["audit"] = _build_pending_conflict_audit(
        safe_auto_demoted_cards=safe_auto_demoted_pending_audit,
        safe_static_alias_cards=safe_pending_static_alias_audit,
        safe_pending_provider_cards=safe_pending_provider_lower_jobs_audit,
    )
    return {
        "summary": {
            "conflictCount": len(conflicts),
            "familyCount": len(conflicts),
            "rowCount": sum(int(card.get("rowCount") or 0) for card in conflicts),
            "winnerCount": len(conflicts),
            "loserCount": sum(len(card.get("losers") or []) for card in conflicts),
        },
        "triage": _build_triage_summary(conflicts),
        "review": _build_review_summary(conflicts),
        "automation": automation,
        "suppressedIndependentProviderBoards": _build_independent_provider_board_audit(
            independent_provider_board_audit
        ),
        "conflicts": conflicts,
    }


def load_registry_conflicts_payload(
    *,
    load_state: Callable[[], Any],
    load_json_object: Callable[..., Any],
    source_state_path: Path,
    adjudication_payload: Any = None,
) -> dict[str, Any]:
    from src.source_registry_io import load_runtime_evidence

    registry_state = load_state()
    source_state_payload = read_pipeline_json_object(Path(source_state_path), {})
    fetch_report_payload = load_runtime_evidence(
        Path(source_state_path).with_name("jobs-fetch-report.json"), {}
    )
    job_rows = read_json(Path(source_state_path).with_name("jobs-unified.json"), [])
    source_state_payload = _merge_fetch_report_source_details(
        source_state_payload,
        fetch_report_payload,
    )
    payload = derive_registry_conflict_queue(
        registry_state,
        source_state_payload,
        adjudication_payload,
        job_rows,
    )
    warnings: list[str] = []
    if not Path(source_state_path).exists():
        warnings.append("missing_jobs_source_state_artifact")
    if warnings:
        payload["warnings"] = warnings
    return payload
