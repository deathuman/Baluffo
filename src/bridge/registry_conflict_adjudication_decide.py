"""Registry conflict adjudication — decision, selection, and family summary.

AI boundary owns: loser classification, conflict selection, autopilot demotion, and per-family adjudication assembly.
AI boundary implement in: this registry_conflict_adjudication_decide.py leaf.
AI boundary search before contracts: conflict adjudication routes, progress payloads, and adjudication tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry adjudication tests."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.bridge.registry_conflict_adjudication_core import (
    ADJUDICATION_REASON,
    _as_dict,
    _as_list,
    _clean,
    _row_id,
    _row_state,
)
from src.bridge.registry_conflict_adjudication_probe import (
    _best_probe,
    _overlap,
    _probe_row,
    _public_probe,
)
from src.jobs.text_utils import normalize_url
from src.source_registry import source_identity
from src.source_registry_state import transition_registry_to_pending


def _classify_loser(
    best: dict[str, Any], loser: dict[str, Any]
) -> tuple[str, str, str, dict[str, Any]]:
    overlap = _overlap(best, loser)
    best_jobs = int(best.get("jobsFound") or 0)
    loser_jobs = int(loser.get("jobsFound") or 0)
    same_final = normalize_url(best.get("finalUrl")) == normalize_url(loser.get("finalUrl"))
    if bool(best.get("ok")) and best_jobs > 0 and not bool(loser.get("ok")):
        return (
            "auto_demote_applied",
            "high",
            "winner has live jobs while loser failed probe",
            overlap,
        )
    if same_final and bool(best.get("ok")) and best_jobs >= loser_jobs:
        return "auto_demote_applied", "high", "sources resolve to the same final URL", overlap
    if (
        bool(best.get("ok"))
        and best_jobs > 0
        and overlap["ratio"] >= 0.8
        and best_jobs >= loser_jobs
    ):
        loser_newer = _clean(loser.get("newestJobDate")) > _clean(best.get("newestJobDate"))
        if not loser_newer:
            return "auto_demote_applied", "high", "sources return the same job set", overlap
    if bool(best.get("ok")) and bool(loser.get("ok")) and best_jobs > 0 and loser_jobs == 0:
        return (
            "auto_demote_applied",
            "high",
            "winner has live jobs while loser returned zero jobs",
            overlap,
        )
    if bool(best.get("ok")) and bool(loser.get("ok")) and best_jobs > 0 and loser_jobs > 0:
        if overlap["ratio"] < 0.5:
            return "keep_both", "medium", "both sources are live and job sets differ", overlap
        return (
            "recommended_demotion",
            "medium",
            "sources overlap but evidence is not strict enough for autopilot",
            overlap,
        )
    if not bool(best.get("ok")) and not bool(loser.get("ok")):
        return "probe_failed", "low", "both sources failed probe", overlap
    return "needs_review", "low", "insufficient live evidence for safe demotion", overlap


def _selected_conflicts(
    conflict_payload: dict[str, Any], payload: dict[str, Any]
) -> list[dict[str, Any]]:
    family_filter = {
        _clean(item).lower() for item in _as_list(payload.get("familyKeys")) if _clean(item)
    }
    source_filter = {
        _clean(item).lower() for item in _as_list(payload.get("sourceIds")) if _clean(item)
    }
    conflicts = []
    for card in _as_list(conflict_payload.get("conflicts")):
        if not isinstance(card, dict):
            continue
        family_key = _clean(card.get("familyKey"))
        rows = [_as_dict(row) for row in _as_list(card.get("rows"))]
        active_rows = [row for row in rows if _row_state(row) == "active"]
        if len(active_rows) < 2:
            continue
        if family_filter and family_key.lower() not in family_filter:
            continue
        if source_filter:
            active_rows = [row for row in active_rows if _row_id(row).lower() in source_filter]
            if len(active_rows) < 2:
                continue
        next_card = dict(card)
        next_card["rows"] = active_rows
        conflicts.append(next_card)
    return conflicts


def _demote_ids(
    state: dict[str, list[dict[str, Any]]], target_ids: set[str], now: str
) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    moved: list[dict[str, Any]] = []
    active: list[dict[str, Any]] = []
    applied: list[str] = []
    for row in state.get("active") or []:
        row_id = source_identity(row)
        if row_id in target_ids:
            moved.append(
                transition_registry_to_pending(
                    row,
                    reason=ADJUDICATION_REASON,
                    actor=ADJUDICATION_REASON,
                    at=now,
                )
            )
            applied.append(row_id)
        else:
            active.append(row)
    next_state = {
        "active": active,
        "pending": list(state.get("pending") or []) + moved,
        "rejected": list(state.get("rejected") or []),
    }
    return next_state, applied


def _decision_status(status: str, apply_autopilot: bool) -> str:
    if apply_autopilot or status != "auto_demote_applied":
        return status
    return "recommended_demotion"


def _family_status(decisions: list[dict[str, Any]]) -> str:
    ordered_statuses = (
        "auto_demote_applied",
        "recommended_demotion",
        "keep_both",
        "probe_failed",
    )
    statuses = {str(decision.get("status") or "") for decision in decisions}
    return next((status for status in ordered_statuses if status in statuses), "needs_review")


def _summary_from_families(families: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "autoDemoteApplied": sum(1 for row in families if row["status"] == "auto_demote_applied"),
        "recommendedDemotion": sum(
            1 for row in families if row["status"] == "recommended_demotion"
        ),
        "keepBoth": sum(1 for row in families if row["status"] == "keep_both"),
        "needsReview": sum(1 for row in families if row["status"] == "needs_review"),
        "probeFailed": sum(1 for row in families if row["status"] == "probe_failed"),
    }


def _build_family_adjudication(
    card: dict[str, Any],
    *,
    timeout_s: int,
    apply_autopilot: bool,
    progress_callback: Callable[[str, dict[str, Any], dict[str, Any] | None], None] | None = None,
) -> tuple[dict[str, Any] | None, set[str]]:
    probes = []
    for row in [_as_dict(item) for item in _as_list(card.get("rows"))]:
        if progress_callback:
            progress_callback("source_started", row, None)
        probe = _probe_row(row, timeout_s)
        probes.append(probe)
        if progress_callback:
            progress_callback("source_finished", row, probe)
    if not probes:
        return None, set()
    best = _best_probe(probes)
    target_ids: set[str] = set()
    decisions = []
    for probe in probes:
        if probe.get("sourceId") == best.get("sourceId"):
            continue
        status, confidence, reason, overlap = _classify_loser(best, probe)
        if status == "auto_demote_applied":
            target_ids.add(_clean(probe.get("sourceId")))
        decisions.append(
            {
                "sourceId": _clean(probe.get("sourceId")),
                "status": _decision_status(status, apply_autopilot),
                "confidence": confidence,
                "reason": reason,
                "overlap": overlap,
            }
        )
    return (
        {
            "familyKey": _clean(card.get("familyKey")),
            "status": _family_status(decisions),
            "winnerSourceId": _clean(best.get("sourceId")),
            "checkedSourceIds": [_clean(probe.get("sourceId")) for probe in probes],
            "probes": [_public_probe(probe) for probe in probes],
            "decisions": decisions,
        },
        target_ids,
    )
