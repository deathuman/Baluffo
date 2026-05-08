"""Duplicate and pending-noise policies for source registry rows."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from src.shared.utils import now_iso
from src.source_registry_identity import (
    _clean_family_token,
    ensure_source_id,
    source_family_key,
    source_identity,
    unique_sources,
)
from src.source_registry_state import (
    REGISTRY_REASON_DUPLICATE_FAMILY,
    _coerce_int,
    transition_registry_to_pending,
)


def _state_rows_by_key(source_state: Any) -> dict[str, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(source_state, dict):
        for key, value in source_state.items():
            if isinstance(value, dict):
                row = dict(value)
                row.setdefault("name", key)
                rows.append(row)
    elif isinstance(source_state, list):
        rows = [row for row in source_state if isinstance(row, dict)]
    by_key: dict[str, dict[str, Any]] = {}
    for row in rows:
        for key in (
            str(row.get("id") or "").strip().lower(),
            str(row.get("sourceId") or "").strip().lower(),
            str(row.get("name") or "").strip().lower(),
        ):
            if key:
                by_key[key] = row
    return by_key


def _source_state_for_row(
    row: dict[str, Any], source_state_by_key: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    for key in (
        source_identity(row),
        str(row.get("sourceId") or "").strip().lower(),
        str(row.get("name") or "").strip().lower(),
    ):
        if key and key in source_state_by_key:
            return source_state_by_key[key]
    return {}


def _adapter_priority(row: dict[str, Any]) -> int:
    adapter = str(row.get("adapter") or "").strip().lower()
    if adapter in {"greenhouse", "lever", "ashby", "teamtailor"}:
        return 4
    if adapter in {"workable", "smartrecruiters", "bamboohr", "personio", "recruitee"}:
        return 3
    if adapter and adapter != "static":
        return 2
    if adapter == "static":
        return 1
    return 0


def _metadata_score(row: dict[str, Any]) -> int:
    keys = ("api_url", "feed_url", "board_url", "listing_url", "careersUrl", "url")
    score = sum(1 for key in keys if str(row.get(key) or "").strip())
    pages = row.get("pages")
    if isinstance(pages, list) and any(str(item or "").strip() for item in pages):
        score += 1
    return score


def _row_jobs_evidence(row: dict[str, Any], state: dict[str, Any]) -> int:
    return max(
        _coerce_int(state.get("lastKeptCount"), 0),
        _coerce_int(state.get("lastJobsKept"), 0),
        _coerce_int(row.get("lastKeptCount"), 0),
        _coerce_int(row.get("lastJobsKept"), 0),
        _coerce_int(row.get("jobsFound"), 0),
        _coerce_int(row.get("sampleCount"), 0),
    )


def _duplicate_winner_score(
    row: dict[str, Any], source_state_by_key: dict[str, dict[str, Any]]
) -> tuple[int, int, int, int, int, int, str]:
    state = _source_state_for_row(row, source_state_by_key)
    row_status = str(row.get("status") or state.get("lastStatus") or "").strip().lower()
    candidate_state = str(row.get("candidateState") or "").strip().lower()
    quarantined = candidate_state in {"quarantined", "rejected"} or bool(
        row.get("quarantineReason")
    )
    return (
        0 if quarantined else 1,
        _row_jobs_evidence(row, state),
        1 if row_status in {"ok", "success", "healthy"} else 0,
        _adapter_priority(row),
        _coerce_int(row.get("rankScore") or row.get("score"), 0),
        _metadata_score(row),
        source_identity(row),
    )


def _duplicate_winner_score_payload(
    score: tuple[int, int, int, int, int, int, str],
) -> dict[str, Any]:
    return {
        "quarantinePenalty": int(score[0]),
        "lastKeptCount": int(score[1]),
        "statusScore": int(score[2]),
        "adapterPriority": int(score[3]),
        "rankScore": int(score[4]),
        "metadataScore": int(score[5]),
        "identity": str(score[6]),
    }


def _duplicate_winner_rationale(
    row: dict[str, Any],
    source_state_by_key: dict[str, dict[str, Any]],
    *,
    score: tuple[int, int, int, int, int, int, str] | None = None,
) -> list[dict[str, str]]:
    state = _source_state_for_row(row, source_state_by_key)
    score = score or _duplicate_winner_score(row, source_state_by_key)
    source_health = str(state.get("health") or "").strip().lower() or "unknown"
    source_health_reason = str(state.get("healthReason") or "").strip()
    if source_health_reason:
        source_health = f"{source_health}: {source_health_reason}"
    quarantined = bool(
        str(row.get("candidateState") or state.get("candidateState") or "").strip().lower()
        in {"quarantined", "rejected"}
        or str(row.get("quarantineReason") or state.get("quarantineReason") or "").strip()
    )
    source_jobs_kept = str(
        state.get("lastJobsKept")
        or state.get("lastKeptCount")
        or row.get("lastJobsKept")
        or row.get("lastKeptCount")
        or 0
    )
    failure_count = str(
        state.get("failureCount")
        or state.get("consecutiveFailures")
        or row.get("failureCount")
        or 0
    )
    zero_job_streak = str(
        state.get("zeroJobStreak")
        or state.get("consecutiveZeroKept")
        or row.get("zeroJobStreak")
        or row.get("consecutiveZeroKept")
        or 0
    )
    adapter = str(row.get("adapter") or "").strip().lower() or "unknown"
    return [
        {
            "label": "Quarantine penalty",
            "value": "applied" if quarantined else "clear",
        },
        {
            "label": "Source health",
            "value": source_health,
        },
        {
            "label": "Last jobs kept",
            "value": source_jobs_kept,
        },
        {
            "label": "Failure count",
            "value": failure_count,
        },
        {
            "label": "Zero-job streak",
            "value": zero_job_streak,
        },
        {
            "label": "Adapter priority",
            "value": f"{adapter} ({score[3]})",
        },
        {
            "label": "Rank score",
            "value": str(score[4]),
        },
        {
            "label": "Metadata score",
            "value": str(score[5]),
        },
        {
            "label": "Identity",
            "value": score[6],
        },
    ]


def duplicate_family_conflict_cards(
    rows: Iterable[dict[str, Any]],
    *,
    target_families: Iterable[str] | None = None,
    source_state: Any = None,
) -> list[dict[str, Any]]:
    target_keys = {
        token for token in (_clean_family_token(item) for item in (target_families or [])) if token
    }
    source_state_by_key = _state_rows_by_key(source_state)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in [ensure_source_id(dict(row)) for row in rows if isinstance(row, dict)]:
        family_key = source_family_key(row)
        if not family_key:
            continue
        if target_keys and family_key not in target_keys:
            continue
        grouped.setdefault(family_key, []).append(row)

    cards: list[dict[str, Any]] = []
    for family_key, family_rows in sorted(grouped.items()):
        if len(family_rows) < 2:
            continue
        ordered_rows = sorted(
            family_rows,
            key=lambda row: _duplicate_winner_score(row, source_state_by_key),
            reverse=True,
        )
        winner = ordered_rows[0]
        winner_score = _duplicate_winner_score(winner, source_state_by_key)
        losers = ordered_rows[1:]
        cards.append(
            {
                "familyKey": family_key,
                "rowCount": len(ordered_rows),
                "winner": winner,
                "winnerScore": _duplicate_winner_score_payload(winner_score),
                "winnerRationale": _duplicate_winner_rationale(
                    winner,
                    source_state_by_key,
                    score=winner_score,
                ),
                "losers": losers,
                "rows": ordered_rows,
            }
        )
    return cards


def _demote_duplicate_variant(
    row: dict[str, Any],
    *,
    winner: dict[str, Any],
    family_key: str,
    actor: str,
    at: str,
) -> dict[str, Any]:
    updated = transition_registry_to_pending(
        row,
        reason=REGISTRY_REASON_DUPLICATE_FAMILY,
        actor=actor,
        at=at,
    )
    updated["candidateState"] = "hidden"
    updated["hiddenFromDefault"] = True
    updated["pendingReason"] = REGISTRY_REASON_DUPLICATE_FAMILY
    updated["duplicateFamilyKey"] = family_key
    updated["duplicateOfSourceId"] = source_identity(winner)
    if str(winner.get("name") or "").strip():
        updated["duplicateOfSourceName"] = str(winner.get("name") or "").strip()
    return ensure_source_id(updated)


def demote_duplicate_active_variants(
    active_rows: Iterable[dict[str, Any]],
    *,
    target_families: Iterable[str] | None = None,
    source_state: Any = None,
    actor: str = "registry_noise_cleanup",
    at: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    timestamp = str(at or now_iso())
    cards = duplicate_family_conflict_cards(
        active_rows,
        target_families=target_families,
        source_state=source_state,
    )
    demoted_ids: set[str] = set()
    demoted_rows: list[dict[str, Any]] = []
    for card in cards:
        family_key = str(card.get("familyKey") or "")
        winner = dict(card.get("winner") or {})
        winner_id = source_identity(winner)
        for row in [dict(row) for row in card.get("losers") or [] if isinstance(row, dict)]:
            row_id = source_identity(row)
            if row_id == winner_id:
                continue
            demoted_ids.add(row_id)
            demoted_rows.append(
                _demote_duplicate_variant(
                    row,
                    winner=winner,
                    family_key=family_key,
                    actor=actor,
                    at=timestamp,
                )
            )

    remaining_active = [row for row in active_rows if source_identity(row) not in demoted_ids]
    return unique_sources(remaining_active), unique_sources(demoted_rows)
