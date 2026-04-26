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
        _coerce_int(state.get("lastKeptCount"), 0),
        1 if row_status in {"ok", "success", "healthy"} else 0,
        _adapter_priority(row),
        _coerce_int(row.get("rankScore") or row.get("score"), 0),
        _metadata_score(row),
        source_identity(row),
    )


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
    target_keys = {
        _clean_family_token(item) for item in (target_families or []) if _clean_family_token(item)
    }
    rows = [ensure_source_id(dict(row)) for row in active_rows if isinstance(row, dict)]
    source_state_by_key = _state_rows_by_key(source_state)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        family_key = source_family_key(row)
        if not family_key:
            continue
        if target_keys and family_key not in target_keys:
            continue
        grouped.setdefault(family_key, []).append(row)

    demoted_ids: set[str] = set()
    demoted_rows: list[dict[str, Any]] = []
    for family_key, family_rows in grouped.items():
        if len(family_rows) < 2:
            continue
        winner = max(family_rows, key=lambda row: _duplicate_winner_score(row, source_state_by_key))
        winner_id = source_identity(winner)
        for row in family_rows:
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

    remaining_active = [row for row in rows if source_identity(row) not in demoted_ids]
    return unique_sources(remaining_active), unique_sources(demoted_rows)
