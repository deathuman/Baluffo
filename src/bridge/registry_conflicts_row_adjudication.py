"""Registry conflict row helpers — adjudication families and independent-provider boards.

AI boundary owns: adjudication family/probe matching and independent-provider-board audit derivation.
AI boundary implement in: this registry_conflicts_row_adjudication.py leaf.
AI boundary search before contracts: registry conflict routes, registry_conflicts coordinator, and frontend registry conflict callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry conflict row tests."""

from __future__ import annotations

from typing import Any

from src.bridge.registry_conflicts_row_core import (
    _as_dict,
    _as_list,
    _clean_text,
    _int_value,
    _is_provider_row,
    _jobs_found_count,
    _row_adapter,
    _row_identity,
    _row_state,
)
from src.bridge.registry_conflicts_row_identity import (
    _identity_overlap_ratio,
    _provider_slug,
    _row_job_identity_keys,
)
from src.source_registry import static_listing_url_aliases
from src.source_registry_policy import duplicate_family_conflict_cards

INDEPENDENT_PROVIDER_BOARD_ADAPTERS = {"greenhouse"}

INDEPENDENT_PROVIDER_BOARD_OVERLAP_THRESHOLD = 0.5


def _adjudication_families_by_key(adjudication_payload: Any) -> dict[str, dict[str, Any]]:
    payload = _as_dict(adjudication_payload)
    by_key: dict[str, dict[str, Any]] = {}
    observed_at = _clean_text(payload.get("finishedAt") or payload.get("startedAt"))
    for row in _as_list(payload.get("families")):
        if not isinstance(row, dict) or not _clean_text(row.get("familyKey")):
            continue
        family = dict(row)
        if observed_at:
            family["_observedAt"] = observed_at
        by_key[_clean_text(row.get("familyKey"))] = family
    return by_key


def _adjudication_probe_by_source_id(family: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        _clean_text(row.get("sourceId")): row
        for row in _as_list(family.get("probes"))
        if isinstance(row, dict) and _clean_text(row.get("sourceId"))
    }


def _adjudication_probe_matches_for_rows(
    rows: list[dict[str, Any]], probes: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    matches: dict[str, dict[str, Any]] = {}
    unmatched_probe_ids = set(probes)
    row_aliases = {
        _row_identity(row): static_listing_url_aliases(row) for row in rows if _row_identity(row)
    }
    probe_aliases = {
        probe_id: static_listing_url_aliases(probe)
        for probe_id, probe in probes.items()
        if probe_id
    }
    for row in rows:
        row_id = _row_identity(row)
        if not row_id:
            continue
        if row_id in probes:
            matches[row_id] = probes[row_id]
            unmatched_probe_ids.discard(row_id)
            continue
        aliases = row_aliases.get(row_id) or set()
        if not aliases:
            continue
        alias_matches = [
            probe_id
            for probe_id in unmatched_probe_ids
            if aliases & (probe_aliases.get(probe_id) or set())
        ]
        if len(alias_matches) == 1:
            probe_id = alias_matches[0]
            matches[row_id] = probes[probe_id]
            unmatched_probe_ids.discard(probe_id)
    if unmatched_probe_ids:
        return {}
    return matches


def _adjudication_complete_for_rows(rows: list[dict[str, Any]], family: dict[str, Any]) -> bool:
    if _clean_text(family.get("status")).lower() in {"", "running", "failed"}:
        return False
    probes = _adjudication_probe_by_source_id(family)
    row_ids = {_row_identity(row) for row in rows if _row_identity(row)}
    probe_matches = _adjudication_probe_matches_for_rows(rows, probes)
    if not row_ids or set(probe_matches) != row_ids:
        return False
    return all(
        _int_value(probe.get("httpStatus")) > 0 and not _clean_text(probe.get("error"))
        for probe in probe_matches.values()
    )


def _adjudicated_independent_provider_loser_ids(
    rows: list[dict[str, Any]], family: dict[str, Any]
) -> set[str]:
    if not family:
        return set()
    active_provider_ids = {
        _row_identity(row)
        for row in rows
        if _row_state(row) == "active" and _is_provider_row(row) and _row_identity(row)
    }
    if len(active_provider_ids) < 2:
        return set()
    checked_ids = {
        _clean_text(source_id)
        for source_id in _as_list(family.get("checkedSourceIds"))
        if _clean_text(source_id)
    }
    if not active_provider_ids <= checked_ids:
        return set()
    independent_ids: set[str] = set()
    for decision in _as_list(family.get("decisions")):
        if not isinstance(decision, dict):
            continue
        source_id = _clean_text(decision.get("sourceId"))
        status = _clean_text(decision.get("status")).lower()
        reason = _clean_text(decision.get("reason")).lower()
        if (
            source_id in active_provider_ids
            and status == "keep_both"
            and "job sets differ" in reason
        ):
            independent_ids.add(source_id)
    return independent_ids


def _active_same_adapter_provider_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active_rows = [row for row in rows if _row_state(row) == "active"]
    if len(active_rows) < 2 or len(active_rows) != len(rows):
        return []
    if not all(_is_provider_row(row) for row in active_rows):
        return []
    adapters = {_row_adapter(row) for row in active_rows}
    if len(adapters) != 1 or next(iter(adapters)) not in INDEPENDENT_PROVIDER_BOARD_ADAPTERS:
        return []
    row_ids = [_row_identity(row) for row in active_rows]
    if any(not row_id for row_id in row_ids) or len(set(row_ids)) != len(row_ids):
        return []
    slugs = [_provider_slug(row) for row in active_rows]
    if any(not slug for slug in slugs) or len(set(slugs)) != len(slugs):
        return []
    return active_rows


def _adjudication_proves_independent_provider_boards(
    rows: list[dict[str, Any]], family: dict[str, Any] | None
) -> bool:
    if not family:
        return False
    row_ids = {_row_identity(row) for row in rows if _row_identity(row)}
    checked_ids = {
        _clean_text(source_id)
        for source_id in _as_list(family.get("checkedSourceIds"))
        if _clean_text(source_id)
    }
    if not row_ids or not row_ids <= checked_ids:
        return False
    keep_both_ids = set()
    for decision in _as_list(family.get("decisions")):
        if not isinstance(decision, dict):
            continue
        source_id = _clean_text(decision.get("sourceId"))
        status = _clean_text(decision.get("status")).lower()
        reason = _clean_text(decision.get("reason")).lower()
        if status == "keep_both" and "job sets differ" in reason and source_id:
            keep_both_ids.add(source_id)
    winner_id = _clean_text(family.get("winnerSourceId"))
    expected_decisions = row_ids - {winner_id}
    return bool(expected_decisions) and expected_decisions <= keep_both_ids


def _current_jobs_prove_independent_provider_boards(
    rows: list[dict[str, Any]], job_index: dict[str, set[str]]
) -> bool:
    row_job_keys = [_row_job_identity_keys(row, job_index) for row in rows]
    if any(not keys for keys in row_job_keys):
        return False
    for index, left in enumerate(row_job_keys):
        for right in row_job_keys[index + 1 :]:
            if _identity_overlap_ratio(left, right) >= INDEPENDENT_PROVIDER_BOARD_OVERLAP_THRESHOLD:
                return False
    return True


def _independent_provider_board_audit_row(
    *,
    family_key: str,
    rows: list[dict[str, Any]],
    evidence_reason: str,
) -> dict[str, Any]:
    return {
        "familyKey": family_key,
        "rowCount": len(rows),
        "adapter": _row_adapter(rows[0]) if rows else "",
        "sourceIds": [_row_identity(row) for row in rows if _row_identity(row)],
        "evidenceReason": evidence_reason,
    }


def _row_with_live_adjudication(
    row: dict[str, Any], probe: dict[str, Any], *, observed_at: str = ""
) -> dict[str, Any]:
    next_row = dict(row)
    if "jobsFound" in next_row or "sampleCount" in next_row:
        next_row["registryJobsFound"] = _jobs_found_count(next_row)
    live_jobs = max(0, _int_value(probe.get("jobsFound")))
    next_row["liveJobsFound"] = live_jobs
    next_row["jobsFound"] = live_jobs
    next_row["sampleCount"] = live_jobs
    next_row["liveProbeOk"] = bool(probe.get("ok"))
    next_row["liveProbeHttpStatus"] = _int_value(probe.get("httpStatus"))
    next_row["liveProbeFinalUrl"] = _clean_text(probe.get("finalUrl"))
    if observed_at:
        next_row.setdefault("lastCheckedAt", observed_at)
        next_row.setdefault("lastSeenInFetchAt", observed_at)
        if probe.get("ok"):
            next_row.setdefault("lastSuccessAt", observed_at)
            next_row.setdefault("lastSuccessfulFetchAt", observed_at)
    if probe.get("ok") and live_jobs > 0:
        next_row.setdefault("lastStatus", "ok")
        next_row.setdefault("health", "healthy")
        next_row.setdefault("healthReason", "live adjudication found jobs")
    elif probe.get("ok"):
        next_row.setdefault("lastStatus", "ok")
        next_row.setdefault("health", "warning")
        next_row.setdefault("healthReason", "live adjudication found no jobs")
    else:
        next_row.setdefault("lastStatus", "error")
        next_row.setdefault("health", "broken")
        next_row.setdefault("healthReason", "live adjudication probe failed")
    return next_row


def _with_live_adjudication_card(
    card: dict[str, Any],
    *,
    family: dict[str, Any],
    source_state_payload: Any,
) -> dict[str, Any]:
    rows = [_as_dict(row) for row in _as_list(card.get("rows")) if isinstance(row, dict)]
    if not _adjudication_complete_for_rows(rows, family):
        return {**card, "effectiveWinnerSource": "registry"}
    probes = _adjudication_probe_by_source_id(family)
    probe_matches = _adjudication_probe_matches_for_rows(rows, probes)
    original_winner_id = _row_identity(_as_dict(card.get("winner")))
    observed_at = _clean_text(family.get("_observedAt"))
    live_rows = [
        _row_with_live_adjudication(row, probe_matches[_row_identity(row)], observed_at=observed_at)
        for row in rows
        if _row_identity(row) in probe_matches
    ]
    recalculated = duplicate_family_conflict_cards(
        live_rows,
        target_families=[_clean_text(card.get("familyKey"))],
        source_state=source_state_payload,
    )
    if not recalculated:
        return {**card, "effectiveWinnerSource": "registry"}
    next_card = dict(recalculated[0])
    next_winner_id = _row_identity(_as_dict(next_card.get("winner")))
    next_card["adjudication"] = family
    next_card["liveAdjudicationComplete"] = True
    next_card["effectiveWinnerSource"] = (
        "live_adjudication" if next_winner_id != original_winner_id else "registry"
    )
    return next_card
