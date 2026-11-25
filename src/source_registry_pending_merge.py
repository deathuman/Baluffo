"""Field-level merge of discovery-run pending rows onto authority pending rows.

The authority owns a pending row's identity, routing, display and lifecycle: an
admin or sync may have changed them while discovery ran. A run owns the
objective observations it just measured. This leaf carries those observations
forward only when the run's probe is newer than the authority row's, so a stale
run snapshot cannot roll a fresher check back. A run may fill a blank display or
routing field only when its probe is newer than the authority row's; it never
replaces a non-blank one.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from src.shared.utils import parse_iso
from src.source_registry_identity import source_identity, source_url_fingerprint, unique_sources
from src.source_registry_state import (
    REGISTRY_REASON_REPEATED_ZERO_JOBS,
    apply_repeated_zero_job_hide,
    is_hidden_from_default,
)

# Observations the run measured. They are copied only when the run's
# ``lastProbedAt`` is newer than the authority row's.
#
# Only fields the discovery run itself writes belong here: every other field is
# owned by a fresher writer, and the run carries a start-of-run snapshot.
RUN_OBSERVATION_FIELDS = (
    "jobsFound",
    "sampleCount",
    "lastProbedAt",
    "lastProbeStatus",
    "lastProbeError",
    "confidence",
    "evidenceScore",
    "evidenceTypes",
    "score",
    "rankScore",
    "rankReasons",
    "reasons",
    "weakSignal",
    "deferred",
    "deferReason",
    "promotionLane",
)

# Identity, routing and display fields owned by the authority (admin, sync or
# manual add). A run may fill a blank value when its probe is newer, but never
# replace a non-blank one.
RUN_FILL_ONLY_FIELDS = (
    "name",
    "studio",
    "company",
    "careersUrl",
    "url",
    "listing_url",
    "board_url",
    "api_url",
    "feed_url",
    "endpointUrl",
    "finalUrl",
    "pages",
)

# Routing fields decide where a source is fetched. A run that disagrees with the
# authority on one needs review before auto-approval; display-only fields do not.
RUN_ROUTING_FIELDS = frozenset(
    {
        "careersUrl",
        "url",
        "listing_url",
        "board_url",
        "api_url",
        "feed_url",
        "endpointUrl",
        "finalUrl",
        "pages",
    }
)

_FAILED_PROBE_STATUSES = frozenset({"error", "failed", "failure"})

# A large run can disagree with the authority on many rows; keep the visible
# sample bounded while the audit count stays exact.
MERGE_CONFLICT_LIMIT = 20
_CONFLICT_VALUE_CHARS = 300


def _bump(audit: dict[str, int], key: str) -> None:
    audit[key] = audit.get(key, 0) + 1


def _run_is_newer(current: Mapping[str, Any], run: Mapping[str, Any]) -> bool:
    run_at = parse_iso(run.get("lastProbedAt"))
    if run_at is None:
        return False
    current_at = parse_iso(current.get("lastProbedAt"))
    return current_at is None or run_at > current_at


def _run_reports_probe_failure(run: Mapping[str, Any]) -> bool:
    if str(run.get("lastProbeError") or "").strip():
        return True
    status = str(run.get("lastProbeStatus") or "").strip().lower()
    return status in _FAILED_PROBE_STATUSES


def _copy_value(value: Any) -> Any:
    return list(value) if isinstance(value, list) else value


def _carry_observations(current: dict[str, Any], run: Mapping[str, Any]) -> None:
    failed_probe = _run_reports_probe_failure(run)
    for field in RUN_OBSERVATION_FIELDS:
        if field not in run:
            continue
        if failed_probe and field in {"jobsFound", "sampleCount"}:
            # A failed probe's zero is not a fresh opening count; keep the last
            # real measurement and carry the failure evidence instead.
            continue
        value = run[field]
        current[field] = str(value or "") if field == "lastProbeError" else _copy_value(value)
    if "lastProbeError" not in run and not failed_probe:
        current["lastProbeError"] = ""


def _bounded_conflict_value(value: Any) -> str:
    """Bound one conflicting value so a large run cannot bloat the audit."""
    text = ", ".join(str(item) for item in value) if isinstance(value, list) else str(value)
    text = text.strip()
    if len(text) <= _CONFLICT_VALUE_CHARS:
        return text
    return f"{text[: _CONFLICT_VALUE_CHARS - 3]}..."


def _fill_only_values_differ(current: Any, run: Any) -> bool:
    if isinstance(current, list) or isinstance(run, list):
        return bool(current != run)
    return str(current).strip() != str(run).strip()


def _record_fill_only_conflict(
    *,
    conflicts: list[dict[str, Any]] | None,
    audit: dict[str, int],
    identity: str,
    field: str,
    current: Any,
    run: Any,
    routing_conflict_ids: set[str] | None = None,
) -> None:
    _bump(audit, "fieldConflicts")
    if routing_conflict_ids is not None and identity and field in RUN_ROUTING_FIELDS:
        # Keep this complete even when the visible conflict sample is capped.
        routing_conflict_ids.add(identity)
    if conflicts is None or len(conflicts) >= MERGE_CONFLICT_LIMIT:
        return
    conflicts.append(
        {
            "identity": identity,
            "field": field,
            "authorityValue": _bounded_conflict_value(current),
            "runValue": _bounded_conflict_value(run),
        }
    )


def _record_placed_fill_only_conflicts(
    run: Mapping[str, Any],
    authority: Mapping[str, Any],
    *,
    identity: str,
    conflicts: list[dict[str, Any]] | None,
    audit: dict[str, int],
) -> None:
    """Compare a dropped run row's fill-only fields without changing authority state."""
    for field in RUN_FILL_ONLY_FIELDS:
        value = run.get(field)
        if not value:
            continue
        if field == "pages" and not isinstance(value, list):
            continue
        authority_value = authority.get(field)
        if not str(authority_value or "").strip():
            # A blank authority field is a fill opportunity, not a conflict.
            continue
        if not _fill_only_values_differ(authority_value, value):
            continue
        _bump(audit, "activeFieldConflicts")
        if conflicts is None or len(conflicts) >= MERGE_CONFLICT_LIMIT:
            continue
        conflicts.append(
            {
                "identity": identity,
                "field": field,
                "authorityValue": _bounded_conflict_value(authority_value),
                "runValue": _bounded_conflict_value(value),
            }
        )


def _carry_fill_only(
    current: dict[str, Any],
    run: Mapping[str, Any],
    *,
    identity: str,
    conflicts: list[dict[str, Any]] | None,
    audit: dict[str, int],
    routing_conflict_ids: set[str] | None = None,
) -> None:
    for field in RUN_FILL_ONLY_FIELDS:
        value = run.get(field)
        if not value:
            continue
        if field == "pages" and not isinstance(value, list):
            continue
        current_value = current.get(field)
        if not str(current_value or "").strip():
            current[field] = _copy_value(value)
            continue
        if not _fill_only_values_differ(current_value, value):
            continue
        _record_fill_only_conflict(
            conflicts=conflicts,
            audit=audit,
            identity=identity,
            field=field,
            current=current_value,
            run=value,
            routing_conflict_ids=routing_conflict_ids,
        )


def _carry_zero_job_hide(row: dict[str, Any], run_row: dict[str, Any]) -> None:
    """Carry a run's repeated-zero-job hide onto an existing pending row.

    The run already ran the hide predicate; reuse the policy's transition fields
    instead of keeping a second copy of them here.
    """
    if is_hidden_from_default(row) or not is_hidden_from_default(run_row):
        return
    if str(run_row.get("pendingReason") or "").strip() != REGISTRY_REASON_REPEATED_ZERO_JOBS:
        return
    row.update(
        apply_repeated_zero_job_hide(
            row, at=str(run_row.get("lastDemotedAt") or "").strip() or None
        )
    )


def _run_row_drop_key(
    identity: str,
    row: dict[str, Any],
    *,
    placed: set[str],
    tombstoned_ids: set[str],
    tombstoned_fingerprints: set[str],
) -> str:
    if identity in placed:
        return "rowsDroppedPlaced"
    if identity in tombstoned_ids:
        return "rowsDroppedTombstoned"
    if tombstoned_fingerprints and source_url_fingerprint(row) in tombstoned_fingerprints:
        return "rowsDroppedTombstoned"
    return ""


def _collapse_run_rows(
    run_pending: Sequence[Mapping[str, Any]],
    *,
    placed: set[str],
    placed_rows: Mapping[str, Mapping[str, Any]] | None,
    tombstoned_ids: set[str],
    tombstoned_fingerprints: set[str],
    audit: dict[str, int],
    active_conflicts: list[dict[str, Any]] | None,
) -> dict[str, dict[str, Any]]:
    run_by_id: dict[str, dict[str, Any]] = {}
    for raw in run_pending:
        if not isinstance(raw, Mapping):
            continue
        row = dict(raw)
        identity = source_identity(row)
        if not identity:
            continue
        drop_key = _run_row_drop_key(
            identity,
            row,
            placed=placed,
            tombstoned_ids=tombstoned_ids,
            tombstoned_fingerprints=tombstoned_fingerprints,
        )
        if drop_key:
            _bump(audit, drop_key)
            if drop_key == "rowsDroppedPlaced" and placed_rows is not None:
                authority_row = placed_rows.get(identity.strip().lower())
                if authority_row is not None:
                    _record_placed_fill_only_conflicts(
                        row,
                        authority_row,
                        identity=identity,
                        conflicts=active_conflicts,
                        audit=audit,
                    )
            continue
        prior = run_by_id.get(identity)
        if prior is not None:
            _bump(audit, "duplicateRunRows")
            # Duplicate identities are collapsed by probe freshness, not input
            # order: a provider-migration and a direct candidate for the same
            # source must not let the weaker copy win.
            if not _run_is_newer(prior, row):
                continue
        run_by_id[identity] = row
    return run_by_id


def merge_run_pending_rows(
    current_pending: Iterable[Mapping[str, Any]],
    run_pending: Sequence[Mapping[str, Any]],
    *,
    placed_ids: Iterable[str] = (),
    placed_rows: Mapping[str, Mapping[str, Any]] | None = None,
    tombstones: Mapping[str, Any] | None = None,
    audit: dict[str, int] | None = None,
    conflicts: list[dict[str, Any]] | None = None,
    active_conflicts: list[dict[str, Any]] | None = None,
    routing_conflict_ids: set[str] | None = None,
) -> tuple[list[dict[str, Any]], int, int]:
    """Merge a run's pending rows onto the authority's pending bucket.

    The authority keeps identity, routing, display and lifecycle: a run may
    carry its newer observations, and a repeated-zero-job hide it applied is a
    lifecycle transition the authority does not own. Rows the authority already
    placed (active/rejected) or tombstoned are not re-added, by identity or by
    URL fingerprint. This is the one pending merge; the authority publish and the
    bridge's failed-publish restage both call it instead of keeping a copy.

    Returns ``(merged_pending, restaged_count, updated_count)``, where
    ``restaged_count`` counts rows that were not pending before and
    ``updated_count`` counts existing rows whose content changed. When ``audit``
    is passed it is filled with ``rowsDroppedPlaced``, ``rowsDroppedTombstoned``,
    ``duplicateRunRows`` and ``observationsSkippedStaleRun`` counters, plus
    ``fieldConflicts`` for non-blank run values the authority kept. When
    ``conflicts`` is passed, up to ``MERGE_CONFLICT_LIMIT`` of those conflicts
    are appended as ``{identity, field, authorityValue, runValue}`` records with
    bounded values; the count remains exact when the list is capped. When
    ``routing_conflict_ids`` is passed, every identity with a routing-field
    conflict is added, including conflicts beyond the bounded sample. When
    ``placed_rows`` is passed, a run row dropped because it is already active or
    rejected has its fill-only fields compared with that authority row; the
    comparison never mutates the authority row. ``active_conflicts`` receives up
    to ``MERGE_CONFLICT_LIMIT`` of those records, while
    ``audit["activeFieldConflicts"]`` keeps the exact count.
    """
    stats = audit if audit is not None else {}
    placed = {str(value).strip().lower() for value in placed_ids if str(value).strip()}
    records = tombstones if isinstance(tombstones, Mapping) else {}
    tombstoned_ids = {str(key).strip().lower() for key in records}
    tombstoned_fingerprints = {
        str(record.get("sourceUrlFingerprint") or "").strip().lower()
        for record in records.values()
        if isinstance(record, Mapping)
    }
    tombstoned_fingerprints.discard("")

    placed_rows_by_id = {
        str(key).strip().lower(): row
        for key, row in (placed_rows or {}).items()
        if isinstance(row, Mapping)
    }
    current_rows = [dict(row) for row in current_pending if isinstance(row, Mapping)]
    current_ids = {source_identity(row) for row in current_rows}
    run_by_id = _collapse_run_rows(
        run_pending,
        placed=placed,
        placed_rows=placed_rows_by_id,
        tombstoned_ids=tombstoned_ids,
        tombstoned_fingerprints=tombstoned_fingerprints,
        audit=stats,
        active_conflicts=active_conflicts,
    )

    updated_count = 0
    for row in current_rows:
        identity = source_identity(row)
        run_row = run_by_id.get(identity)
        if run_row is None:
            continue
        before = dict(row)
        run_is_newer = _run_is_newer(row, run_row)
        if run_is_newer:
            _carry_fill_only(
                row,
                run_row,
                identity=identity,
                conflicts=conflicts,
                audit=stats,
                routing_conflict_ids=routing_conflict_ids,
            )
            _carry_observations(row, run_row)
        elif any(field in run_row for field in RUN_OBSERVATION_FIELDS):
            # The authority probed later, so its values stay. Count only run rows
            # that actually carried observations to skip.
            _bump(stats, "observationsSkippedStaleRun")
        _carry_zero_job_hide(row, run_row)
        if row != before:
            updated_count += 1

    extra = [row for identity, row in run_by_id.items() if identity not in current_ids]
    return unique_sources([*current_rows, *extra]), len(extra), updated_count


__all__ = [
    "MERGE_CONFLICT_LIMIT",
    "RUN_FILL_ONLY_FIELDS",
    "RUN_OBSERVATION_FIELDS",
    "RUN_ROUTING_FIELDS",
    "merge_run_pending_rows",
]
