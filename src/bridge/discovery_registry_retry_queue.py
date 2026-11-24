"""Bounded reconciliation retry queue snapshots.

The bridge keeps a small queue of terminal discovery reports whose registry
reconciliation hit the write-lock timeout. Each entry must survive a restart
without storing the whole report, and the queue itself must stay bounded.
"""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

from src.shared.json_shapes import as_json_list, as_json_object
from src.shared.utils import parse_iso
from src.source_registry_auto_approval import _cap_deferred_candidate_is_auto_approvable
from src.source_registry_identity import source_identity

RECONCILE_RETRY_MAX_ENTRIES = 25
# The entry cap bounds count, not bytes: a run with hundreds of cap-deferred
# candidates writes a large snapshot, and every drain reloads and parses it.
# The budget bounds the accumulated uncompressed parse cost; gzip shrinks the
# file on disk. The entry being recorded is always kept, even when it alone
# exceeds the budget.
RECONCILE_RETRY_MAX_BYTES = 2 * 1024 * 1024
_RETRY_REPORT_FIELDS = ("startedAt", "finishedAt")
_RETRY_SUMMARY_FIELDS = ("approvedCandidateCount", "liveCandidateCount")
_RETRY_AUTO_APPROVAL_FIELDS = ("enabled", "status", "approvedCount")
_RETRY_FINALIZATION_FIELDS = (
    "status",
    "error",
    "pendingCount",
    "activeCount",
    "rejectedCount",
    "pendingRows",
)
_RETRY_LIFECYCLE_FIELDS = ("heartbeatAt",)
_RETRY_FOLDED_CANDIDATE_IDS = "pendingCandidateIds"


def _projected_fields(source: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {field: source[field] for field in fields if field in source}


def _row_identity(row: dict[str, Any]) -> str:
    return str(source_identity(row) or "").strip()


def _pending_rows(finalization: dict[str, Any]) -> list[dict[str, Any]]:
    return [row for row in as_json_list(finalization.get("pendingRows")) if isinstance(row, dict)]


def _pending_rows_by_id(finalization: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows_by_id: dict[str, dict[str, Any]] = {}
    for row in _pending_rows(finalization):
        identity = _row_identity(row)
        if identity:
            rows_by_id[identity] = row
    return rows_by_id


def _row_covers(pending_row: dict[str, Any], candidate: dict[str, Any]) -> bool:
    """Whether the pending row already stores every candidate field verbatim."""
    return all(
        field in pending_row and pending_row[field] == value for field, value in candidate.items()
    )


def _retry_candidates(
    report: dict[str, Any], finalization: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[str]]:
    """Keep the candidates the drain reads and fold pending-row copies into ``pendingRows``.

    A queued candidate is stored verbatim as its pending row, so persisting both
    writes the same evidence twice. The pending row stays authoritative for the
    copy; its identity is listed so the drain can restore the candidate view. A
    cap-deferred candidate stays in the list because the auto-approver stages
    that row itself into active.
    """
    pending_by_id = _pending_rows_by_id(finalization)
    candidates: list[dict[str, Any]] = []
    folded_ids: list[str] = []
    for row in as_json_list(report.get("candidates")):
        if not isinstance(row, dict):
            continue
        identity = _row_identity(row)
        if not identity:
            continue
        pending_row = pending_by_id.get(identity)
        cap_deferred = _cap_deferred_candidate_is_auto_approvable(row)
        if pending_row is not None and not cap_deferred and _row_covers(pending_row, row):
            folded_ids.append(identity)
            continue
        if pending_row is not None or cap_deferred:
            # Stored whole: auto-approval stages this row into active, so the
            # snapshot must carry every field that row writes.
            candidates.append(row)
    return candidates, folded_ids


def retry_report_snapshot(report: dict[str, Any]) -> dict[str, Any]:
    """Keep only the report fields the retry drain reads."""
    snapshot = _projected_fields(report, _RETRY_REPORT_FIELDS)
    summary = _projected_fields(as_json_object(report.get("summary")), _RETRY_SUMMARY_FIELDS)
    if summary:
        snapshot["summary"] = summary

    runtime = as_json_object(report.get("runtime"))
    finalization = as_json_object(runtime.get("registryFinalization"))
    runtime_snapshot: dict[str, Any] = {}
    auto_approval = _projected_fields(
        as_json_object(runtime.get("autoApproval")), _RETRY_AUTO_APPROVAL_FIELDS
    )
    if auto_approval:
        runtime_snapshot["autoApproval"] = auto_approval
    projected_finalization = _projected_fields(finalization, _RETRY_FINALIZATION_FIELDS)
    if projected_finalization:
        runtime_snapshot["registryFinalization"] = projected_finalization
    lifecycle = _projected_fields(as_json_object(runtime.get("lifecycle")), _RETRY_LIFECYCLE_FIELDS)
    if lifecycle:
        runtime_snapshot["lifecycle"] = lifecycle
    if runtime_snapshot:
        snapshot["runtime"] = runtime_snapshot

    candidates, folded_ids = _retry_candidates(report, finalization)
    if candidates:
        snapshot["candidates"] = candidates
    if folded_ids:
        snapshot[_RETRY_FOLDED_CANDIDATE_IDS] = folded_ids
    return deepcopy(snapshot)


def retry_report_for_drain(report: dict[str, Any]) -> dict[str, Any]:
    """Restore the candidate copies the snapshot folded into ``pendingRows``.

    Auto-approval reads a pending row's report-side evidence through the
    candidate list. The snapshot stores that evidence once; this rebuilds the
    copies for the drain without persisting them twice.
    """
    folded = {
        str(value).strip()
        for value in as_json_list(report.get(_RETRY_FOLDED_CANDIDATE_IDS))
        if str(value).strip()
    }
    if not folded:
        return report
    finalization = as_json_object(as_json_object(report.get("runtime")).get("registryFinalization"))
    candidates = as_json_list(report.get("candidates"))
    present = {
        identity
        for row in candidates
        if isinstance(row, dict)
        for identity in (_row_identity(row),)
        if identity
    }
    restored: list[dict[str, Any]] = []
    for row in _pending_rows(finalization):
        identity = _row_identity(row)
        if identity in folded and identity not in present:
            restored.append(dict(row))
    rehydrated = {key: value for key, value in report.items() if key != _RETRY_FOLDED_CANDIDATE_IDS}
    if restored:
        rehydrated["candidates"] = [*candidates, *restored]
    return rehydrated


def _retry_finished_timestamp(entry: dict[str, Any]) -> float:
    parsed = parse_iso(entry.get("finishedAt"))
    return parsed.timestamp() if parsed is not None else 0.0


def _oldest_run_id(retries: dict[str, dict[str, Any]], *, keep_run_id: str) -> str:
    candidates = [run_id for run_id in retries if run_id != keep_run_id]
    if not candidates:
        return ""
    return min(
        candidates,
        key=lambda run_id: (_retry_finished_timestamp(retries[run_id]), run_id),
    )


def _serialized_bytes(payload: Any) -> int:
    return len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def retry_queue_bytes(retries: dict[str, dict[str, Any]]) -> int:
    """Uncompressed serialized size of the queue payload, the unit the budget uses."""
    return _serialized_bytes({"retries": retries})


def _entry_serialized_size(run_id: str, entry: dict[str, Any]) -> int:
    """Bytes one entry contributes to the queue object, without its comma separator."""
    key = json.dumps(run_id, ensure_ascii=False)
    value = json.dumps(entry, ensure_ascii=False, separators=(",", ":"))
    return len(f"{key}:{value}".encode())


def retry_entry_bytes(run_id: str, entry: dict[str, Any]) -> int:
    """Serialized size of one queued entry, measured for the oversized warning."""
    return _entry_serialized_size(run_id, entry) + 2  # the wrapping braces


def evict_oldest_retries(
    retries: dict[str, dict[str, Any]],
    *,
    keep_run_id: str,
    max_entries: int,
) -> list[str]:
    """Drop the oldest queued retries while preserving the entry just recorded."""
    evicted: list[str] = []
    while len(retries) > max_entries:
        oldest = _oldest_run_id(retries, keep_run_id=keep_run_id)
        if not oldest:
            break
        retries.pop(oldest)
        evicted.append(oldest)
    return evicted


def evict_retries_over_budget(
    retries: dict[str, dict[str, Any]],
    *,
    keep_run_id: str,
    max_bytes: int,
) -> list[str]:
    """Drop the oldest queued retries until the uncompressed queue fits the budget.

    The budget bounds the accumulated parse cost of the queued snapshots, not the
    gzip file size. The entry just recorded is never dropped, so the queue can
    stay over budget when that entry alone exceeds it; the caller logs that case.
    The queue is sized once, then each eviction subtracts the removed entry's
    exact serialized bytes and its separating comma, so the queue is never
    re-serialized per eviction.
    """
    budget = max(1, int(max_bytes))
    total = retry_queue_bytes(retries)
    evicted: list[str] = []
    while len(retries) > 1 and total > budget:
        oldest = _oldest_run_id(retries, keep_run_id=keep_run_id)
        # The queue has at least two entries here, so one comma goes with it.
        total -= _entry_serialized_size(oldest, retries[oldest]) + 1
        retries.pop(oldest)
        evicted.append(oldest)
    return evicted


__all__ = [
    "RECONCILE_RETRY_MAX_BYTES",
    "RECONCILE_RETRY_MAX_ENTRIES",
    "evict_oldest_retries",
    "evict_retries_over_budget",
    "retry_entry_bytes",
    "retry_queue_bytes",
    "retry_report_for_drain",
    "retry_report_snapshot",
]
