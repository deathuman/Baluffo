"""Reconcile carried source-bundle entries with their surviving primary rows.

A carried ``sourceBundle`` entry can point at a job that survives as a separate
row after a merge guard rejects the pair. This leaf plans moving that entry to
the row whose primary job link owns it, without changing dedupe policy or row
order.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, MutableSequence, Sequence
from dataclasses import dataclass
from typing import Any

from src.jobs.common.url import fingerprint_url
from src.jobs.text_utils import clean_text, norm_text

SourceBundleEntryKey = tuple[str, str, str]


@dataclass(frozen=True)
class SourceBundleMove:
    source_index: int
    target_index: int
    entry_index: int
    entry: dict[str, Any]
    entry_key: SourceBundleEntryKey
    target_has_entry: bool


@dataclass(frozen=True)
class AmbiguousSourceBundleEntry:
    source_index: int
    entry_index: int
    entry: dict[str, Any]
    candidate_indices: tuple[int, ...]


@dataclass(frozen=True)
class ReconciliationPlan:
    moves: tuple[SourceBundleMove, ...]
    ambiguous: tuple[AmbiguousSourceBundleEntry, ...]

    @property
    def mismatch_count(self) -> int:
        return len(self.moves) + len(self.ambiguous)


def _source_bundle_entry_key(entry: Mapping[str, Any]) -> SourceBundleEntryKey:
    return (
        norm_text(entry.get("source")),
        norm_text(entry.get("sourceJobId")),
        norm_text(entry.get("jobLink")),
    )


def _source_bundle_count(row: Mapping[str, Any]) -> int:
    try:
        return max(0, int(row.get("sourceBundleCount") or 0))
    except (TypeError, ValueError):
        return 0


def _bundle_has_entry(row: Mapping[str, Any], entry_key: SourceBundleEntryKey) -> bool:
    bundle = row.get("sourceBundle")
    if not isinstance(bundle, list):
        return False
    return any(
        isinstance(item, Mapping) and _source_bundle_entry_key(item) == entry_key for item in bundle
    )


def plan_source_bundle_reconciliation(
    rows: Sequence[Mapping[str, Any]],
) -> ReconciliationPlan:
    """Plan moves for entries whose job link belongs to another surviving row."""
    primary_indices: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        primary = fingerprint_url(row.get("jobLink"))
        if primary:
            primary_indices[primary].append(index)

    moves: list[SourceBundleMove] = []
    ambiguous: list[AmbiguousSourceBundleEntry] = []
    for source_index, row in enumerate(rows):
        bundle = row.get("sourceBundle")
        if not isinstance(bundle, list):
            continue
        source_primary = fingerprint_url(row.get("jobLink"))
        for entry_index, raw_entry in enumerate(bundle):
            if not isinstance(raw_entry, Mapping):
                continue
            entry = dict(raw_entry)
            entry_primary = fingerprint_url(entry.get("jobLink"))
            if not entry_primary or entry_primary == source_primary:
                continue
            candidate_indices = tuple(
                index for index in primary_indices.get(entry_primary, ()) if index != source_index
            )
            if not candidate_indices:
                continue
            if len(candidate_indices) > 1:
                ambiguous.append(
                    AmbiguousSourceBundleEntry(
                        source_index=source_index,
                        entry_index=entry_index,
                        entry=entry,
                        candidate_indices=candidate_indices,
                    )
                )
                continue
            target_index = candidate_indices[0]
            entry_key = _source_bundle_entry_key(entry)
            moves.append(
                SourceBundleMove(
                    source_index=source_index,
                    target_index=target_index,
                    entry_index=entry_index,
                    entry=entry,
                    entry_key=entry_key,
                    target_has_entry=_bundle_has_entry(rows[target_index], entry_key),
                )
            )
    return ReconciliationPlan(moves=tuple(moves), ambiguous=tuple(ambiguous))


def _row_report_id(row: Mapping[str, Any], index: int) -> str:
    value = row.get("id")
    return str(value if value is not None else index)


def _row_bundle_count(row: Mapping[str, Any]) -> int:
    bundle = row.get("sourceBundle")
    return len(bundle) if isinstance(bundle, list) else 0


def source_bundle_mismatch_examples(
    rows: Sequence[Mapping[str, Any]],
    plan: ReconciliationPlan,
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Describe planned moves for report payloads without changing rows."""
    items: list[tuple[int, int, dict[str, Any]]] = []
    for move in plan.moves:
        source = rows[move.source_index]
        target = rows[move.target_index]
        items.append(
            (
                move.source_index,
                move.entry_index,
                {
                    "status": "movable",
                    "sourceRowId": _row_report_id(source, move.source_index),
                    "sourceTitle": clean_text(source.get("title")),
                    "sourceCompany": clean_text(source.get("company")),
                    "sourceJobLink": clean_text(source.get("jobLink")),
                    "sourceBundleCount": _row_bundle_count(source),
                    "entrySource": clean_text(move.entry.get("source")),
                    "entrySourceJobId": clean_text(move.entry.get("sourceJobId")),
                    "entryJobLink": clean_text(move.entry.get("jobLink")),
                    "targetRowId": _row_report_id(target, move.target_index),
                    "targetTitle": clean_text(target.get("title")),
                    "targetCompany": clean_text(target.get("company")),
                    "targetJobLink": clean_text(target.get("jobLink")),
                    "targetAlreadyHasEntry": move.target_has_entry,
                    "candidateRowIds": [],
                },
            )
        )
    for item in plan.ambiguous:
        source = rows[item.source_index]
        items.append(
            (
                item.source_index,
                item.entry_index,
                {
                    "status": "ambiguous",
                    "sourceRowId": _row_report_id(source, item.source_index),
                    "sourceTitle": clean_text(source.get("title")),
                    "sourceCompany": clean_text(source.get("company")),
                    "sourceJobLink": clean_text(source.get("jobLink")),
                    "sourceBundleCount": _row_bundle_count(source),
                    "entrySource": clean_text(item.entry.get("source")),
                    "entrySourceJobId": clean_text(item.entry.get("sourceJobId")),
                    "entryJobLink": clean_text(item.entry.get("jobLink")),
                    "targetRowId": "",
                    "targetTitle": "",
                    "targetCompany": "",
                    "targetJobLink": "",
                    "targetAlreadyHasEntry": False,
                    "candidateRowIds": [
                        _row_report_id(rows[index], index) for index in item.candidate_indices
                    ],
                },
            )
        )
    items.sort(key=lambda item: (item[0], item[1]))
    return [example for _, _, example in items[: max(0, int(limit))]]


def apply_source_bundle_reconciliation(
    rows: MutableSequence[dict[str, Any]],
    plan: ReconciliationPlan,
) -> None:
    """Apply a plan in place, removing entries before appending them to targets."""
    moves_by_source: dict[int, list[SourceBundleMove]] = defaultdict(list)
    moves_by_target: dict[int, list[SourceBundleMove]] = defaultdict(list)
    for move in plan.moves:
        moves_by_source[move.source_index].append(move)
        moves_by_target[move.target_index].append(move)

    for source_index, moves in moves_by_source.items():
        row = rows[source_index]
        bundle = row.get("sourceBundle")
        if not isinstance(bundle, list):
            continue
        remove_indices = {move.entry_index for move in moves}
        if not remove_indices.issubset(range(len(bundle))):
            raise ValueError(f"source row {source_index} bundle changed during planning")
        remaining = [entry for index, entry in enumerate(bundle) if index not in remove_indices]
        removed = len(bundle) - len(remaining)
        row["sourceBundle"] = remaining
        row["sourceBundleCount"] = max(len(remaining), _source_bundle_count(row) - removed)

    for target_index, moves in moves_by_target.items():
        row = rows[target_index]
        bundle = row.get("sourceBundle")
        if not isinstance(bundle, list):
            bundle = []
        keys = {_source_bundle_entry_key(entry) for entry in bundle if isinstance(entry, Mapping)}
        added = 0
        for move in moves:
            if move.entry_key in keys:
                continue
            bundle.append(dict(move.entry))
            keys.add(move.entry_key)
            added += 1
        row["sourceBundle"] = bundle
        row["sourceBundleCount"] = max(len(bundle), _source_bundle_count(row) + added)
