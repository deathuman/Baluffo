"""Freshness, duplicate and audit rules of the pending-row merge."""

from __future__ import annotations

from typing import Any

from src.source_registry_identity import source_identity
from src.source_registry_pending_merge import (
    MERGE_CONFLICT_LIMIT,
    merge_run_pending_rows,
)

_CURRENT = {
    "id": "b",
    "adapter": "static",
    "name": "Studio B",
    "careersUrl": "https://b.example/careers",
    "jobsFound": 1,
    "lastProbedAt": "2026-07-01T09:00:00+00:00",
}


def test_duplicate_run_rows_keep_the_newest_probe_in_either_order() -> None:
    older = {**_CURRENT, "jobsFound": 2, "lastProbedAt": "2026-07-01T09:30:00+00:00"}
    newer = {**_CURRENT, "jobsFound": 7, "lastProbedAt": "2026-07-01T10:00:00+00:00"}

    for run_rows in ([older, newer], [newer, older]):
        merged, restaged, updated = merge_run_pending_rows([_CURRENT], run_rows)
        assert merged[0]["jobsFound"] == 7
        assert (restaged, updated) == (0, 1)


def test_audit_counts_dropped_rows_duplicates_and_skipped_observations() -> None:
    placed = {**_CURRENT, "id": "placed"}
    tombstoned = {**_CURRENT, "id": "tombstoned"}
    stale = {**_CURRENT, "jobsFound": 5, "lastProbedAt": "2026-07-01T08:00:00+00:00"}
    duplicate = {**_CURRENT, "jobsFound": 4, "lastProbedAt": "2026-07-01T08:30:00+00:00"}
    audit: dict[str, int] = {}

    merged, restaged, updated = merge_run_pending_rows(
        [_CURRENT],
        [stale, duplicate, placed, tombstoned],
        placed_ids=[source_identity(placed)],
        tombstones={source_identity(tombstoned): {"id": "tombstoned"}},
        audit=audit,
    )

    assert merged[0]["jobsFound"] == 1
    assert (restaged, updated) == (0, 0)
    assert audit == {
        "rowsDroppedPlaced": 1,
        "rowsDroppedTombstoned": 1,
        "duplicateRunRows": 1,
        "observationsSkippedStaleRun": 1,
    }


def test_stale_run_cannot_refill_a_field_the_authority_cleared() -> None:
    current = {
        **_CURRENT,
        "name": "",
        "careersUrl": "",
        "lastProbedAt": "2026-07-01T10:00:00+00:00",
    }
    run = {
        **_CURRENT,
        "name": "Run Studio B",
        "careersUrl": "https://b.example/run",
        "lastProbedAt": "2026-07-01T09:00:00+00:00",
    }

    merged, restaged, updated = merge_run_pending_rows([current], [run])

    assert (restaged, updated) == (0, 0)
    assert merged[0]["name"] == ""
    assert merged[0]["careersUrl"] == ""


def test_fresh_run_fills_a_field_the_authority_left_blank() -> None:
    current = {
        **_CURRENT,
        "name": "",
        "careersUrl": "",
        "lastProbedAt": "2026-07-01T09:00:00+00:00",
    }
    run = {
        **_CURRENT,
        "name": "Run Studio B",
        "careersUrl": "https://b.example/run",
        "lastProbedAt": "2026-07-01T10:00:00+00:00",
    }

    merged, restaged, updated = merge_run_pending_rows([current], [run])

    assert (restaged, updated) == (0, 1)
    assert merged[0]["name"] == "Run Studio B"
    assert merged[0]["careersUrl"] == "https://b.example/run"


def test_fill_only_conflicts_record_the_dropped_run_values() -> None:
    current = {
        **_CURRENT,
        "careersUrl": "https://b.example/old-careers",
        "lastProbedAt": "2026-07-01T09:00:00+00:00",
    }
    run = {
        **_CURRENT,
        "name": "Migrated Studio B",
        "careersUrl": "https://b.example/new-careers",
        "lastProbedAt": "2026-07-01T10:00:00+00:00",
    }
    audit: dict[str, int] = {}
    conflicts: list[dict[str, Any]] = []
    routing_conflict_ids: set[str] = set()

    merged, restaged, updated = merge_run_pending_rows(
        [current],
        [run],
        audit=audit,
        conflicts=conflicts,
        routing_conflict_ids=routing_conflict_ids,
    )

    assert (restaged, updated) == (0, 1)
    assert merged[0]["name"] == "Studio B"
    assert merged[0]["careersUrl"] == "https://b.example/old-careers"
    assert audit["fieldConflicts"] == 2
    assert conflicts == [
        {
            "identity": source_identity(current),
            "field": "name",
            "authorityValue": "Studio B",
            "runValue": "Migrated Studio B",
        },
        {
            "identity": source_identity(current),
            "field": "careersUrl",
            "authorityValue": "https://b.example/old-careers",
            "runValue": "https://b.example/new-careers",
        },
    ]
    # A display-only name conflict is not a routing conflict.
    assert routing_conflict_ids == {source_identity(current)}


def test_fill_only_conflicts_are_capped_and_values_bounded() -> None:
    current = [
        {
            "id": f"row-{index}",
            "adapter": "static",
            "name": f"Authority {index}",
            "lastProbedAt": "2026-07-01T09:00:00+00:00",
        }
        for index in range(MERGE_CONFLICT_LIMIT + 1)
    ]
    run = [
        {
            "id": f"row-{index}",
            "adapter": "static",
            "name": f"Run {index}",
            "lastProbedAt": "2026-07-01T10:00:00+00:00",
        }
        for index in range(MERGE_CONFLICT_LIMIT + 1)
    ]
    run[0]["name"] = "R" * 400
    audit: dict[str, int] = {}
    conflicts: list[dict[str, Any]] = []

    merged, restaged, updated = merge_run_pending_rows(
        current, run, audit=audit, conflicts=conflicts
    )

    assert len(merged) == MERGE_CONFLICT_LIMIT + 1
    assert (restaged, updated) == (0, MERGE_CONFLICT_LIMIT + 1)
    assert audit["fieldConflicts"] == MERGE_CONFLICT_LIMIT + 1
    assert len(conflicts) == MERGE_CONFLICT_LIMIT
    assert conflicts[0]["runValue"] == f"{'R' * 297}..."
    assert len(conflicts[0]["runValue"]) == 300
    assert conflicts[-1]["runValue"] == f"Run {MERGE_CONFLICT_LIMIT - 1}"


def test_placed_rows_record_fill_only_conflicts_without_mutation() -> None:
    active = {
        "id": "active-row",
        "adapter": "static",
        "name": "Active Authority",
        "careersUrl": "https://active.example/careers",
        "jobsFound": 2,
    }
    rejected = {
        "id": "rejected-row",
        "adapter": "static",
        "name": "Rejected Authority",
        "careersUrl": "https://rejected.example/careers",
        "jobsFound": 1,
    }
    run = [
        {
            **active,
            "name": "Active Run",
            "careersUrl": "https://active.example/migrated",
            "jobsFound": 9,
        },
        {
            **rejected,
            "name": "Rejected Run",
            "careersUrl": "https://rejected.example/migrated",
            "jobsFound": 8,
        },
    ]
    audit: dict[str, int] = {}
    active_conflicts: list[dict[str, Any]] = []

    merged, restaged, updated = merge_run_pending_rows(
        [],
        run,
        placed_ids=[source_identity(active), source_identity(rejected)],
        placed_rows={
            source_identity(active): active,
            source_identity(rejected): rejected,
        },
        audit=audit,
        active_conflicts=active_conflicts,
    )

    assert merged == []
    assert (restaged, updated) == (0, 0)
    assert active == {
        "id": "active-row",
        "adapter": "static",
        "name": "Active Authority",
        "careersUrl": "https://active.example/careers",
        "jobsFound": 2,
    }
    assert rejected == {
        "id": "rejected-row",
        "adapter": "static",
        "name": "Rejected Authority",
        "careersUrl": "https://rejected.example/careers",
        "jobsFound": 1,
    }
    assert audit == {"rowsDroppedPlaced": 2, "activeFieldConflicts": 4}
    assert active_conflicts == [
        {
            "identity": "active-row",
            "field": "name",
            "authorityValue": "Active Authority",
            "runValue": "Active Run",
        },
        {
            "identity": "active-row",
            "field": "careersUrl",
            "authorityValue": "https://active.example/careers",
            "runValue": "https://active.example/migrated",
        },
        {
            "identity": "rejected-row",
            "field": "name",
            "authorityValue": "Rejected Authority",
            "runValue": "Rejected Run",
        },
        {
            "identity": "rejected-row",
            "field": "careersUrl",
            "authorityValue": "https://rejected.example/careers",
            "runValue": "https://rejected.example/migrated",
        },
    ]


def test_placed_row_conflicts_are_bounded_and_values_bounded() -> None:
    placed_rows = [
        {
            "id": f"row-{index}",
            "adapter": "static",
            "name": f"Authority {index}",
        }
        for index in range(MERGE_CONFLICT_LIMIT + 1)
    ]
    run = [
        {
            **row,
            "name": "R" * 400 if index == 0 else f"Run {index}",
        }
        for index, row in enumerate(placed_rows)
    ]
    audit: dict[str, int] = {}
    active_conflicts: list[dict[str, Any]] = []

    merge_run_pending_rows(
        [],
        run,
        placed_ids=[source_identity(row) for row in placed_rows],
        placed_rows={source_identity(row): row for row in placed_rows},
        audit=audit,
        active_conflicts=active_conflicts,
    )

    assert audit == {
        "rowsDroppedPlaced": MERGE_CONFLICT_LIMIT + 1,
        "activeFieldConflicts": MERGE_CONFLICT_LIMIT + 1,
    }
    assert len(active_conflicts) == MERGE_CONFLICT_LIMIT
    assert active_conflicts[0]["runValue"] == f"{'R' * 297}..."
    assert len(active_conflicts[0]["runValue"]) == 300


def test_routing_conflict_ids_are_complete_beyond_the_conflict_sample_cap() -> None:
    current = [
        {
            "id": f"row-{index}",
            "adapter": "static",
            "name": f"Studio {index}",
            "careersUrl": f"https://example.com/{index}/careers",
            "lastProbedAt": "2026-07-01T09:00:00+00:00",
        }
        for index in range(MERGE_CONFLICT_LIMIT + 1)
    ]
    run = [
        {
            **row,
            "careersUrl": f"https://example.com/{index}/migrated",
            "lastProbedAt": "2026-07-01T10:00:00+00:00",
        }
        for index, row in enumerate(current)
    ]
    conflicts: list[dict[str, Any]] = []
    routing_conflict_ids: set[str] = set()

    merge_run_pending_rows(
        current,
        run,
        conflicts=conflicts,
        routing_conflict_ids=routing_conflict_ids,
    )

    assert len(conflicts) == MERGE_CONFLICT_LIMIT
    assert routing_conflict_ids == {f"row-{index}" for index in range(MERGE_CONFLICT_LIMIT + 1)}
