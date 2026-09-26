"""The safe-demote post-condition: a family must never be left without an active row.

The demotion helpers move rows without checking the result, so a target set that covers every
active row of a board silently strips that board of coverage. These cover the guard directly; the
real-world trigger is cross-family target overlap, where a row wins one family and loses another.
"""

from __future__ import annotations

from typing import Any

from src.bridge.registry_conflicts_demotions import (
    _restore_families_left_without_active_rows,
)
from src.source_registry_identity import source_identity

FAMILY = "stranded studio"
ROW_A = "static:listing_url:https://stranded.example/careers?l=thai"
ROW_B = "static:listing_url:https://stranded.example/jobs?l=thai"
ROW_C = "static:listing_url:https://stranded.example/join-us?l=thai"


def _row(source_id: str, *, rank: int, jobs: int) -> dict[str, Any]:
    url = source_id.removeprefix("static:listing_url:")
    return {
        "id": source_id,
        "name": "Stranded Studio (GameDevMap)",
        "studio": "Stranded Studio",
        "adapter": "static",
        "registryState": "pending",
        "candidateState": "validated",
        "listing_url": url,
        "careersUrl": url,
        "rankScore": rank,
        "jobsFound": jobs,
    }


def _card(*rows: dict[str, Any]) -> dict[str, Any]:
    return {"familyKey": FAMILY, "rows": [dict(row) for row in rows]}


def _state(active: list[dict[str, Any]], pending: list[dict[str, Any]]) -> dict[str, Any]:
    return {"active": active, "pending": pending, "rejected": []}


def test_restores_one_row_when_a_family_would_be_left_without_an_active_row() -> None:
    rows = [
        _row(ROW_A, rank=10, jobs=0),
        _row(ROW_B, rank=42, jobs=3),
        _row(ROW_C, rank=7, jobs=1),
    ]
    card = _card(*rows)
    eligible = {source_identity(row): card for row in rows}
    state = _state([], [dict(row) for row in rows])

    next_active, restored = _restore_families_left_without_active_rows(
        state,
        eligible_by_id=eligible,
        moved_ids=set(eligible),
        now="2026-09-26T00:00:00+00:00",
        actor="registry_conflict_safe_auto_demote",
    )

    # Best registration wins the restore: highest rankScore, then jobsFound, then id.
    assert len(restored) == 1
    assert restored[0] == {
        "id": ROW_B,
        "familyKey": FAMILY,
        "reason": "restored_family_left_without_active_row",
    }
    assert [source_identity(row) for row in next_active] == [ROW_B]
    restored_row = next_active[0]
    assert restored_row["registryState"] == "active"
    assert restored_row["pendingReason"] == ""
    assert restored_row["enabledByDefault"] is True
    assert restored_row["candidateState"] == "live"
    assert restored_row["stateChangedBy"] == "registry_conflict_safe_auto_demote"


def test_leaves_a_family_alone_when_one_row_is_still_active() -> None:
    rows = [_row(ROW_A, rank=10, jobs=0), _row(ROW_B, rank=42, jobs=3)]
    card = _card(*rows)
    eligible = {ROW_A: card}
    state = _state([dict(rows[1])], [dict(rows[0])])

    next_active, restored = _restore_families_left_without_active_rows(
        state,
        eligible_by_id=eligible,
        moved_ids={ROW_A},
        now="2026-09-26T00:00:00+00:00",
        actor="registry_conflict_safe_auto_demote",
    )

    assert restored == []
    assert [source_identity(row) for row in next_active] == [ROW_B]


def test_ignores_families_this_call_did_not_demote_from() -> None:
    """A board an operator parked earlier must not be resurrected as a side effect."""
    rows = [_row(ROW_A, rank=10, jobs=0)]
    card = _card(*rows)
    state = _state([], [dict(row) for row in rows])

    next_active, restored = _restore_families_left_without_active_rows(
        state,
        eligible_by_id={ROW_A: card},
        moved_ids=set(),
        now="2026-09-26T00:00:00+00:00",
        actor="registry_conflict_safe_auto_demote",
    )

    assert restored == []
    assert next_active == []


def test_skips_a_family_whose_rows_are_all_rejected() -> None:
    rows = [_row(ROW_A, rank=10, jobs=0), _row(ROW_B, rank=42, jobs=3)]
    card = _card(*rows)
    eligible = {source_identity(row): card for row in rows}
    state = _state([], [])

    next_active, restored = _restore_families_left_without_active_rows(
        state,
        eligible_by_id=eligible,
        moved_ids=set(eligible),
        now="2026-09-26T00:00:00+00:00",
        actor="registry_conflict_safe_auto_demote",
    )

    assert restored == []
    assert next_active == []
