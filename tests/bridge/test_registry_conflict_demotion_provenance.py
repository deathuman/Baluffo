"""Conflict demotion must record provenance without stamping a gate.

The safe conflict demote moved rows active -> pending while recording nothing about what they lost
to, so a later audit could not tell whether the family kept a winner. That is the gap that let 37
studios sit with every row parked and no active registration.

The obvious repair -- stamping `duplicateOfSourceId`, the field the duplicate-URL policy uses --
is not a neutral rename:

* `registry_service.py` / `registry_sync_summary.py` / `source_registry_io_load.py` all read it to
  publish `duplicatePendingCount`, so stamping it would fold every conflict-demoted row into a
  documented operator KPI that currently counts real duplicates only.
* `scripts/source_policy_soak_report_links.py` subtracts 25 `evidenceScore` and appends a
  `duplicate_static_row` blocker for a row carrying it.
* `transition_registry_to_active` does not clear the field, so a stamped row that was later
  re-promoted would become an active row that every one of those readers now treats as a duplicate.

`hiddenFromDefault` is worse: it is a live gate, so a stamped row would drop out of the admin
pending listings and the pending provider-migration fetch lane outright.

So the provenance lands on `conflictFamilyKey` and `supersededBySourceId`, which nothing reads.
These tests pin that split, plus the one place the winner does belong: the `applied` record in the
run response.
"""

from __future__ import annotations

from typing import Any

from src.bridge.registry_conflicts_demotions import (
    _restore_families_left_without_active_rows,
    _safe_demotion_applied_entry,
    _stamp_conflict_provenance,
)

LOSER_ID = "static:listing_url:https://loser.test/careers"
WINNER_ID = "static:listing_url:https://winner.test/careers"


def _card(winner_id: str = WINNER_ID, family_key: str = "static studio") -> dict[str, Any]:
    return {
        "familyKey": family_key,
        "winner": {"id": winner_id},
        "safeAutomation": {"eligible": True, "action": "auto_demote_static_normalized_url_alias"},
    }


def _row(row_id: str = LOSER_ID) -> dict[str, Any]:
    return {"id": row_id, "studio": "Example", "adapter": "static", "jobsFound": 2}


def test_stamping_records_family_and_winner() -> None:
    stamped = _stamp_conflict_provenance(_row(), _card())

    assert stamped["conflictFamilyKey"] == "static studio"
    assert stamped["supersededBySourceId"] == WINNER_ID


def test_stamping_never_sets_a_gate_field() -> None:
    """`duplicateOfSourceId` and `hiddenFromDefault` are read as gates, not as history."""
    stamped = _stamp_conflict_provenance(_row(), _card())

    assert "duplicateOfSourceId" not in stamped
    assert "hiddenFromDefault" not in stamped
    assert "duplicateFamilyKey" not in stamped
    assert "duplicateOfSourceName" not in stamped


def test_stamping_does_not_mutate_the_input_row() -> None:
    original = _row()
    _stamp_conflict_provenance(original, _card())

    assert original == _row()


def test_applying_a_demotion_end_to_end_leaves_the_stored_row_free_of_gates() -> None:
    """The full transition, not just the helper: a demoted row must stay visible and fetchable."""
    from src.bridge.registry_conflicts_demotions import _apply_safe_demotion_targets
    from src.source_registry_state import is_hidden_from_default

    loser = _row()
    demoted_moved, applied, moved = _apply_safe_demotion_targets(
        {"active": [loser], "pending": [], "rejected": []},
        target_ids={LOSER_ID},
        eligible_by_id={LOSER_ID: _card()},
        now="2026-09-26T00:00:00+00:00",
        actor="test",
    )

    assert demoted_moved == []
    assert [r["id"] for r in moved] == [LOSER_ID]
    assert applied == [
        {
            "id": LOSER_ID,
            "familyKey": "static studio",
            "action": "auto_demote_static_normalized_url_alias",
            "winnerId": WINNER_ID,
        }
    ]
    stored = moved[0]
    assert stored["registryState"] == "pending"
    assert stored["pendingReason"] == "registry_conflict_safe_auto_demote"
    assert stored["conflictFamilyKey"] == "static studio"
    assert stored["supersededBySourceId"] == WINNER_ID
    # The two fields that would actually change behaviour stay absent, so the row is still counted
    # as a conflict demotion and is still visible to admin review and the pending fetch lane.
    assert "duplicateOfSourceId" not in stored
    assert "hiddenFromDefault" not in stored
    assert is_hidden_from_default(stored) is False


def test_a_row_that_is_its_own_winner_is_not_marked_as_superseded() -> None:
    """The restore path re-promotes the family's own loser, which must not look like a loser."""
    stamped = _stamp_conflict_provenance(_row(), _card(winner_id=LOSER_ID))

    assert "supersededBySourceId" not in stamped
    assert stamped["conflictFamilyKey"] == "static studio"


def test_a_stale_superseded_pointer_is_cleared_when_the_winner_is_the_row_itself() -> None:
    row = _row()
    row["supersededBySourceId"] = "static:listing_url:https://stale.test/careers"

    stamped = _stamp_conflict_provenance(row, _card(winner_id=LOSER_ID))

    assert "supersededBySourceId" not in stamped


def test_a_card_without_a_winner_still_records_the_family() -> None:
    stamped = _stamp_conflict_provenance(_row(), {"familyKey": "static studio"})

    assert stamped["conflictFamilyKey"] == "static studio"
    assert "supersededBySourceId" not in stamped


def test_applied_entry_names_the_winner() -> None:
    entry = _safe_demotion_applied_entry(LOSER_ID, _card())

    assert entry == {
        "id": LOSER_ID,
        "familyKey": "static studio",
        "action": "auto_demote_static_normalized_url_alias",
        "winnerId": WINNER_ID,
    }


def test_applied_entry_tolerates_a_card_with_no_winner() -> None:
    entry = _safe_demotion_applied_entry(LOSER_ID, {"familyKey": "static studio"})

    assert entry["winnerId"] == ""
    assert entry["id"] == LOSER_ID


def test_applied_entry_falls_back_to_the_winner_row_fields() -> None:
    card = _card()
    card["winner"] = {"sourceId": WINNER_ID}

    assert _safe_demotion_applied_entry(LOSER_ID, card)["winnerId"] == WINNER_ID


def test_restoring_a_family_clears_the_superseded_pointer() -> None:
    """A restored row is the family's survivor, so a stale pointer would misreport it as a loser."""
    loser = _row()
    loser["conflictFamilyKey"] = "static studio"
    loser["supersededBySourceId"] = "static:listing_url:https://someone-else.test/careers"

    next_active, restored = _restore_families_left_without_active_rows(
        {"active": [], "pending": [loser]},
        eligible_by_id={LOSER_ID: {**_card(), "rows": [loser]}},
        moved_ids={LOSER_ID},
        now="2026-09-26T00:00:00+00:00",
        actor="test",
    )

    assert len(restored) == 1
    assert restored[0]["id"] == LOSER_ID
    promoted = next_active[0]
    assert promoted["registryState"] == "active"
    assert "supersededBySourceId" not in promoted
    # The family key is history and stays; the pointer is a claim about the present and goes.
    assert promoted["conflictFamilyKey"] == "static studio"
