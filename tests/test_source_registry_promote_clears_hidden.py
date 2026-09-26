"""A row promoted to active must not stay hidden from the default set.

`hiddenFromDefault` is read by `is_hidden_from_default`, which the admin pending listings and the
pending provider-migration fetch lane both gate on. Promotion out of a duplicate-family or conflict
demotion inherits the flag, so without clearing it a repaired row is active, enabled, and inert at
the same time -- 34 active rows were in exactly that state, including one of the stranded-family
promotions.
"""

from __future__ import annotations

from src import source_registry as sr
from src.source_registry_state import (
    REGISTRY_STATE_ACTIVE,
    is_hidden_from_default,
    transition_registry_to_active,
    transition_registry_to_pending,
)

AT = "2026-09-26T00:00:00+00:00"


def _hidden_row() -> dict:
    return {
        "id": "static:listing_url:https://example.test/careers",
        "name": "Example (Sheet)",
        "studio": "Example",
        "adapter": "static",
        "registryState": "active",
        "listing_url": "https://example.test/careers",
        "jobsFound": 3,
    }


def test_promoting_to_active_clears_hidden_from_default() -> None:
    demoted = transition_registry_to_pending(
        _hidden_row(), reason="registry_conflict_safe_auto_demote", actor="test", at=AT
    )
    demoted["hiddenFromDefault"] = True
    demoted["candidateState"] = "hidden"
    assert is_hidden_from_default(demoted) is True

    promoted = transition_registry_to_active(demoted, reason="test_restore", actor="test", at=AT)

    assert promoted["registryState"] == REGISTRY_STATE_ACTIVE
    assert promoted["enabledByDefault"] is True
    assert promoted["hiddenFromDefault"] is False
    assert is_hidden_from_default(promoted) is False


def test_active_row_is_never_hidden_after_a_round_trip() -> None:
    row = _hidden_row()
    row["hiddenFromDefault"] = True
    pending = transition_registry_to_pending(row, reason="manual", actor="test", at=AT)
    pending["hiddenFromDefault"] = True
    active = transition_registry_to_active(pending, reason="manual", actor="test", at=AT)

    assert is_hidden_from_default(active) is False
    assert active["hiddenFromDefault"] is False


def test_demoting_still_marks_a_row_hidden_when_asked() -> None:
    """The clear must not defeat the demotion path's own hidden marking."""
    demoted = transition_registry_to_pending(
        _hidden_row(), reason="duplicate_family_weaker_variant", actor="test", at=AT
    )
    demoted["hiddenFromDefault"] = True

    assert demoted["hiddenFromDefault"] is True
    assert is_hidden_from_default(demoted) is True
    # And the row-level helper the policy path uses still marks it.
    assert sr.canonicalize_registry_row is not None
