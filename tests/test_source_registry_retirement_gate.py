"""The retirement gate: no evidence, no retirement.

The plan's standing rule is not to retire a board without saying so, and the costliest
direction for the decision to err in is retiring a live board -- the Bungie rebrand is
what that looked like when it was guessed. So the transition itself refuses without the
terminal classification, the terminal board-root probe and the passing control, and these
tests pin each refusal.
"""

from __future__ import annotations

import pytest

from src.source_discovery.redirect_classification import SITE_GONE
from src.source_registry_state import (
    REGISTRY_REASON_EVIDENCE_GATED_RETIRE,
    REGISTRY_STATE_REJECTED,
    RETIRE_TERMINAL_CLASSIFICATION,
    transition_registry_to_retired,
)

_EVIDENCE = {"terminal": True, "control_ok": True}


def _row() -> dict[str, object]:
    return {
        "id": "static:listing_url:https://dead.example/jobs",
        "adapter": "static",
        "registryState": "active",
    }


def test_the_terminal_classification_is_the_redirect_modules_own() -> None:
    """The literal is duplicated to avoid an import cycle; this pins the two together."""
    assert RETIRE_TERMINAL_CLASSIFICATION == SITE_GONE


def test_a_full_evidence_set_retires_the_row() -> None:
    retired = transition_registry_to_retired(
        _row(), classification=SITE_GONE, probe=_EVIDENCE, actor="test-actor"
    )
    assert retired["registryState"] == REGISTRY_STATE_REJECTED
    assert retired["stateChangedBy"] == "test-actor"
    assert retired["quarantineReason"] == REGISTRY_REASON_EVIDENCE_GATED_RETIRE


@pytest.mark.parametrize(
    "classification",
    ["site_rebranded", "platform_migration", "insecure_downgrade", "unparseable_redirect", ""],
)
def test_a_non_terminal_classification_refuses(classification: str) -> None:
    with pytest.raises(ValueError, match="not"):
        transition_registry_to_retired(
            _row(), classification=classification, probe=_EVIDENCE, actor="test-actor"
        )


def test_a_live_board_root_refuses() -> None:
    with pytest.raises(ValueError, match="terminal"):
        transition_registry_to_retired(
            _row(),
            classification=SITE_GONE,
            probe={"terminal": False, "control_ok": True},
            actor="test-actor",
        )


def test_a_failed_control_refuses() -> None:
    """A probe whose control failed proves nothing about the candidate."""
    with pytest.raises(ValueError, match="control"):
        transition_registry_to_retired(
            _row(),
            classification=SITE_GONE,
            probe={"terminal": True, "control_ok": False},
            actor="test-actor",
        )
