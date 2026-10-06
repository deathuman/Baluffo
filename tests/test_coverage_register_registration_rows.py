"""`coverage_register.py --apply` must write registrations, not candidates.

The tool appended candidate rows straight into the registry file: fields like `decision`,
`missingCount` and `status: new_candidate`, and no `registryState`. Nothing about that shows
up in the file's row count -- the preflight reported 1,811 seed rows and 27 of them collected
nothing, because `_infer_registry_state` reads a missing state as `pending` and pending rows
are not watched.

Two things are pinned here. `build_registration_row` produces a row that survives the seed
guardrail, and `apply_rows` refuses the write rather than committing rows that do not.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools"))
if str(ROOT / "tools" / "repo_health") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools" / "repo_health"))

import coverage_register as reg  # noqa: E402

from tools.repo_health.source_registry_duplicate_url_policy import (  # noqa: E402
    list_seed_rows_not_registrations,
)


def _candidate(**over: Any) -> dict[str, Any]:
    """A `coverage_boards` candidate as it reaches `apply_rows`: evidence, no state."""
    row = {
        "_id": "greenhouse:slug:voodoo",
        "adapter": "greenhouse",
        "host": "job-boards.greenhouse.io",
        "tenant": "voodoo",
        "company": "Voodoo",
        "board_url": "https://job-boards.greenhouse.io/voodoo",
        "status": "new_candidate",
        "missingCount": 70,
        "sampleTitles": ["Technical Artist"],
        "companies": ["Voodoo"],
        "decision": "register",
        "openings": 70,
    }
    row.update(over)
    return row


# the defect ---------------------------------------------------------------


def test_a_raw_candidate_is_not_a_registration() -> None:
    """The starting point, so the test below is testing the fix and not the input.

    The candidate carries its id under `_id`, which the builder renames, so the row as it
    stands is unaddressable as well as stateless. That is the honest reading of it.
    """
    failures = list_seed_rows_not_registrations([_candidate()])
    assert failures, "a raw candidate must not pass the seed guardrail"
    assert any("no id" in f for f in failures)


def test_a_candidate_with_its_id_but_no_state_is_still_not_a_registration() -> None:
    """The state defect in isolation, which is the one that mattered: an id'd row with no
    ``registryState`` loads, counts, and collects nothing."""
    candidate = _candidate()
    candidate["id"] = candidate.pop("_id")
    failures = list_seed_rows_not_registrations([candidate])
    assert len(failures) == 1
    assert "registryState" in failures[0]
    assert "pending" in failures[0]


def test_a_built_row_survives_the_seed_guardrail() -> None:
    built = reg.build_registration_row(_candidate())
    assert list_seed_rows_not_registrations([built]) == []


def test_the_state_is_stamped_active_through_the_repo_transition() -> None:
    """Not assigned by hand: the same function the runtime uses sets state and provenance."""
    built = reg.build_registration_row(_candidate())
    assert built["registryState"] == "active"
    assert built["stateChangedBy"] == reg.COVERAGE_REGISTER_ACTOR
    assert built["lastPromotedAt"], "an active row records when it was promoted"


def test_the_actor_says_what_actually_happened() -> None:
    """These boards were verified by a real fetch, not approved by discovery. Claiming
    `discovery_auto_approve` here would put a false provenance on the row."""
    built = reg.build_registration_row(_candidate())
    assert "discovery_auto_approve" not in built["stateChangedBy"]


def test_candidate_bookkeeping_does_not_survive_into_the_row() -> None:
    """`decision: register` and `missingCount` are planning fields. Left in a registry row
    they read as evidence about a board the registry has never observed."""
    built = reg.build_registration_row(_candidate())
    for field in ("decision", "missingCount", "sampleTitles", "status", "companies", "_id"):
        assert field not in built


def test_evidence_the_board_did_carry_is_kept() -> None:
    """Stripping the planning fields must not strip the board's address or tenant."""
    built = reg.build_registration_row(_candidate())
    assert built["board_url"] == "https://job-boards.greenhouse.io/voodoo"
    assert built["adapter"] == "greenhouse"
    assert built["tenant"] == "voodoo"


def test_name_and_studio_come_from_the_company() -> None:
    built = reg.build_registration_row(_candidate())
    assert built["studio"] == "Voodoo"
    assert "Voodoo" in built["name"]


def test_an_existing_name_is_not_overwritten() -> None:
    built = reg.build_registration_row(_candidate(name="Voodoo (Greenhouse)"))
    assert built["name"] == "Voodoo (Greenhouse)"


def test_a_provider_row_addressed_only_by_slug_is_still_a_registration() -> None:
    """`greenhouse:slug:bandainamco` has no URL field and is a real collecting row, so the
    guardrail accepts the slug. Building must not break that by dropping it."""
    candidate = _candidate(
        _id="greenhouse:slug:bandainamco",
        board_url=None,
        slug="bandainamco",
        host="job-boards.greenhouse.io",
    )
    candidate.pop("board_url")
    built = reg.build_registration_row(candidate)
    assert built["slug"] == "bandainamco"
    assert list_seed_rows_not_registrations([built]) == []


# apply_rows refuses what it cannot make valid ---------------------------


def test_apply_rows_refuses_rows_that_are_not_registrations(tmp_path: Path) -> None:
    """The write is gated, not assumed. A row with no adapter and no address cannot be fixed
    by stamping state, so it must abort rather than land inert."""
    registry = tmp_path / "seed.json"
    registry.write_text(json.dumps([{"id": "existing:1", "registryState": "active"}]), "utf-8")

    # No adapter and no address: stamping a state cannot repair this one.
    bad = {"_id": "greenhouse:slug:broken", "status": "new_candidate"}
    with pytest.raises(SystemExit) as excinfo:
        reg.apply_rows(registry, [bad], tmp_path / "backup")
    assert "not registrations" in str(excinfo.value)
    assert json.loads(registry.read_text("utf-8")) == [
        {"id": "existing:1", "registryState": "active"}
    ]


def test_apply_rows_leaves_the_registry_untouched_when_it_aborts(tmp_path: Path) -> None:
    """A refused write must not leave a partial registry: the missing rows look absent rather
    than unfinished, which is the worse state."""
    registry = tmp_path / "seed.json"
    original = [{"id": "existing:1", "registryState": "active"}]
    registry.write_text(json.dumps(original), "utf-8")

    with pytest.raises(SystemExit):
        reg.apply_rows(registry, [{"_id": "greenhouse:slug:x"}], tmp_path / "backup")
    assert json.loads(registry.read_text("utf-8")) == original


def test_apply_rows_writes_valid_rows_and_reads_them_back(tmp_path: Path) -> None:
    registry = tmp_path / "seed.json"
    registry.write_text(json.dumps([{"id": "existing:1", "registryState": "active"}]), "utf-8")

    added = reg.apply_rows(registry, [_candidate()], tmp_path / "backup")

    assert added == 1
    readback = json.loads(registry.read_text("utf-8"))
    assert len(readback) == 2
    written = next(r for r in readback if r["id"] == "greenhouse:slug:voodoo")
    assert list_seed_rows_not_registrations([written]) == []
    assert written["registryState"] == "active"
    assert (tmp_path / "backup" / "seed.json").exists(), "a backup is taken before writing"


def test_apply_rows_skips_a_board_the_registry_already_holds(tmp_path: Path) -> None:
    registry = tmp_path / "seed.json"
    registry.write_text(
        json.dumps([{"id": "greenhouse:slug:voodoo", "registryState": "active"}]), "utf-8"
    )
    assert reg.apply_rows(registry, [_candidate()], tmp_path / "backup") == 0
    assert len(json.loads(registry.read_text("utf-8"))) == 1
