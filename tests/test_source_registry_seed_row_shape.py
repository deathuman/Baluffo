"""A committed seed row must be a registration, not a candidate.

`coverage_register.py --apply` wrote candidate rows into
`data/defaults/source-registry-active.seed.json`: fields like `decision`,
`missingCount` and `status: new_candidate`, with no `registryState`. Nothing about that is
visible in the file's row count. The rows load, canonicalise to `pending`, and a pending row
is not watched -- so twenty-seven of them read as twenty-seven registered boards in the
preflight while collecting nothing.

That is the false green this project keeps having to undo, and it arrived through the tool
meant to implement the fix. The seed had no shape check: `lint:repo-guardrails` returned 0
with the broken rows in place.

The check itself lives in `source_registry_duplicate_url_policy` beside the other active-seed
checks. This file pins its behaviour; the sibling test file covers the URL rules.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from tools.repo_health import source_registry_duplicate_url_policy as policy

ROOT = Path(__file__).resolve().parents[1]


def _registration(source_id: str, **over: Any) -> dict:
    """A minimal row that IS a registration: state, adapter, and an address."""
    row = {
        "id": source_id,
        "adapter": "greenhouse",
        "board_url": "https://boards.example/x",
        "registryState": "active",
    }
    row.update(over)
    return row


# the committed seed -------------------------------------------------------


def test_the_committed_seed_is_all_registrations() -> None:
    """The whole point: green means every committed row would actually be fetched.

    This also pins the 1,784 rows that are already there, so the check cannot be satisfied
    by loosening itself later.
    """
    assert policy.check_active_seed_rows_are_registrations(ROOT) == []


# the defect this exists for ------------------------------------------------


def test_a_row_without_registry_state_is_not_a_registration() -> None:
    """`_infer_registry_state` reads a missing state as `pending`, and pending is not watched."""
    row = {
        "id": "ashby:board_url:https://jobs.ashbyhq.com/voodoo",
        "adapter": "ashby",
        "board_url": "https://jobs.ashbyhq.com/voodoo",
        "status": "new_candidate",
        "missingCount": 70,
        "decision": "register",
        "openings": 70,
    }
    failures = policy.list_seed_rows_not_registrations([row])
    assert len(failures) == 1
    assert "registryState" in failures[0]
    assert "pending" in failures[0]


def test_the_candidate_shape_hint_names_the_tool_that_wrote_it() -> None:
    """The message has to say where the row came from, or the next person re-runs the tool."""
    row = {"id": "g:1", "adapter": "greenhouse", "board_url": "u", "decision": "register"}
    assert any("coverage_register" in f for f in policy.list_seed_rows_not_registrations([row]))


def test_a_candidate_row_without_its_state_is_one_failure_not_three() -> None:
    """It has an adapter and a URL, so only the state is wrong. Counting it three ways
    would make a real registry problem harder to read in the failure output."""
    row = {"id": "g:1", "adapter": "ashby", "board_url": "u", "status": "new_candidate"}
    assert len(policy.list_seed_rows_not_registrations([row])) == 1


# state values --------------------------------------------------------------


def test_an_unrecognised_state_is_flagged() -> None:
    failures = policy.list_seed_rows_not_registrations(
        [_registration("g:1", registryState="promoted")]
    )
    assert len(failures) == 1
    assert "not one of" in failures[0]


@pytest.mark.parametrize("state", ["active", "pending", "rejected"])
def test_every_known_state_is_accepted(state: str) -> None:
    """`pending` and `rejected` are legitimate committed states, not defects. Only an
    *absent* state is the problem, because only then does inference decide for us."""
    assert (
        policy.list_seed_rows_not_registrations([_registration("g:1", registryState=state)]) == []
    )


# the other two required fields --------------------------------------------


def test_a_row_with_no_id_is_flagged() -> None:
    row = {"adapter": "greenhouse", "board_url": "u", "registryState": "active"}
    failures = policy.list_seed_rows_not_registrations([row])
    assert len(failures) == 1
    assert "no id" in failures[0]


def test_a_row_with_no_adapter_is_flagged() -> None:
    row = {"id": "g:1", "board_url": "u", "registryState": "active"}
    failures = policy.list_seed_rows_not_registrations([row])
    assert len(failures) == 1
    assert "adapter" in failures[0]


def test_a_row_missing_state_and_address_reports_both() -> None:
    """One pass reports every defect in the row, so a repair is one read not two."""
    assert len(policy.list_seed_rows_not_registrations([{"id": "g:1", "adapter": "x"}])) == 2


# addressing ----------------------------------------------------------------


@pytest.mark.parametrize(
    "field", ["board_url", "listing_url", "careersUrl", "api_url", "url", "base_url"]
)
def test_any_url_field_counts_as_an_address(field: str) -> None:
    row = {"id": "g:1", "adapter": "static", "registryState": "active", field: "https://x.example"}
    assert policy.list_seed_rows_not_registrations([row]) == []


@pytest.mark.parametrize("field", ["slug", "account", "tenant", "base_url", "board_id"])
def test_a_provider_address_field_counts_as_an_address(field: str) -> None:
    """`greenhouse:slug:bandainamco` and `lever:account:grand` are real, active, collecting
    rows carrying no URL field at all. Demanding a URL would flag working registrations, so
    the check accepts the tenant field the adapter builds its endpoint from."""
    row = {"id": "p:1", "adapter": "greenhouse", "registryState": "active", field: "tenant-x"}
    assert policy.list_seed_rows_not_registrations([row]) == []


def test_a_row_with_no_address_at_all_is_flagged() -> None:
    row = {"id": "g:1", "adapter": "greenhouse", "registryState": "active"}
    failures = policy.list_seed_rows_not_registrations([row])
    assert len(failures) == 1
    assert "no fetchable address" in failures[0]


def test_an_empty_address_field_does_not_satisfy_the_check() -> None:
    """Present-but-blank is the same defect as absent: the fetch still has nowhere to go."""
    row = {"id": "g:1", "adapter": "static", "registryState": "active", "board_url": "   "}
    assert len(policy.list_seed_rows_not_registrations([row])) == 1


# loader-error discrimination ----------------------------------------------


def test_a_load_error_is_not_mistaken_for_rows() -> None:
    """`_load_active_seed` returns `list[dict]` on success and `list[str]` of messages on
    error. Keying on `isinstance(loaded, list)` conflates them and returns the 1,784 rows
    themselves as 1,784 failures -- which is how this check first reported a clean seed as
    wholly broken."""
    loaded: list[object] = ["active seed not found: /nope"]
    assert loaded and isinstance(loaded[0], str)


def test_non_object_rows_are_reported_without_dumping_them() -> None:
    """A type, not a row-shaped message that would print a whole registry entry."""
    assert policy.list_seed_rows_not_registrations(cast(Any, ["a string"])) == [
        "seed row is not an object: str"
    ]


def test_an_empty_seed_passes() -> None:
    assert policy.list_seed_rows_not_registrations([]) == []
