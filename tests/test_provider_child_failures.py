"""Per-board failures inside multi-board provider families.

A family such as ``lever_sources`` fetches many boards behind one report row, and
its per-board results live in ``details``. ``derive_source_health`` walked only
top-level rows, so a deleted board stayed invisible: the family reported
``status: ok`` and ``health: healthy`` because its other boards still returned
jobs, and the dead board appeared only inside a prose ``error`` string. Twelve
boards were in that state, including ``Voodoo (Lever)`` after Voodoo moved to
Ashby.

These tests pin that children are surfaced under their own additive key, and that
the existing family counts Admin already presents do not move.
"""

from typing import Any

from src.jobs.common.contracts_source_health import (
    _source_health_row,
    derive_source_health,
    normalize_source_health_payload,
)

DeadBoard = {
    "name": "Voodoo (Lever)",
    "status": "error",
    "adapter": "lever",
    "studio": "Voodoo",
    "fetchedCount": 0,
    "keptCount": 0,
    "lowConfidenceDropped": 0,
    "error": "HTTP 404 for https://api.lever.co/v0/postings/voodoo?mode=json",
}
LiveBoard = {
    "name": "Larian Studios (Lever)",
    "status": "ok",
    "adapter": "lever",
    "studio": "Larian Studios",
    "fetchedCount": 75,
    "keptCount": 75,
    "lowConfidenceDropped": 0,
    "error": "",
}


def _family_rows(**overrides: Any) -> list[dict[str, Any]]:
    family = {
        "name": "lever_sources",
        "status": "ok",
        "adapter": "lever",
        "studio": "multiple",
        "fetchedCount": 75,
        "keptCount": 75,
        "error": f"lever_sources: {DeadBoard['name']}: {DeadBoard['error']}",
        "details": [DeadBoard, LiveBoard],
    }
    family.update(overrides)
    return [family]


def test_dead_child_board_is_surfaced_with_its_family() -> None:
    health = derive_source_health(_family_rows())

    assert health["providerChildFailureCount"] == 1
    failures = health["providerChildFailures"]
    assert len(failures) == 1

    row = failures[0]
    assert row["name"] == "Voodoo (Lever)"
    assert row["family"] == "lever_sources"
    assert row["status"] == "error"
    assert row["health"] == "broken"
    assert row["healthReason"] == "latest fetch failed"
    assert "HTTP 404" in row["error"]


def test_healthy_child_board_is_not_listed_as_a_failure() -> None:
    health = derive_source_health(_family_rows())
    assert "Larian Studios (Lever)" not in {row["name"] for row in health["providerChildFailures"]}


def test_family_counts_are_unchanged_by_child_surfacing() -> None:
    """The additive key must not move counts Admin already presents."""
    rows = _family_rows()
    health = derive_source_health(rows)

    assert health["totalSources"] == 1
    assert health["okSources"] == 1
    assert health["failedSources"] == 0
    assert health["sourcesNeedingAttention"] == []
    # The family still reads healthy, exactly as it did before this change.
    assert health["providerChildFailureCount"] == 1


def test_static_source_details_are_not_mistaken_for_board_lists() -> None:
    """A ``static_source`` row's ``details`` is one payload, not a board list.

    Without the adapter filter this restates the parent's own failure as a child
    and inflates the count: the live report has 287 such rows.
    """
    rows = [
        {
            "name": "static_source::static:listing_url:https://2kmadrid.com/careers/",
            "status": "error",
            "adapter": "static",
            "studio": "2K Madrid",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "no jobs extracted from source pages",
            "details": [
                {
                    "name": "2K Madrid (GameDevMap)",
                    "status": "error",
                    "adapter": "static",
                    "error": "no jobs extracted from source pages",
                }
            ],
        }
    ]
    health = derive_source_health(rows)

    assert health["providerChildFailureCount"] == 0
    assert health["providerChildFailures"] == []
    # The parent failure is still reported the normal way.
    assert health["failedSources"] == 1


def test_absent_details_yield_no_children() -> None:
    rows = [
        {
            "name": "lever_sources",
            "status": "ok",
            "adapter": "lever",
            "fetchedCount": 320,
            "keptCount": 296,
        }
    ]
    health = derive_source_health(rows)
    assert health["providerChildFailureCount"] == 0
    assert health["providerChildFailures"] == []


def test_child_failure_buckets_are_counted() -> None:
    """A child carrying a failureBucket reaches topFailureBuckets."""
    rows = _family_rows(
        details=[
            {**DeadBoard, "failureBucket": "site_changed"},
            {**LiveBoard, "failureBucket": "none"},
        ]
    )
    health = derive_source_health(rows)

    buckets = {row["key"]: row["count"] for row in health["topFailureBuckets"]}
    assert buckets["site_changed"] == 1


def test_normalization_preserves_the_child_failure_keys() -> None:
    """A stored payload must not erase or fabricate the new keys."""
    rows = _family_rows()
    derived = derive_source_health(rows)

    carried = normalize_source_health_payload(
        {"providerChildFailureCount": derived["providerChildFailureCount"]}, rows
    )
    assert carried["providerChildFailureCount"] == derived["providerChildFailureCount"]
    assert [row["name"] for row in carried["providerChildFailures"]] == ["Voodoo (Lever)"]


def test_child_rows_carry_only_the_health_contract_plus_family() -> None:
    """Child rows are contract rows; no raw payload leaks through."""
    rows = _family_rows()
    failures = derive_source_health(rows)["providerChildFailures"]

    row = failures[0]
    # lowConfidenceDropped is not a health-contract field and must not appear.
    assert "lowConfidenceDropped" not in row
    assert "studio" not in row
    assert row["family"] == "lever_sources"
    assert set(row) == set(_source_health_row(DeadBoard)) | {"family"}


def test_child_failure_rows_are_capped_by_the_triage_limit() -> None:
    details = [{**DeadBoard, "name": f"Dead Board {index} (Lever)"} for index in range(25)]
    health = derive_source_health(_family_rows(details=details))

    # The count is the true total; the list is bounded like every other triage list.
    assert health["providerChildFailureCount"] == 25
    assert len(health["providerChildFailures"]) == 10
