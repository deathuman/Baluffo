"""Budget-truncated partial runs must not mark their unfetched tail missing."""

from typing import Any

from src.jobs.state_lifecycle import build_lifecycle_source_evidence


def _budget_truncated_report(kept: int) -> dict[str, Any]:
    return {
        "name": "static_source::static:listing_url:https://example.com/careers",
        "status": "ok",
        "keptCount": kept,
        "error": (
            "static_source::static:listing_url:https://example.com/careers: "
            "static:Example (Sheet):https://example.com/jobs/page/7: "
            "time budget exceeded (25s)"
        ),
    }


def test_budget_truncated_partial_run_is_not_missing_eligible() -> None:
    evidence = build_lifecycle_source_evidence(
        [_budget_truncated_report(kept=117)],
        allow_missing=True,
    )

    assert evidence["eligibleMissingSources"] == set()
    assert evidence["skippedMissingSources"] == {
        "static_source::static:listing_url:https://example.com/careers"
    }


def test_clean_partial_run_stays_missing_eligible() -> None:
    evidence = build_lifecycle_source_evidence(
        [
            {
                "name": "static_source::static:listing_url:https://example.com/careers",
                "status": "ok",
                "keptCount": 9,
            }
        ],
        allow_missing=True,
    )

    assert evidence["eligibleMissingSources"] == {
        "static_source::static:listing_url:https://example.com/careers"
    }
    assert evidence["skippedMissingSources"] == set()


def test_budget_truncated_zero_kept_run_stays_skipped() -> None:
    evidence = build_lifecycle_source_evidence(
        [_budget_truncated_report(kept=0)],
        allow_missing=True,
    )

    assert evidence["eligibleMissingSources"] == set()
    assert evidence["skippedMissingSources"] == {
        "static_source::static:listing_url:https://example.com/careers"
    }


def test_snake_case_budget_marker_also_shields() -> None:
    row = _budget_truncated_report(kept=4)
    row["error"] = "static:Example (Sheet): time_budget_exceeded"
    evidence = build_lifecycle_source_evidence([row], allow_missing=True)

    assert evidence["eligibleMissingSources"] == set()
    assert evidence["skippedMissingSources"] == {
        "static_source::static:listing_url:https://example.com/careers"
    }
