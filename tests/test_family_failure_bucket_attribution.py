"""Regression guard for family-level failureBucket attribution.

Defect (reported and confirmed): a provider family that SUCCEEDED and kept jobs
inherited its failure bucket from a single dead board.

Chain:
  greenhouse_runner collects per-board errors; one dead board (HTTP 404) puts
  that board's error text on the family row.
  "404" is a site_changed signal (taxonomy.py:156).
  _classify_report_outcome (pipeline_source_results.py) assigned the bucket
  whenever `report.get("error")` was truthy, regardless of status or keptCount.
  _family_summary then copied it to the family and the benchmark escalated to
  action=source_policy_review / priority=100 / hide_source_after_review.

Observed on greenhouse_boards: status=ok, keptCount=1126, 61/61 boards
refreshed, yet the source was recommended for hiding.

These tests pin both halves: the false positive is suppressed, and genuine
failures still get a bucket.
"""

from __future__ import annotations

from typing import Any

from src.jobs.pipeline_source_results import _classify_report_outcome


class _StubRoot:
    """Minimal stand-in for the root module's zero-extract resolver."""

    @staticmethod
    def _failure_bucket_from_zero_extract_context(ctx: Any, classification: str) -> Any:
        from src.jobs.common.taxonomy import FailureBucket

        return FailureBucket.UNKNOWN


def _classify(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "name": "greenhouse_boards",
        "status": "ok",
        "keptCount": 0,
        "adapter": "greenhouse",
    }
    row.update(overrides)
    _classify_report_outcome(report=row, root_module=_StubRoot())
    return row


def test_successful_family_with_partial_board_error_keeps_no_failure_bucket() -> None:
    """The reported defect: 1,126 jobs kept, one board 404, family must not be bucketed."""
    row = _classify(
        status="ok",
        keptCount=1126,
        error=(
            "greenhouse_boards: greenhouse:arenanet: HTTP 404 for "
            "https://boards-api.greenhouse.io/v1/boards/arenanet/jobs"
        ),
        providerUrl="https://boards-api.greenhouse.io/v1/boards/arenanet/jobs",
    )

    assert not row.get("failureBucket"), (
        "a family that kept 1,126 jobs from 61 refreshed boards must not inherit "
        "site_changed from one dead board; that escalates to priority=100 "
        "hide_source_after_review in the benchmark"
    )


def test_partial_board_error_on_successful_family_preserves_evidence() -> None:
    """Suppressing the bucket must not discard the partial-failure evidence."""
    row = _classify(
        status="ok",
        keptCount=1126,
        error="greenhouse_boards: greenhouse:arenanet: HTTP 404",
        providerUrl="https://boards-api.greenhouse.io/v1/boards/arenanet/jobs",
    )

    assert row.get("error"), "the per-board error text must survive"
    assert row.get("providerUrl"), "the failing board URL must survive"


def test_error_status_still_gets_a_failure_bucket() -> None:
    row = _classify(
        status="error",
        keptCount=0,
        error="HTTP 404 for https://example.com/jobs",
    )

    assert row.get("failureBucket"), "a genuinely errored source must stay bucketed"


def test_zero_kept_source_still_gets_a_failure_bucket() -> None:
    row = _classify(
        status="ok",
        keptCount=0,
        error="HTTP 404 for https://example.com/jobs",
    )

    assert row.get("failureBucket"), "a zero-extract source must stay bucketed"


def test_zero_kept_classification_still_assigned_on_success() -> None:
    row = _classify(status="ok", keptCount=0)

    assert row.get("zeroKeptClassification"), (
        "zero-kept classification is independent of the failure bucket"
    )


def test_healthy_family_has_no_failure_bucket() -> None:
    row = _classify(status="ok", keptCount=291)

    assert not row.get("failureBucket")


def test_excluded_source_gets_no_zero_kept_classification() -> None:
    """An excluded source is never treated as a zero-extract failure.

    Note: ``failureBucket`` still becomes ``"unknown"`` here, because the
    ``keptCount == 0`` disjunct on :715 fires independently of status. That is
    pre-existing behaviour (verified identical with and without the family
    attribution fix) and is harmless -- ``unknown`` is not an escalation
    signal. Only the zero-kept classification is status-gated.
    """
    row = _classify(status="excluded", keptCount=0)

    assert not row.get("zeroKeptClassification"), (
        "zero-kept classification is gated on status != 'excluded'"
    )
    assert row.get("failureBucket") in {None, "", "unknown"}, (
        "an excluded source must not receive an actionable failure bucket"
    )
