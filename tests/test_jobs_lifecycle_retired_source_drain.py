"""Focused tests: availability rows of retired/tombstoned/repointed sources drain.

Fix A: when the lifecycle source-evidence payload is authoritative (built from
the full registered-run universe), a lifecycle row whose source is absent from
eligible/failed/skipped belongs to a registry row that no loader can re-observe
(retired, tombstoned, repointed, or pending). Such rows must drain through the
missing path (unavailable, then archived after the remove-to-archive window)
instead of being preserved forever as verification_overdue. The failed/skipped
shields and the legacy no-universe behavior must survive unchanged.
"""

from __future__ import annotations

from typing import Any

from src.jobs.models import CanonicalJob
from src.jobs.state_lifecycle import apply_job_lifecycle_state

FINISHED_AT = "2026-04-30T12:00:00+00:00"


def _previous_job(
    source: str,
    *,
    status: str = "active",
    availability: str = "available",
    failures: int = 0,
    verified_at: str = "2026-04-20T09:00:00+00:00",
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "status": status,
        "firstSeenAt": "2026-04-01T09:00:00+00:00",
        "lastSeenAt": "2026-04-20T09:00:00+00:00",
        "title": "Retirement Engineer",
        "company": "Orphan Studio",
        "city": "Remote",
        "country": "Remote",
        "jobLink": "https://example.com/jobs/orphan",
        "source": source,
        "sourceJobId": "orphan-1",
        "postedAt": "2026-03-01T00:00:00+00:00",
        "availabilityId": "availability-orphan-1",
        "availabilityStatus": availability,
        "availabilityVerifiedAt": verified_at,
        "consecutiveAvailabilityFailures": failures,
    }
    if status == "likely_removed":
        row["removedAt"] = "2026-04-10T09:00:00+00:00"
    return row


def _active_job(source: str) -> CanonicalJob:
    return CanonicalJob.from_mapping(
        {
            "dedupKey": "orphan-1",
            "title": "Retirement Engineer",
            "company": "Orphan Studio",
            "city": "Remote",
            "country": "Remote",
            "jobLink": "https://example.com/jobs/orphan",
            "source": source,
            "sourceJobId": "orphan-1",
            "postedAt": "2026-03-01T00:00:00+00:00",
        }
    )


def _evidence(
    *,
    eligible: tuple[str, ...] = (),
    failed: tuple[str, ...] = (),
    skipped: tuple[str, ...] = (),
) -> dict[str, set[str]]:
    return {
        "eligibleMissingSources": set(eligible),
        "failedMissingSources": set(failed),
        "skippedMissingSources": set(skipped),
    }


def _apply(
    lifecycle_row: dict[str, Any],
    *,
    evidence: dict[str, set[str]],
    known: set[str] | None,
    current_rows: list[CanonicalJob] | None = None,
    remove_to_archive_days: int = 14,
) -> tuple[list[CanonicalJob], dict[str, Any], dict[str, Any], dict[str, int]]:
    rows, lifecycle_rows, _archive, summary = apply_job_lifecycle_state(
        deduped_rows=current_rows or [],
        lifecycle_rows={"orphan-1": lifecycle_row},
        finished_at=FINISHED_AT,
        allow_mark_missing=False,
        eligible_missing_sources=evidence["eligibleMissingSources"],
        source_evidence=evidence,
        known_missing_evidence_sources=known,
        remove_to_archive_days=remove_to_archive_days,
    )
    return rows, lifecycle_rows["orphan-1"], lifecycle_rows, summary


def test_retired_source_row_drains_to_unavailable() -> None:
    """Source absent from an authoritative universe: unavailable + excluded from feed."""

    rows, entry, _lifecycle, summary = _apply(
        _previous_job("retired_source"),
        evidence=_evidence(eligible=("live_source",), skipped=("skipped_source",)),
        known={"live_source", "skipped_source", "failed_source"},
    )

    assert entry["status"] == "likely_removed"
    assert entry["availabilityStatus"] == "unavailable"
    assert entry["availabilityUnavailableAt"] == FINISHED_AT
    assert entry["consecutiveAvailabilityFailures"] == 0
    assert entry["removedAt"] == FINISHED_AT
    assert rows == []  # drained rows are not projected into the feed
    assert summary["retiredSourceDrained"] == 1
    assert summary["preservedBecauseSourceFailed"] == 0
    assert summary["preservedBecauseSourceSkipped"] == 0
    assert summary["availabilityUnavailable"] == 1
    assert summary["availabilityOverdue"] == 0


def test_retired_source_row_archives_after_remove_to_archive_window() -> None:
    """Already likely_removed past the window: archived (ages out via retention)."""

    _rows, entry, _lifecycle, summary = _apply(
        _previous_job("retired_source", status="likely_removed"),
        evidence=_evidence(eligible=("live_source",)),
        known={"live_source"},
    )

    assert entry["status"] == "archived"
    assert entry["availabilityStatus"] == "unavailable"
    assert entry["archivedAt"] == FINISHED_AT
    assert summary["retiredSourceDrained"] == 1


def test_retired_source_row_not_archived_inside_window() -> None:
    """Recently removed rows drain to unavailable but stay likely_removed until aged."""

    _rows, entry, _lifecycle, _summary = _apply(
        _previous_job("retired_source", status="likely_removed"),
        evidence=_evidence(eligible=("live_source",)),
        known={"live_source"},
        remove_to_archive_days=30,
    )

    assert entry["status"] == "likely_removed"
    assert entry["availabilityStatus"] == "unavailable"


def test_skipped_source_shield_survives_authoritative_universe() -> None:
    """A known skipped source (cadence/subset/exclusion) is still preserved, not drained."""

    _rows, entry, _lifecycle, summary = _apply(
        _previous_job("skipped_source"),
        evidence=_evidence(eligible=("live_source",), skipped=("skipped_source",)),
        known={"live_source", "skipped_source"},
    )

    assert entry["status"] == "active"
    assert entry["availabilityStatus"] == "available"
    assert entry["lifecycleEvent"] == "preserved"
    assert entry["lifecycleReason"] == "source_skipped"
    assert summary["preservedBecauseSourceSkipped"] == 1
    assert summary["retiredSourceDrained"] == 0


def test_failed_source_shield_survives_authoritative_universe() -> None:
    """A known failed source still decays through the overdue path, never drains."""

    _rows, entry, _lifecycle, summary = _apply(
        _previous_job("failed_source", availability="verification_overdue", failures=5),
        evidence=_evidence(eligible=("live_source",), failed=("failed_source",)),
        known={"live_source", "failed_source"},
    )

    assert entry["status"] == "active"
    assert entry["availabilityStatus"] == "verification_overdue"
    assert entry["consecutiveAvailabilityFailures"] == 6
    assert entry["lifecycleReason"] == "source_failed"
    assert summary["preservedBecauseSourceFailed"] == 1
    assert summary["retiredSourceDrained"] == 0
    assert summary["availabilityOverdue"] == 1


def test_empty_universe_keeps_legacy_preserved_behavior() -> None:
    """No authoritative universe (custom loaders): legacy source_skipped preservation."""

    _rows, entry, _lifecycle, summary = _apply(
        _previous_job("mystery_source"),
        evidence=_evidence(),
        known=None,
    )

    assert entry["status"] == "active"
    assert entry["availabilityStatus"] == "available"
    assert entry["lifecycleReason"] == "source_skipped"
    assert summary["preservedBecauseSourceSkipped"] == 1
    assert summary["retiredSourceDrained"] == 0


def test_overdue_row_of_retired_source_resets_through_drain() -> None:
    """The stranded-289 shape: overdue + high failure count drains and resets counters."""

    _rows, entry, _lifecycle, summary = _apply(
        _previous_job(
            "retired_source",
            availability="verification_overdue",
            failures=6,
            verified_at="2026-03-01T09:00:00+00:00",
        ),
        evidence=_evidence(eligible=("live_source",)),
        known={"live_source"},
    )

    assert entry["status"] == "likely_removed"
    assert entry["availabilityStatus"] == "unavailable"
    assert entry["consecutiveAvailabilityFailures"] == 0
    assert summary["retiredSourceDrained"] == 1
    assert summary["availabilityOverdue"] == 0
    assert summary["availabilityUnavailable"] == 1


def test_reobserved_row_is_never_drained() -> None:
    """An observed row of a universe-absent source is seen first; the drain skips it."""

    rows, entry, _lifecycle, summary = _apply(
        _previous_job("returned_source"),
        evidence=_evidence(eligible=("live_source",)),
        known={"live_source"},
        current_rows=[_active_job("returned_source")],
    )

    assert entry["status"] == "active"
    assert entry["availabilityStatus"] == "available"
    assert summary["retiredSourceDrained"] == 0
    assert len(rows) == 1
