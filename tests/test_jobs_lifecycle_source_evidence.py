from pathlib import Path
from typing import Any

from src import jobs_fetcher as jf
from src.jobs.models import CanonicalJob
from src.jobs.state_lifecycle import (
    apply_job_lifecycle_state,
    build_lifecycle_source_evidence,
)
from src.shared.json_io import read_json
from tests.helpers.temp_paths import workspace_tmpdir

FINISHED_AT = "2026-04-30T12:00:00+00:00"


def _previous_job(source: str, *, status: str = "active") -> dict[str, Any]:
    row = {
        "status": status,
        "firstSeenAt": "2026-04-01T09:00:00+00:00",
        "lastSeenAt": "2026-04-20T09:00:00+00:00",
        "title": "Lifecycle Engineer",
        "company": "Evidence Studio",
        "city": "Remote",
        "country": "Remote",
        "jobLink": "https://example.com/jobs/lifecycle",
        "source": source,
        "sourceJobId": "life-1",
        "postedAt": "2026-03-01T00:00:00+00:00",
    }
    if status == "likely_removed":
        row["removedAt"] = "2026-04-25T09:00:00+00:00"
    return row


def _active_job(source: str) -> CanonicalJob:
    return CanonicalJob.from_mapping(
        {
            "dedupKey": "job-1",
            "title": "Lifecycle Engineer",
            "company": "Evidence Studio",
            "city": "Remote",
            "country": "Remote",
            "jobLink": "https://example.com/jobs/lifecycle",
            "source": source,
            "sourceJobId": "life-1",
            "postedAt": "2026-03-01T00:00:00+00:00",
        }
    )


def _apply_with_reports(
    reports: list[dict[str, object]],
    *,
    previous_source: str,
    current_rows: list[CanonicalJob] | None = None,
    previous_status: str = "active",
) -> tuple[list[CanonicalJob], dict[str, object], dict[str, int]]:
    evidence = build_lifecycle_source_evidence(
        reports,
        selected_source_names={str(row.get("name") or "") for row in reports},
        allow_missing=True,
    )
    rows, lifecycle_rows, _archive_rows_by_year, summary = apply_job_lifecycle_state(
        deduped_rows=current_rows or [],
        lifecycle_rows={"job-1": _previous_job(previous_source, status=previous_status)},
        finished_at=FINISHED_AT,
        allow_mark_missing=False,
        eligible_missing_sources=evidence["eligibleMissingSources"],
        source_evidence=evidence,
    )
    return rows, lifecycle_rows["job-1"], summary


def test_source_error_preserves_missing_previous_job() -> None:
    _rows, entry, summary = _apply_with_reports(
        [{"name": "error_source", "status": "error", "error": "timeout"}],
        previous_source="error_source",
    )

    assert entry["status"] == "active"
    assert not entry.get("removedAt")
    assert entry["city"] == "Remote"
    assert entry["country"] == "Remote"
    assert entry["lifecycleEvent"] == "preserved"
    assert entry["lifecycleReason"] == "source_failed"
    assert summary["preservedBecauseSourceFailed"] == 1
    assert summary["likelyRemoved"] == 0


def test_excluded_or_skipped_source_preserves_missing_previous_job() -> None:
    _rows, entry, summary = _apply_with_reports(
        [
            {
                "name": "cache_skipped",
                "status": "excluded",
                "exclusionReason": "cache_static_empty_fresh",
                "cacheDecision": "skip_fresh",
            }
        ],
        previous_source="cache_skipped",
    )

    assert entry["status"] == "active"
    assert not entry.get("removedAt")
    assert entry["lifecycleEvent"] == "preserved"
    assert entry["lifecycleReason"] == "source_skipped"
    assert summary["preservedBecauseSourceSkipped"] == 1
    assert summary["likelyRemoved"] == 0


def test_dynamic_redundant_provider_suppression_preserves_missing_previous_static_job() -> None:
    _rows, entry, summary = _apply_with_reports(
        [
            {
                "name": "static_source::static:listing_url:https://studio.example/jobs",
                "adapter": "static",
                "status": "excluded",
                "exclusionReason": "dynamic_redundant_provider",
                "coveredByProviderSourceId": "Studio Greenhouse",
                "providerCoverageStatus": "validated_provider",
            }
        ],
        previous_source="static_source::static:listing_url:https://studio.example/jobs",
    )

    assert entry["status"] == "active"
    assert not entry.get("removedAt")
    assert entry["lifecycleEvent"] == "preserved"
    assert entry["lifecycleReason"] == "source_skipped"
    assert summary["preservedBecauseSourceSkipped"] == 1
    assert summary["likelyRemoved"] == 0


def test_absent_source_evidence_preserves_missing_previous_job() -> None:
    _rows, entry, summary = _apply_with_reports([], previous_source="not_selected")

    assert entry["status"] == "active"
    assert not entry.get("removedAt")
    assert entry["lifecycleEvent"] == "preserved"
    assert entry["lifecycleReason"] == "source_skipped"
    assert summary["preservedBecauseSourceSkipped"] == 1
    assert summary["likelyRemoved"] == 0


def test_explicit_empty_success_marks_missing_previous_job_likely_removed() -> None:
    _rows, entry, summary = _apply_with_reports(
        [
            {
                "name": "empty_source",
                "status": "ok",
                "keptCount": 0,
                "classification": "empty_confirmed",
                "failureBucket": "no_openings",
            }
        ],
        previous_source="empty_source",
    )

    assert entry["status"] == "likely_removed"
    assert entry["removedAt"] == FINISHED_AT
    assert entry.get("lifecycleEvent") == ""
    assert entry.get("lifecycleReason") == ""
    assert summary["likelyRemoved"] == 1
    assert summary["eligibleMissingSourceCount"] == 1


def test_needs_review_success_preserves_missing_previous_job() -> None:
    _rows, entry, summary = _apply_with_reports(
        [
            {
                "name": "needs_review_source",
                "status": "ok",
                "keptCount": 0,
                "classification": "needs_review",
                "failureBucket": "needs_review",
                "browserFallbackRecommended": True,
            }
        ],
        previous_source="needs_review_source",
    )

    assert entry["status"] == "active"
    assert not entry.get("removedAt")
    assert entry["lifecycleEvent"] == "preserved"
    assert entry["lifecycleReason"] == "source_skipped"
    assert summary["preservedBecauseSourceSkipped"] == 1
    assert summary["likelyRemoved"] == 0


def test_reappeared_previous_removed_job_becomes_active() -> None:
    rows, entry, summary = _apply_with_reports(
        [{"name": "active_source", "status": "ok", "keptCount": 1}],
        previous_source="active_source",
        current_rows=[_active_job("active_source")],
        previous_status="likely_removed",
    )

    assert entry["status"] == "active"
    assert entry["firstSeenAt"] == "2026-04-01T09:00:00+00:00"
    assert entry["lastSeenAt"] == FINISHED_AT
    assert "removedAt" not in entry
    assert entry["lifecycleEvent"] == "reappeared"
    assert summary["reappeared"] == 1
    assert rows[0].lifecycleEvent == "reappeared"
    assert rows[0].lifecycleReason == ""


def test_pipeline_preserves_missing_job_when_owning_source_fails() -> None:
    def failing_source_job(**_: object):
        return [
            {
                "title": "Reliability Engineer",
                "company": "Lifecycle Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/lifecycle/reliability-engineer",
                "sector": "Game",
                "sourceJobId": "life-fail-1",
                "postedAt": "2026-03-01",
            }
        ]

    def failing_loader(**_: object):
        raise RuntimeError("timeout")

    def ok_loader(**_: object):
        return []

    previous_default_loaders = jf.default_source_loaders
    try:
        with workspace_tmpdir("jobs-fetcher-lifecycle-failed-source") as tmp:
            out = Path(tmp)
            jf.default_source_loaders = lambda: [
                ("failed_source", failing_source_job),
                ("ok_source", ok_loader),
            ]
            first = jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )
            assert int(first["summary"].get("outputCount") or 0) == 1

            jf.default_source_loaders = lambda: [
                ("failed_source", failing_loader),
                ("ok_source", ok_loader),
            ]
            second = jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )

            assert int(second["summary"].get("failedSources") or 0) == 1
            assert int(second["summary"].get("lifecycleLikelyRemovedCount") or 0) == 0
            lifecycle_summary = second.get("lifecycleSummary") or {}
            assert int(lifecycle_summary.get("preservedBecauseSourceFailedCount") or 0) == 1

            lifecycle_payload = read_json(out / "jobs-lifecycle-state.json", {})
            entry = next(iter((lifecycle_payload.get("jobs") or {}).values()))
            assert str(entry.get("status") or "") == "active"
            assert not str(entry.get("removedAt") or "")
    finally:
        jf.default_source_loaders = previous_default_loaders


def test_seeded_row_is_not_observed_and_successful_source_absence_retires_it() -> None:
    def one_job_loader(**_: object):
        return [_active_job("seed_source").to_dict()]

    def empty_loader(**_: object):
        return []

    previous_default_loaders = jf.default_source_loaders
    try:
        with workspace_tmpdir("jobs-fetcher-seed-not-observed") as tmp:
            out = Path(tmp)
            jf.default_source_loaders = lambda: [("seed_source", one_job_loader)]
            first = jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )
            first_row = read_json(out / "jobs-unified.json", [])[0]
            first_seen = first_row["lastSeenAt"]

            jf.default_source_loaders = lambda: [("seed_source", empty_loader)]
            second = jf.run_pipeline(
                output_dir=out,
                preserve_previous_on_empty=False,
                force_refresh_all=True,
                seed_from_existing_output=True,
            )

            assert int(second["summary"].get("outputCount") or 0) == 0
            lifecycle = read_json(out / "jobs-lifecycle-state.json", {})["jobs"]
            entry = next(iter(lifecycle.values()))
            assert entry["lastSeenAt"] == first_seen
            assert entry["availabilityStatus"] == "unavailable"
            assert entry["availabilityEvidence"]["kind"] == "source_absent"
            history = read_json(out / "jobs-availability-history.json", {})
            assert history["rows"][0]["availabilityId"] == entry["availabilityId"]
            assert "canonicalRow" not in history["rows"][0]
            tombstones = read_json(out / "jobs-availability-tombstones.json", {})["rows"]
            assert (
                tombstones[entry["availabilityId"]]["canonicalRow"]["workType"]
                == first_row["workType"]
            )
    finally:
        jf.default_source_loaders = previous_default_loaders


def test_two_failed_attempts_and_seven_days_without_confirmation_marks_overdue() -> None:
    entry = _previous_job("failed_source")
    entry["availabilityId"] = "availability_failed"
    entry["availabilityStatus"] = "available"
    entry["availabilityVerifiedAt"] = "2026-04-20T09:00:00+00:00"
    entry["consecutiveAvailabilityFailures"] = 1
    evidence = build_lifecycle_source_evidence(
        [{"name": "failed_source", "status": "error", "error": "timeout"}],
        selected_source_names={"failed_source"},
        allow_missing=True,
    )
    rows, lifecycle, _archive, summary = apply_job_lifecycle_state(
        deduped_rows=[],
        lifecycle_rows={"job-1": entry},
        finished_at=FINISHED_AT,
        allow_mark_missing=False,
        source_evidence=evidence,
    )

    assert rows == []
    assert lifecycle["job-1"]["status"] == "active"
    assert lifecycle["job-1"]["availabilityStatus"] == "verification_overdue"
    assert lifecycle["job-1"]["consecutiveAvailabilityFailures"] == 2
    assert summary["availabilityOverdue"] == 1


def test_identity_alias_retains_availability_id_when_dedup_winner_changes() -> None:
    previous = _previous_job("source_a")
    previous.update(
        {
            "availabilityId": "availability_stable",
            "availabilityStatus": "available",
            "availabilityAliases": [
                "source:source_a:life-1",
                "url:2f4cfd84a5f6d8dbe14b659f1024ec8b8e9bb56c8dba11f25e308ea31d76e258",
            ],
        }
    )
    current = CanonicalJob.from_mapping(
        {**_active_job("source_a").to_dict(), "dedupKey": "new-winner-key"}
    )
    rows, lifecycle, _archive, _summary = apply_job_lifecycle_state(
        deduped_rows=[current],
        observed_rows=[current],
        lifecycle_rows={"old-winner-key": previous},
        finished_at=FINISHED_AT,
        allow_mark_missing=False,
    )

    assert list(lifecycle) == ["old-winner-key"]
    assert rows[0].availabilityId == "availability_stable"


def test_lifecycle_identity_does_not_recover_by_fuzzy_title_company() -> None:
    first = CanonicalJob.from_mapping(
        {"title": "Designer", "company": "Studio", "dedupKey": "job-a"}
    )
    second = CanonicalJob.from_mapping(
        {"title": "Designer", "company": "Studio", "dedupKey": "job-b"}
    )
    rows, lifecycle, _archive, _summary = apply_job_lifecycle_state(
        deduped_rows=[second],
        observed_rows=[second],
        lifecycle_rows={"job-a": {"status": "active", **first.to_dict()}},
        finished_at=FINISHED_AT,
        allow_mark_missing=False,
    )

    assert set(lifecycle) == {"job-a"}
    assert rows[0].availabilityId == ""
