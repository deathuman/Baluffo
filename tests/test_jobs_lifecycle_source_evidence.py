import json
from pathlib import Path

from src import jobs_fetcher as jf
from src.jobs.models import CanonicalJob
from src.jobs.state_lifecycle import (
    apply_job_lifecycle_state,
    build_lifecycle_source_evidence,
)
from tests.helpers.temp_paths import workspace_tmpdir

FINISHED_AT = "2026-04-30T12:00:00+00:00"


def _previous_job(source: str, *, status: str = "active") -> dict[str, str]:
    row = {
        "status": status,
        "firstSeenAt": "2026-04-01T09:00:00+00:00",
        "lastSeenAt": "2026-04-20T09:00:00+00:00",
        "title": "Lifecycle Engineer",
        "company": "Evidence Studio",
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
) -> tuple[dict[str, object], dict[str, int]]:
    evidence = build_lifecycle_source_evidence(
        reports,
        selected_source_names={str(row.get("name") or "") for row in reports},
        allow_missing=True,
    )
    _rows, lifecycle_rows, summary = apply_job_lifecycle_state(
        deduped_rows=current_rows or [],
        lifecycle_rows={"job-1": _previous_job(previous_source, status=previous_status)},
        finished_at=FINISHED_AT,
        allow_mark_missing=False,
        eligible_missing_sources=evidence["eligibleMissingSources"],
        source_evidence=evidence,
    )
    return lifecycle_rows["job-1"], summary


def test_source_error_preserves_missing_previous_job() -> None:
    entry, summary = _apply_with_reports(
        [{"name": "error_source", "status": "error", "error": "timeout"}],
        previous_source="error_source",
    )

    assert entry["status"] == "active"
    assert not entry.get("removedAt")
    assert summary["preservedBecauseSourceFailed"] == 1
    assert summary["likelyRemoved"] == 0


def test_excluded_or_skipped_source_preserves_missing_previous_job() -> None:
    entry, summary = _apply_with_reports(
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
    assert summary["preservedBecauseSourceSkipped"] == 1
    assert summary["likelyRemoved"] == 0


def test_dynamic_redundant_provider_suppression_preserves_missing_previous_static_job() -> None:
    entry, summary = _apply_with_reports(
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
    assert summary["preservedBecauseSourceSkipped"] == 1
    assert summary["likelyRemoved"] == 0


def test_absent_source_evidence_preserves_missing_previous_job() -> None:
    entry, summary = _apply_with_reports([], previous_source="not_selected")

    assert entry["status"] == "active"
    assert not entry.get("removedAt")
    assert summary["preservedBecauseSourceSkipped"] == 1
    assert summary["likelyRemoved"] == 0


def test_explicit_empty_success_marks_missing_previous_job_likely_removed() -> None:
    entry, summary = _apply_with_reports(
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
    assert summary["likelyRemoved"] == 1
    assert summary["eligibleMissingSourceCount"] == 1


def test_needs_review_success_preserves_missing_previous_job() -> None:
    entry, summary = _apply_with_reports(
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
    assert summary["preservedBecauseSourceSkipped"] == 1
    assert summary["likelyRemoved"] == 0


def test_reappeared_previous_removed_job_becomes_active() -> None:
    entry, summary = _apply_with_reports(
        [{"name": "active_source", "status": "ok", "keptCount": 1}],
        previous_source="active_source",
        current_rows=[_active_job("active_source")],
        previous_status="likely_removed",
    )

    assert entry["status"] == "active"
    assert entry["firstSeenAt"] == "2026-04-01T09:00:00+00:00"
    assert entry["lastSeenAt"] == FINISHED_AT
    assert "removedAt" not in entry
    assert summary["reappeared"] == 1


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

            lifecycle_payload = json.loads(
                (out / "jobs-lifecycle-state.json").read_text(encoding="utf-8")
            )
            entry = next(iter((lifecycle_payload.get("jobs") or {}).values()))
            assert str(entry.get("status") or "") == "active"
            assert not str(entry.get("removedAt") or "")
    finally:
        jf.default_source_loaders = previous_default_loaders
