from src.source_discovery.reporting_backlog import update_candidate_review_metadata
from src.source_registry import (
    REGISTRY_REASON_REPEATED_ZERO_JOBS,
    hide_repeated_zero_job_pending,
)


def test_repeated_zero_job_deferred_candidate_becomes_hidden_after_threshold() -> None:
    updated = update_candidate_review_metadata(
        {
            "id": "zero-job-pending",
            "name": "Zero Job Pending",
            "adapter": "static",
            "jobsFound": 0,
            "sampleCount": 0,
            "deferred": True,
            "deferReason": "top_n_cap",
        },
        prior_candidate={"deferCount": 2, "firstDeferredAt": "2026-04-20T00:00:00Z"},
        now_iso="2026-04-26T00:00:00Z",
    )
    hidden = hide_repeated_zero_job_pending(updated, at="2026-04-26T00:00:00Z")

    assert updated["deferCount"] == 3
    assert hidden["candidateState"] == "hidden"
    assert hidden["hiddenFromDefault"] is True
    assert hidden["pendingReason"] == REGISTRY_REASON_REPEATED_ZERO_JOBS


def test_repeated_zero_job_candidate_stays_visible_before_threshold() -> None:
    updated = update_candidate_review_metadata(
        {
            "id": "new-zero-job-pending",
            "name": "New Zero Job Pending",
            "adapter": "static",
            "jobsFound": 0,
            "sampleCount": 0,
            "deferred": True,
            "deferReason": "top_n_cap",
        },
        prior_candidate={"deferCount": 1},
        now_iso="2026-04-26T00:00:00Z",
    )
    visible = hide_repeated_zero_job_pending(updated, at="2026-04-26T00:00:00Z")

    assert updated["deferCount"] == 2
    assert visible["candidateState"] == "validated"
    assert "hiddenFromDefault" not in visible
