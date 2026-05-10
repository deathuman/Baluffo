from __future__ import annotations

from src.jobs.reporting_dedup_evidence import build_dedup_evidence
from tests.test_jobs_dedup_audit_gate import _row


def test_dedup_audit_gate_details_cover_all_current_blocker_families() -> None:
    evidence = build_dedup_evidence(
        {
            "mergedCount": 2,
            "mergedByPrimaryUrl": 1,
            "mergedBySparseIdentity": 1,
            "collisionSamples": [
                {
                    "reason": "sparse_identity",
                    "existingDedupKey": "key-existing",
                    "incomingSource": "static_source::studio",
                    "incomingTitle": "Senior Engineer",
                    "incomingCompany": "Studio One",
                    "incomingJobLink": "https://static.example/jobs/1",
                }
            ],
        },
        [
            _row(
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "gh-1",
                        "jobLink": "https://provider.example/jobs/1",
                        "adapter": "greenhouse",
                    },
                    {
                        "source": "static_source::static:listing_url:https://studio.example/careers",
                        "sourceJobId": "static-1",
                        "jobLink": "https://static.example/jobs/1",
                        "adapter": "static",
                    },
                ],
            )
        ],
    )

    gate = evidence["dedupAuditGate"]
    assert gate["blockers"] == [
        "current_run_non_primary_merges_need_review",
        "provider_static_disagreement_needs_review",
        "high_risk_review_queue_causes_need_review",
    ]
    details = {item["key"]: item for item in gate["blockerDetails"]}
    assert details["current_run_non_primary_merges_need_review"]["count"] == 1
    assert details["provider_static_disagreement_needs_review"]["count"] == 1
    assert details["high_risk_review_queue_causes_need_review"]["count"] == 1
    assert details["high_risk_review_queue_causes_need_review"]["counts"] == {
        "provider_static_disagreement": 1
    }


def test_dedup_audit_gate_details_use_current_run_cause_counts() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 1, "mergedByPrimaryUrl": 1},
        [
            _row(
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "gh-1",
                        "jobLink": "https://provider.example/jobs/1",
                        "adapter": "greenhouse",
                    },
                    {
                        "source": "static_source::static:listing_url:https://studio.example/careers",
                        "sourceJobId": "static-1",
                        "jobLink": "https://static.example/jobs/1",
                        "adapter": "static",
                    },
                ],
            )
        ],
    )

    gate = evidence["dedupAuditGate"]
    detail = next(
        item
        for item in gate["blockerDetails"]
        if item["key"] == "high_risk_review_queue_causes_need_review"
    )
    assert gate["currentRunBlockingReviewQueueCauseCounts"]["provider_static_disagreement"] == 1
    assert detail["counts"] == {"provider_static_disagreement": 1}


def test_dedup_audit_gate_details_warn_for_carried_high_risk_causes() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                title="Product-management",
                company="eBay",
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "google_sheets",
                        "sourceJobId": "sheet-1",
                        "jobLink": "https://example.com/jobs/1",
                        "adapter": "google_sheets",
                    },
                    {
                        "source": "google_sheets",
                        "sourceJobId": "sheet-2",
                        "jobLink": "https://example.com/jobs/2",
                        "adapter": "google_sheets",
                    },
                ],
            )
        ],
        seeded_from_existing_output=True,
    )

    gate = evidence["dedupAuditGate"]
    detail = next(
        item
        for item in gate["warningDetails"]
        if item["key"] == "carried_high_risk_review_queue_causes_present"
    )
    assert "high_risk_review_queue_causes_need_review" not in gate["blockers"]
    assert (
        gate["carriedBlockingReviewQueueCauseCounts"]["spreadsheet_role_bucket_needs_review"] == 1
    )
    assert detail["count"] == 1
    assert detail["counts"] == {"spreadsheet_role_bucket_needs_review": 1}
