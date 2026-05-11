from __future__ import annotations

from src.jobs.reporting_dedup_evidence import build_dedup_evidence
from tests.test_jobs_dedup_audit_gate import _row


def test_dedup_audit_gate_details_cover_all_current_blocker_families() -> None:
    evidence = build_dedup_evidence(
        {
            "mergedCount": 1,
            "mergedByPrimaryUrl": 1,
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
    assert gate["blockers"] == ["provider_static_disagreement_needs_review"]
    details = {item["key"]: item for item in gate["blockerDetails"]}
    assert details["provider_static_disagreement_needs_review"]["count"] == 1
    assert "high_risk_review_queue_causes_need_review" not in details


def test_dedup_audit_gate_details_do_not_duplicate_provider_static_in_review_queue() -> None:
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
    assert "provider_static_disagreement_needs_review" in gate["blockers"]
    assert "high_risk_review_queue_causes_need_review" not in gate["blockers"]
    assert gate["currentRunBlockingReviewQueueCauseCounts"]["provider_static_disagreement"] == 0


def test_dedup_audit_gate_details_warn_for_carried_monitor_causes() -> None:
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
        if item["key"] == "monitor_review_queue_diagnostics_present"
    )
    assert "high_risk_review_queue_causes_need_review" not in gate["blockers"]
    assert gate["carriedMonitorReviewQueueCauseCounts"]["spreadsheet_role_bucket_needs_review"] == 1
    assert detail["count"] == 1
    assert detail["counts"] == {"carried.spreadsheet_role_bucket_needs_review": 1}
