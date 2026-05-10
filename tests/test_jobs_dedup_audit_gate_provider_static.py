from __future__ import annotations

from src.jobs.reporting_dedup_evidence import build_dedup_evidence
from tests.test_jobs_dedup_audit_gate import _row


def test_dedup_audit_gate_warns_when_carried_title_company_collision_is_only_location_pollution() -> (
    None
):
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                title="Concept Artist / Illustrator",
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
                        "jobLink": "https://static.example/jobs/2",
                        "adapter": "static",
                    },
                ],
                locations=[
                    {"city": "Illustrator", "country": ""},
                    {"city": "Salem", "country": "US"},
                ],
            )
        ],
        seeded_from_existing_output=True,
    )

    gate = evidence["dedupAuditGate"]
    assert gate["status"] == "warning"
    assert gate["lifecycleUxReady"] is True
    assert "provider_static_disagreement_needs_review" not in gate["blockers"]
    assert "carried_provider_static_location_pollution_present" in gate["warnings"]
    assert gate["examples"][0]["carriedLocationPollutionAudit"] == "carried_location_pollution"


def test_dedup_audit_gate_allows_lifecycle_when_only_carried_auto_safe_disagreements_remain() -> (
    None
):
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "gh-123456",
                        "jobLink": "https://studio.example/provider/jobs/123456",
                        "adapter": "greenhouse",
                    },
                    {
                        "source": "static_source::static:listing_url:https://studio.example/careers",
                        "sourceJobId": "static-123456",
                        "jobLink": "https://studio.example/careers/jobs/123456",
                        "adapter": "static",
                    },
                ],
            )
        ],
        seeded_from_existing_output=True,
    )

    gate = evidence["dedupAuditGate"]
    assert gate["status"] == "warning"
    assert gate["lifecycleUxReady"] is True
    assert gate["providerStaticDisagreementBlockedCount"] == 0
    assert gate["providerStaticDisagreementWarningCount"] == 1
    assert "provider_static_disagreement_needs_review" not in gate["blockers"]
    assert "carried_provider_static_auto_safe_variants_present" in gate["warnings"]
