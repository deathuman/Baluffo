from __future__ import annotations

from src.jobs.reporting_dedup_evidence import build_dedup_evidence


def _row(**overrides):
    payload = {
        "id": "job-1",
        "dedupKey": "key-1",
        "title": "Senior Engineer",
        "company": "Studio One",
        "jobLink": "https://example.com/jobs/1",
        "locationSummary": "Amsterdam, NL",
        "sourceBundleCount": 1,
        "sourceBundle": [
            {
                "source": "greenhouse:slug:studio-one",
                "sourceJobId": "gh-1",
                "jobLink": "https://example.com/jobs/1",
                "adapter": "greenhouse",
            }
        ],
        "locations": [{"city": "Amsterdam", "country": "NL"}],
    }
    payload.update(overrides)
    return payload


def test_dedup_audit_gate_returns_safe_defaults_for_empty_evidence() -> None:
    evidence = build_dedup_evidence({}, [])

    assert evidence["dedupAuditGate"] == {
        "status": "pass",
        "lifecycleUxReady": True,
        "currentRunMergedCount": 0,
        "sourceBundleCollisionCount": 0,
        "highRiskReviewQueueCount": 0,
        "providerStaticDisagreementCount": 0,
        "providerStaticDisagreementCurrentRunCount": 0,
        "providerStaticDisagreementCarriedCount": 0,
        "googleSheetsGenericRoleGuardActive": True,
        "carriedCollisionLikelyHistoricalCount": 0,
        "currentRunSourceBundleCollisionCount": 0,
        "carriedSourceBundleCollisionCount": 0,
        "reviewQueueCauseCounts": evidence["reviewQueueCauseCounts"],
        "currentRunHighRiskReviewQueueCount": 0,
        "carriedHighRiskReviewQueueCount": 0,
        "blockers": [],
        "warnings": [],
        "examples": [],
        "nonzeroReviewQueueCauseCounts": {},
    }


def test_dedup_audit_gate_warns_on_carried_historical_collisions() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "gh-1",
                        "jobLink": "https://example.com/jobs/1",
                        "adapter": "greenhouse",
                    },
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "gh-1",
                        "jobLink": "https://example.com/jobs/1",
                        "adapter": "greenhouse",
                    },
                ],
            )
        ],
        seeded_from_existing_output=True,
    )

    gate = evidence["dedupAuditGate"]
    assert gate["status"] == "warning"
    assert gate["lifecycleUxReady"] is True
    assert gate["carriedCollisionLikelyHistoricalCount"] == 1
    assert gate["currentRunSourceBundleCollisionCount"] == 0
    assert gate["carriedSourceBundleCollisionCount"] == 1
    assert "carried_source_bundle_collisions_present" in gate["warnings"]


def test_dedup_audit_gate_warns_on_carried_google_sheets_role_buckets() -> None:
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
    assert gate["status"] == "warning"
    assert gate["lifecycleUxReady"] is True
    assert gate["carriedHighRiskReviewQueueCount"] == 1
    assert gate["currentRunHighRiskReviewQueueCount"] == 0
    assert "carried_high_risk_review_queue_causes_present" in gate["warnings"]
    assert evidence["carriedBundleExamples"][0]["bundleEvidenceOrigin"] == (
        "carried_from_existing_output"
    )


def test_dedup_audit_gate_blocks_current_run_non_primary_merges() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 1, "mergedByPrimaryUrl": 0, "mergedBySparseIdentity": 1},
        [],
    )

    gate = evidence["dedupAuditGate"]
    assert gate["status"] == "blocked"
    assert gate["lifecycleUxReady"] is False
    assert gate["currentRunMergedCount"] == 1
    assert "current_run_non_primary_merges_need_review" in gate["blockers"]


def test_dedup_audit_gate_blocks_provider_static_disagreement() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
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
                        "sourceJobId": "",
                        "jobLink": "https://static.example/jobs/1",
                        "adapter": "static",
                    },
                ],
            )
        ],
        seeded_from_existing_output=True,
    )

    gate = evidence["dedupAuditGate"]
    assert gate["status"] == "blocked"
    assert gate["lifecycleUxReady"] is False
    assert gate["providerStaticDisagreementCount"] == 1
    assert gate["providerStaticDisagreementCurrentRunCount"] == 0
    assert gate["providerStaticDisagreementCarriedCount"] == 1
    assert gate["highRiskReviewQueueCount"] == 1
    assert gate["carriedHighRiskReviewQueueCount"] == 1
    assert "provider_static_disagreement_needs_review" in gate["blockers"]
    assert gate["examples"][0]["suspectedCause"] == "provider_static_disagreement"
    assert gate["examples"][0]["recommendedReviewAction"] == ("review_provider_static_disagreement")


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


def test_dedup_audit_gate_blocks_current_run_provider_static_disagreement() -> None:
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
    assert gate["status"] == "blocked"
    assert gate["providerStaticDisagreementCount"] == 1
    assert gate["providerStaticDisagreementCurrentRunCount"] == 1
    assert gate["providerStaticDisagreementCarriedCount"] == 0
    assert "provider_static_disagreement_needs_review" in gate["blockers"]


def test_dedup_audit_gate_blocks_carried_real_multi_location_provider_static_conflict() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
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
                        "jobLink": "https://static.example/jobs/2",
                        "adapter": "static",
                    },
                ],
                locations=[
                    {"city": "Amsterdam", "country": "NL"},
                    {"city": "Rotterdam", "country": "NL"},
                ],
            )
        ],
        seeded_from_existing_output=True,
    )

    gate = evidence["dedupAuditGate"]
    assert gate["status"] == "blocked"
    assert gate["lifecycleUxReady"] is False
    assert "provider_static_disagreement_needs_review" in gate["blockers"]
    assert gate["examples"][0]["carriedLocationPollutionAudit"] == (
        "possible_real_multi_location_conflict"
    )


def test_dedup_audit_gate_prefers_unresolved_provider_static_examples_over_pollution_only_rows() -> (
    None
):
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                dedupKey="key-pollution",
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
            ),
            _row(
                dedupKey="key-blocker",
                title="Senior Engineer",
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "gh-2",
                        "jobLink": "https://provider.example/jobs/3",
                        "adapter": "greenhouse",
                    },
                    {
                        "source": "static_source::static:listing_url:https://studio.example/careers",
                        "sourceJobId": "static-2",
                        "jobLink": "https://static.example/jobs/4",
                        "adapter": "static",
                    },
                ],
                locations=[{"city": "Amsterdam", "country": "NL"}],
            ),
        ],
        seeded_from_existing_output=True,
    )

    gate = evidence["dedupAuditGate"]
    assert gate["status"] == "blocked"
    assert gate["examples"][0]["recommendedReviewAction"] == "review_provider_static_disagreement"
    assert gate["examples"][0]["disagreementClassification"] != "title_company_collision"


def test_dedup_audit_gate_counts_fresh_collisions_as_current_run() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 1, "mergedByPrimaryUrl": 1},
        [
            _row(
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "gh-1",
                        "jobLink": "https://example.com/jobs/1",
                        "adapter": "greenhouse",
                    },
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "gh-1",
                        "jobLink": "https://example.com/jobs/1",
                        "adapter": "greenhouse",
                    },
                ],
            )
        ],
    )

    gate = evidence["dedupAuditGate"]
    assert gate["currentRunSourceBundleCollisionCount"] == 1
    assert gate["carriedSourceBundleCollisionCount"] == 0
    assert evidence["topSourceBundleOutliers"][0]["bundleEvidenceOrigin"] == "current_run"


def test_dedup_audit_gate_warns_on_current_primary_url_merges() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 1, "mergedByPrimaryUrl": 1},
        [],
    )

    gate = evidence["dedupAuditGate"]
    assert gate["status"] == "warning"
    assert gate["lifecycleUxReady"] is True
    assert "current_run_primary_url_merges_present" in gate["warnings"]
