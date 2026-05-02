from __future__ import annotations

from src.jobs.common.contracts_fetch_report import normalize_fetch_report_payload
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


def test_dedup_evidence_reports_top_merged_jobs_and_reason_counts() -> None:
    rows = [
        _row(
            sourceBundleCount=3,
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
                {
                    "source": "social_reddit",
                    "sourceJobId": "reddit-1",
                    "jobLink": "https://reddit.example/post/1",
                    "adapter": "reddit",
                },
            ],
        )
    ]

    evidence = build_dedup_evidence(
        {
            "mergedCount": 4,
            "mergedByPrimaryUrl": 1,
            "mergedBySecondaryKey": 1,
            "mergedBySocialKey": 1,
            "collisionSamplesCount": 2,
        },
        rows,
    )

    assert evidence["mergeReasonCounts"] == {
        "primaryUrl": 1,
        "secondaryKey": 1,
        "socialKey": 1,
        "sparseIdentity": 1,
        "unknown": 0,
    }
    assert evidence["sourceBundleComposition"] == {
        "provider": 1,
        "static": 1,
        "social": 1,
        "other": 0,
    }
    assert evidence["sourceBundleCollisionCount"] == 1
    assert evidence["topMergedJobs"][0]["sourceBundleCount"] == 3
    assert evidence["topSourceBundleOutliers"][0]["sourceBundleCount"] == 3
    assert evidence["topMergedJobs"][0]["sourceClasses"]["static"] == 1
    assert evidence["outlierReasonCounts"]["provider_static_disagreement"] == 1
    assert evidence["topSourceBundleOutliers"][0]["outlierReason"] == (
        "provider_static_disagreement"
    )
    assert evidence["topSourceBundleOutliers"][0]["uniqueJobLinkCount"] == 3
    assert evidence["topSourceBundleOutliers"][0]["providerSourceJobIdCount"] == 1
    assert evidence["topSourceBundleOutliers"][0]["hasStrongIdentity"] is True
    assert evidence["topSourceBundleOutliers"][0]["dominantSourceClass"] == "provider"
    assert evidence["topSourceBundleOutliers"][0]["identityShape"] == "provider_id_backed"
    assert evidence["identityShapeCounts"]["provider_id_backed"] == 1


def test_dedup_evidence_flags_risky_location_and_provider_static_disagreement() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 1},
        [
            _row(
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "",
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
                locations=[
                    {"city": "Amsterdam", "country": "NL"},
                    {"city": "Rotterdam", "country": "NL"},
                ],
            )
        ],
    )

    reasons = evidence["riskyMergeExamples"][0]["riskReasons"]
    assert "same_title_company_different_location" in reasons
    assert "provider_static_duplicate_disagreement" in reasons
    assert "missing_provider_ids" in reasons
    assert "weak_title_company_only_evidence" in reasons
    assert evidence["riskReasonCounts"] == {
        "same_title_company_different_location": 1,
        "provider_static_duplicate_disagreement": 1,
        "missing_provider_ids": 1,
        "weak_title_company_only_evidence": 1,
    }
    assert evidence["outlierReasonCounts"]["provider_static_disagreement"] == 1
    assert evidence["locationDivergenceExamples"][0]["distinctLocationCount"] == 2
    assert evidence["locationDivergenceExamples"][0]["sampleLocations"] == [
        "amsterdam, nl",
        "rotterdam, nl",
    ]


def test_exact_url_bundle_is_not_risky_only_because_it_merged() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 1, "mergedByPrimaryUrl": 1},
        [
            _row(
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "",
                        "jobLink": "https://example.com/jobs/1?utm_source=x",
                        "adapter": "greenhouse",
                    },
                    {
                        "source": "static_source::static:listing_url:https://studio.example/careers",
                        "sourceJobId": "",
                        "jobLink": "https://example.com/jobs/1",
                        "adapter": "static",
                    },
                ],
            )
        ],
    )

    assert evidence["topMergedJobs"]
    assert evidence["riskyMergeExamples"] == []
    assert evidence["riskReasonCounts"]["missing_provider_ids"] == 0


def test_dedup_evidence_caps_samples_deterministically() -> None:
    rows = [
        _row(
            id=f"job-{index}",
            dedupKey=f"key-{index:02d}",
            title=f"Role {index:02d}",
            company=f"Studio {index:02d}",
            sourceBundleCount=2,
            sourceBundle=[
                {"source": "source-a", "sourceJobId": "", "jobLink": "", "adapter": "custom"},
                {"source": "source-b", "sourceJobId": "", "jobLink": "", "adapter": "custom"},
            ],
        )
        for index in range(12)
    ]

    evidence = build_dedup_evidence({"mergedCount": 12}, list(reversed(rows)))

    assert evidence["riskyMergeExampleCount"] == 12
    assert evidence["riskReasonCounts"]["weak_title_company_only_evidence"] == 12
    assert len(evidence["riskyMergeExamples"]) == 10
    assert len(evidence["topSourceBundleOutliers"]) == 10
    assert evidence["outlierReasonCounts"]["sparse_title_company_bundle"] == 12
    assert [row["dedupKey"] for row in evidence["riskyMergeExamples"][:3]] == [
        "key-00",
        "key-01",
        "key-02",
    ]
    assert [row["dedupKey"] for row in evidence["topSourceBundleOutliers"][:3]] == [
        "key-00",
        "key-01",
        "key-02",
    ]


def test_dedup_evidence_reports_carried_bundle_collisions_without_current_merges() -> None:
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
                        "source": "static_source::static:listing_url:https://studio.example/careers",
                        "sourceJobId": "",
                        "jobLink": "https://example.com/jobs/1",
                        "adapter": "static",
                    },
                ],
            )
        ],
    )

    assert evidence["mergedCount"] == 0
    assert evidence["mergeReasonCounts"] == {
        "primaryUrl": 0,
        "secondaryKey": 0,
        "socialKey": 0,
        "sparseIdentity": 0,
        "unknown": 0,
    }
    assert evidence["sourceBundleCollisionCount"] == 1
    assert evidence["topSourceBundleOutliers"][0]["sourceBundleCount"] == 2
    assert evidence["topSourceBundleOutliers"][0]["outlierReason"] == "unknown"
    assert evidence["topSourceBundleOutliers"][0]["identityShape"] == "provider_id_backed"


def test_dedup_evidence_classifies_multi_location_strong_identity() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "provider-1",
                        "jobLink": "https://example.com/jobs/1",
                        "adapter": "greenhouse",
                    },
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "provider-2",
                        "jobLink": "https://example.com/jobs/1",
                        "adapter": "greenhouse",
                    },
                ],
                locations=[
                    {"city": "Amsterdam", "country": "NL"},
                    {"city": "Berlin", "country": "DE"},
                ],
            )
        ],
    )

    outlier = evidence["topSourceBundleOutliers"][0]
    assert outlier["outlierReason"] == "multi_location_strong_identity"
    assert outlier["identityShape"] == "provider_id_backed"
    assert outlier["distinctLocationCount"] == 2
    assert outlier["sharedPrimaryUrl"] is True
    assert evidence["outlierReasonCounts"]["multi_location_strong_identity"] == 1


def test_dedup_evidence_classifies_location_divergence_without_strong_identity() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundleCount=2,
                sourceBundle=[
                    {"source": "source-a", "sourceJobId": "", "jobLink": "", "adapter": "custom"},
                    {"source": "source-b", "sourceJobId": "", "jobLink": "", "adapter": "custom"},
                ],
                locations=[
                    {"city": "Amsterdam", "country": "NL"},
                    {"city": "Berlin", "country": "DE"},
                ],
            )
        ],
    )

    assert evidence["topSourceBundleOutliers"][0]["outlierReason"] == (
        "location_divergence_without_strong_identity"
    )
    assert evidence["topSourceBundleOutliers"][0]["identityShape"] == "missing_url_and_ids"
    assert evidence["outlierReasonCounts"]["location_divergence_without_strong_identity"] == 1


def test_dedup_evidence_classifies_large_other_source_bundles() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundleCount=12,
                sourceBundle=[
                    {
                        "source": "category-a",
                        "sourceJobId": "",
                        "jobLink": "https://example.com/category/a",
                        "adapter": "custom",
                    },
                    {
                        "source": "category-b",
                        "sourceJobId": "",
                        "jobLink": "https://example.com/category/b",
                        "adapter": "custom",
                    },
                ],
            )
        ],
    )

    outlier = evidence["topSourceBundleOutliers"][0]
    assert outlier["outlierReason"] == "large_other_source_bundle"
    assert outlier["identityShape"] == "many_unique_urls_same_title"
    assert outlier["dominantSourceClass"] == "other"
    assert evidence["outlierReasonCounts"]["large_other_source_bundle"] == 1


def test_dedup_evidence_empty_rows_returns_empty_aggregates() -> None:
    evidence = build_dedup_evidence({}, [])

    assert evidence["mergedCount"] == 0
    assert evidence["sourceBundleCollisionCount"] == 0
    assert evidence["riskReasonCounts"] == {
        "same_title_company_different_location": 0,
        "provider_static_duplicate_disagreement": 0,
        "missing_provider_ids": 0,
        "weak_title_company_only_evidence": 0,
    }
    assert evidence["outlierReasonCounts"] == {
        "multi_location_strong_identity": 0,
        "location_divergence_without_strong_identity": 0,
        "provider_static_disagreement": 0,
        "large_other_source_bundle": 0,
        "sparse_title_company_bundle": 0,
        "unknown": 0,
    }
    assert evidence["identityShapeCounts"] == {
        "shared_job_detail_url": 0,
        "shared_listing_or_category_url": 0,
        "many_unique_urls_same_title": 0,
        "provider_id_backed": 0,
        "missing_url_and_ids": 0,
        "mixed_or_unknown_identity": 0,
    }
    assert evidence["reviewQueueCounts"] == {
        "review_many_urls_same_title": 0,
        "review_listing_url_bundle": 0,
        "review_category_title_bundle": 0,
        "review_open_application_bundle": 0,
        "review_provider_static_disagreement": 0,
        "monitor": 0,
    }
    assert not any(evidence["identityQualityCounts"].values())
    assert "unknown" in evidence["identityQualityCounts"]
    assert not any(evidence["reviewQueueCauseCounts"].values())
    assert "unknown" in evidence["reviewQueueCauseCounts"]
    assert evidence["topSourceBundleOutliers"] == []
    assert evidence["locationDivergenceExamples"] == []
    assert evidence["reviewQueue"] == []
    assert evidence["riskyMergeExamples"] == []


def test_fetch_report_normalization_preserves_dedup_evidence() -> None:
    normalized = normalize_fetch_report_payload(
        {
            "summary": {"inputCount": 2, "outputCount": 1},
            "sources": [],
            "dedupEvidence": {
                "schemaVersion": 1,
                "mergedCount": 1,
                "topMergedJobs": [{"title": "Senior Engineer"}],
            },
        }
    )

    assert normalized["dedupEvidence"]["mergedCount"] == 1
    assert normalized["dedupEvidence"]["topMergedJobs"][0]["title"] == "Senior Engineer"
