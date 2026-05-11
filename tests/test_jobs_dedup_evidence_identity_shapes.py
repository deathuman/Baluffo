from __future__ import annotations

from src.jobs.reporting_dedup_evidence import build_dedup_evidence


def _row(**overrides):
    payload = {
        "id": "job-identity",
        "dedupKey": "identity-key",
        "title": "Senior Engineer",
        "company": "Studio One",
        "jobLink": "https://example.com/jobs/1",
        "locationSummary": "Amsterdam, NL",
        "sourceBundleCount": 2,
        "sourceBundle": [],
        "locations": [{"city": "Amsterdam", "country": "NL"}],
    }
    payload.update(overrides)
    return payload


def test_dedup_evidence_reports_shared_job_detail_url_identity() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundle=[
                    {
                        "source": "static-a",
                        "sourceJobId": "",
                        "jobLink": "https://studio.example/jobs/123-senior-engineer",
                        "adapter": "custom",
                    },
                    {
                        "source": "static-b",
                        "sourceJobId": "",
                        "jobLink": "https://studio.example/jobs/123-senior-engineer",
                        "adapter": "custom",
                    },
                ],
            )
        ],
    )

    outlier = evidence["topSourceBundleOutliers"][0]
    assert outlier["identityShape"] == "shared_job_detail_url"
    assert outlier["identityQuality"] == "shared_detail_url_strong"
    assert outlier["sharedUrlHost"] == "studio.example"
    assert outlier["sharedUrlPath"] == "/jobs/123-senior-engineer"
    assert outlier["identityCaveats"] == [
        "other_source_class_dominant",
        "shared_url_without_provider_ids",
    ]
    assert evidence["identityShapeCounts"]["shared_job_detail_url"] == 1
    assert evidence["identityQualityCounts"]["shared_detail_url_strong"] == 1
    assert evidence["reviewQueueCounts"]["monitor"] == 1
    assert evidence["reviewQueueCauseCounts"]["unknown"] == 1
    assert evidence["reviewQueue"] == []


def test_dedup_evidence_reports_shared_listing_url_identity_and_caveats() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                title="Accounting",
                sourceBundle=[
                    {
                        "source": "kforce-a",
                        "sourceJobId": "",
                        "jobLink": "https://kforce.example/jobs",
                        "adapter": "custom",
                    },
                    {
                        "source": "kforce-b",
                        "sourceJobId": "",
                        "jobLink": "https://kforce.example/jobs",
                        "adapter": "custom",
                    },
                ],
                locations=[
                    {"city": "Phoenix", "country": "US"},
                    {"city": "Tempe", "country": "US"},
                ],
            )
        ],
    )

    outlier = evidence["topSourceBundleOutliers"][0]
    assert outlier["identityShape"] == "shared_listing_or_category_url"
    assert outlier["identityQuality"] == "shared_listing_url_weak"
    assert outlier["titleShape"] == "category_like"
    assert "shared_url_looks_like_listing_or_category" in outlier["identityCaveats"]
    assert "category_like_title" in outlier["identityCaveats"]
    assert evidence["identityShapeCounts"]["shared_listing_or_category_url"] == 1
    assert evidence["identityQualityCounts"]["shared_listing_url_weak"] == 1
    assert evidence["reviewQueueCounts"]["monitor"] == 1
    assert evidence["reviewQueueCauseCounts"]["category_or_department_bucket"] == 1
    assert evidence["reviewQueue"][0]["recommendedReviewAction"] == "monitor"
    assert evidence["reviewQueue"][0]["suspectedCause"] == "category_or_department_bucket"
    assert "caveat:category_like_title" in evidence["reviewQueue"][0]["causeEvidence"]
    assert evidence["reviewQueue"][0]["sampleSources"] == ["kforce-a", "kforce-b"]


def test_dedup_evidence_reports_many_unique_urls_same_title() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundle=[
                    {
                        "source": "static-a",
                        "sourceJobId": "",
                        "jobLink": "https://studio.example/jobs/designer-a",
                        "adapter": "static",
                    },
                    {
                        "source": "static-b",
                        "sourceJobId": "",
                        "jobLink": "https://studio.example/jobs/designer-b",
                        "adapter": "static",
                    },
                ],
            )
        ],
    )

    outlier = evidence["topSourceBundleOutliers"][0]
    assert outlier["identityShape"] == "many_unique_urls_same_title"
    assert outlier["identityQuality"] == "many_urls_many_hosts_weak"
    assert outlier["uniqueUrlHostCount"] == 1
    assert outlier["uniqueUrlPathPrefixCount"] == 2
    assert "many_unique_urls_same_title" in outlier["identityCaveats"]
    assert evidence["identityShapeCounts"]["many_unique_urls_same_title"] == 1
    assert evidence["identityQualityCounts"]["many_urls_many_hosts_weak"] == 1
    assert evidence["reviewQueueCounts"]["monitor"] == 1
    assert evidence["reviewQueueCauseCounts"]["unknown"] == 1
    assert evidence["reviewQueue"][0]["recommendedReviewAction"] == "monitor"
    assert evidence["reviewQueue"][0]["suspectedCause"] == "unknown"


def test_dedup_evidence_reports_speculative_title_caveat() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                title="Initiativbewerbung - Playa Games",
                sourceBundle=[
                    {
                        "source": "static-a",
                        "sourceJobId": "",
                        "jobLink": "https://studio.example/jobs/open-application-a",
                        "adapter": "static",
                    },
                    {
                        "source": "static-b",
                        "sourceJobId": "",
                        "jobLink": "https://studio.example/jobs/open-application-b",
                        "adapter": "static",
                    },
                ],
            )
        ],
    )

    outlier = evidence["topSourceBundleOutliers"][0]
    assert outlier["titleShape"] == "speculative_or_open_application"
    assert "speculative_or_open_application_title" in outlier["identityCaveats"]
    assert evidence["reviewQueueCounts"]["review_open_application_bundle"] == 1
    assert evidence["reviewQueueCauseCounts"]["open_application_family"] == 1
    assert evidence["reviewQueue"][0]["recommendedReviewAction"] == (
        "review_open_application_bundle"
    )
    assert evidence["reviewQueue"][0]["suspectedCause"] == "open_application_family"


def test_dedup_evidence_review_queue_counts_category_titles() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                title="Software-development-&-engineering",
                sourceBundle=[
                    {
                        "source": "apple-a",
                        "sourceJobId": "",
                        "jobLink": "https://jobs.example/software/a",
                        "adapter": "custom",
                    },
                    {
                        "source": "apple-b",
                        "sourceJobId": "",
                        "jobLink": "https://jobs.example/software/b",
                        "adapter": "custom",
                    },
                ],
            )
        ],
    )

    assert evidence["reviewQueueCounts"]["monitor"] == 1
    assert evidence["reviewQueue"][0]["recommendedReviewAction"] == "monitor"
    assert evidence["reviewQueue"][0]["suspectedCause"] == "category_or_department_bucket"


def test_dedup_evidence_review_queue_counts_provider_static_disagreement() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
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
            )
        ],
    )

    assert evidence["reviewQueueCounts"]["review_provider_static_disagreement"] == 1
    assert evidence["reviewQueueCauseCounts"]["provider_static_disagreement"] == 1
    assert evidence["reviewQueue"][0]["recommendedReviewAction"] == (
        "review_provider_static_disagreement"
    )
    assert evidence["reviewQueue"][0]["suspectedCause"] == "provider_static_disagreement"


def test_dedup_evidence_classifies_listing_page_bundle_without_category_title() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                title="Senior Producer",
                sourceBundle=[
                    {
                        "source": "studio-a",
                        "sourceJobId": "",
                        "jobLink": "https://studio.example/careers",
                        "adapter": "custom",
                    },
                    {
                        "source": "studio-b",
                        "sourceJobId": "",
                        "jobLink": "https://studio.example/careers",
                        "adapter": "custom",
                    },
                ],
            )
        ],
    )

    row = evidence["reviewQueue"][0]
    assert row["recommendedReviewAction"] == "monitor"
    assert row["suspectedCause"] == "listing_page_bundle"
    assert "identity:shared_listing_or_category_url" in row["causeEvidence"]
    assert evidence["reviewQueueCauseCounts"]["listing_page_bundle"] == 1


def test_dedup_evidence_classifies_parser_or_directory_text_pollution() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                title="nDreams 4",
                company="11 bit studios 3",
                sourceBundle=[
                    {
                        "source": "directory-a",
                        "sourceJobId": "",
                        "jobLink": "https://directory.example/studios/ndreams",
                        "adapter": "custom",
                    },
                    {
                        "source": "directory-b",
                        "sourceJobId": "",
                        "jobLink": "https://directory.example/studios/11-bit",
                        "adapter": "custom",
                    },
                ],
            )
        ],
    )

    row = evidence["reviewQueue"][0]
    assert row["suspectedCause"] == "parser_or_directory_text_pollution"
    assert "title_numeric_suffix" in row["titleCompanyPollutionSignals"]
    assert "company_numeric_suffix" in row["titleCompanyPollutionSignals"]
    assert "pollution:title_numeric_suffix" in row["causeEvidence"]
    assert evidence["reviewQueueCauseCounts"]["parser_or_directory_text_pollution"] == 1


def test_dedup_evidence_classifies_provider_backed_bundle_as_likely_legitimate() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "job-1",
                        "jobLink": "https://boards.greenhouse.io/studio/jobs/1",
                        "adapter": "greenhouse",
                    },
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "job-1",
                        "jobLink": "https://boards.greenhouse.io/studio/jobs/1",
                        "adapter": "greenhouse",
                    },
                ],
            )
        ],
    )

    outlier = evidence["topSourceBundleOutliers"][0]
    assert outlier["identityQuality"] == "provider_id_strong"
    assert evidence["identityQualityCounts"]["provider_id_strong"] == 1
    assert outlier["suspectedCause"] == "likely_legitimate_multi_role_family"
    assert "strong_identity" in outlier["causeEvidence"]
    assert evidence["reviewQueueCauseCounts"]["likely_legitimate_multi_role_family"] == 1
    assert evidence["reviewQueue"] == []


def test_dedup_evidence_review_queue_is_capped_and_stable() -> None:
    rows = [
        _row(
            id=f"job-{index}",
            dedupKey=f"key-{index:02d}",
            title=f"Role {index:02d}",
            company=f"Studio {index:02d}",
            sourceBundleCount=2,
            sourceBundle=[
                {
                    "source": f"static-{index}-a",
                    "sourceJobId": "",
                    "jobLink": f"https://studio.example/jobs/{index}-a",
                    "adapter": "static",
                },
                {
                    "source": f"static-{index}-b",
                    "sourceJobId": "",
                    "jobLink": f"https://studio.example/jobs/{index}-b",
                    "adapter": "static",
                },
            ],
        )
        for index in range(12)
    ]

    evidence = build_dedup_evidence({"mergedCount": 0}, list(reversed(rows)))

    assert evidence["reviewQueueCounts"]["monitor"] == 12
    assert len(evidence["reviewQueue"]) == 10
    assert [row["dedupKey"] for row in evidence["reviewQueue"][:3]] == [
        "key-00",
        "key-01",
        "key-02",
    ]
