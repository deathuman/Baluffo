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
    assert outlier["sharedUrlHost"] == "studio.example"
    assert outlier["sharedUrlPath"] == "/jobs/123-senior-engineer"
    assert outlier["identityCaveats"] == [
        "other_source_class_dominant",
        "shared_url_without_provider_ids",
    ]
    assert evidence["identityShapeCounts"]["shared_job_detail_url"] == 1


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
    assert outlier["titleShape"] == "category_like"
    assert "shared_url_looks_like_listing_or_category" in outlier["identityCaveats"]
    assert "category_like_title" in outlier["identityCaveats"]
    assert evidence["identityShapeCounts"]["shared_listing_or_category_url"] == 1


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
    assert outlier["uniqueUrlHostCount"] == 1
    assert outlier["uniqueUrlPathPrefixCount"] == 2
    assert "many_unique_urls_same_title" in outlier["identityCaveats"]
    assert evidence["identityShapeCounts"]["many_unique_urls_same_title"] == 1


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
