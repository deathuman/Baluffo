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


def test_dedup_evidence_classifies_many_urls_same_host_as_weak_non_provider_identity() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                title="Product-management",
                company="eBay",
                sourceBundleCount=3,
                sourceBundle=[
                    {
                        "source": "directory-a",
                        "sourceJobId": "",
                        "jobLink": "https://directory.example/jobs/123/product-a",
                        "adapter": "custom",
                    },
                    {
                        "source": "directory-b",
                        "sourceJobId": "",
                        "jobLink": "https://directory.example/jobs/123/product-b",
                        "adapter": "custom",
                    },
                    {
                        "source": "directory-c",
                        "sourceJobId": "",
                        "jobLink": "https://directory.example/jobs/123/product-c",
                        "adapter": "custom",
                    },
                ],
            )
        ],
    )

    row = evidence["reviewQueue"][0]
    assert row["identityQuality"] == "many_urls_same_host_weak"
    assert row["suspectedCause"] == "non_provider_url_identity_needs_review"
    assert "quality:many_urls_same_host_weak" in row["causeEvidence"]
    assert "quality:many_urls_same_host_weak" in row["identityQualityEvidence"]
    assert evidence["identityQualityCounts"]["many_urls_same_host_weak"] == 1
    assert evidence["reviewQueueCauseCounts"]["non_provider_url_identity_needs_review"] == 1


def test_dedup_evidence_classifies_many_urls_many_hosts_as_weak_identity() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundle=[
                    {
                        "source": "directory-a",
                        "sourceJobId": "",
                        "jobLink": "https://a.example/jobs/producer",
                        "adapter": "custom",
                    },
                    {
                        "source": "directory-b",
                        "sourceJobId": "",
                        "jobLink": "https://b.example/jobs/producer",
                        "adapter": "custom",
                    },
                ],
            )
        ],
    )

    row = evidence["reviewQueue"][0]
    assert row["identityQuality"] == "many_urls_many_hosts_weak"
    assert evidence["identityQualityCounts"]["many_urls_many_hosts_weak"] == 1


def test_dedup_evidence_classifies_non_provider_source_ids_as_untrusted() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundle=[
                    {
                        "source": "directory-a",
                        "sourceJobId": "directory-job-1",
                        "jobLink": "https://directory.example/jobs/1",
                        "adapter": "custom",
                    },
                    {
                        "source": "directory-b",
                        "sourceJobId": "directory-job-1",
                        "jobLink": "https://directory.example/jobs/1",
                        "adapter": "custom",
                    },
                ],
            )
        ],
    )

    outlier = evidence["topSourceBundleOutliers"][0]
    assert outlier["identityQuality"] == "other_source_id_untrusted"
    assert outlier["nonProviderSourceJobIdCount"] == 1
    assert evidence["identityQualityCounts"]["other_source_id_untrusted"] == 1


def test_dedup_evidence_classifies_missing_identity() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundle=[
                    {
                        "source": "directory-a",
                        "sourceJobId": "",
                        "jobLink": "",
                        "adapter": "custom",
                    },
                    {
                        "source": "directory-b",
                        "sourceJobId": "",
                        "jobLink": "",
                        "adapter": "custom",
                    },
                ],
            )
        ],
    )

    outlier = evidence["topSourceBundleOutliers"][0]
    assert outlier["identityQuality"] == "missing_identity"
    assert evidence["identityQualityCounts"]["missing_identity"] == 1
