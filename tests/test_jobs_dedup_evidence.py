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
    assert evidence["topMergedJobs"][0]["sourceBundleCount"] == 3
    assert evidence["topMergedJobs"][0]["sourceClasses"]["static"] == 1


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
    assert len(evidence["riskyMergeExamples"]) == 10
    assert [row["dedupKey"] for row in evidence["riskyMergeExamples"][:3]] == [
        "key-00",
        "key-01",
        "key-02",
    ]


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
