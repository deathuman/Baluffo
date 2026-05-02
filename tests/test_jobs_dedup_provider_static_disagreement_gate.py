from __future__ import annotations

from src.jobs.reporting_dedup_evidence import build_dedup_evidence
from tests.test_jobs_dedup_provider_static_disagreement import _row


def test_dedup_evidence_downgrades_carried_canonical_url_disagreement_to_warning() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "gh-1",
                        "jobLink": "https://studio.example/provider/jobs/1",
                        "adapter": "greenhouse",
                    },
                    {
                        "source": "static_source::static:listing_url:https://studio.example/careers",
                        "sourceJobId": "static-1",
                        "jobLink": "https://studio.example/careers/jobs/1",
                        "adapter": "static",
                    },
                ],
            )
        ],
        seeded_from_existing_output=True,
    )

    row = evidence["providerStaticDisagreementExamples"][0]
    assert row["disagreementClassification"] == "provider_redirect_or_canonical_url"
    assert row["disagreementGateDisposition"] == "warning"
    assert "auto_safe_carried_provider_redirect_or_canonical_url" in row["disagreementGateEvidence"]
    assert evidence["providerStaticDisagreementGateCounts"]["blocked"] == 0
    assert evidence["providerStaticDisagreementGateCounts"]["warning"] == 1
    assert evidence["providerStaticDisagreementGateCounts"]["autoSafeWarning"] == 1


def test_dedup_evidence_downgrades_carried_static_parser_variant_to_warning() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _row(
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "greenhouse:studio-one:4022147009",
                        "jobLink": "https://job-boards.greenhouse.io/studioone/jobs/4022147009",
                        "adapter": "greenhouse",
                    },
                    {
                        "source": "static_source::static:listing_url:https://studio.example/careers",
                        "sourceJobId": "static-1",
                        "jobLink": "https://studio.example/work-with-us/4022147009",
                        "adapter": "static",
                    },
                ],
            )
        ],
        seeded_from_existing_output=True,
    )

    row = evidence["providerStaticDisagreementExamples"][0]
    assert row["disagreementClassification"] == "static_parser_url_variant"
    assert row["disagreementGateDisposition"] == "warning"
    assert "auto_safe_carried_static_parser_url_variant" in row["disagreementGateEvidence"]
    assert evidence["providerStaticDisagreementGateCounts"]["blocked"] == 0
    assert evidence["providerStaticDisagreementGateCounts"]["warning"] == 1
    assert evidence["providerStaticDisagreementGateCounts"]["autoSafeWarning"] == 1


def test_dedup_evidence_reviewed_safe_downgrades_targeted_row_only() -> None:
    review_state = {
        "pairs": {
            "ignored": {
                "disagreementClassification": "same_job_different_urls",
                "providerSourceJobIds": ["gh-1"],
                "staticSourceJobIds": ["static-1"],
                "dedupKey": "key-1",
                "reviewStatus": "reviewed_safe",
                "reviewedAt": "2026-05-02T10:00:00Z",
                "reviewedBy": "admin",
                "reviewNote": "safe carried disagreement",
            }
        }
    }
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
                        "jobLink": "https://static.example/jobs/1",
                        "adapter": "static",
                    },
                ],
            ),
            _row(
                dedupKey="key-2",
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "gh-2",
                        "jobLink": "https://provider.example/jobs/2",
                        "adapter": "greenhouse",
                    },
                    {
                        "source": "static_source::static:listing_url:https://studio.example/careers",
                        "sourceJobId": "static-2",
                        "jobLink": "https://static.example/jobs/2",
                        "adapter": "static",
                    },
                ],
            ),
        ],
        seeded_from_existing_output=True,
        review_state=review_state,
    )

    rows = {row["dedupKey"]: row for row in evidence["providerStaticDisagreementExamples"]}
    assert rows["key-1"]["dedupReviewStatus"] == "reviewed_safe"
    assert rows["key-1"]["disagreementGateDisposition"] == "warning"
    assert rows["key-2"]["dedupReviewStatus"] == ""
    assert rows["key-2"]["disagreementGateDisposition"] == "blocked"
    assert evidence["providerStaticDisagreementGateCounts"]["blocked"] == 1
    assert evidence["providerStaticDisagreementGateCounts"]["warning"] == 1
    assert evidence["providerStaticDisagreementGateCounts"]["reviewedSafeWarning"] == 1


def test_dedup_evidence_confirmed_blocking_preserves_blocker_metadata() -> None:
    review_state = {
        "pairs": {
            "ignored": {
                "disagreementClassification": "same_job_different_urls",
                "providerSourceJobIds": ["gh-1"],
                "staticSourceJobIds": ["static-1"],
                "dedupKey": "key-1",
                "reviewStatus": "confirmed_blocking",
                "reviewedAt": "2026-05-02T10:00:00Z",
                "reviewedBy": "admin",
                "reviewNote": "still unresolved",
            }
        }
    }
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
                        "jobLink": "https://static.example/jobs/1",
                        "adapter": "static",
                    },
                ],
            )
        ],
        seeded_from_existing_output=True,
        review_state=review_state,
    )

    row = evidence["providerStaticDisagreementExamples"][0]
    assert row["dedupReviewStatus"] == "confirmed_blocking"
    assert row["disagreementGateDisposition"] == "blocked"
    assert "manual_review_confirmed_blocking" in row["disagreementGateEvidence"]
    assert evidence["providerStaticDisagreementGateCounts"]["confirmedBlocking"] == 1
