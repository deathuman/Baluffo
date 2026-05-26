from __future__ import annotations

from src.jobs.common.dedup_evidence_provider_static import _provider_static_collision_review_hint
from src.jobs.reporting_dedup_evidence import build_dedup_evidence
from tests.test_jobs_dedup_provider_static_title_company import _provider_static_row


def test_dedup_evidence_reports_collision_review_hint_variants() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 4},
        [
            _provider_static_row(
                dedupKey="missing-side",
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
            ),
            _provider_static_row(
                dedupKey="multi-source",
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
                        "sourceJobId": "static-1",
                        "jobLink": "https://static.example/jobs/1",
                        "adapter": "static",
                    },
                    {
                        "source": "static_source::static:listing_url:https://studio.example/jobs",
                        "sourceJobId": "static-2",
                        "jobLink": "https://static-two.example/jobs/1",
                        "adapter": "static",
                    },
                ],
            ),
            _provider_static_row(
                dedupKey="same-location",
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
        ],
    )

    hints = {
        row["dedupKey"]: row["collisionReviewHint"]
        for row in evidence["providerStaticDisagreementExamples"]
    }
    assert hints["missing-side"] == "provider_static_location_missing"
    assert hints["multi-source"] == "multiple_sources_need_manual_review"
    assert hints["same-location"] == "same_location_different_provider_static_urls"


def test_dedup_evidence_collision_review_hint_unknown_fallback() -> None:
    assert (
        _provider_static_collision_review_hint(
            classification="needs_manual_review",
            summary={"sourceBundleCount": 1, "distinctLocationCount": 2},
            provider_urls=["https://provider.example/jobs/1"],
            static_urls=["https://static.example/jobs/1"],
            provider_ids=["gh-1"],
            static_ids=["static-1"],
        )
        == "unknown"
    )
