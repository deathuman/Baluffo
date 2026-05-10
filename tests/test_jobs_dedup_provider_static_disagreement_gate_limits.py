from __future__ import annotations

from src.jobs.reporting_dedup_evidence import build_dedup_evidence
from tests.test_jobs_dedup_provider_static_disagreement import _row


def test_dedup_evidence_preserves_all_provider_static_blockers_beyond_example_limit() -> None:
    rows = [
        _row(
            dedupKey=f"blocked-{index}",
            title=f"Blocked Role {index}",
            sourceBundleCount=2,
            sourceBundle=[
                {
                    "source": "greenhouse:slug:studio-one",
                    "sourceJobId": f"greenhouse:studio-one:provider-{index}",
                    "jobLink": f"https://provider.example/jobs/provider-{index}",
                    "adapter": "greenhouse",
                },
                {
                    "source": "static_source::static:listing_url:https://studio.example/careers",
                    "sourceJobId": f"static-{index}",
                    "jobLink": f"https://static.example/jobs/static-{index}",
                    "adapter": "static",
                },
            ],
        )
        for index in range(4)
    ]

    evidence = build_dedup_evidence({"mergedCount": 1}, rows, risky_limit=2)
    examples = evidence["providerStaticDisagreementExamples"]

    assert len(examples) == 4
    assert {row["dedupKey"] for row in examples} == {
        "blocked-0",
        "blocked-1",
        "blocked-2",
        "blocked-3",
    }
    assert all(row["disagreementGateDisposition"] == "blocked" for row in examples)
    assert evidence["providerStaticDisagreementGateCounts"]["blocked"] == 4


def test_dedup_evidence_downgrades_current_run_static_parser_variant_with_extra_source() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 1},
        [
            _row(
                sourceBundleCount=3,
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
                    {
                        "source": "mastodon:studio-one",
                        "sourceJobId": "social-4022147009",
                        "jobLink": "https://social.example/studio-one/4022147009",
                        "adapter": "mastodon",
                    },
                ],
            )
        ],
    )

    row = evidence["providerStaticDisagreementExamples"][0]
    assert row["disagreementClassification"] == "static_parser_url_variant"
    assert row["providerStaticOnly"] is False
    assert row["disagreementGateDisposition"] == "warning"
    assert "auto_safe_current_static_parser_url_variant" in row["disagreementGateEvidence"]
    assert evidence["providerStaticDisagreementGateCounts"]["blocked"] == 0
    assert evidence["providerStaticDisagreementGateCounts"]["autoSafeWarning"] == 1


def test_dedup_evidence_blocks_current_run_variant_with_multiple_concrete_tokens() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 1},
        [
            _row(
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:studio-one",
                        "sourceJobId": "greenhouse:studio-one:4022147009:4022147010",
                        "jobLink": (
                            "https://job-boards.greenhouse.io/studioone/jobs/"
                            "4022147009/related/4022147010"
                        ),
                        "adapter": "greenhouse",
                    },
                    {
                        "source": "static_source::static:listing_url:https://studio.example/careers",
                        "sourceJobId": "static-1",
                        "jobLink": (
                            "https://studio.example/work-with-us/4022147009/related/4022147010"
                        ),
                        "adapter": "static",
                    },
                ],
            )
        ],
    )

    row = evidence["providerStaticDisagreementExamples"][0]
    assert row["disagreementClassification"] == "static_parser_url_variant"
    assert row["concreteSharedIdentifierTokens"] == ["4022147009", "4022147010"]
    assert row["disagreementGateDisposition"] == "blocked"
    assert evidence["providerStaticDisagreementGateCounts"]["blocked"] == 1
