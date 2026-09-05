from __future__ import annotations

from src.jobs.common.dedup_evidence_bundle import _looks_like_job_identifier_token
from src.jobs.reporting_dedup_evidence import build_dedup_evidence
from tests.test_jobs_dedup_provider_static_disagreement import _row


def test_looks_like_job_identifier_token_accepts_job_id_shapes() -> None:
    assert _looks_like_job_identifier_token("7668112003") is True
    assert _looks_like_job_identifier_token("7839485") is True
    assert _looks_like_job_identifier_token("376431") is True


def test_looks_like_job_identifier_token_rejects_slug_shapes() -> None:
    assert _looks_like_job_identifier_token("31stunion") is False
    assert _looks_like_job_identifier_token("wargaming1970") is False
    assert _looks_like_job_identifier_token("2026release") is False
    assert _looks_like_job_identifier_token("studioone") is False


def test_dedup_evidence_auto_safes_current_run_digit_bearing_slug_board_variant() -> None:
    """31st Union regression: a digit-bearing studio slug shared by greenhouse
    board URL variants must not defeat the single-job auto-safe."""

    evidence = build_dedup_evidence(
        {"mergedCount": 1},
        [
            _row(
                company="31st Union",
                title="Producer",
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:31stunion",
                        "sourceJobId": "greenhouse:31stunion:7979993003",
                        "jobLink": "https://job-boards.greenhouse.io/31stunion/jobs/7979993003",
                        "adapter": "greenhouse",
                    },
                    {
                        "source": (
                            "static_source::static:listing_url:"
                            "https://thirtyfirstunion.com/careers/"
                        ),
                        "sourceJobId": "static-1",
                        "jobLink": "https://boards.greenhouse.io/31stunion/jobs/7979993003",
                        "adapter": "static",
                    },
                ],
            )
        ],
    )

    row = evidence["providerStaticDisagreementExamples"][0]
    assert row["disagreementClassification"] == "static_parser_url_variant"
    assert row["concreteSharedIdentifierTokens"] == ["7979993003"]
    assert row["disagreementGateDisposition"] == "warning"
    assert "auto_safe_current_static_parser_url_variant" in row["disagreementGateEvidence"]
    assert evidence["providerStaticDisagreementGateCounts"]["blocked"] == 0
    assert evidence["providerStaticDisagreementGateCounts"]["autoSafeWarning"] == 1


def test_dedup_evidence_auto_safe_survives_extra_static_job_urls_from_same_board() -> None:
    """31st Union Lead UI Engineer shape: the static listing row carries URLs for
    two board jobs; only the provider job's ID is shared, so identity stays 1."""

    evidence = build_dedup_evidence(
        {"mergedCount": 1},
        [
            _row(
                company="31st Union",
                title="Lead UI Engineer",
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:31stunion",
                        "sourceJobId": "greenhouse:31stunion:7668112003",
                        "jobLink": "https://job-boards.greenhouse.io/31stunion/jobs/7668112003",
                        "adapter": "greenhouse",
                    },
                    {
                        "source": (
                            "static_source::static:listing_url:"
                            "https://thirtyfirstunion.com/careers/"
                        ),
                        "sourceJobId": "static-1",
                        "jobLink": "https://boards.greenhouse.io/31stunion/jobs/7668112003",
                        "adapter": "static",
                    },
                    {
                        "source": (
                            "static_source::static:listing_url:"
                            "https://thirtyfirstunion.com/careers/"
                        ),
                        "sourceJobId": "static-2",
                        "jobLink": "https://boards.greenhouse.io/31stunion/jobs/7807544003",
                        "adapter": "static",
                    },
                ],
            )
        ],
    )

    row = evidence["providerStaticDisagreementExamples"][0]
    assert row["disagreementClassification"] == "static_parser_url_variant"
    assert row["concreteSharedIdentifierTokens"] == ["7668112003"]
    assert row["disagreementGateDisposition"] == "warning"
    assert "auto_safe_current_static_parser_url_variant" in row["disagreementGateEvidence"]
    assert evidence["providerStaticDisagreementGateCounts"]["blocked"] == 0


def test_dedup_evidence_blocks_variant_when_job_ids_differ_despite_shared_slug() -> None:
    """A shared board slug without a shared job ID is not a single-job identity."""

    evidence = build_dedup_evidence(
        {"mergedCount": 1},
        [
            _row(
                company="31st Union",
                title="Producer",
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse:slug:31stunion",
                        "sourceJobId": "greenhouse:31stunion:7979993003",
                        "jobLink": "https://job-boards.greenhouse.io/31stunion/jobs/7979993003",
                        "adapter": "greenhouse",
                    },
                    {
                        "source": (
                            "static_source::static:listing_url:"
                            "https://thirtyfirstunion.com/careers/"
                        ),
                        "sourceJobId": "static-1",
                        "jobLink": "https://boards.greenhouse.io/31stunion/jobs/7807544003",
                        "adapter": "static",
                    },
                ],
            )
        ],
    )

    row = evidence["providerStaticDisagreementExamples"][0]
    assert row["disagreementClassification"] == "static_parser_url_variant"
    assert row["concreteSharedIdentifierTokens"] == []
    assert row["disagreementGateDisposition"] == "blocked"
    assert "current_run_or_unclassified_origin" in row["disagreementGateEvidence"]
    assert evidence["providerStaticDisagreementGateCounts"]["blocked"] == 1
