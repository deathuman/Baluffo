from __future__ import annotations

from src.jobs.reporting_dedup_evidence import build_dedup_evidence
from tests.test_jobs_dedup_provider_static_disagreement import _row


def test_dedup_evidence_downgrades_current_run_wargaming_greenhouse_vacancy_alias() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 1, "mergedByPrimaryUrl": 1},
        [
            _row(
                company="Wargaming",
                title="Senior 3D Environment Artist (World of Tanks)",
                locations=[{"city": "Berlin", "country": "DE"}],
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "greenhouse_boards",
                        "sourceJobId": "greenhouse:wargamingen:7650088",
                        "jobLink": (
                            "https://boards.greenhouse.io/wargamingen/jobs/7650088?gh_jid=7650088"
                        ),
                        "adapter": "greenhouse",
                    },
                    {
                        "source": (
                            "static_source::static:listing_url:https://wargaming.com/en/careers/"
                        ),
                        "sourceJobId": (
                            "static:static:listing_url:https://wargaming.com/en/careers/:191ebdd1a0"
                        ),
                        "jobLink": "https://wargaming.com/en/careers/vacancy_3369860_berlin",
                        "adapter": "static",
                    },
                ],
            )
        ],
    )

    row = evidence["providerStaticDisagreementExamples"][0]
    assert row["disagreementClassification"] == "same_job_different_urls"
    assert row["concreteSharedIdentifierTokens"] == []
    assert "wargaming_greenhouse_careers_vacancy_alias" in row["disagreementClassificationEvidence"]
    assert row["disagreementGateDisposition"] == "warning"
    assert (
        "auto_safe_current_wargaming_greenhouse_careers_vacancy_alias"
        in row["disagreementGateEvidence"]
    )
    assert row["operatorReviewReason"] == "auto_safe_provider_static_variant"
    assert evidence["providerStaticDisagreementGateCounts"]["blocked"] == 0
    assert evidence["providerStaticDisagreementGateCounts"]["autoSafeWarning"] == 1
    assert "provider_static_disagreement_needs_review" not in evidence["dedupAuditGate"]["blockers"]
