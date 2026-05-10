from __future__ import annotations

from src.jobs.reporting_dedup_evidence import build_dedup_evidence
from tests.test_jobs_dedup_provider_static_disagreement import _row


def test_dedup_evidence_downgrades_smartrecruiters_title_location_alias() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 1},
        [
            _row(
                company="People can Fly Studio",
                title="Artiste Technique Senior / Senior Technical Artist",
                locations=[{"city": "Montréal", "country": "CA"}],
                sourceBundleCount=4,
                sourceBundle=[
                    {
                        "source": "smartrecruiters_sources",
                        "sourceJobId": "smartrecruiters:PeopleCanFly:744000106807696",
                        "jobLink": "https://jobs.smartrecruiters.com/PeopleCanFly/744000106807696",
                        "adapter": "smartrecruiters",
                    },
                    {
                        "source": "smartrecruiters_sources",
                        "sourceJobId": "smartrecruiters:PeopleCanFly:744000082413749",
                        "jobLink": "https://jobs.smartrecruiters.com/PeopleCanFly/744000082413749",
                        "adapter": "smartrecruiters",
                    },
                    {
                        "source": (
                            "static_source::static:listing_url:"
                            "https://careers.smartrecruiters.com/peoplecanfly"
                        ),
                        "sourceJobId": "static-1",
                        "jobLink": (
                            "https://jobs.smartrecruiters.com/PeopleCanFly/"
                            "744000106807696-artiste-technique-senior-senior-technical-artist"
                        ),
                        "adapter": "static",
                    },
                    {
                        "source": (
                            "static_source::static:listing_url:"
                            "https://careers.smartrecruiters.com/peoplecanfly"
                        ),
                        "sourceJobId": "static-2",
                        "jobLink": (
                            "https://jobs.smartrecruiters.com/PeopleCanFly/"
                            "744000082413749-artiste-technique-senior-senior-technical-artist"
                        ),
                        "adapter": "static",
                    },
                ],
            )
        ],
    )

    row = evidence["providerStaticDisagreementExamples"][0]
    assert (
        "smartrecruiters_same_board_title_location_alias"
        in row["disagreementClassificationEvidence"]
    )
    assert row["disagreementGateDisposition"] == "warning"
    assert (
        "auto_safe_current_smartrecruiters_title_location_alias" in row["disagreementGateEvidence"]
    )
    assert row["operatorReviewRecommendation"] == "safe_duplicate"
    assert evidence["providerStaticDisagreementGateCounts"]["blocked"] == 0
    assert evidence["providerStaticDisagreementGateCounts"]["autoSafeWarning"] == 1
