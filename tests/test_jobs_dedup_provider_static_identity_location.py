from __future__ import annotations

from src.jobs.reporting_dedup_evidence import build_dedup_evidence


def test_dedup_evidence_warns_on_carried_provider_identity_location_conflicts() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            {
                "id": "job-1",
                "dedupKey": "key-1",
                "title": "Concept Artist / Illustrator",
                "company": "Epoch Games",
                "jobLink": "https://jobs.smartrecruiters.com/EpochGames/744000018988355",
                "locationSummary": "Illustrator; Salem, US; Winston-Salem, US",
                "sourceBundleCount": 2,
                "sourceBundle": [
                    {
                        "source": "smartrecruiters:epoch",
                        "sourceJobId": "smartrecruiters:EpochGames:744000018988355",
                        "jobLink": "https://jobs.smartrecruiters.com/EpochGames/744000018988355",
                        "adapter": "smartrecruiters",
                    },
                    {
                        "source": "static_source::static:listing_url:https://careers.smartrecruiters.com/epochgames",
                        "sourceJobId": "static:static:listing_url:https://careers.smartrecruiters.com/epochgames:cab575a102",
                        "jobLink": "https://jobs.smartrecruiters.com/EpochGames/744000018988355-3d-character-artist",
                        "adapter": "static",
                    },
                ],
                "locations": [
                    {"city": "Illustrator", "country": ""},
                    {"city": "Salem", "country": "US"},
                    {"city": "Winston-Salem", "country": "US"},
                ],
            }
        ],
        seeded_from_existing_output=True,
    )

    collision = evidence["providerStaticTitleCompanyCollisionExamples"][0]
    assert (
        collision["carriedLocationPollutionAudit"] == "carried_provider_identity_location_conflict"
    )
    assert collision["disagreementGateDisposition"] == "warning"
    assert "carried_provider_identity_location_conflict" in collision["disagreementGateEvidence"]
    assert evidence["providerStaticDisagreementGateCounts"]["blocked"] == 0
    assert evidence["providerStaticDisagreementGateCounts"]["warning"] == 1
    assert evidence["providerStaticTitleCompanyCollisionAuditCounts"] == {
        "carried_location_pollution": 0,
        "carried_location_variant": 0,
        "carried_provider_identity_location_conflict": 1,
        "possible_real_multi_location_conflict": 0,
        "not_carried": 0,
        "unknown": 0,
    }
