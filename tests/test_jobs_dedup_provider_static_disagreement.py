from __future__ import annotations

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


def test_dedup_evidence_reports_provider_static_disagreement_examples() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 1},
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
    )

    assert evidence["providerStaticDisagreementCounts"] == {
        "total": 1,
        "currentRun": 1,
        "carried": 0,
    }
    disagreement = evidence["providerStaticDisagreementExamples"][0]
    assert disagreement["title"] == "Senior Engineer"
    assert disagreement["company"] == "Studio One"
    assert disagreement["providerSources"] == ["greenhouse:slug:studio-one"]
    assert disagreement["staticSources"] == [
        "static_source::static:listing_url:https://studio.example/careers"
    ]
    assert disagreement["providerSourceJobIds"] == ["gh-1"]
    assert disagreement["staticSourceJobIds"] == ["static-1"]
    assert disagreement["providerUrls"] == ["https://provider.example/jobs/1"]
    assert disagreement["staticUrls"] == ["https://static.example/jobs/1"]
    assert "shared_primary_url:false" in disagreement["disagreementEvidence"]


def test_dedup_evidence_tracks_carried_provider_static_disagreement() -> None:
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
    )

    assert evidence["providerStaticDisagreementCounts"] == {
        "total": 1,
        "currentRun": 0,
        "carried": 1,
    }
    assert evidence["providerStaticDisagreementExamples"][0]["bundleEvidenceOrigin"] == (
        "carried_from_existing_output"
    )


def test_dedup_evidence_ignores_non_provider_static_mixed_bundle_for_disagreement() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 1},
        [
            _row(
                sourceBundleCount=2,
                sourceBundle=[
                    {
                        "source": "google_sheets",
                        "sourceJobId": "sheet-1",
                        "jobLink": "https://sheet.example/jobs/1",
                        "adapter": "google_sheets",
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
    )

    assert evidence["providerStaticDisagreementCounts"]["total"] == 0
    assert evidence["providerStaticDisagreementExamples"] == []
