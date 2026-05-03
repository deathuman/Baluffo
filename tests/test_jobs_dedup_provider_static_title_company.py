from __future__ import annotations

from src.jobs.reporting_dedup_evidence import (
    _provider_static_collision_review_hint,
    build_dedup_evidence,
)


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


def _provider_static_row(**overrides):
    payload = {
        "sourceBundleCount": 2,
        "sourceBundle": [
            {
                "source": "greenhouse:slug:studio-one",
                "sourceJobId": "gh-1",
                "jobLink": "https://provider.example/jobs/1",
                "adapter": "greenhouse",
            },
            {
                "source": "static_source::static:listing_url:https://studio.example/careers",
                "sourceJobId": "static-1",
                "jobLink": "https://static.example/jobs/2",
                "adapter": "static",
            },
        ],
    }
    payload.update(overrides)
    return _row(**payload)


def test_dedup_evidence_reports_title_company_collision_examples() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 1},
        [
            _provider_static_row(
                locations=[
                    {"city": "Amsterdam", "country": "NL"},
                    {"city": "Rotterdam", "country": "NL"},
                ],
            )
        ],
    )

    assert evidence["providerStaticDisagreementExamples"][0]["disagreementClassification"] == (
        "title_company_collision"
    )
    assert evidence["providerStaticTitleCompanyCollisionCounts"] == {
        "total": 1,
        "currentRun": 1,
        "carried": 0,
    }
    collision = evidence["providerStaticTitleCompanyCollisionExamples"][0]
    assert collision["title"] == "Senior Engineer"
    assert collision["company"] == "Studio One"
    assert collision["providerSourceJobIds"] == ["gh-1"]
    assert collision["staticSourceJobIds"] == ["static-1"]
    assert collision["distinctLocationCount"] == 2
    assert collision["sampleLocations"] == ["amsterdam, nl", "rotterdam, nl"]
    assert collision["collisionReviewHint"] == "different_locations_same_title_company"
    assert collision["carriedLocationPollutionAudit"] == "not_carried"
    assert collision["carriedLocationPollutionEvidence"] == ["origin:current_run"]


def test_dedup_evidence_tracks_carried_title_company_collision_counts() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _provider_static_row(
                locations=[
                    {"city": "Amsterdam", "country": "NL"},
                    {"city": "Rotterdam", "country": "NL"},
                ],
            )
        ],
        seeded_from_existing_output=True,
    )

    assert evidence["providerStaticTitleCompanyCollisionCounts"] == {
        "total": 1,
        "currentRun": 0,
        "carried": 1,
    }
    assert (
        evidence["providerStaticTitleCompanyCollisionExamples"][0]["bundleEvidenceOrigin"]
        == "carried_from_existing_output"
    )
    assert evidence["providerStaticTitleCompanyCollisionAuditCounts"] == {
        "carried_location_pollution": 0,
        "carried_location_variant": 0,
        "carried_provider_identity_location_conflict": 0,
        "possible_real_multi_location_conflict": 1,
        "not_carried": 0,
        "unknown": 0,
    }
    assert evidence["dedupAuditGate"]["examples"][0]["recommendedReviewAction"] == (
        "review_provider_static_disagreement"
    )


def test_dedup_evidence_excludes_non_title_company_disagreement_from_collision_examples() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 1},
        [
            _provider_static_row(
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

    assert evidence["providerStaticDisagreementCounts"]["total"] == 1
    assert evidence["providerStaticTitleCompanyCollisionCounts"] == {
        "total": 0,
        "currentRun": 0,
        "carried": 0,
    }
    assert evidence["providerStaticTitleCompanyCollisionExamples"] == []


def test_dedup_evidence_audits_carried_location_pollution_from_title_overlap() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _provider_static_row(
                title="Concept Artist / Illustrator",
                locations=[
                    {"city": "Illustrator", "country": ""},
                    {"city": "Salem", "country": "US"},
                ],
            )
        ],
        seeded_from_existing_output=True,
    )

    collision = evidence["providerStaticTitleCompanyCollisionExamples"][0]
    assert collision["carriedLocationPollutionAudit"] == "carried_location_pollution"
    assert "location_token_overlaps_title" in collision["carriedLocationPollutionEvidence"]
    assert "plausible_location_count:1" in collision["carriedLocationPollutionEvidence"]
    assert "polluted_location_count:1" in collision["carriedLocationPollutionEvidence"]
    assert evidence["providerStaticTitleCompanyCollisionAuditCounts"] == {
        "carried_location_pollution": 1,
        "carried_location_variant": 0,
        "carried_provider_identity_location_conflict": 0,
        "possible_real_multi_location_conflict": 0,
        "not_carried": 0,
        "unknown": 0,
    }


def test_dedup_evidence_audits_carried_location_pollution_from_repeated_company_token() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _provider_static_row(
                dedupKey="key-1",
                title="3D Character Artist",
                locations=[
                    {"city": "Illustrator", "country": ""},
                    {"city": "Salem", "country": "US"},
                ],
            ),
            _provider_static_row(
                dedupKey="key-2",
                title="Lead Animator",
                locations=[
                    {"city": "Illustrator", "country": ""},
                    {"city": "Salem", "country": "US"},
                ],
            ),
            _provider_static_row(
                dedupKey="key-3",
                title="Unreal Engine Programmer",
                locations=[
                    {"city": "Illustrator", "country": ""},
                    {"city": "Salem", "country": "US"},
                ],
            ),
        ],
        seeded_from_existing_output=True,
    )

    rows = evidence["providerStaticTitleCompanyCollisionExamples"]
    assert len(rows) == 3
    assert all(row["carriedLocationPollutionAudit"] == "carried_location_pollution" for row in rows)
    assert any(
        "repeated_company_location_token:illustrator" in row["carriedLocationPollutionEvidence"]
        for row in rows
    )
    assert evidence["providerStaticTitleCompanyCollisionAuditCounts"] == {
        "carried_location_pollution": 3,
        "carried_location_variant": 0,
        "carried_provider_identity_location_conflict": 0,
        "possible_real_multi_location_conflict": 0,
        "not_carried": 0,
        "unknown": 0,
    }


def test_dedup_evidence_audits_carried_real_multi_location_conflict() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _provider_static_row(
                locations=[
                    {"city": "Amsterdam", "country": "NL"},
                    {"city": "Rotterdam", "country": "NL"},
                ],
            )
        ],
        seeded_from_existing_output=True,
    )

    collision = evidence["providerStaticTitleCompanyCollisionExamples"][0]
    assert collision["carriedLocationPollutionAudit"] == "possible_real_multi_location_conflict"
    assert "plausible_location_count:2" in collision["carriedLocationPollutionEvidence"]


def test_dedup_evidence_warns_on_carried_location_variants_with_shared_evidence() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            _provider_static_row(
                sourceBundle=[
                    {
                        "source": "smartrecruiters:studio-one",
                        "sourceJobId": "sr-7440001",
                        "jobLink": "https://jobs.smartrecruiters.com/StudioOne/7440001",
                        "adapter": "smartrecruiters",
                    },
                    {
                        "source": "static_source::static:listing_url:https://studio.example/jobs",
                        "sourceJobId": "static-1",
                        "jobLink": "https://jobs.smartrecruiters.com/StudioOne/7440001-title",
                        "adapter": "static",
                    },
                ],
                locations=[
                    {"city": "Warszawa", "country": "PL"},
                    {"city": "Warszawa", "country": "Poland"},
                ],
            )
        ],
        seeded_from_existing_output=True,
    )

    collision = evidence["providerStaticTitleCompanyCollisionExamples"][0]
    assert collision["carriedLocationPollutionAudit"] == "carried_location_variant"
    assert collision["disagreementGateDisposition"] == "warning"
    assert "carried_location_variant" in collision["disagreementGateEvidence"]
    assert evidence["providerStaticDisagreementGateCounts"]["blocked"] == 0
    assert evidence["providerStaticDisagreementGateCounts"]["warning"] == 1
    assert evidence["providerStaticTitleCompanyCollisionAuditCounts"] == {
        "carried_location_pollution": 0,
        "carried_location_variant": 1,
        "carried_provider_identity_location_conflict": 0,
        "possible_real_multi_location_conflict": 0,
        "not_carried": 0,
        "unknown": 0,
    }


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
