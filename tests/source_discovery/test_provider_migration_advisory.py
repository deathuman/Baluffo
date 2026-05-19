from __future__ import annotations

import pytest

from src.source_discovery.candidate_review import (
    build_candidate_review_payload,
    enrich_candidates_for_review,
)
from src.source_discovery.provider_migration_advisory import (
    build_provider_migration_payload,
    enrich_provider_migration_metadata,
    provider_staging_decision_for_advisory,
    stage_provider_candidates_from_advisories,
    stage_provider_candidates_with_diagnostics,
)


def test_provider_migration_advisory_recommends_supported_provider_source() -> None:
    row = enrich_provider_migration_metadata(
        {
            "name": "Static Studio",
            "adapter": "static",
            "pages": ["https://studio.example/careers"],
            "atsLinks": ["https://boards.greenhouse.io/staticstudio"],
            "jobsFound": 3,
        }
    )

    assert row["detectedProviderFamily"] == "greenhouse"
    assert row["detectedProviderId"] == "staticstudio"
    assert row["recommendedAction"] == "add_provider_source"
    assert "provider_url_evidence" in row["migrationReasons"]


def test_provider_migration_advisory_marks_existing_provider_coverage() -> None:
    rows = enrich_candidates_for_review(
        [
            {
                "name": "Static Studio",
                "adapter": "static",
                "pages": ["https://studio.example/careers"],
                "atsLinks": ["https://boards.greenhouse.io/staticstudio"],
                "jobsFound": 2,
            }
        ],
        active_rows=[{"adapter": "greenhouse", "slug": "staticstudio"}],
    )

    assert rows[0]["recommendedAction"] == "already_covered_by_provider"
    assert rows[0]["existingProviderSourceState"] == "active"
    assert rows[0]["existingProviderSourceId"] == "greenhouse:slug:staticstudio"


def test_provider_migration_advisory_classifies_unsupported_provider_evidence() -> None:
    row = enrich_provider_migration_metadata(
        {
            "name": "Jobvite Studio",
            "adapter": "static",
            "atsLinks": ["https://jobs.jobvite.com/jobvitestudio"],
        }
    )

    assert row["detectedProviderFamily"] == "jobvite"
    assert row["recommendedAction"] == "unsupported_provider"
    assert "unsupported_provider_evidence" in row["migrationReasons"]


def test_provider_migration_advisory_stages_safe_oracle_hcm_jobs_page() -> None:
    url = "https://example.fa.ocs.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/jobs"
    row = enrich_provider_migration_metadata(
        {
            "name": "Oracle Studio",
            "adapter": "static",
            "listing_url": url,
        }
    )

    assert row["detectedProviderFamily"] == "oracle_hcm"
    assert row["detectedProviderId"] == "/hcmUI/CandidateExperience/en/sites/CX_1/jobs"
    assert row["recommendedAction"] == "add_provider_source"

    staged, diagnostic = provider_staging_decision_for_advisory(
        {
            "name": "Oracle Studio",
            "adapter": "static",
            "listing_url": url,
            "jobsFound": 1,
        },
        at="2026-05-19T12:00:00+00:00",
    )

    assert diagnostic["providerStagingDecision"] == "staged"
    assert staged["adapter"] == "oracle_hcm"
    assert staged["listing_url"] == url
    assert staged["base_url"] == "https://example.fa.ocs.oraclecloud.com"
    assert staged["site_path"] == "/hcmUI/CandidateExperience/en/sites/CX_1/jobs"


def test_provider_migration_advisory_keeps_unsafe_oracle_hcm_evidence_unsupported() -> None:
    row = enrich_provider_migration_metadata(
        {
            "name": "Oracle Detail",
            "adapter": "static",
            "atsLinks": [
                "https://example.fa.ocs.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/job/123"
            ],
        }
    )

    assert row["detectedProviderFamily"] == "oracle_hcm"
    assert row["recommendedAction"] == "unsupported_provider"
    assert "unsupported_provider_evidence" in row["migrationReasons"]


@pytest.mark.parametrize(
    ("url", "family"),
    [
        (
            "https://jobs.jobvite.com/jobvitestudio",
            "jobvite",
        ),
        (
            "https://careers-example.icims.com/jobs/search",
            "icims",
        ),
        (
            "https://career4.successfactors.com/career?company=example",
            "successfactors",
        ),
        (
            "https://example.csod.com/ux/ats/careersite/1/home",
            "cornerstone_csod",
        ),
        (
            "https://example.homerun.co/jobs",
            "homerun",
        ),
        (
            "https://hrmos.co/pages/example/jobs",
            "hrmos",
        ),
    ],
)
def test_provider_migration_advisory_classifies_unsupported_ats_families(
    url: str, family: str
) -> None:
    row = enrich_provider_migration_metadata(
        {
            "name": f"{family} Studio",
            "adapter": "static",
            "atsLinks": [url],
        }
    )

    assert row["detectedProviderFamily"] == family
    assert row["recommendedAction"] == "unsupported_provider"
    assert "unsupported_provider_evidence" in row["migrationReasons"]


def test_provider_migration_advisory_keeps_static_or_insufficient_rows() -> None:
    productive = enrich_provider_migration_metadata(
        {"name": "Productive Static", "adapter": "static", "jobsFound": 4}
    )
    empty = enrich_provider_migration_metadata(
        {"name": "Empty Static", "adapter": "static", "jobsFound": 0}
    )

    assert productive["recommendedAction"] == "keep_static"
    assert empty["recommendedAction"] == "insufficient_evidence"


def test_provider_migration_payload_is_nested_under_candidate_review() -> None:
    enriched = enrich_candidates_for_review(
        [
            {
                "name": "Static Studio",
                "adapter": "static",
                "atsLinks": ["https://boards.greenhouse.io/staticstudio"],
                "jobsFound": 2,
                "score": 70,
            },
            {
                "name": "Jobvite Studio",
                "adapter": "static",
                "atsLinks": ["https://jobs.jobvite.com/jobvitestudio"],
            },
        ],
        pending_rows=[{"adapter": "greenhouse", "slug": "staticstudio"}],
    )
    migration = build_provider_migration_payload(enriched)
    review = build_candidate_review_payload(enriched)

    assert migration["actionCounts"]["already_covered_by_provider"] == 1
    assert migration["actionCounts"]["unsupported_provider"] == 1
    assert migration["alreadyCoveredByProvider"][0]["existingProviderSourceState"] == "pending"
    assert (
        review["providerMigration"]["unsupportedProviderCandidates"][0]["name"] == "Jobvite Studio"
    )


def test_provider_migration_payload_tracks_staged_provider_candidates() -> None:
    staged, diagnostic = provider_staging_decision_for_advisory(
        {
            "name": "Static Studio",
            "adapter": "static",
            "atsLinks": ["https://boards.greenhouse.io/staticstudio"],
            "jobsFound": 3,
        },
        at="2026-04-30T12:00:00+00:00",
    )

    assert diagnostic["providerStagingDecision"] == "staged"
    migration = build_provider_migration_payload([staged])

    assert migration["stagedProviderCount"] == 1
    assert migration["stagedProviderCandidates"][0]["name"] == "Static Studio (Greenhouse)"
    assert migration["stagedProviderCandidates"][0]["createdFromAdvisory"] is True


def test_stage_provider_candidate_keeps_discovery_and_registry_state_separate() -> None:
    staged, diagnostic = provider_staging_decision_for_advisory(
        {
            "name": "Static Studio",
            "adapter": "static",
            "pages": ["https://studio.example/careers"],
            "atsLinks": ["https://boards.greenhouse.io/staticstudio"],
            "jobsFound": 3,
        },
        at="2026-04-30T12:00:00+00:00",
    )

    assert diagnostic["providerStagingDecision"] == "staged"
    assert staged["adapter"] == "greenhouse"
    assert staged["slug"] == "staticstudio"
    assert staged["candidateState"] == "staged_provider_candidate"
    assert staged["createdFromAdvisory"] is True
    assert staged["migrationSourceIdentity"] == "static:name:static studio"
    assert staged["jobsFound"] == 0
    assert "registryState" not in staged
    assert "pendingReason" not in staged
    assert "stateChangedAt" not in staged
    assert "stateChangedBy" not in staged


def test_stage_provider_candidate_supports_safe_workday_listing_url() -> None:
    staged, diagnostic = provider_staging_decision_for_advisory(
        {
            "name": "Workday Static Studio",
            "adapter": "static",
            "pages": ["https://studio.example/careers"],
            "atsLinks": ["https://tencent.wd1.myworkdayjobs.com/en-US/timi_careers?q=game"],
            "jobsFound": 3,
        },
        at="2026-04-30T12:00:00+00:00",
    )

    assert diagnostic["providerStagingDecision"] == "staged"
    assert staged["adapter"] == "workday"
    assert staged["listing_url"] == (
        "https://tencent.wd1.myworkdayjobs.com/en-US/timi_careers?q=game"
    )
    assert staged["company"] == "Workday Static Studio"
    assert staged["migrationSourceIdentity"] == "static:name:workday static studio"


def test_stage_provider_candidate_blocks_unsafe_workday_root_url() -> None:
    candidate, diagnostic = provider_staging_decision_for_advisory(
        {
            "name": "Unsafe Workday",
            "adapter": "static",
            "detectedProviderFamily": "workday",
            "detectedProviderUrl": "https://tencent.wd1.myworkdayjobs.com/",
            "jobsFound": 3,
        },
        at="2026-04-30T12:00:00+00:00",
    )

    assert candidate == {}
    assert diagnostic["recommendedAction"] == "review_provider_migration"
    assert "provider_row_build_failure" in diagnostic["providerStagingBlockers"]


def test_stage_provider_candidates_blocks_active_pending_and_weak_actions() -> None:
    strong = {
        "name": "Static Studio",
        "adapter": "static",
        "atsLinks": ["https://boards.greenhouse.io/staticstudio"],
        "jobsFound": 2,
    }
    unsupported = {
        "name": "Unsupported",
        "adapter": "static",
        "atsLinks": ["https://jobs.jobvite.com/unsupported"],
    }
    weak = {"name": "Weak Static", "adapter": "static", "jobsFound": 0}

    assert (
        stage_provider_candidates_from_advisories(
            [strong],
            active_rows=[{"adapter": "greenhouse", "slug": "staticstudio"}],
            at="2026-04-30T12:00:00+00:00",
        )
        == []
    )
    assert (
        stage_provider_candidates_from_advisories(
            [strong],
            pending_rows=[{"adapter": "greenhouse", "slug": "staticstudio"}],
            at="2026-04-30T12:00:00+00:00",
        )
        == []
    )
    assert (
        stage_provider_candidates_from_advisories(
            [unsupported, weak],
            at="2026-04-30T12:00:00+00:00",
        )
        == []
    )


def test_provider_staging_diagnostics_explain_active_and_pending_duplicates() -> None:
    strong = {
        "name": "Static Studio",
        "adapter": "static",
        "atsLinks": ["https://boards.greenhouse.io/staticstudio"],
        "jobsFound": 2,
    }

    active = stage_provider_candidates_with_diagnostics(
        [strong],
        active_rows=[{"adapter": "greenhouse", "slug": "staticstudio"}],
        at="2026-04-30T12:00:00+00:00",
    )
    pending = stage_provider_candidates_with_diagnostics(
        [strong],
        pending_rows=[{"adapter": "greenhouse", "slug": "staticstudio"}],
        at="2026-04-30T12:00:00+00:00",
    )

    assert active["staged"] == []
    assert active["diagnostics"][0]["providerStagingDecision"] == "skipped"
    assert "existing_provider" in active["diagnostics"][0]["providerStagingBlockers"]
    assert pending["staged"] == []
    assert "existing_provider" in pending["diagnostics"][0]["providerStagingBlockers"]


def test_provider_staging_reports_provider_row_build_failure() -> None:
    candidate, diagnostic = provider_staging_decision_for_advisory(
        {
            "name": "Broken Provider",
            "adapter": "static",
            "detectedProviderFamily": "greenhouse",
            "detectedProviderUrl": "https://boards.greenhouse.io/",
            "slug": "broken-provider",
        },
        at="2026-04-30T12:00:00+00:00",
    )

    assert candidate == {}
    assert diagnostic["recommendedAction"] == "add_provider_source"
    assert "provider_row_build_failure" in diagnostic["providerStagingBlockers"]


def test_provider_staging_allows_static_like_generic_static_evidence() -> None:
    staged = stage_provider_candidates_from_advisories(
        [
            {
                "name": "Generic Static Studio",
                "adapter": "generic_static",
                "discoveryStage": "generic_static",
                "atsLinks": ["https://boards.greenhouse.io/genericstaticstudio"],
                "jobsFound": 3,
            }
        ],
        at="2026-04-30T12:00:00+00:00",
    )

    assert len(staged) == 1
    assert staged[0]["adapter"] == "greenhouse"
    assert staged[0]["slug"] == "genericstaticstudio"


def test_provider_staging_reports_adapter_mismatch_for_provider_rows() -> None:
    candidate, diagnostic = provider_staging_decision_for_advisory(
        {
            "name": "Provider Row",
            "adapter": "greenhouse",
            "api_url": "https://boards-api.greenhouse.io/v1/boards/providerrow/jobs?content=true",
        },
        at="2026-04-30T12:00:00+00:00",
    )

    assert candidate == {}
    assert diagnostic["recommendedAction"] == "add_provider_source"
    assert "adapter_mismatch" in diagnostic["providerStagingBlockers"]


def test_provider_staging_original_static_identity_does_not_block_provider_identity() -> None:
    result = stage_provider_candidates_with_diagnostics(
        [
            {
                "id": "greenhouse:slug:staticstudio",
                "name": "Static Studio",
                "adapter": "static",
                "atsLinks": ["https://boards.greenhouse.io/staticstudio"],
                "jobsFound": 2,
            }
        ],
        at="2026-04-30T12:00:00+00:00",
    )

    assert len(result["staged"]) == 1
    assert "identity_collision" not in result["diagnostics"][0]["providerStagingBlockers"]


def test_provider_migration_payload_includes_staging_blocker_counts() -> None:
    payload = build_provider_migration_payload(
        [
            {
                "name": "Provider Row",
                "adapter": "greenhouse",
                "api_url": "https://boards-api.greenhouse.io/v1/boards/providerrow/jobs?content=true",
            }
        ]
    )

    assert payload["stagingSkippedCount"] == 1
    assert payload["stagingBlockedByAdapterMismatchCount"] == 1
    assert payload["stagingBlockerCounts"]["adapter_mismatch"] == 1
    assert payload["stagingBlockerExamples"][0]["providerStagingDecision"] == "skipped"
