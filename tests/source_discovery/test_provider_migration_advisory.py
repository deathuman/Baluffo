from __future__ import annotations

from src.source_discovery.candidate_review import (
    build_candidate_review_payload,
    enrich_candidates_for_review,
)
from src.source_discovery.provider_migration_advisory import (
    build_provider_migration_payload,
    enrich_provider_migration_metadata,
    stage_provider_candidates_from_advisories,
    staged_provider_candidate_from_advisory,
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
    staged = staged_provider_candidate_from_advisory(
        {
            "name": "Static Studio",
            "adapter": "static",
            "atsLinks": ["https://boards.greenhouse.io/staticstudio"],
            "jobsFound": 3,
        },
        at="2026-04-30T12:00:00+00:00",
    )

    migration = build_provider_migration_payload([staged])

    assert migration["stagedProviderCount"] == 1
    assert migration["stagedProviderCandidates"][0]["name"] == "Static Studio (Greenhouse)"
    assert migration["stagedProviderCandidates"][0]["createdFromAdvisory"] is True


def test_stage_provider_candidate_keeps_discovery_and_registry_state_separate() -> None:
    staged = staged_provider_candidate_from_advisory(
        {
            "name": "Static Studio",
            "adapter": "static",
            "pages": ["https://studio.example/careers"],
            "atsLinks": ["https://boards.greenhouse.io/staticstudio"],
            "jobsFound": 3,
        },
        at="2026-04-30T12:00:00+00:00",
    )

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
