from __future__ import annotations

from src.source_discovery.candidate_review import (
    build_candidate_review_payload,
    enrich_candidate_review_metadata,
)


def test_candidate_review_derives_rank_reasons_and_promote_recommendation() -> None:
    row = enrich_candidate_review_metadata(
        {"name": "Live Studio", "adapter": "greenhouse", "jobsFound": 4, "score": 82}
    )

    assert row["rankScore"] == 82
    assert row["providerDetected"] is True
    assert row["providerFamily"] == "greenhouse"
    assert row["promotionRecommendation"] == "promote_candidate"
    assert "jobs_found" in row["rankReasons"]
    assert "provider_detected" in row["rankReasons"]


def test_candidate_review_classifies_duplicates_against_active_and_pending() -> None:
    active = [{"adapter": "greenhouse", "api_url": "https://boards.example/api"}]
    pending = [{"adapter": "lever", "api_url": "https://lever.example/api"}]

    active_row = enrich_candidate_review_metadata(
        {"adapter": "greenhouse", "api_url": "https://boards.example/api", "jobsFound": 3},
        active_ids={active[0]["adapter"] + ":api_url:" + active[0]["api_url"]},
    )
    pending_row = enrich_candidate_review_metadata(
        {"adapter": "lever", "api_url": "https://lever.example/api", "jobsFound": 3},
        pending_ids={pending[0]["adapter"] + ":api_url:" + pending[0]["api_url"]},
    )

    assert active_row["duplicateOfActiveSource"] is True
    assert active_row["promotionRecommendation"] == "duplicate_candidate"
    assert pending_row["duplicateOfPendingSource"] is True
    assert pending_row["promotionRecommendation"] == "duplicate_candidate"


def test_candidate_review_classifies_zero_job_noise_hidden_and_browser_probe() -> None:
    noise = enrich_candidate_review_metadata(
        {"name": "Noise", "adapter": "static", "jobsFound": 0, "score": 10, "dropReason": "weak"}
    )
    hidden = enrich_candidate_review_metadata(
        {"name": "Hidden", "adapter": "static", "jobsFound": 0, "hiddenFromDefault": True}
    )
    browser = enrich_candidate_review_metadata(
        {"name": "Blocked", "adapter": "static", "jobsFound": 0, "lastProbeError": "HTTP 403"}
    )

    assert noise["promotionRecommendation"] == "reject_candidate"
    assert hidden["promotionRecommendation"] == "hide_pending"
    assert browser["promotionRecommendation"] == "needs_browser_probe"
    assert browser["browserFallbackRecommended"] is True


def test_candidate_review_payload_builds_compact_review_lanes() -> None:
    payload = build_candidate_review_payload(
        [
            {"name": "Live", "adapter": "greenhouse", "jobsFound": 8, "score": 90},
            {"name": "Blocked", "adapter": "static", "lastProbeError": "Cloudflare challenge"},
            {"name": "Hidden", "adapter": "static", "hiddenFromDefault": True},
        ]
    )

    assert payload["totalCandidates"] == 3
    assert payload["recommendationCounts"]["promote_candidate"] == 1
    assert payload["recommendationCounts"]["needs_browser_probe"] == 1
    assert payload["topCandidates"][0]["name"] == "Live"
    assert payload["providerBackedCandidates"][0]["providerFamily"] == "greenhouse"
    assert payload["needsBrowserProbeCandidates"][0]["name"] == "Blocked"
    assert payload["hiddenOrDeferredCandidates"][0]["name"] == "Hidden"


def test_candidate_review_payload_includes_provider_staging_diagnostics() -> None:
    payload = build_candidate_review_payload(
        [
            {
                "name": "Static Studio",
                "adapter": "static",
                "atsLinks": ["https://boards.greenhouse.io/staticstudio"],
                "jobsFound": 3,
            },
            {
                "name": "Provider Row",
                "adapter": "greenhouse",
                "api_url": "https://boards-api.greenhouse.io/v1/boards/providerrow/jobs?content=true",
            },
        ],
        at="2026-04-30T12:00:00+00:00",
    )

    migration = payload["providerMigration"]
    assert migration["stageableProviderCandidateCount"] == 1
    assert migration["stagedProviderCandidateCount"] == 1
    assert migration["stagingSkippedCount"] == 1
    assert migration["stagingBlockedByAdapterMismatchCount"] == 1
    assert migration["stagedProviderCandidates"][0]["providerStagingDecision"] == "staged"
