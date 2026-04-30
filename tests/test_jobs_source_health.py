from src.jobs.common.contracts_source_health import derive_source_health


def test_derive_source_health_ranks_mixed_source_rows() -> None:
    health = derive_source_health(
        [
            {
                "name": "provider_good",
                "adapter": "greenhouse",
                "status": "ok",
                "fetchedCount": 12,
                "keptCount": 10,
                "durationMs": 900,
            },
            {
                "name": "static_zero_review",
                "adapter": "static",
                "status": "ok",
                "fetchedCount": 4,
                "keptCount": 0,
                "durationMs": 45000,
                "failureBucket": "needs_review",
                "classification": "needs_review",
                "browserFallbackRecommended": True,
            },
            {
                "name": "static_empty",
                "adapter": "static",
                "status": "ok",
                "fetchedCount": 0,
                "keptCount": 0,
                "durationMs": 200,
                "failureBucket": "no_openings",
                "zeroKeptClassification": "legit_empty",
            },
            {
                "name": "blocked_source",
                "adapter": "static",
                "status": "error",
                "fetchedCount": 0,
                "keptCount": 0,
                "durationMs": 12000,
                "failureBucket": "blocked_or_challenge",
                "classification": "blocked_or_challenge",
                "error": "HTTP 403",
            },
            {
                "name": "cache_skipped",
                "adapter": "static",
                "status": "excluded",
                "keptCount": 0,
                "exclusionReason": "cache_skip",
            },
            {
                "name": "static_source::covered",
                "adapter": "static",
                "status": "excluded",
                "keptCount": 0,
                "exclusionReason": "dynamic_redundant_provider",
                "coveredByProviderSourceId": "Studio Greenhouse",
                "coveredByProviderAdapter": "greenhouse",
                "providerCoverageStatus": "validated_provider",
                "providerCoverageConsecutiveSuccesses": 2,
            },
        ]
    )

    assert health["totalSources"] == 6
    assert health["okSources"] == 3
    assert health["failedSources"] == 1
    assert health["excludedSources"] == 2
    assert health["skippedSources"] == 2
    assert health["dynamicRedundantStaticSources"] == 1
    assert health["dynamicRedundantStatic"][0]["coveredByProviderSourceId"] == "Studio Greenhouse"
    assert health["zeroKeptSources"] == 3
    assert health["zeroKeptNeedsReviewSources"] == 2
    assert health["browserFallbackRecommendedSources"] == 1
    assert health["slowestSources"][0]["name"] == "static_zero_review"
    assert health["topProductiveSources"][0]["name"] == "provider_good"
    assert [row["name"] for row in health["zeroKeptNeedsReview"]] == [
        "static_zero_review",
        "blocked_source",
    ]
    assert health["topFailureBuckets"][0]["key"] == "needs_review"
    assert any(row["key"] == "blocked_or_challenge" for row in health["topClassifications"])
