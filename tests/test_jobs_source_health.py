from src.jobs.common.contracts_source_health import (
    derive_source_health,
    normalize_source_health_payload,
)


def test_derive_source_health_ranks_mixed_source_rows() -> None:
    source_rows = [
        {
            "name": "provider_good",
            "adapter": "greenhouse",
            "status": "ok",
            "fetchedCount": 12,
            "keptCount": 10,
            "durationMs": 900,
            "lastStatus": "ok",
            "lastRunAt": "2026-05-04T10:00:00+00:00",
            "lastCheckedAt": "2026-05-04T10:00:00+00:00",
            "lastSuccessAt": "2026-05-04T10:00:00+00:00",
            "lastSuccessfulFetchAt": "2026-05-04T10:00:00+00:00",
            "lastSeenInFetchAt": "2026-05-04T10:00:00+00:00",
            "lastKeptCount": 10,
            "lastJobsKept": 10,
            "failureCount": 0,
            "consecutiveFailures": 0,
            "zeroJobStreak": 0,
            "consecutiveZeroKept": 0,
            "healthScore": 100,
            "health": "healthy",
            "healthReason": "last fetch kept jobs",
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
            "lastStatus": "ok",
            "lastRunAt": "2026-05-04T10:05:00+00:00",
            "lastCheckedAt": "2026-05-04T10:05:00+00:00",
            "lastSuccessAt": "2026-05-04T10:05:00+00:00",
            "lastSuccessfulFetchAt": "2026-05-04T10:05:00+00:00",
            "lastSeenInFetchAt": "2026-05-04T10:05:00+00:00",
            "lastKeptCount": 0,
            "lastJobsKept": 0,
            "failureCount": 0,
            "consecutiveFailures": 0,
            "zeroJobStreak": 1,
            "consecutiveZeroKept": 1,
            "healthScore": 88,
            "health": "warning",
            "healthReason": "latest fetch kept no jobs",
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
            "lastStatus": "ok",
            "lastRunAt": "2026-05-04T10:10:00+00:00",
            "lastCheckedAt": "2026-05-04T10:10:00+00:00",
            "lastSuccessAt": "2026-05-04T10:10:00+00:00",
            "lastSuccessfulFetchAt": "2026-05-04T10:10:00+00:00",
            "lastSeenInFetchAt": "2026-05-04T10:10:00+00:00",
            "lastKeptCount": 0,
            "lastJobsKept": 0,
            "failureCount": 0,
            "consecutiveFailures": 0,
            "zeroJobStreak": 0,
            "consecutiveZeroKept": 0,
            "healthScore": 100,
            "health": "warning",
            "healthReason": "latest fetch kept no jobs",
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
            "lastStatus": "error",
            "lastRunAt": "2026-05-04T10:15:00+00:00",
            "lastCheckedAt": "2026-05-04T10:15:00+00:00",
            "lastSuccessAt": "2026-05-04T09:59:00+00:00",
            "lastSuccessfulFetchAt": "2026-05-04T09:59:00+00:00",
            "lastSeenInFetchAt": "2026-05-04T10:15:00+00:00",
            "lastKeptCount": 0,
            "lastJobsKept": 0,
            "failureCount": 2,
            "consecutiveFailures": 2,
            "zeroJobStreak": 0,
            "consecutiveZeroKept": 0,
            "healthScore": 42,
            "health": "broken",
            "healthReason": "latest fetch failed",
        },
        {
            "name": "cache_skipped",
            "adapter": "static",
            "status": "excluded",
            "keptCount": 0,
            "exclusionReason": "cache_skip",
            "lastStatus": "excluded",
            "lastRunAt": "2026-05-04T10:20:00+00:00",
            "lastCheckedAt": "2026-05-04T10:20:00+00:00",
            "lastSuccessAt": "2026-05-04T10:20:00+00:00",
            "lastSuccessfulFetchAt": "2026-05-04T10:20:00+00:00",
            "lastSeenInFetchAt": "2026-05-04T10:20:00+00:00",
            "lastKeptCount": 0,
            "lastJobsKept": 0,
            "failureCount": 0,
            "consecutiveFailures": 0,
            "zeroJobStreak": 0,
            "consecutiveZeroKept": 0,
            "healthScore": 100,
            "health": "unknown",
            "healthReason": "excluded",
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
            "lastStatus": "excluded",
            "lastRunAt": "2026-05-04T10:25:00+00:00",
            "lastCheckedAt": "2026-05-04T10:25:00+00:00",
            "lastSuccessAt": "2026-05-04T10:25:00+00:00",
            "lastSuccessfulFetchAt": "2026-05-04T10:25:00+00:00",
            "lastSeenInFetchAt": "2026-05-04T10:25:00+00:00",
            "lastKeptCount": 0,
            "lastJobsKept": 0,
            "failureCount": 0,
            "consecutiveFailures": 0,
            "zeroJobStreak": 0,
            "consecutiveZeroKept": 0,
            "healthScore": 100,
            "health": "unknown",
            "healthReason": "excluded",
        },
    ]

    health = derive_source_health(source_rows)
    normalized = normalize_source_health_payload(health, source_rows)

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
    assert health["topProductiveSources"][0]["lastSuccessfulFetchAt"] == "2026-05-04T10:00:00+00:00"
    assert health["topProductiveSources"][0]["health"] == "healthy"
    assert [row["name"] for row in health["zeroKeptNeedsReview"]] == [
        "static_zero_review",
        "blocked_source",
    ]
    assert health["zeroKeptNeedsReview"][0]["health"] == "warning"
    assert health["zeroKeptNeedsReview"][0]["healthReason"] == "latest fetch kept no jobs"
    assert health["sourcesNeedingAttention"][0]["name"] == "blocked_source"
    assert health["sourcesNeedingAttention"][0]["health"] == "broken"
    assert health["sourcesNeedingAttention"][0]["healthReason"] == "latest fetch failed"
    assert health["topFailureBuckets"][0]["key"] == "needs_review"
    assert any(row["key"] == "blocked_or_challenge" for row in health["topClassifications"])
    assert (
        normalized["sourcesNeedingAttention"][0]["lastSuccessfulFetchAt"]
        == "2026-05-04T09:59:00+00:00"
    )
    assert (
        normalized["sourcesNeedingAttention"][0]["lastSeenInFetchAt"] == "2026-05-04T10:15:00+00:00"
    )
    assert normalized["sourcesNeedingAttention"][0]["failureCount"] == 2
    assert normalized["sourcesNeedingAttention"][0]["zeroJobStreak"] == 0
