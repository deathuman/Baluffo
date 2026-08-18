from typing import Any

from src.bridge.report_normalizer import normalize_fetch_report_contract
from src.jobs.common.contracts_fetch_report import normalize_fetch_report_payload
from src.jobs.common.contracts_source_reports import normalize_source_report_row
from src.jobs.text_utils import clean_text, norm_text
from src.shared.fetch_report_normalization import (
    normalize_bridge_fetch_report_source_row,
    normalize_jobs_fetch_report_source_row_base,
)


def test_bridge_and_jobs_fetch_report_normalizers_share_completed_progress_overlap() -> None:
    payload = {
        "startedAt": "2026-06-17T08:00:00+00:00",
        "finishedAt": "2026-06-17T08:03:00+00:00",
        "summary": {
            "sourceCount": 4,
            "successfulSources": 2,
            "failedSources": 1,
            "excludedSources": 1,
            "outputCount": 12,
        },
    }

    bridge_progress = normalize_fetch_report_contract(payload)["taskProgress"]
    jobs_progress = normalize_fetch_report_payload(payload)["taskProgress"]

    assert bridge_progress["active"] is jobs_progress["active"] is False
    assert bridge_progress["phaseKey"] == jobs_progress["phaseKey"] == "completed"
    assert bridge_progress["phaseLabel"] == jobs_progress["phaseLabel"] == "Completed"
    assert bridge_progress["mode"] == jobs_progress["mode"] == "determinate"
    assert bridge_progress["ratio"] == jobs_progress["ratio"] == 1.0
    for key in (
        "resolvedSources",
        "sourceCount",
        "outputCount",
        "failedSources",
        "excludedSources",
    ):
        assert bridge_progress["counts"][key] == jobs_progress["counts"][key]
    assert {"totalTasks", "queuedTasks", "runningTasks", "completedTasks"} <= set(
        jobs_progress["counts"]
    )
    assert "totalTasks" not in bridge_progress["counts"]


def test_jobs_completed_progress_preserves_summary_source_count_when_lower_than_resolved() -> None:
    payload = {
        "finishedAt": "2026-06-17T08:03:00+00:00",
        "summary": {
            "sourceCount": 1,
            "successfulSources": 2,
            "failedSources": 0,
            "excludedSources": 0,
        },
    }

    bridge_counts = normalize_fetch_report_contract(payload)["taskProgress"]["counts"]
    jobs_counts = normalize_fetch_report_payload(payload)["taskProgress"]["counts"]

    assert bridge_counts["sourceCount"] == 2
    assert jobs_counts["sourceCount"] == 1
    assert jobs_counts["totalTasks"] == 1
    assert jobs_counts["completedTasks"] == 2


def test_bridge_and_jobs_normalizers_preserve_terminal_fetch_failure() -> None:
    payload = {
        "status": "error",
        "runId": "fetch_failed",
        "startedAt": "2026-07-17T08:00:00+00:00",
        "finishedAt": "2026-07-17T08:03:00+00:00",
        "summary": {
            "sourceCount": 2,
            "successfulSources": 2,
            "failedSources": 0,
            "excludedSources": 0,
            "candidateCount": 100,
            "outputCount": 0,
            "error": "availability_identity_preflight_failed",
            "errorCode": "availability_identity_preflight_failed",
        },
        "availabilitySummary": {
            "rejectedRowCount": 3,
            "rejectionReasonCounts": {"conflicting_source_alias_without_public_url": 3},
        },
    }

    bridge = normalize_fetch_report_contract(payload)
    jobs = normalize_fetch_report_payload(payload)

    assert bridge["status"] == jobs["status"] == "error"
    assert bridge["taskProgress"]["phaseKey"] == jobs["taskProgress"]["phaseKey"] == "failed"
    assert bridge["taskProgress"]["active"] is jobs["taskProgress"]["active"] is False
    assert (
        bridge["taskProgress"]["counts"]["errorCode"]
        == jobs["taskProgress"]["counts"]["errorCode"]
        == "availability_identity_preflight_failed"
    )
    assert bridge["availabilitySummary"]["rejectionReasonCounts"] == {
        "conflicting_source_alias_without_public_url": 3
    }
    assert jobs["availabilitySummary"] == bridge["availabilitySummary"]


def test_bridge_fetch_report_keeps_empty_timing_summary_shape() -> None:
    bridge = normalize_fetch_report_contract({})
    jobs = normalize_fetch_report_payload({})

    assert bridge["runtime"]["timingSummary"]["totalDurationMs"] == 0
    assert bridge["runtime"]["timingSummary"]["stageTotalsMs"]["fetchAndParse"] == 0
    assert "timingSummary" not in jobs["runtime"]


def test_bridge_and_jobs_fetch_report_normalizers_share_source_row_base_overlap() -> None:
    payload = {
        "sources": [
            {
                "name": "Source A",
                "status": "OK",
                "adapter": "static",
                "fetchStrategy": "auto",
                "studio": "Studio A",
                "fetchedCount": 12,
                "keptCount": 3,
                "lowConfidenceDropped": 1,
                "error": "",
                "durationMs": 44,
                "lastStatus": "ok",
                "lastRunAt": "2026-06-17T08:00:00+00:00",
                "lastCheckedAt": "2026-06-17T08:01:00+00:00",
                "lastSuccessAt": "2026-06-17T08:02:00+00:00",
                "lastSuccessfulFetchAt": "2026-06-17T08:02:00+00:00",
                "lastSeenInFetchAt": "2026-06-17T08:03:00+00:00",
                "lastKeptCount": 2,
                "lastJobsKept": 2,
                "consecutiveFailures": 1,
                "failureCount": 1,
                "consecutiveZeroKept": 0,
                "zeroJobStreak": 0,
                "healthScore": 75,
                "health": "warning",
                "healthReason": "slow",
            }
        ]
    }

    bridge_row = normalize_fetch_report_contract(payload)["sources"][0]
    jobs_row = normalize_fetch_report_payload(payload)["sources"][0]

    for key in (
        "name",
        "status",
        "adapter",
        "fetchStrategy",
        "studio",
        "fetchedCount",
        "keptCount",
        "lowConfidenceDropped",
        "error",
        "durationMs",
        "lastStatus",
        "lastRunAt",
        "lastCheckedAt",
        "lastSuccessAt",
        "lastSuccessfulFetchAt",
        "lastSeenInFetchAt",
        "lastKeptCount",
        "lastJobsKept",
        "consecutiveFailures",
        "failureCount",
        "consecutiveZeroKept",
        "zeroJobStreak",
        "healthScore",
        "health",
        "healthReason",
    ):
        assert bridge_row[key] == jobs_row[key]


def test_bridge_and_jobs_source_row_defaults_remain_compatible_but_distinct() -> None:
    payload: dict[str, Any] = {"sources": [{}]}

    bridge_row = normalize_fetch_report_contract(payload)["sources"][0]
    jobs_row = normalize_fetch_report_payload(payload)["sources"][0]

    assert bridge_row["status"] == ""
    assert jobs_row["status"] == "error"
    assert bridge_row["adapter"] == ""
    assert jobs_row["adapter"] == "custom"
    assert bridge_row["fetchStrategy"] == ""
    assert jobs_row["fetchStrategy"] == "auto"
    assert bridge_row["healthScore"] == 0
    assert jobs_row["healthScore"] == 100


def test_jobs_source_report_row_uses_shared_base_contract() -> None:
    row = {
        "name": "Source A",
        "status": "OK",
        "adapter": "",
        "fetchStrategy": "",
        "studio": "Studio A",
        "lastRunAt": "2026-06-18T08:00:00+00:00",
        "lastKeptCount": "4",
        "consecutiveFailures": "2",
        "consecutiveZeroKept": "1",
        "duplicateRate": "0.25",
        "healthScore": "",
    }

    normalized = normalize_source_report_row(row)
    shared_base = normalize_jobs_fetch_report_source_row_base(
        row,
        clean_text_func=clean_text,
        normalize_text_func=norm_text,
    )

    for key, value in shared_base.items():
        assert normalized[key] == value
    assert normalized["status"] == "ok"
    assert normalized["adapter"] == "custom"
    assert normalized["fetchStrategy"] == "auto"
    assert normalized["lastSeenInFetchAt"] == row["lastRunAt"]
    assert normalized["lastJobsKept"] == 4
    assert normalized["failureCount"] == 2
    assert normalized["zeroJobStreak"] == 1
    assert normalized["duplicateRate"] == 0.25
    assert normalized["healthScore"] == 100


def test_bridge_fetch_report_source_row_uses_shared_enrichment_helper() -> None:
    row = {
        "name": "Source A",
        "status": "OK",
        "adapter": "STATIC",
        "classification": "needs_review",
        "failureBucket": "zero_kept",
        "zeroKeptClassification": "stale_source",
        "browserFallbackRecommended": True,
        "exclusionReason": "only_sources_filter",
        "coveredByProviderSourceId": "provider:source-a",
        "coveredByProviderAdapter": "greenhouse",
        "providerCoverageStatus": "covered",
        "providerCoverageConsecutiveSuccesses": "3",
        "providerCoverageLatestKeptCount": "12",
        "migrationSourceIdentity": "static:source-a",
        "cacheDecision": "hit",
        "cacheDecisionReason": "fresh",
        "details": [
            {
                "name": "Job A",
                "status": "OK",
                "adapter": "STATIC",
                "studio": "Studio A",
                "fetchedCount": "2",
                "keptCount": "1",
                "lowConfidenceDropped": "1",
                "error": "",
            },
            "{'name': 'Job B', 'status': 'OK', 'adapter': 'STATIC', 'studio': 'Studio B'}",
            "{'name': }",
        ],
    }

    bridge_row = normalize_fetch_report_contract({"sources": [row]})["sources"][0]
    shared_row = normalize_bridge_fetch_report_source_row(row)

    assert shared_row is not None
    assert bridge_row == shared_row
    assert bridge_row["adapter"] == "static"
    assert bridge_row["browserFallbackRecommended"] is True
    assert bridge_row["providerCoverageConsecutiveSuccesses"] == 3
    assert bridge_row["providerCoverageLatestKeptCount"] == 12
    assert bridge_row["details"] == [
        {
            "name": "Job A",
            "status": "ok",
            "adapter": "static",
            "studio": "Studio A",
            "fetchedCount": 2,
            "keptCount": 1,
            "lowConfidenceDropped": 1,
            "error": "",
        },
        {
            "name": "Job B",
            "status": "ok",
            "adapter": "static",
            "studio": "Studio B",
            "fetchedCount": 0,
            "keptCount": 0,
            "lowConfidenceDropped": 0,
            "error": "",
        },
    ]


def test_bridge_and_jobs_fetch_report_normalizers_share_social_and_timing_overlap() -> None:
    payload = {
        "socialSummary": {
            "keptCount": 5,
            "uniqueKeptCount": 4,
            "officialBoardOverlapCount": 1,
            "duplicateCount": 1,
            "duplicateRate": 0.2,
            "lowConfidenceDropped": 2,
            "channels": {
                "reddit": {
                    "keptCount": 3,
                    "uniqueKeptCount": 2,
                    "officialBoardOverlapCount": 1,
                    "duplicateCount": 1,
                    "duplicateRate": 0.25,
                    "lowConfidenceDropped": 1,
                }
            },
        },
        "runtime": {
            "timingSummary": {
                "totalDurationMs": 100,
                "wallClockDurationMs": 120,
                "medianSourceDurationMs": 10,
                "p95SourceDurationMs": 20,
                "stageTotalsMs": {"fetchAndParse": 80, "detailFetch": 30},
                "adapterTimings": [
                    {
                        "adapter": "static",
                        "sourceCount": 2,
                        "durationMs": 50,
                        "medianDurationMs": 25,
                        "fetchedCount": 6,
                        "keptCount": 4,
                        "errorCount": 1,
                        "zeroKeptCount": 0,
                    }
                ],
                "slowestAdapters": [
                    {
                        "adapter": "static",
                        "sourceCount": 2,
                        "durationMs": 50,
                        "medianDurationMs": 25,
                        "fetchedCount": 6,
                        "keptCount": 4,
                        "errorCount": 1,
                        "zeroKeptCount": 0,
                    }
                ],
                "highCostLowYieldSources": [
                    {"name": "slow", "adapter": "static", "durationMs": 99, "keptCount": 0}
                ],
                "detailHeavySources": [
                    {
                        "name": "detail-heavy",
                        "adapter": "static",
                        "durationMs": 80,
                        "keptCount": 2,
                        "detailFetchMs": 70,
                    }
                ],
            }
        },
    }

    bridge = normalize_fetch_report_contract(payload)
    jobs = normalize_fetch_report_payload(payload)

    for key in (
        "keptCount",
        "uniqueKeptCount",
        "officialBoardOverlapCount",
        "duplicateCount",
        "duplicateRate",
        "lowConfidenceDropped",
    ):
        assert bridge["socialSummary"][key] == jobs["socialSummary"][key]
        assert (
            bridge["socialSummary"]["channels"]["reddit"][key]
            == jobs["socialSummary"]["channels"]["reddit"][key]
        )

    bridge_timing = bridge["runtime"]["timingSummary"]
    jobs_timing = jobs["runtime"]["timingSummary"]
    for key in ("totalDurationMs", "medianSourceDurationMs", "p95SourceDurationMs"):
        assert bridge_timing[key] == jobs_timing[key]
    for key in ("fetchAndParse", "detailFetch"):
        assert bridge_timing["stageTotalsMs"][key] == jobs_timing["stageTotalsMs"][key]
    assert bridge_timing["adapterTimings"][0] == jobs_timing["adapterTimings"][0]
    assert bridge_timing["slowestAdapters"][0] == jobs_timing["slowestAdapters"][0]
    assert bridge_timing["highCostLowYieldSources"][0] == jobs_timing["highCostLowYieldSources"][0]
    assert "wallClockDurationMs" not in bridge_timing
    assert jobs_timing["wallClockDurationMs"] == 120
    assert "detailHeavySources" not in bridge_timing
    assert jobs_timing["detailHeavySources"][0]["detailFetchMs"] == 70
