from src.bridge.report_normalizer import normalize_fetch_report_contract
from src.jobs.common.contracts_fetch_report import normalize_fetch_report_payload


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
    payload = {"sources": [{}]}

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
