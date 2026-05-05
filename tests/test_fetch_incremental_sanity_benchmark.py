from __future__ import annotations

from src import fetch_incremental_sanity_benchmark as benchmark


def test_runtime_duration_uses_total_duration_first() -> None:
    assert benchmark._runtime_duration_ms(
        {"runtime": {"totalDurationMs": 123, "wallClockDurationMs": 456}}
    ) == 123


def test_runtime_duration_falls_back_to_wall_clock_duration() -> None:
    assert benchmark._runtime_duration_ms({"runtime": {"wallClockDurationMs": 456}}) == 456


def test_runtime_duration_uses_nested_timing_summary() -> None:
    assert benchmark._runtime_duration_ms(
        {"runtime": {"timingSummary": {"totalDurationMs": 789}}}
    ) == 789


def test_source_names_for_group_uses_group_sources() -> None:
    args = benchmark.parse_args(["--group", "smoke"])

    assert benchmark.source_names_for_args(args) == ["greenhouse_boards", "lever_sources"]


def test_source_names_for_static_group_uses_static_marker() -> None:
    args = benchmark.parse_args(["--group", "static-detail"])

    assert benchmark.source_names_for_args(args) == ["__static_first_5__"]


def test_source_names_for_args_uses_custom_sources() -> None:
    args = benchmark.parse_args(["--sources", "one", "two"])

    assert benchmark.source_names_for_args(args) == ["one", "two"]


def test_stage_durations_sum_timing_summary_stages() -> None:
    stages = benchmark._stage_durations_ms(
        {"runtime": {"timingSummary": {"stageTotalsMs": {"fetchAndParse": 10}}}},
        {
            "runtime": {
                "timingSummary": {
                    "stageTotalsMs": {"fetchAndParse": 5, "canonicalization": 7}
                }
            }
        },
    )

    assert stages == {"canonicalization": 7, "fetchAndParse": 15}


def test_network_wait_counters_collect_cache_and_adapter_proxy_counts() -> None:
    counters = benchmark._network_wait_counters(
        {
            "summary": {"cacheSkippedCount": 2, "revalidatedCount": 1, "failedSources": 1},
            "runtime": {
                "timingSummary": {
                    "adapterTimings": [
                        {"adapter": "greenhouse", "durationMs": 10, "errorCount": 1}
                    ]
                }
            },
            "sources": [{"boardCacheDecisionCounts": {"run_now": 2, "skip_fresh": 3}}],
        }
    )

    assert counters["cacheSkippedCount"] == 2
    assert counters["revalidatedCount"] == 1
    assert counters["failedSources"] == 1
    assert counters["timeoutOrErrorCount"] == 1
    assert counters["boardRefreshedCount"] == 2
    assert counters["boardSkippedCount"] == 3
    assert counters["adapterDurationsMs"] == {"greenhouse": 10}


def test_slowest_sources_normalizes_runtime_rows() -> None:
    rows = benchmark._slowest_sources(
        {
            "runtime": {
                "slowestSources": [
                    {
                        "name": "lever_sources",
                        "adapter": "lever",
                        "durationMs": 200,
                        "keptCount": 10,
                    },
                    {
                        "name": "greenhouse_boards",
                        "adapter": "greenhouse",
                        "durationMs": 300,
                        "keptCount": 20,
                        "detailPagesVisited": 2,
                        "detailYieldPct": 50,
                    },
                ]
            }
        },
        limit=1,
    )

    assert rows == [
        {
            "name": "greenhouse_boards",
            "adapter": "greenhouse",
            "durationMs": 300,
            "keptCount": 20,
            "detailPagesVisited": 2,
            "detailYieldPct": 50,
        }
    ]
