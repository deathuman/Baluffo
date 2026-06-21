from __future__ import annotations

from src import fetch_incremental_sanity_benchmark as benchmark


def test_runtime_duration_uses_total_duration_first() -> None:
    assert (
        benchmark._runtime_duration_ms(
            {"runtime": {"totalDurationMs": 123, "wallClockDurationMs": 456}}
        )
        == 123
    )


def test_runtime_duration_falls_back_to_wall_clock_duration() -> None:
    assert benchmark._runtime_duration_ms({"runtime": {"wallClockDurationMs": 456}}) == 456


def test_runtime_duration_uses_nested_timing_summary() -> None:
    assert (
        benchmark._runtime_duration_ms({"runtime": {"timingSummary": {"totalDurationMs": 789}}})
        == 789
    )


def test_source_names_for_group_uses_group_sources() -> None:
    args = benchmark.parse_args(["--group", "smoke"])

    assert benchmark.source_names_for_args(args) == ["greenhouse_boards", "lever_sources"]


def test_source_names_for_static_group_uses_static_marker() -> None:
    args = benchmark.parse_args(["--group", "static-detail"])

    assert benchmark.source_names_for_args(args) == [benchmark.STATIC_DETAIL_TARGET_MARKER]


def test_source_names_for_static_outliers_group_uses_static_marker() -> None:
    args = benchmark.parse_args(["--group", "static-outliers"])

    assert benchmark.source_names_for_args(args) == [benchmark.STATIC_OUTLIER_TARGET_MARKER]


def test_source_names_for_args_uses_custom_sources() -> None:
    args = benchmark.parse_args(["--sources", "one", "two"])

    assert benchmark.source_names_for_args(args) == ["one", "two"]


def test_stage_durations_sum_timing_summary_stages() -> None:
    stages = benchmark._stage_durations_ms(
        {"runtime": {"timingSummary": {"stageTotalsMs": {"fetchAndParse": 10}}}},
        {
            "runtime": {
                "timingSummary": {"stageTotalsMs": {"fetchAndParse": 5, "canonicalization": 7}}
            }
        },
    )

    assert stages == {"canonicalization": 7, "fetchAndParse": 15}


def test_urls_in_text_extracts_embedded_static_error_urls() -> None:
    assert benchmark._urls_in_text(
        "static:Studio:https://www.maliyo.com/career/: time budget exceeded (25s)"
    ) == ["https://www.maliyo.com/career/"]


def test_timeout_diagnostics_splits_listing_and_detail_timeout_urls() -> None:
    diagnostics = benchmark._timeout_diagnostics(
        "static_source::static:listing_url:https://www.maliyo.com/career/",
        (
            "static:Maliyo:https://www.maliyo.com/career/: time budget exceeded (25s); "
            "static:Maliyo:https://www.maliyo.com/jobs/designer: time budget exceeded (25s)"
        ),
        {"detailPagesVisited": 9, "detailYieldPct": 100},
    )

    assert diagnostics["timeoutUrlRoleCounts"] == {
        "listing": 1,
        "detail_or_registry_page": 1,
    }
    assert diagnostics["listingTimeouts"] == {
        "timeoutUrlCount": 1,
        "timeoutUrls": ["https://www.maliyo.com/career/"],
        "firstTimeoutUrl": "https://www.maliyo.com/career/",
        "lastTimeoutUrl": "https://www.maliyo.com/career/",
    }
    assert diagnostics["detailTimeouts"] == {
        "timeoutUrlCount": 1,
        "timeoutUrls": ["https://www.maliyo.com/jobs/designer"],
        "firstTimeoutUrl": "https://www.maliyo.com/jobs/designer",
        "lastTimeoutUrl": "https://www.maliyo.com/jobs/designer",
    }
    assert diagnostics["detailPagesVisited"] == 9
    assert diagnostics["detailYieldPct"] == 100


def test_source_decision_matrix_markdown_renders_timeout_url_buckets() -> None:
    markdown = benchmark._render_source_decision_matrix_markdown(
        [
            {
                "name": "static_source::static:listing_url:https://www.maliyo.com/career/",
                "action": "timeout_or_network_budget",
                "keptCount": 11,
                "durationMs": 25906,
                "decisionType": "slow_productive_static",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": False,
                "evidence": {
                    "timeoutDiagnostics": {
                        "timeoutErrorCount": 2,
                        "networkErrorCount": 0,
                        "timeoutUrlCount": 2,
                        "timeoutUrls": [
                            "https://www.maliyo.com/career/",
                            "https://www.maliyo.com/jobs/designer",
                        ],
                        "timeoutUrlRoleCounts": {
                            "listing": 1,
                            "detail_or_registry_page": 1,
                        },
                        "listingTimeouts": {"timeoutUrls": ["https://www.maliyo.com/career/"]},
                        "detailTimeouts": {"timeoutUrls": ["https://www.maliyo.com/jobs/designer"]},
                    }
                },
            }
        ]
    )

    assert "- Listing timeout URLs: https://www.maliyo.com/career/" in markdown
    assert "- Detail timeout URLs: https://www.maliyo.com/jobs/designer" in markdown


def test_network_wait_counters_collect_cache_and_adapter_proxy_counts() -> None:
    counters = benchmark._network_wait_counters(
        {
            "summary": {"cacheSkippedCount": 2, "revalidatedCount": 1, "failedSources": 1},
            "runtime": {
                "timingSummary": {
                    "adapterTimings": [{"adapter": "greenhouse", "durationMs": 10, "errorCount": 1}]
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


def test_slowest_provider_boards_extracts_detail_timings() -> None:
    rows = benchmark._slowest_provider_boards(
        {
            "sources": [
                {
                    "name": "greenhouse_boards",
                    "adapter": "greenhouse",
                    "details": [
                        {
                            "adapter": "greenhouse",
                            "name": "Fast Board",
                            "studio": "Fast",
                            "slug": "fast",
                            "status": "ok",
                            "cacheDecision": "run_now",
                            "durationMs": 100,
                            "fetchMs": 90,
                            "parseMs": 10,
                            "keptCount": 2,
                            "providerUrl": "https://example.com/fast",
                        },
                        {
                            "adapter": "greenhouse",
                            "name": "Slow Board",
                            "studio": "Slow",
                            "slug": "slow",
                            "status": "ok",
                            "cacheDecision": "run_now",
                            "durationMs": 300,
                            "fetchMs": 280,
                            "parseMs": 20,
                            "keptCount": 5,
                            "providerUrl": "https://example.com/slow",
                        },
                    ],
                }
            ]
        },
        limit=1,
    )

    assert rows == [
        {
            "source": "greenhouse_boards",
            "adapter": "greenhouse",
            "name": "Slow Board",
            "studio": "Slow",
            "slug": "slow",
            "status": "ok",
            "cacheDecision": "run_now",
            "durationMs": 300,
            "fetchMs": 280,
            "parseMs": 20,
            "keptCount": 5,
            **{"providerUrl": "https://example.com/slow", "providerHost": "example.com"},
            "error": "",
        }
    ]
