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
            "providerUrl": "https://example.com/slow",
            "error": "",
        }
    ]


def test_family_summary_includes_static_stats_and_taxonomy() -> None:
    summary = benchmark._family_summary(
        {
            "sources": [
                {
                    "name": "static_source::example",
                    "adapter": "static",
                    "status": "ok",
                    "durationMs": 25,
                    "keptCount": 2,
                    "failureBucket": "needs_review",
                    "zeroKeptClassification": "needs_review",
                    "stats": {"external_detail_links_capped": 4},
                    "loss": {"staticNonJobUrlRejected": 4},
                }
            ]
        },
        ["static_source::example"],
    )

    row = summary["static_source::example"]
    assert row["failureBucket"] == "needs_review"
    assert row["zeroKeptClassification"] == "needs_review"
    assert row["stats"] == {"external_detail_links_capped": 4}
    assert row["loss"] == {"staticNonJobUrlRejected": 4}


def test_source_policy_signals_flags_site_changed_high_merge_and_network_wait() -> None:
    rows = benchmark._source_policy_signals(
        {
            "sources": [
                {
                    "name": "static_source::super-lucky",
                    "adapter": "static",
                    "durationMs": 17000,
                    "keptCount": 50,
                    "failureBucket": "site_changed",
                    "error": "Network error for https://example: Server disconnected",
                    "loss": {"rawFetched": 50, "dedupMerged": 32, "finalOutput": 18},
                },
                {
                    "name": "static_source::quiet",
                    "adapter": "static",
                    "durationMs": 10,
                    "keptCount": 1,
                    "loss": {"rawFetched": 1, "dedupMerged": 0, "finalOutput": 1},
                },
            ]
        },
        ["static_source::super-lucky", "static_source::quiet"],
    )

    assert rows == [
        {
            "name": "static_source::super-lucky",
            "adapter": "static",
            "durationMs": 17000,
            "keptCount": 50,
            "rawFetched": 50,
            "dedupMerged": 32,
            "finalOutput": 18,
            "mergeRatioPct": 64,
            "failureBucket": "site_changed",
            "zeroKeptClassification": "",
            "flags": ["failure:site_changed", "high_merge_ratio", "network_wait"],
        }
    ]


def test_registry_page_signal_flags_off_listing_host_pages() -> None:
    signal = benchmark._registry_page_signal_for_row(
        "static_source::example",
        {
            "listing_url": "https://www.example.com/careers",
            "pages": [
                "https://www.example.com/careers",
                "https://parent.example/jobs",
                "https://parent.example/culture",
                "https://ats.example/jobs/1",
            ],
        },
    )

    assert signal == {
        "name": "static_source::example",
        "listingHost": "example.com",
        "pageCount": 4,
        "offListingHostPageCount": 3,
        "offListingHosts": ["parent.example", "ats.example"],
        "offListingHostPages": [
            "https://parent.example/jobs",
            "https://parent.example/culture",
            "https://ats.example/jobs/1",
        ],
    }


def test_registry_scope_summary_orders_cross_host_sources() -> None:
    summary = benchmark._registry_scope_summary(
        {
            "static_source::small": {
                "name": "static_source::small",
                "pageCount": 2,
                "offListingHostPageCount": 1,
            },
            "static_source::large": {
                "name": "static_source::large",
                "pageCount": 5,
                "offListingHostPageCount": 4,
            },
        }
    )

    assert summary == {
        "sourceCount": 2,
        "offListingHostPageCount": 5,
        "sources": [
            {
                "name": "static_source::large",
                "pageCount": 5,
                "offListingHostPageCount": 4,
            },
            {
                "name": "static_source::small",
                "pageCount": 2,
                "offListingHostPageCount": 1,
            },
        ],
    }


def test_next_optimization_targets_prioritizes_source_policy_before_timeouts() -> None:
    targets = benchmark._next_optimization_targets(
        [
            {
                "name": "static_source::maliyo",
                "durationMs": 25000,
                "keptCount": 5,
                "flags": ["failure:unknown", "time_budget"],
            },
            {
                "name": "static_source::super-lucky",
                "durationMs": 18000,
                "keptCount": 50,
                "flags": ["failure:site_changed", "high_merge_ratio"],
            },
            {
                "name": "static_source::netflix",
                "durationMs": 16000,
                "keptCount": 0,
                "flags": ["failure:needs_review", "zero_kept:needs_review"],
            },
            {
                "name": "static_source::koei",
                "durationMs": 24000,
                "keptCount": 8,
                "flags": ["failure:unknown", "time_budget", "network_wait"],
            },
        ],
        registry_page_signals={
            "static_source::super-lucky": {
                "listingHost": "superluckycasino.com",
                "offListingHosts": ["stillfront.com"],
                "offListingHostPageCount": 4,
            },
            "static_source::koei": {
                "listingHost": "koeitecmo.vn",
                "offListingHosts": ["careerviet.vn"],
                "offListingHostPageCount": 2,
            },
        },
    )

    assert targets == [
        {
            "name": "static_source::super-lucky",
            "action": "source_policy_review",
            "priority": 100,
            "durationMs": 18000,
            "keptCount": 50,
            "outputContractRisk": True,
            "requiresExplicitDecision": True,
            "registryPageEvidence": {
                "listingHost": "superluckycasino.com",
                "offListingHosts": ["stillfront.com"],
                "offListingHostPageCount": 4,
            },
            "reasons": ["site_changed", "high_merge_ratio"],
        },
        {
            "name": "static_source::netflix",
            "action": "source_policy_review",
            "priority": 90,
            "durationMs": 16000,
            "keptCount": 0,
            "outputContractRisk": False,
            "requiresExplicitDecision": False,
            "registryPageEvidence": {},
            "reasons": ["needs_review"],
        },
        {
            "name": "static_source::koei",
            "action": "source_scope_and_timeout_review",
            "priority": 65,
            "durationMs": 24000,
            "keptCount": 8,
            "outputContractRisk": True,
            "requiresExplicitDecision": True,
            "registryPageEvidence": {
                "listingHost": "koeitecmo.vn",
                "offListingHosts": ["careerviet.vn"],
                "offListingHostPageCount": 2,
            },
            "reasons": ["time_budget", "network_wait", "cross_host_registry_pages"],
        },
        {
            "name": "static_source::maliyo",
            "action": "timeout_or_network_budget",
            "priority": 30,
            "durationMs": 25000,
            "keptCount": 5,
            "outputContractRisk": False,
            "requiresExplicitDecision": False,
            "registryPageEvidence": {},
            "reasons": ["time_budget"],
        },
    ]
