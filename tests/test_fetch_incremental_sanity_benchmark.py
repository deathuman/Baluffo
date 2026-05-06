from __future__ import annotations

import gzip
import json

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


def test_source_decision_matrix_preserves_behavior_for_policy_scope_and_timeout_targets() -> None:
    source_policy_signals = [
        {
            "name": "static_source::super-lucky",
            "durationMs": 24614,
            "keptCount": 33,
            "mergeRatioPct": 76,
            "failureBucket": "site_changed",
            "zeroKeptClassification": "",
            "flags": ["failure:site_changed", "high_merge_ratio", "time_budget"],
        },
        {
            "name": "static_source::atvis",
            "durationMs": 16749,
            "keptCount": 7,
            "mergeRatioPct": 0,
            "failureBucket": "site_changed",
            "zeroKeptClassification": "",
            "flags": ["failure:site_changed"],
        },
        {
            "name": "static_source::koei",
            "durationMs": 24527,
            "keptCount": 10,
            "mergeRatioPct": 10,
            "failureBucket": "unknown",
            "zeroKeptClassification": "",
            "flags": ["failure:unknown", "time_budget"],
        },
        {
            "name": "static_source::maliyo",
            "durationMs": 27072,
            "keptCount": 5,
            "mergeRatioPct": 0,
            "failureBucket": "unknown",
            "zeroKeptClassification": "",
            "flags": ["failure:unknown", "time_budget"],
        },
        {
            "name": "static_source::netflix",
            "durationMs": 17503,
            "keptCount": 0,
            "mergeRatioPct": 0,
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "flags": ["failure:needs_review", "zero_kept:needs_review"],
        },
    ]
    targets = benchmark._next_optimization_targets(
        source_policy_signals,
        registry_page_signals={
            "static_source::super-lucky": {
                "listingHost": "superluckycasino.com",
                "offListingHosts": ["stillfront.com"],
                "offListingHostPageCount": 4,
            },
            "static_source::atvis": {
                "listingHost": "atvis.com",
                "offListingHosts": ["linkedin.com"],
                "offListingHostPageCount": 1,
            },
            "static_source::koei": {
                "listingHost": "koeitecmo.vn",
                "offListingHosts": ["careerviet.vn"],
                "offListingHostPageCount": 2,
            },
        },
    )
    rows = benchmark._source_decision_matrix(
        targets,
        source_policy_signals,
        {
            "runtime": {
                "slowestSources": [
                    {
                        "name": "static_source::maliyo",
                        "detailPagesVisited": 3,
                        "detailYieldPct": 100,
                    }
                ]
            },
            "sources": [
                {
                    "name": "static_source::super-lucky",
                    "status": "ok",
                    "keptCount": 33,
                    "error": "first super lucky error; second super lucky error; third super lucky error; fourth super lucky error",
                    "failureBucket": "site_changed",
                },
                {
                    "name": "static_source::atvis",
                    "status": "ok",
                    "keptCount": 7,
                    "error": "HTTP 404 for listing; HTTP 404 for oembed",
                    "failureBucket": "site_changed",
                },
                {
                    "name": "static_source::koei",
                    "status": "ok",
                    "keptCount": 10,
                    "error": "time budget exceeded for careerviet",
                    "failureBucket": "unknown",
                },
                {
                    "name": "static_source::maliyo",
                    "status": "ok",
                    "keptCount": 5,
                    "error": "time budget exceeded",
                    "failureBucket": "unknown",
                },
                {
                    "name": "static_source::netflix",
                    "status": "ok",
                    "keptCount": 0,
                    "error": "",
                    "failureBucket": "needs_review",
                    "zeroKeptClassification": "needs_review",
                },
            ],
            "jobs": [
                {
                    "source": "static_source::super-lucky",
                    "jobLink": "https://www.stillfront.com/en/career/join-the-team",
                },
                {
                    "sourceBundle": [{"source": "static_source::super-lucky"}],
                    "jobLink": "https://jobs.example.com/role",
                },
                {
                    "source": "static_source::maliyo",
                    "jobLink": "https://www.maliyo.com/career/game-designer",
                },
            ],
        },
    )

    by_name = {row["name"]: row for row in rows}
    assert all(row["recommendedFirstPass"] == "preserve_current_behavior" for row in rows)
    assert all(row["behaviorChangeAllowed"] is False for row in rows)

    super_lucky = by_name["static_source::super-lucky"]
    assert super_lucky["decisionType"] == "explicit_source_policy"
    assert super_lucky["requiresExplicitDecision"] is True
    assert super_lucky["evidence"]["mergeRatioPct"] == 76
    assert "failure:site_changed" in super_lucky["evidence"]["flags"]
    assert "high_merge_ratio" in super_lucky["evidence"]["flags"]
    assert "time_budget" in super_lucky["evidence"]["reasons"]
    assert super_lucky["evidence"]["registryPageEvidence"]["offListingHosts"] == [
        "stillfront.com"
    ]
    assert super_lucky["evidence"]["sourcePolicyDecision"] == {
        "policyDecisionNeeded": True,
        "sourceScopeIdentity": {
            "listingHost": "superluckycasino.com",
            "offListingHosts": ["stillfront.com"],
        },
        "keptOutputHostBreakdown": {
            "totalKeptCount": 2,
            "hostCount": 2,
            "hosts": [
                {"host": "stillfront.com", "keptCount": 1},
                {"host": "jobs.example.com", "keptCount": 1},
            ],
        },
        "suggestedDecision": "split_source",
    }
    assert len(super_lucky["evidence"]["errorSamples"]) == 3

    atvis = by_name["static_source::atvis"]
    assert atvis["decisionType"] == "explicit_source_policy"
    assert atvis["requiresExplicitDecision"] is True
    assert atvis["evidence"]["registryPageEvidence"]["offListingHosts"] == ["linkedin.com"]
    assert atvis["evidence"]["errorSamples"] == [
        "HTTP 404 for listing",
        "HTTP 404 for oembed",
    ]

    koei = by_name["static_source::koei"]
    assert koei["decisionType"] == "explicit_source_scope"
    assert koei["requiresExplicitDecision"] is True
    assert "time_budget" in koei["evidence"]["reasons"]
    assert koei["evidence"]["registryPageEvidence"]["offListingHosts"] == ["careerviet.vn"]

    maliyo = by_name["static_source::maliyo"]
    assert maliyo["decisionType"] == "slow_productive_static"
    assert maliyo["requiresExplicitDecision"] is False
    assert maliyo["evidence"]["detailTiming"] == {
        "detailPagesVisited": 3,
        "detailYieldPct": 100,
    }
    assert maliyo["evidence"]["timeoutDiagnostics"] == {
        "timeoutErrorCount": 1,
        "networkErrorCount": 0,
        "timeoutUrlCount": 0,
        "timeoutUrls": [],
        "firstTimeoutUrl": "",
        "lastTimeoutUrl": "",
        "timeoutUrlRoleCounts": {},
        "detailPagesVisited": 3,
        "detailYieldPct": 100,
    }

    netflix = by_name["static_source::netflix"]
    assert netflix["decisionType"] == "follow_up_review"
    assert netflix["requiresExplicitDecision"] is False
    assert netflix["evidence"]["zeroKeptClassification"] == "needs_review"


def test_source_decision_matrix_markdown_renders_operator_review_evidence() -> None:
    markdown = benchmark._render_source_decision_matrix_markdown(
        [
            {
                "name": "static_source::super-lucky",
                "action": "source_policy_review",
                "priority": 100,
                "keptCount": 33,
                "durationMs": 24614,
                "decisionType": "explicit_source_policy",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": True,
                "evidence": {
                    "flags": ["failure:site_changed", "high_merge_ratio", "time_budget"],
                    "reasons": ["site_changed", "high_merge_ratio", "time_budget"],
                    "mergeRatioPct": 76,
                    "failureBucket": "site_changed",
                    "zeroKeptClassification": "",
                    "registryPageEvidence": {
                        "listingHost": "superluckycasino.com",
                        "offListingHosts": ["stillfront.com"],
                        "offListingHostPages": [
                            "https://www.stillfront.com/en/career/",
                        ],
                    },
                    "detailTiming": {},
                    "sourcePolicyDecision": {
                        "policyDecisionNeeded": True,
                        "suggestedDecision": "split_source",
                        "keptOutputHostBreakdown": {
                            "hosts": [
                                {"host": "stillfront.com", "keptCount": 2},
                                {"host": "superluckycasino.com", "keptCount": 1},
                            ],
                        },
                    },
                    "errorSamples": ["first", "second", "third"],
                },
                "nextDecision": "Decide policy.",
            },
            {
                "name": "static_source::koei",
                "action": "source_scope_and_timeout_review",
                "priority": 65,
                "keptCount": 10,
                "durationMs": 24527,
                "decisionType": "explicit_source_scope",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": True,
                "evidence": {
                    "flags": ["failure:unknown", "time_budget"],
                    "reasons": ["time_budget", "cross_host_registry_pages"],
                    "registryPageEvidence": {
                        "listingHost": "koeitecmo.vn",
                        "offListingHosts": ["careerviet.vn"],
                    },
                },
                "nextDecision": "Decide scope.",
            },
            {
                "name": "static_source::maliyo",
                "action": "timeout_or_network_budget",
                "priority": 30,
                "keptCount": 5,
                "durationMs": 27072,
                "decisionType": "slow_productive_static",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": False,
                "evidence": {
                    "flags": ["failure:unknown", "time_budget"],
                    "reasons": ["time_budget"],
                    "detailTiming": {"detailPagesVisited": 3, "detailYieldPct": 100},
                    "timeoutDiagnostics": {
                        "timeoutErrorCount": 1,
                        "networkErrorCount": 0,
                        "timeoutUrlCount": 1,
                        "timeoutUrls": ["https://www.maliyo.com/career/"],
                        "firstTimeoutUrl": "https://www.maliyo.com/career/",
                        "lastTimeoutUrl": "https://www.maliyo.com/career/",
                        "timeoutUrlRoleCounts": {"listing": 1},
                    },
                },
                "nextDecision": "Inspect timeout.",
            },
            {
                "name": "static_source::netflix",
                "action": "source_policy_review",
                "priority": 90,
                "keptCount": 0,
                "durationMs": 17503,
                "decisionType": "follow_up_review",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": False,
                "evidence": {
                    "flags": ["failure:needs_review", "zero_kept:needs_review"],
                    "reasons": ["needs_review"],
                    "zeroKeptClassification": "needs_review",
                },
                "nextDecision": "Review later.",
            },
        ]
    )

    assert "# Source Decision Matrix" in markdown
    assert "`explicit_source_policy`" in markdown
    assert "`explicit_source_scope`" in markdown
    assert "`slow_productive_static`" in markdown
    assert "`follow_up_review`" in markdown
    assert "`preserve_current_behavior`" in markdown
    assert "- Behavior change allowed: `false`" in markdown
    assert "stillfront.com" in markdown
    assert "careerviet.vn" in markdown
    assert "Policy decision needed: `true`" in markdown
    assert "Suggested source-policy decision: `split_source`" in markdown
    assert "Kept output hosts: stillfront.com=2, superluckycasino.com=1" in markdown
    assert "Error samples: first, second, third" in markdown
    assert "Detail timing: `pages=3, yield=100%`" in markdown
    assert "Timeout diagnostics: `timeouts=1, network=0, timeoutUrls=1`" in markdown
    assert "Timeout URL roles: listing=1" in markdown
    assert "https://www.maliyo.com/career/" in markdown


def test_source_decision_matrix_markdown_is_written_next_to_summary(tmp_path) -> None:
    payload = {
        "sourceDecisionMatrix": [
            {
                "name": "static_source::maliyo",
                "action": "timeout_or_network_budget",
                "priority": 30,
                "keptCount": 5,
                "durationMs": 27072,
                "decisionType": "slow_productive_static",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": False,
                "evidence": {"flags": ["time_budget"]},
                "nextDecision": "Inspect timeout.",
            }
        ],
        "nextOptimizationTargets": [{"name": "static_source::maliyo"}],
    }

    (tmp_path / "benchmark-summary.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )
    (tmp_path / "source-decision-matrix.md").write_text(
        benchmark._render_source_decision_matrix_markdown(
            [dict(row) for row in benchmark._as_list(payload.get("sourceDecisionMatrix"))]
        ),
        encoding="utf-8",
    )

    summary = json.loads((tmp_path / "benchmark-summary.json").read_text(encoding="utf-8"))
    markdown = (tmp_path / "source-decision-matrix.md").read_text(encoding="utf-8")
    assert summary["nextOptimizationTargets"] == [{"name": "static_source::maliyo"}]
    assert "static_source::maliyo" in markdown
    assert "`slow_productive_static`" in markdown


def test_load_output_jobs_reads_plain_or_gzip_backed_pipeline_json(tmp_path) -> None:
    with gzip.open(tmp_path / "jobs-unified.json.gz", mode="wt", encoding="utf-8") as handle:
        json.dump(
            [
                {"source": "static_source::super-lucky", "jobLink": "https://stillfront.com/a"},
                "not-a-row",
            ],
            handle,
        )

    assert benchmark._load_output_jobs(tmp_path) == [
        {"source": "static_source::super-lucky", "jobLink": "https://stillfront.com/a"}
    ]


def test_source_decision_log_template_renders_operator_fields_and_guardrails() -> None:
    markdown = benchmark._render_source_decision_log_template_markdown(
        [
            {
                "name": "static_source::super-lucky",
                "action": "source_policy_review",
                "keptCount": 33,
                "durationMs": 24614,
                "decisionType": "explicit_source_policy",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": True,
                "evidence": {
                    "flags": ["failure:site_changed", "high_merge_ratio"],
                    "reasons": ["site_changed"],
                    "failureBucket": "site_changed",
                    "registryPageEvidence": {"offListingHosts": ["stillfront.com"]},
                    "errorSamples": ["HTTP 404"],
                },
            },
            {
                "name": "static_source::koei",
                "action": "source_scope_and_timeout_review",
                "keptCount": 10,
                "durationMs": 24527,
                "decisionType": "explicit_source_scope",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": True,
                "evidence": {
                    "flags": ["failure:unknown", "time_budget"],
                    "reasons": ["time_budget"],
                    "registryPageEvidence": {"offListingHosts": ["careerviet.vn"]},
                },
            },
            {
                "name": "static_source::maliyo",
                "action": "timeout_or_network_budget",
                "keptCount": 5,
                "durationMs": 27072,
                "decisionType": "slow_productive_static",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": False,
                "evidence": {"flags": ["time_budget"]},
            },
            {
                "name": "static_source::netflix",
                "action": "source_policy_review",
                "keptCount": 0,
                "durationMs": 17503,
                "decisionType": "follow_up_review",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": False,
                "evidence": {"zeroKeptClassification": "needs_review"},
            },
        ]
    )

    assert "local review evidence only" in markdown
    assert "`explicit_source_policy`" in markdown
    assert "`explicit_source_scope`" in markdown
    assert "`slow_productive_static`" in markdown
    assert "`follow_up_review`" in markdown
    assert "`preserve_current_behavior`" in markdown
    assert "- Behavior change allowed: `false`" in markdown
    assert "- Requires explicit decision: `true`" in markdown
    assert "stillfront.com" in markdown
    assert "careerviet.vn" in markdown
    assert "Decision: preserve / investigate / change_later" in markdown
    assert "Chosen action:" in markdown
    assert "Reason:" in markdown
    assert "Risk accepted: yes/no" in markdown
    assert "Follow-up owner/date:" in markdown


def test_source_decision_log_template_empty_state_is_useful() -> None:
    markdown = benchmark._render_source_decision_log_template_markdown([])

    assert "# Source Decision Log Template" in markdown
    assert "No source decision rows were generated." in markdown
    assert "local review evidence only" in markdown


def test_static_outlier_artifact_family_includes_decision_log_template(tmp_path) -> None:
    payload = {
        "sourceDecisionMatrix": [
            {
                "name": "static_source::maliyo",
                "action": "timeout_or_network_budget",
                "priority": 30,
                "keptCount": 5,
                "durationMs": 27072,
                "decisionType": "slow_productive_static",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": False,
                "evidence": {"flags": ["time_budget"]},
                "nextDecision": "Inspect timeout.",
            }
        ],
        "nextOptimizationTargets": [{"name": "static_source::maliyo"}],
    }
    rows = [dict(row) for row in benchmark._as_list(payload.get("sourceDecisionMatrix"))]

    (tmp_path / "benchmark-summary.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )
    (tmp_path / "source-decision-matrix.md").write_text(
        benchmark._render_source_decision_matrix_markdown(rows),
        encoding="utf-8",
    )
    (tmp_path / "source-decision-log-template.md").write_text(
        benchmark._render_source_decision_log_template_markdown(rows),
        encoding="utf-8",
    )

    summary = json.loads((tmp_path / "benchmark-summary.json").read_text(encoding="utf-8"))
    matrix_markdown = (tmp_path / "source-decision-matrix.md").read_text(encoding="utf-8")
    log_markdown = (tmp_path / "source-decision-log-template.md").read_text(encoding="utf-8")
    assert summary["nextOptimizationTargets"] == [{"name": "static_source::maliyo"}]
    assert "`slow_productive_static`" in matrix_markdown
    assert "Decision: preserve / investigate / change_later" in log_markdown


def test_source_decision_matrix_keeps_unproductive_timeout_as_timeout_diagnostics() -> None:
    source_policy_signals = [
        {
            "name": "static_source::slow-empty",
            "durationMs": 27072,
            "keptCount": 0,
            "mergeRatioPct": 0,
            "failureBucket": "unknown",
            "zeroKeptClassification": "",
            "flags": ["failure:unknown", "time_budget"],
        },
    ]
    targets = benchmark._next_optimization_targets(source_policy_signals)

    rows = benchmark._source_decision_matrix(
        targets,
        source_policy_signals,
        {
            "runtime": {
                "slowestSources": [
                    {
                        "name": "static_source::slow-empty",
                        "detailPagesVisited": 5,
                        "detailYieldPct": 0,
                    }
                ]
            },
            "sources": [
                {
                    "name": "static_source::slow-empty",
                    "status": "ok",
                    "keptCount": 0,
                    "error": "time budget exceeded",
                    "failureBucket": "unknown",
                }
            ],
        },
    )

    assert rows[0]["decisionType"] == "timeout_diagnostics"


def test_source_decision_trend_compares_current_rows_to_previous_payload() -> None:
    trend = benchmark._source_decision_trend(
        [
            {
                "name": "static_source::maliyo",
                "decisionType": "slow_productive_static",
                "keptCount": 7,
                "durationMs": 25346,
                "requiresExplicitDecision": False,
            },
            {
                "name": "static_source::super-lucky",
                "decisionType": "explicit_source_policy",
                "keptCount": 43,
                "durationMs": 24133,
                "requiresExplicitDecision": True,
            },
            {
                "name": "static_source::new",
                "decisionType": "timeout_diagnostics",
                "keptCount": 0,
                "durationMs": 10,
                "requiresExplicitDecision": False,
            },
        ],
        {
            "sourceDecisionMatrix": [
                {
                    "name": "static_source::maliyo",
                    "decisionType": "slow_productive_static",
                    "keptCount": 7,
                    "durationMs": 25111,
                    "requiresExplicitDecision": False,
                },
                {
                    "name": "static_source::super-lucky",
                    "decisionType": "timeout_diagnostics",
                    "keptCount": 26,
                    "durationMs": 25086,
                    "requiresExplicitDecision": False,
                },
                {
                    "name": "static_source::missing-now",
                    "decisionType": "follow_up_review",
                    "keptCount": 0,
                    "durationMs": 100,
                    "requiresExplicitDecision": False,
                },
            ]
        },
    )

    by_name = {row["name"]: row for row in trend["rows"]}
    assert trend["status"] == "compared"
    assert trend["rowCount"] == 3
    assert trend["newCount"] == 1
    assert trend["missingPreviousCount"] == 1
    assert trend["changedDecisionTypeCount"] == 1
    assert trend["stableSlowProductiveCount"] == 1
    assert by_name["static_source::maliyo"]["previousDecisionType"] == "slow_productive_static"
    assert by_name["static_source::super-lucky"]["decisionTypeChanged"] is True
    assert by_name["static_source::new"]["previousDecisionType"] == ""


def test_source_decision_trend_handles_missing_previous_payload() -> None:
    trend = benchmark._source_decision_trend(
        [
            {
                "name": "static_source::maliyo",
                "decisionType": "slow_productive_static",
                "keptCount": 7,
                "durationMs": 25346,
                "requiresExplicitDecision": False,
            }
        ],
        None,
    )
    markdown = benchmark._render_source_decision_trend_markdown(trend)

    assert trend["status"] == "no_previous"
    assert trend["newCount"] == 1
    assert "Status: `no_previous`" in markdown
    assert "`slow_productive_static`" in markdown
    assert "Previous decision type: `-`" in markdown
