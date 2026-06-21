from __future__ import annotations

from src import fetch_incremental_sanity_benchmark as benchmark


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
