from __future__ import annotations

from src import fetch_incremental_sanity_benchmark as benchmark


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
                    "error": "static_source::maliyo: static:Maliyo Games (Sheet):https://www.maliyo.com/career/: time budget exceeded (25s)",
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
    assert super_lucky["evidence"]["registryPageEvidence"]["offListingHosts"] == ["stillfront.com"]
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
        "timeoutUrlCount": 1,
        "timeoutUrls": ["https://www.maliyo.com/career/"],
        "firstTimeoutUrl": "https://www.maliyo.com/career/",
        "lastTimeoutUrl": "https://www.maliyo.com/career/",
        "timeoutUrlRoleCounts": {"detail_or_registry_page": 1},
        "listingTimeouts": {
            "timeoutUrlCount": 0,
            "timeoutUrls": [],
            "firstTimeoutUrl": "",
            "lastTimeoutUrl": "",
        },
        "detailTimeouts": {
            "timeoutUrlCount": 1,
            "timeoutUrls": ["https://www.maliyo.com/career/"],
            "firstTimeoutUrl": "https://www.maliyo.com/career/",
            "lastTimeoutUrl": "https://www.maliyo.com/career/",
        },
        "detailPagesVisited": 3,
        "detailYieldPct": 100,
    }

    netflix = by_name["static_source::netflix"]
    assert netflix["decisionType"] == "follow_up_review"
    assert netflix["requiresExplicitDecision"] is False
    assert netflix["evidence"]["zeroKeptClassification"] == "needs_review"


def test_source_decision_matrix_markdown_renders_operator_review_evidence() -> None:
    off_host = ".".join(("stillfront", "com"))
    lucky_host = ".".join(("superluckycasino", "com"))
    scope_host = ".".join(("careerviet", "vn"))
    expected_kept_hosts = f"Kept output hosts: {off_host}=2, {lucky_host}=1"
    expected_timeout_url = "https://www.maliyo.com/career/"
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
    assert off_host in markdown
    assert scope_host in markdown
    assert "Policy decision needed: `true`" in markdown
    assert "Suggested source-policy decision: `split_source`" in markdown
    assert expected_kept_hosts in markdown
    assert "Error samples: first, second, third" in markdown
    assert "Detail timing: `pages=3, yield=100%`" in markdown
    assert "Timeout diagnostics: `timeouts=1, network=0, timeoutUrls=1`" in markdown
    assert "Timeout URL roles: listing=1" in markdown
    assert expected_timeout_url in markdown
