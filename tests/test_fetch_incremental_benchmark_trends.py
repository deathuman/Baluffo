from __future__ import annotations

from src import fetch_incremental_sanity_benchmark as benchmark


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
