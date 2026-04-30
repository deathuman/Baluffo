from src import fetcher_metrics as fm


def test_build_metrics_computes_duplicate_and_history_stats() -> None:
    report = {
        "startedAt": "2026-03-09T10:00:00+00:00",
        "finishedAt": "2026-03-09T10:02:00+00:00",
        "runtime": {
            "timingSummary": {
                "totalDurationMs": 50,
                "medianSourceDurationMs": 25,
                "p95SourceDurationMs": 40,
                "stageTotalsMs": {"fetchAndParse": 35, "canonicalization": 15},
                "stageTop": [{"stage": "fetchAndParse", "durationMs": 35}],
                "adapterTimings": [{"adapter": "static", "durationMs": 40, "sourceCount": 2}],
                "slowestAdapters": [{"adapter": "static", "durationMs": 40, "sourceCount": 2}],
                "highCostLowYieldSources": [{"name": "b", "durationMs": 40, "keptCount": 0}],
            }
        },
        "summary": {"inputCount": 10, "mergedCount": 2, "outputCount": 8},
        "sources": [
            {"name": "a", "status": "ok", "durationMs": 10, "keptCount": 8},
            {
                "name": "b",
                "status": "error",
                "durationMs": 40,
                "keptCount": 0,
                "failureBucket": "timeout",
                "browserFallbackRecommended": True,
            },
            {"name": "c", "status": "excluded", "durationMs": 0, "exclusionReason": "cache_skip"},
        ],
        "providerStaticOverlap": {
            "suppressedStaticCount": 1,
            "auditedPairCount": 1,
            "safePairCount": 1,
            "pairs": [
                {
                    "staticSourceName": "static_source::covered",
                    "providerSourceName": "Studio Greenhouse",
                    "auditStatus": "safe",
                }
            ],
        },
        "staticSuppressionPolicy": {
            "eligibleCount": 1,
            "suppressedCount": 1,
            "pausedCount": 0,
            "warningCount": 0,
            "suppressedPairs": [
                {
                    "staticSourceName": "static_source::covered",
                    "providerSourceName": "Studio Greenhouse",
                    "decision": "suppressed",
                    "reason": "prior_audit_safe",
                    "lastAuditStatus": "safe",
                }
            ],
            "pausedPairs": [],
            "warningPairs": [],
        },
    }
    history = [
        {"type": "fetch", "durationMs": 1000, "finishedAt": "2026-03-09T10:02:00+00:00"},
        {"type": "fetch", "durationMs": 3000, "finishedAt": "2026-03-09T09:02:00+00:00"},
        {"type": "discovery", "durationMs": 4000, "finishedAt": "2026-03-09T08:02:00+00:00"},
    ]
    metrics = fm.build_metrics(report, history, window=5)
    latest = metrics["latestRun"]
    assert latest["duplicateRate"] == 0.2
    assert latest["outputYieldRate"] == 0.8
    assert latest["sourceFailureRate"] == 0.3333
    assert latest["failedSources"] == 1
    assert latest["durationMs"] == 50
    assert latest["medianSourceDurationMs"] == 25
    assert latest["p95SourceDurationMs"] == 40
    assert latest["stageTop"][0]["stage"] == "fetchAndParse"
    assert latest["slowestSources"][0]["name"] == "b"
    assert latest["sourceHealth"]["totalSources"] == 3
    assert latest["sourceHealth"]["browserFallbackRecommendedSources"] == 1
    assert latest["sourceHealth"]["sourcesNeedingAttention"][0]["name"] == "b"
    assert latest["providerStaticOverlap"]["safePairCount"] == 1
    assert latest["providerStaticOverlap"]["pairs"][0]["auditStatus"] == "safe"
    assert latest["staticSuppressionPolicy"]["suppressedCount"] == 1
    assert metrics["history"]["windowRuns"] == 2
    assert metrics["history"]["medianDurationMs"] == 2000


def test_sanitize_source_label_removes_control_chars_and_truncates() -> None:
    raw = "bad\x00source\tname\nwith\rcntl and a very very very very very very very long tail"
    clean = fm.sanitize_source_label(raw, max_len=32)
    assert "\x00" not in clean
    assert "\n" not in clean
    assert "\r" not in clean
    assert len(clean) <= 32


def test_sanitize_source_label_normalizes_static_source_listing_prefix() -> None:
    raw = "static_source::static:listing_url:https://studio.example.com/careers/jobs?utm=x"
    clean = fm.sanitize_source_label(raw)
    assert clean.startswith("static:studio.example.com/careers/jobs")
