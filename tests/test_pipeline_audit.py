from src import pipeline_audit as audit


def test_build_report_classifies_failures_and_slow_entries() -> None:
    discovery_report = {
        "startedAt": "2026-03-19T20:00:00+00:00",
        "finishedAt": "2026-03-19T20:00:30+00:00",
        "summary": {
            "queuedCandidateCount": 3,
            "failedProbeCount": 1,
            "probeMissCount": 0,
            "discoverableButDeferredCount": 2,
            "lossAccounting": {"queueFiltered": 1},
        },
        "runtime": {
            "totalDurationMs": 30000,
            "stageTop": [{"stage": "probe", "durationMs": 18000}],
            "adapterTimings": [{"adapter": "greenhouse", "durationMs": 9000, "queuedCount": 2}],
        },
        "failures": [
            {"name": "Bad Studio", "adapter": "static", "stage": "probe_miss", "dropStage": "probe_failed", "error": "timeout"},
            {"name": "Deferred Studio", "adapter": "greenhouse", "stage": "queue_filtered", "dropStage": "queue_filtered", "error": "low score"},
        ],
        "topFailures": [{"key": "static:bad.example", "count": 1}],
    }
    fetch_report = {
        "startedAt": "2026-03-19T20:01:00+00:00",
        "finishedAt": "2026-03-19T20:04:00+00:00",
        "summary": {"successfulSources": 1, "failedSources": 1, "outputCount": 12},
        "runtime": {
            "slowestSources": [{"name": "static_sources", "adapter": "static", "durationMs": 31000, "keptCount": 0}],
            "timingSummary": {
                "totalDurationMs": 180000,
                "stageTop": [{"stage": "detailFetch", "durationMs": 120000}],
                "slowestAdapters": [{"adapter": "static", "durationMs": 150000, "sourceCount": 4}],
                "highCostLowYieldSources": [{"name": "stormind", "adapter": "static", "durationMs": 25000, "keptCount": 0}],
            },
        },
        "sources": [
            {
                "name": "static_sources",
                "adapter": "static",
                "status": "error",
                "durationMs": 31000,
                "fetchedCount": 25,
                "keptCount": 0,
                "error": "HTTP 403",
                "details": [
                    {
                        "name": "Stormind Games",
                        "adapter": "static",
                        "status": "ok",
                        "classification": "fetch_ok_extract_zero",
                        "fetchedCount": 25,
                        "keptCount": 0,
                        "stats": {"listing_fetch_ms": 10000, "detail_fetch_ms": 15000},
                    }
                ],
            },
            {"name": "greenhouse_boards", "adapter": "greenhouse", "status": "ok", "durationMs": 3000, "fetchedCount": 12, "keptCount": 12},
        ],
        "outputs": {"report": "data/jobs-fetch-report.json"},
    }
    jobs = [{"id": "1"}, {"id": "2"}]

    report = audit.build_report(discovery_report, fetch_report, jobs)

    assert int((report.get("totals") or {}).get("totalJobs") or 0) == 2
    assert str((((report.get("fetch") or {}).get("slowestAdapters") or [])[0].get("adapter")) or "") == "static"
    assert any(str(row.get("category") or "") == "fetch_source_error" for row in (report.get("issues") or {}).get("hard_failures", []))
    assert any(str(row.get("category") or "") == "fetch_ok_extract_zero" for row in (report.get("issues") or {}).get("soft_failures", []))
    assert any(str(row.get("category") or "") == "slow_low_yield" for row in (report.get("issues") or {}).get("high_cost_low_yield", []))
    assert any(str(row.get("category") or "") == "queue_filtered" for row in (report.get("issues") or {}).get("coverage_risks", []))


def test_render_markdown_includes_key_sections() -> None:
    report = {
        "generatedAt": "2026-03-19T20:00:00+00:00",
        "discovery": {"totalDurationMs": 1000, "stageTop": [], "queueFilteredCount": 0, "discoverableButDeferredCount": 0},
        "fetch": {"totalDurationMs": 2000, "slowestAdapters": [], "slowestSourceLoaders": [], "slowestSourceEntries": [], "productiveExpensiveSources": []},
        "totals": {"totalJobs": 3},
        "issues": {"hard_failures": [], "soft_failures": [], "high_cost_low_yield": [], "coverage_risks": []},
        "recommendations": ["Do the next thing."],
    }
    text = audit.render_markdown(report)
    assert "Pipeline Audit Report" in text
    assert "Executive summary" in text
    assert "Recommendations" in text


def test_build_report_includes_productive_expensive_sources() -> None:
    discovery_report = {"summary": {}, "runtime": {}, "failures": [], "topFailures": []}
    fetch_report = {
        "summary": {"successfulSources": 1, "failedSources": 0, "outputCount": 50},
        "runtime": {
            "slowestSources": [],
            "timingSummary": {"totalDurationMs": 100000, "wallClockDurationMs": 45000, "stageTop": [], "slowestAdapters": [], "highCostLowYieldSources": [], "detailHeavySources": []},
        },
        "sources": [
            {"name": "static_source::cygames", "adapter": "static", "status": "ok", "durationMs": 50000, "fetchedCount": 100, "keptCount": 80},
        ],
        "outputs": {"report": "data/jobs-fetch-report.json"},
    }

    report = audit.build_report(discovery_report, fetch_report, [{"id": "1"}])

    productive = ((report.get("fetch") or {}).get("productiveExpensiveSources") or [])
    assert len(productive) == 1
    assert str(productive[0].get("name") or "") == "static_source::cygames"
    assert int((report.get("totals") or {}).get("fetchWallClockDurationMs") or 0) == 45000
