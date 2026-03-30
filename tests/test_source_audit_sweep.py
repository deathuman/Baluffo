from __future__ import annotations

from src import source_audit_sweep as sweep


def test_build_report_classifies_active_sources_and_mismatches() -> None:
    fetch_report = {
        "runId": "fetch_test",
        "startedAt": "2026-03-30T08:43:11+00:00",
        "finishedAt": "2026-03-30T08:50:20+00:00",
        "sources": [
            {
                "name": "google_sheets",
                "adapter": "csv",
                "status": "ok",
                "keptCount": 3,
                "fetchedCount": 3,
                "details": [
                        {
                            "name": "google_sheets_1er2oaxo",
                            "adapter": "csv",
                            "status": "ok",
                            "keptCount": 3,
                            "fetchedCount": 3,
                        }
                ],
            },
            {
                "name": "static_source::static:listing_url:https://dead.example/jobs",
                "adapter": "static",
                "status": "ok",
                "keptCount": 0,
                "fetchedCount": 0,
                "details": [
                    {
                        "name": "Dead Studio",
                        "adapter": "static",
                        "status": "ok",
                        "keptCount": 0,
                        "fetchedCount": 0,
                        "failureBucket": "dead_listing_page",
                        "zeroKeptClassification": "dead_listing_page",
                    }
                ],
            },
            {
                "name": "static_source::static:listing_url:https://stale.example/jobs",
                "adapter": "static",
                "status": "ok",
                "keptCount": 2,
                "fetchedCount": 2,
                "details": [
                    {
                        "name": "Stale Studio",
                        "adapter": "static",
                        "status": "ok",
                        "keptCount": 2,
                        "fetchedCount": 2,
                    }
                ],
            },
            {
                "name": "greenhouse_boards",
                "adapter": "greenhouse",
                "status": "error",
                "error": "HTTP 500",
                "keptCount": 0,
                "fetchedCount": 0,
                "details": [
                    {
                        "name": "Broken Board",
                        "adapter": "greenhouse",
                        "status": "error",
                        "error": "HTTP 500",
                        "keptCount": 0,
                        "fetchedCount": 0,
                    }
                ],
            },
            {
                "name": "lever_sources",
                "adapter": "lever",
                "status": "ok",
                "keptCount": 0,
                "fetchedCount": 0,
                "details": [
                    {
                        "name": "Review Board",
                        "adapter": "lever",
                        "status": "ok",
                        "keptCount": 0,
                        "fetchedCount": 0,
                        "classification": "needs_review",
                    }
                ],
            },
        ],
    }
    source_state = {
        "google_sheets_1er2oaxo": {"lastStatus": "ok", "lastKeptCount": 3, "lastAdapter": "csv"},
        "Dead Studio": {"lastStatus": "ok", "lastKeptCount": 0, "lastAdapter": "static"},
        "Stale Studio": {"lastStatus": "ok", "lastKeptCount": 2, "lastAdapter": "static"},
        "Broken Board": {"lastStatus": "error", "lastKeptCount": 0, "lastAdapter": "greenhouse"},
        "Review Board": {"lastStatus": "ok", "lastKeptCount": 0, "lastAdapter": "lever"},
        "Registry Current": {"lastStatus": "ok", "lastKeptCount": 4, "lastAdapter": "static"},
        "Registry Empty": {"lastStatus": "ok", "lastKeptCount": 0, "lastAdapter": "static"},
    }
    registry_active = [
        {"name": "google_sheets_1er2oaxo", "adapter": "csv", "candidateState": "live"},
        {"name": "Dead Studio", "adapter": "static", "candidateState": "live"},
        {"name": "Stale Studio", "adapter": "static", "candidateState": "live"},
        {"name": "Broken Board", "adapter": "greenhouse", "candidateState": "live"},
        {"name": "Review Board", "adapter": "lever", "candidateState": "live"},
        {"name": "Registry Current", "adapter": "static", "candidateState": "live"},
        {"name": "Registry Empty", "adapter": "static", "candidateState": "live"},
    ]
    unified_light = [
        {"source": "google_sheets_1er2oaxo"},
        {"source": "google_sheets_1er2oaxo"},
        {"source": "google_sheets_1er2oaxo"},
        {"source": "Registry Current"},
        {"source": "Registry Current"},
        {"source": "Registry Current"},
        {"source": "Registry Current"},
    ]
    report = sweep.build_report(
        fetch_report=fetch_report,
        source_state=source_state,
        registry_active=registry_active,
        unified_light=unified_light,
        manifest={"lastRunId": "20260330_084311", "status": "success"},
    )

    assert report["manifestFresh"] is False
    assert report["scope"]["manifestStale"] is True
    totals = report["totals"]
    assert int(totals["sourceCount"]) == 7
    assert int(totals["workingCount"]) == 2
    assert int(totals["deadListingPageCount"]) == 1
    assert int(totals["needsReviewCount"]) == 2
    assert int(totals["errorCount"]) == 1
    assert int(totals["staleMaterializationCount"]) == 1
    assert int(totals["registryOnlyCount"]) == 2
    assert int(totals["reportOnlyCount"]) == 0

    rows = {row["name"]: row for row in report["sources"]}
    assert rows["google_sheets_1er2oaxo"]["classification"] == "working"
    assert rows["Dead Studio"]["classification"] == "dead_listing_page"
    assert rows["Stale Studio"]["classification"] == "stale_materialization"
    assert rows["Broken Board"]["classification"] == "error"
    assert rows["Review Board"]["classification"] == "needs_review"
    assert rows["Registry Current"]["classification"] == "working"
    assert rows["Registry Empty"]["classification"] == "needs_review"
    assert rows["Registry Current"]["reportPresent"] is False
    assert rows["Registry Current"]["unifiedCount"] == 4
    assert rows["Registry Current"]["materializationGap"] == 4

    families = {row["family"]: row for row in report["families"]}
    assert int(families["google_sheets"]["sourceCount"]) == 1
    assert int(families["static"]["sourceCount"]) == 4
    assert int(families["greenhouse"]["errorCount"]) == 1
    assert int(families["lever"]["needsReviewCount"]) == 1

    issues = report["issues"]
    assert [row["name"] for row in issues["stale_materialization"]] == ["Stale Studio"]
    assert [row["name"] for row in issues["broken_source"]] == ["Broken Board"]
    assert [row["name"] for row in issues["needs_review"]] == ["Registry Empty", "Review Board"]
    assert [row["name"] for row in issues["dead_listing_page"]] == ["Dead Studio"]


def test_render_markdown_includes_summary_sections() -> None:
    report = {
        "generatedAt": "2026-03-30T08:55:00+00:00",
        "manifest": {"lastRunId": "20260330_084311", "status": "success"},
        "scope": {
            "fetchRunId": "fetch_test",
            "fetchStartedAt": "2026-03-30T08:43:11+00:00",
            "fetchFinishedAt": "2026-03-30T08:50:20+00:00",
            "manifestRunId": "20260330_084311",
            "manifestFresh": False,
            "manifestStale": True,
            "reportSourceCount": 2,
            "registryActiveCount": 2,
            "sourceStateCount": 2,
            "unifiedLightCount": 2,
        },
        "totals": {
            "sourceCount": 2,
            "workingCount": 1,
            "deadListingPageCount": 0,
            "needsReviewCount": 1,
            "errorCount": 0,
            "staleMaterializationCount": 0,
            "reportMissingCount": 0,
            "registryOnlyCount": 0,
            "reportOnlyCount": 0,
        },
        "families": [
            {
                "family": "google_sheets",
                "sourceCount": 1,
                "workingCount": 1,
                "deadListingPageCount": 0,
                "needsReviewCount": 0,
                "errorCount": 0,
                "staleMaterializationCount": 0,
            }
        ],
        "issues": {
            "stale_materialization": [],
            "broken_source": [],
            "needs_review": [{"name": "Needs Review", "family": "static", "classification": "needs_review"}],
            "dead_listing_page": [],
        },
    }

    text = sweep.render_markdown(report)
    assert "Active Source Audit Sweep" in text
    assert "Latest fetch" in text
    assert "Manifest snapshot" in text
    assert "stale (fetch run fetch_test)" in text
    assert "Family summary" in text
    assert "Follow-up buckets" in text
    assert "Needs review" in text
