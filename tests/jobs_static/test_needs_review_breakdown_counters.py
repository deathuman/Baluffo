from tests.helpers import jobs_reporting


def test_needs_review_breakdown_reports_raw_and_included_counts() -> None:
    source_reports = [
        {
            "name": "static_source::static:listing_url:https://example.com/a",
            "adapter": "static",
            "studio": "Example A",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 1000,
            "error": "static:Example A (Manual Website): no jobs extracted from source pages",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/b",
            "adapter": "static",
            "studio": "Example B",
            "status": "ok",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 2,
            "durationMs": 25,
            "error": "warning carried on successful source",
        },
        {
            "name": "social_x",
            "adapter": "social",
            "studio": "x",
            "status": "error",
            "classification": "needs_review",
            "keptCount": 0,
            "durationMs": 20,
            "error": "social source marker is raw-only for this static breakdown",
        },
    ]

    breakdown = jobs_reporting.build_needs_review_breakdown(source_reports)

    assert breakdown["byShape"]["no_jobs_extracted"]["count"] == 1
    assert breakdown["rawMarkerCount"] == 3
    assert breakdown["includedCount"] == 1


def test_pipeline_summary_counts_clean_ok_and_ok_with_warnings_separately() -> None:
    summary = jobs_reporting.build_pipeline_summary(
        {},
        [],
        [
            {"name": "clean", "status": "ok", "fetchedCount": 3, "keptCount": 3},
            {
                "name": "warning",
                "status": "ok",
                "fetchedCount": 2,
                "keptCount": 1,
                "warnings": ["detail timeout"],
            },
            {"name": "failed", "status": "error", "error": "boom"},
        ],
        0,
        False,
        2,
        0,
        0,
        json_bytes=0,
        csv_bytes=0,
        light_json_bytes=0,
    )

    assert summary["successfulSources"] == 2
    assert summary["okCleanSources"] == 1
    assert summary["okWithWarningSources"] == 1
    assert summary["failedSources"] == 1


def test_pipeline_summary_reports_output_size_guardrails() -> None:
    summary = jobs_reporting.build_pipeline_summary(
        {},
        [],
        [{"name": "clean", "status": "ok", "fetchedCount": 1, "keptCount": 1}],
        0,
        False,
        1,
        0,
        0,
        json_bytes=80_000_001,
        light_json_bytes=60_000_001,
        csv_bytes=50_000_001,
    )

    assert summary["sizeGuardrailExceeded"] is True
    assert summary["sizeGuardrails"] == {
        "json": {"bytes": 80_000_001, "limitBytes": 80_000_000, "exceeded": True},
        "lightJson": {"bytes": 60_000_001, "limitBytes": 60_000_000, "exceeded": True},
        "csv": {"bytes": 50_000_001, "limitBytes": 50_000_000, "exceeded": True},
    }
