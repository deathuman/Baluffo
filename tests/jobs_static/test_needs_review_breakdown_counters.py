from src.jobs import reporting as jobs_reporting


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
