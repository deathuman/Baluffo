from __future__ import annotations

from scripts import audit_diff


def test_compare_reports_flags_audit_positive_full_zero_mismatch() -> None:
    full_report = {
        "sources": [
            {
                "name": "workable_sources",
                "adapter": "workable",
                "status": "error",
                "keptCount": 0,
                "fetchedCount": 124,
            },
            {
                "name": "personio_sources",
                "adapter": "personio",
                "status": "ok",
                "keptCount": 3,
                "fetchedCount": 3,
            },
        ]
    }
    audit_report = {
        "results": [
            {"adapter": "workable", "jobsCount": 124, "bucket": "working", "durationMs": 10},
            {
                "adapter": "personio",
                "jobsCount": 0,
                "bucket": "adapter-broken",
                "durationMs": 12,
            },
        ]
    }

    report = audit_diff.compare_reports(full_report=full_report, audit_report=audit_report)

    assert report["summary"]["adapterCount"] == 2
    assert report["summary"]["mismatchCount"] == 2
    rows = {row["adapter"]: row for row in report["rows"]}
    assert rows["workable"]["mismatch"] is True
    assert "audit_positive_full_zero" in rows["workable"]["mismatchReasons"]
    assert rows["personio"]["mismatch"] is True
    assert "audit_zero_full_positive" in rows["personio"]["mismatchReasons"]


def test_render_table_includes_adapter_summary() -> None:
    report = {
        "summary": {"adapterCount": 1, "mismatchCount": 0},
        "rows": [
            {
                "adapter": "breezy",
                "auditJobsCount": 2,
                "fullKeptCount": 2,
                "fullStatusSummary": "ok:1",
                "mismatch": False,
                "mismatchReasons": [],
            }
        ],
    }

    output = audit_diff.render_table(report)

    assert "breezy | 2 | 2 | ok:1 | no" in output
