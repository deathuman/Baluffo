from __future__ import annotations

from src.source_discovery import audit_report_summary


def test_audit_report_summary_active_candidate_split_handles_malformed_counts() -> None:
    split = audit_report_summary.active_candidate_split(
        {
            "activeCandidates": "7",
            "activeAdapterCounts": {"static": "3"},
        }
    )

    assert split == {
        "activeCandidates": 7,
        "activeProviderCandidates": 4,
        "activeStaticCandidates": 3,
    }

    malformed = audit_report_summary.active_candidate_split(
        {
            "activeCandidates": "bad",
            "activeAdapterCounts": "bad",
        }
    )

    assert malformed == {
        "activeCandidates": 0,
        "activeProviderCandidates": 0,
        "activeStaticCandidates": 0,
    }


def test_audit_report_summary_top_failure_buckets_combines_and_caps_counts() -> None:
    buckets = audit_report_summary.top_failure_buckets(
        rejected_reason_detail_counts={
            "js_shell": 4,
            "recovery_fetch_failed": 3,
            "": 99,
        },
        failure_counts={
            "homepage_fetch": 5,
            "probe": 2,
            "ignored": 1,
            "malformed": "bad",
        },
        limit=3,
    )

    assert buckets == [
        {"key": "js_shell", "count": 4},
        {"key": "recovery_fetch_failed", "count": 3},
        {"key": "homepage_fetch", "count": 5},
    ]


def test_audit_report_summary_artifact_size_prefers_summary_then_runtime() -> None:
    assert (
        audit_report_summary.artifact_size_bytes(
            summary={"artifactSizeBytes": "123"},
            runtime={"artifactSizeBytes": 456},
        )
        == 123
    )
    assert (
        audit_report_summary.artifact_size_bytes(
            summary={},
            runtime={"artifactSizeBytes": "456"},
        )
        == 456
    )
    assert audit_report_summary.artifact_size_bytes(summary={}, runtime={}) == 0
