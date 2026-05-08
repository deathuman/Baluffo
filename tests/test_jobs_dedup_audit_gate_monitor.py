from __future__ import annotations

from src.jobs.reporting_dedup_evidence import build_dedup_audit_gate


def test_dedup_audit_gate_warns_but_does_not_block_monitor_only_pressure() -> None:
    gate = build_dedup_audit_gate(
        {
            "mergedCount": 0,
            "currentRunHighRiskReviewQueueCount": 10,
            "carriedHighRiskReviewQueueCount": 2,
            "currentRunBlockingReviewQueueCount": 0,
            "carriedBlockingReviewQueueCount": 0,
            "currentRunMonitorReviewQueueCount": 10,
            "carriedMonitorReviewQueueCount": 2,
            "reviewQueueCauseCounts": {
                "unknown": 12,
            },
        }
    )

    assert gate["status"] == "warning"
    assert gate["lifecycleUxReady"] is True
    assert gate["blockingReviewQueueCount"] == 0
    assert gate["monitorReviewQueueCount"] == 12
    assert "high_risk_review_queue_causes_need_review" not in gate["blockers"]
    assert "monitor_review_queue_diagnostics_present" in gate["warnings"]
