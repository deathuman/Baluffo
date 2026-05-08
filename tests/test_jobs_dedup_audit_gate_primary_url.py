from __future__ import annotations

from src.jobs.reporting_dedup_evidence import build_dedup_evidence


def test_dedup_audit_gate_warns_on_current_primary_url_merges() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 1, "mergedByPrimaryUrl": 1},
        [],
    )

    gate = evidence["dedupAuditGate"]
    assert gate["status"] == "warning"
    assert gate["lifecycleUxReady"] is True
    assert "current_run_primary_url_merges_present" in gate["warnings"]
