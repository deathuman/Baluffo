"""The retry snapshot stores a stageable candidate whole and stages it unchanged."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.bridge.discovery_registry_retry_queue import retry_report_snapshot
from src.source_registry import apply_discovery_auto_approval

_NOW = "2026-07-01T10:00:00+00:00"


def _cap_deferred_row() -> dict[str, Any]:
    return {
        "id": "cap-row",
        "adapter": "static",
        "name": "Cap Studio",
        "studio": "Cap Studio",
        "company": "Cap Studio",
        "careersUrl": "https://cap.example/careers",
        "listing_url": "https://cap.example/jobs",
        "url": "https://cap.example/",
        "pages": ["https://cap.example/careers", "https://cap.example/jobs"],
        "jobsFound": 3,
        "sampleCount": 3,
        "weakSignal": False,
        "deferred": True,
        "deferReason": "adapter_cap",
        "dropReason": "",
        "candidateState": "validated",
        "status": "healthy",
        "lastProbeError": "",
        "lastProbedAt": "2026-07-01T09:30:00+00:00",
        "lastProbeStatus": "ok",
        "evidenceTypes": ["seed", "sitemap"],
        "rankScore": 42,
        "rankReasons": ["medium_confidence", "jobs_found_bonus"],
        "reasons": ["medium_confidence", "jobs_found_bonus"],
        "deferCount": 2,
        "firstDeferredAt": "2026-06-01T09:00:00+00:00",
        "lastDeferredAt": "2026-07-01T09:00:00+00:00",
        "confidence": "medium",
        "evidenceScore": 26,
        "promotionLane": "structured_batch",
        "score": 40,
        "discoveredAt": "2026-06-01T09:00:00+00:00",
        "discoveryMethod": "provider_pattern",
        "discoveryStage": "provider_pattern",
        "evidenceSource": "seed",
        "ok": True,
        "httpStatus": 200,
        "lastCheckedAt": "2026-07-01T09:30:00+00:00",
        "coverageAuditOpenings": 0,
        "nlPriority": 3,
        "sourceDirectory": "curated",
        "sourceDirectoryEntryUrl": "https://cap.example/directory",
        "sourceDirectoryUrl": "https://directory.example/cap",
        "demotionNote": "kept for review",
        "remoteFriendly": True,
        "residualFailureClass": "adapter_cap",
        "residualFailureEvidence": "cap reached",
        "registryState": "pending",
        "stateChangedAt": "2026-06-01T09:00:00+00:00",
        "hiddenFromDefault": True,
        "migrationSourceIdentity": "old-cap",
        "providerCoverageOpenings": 12,
        "selectedStaticSourceId": "cap-static",
        "adminBackfillOwned": True,
    }


def test_stageable_candidate_is_stored_whole_and_approves_identically(tmp_path: Path) -> None:
    """A cap-deferred promotion stages the same active row from the snapshot."""
    full_row = _cap_deferred_row()
    snapshot_row = retry_report_snapshot(
        {
            "startedAt": "2026-07-01T09:00:00+00:00",
            "finishedAt": _NOW,
            "summary": {"approvedCandidateCount": 0, "liveCandidateCount": 0},
            "runtime": {
                "autoApproval": {"enabled": True, "approvedCount": 0, "status": "completed"}
            },
            "candidates": [dict(full_row)],
        }
    )["candidates"][0]
    assert snapshot_row == full_row

    def approved_state(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        report = {
            "runId": "run-equivalent",
            "startedAt": "2026-07-01T09:00:00+00:00",
            "finishedAt": _NOW,
            "summary": {"approvedCandidateCount": 0, "liveCandidateCount": 0},
            "runtime": {
                "autoApproval": {"enabled": True, "approvedCount": 0, "status": "completed"}
            },
            "candidates": [dict(row)],
        }
        state: dict[str, list[dict[str, Any]]] = {"active": [], "pending": [], "rejected": []}
        next_state, _approved = apply_discovery_auto_approval(
            state,
            report,
            auto_approve_enabled=True,
            approval_state_path=tmp_path / "source-approval-state.json",
            record_approval_state=False,
            now_iso_fn=lambda: _NOW,
        )
        return next_state

    assert approved_state(snapshot_row) == approved_state(full_row)
