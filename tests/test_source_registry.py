import json
from pathlib import Path

from src import source_registry as sr
from tests.helpers.temp_paths import workspace_tmpdir


def test_source_identity_uses_explicit_id_when_present() -> None:
    row = {"id": "lever:account:sandboxvr", "adapter": "lever", "account": "sandboxvr"}
    token = sr.source_identity(row)
    assert token == "lever:account:sandboxvr"


def test_source_identity_prefers_adapter_keyed_fields() -> None:
    row = {"adapter": "lever", "account": "sandboxvr"}
    token = sr.source_identity(row)
    assert "lever:account:sandboxvr" in token


def test_unique_sources_deduplicates_by_identity() -> None:
    rows = [
        {"adapter": "workable", "account": "hutch"},
        {"adapter": "workable", "account": "hutch"},
        {"adapter": "workable", "account": "wargaming"},
    ]
    deduped = sr.unique_sources(rows)
    assert len(deduped) == 2


def test_save_json_atomic_and_load_array() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        path = Path(tmp) / "registry.json"
        payload = [{"adapter": "smartrecruiters", "company_id": "Gameloft"}]
        sr.save_json_atomic(path, payload)
        assert path.read_text(encoding="utf-8").endswith("\n")
        journal_path = Path(tmp) / "registry.jsonl"
        assert not journal_path.exists()
        loaded = sr.load_json_array(path, [])
        assert loaded == payload


def test_save_json_atomic_and_load_object() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        path = Path(tmp) / "source-approval-state.json"
        payload = {
            "approvedSinceLastRun": 1,
            "updatedAt": "2026-04-01T00:00:00+00:00",
        }
        sr.save_json_atomic(path, payload)
        journal_path = Path(tmp) / "source-approval-state.jsonl"
        assert not journal_path.exists()
        assert sr.load_json_object(path, {}) == payload


def test_normalize_source_url_trims_query_trailing_slash_and_case() -> None:
    normalized = sr.normalize_source_url("HTTPS://Jobs.Ashbyhq.com/Acme/jobs/?foo=1#frag")
    assert normalized == "https://jobs.ashbyhq.com/Acme/jobs"


def test_source_url_fingerprint_prefers_endpoint_fields() -> None:
    row = {
        "adapter": "workable",
        "account": "acme",
        "api_url": "https://apply.workable.com/api/v1/widget/accounts/acme/?details=true",
    }
    assert (
        sr.source_url_fingerprint(row) == "https://apply.workable.com/api/v1/widget/accounts/acme"
    )


def test_source_url_fingerprint_uses_static_pages_when_no_endpoint_field() -> None:
    row = {
        "adapter": "static",
        "pages": ["https://milestone.it/careers/?utm_source=x"],
    }
    assert sr.source_url_fingerprint(row) == "https://milestone.it/careers"


def test_canonicalize_registry_row_backfills_active_transition_metadata() -> None:
    row = {
        "adapter": "static",
        "listing_url": "https://example.com/jobs",
        "approvedAt": "2026-04-09T10:00:00+00:00",
        "liveAt": "2026-04-09T10:05:00+00:00",
    }
    normalized = sr.canonicalize_registry_row(row, bucket="active")
    assert normalized["registryState"] == "active"
    assert normalized["stateChangedAt"] == "2026-04-09T10:00:00+00:00"
    assert normalized["stateChangedBy"] == sr.REGISTRY_MIGRATION_V2
    assert normalized["lastPromotedAt"] == "2026-04-09T10:00:00+00:00"
    assert normalized["approvedAt"] == "2026-04-09T10:00:00+00:00"
    assert normalized["approvedBy"] == sr.REGISTRY_MIGRATION_V2
    assert normalized["liveAt"] == "2026-04-09T10:05:00+00:00"
    assert normalized["pendingReason"] == ""


def test_canonicalize_registry_row_backfills_pending_transition_metadata() -> None:
    row = {
        "adapter": "teamtailor",
        "name": "Pending Row",
        "lastDemotedAt": "2026-04-09T11:00:00+00:00",
    }
    normalized = sr.canonicalize_registry_row(row, bucket="pending")
    assert normalized["registryState"] == "pending"
    assert normalized["stateChangedAt"] == "2026-04-09T11:00:00+00:00"
    assert normalized["stateChangedBy"] == sr.REGISTRY_MIGRATION_V2
    assert normalized["lastDemotedAt"] == "2026-04-09T11:00:00+00:00"
    assert normalized["pendingReason"] == sr.REGISTRY_REASON_PENDING_DEFAULT


def test_apply_discovery_auto_approval_updates_state_report_and_is_idempotent() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        approval_path = Path(tmp) / "source-approval-state.json"
        state = {
            "active": [
                {
                    "id": "active-1",
                    "adapter": "static",
                    "name": "Already Live",
                    "candidateState": "live",
                }
            ],
            "pending": [
                {
                    "id": "pending-ok",
                    "adapter": "greenhouse",
                    "name": "Healthy Pending",
                    "jobsFound": 3,
                    "sampleCount": 3,
                    "weakSignal": False,
                    "evidenceScore": 24,
                    "confidence": "medium",
                    "rankScore": 24,
                    "rankReasons": ["structured_batch_family", "jobs_found_bonus"],
                    "promotionLane": "structured_batch",
                    "status": "healthy",
                },
                {
                    "id": "pending-static",
                    "adapter": "static",
                    "name": "Static Pending",
                    "jobsFound": 3,
                    "status": "healthy",
                    "confidence": "high",
                    "rankScore": 82,
                    "rankReasons": ["live_jobs_detected"],
                    "promotionLane": "manual_review",
                },
                {
                    "id": "pending-bamboo",
                    "adapter": "bamboohr",
                    "name": "Bamboo Pending",
                    "jobsFound": 2,
                    "status": "healthy",
                    "confidence": "medium",
                    "rankScore": 79,
                    "rankReasons": ["structured_family", "jobs_found_bonus"],
                    "promotionLane": "manual_review",
                },
            ],
            "rejected": [],
        }
        report = {
            "summary": {
                "queuedCandidateCount": 1,
                "approvedCandidateCount": 0,
                "liveCandidateCount": 0,
            },
            "runtime": {},
            "candidates": [
                {
                    "id": "pending-ok",
                    "adapter": "greenhouse",
                    "name": "Healthy Pending",
                    "jobsFound": 3,
                    "sampleCount": 3,
                    "weakSignal": False,
                    "evidenceScore": 24,
                    "confidence": "medium",
                    "rankScore": 24,
                    "rankReasons": ["structured_batch_family", "jobs_found_bonus"],
                    "promotionLane": "structured_batch",
                    "status": "healthy",
                },
                {
                    "id": "pending-static",
                    "adapter": "static",
                    "name": "Static Pending",
                    "jobsFound": 3,
                    "confidence": "high",
                    "rankScore": 82,
                    "rankReasons": ["live_jobs_detected"],
                    "promotionLane": "manual_review",
                },
                {
                    "id": "pending-bamboo",
                    "adapter": "bamboohr",
                    "name": "Bamboo Pending",
                    "jobsFound": 2,
                    "confidence": "medium",
                    "rankScore": 79,
                    "rankReasons": ["structured_family", "jobs_found_bonus"],
                    "promotionLane": "manual_review",
                },
            ],
        }

        next_state, approved = sr.apply_discovery_auto_approval(
            state,
            report,
            auto_approve_enabled=True,
            approval_state_path=approval_path,
            now_iso_fn=lambda: "2026-03-20T12:06:00Z",
        )

        assert approved == 3
        assert [row["id"] for row in next_state["active"]] == [
            "active-1",
            "pending-ok",
            "pending-static",
            "pending-bamboo",
        ]
        assert next_state["active"][1]["weakSignal"] is False
        assert [row["id"] for row in next_state["pending"]] == []
        assert report["summary"]["approvedCandidateCount"] == 3
        assert report["summary"]["liveCandidateCount"] == 3
        assert report["runtime"]["autoApproval"] == {"enabled": True, "approvedCount": 3}
        assert report["candidates"][0]["candidateState"] == "live"
        assert report["candidates"][0]["approvedBy"] == "discovery_auto_approve"
        assert report["candidates"][0]["liveAt"] == "2026-03-20T12:06:00Z"
        assert report["candidates"][0]["promotionReason"] == "structured_batch_family"
        assert report["candidates"][1]["candidateState"] == "live"
        assert report["candidates"][1]["approvedBy"] == "discovery_auto_approve"
        assert report["candidates"][1]["liveAt"] == "2026-03-20T12:06:00Z"
        assert report["candidates"][1]["promotionReason"] == "manual_review_only"
        assert report["candidates"][2]["candidateState"] == "live"
        assert report["candidates"][2]["approvedBy"] == "discovery_auto_approve"
        assert report["candidates"][2]["liveAt"] == "2026-03-20T12:06:00Z"
        assert report["candidates"][2]["promotionReason"] == "structured_family_gate"
        assert json.loads(approval_path.read_text(encoding="utf-8")) == {"approvedSinceLastRun": 3}


def test_apply_discovery_auto_approval_ignores_report_domain_cap_deferral_for_clean_pending_row() -> (
    None
):
    with workspace_tmpdir("source-registry") as tmp:
        approval_path = Path(tmp) / "source-approval-state.json"
        state = {
            "active": [],
            "pending": [
                {
                    "id": "workable:account:velanstudios",
                    "adapter": "workable",
                    "name": "Velan Studios, Inc. (Workable)",
                    "jobsFound": 9,
                    "sampleCount": 9,
                    "weakSignal": False,
                    "status": "healthy",
                    "candidateState": "validated",
                }
            ],
            "rejected": [],
        }
        report = {
            "summary": {
                "queuedCandidateCount": 1,
                "approvedCandidateCount": 0,
                "liveCandidateCount": 0,
            },
            "runtime": {},
            "candidates": [
                {
                    "id": "workable:account:velanstudios",
                    "adapter": "workable",
                    "name": "Velan Studios, Inc. (Workable)",
                    "jobsFound": 3,
                    "sampleCount": 3,
                    "weakSignal": False,
                    "deferred": True,
                    "deferReason": "domain_cap",
                    "dropReason": "domain_cap",
                    "promotionLane": "domain_cap_review",
                    "promotionReason": "deferred_candidate",
                    "status": "healthy",
                }
            ],
        }

        next_state, approved = sr.apply_discovery_auto_approval(
            state,
            report,
            auto_approve_enabled=True,
            approval_state_path=approval_path,
            now_iso_fn=lambda: "2026-03-20T12:06:00Z",
        )

        assert approved == 1
        assert [row["id"] for row in next_state["active"]] == ["workable:account:velanstudios"]
        assert next_state["active"][0]["jobsFound"] == 9
        assert next_state["pending"] == []
        assert report["summary"]["approvedCandidateCount"] == 1
        assert report["summary"]["liveCandidateCount"] == 1
        assert report["runtime"]["autoApproval"] == {"enabled": True, "approvedCount": 1}
        assert report["candidates"][0]["candidateState"] == "live"
        assert report["candidates"][0]["approvedBy"] == "discovery_auto_approve"
        assert report["candidates"][0]["liveAt"] == "2026-03-20T12:06:00Z"
        assert json.loads(approval_path.read_text(encoding="utf-8")) == {"approvedSinceLastRun": 1}


def test_apply_discovery_auto_approval_approves_cap_deferred_job_positive_candidate() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        approval_path = Path(tmp) / "source-approval-state.json"
        state = {"active": [], "pending": [], "rejected": []}
        report = {
            "summary": {
                "queuedCandidateCount": 1,
                "approvedCandidateCount": 0,
                "liveCandidateCount": 0,
            },
            "runtime": {},
            "candidates": [
                {
                    "id": "static:listing_url:https://tinybullstudios.com/careers/",
                    "adapter": "static",
                    "name": "Tiny Bull Studios (Gameprog)",
                    "studio": "Tiny Bull Studios",
                    "jobsFound": 3,
                    "sampleCount": 3,
                    "deferred": True,
                    "deferReason": "adapter_cap",
                    "status": "healthy",
                    "weakSignal": False,
                    "rankReasons": ["medium_confidence", "jobs_found_bonus"],
                }
            ],
        }

        next_state, approved = sr.apply_discovery_auto_approval(
            state,
            report,
            auto_approve_enabled=True,
            approval_state_path=approval_path,
            now_iso_fn=lambda: "2026-04-25T10:30:00Z",
        )

        assert approved == 1
        assert [row["id"] for row in next_state["active"]] == [
            "static:listing_url:https://tinybullstudios.com/careers/"
        ]
        assert next_state["active"][0]["stateChangedBy"] == "discovery_auto_approve"
        assert next_state["active"][0]["promotionReason"] == "cap_deferred_jobs_found"
        assert next_state["pending"] == []
        assert report["summary"]["approvedCandidateCount"] == 1
        assert report["summary"]["liveCandidateCount"] == 1
        assert report["runtime"]["autoApproval"] == {"enabled": True, "approvedCount": 1}
        assert report["candidates"][0]["candidateState"] == "live"
        assert report["candidates"][0]["approvedBy"] == "discovery_auto_approve"
        assert json.loads(approval_path.read_text(encoding="utf-8")) == {"approvedSinceLastRun": 1}


def test_apply_discovery_auto_approval_blocks_unsafe_cap_deferred_candidates() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        approval_path = Path(tmp) / "source-approval-state.json"
        state = {"active": [], "pending": [], "rejected": []}

        def candidate(candidate_id: str, **overrides: object) -> dict[str, object]:
            row: dict[str, object] = {
                "id": candidate_id,
                "adapter": "static",
                "name": candidate_id,
                "jobsFound": 2,
                "sampleCount": 2,
                "deferred": True,
                "deferReason": "adapter_cap",
                "status": "healthy",
                "weakSignal": False,
                "rankReasons": ["jobs_found_bonus"],
            }
            row.update(overrides)
            return row

        report = {
            "summary": {
                "queuedCandidateCount": 6,
                "approvedCandidateCount": 0,
                "liveCandidateCount": 0,
            },
            "runtime": {},
            "candidates": [
                candidate("zero-jobs", jobsFound=0, sampleCount=0),
                candidate("errored", status="error"),
                candidate("weak", weakSignal=True),
                candidate("rejected", candidateState="rejected"),
                candidate("non-cap", deferReason="manual_review"),
                candidate(
                    "existing-family", rankReasons=["jobs_found_bonus", "existing_family_match"]
                ),
                candidate(
                    "existing-registry", rankReasons=["jobs_found_bonus", "existing_registry_match"]
                ),
            ],
        }

        next_state, approved = sr.apply_discovery_auto_approval(
            state,
            report,
            auto_approve_enabled=True,
            approval_state_path=approval_path,
            now_iso_fn=lambda: "2026-04-25T10:30:00Z",
        )

        assert approved == 0
        assert next_state["active"] == []
        assert report["summary"]["approvedCandidateCount"] == 0
        assert report["summary"]["liveCandidateCount"] == 0
        assert report["runtime"]["autoApproval"] == {"enabled": True, "approvedCount": 0}
        assert report["candidates"][-2]["promotionReason"] == "skipped_existing_family_match"
        assert report["candidates"][-1]["promotionReason"] == "skipped_existing_family_match"
        assert not approval_path.exists()


def test_apply_discovery_auto_approval_live_shaped_cap_deferred_fixture() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        approval_path = Path(tmp) / "source-approval-state.json"
        state = {"active": [], "pending": [], "rejected": []}
        candidates = []
        for index in range(18):
            rank_reasons = ["medium_confidence", "jobs_found_bonus", "evidence_rank_bonus"]
            if index in {9, 11}:
                rank_reasons.append("existing_family_match")
            candidates.append(
                {
                    "id": f"static:listing_url:https://example.com/studio-{index}/careers",
                    "adapter": "static",
                    "name": f"Studio {index} (Gameprog)",
                    "jobsFound": 3 if index < 10 else 1,
                    "sampleCount": 3 if index < 10 else 1,
                    "deferred": True,
                    "deferReason": "adapter_cap",
                    "status": "healthy",
                    "weakSignal": False,
                    "confidence": "medium",
                    "rankReasons": rank_reasons,
                    "promotionLane": "manual_review",
                }
            )
        report = {
            "summary": {
                "queuedCandidateCount": 113,
                "approvedCandidateCount": 0,
                "liveCandidateCount": 0,
            },
            "runtime": {"autoApproval": {"enabled": True, "approvedCount": 0}},
            "candidates": candidates,
        }

        next_state, approved = sr.apply_discovery_auto_approval(
            state,
            report,
            auto_approve_enabled=True,
            approval_state_path=approval_path,
            now_iso_fn=lambda: "2026-04-25T10:30:00Z",
        )

        assert approved == 16
        assert len(next_state["active"]) == 16
        assert report["summary"]["approvedCandidateCount"] == 16
        assert report["summary"]["liveCandidateCount"] == 16
        assert report["runtime"]["autoApproval"] == {"enabled": True, "approvedCount": 16}
        assert json.loads(approval_path.read_text(encoding="utf-8")) == {"approvedSinceLastRun": 16}


def test_apply_discovery_auto_approval_keeps_failed_or_deferred_candidates_pending() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        approval_path = Path(tmp) / "source-approval-state.json"
        state = {
            "active": [],
            "pending": [
                {
                    "id": "pending-error",
                    "adapter": "greenhouse",
                    "name": "Errored Pending",
                    "jobsFound": 2,
                    "sampleCount": 2,
                    "evidenceScore": 24,
                    "confidence": "medium",
                    "rankScore": 24,
                    "rankReasons": ["structured_batch_family", "jobs_found_bonus"],
                    "promotionLane": "structured_batch",
                    "status": "error",
                    "lastProbeError": "timeout",
                },
                {
                    "id": "pending-blocked",
                    "adapter": "greenhouse",
                    "name": "Blocked Pending",
                    "jobsFound": 2,
                    "sampleCount": 2,
                    "evidenceScore": 24,
                    "confidence": "medium",
                    "rankScore": 24,
                    "rankReasons": ["structured_batch_family", "jobs_found_bonus"],
                    "promotionLane": "structured_batch",
                    "status": "healthy",
                    "candidateState": "quarantined",
                },
                {
                    "id": "pending-deferred",
                    "adapter": "greenhouse",
                    "name": "Deferred Pending",
                    "jobsFound": 2,
                    "sampleCount": 2,
                    "evidenceScore": 24,
                    "confidence": "medium",
                    "rankScore": 24,
                    "rankReasons": ["structured_batch_family", "jobs_found_bonus"],
                    "promotionLane": "structured_batch",
                    "status": "healthy",
                    "weakSignal": True,
                    "deferred": True,
                },
            ],
            "rejected": [],
        }
        report = {
            "summary": {
                "queuedCandidateCount": 2,
                "approvedCandidateCount": 0,
                "liveCandidateCount": 0,
            },
            "runtime": {},
            "candidates": [
                {
                    "id": "pending-error",
                    "adapter": "greenhouse",
                    "name": "Errored Pending",
                    "jobsFound": 2,
                    "sampleCount": 2,
                    "evidenceScore": 24,
                    "confidence": "medium",
                    "rankScore": 24,
                    "rankReasons": ["structured_batch_family", "jobs_found_bonus"],
                    "promotionLane": "structured_batch",
                    "status": "error",
                    "lastProbeError": "timeout",
                },
                {
                    "id": "pending-blocked",
                    "adapter": "greenhouse",
                    "name": "Blocked Pending",
                    "jobsFound": 2,
                    "sampleCount": 2,
                    "evidenceScore": 24,
                    "confidence": "medium",
                    "rankScore": 24,
                    "rankReasons": ["structured_batch_family", "jobs_found_bonus"],
                    "promotionLane": "structured_batch",
                    "status": "healthy",
                    "candidateState": "quarantined",
                },
                {
                    "id": "pending-deferred",
                    "adapter": "greenhouse",
                    "name": "Deferred Pending",
                    "jobsFound": 2,
                    "sampleCount": 2,
                    "evidenceScore": 24,
                    "confidence": "medium",
                    "rankScore": 24,
                    "rankReasons": ["structured_batch_family", "jobs_found_bonus"],
                    "promotionLane": "structured_batch",
                    "status": "healthy",
                    "weakSignal": True,
                    "deferred": True,
                },
            ],
        }

        next_state, approved = sr.apply_discovery_auto_approval(
            state,
            report,
            auto_approve_enabled=True,
            approval_state_path=approval_path,
            now_iso_fn=lambda: "2026-03-20T12:06:00Z",
        )

        assert approved == 0
        assert [row["id"] for row in next_state["active"]] == []
        assert [row["id"] for row in next_state["pending"]] == [
            "pending-error",
            "pending-blocked",
            "pending-deferred",
        ]
        assert report["summary"]["approvedCandidateCount"] == 0
        assert report["summary"]["liveCandidateCount"] == 0
        assert report["runtime"]["autoApproval"] == {"enabled": True, "approvedCount": 0}
        assert report["candidates"][0]["promotionReason"] == "structured_batch_family"
        assert report["candidates"][1]["promotionReason"] == "structured_batch_family"
        assert report["candidates"][2]["promotionReason"] == "deferred_candidate"

        repeat_state, repeat_approved = sr.apply_discovery_auto_approval(
            next_state,
            report,
            auto_approve_enabled=True,
            approval_state_path=approval_path,
            now_iso_fn=lambda: "2026-03-20T12:07:00Z",
        )

        assert repeat_approved == 0
        assert repeat_state == next_state
        assert not approval_path.exists()
