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
        loaded = sr.load_json_array(path, [])
        assert len(loaded) == 1
        assert loaded[0]["company_id"] == "Gameloft"


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
                    "confidence": "high",
                    "rankScore": 84,
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
                    "confidence": "high",
                    "rankScore": 84,
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

        assert approved == 1
        assert [row["id"] for row in next_state["active"]] == ["active-1", "pending-ok"]
        assert [row["id"] for row in next_state["pending"]] == [
            "pending-static",
            "pending-bamboo",
        ]
        assert report["summary"]["approvedCandidateCount"] == 1
        assert report["summary"]["liveCandidateCount"] == 1
        assert report["runtime"]["autoApproval"] == {"enabled": True, "approvedCount": 1}
        assert report["candidates"][0]["candidateState"] == "live"
        assert report["candidates"][0]["approvedBy"] == "discovery_auto_approve"
        assert report["candidates"][0]["liveAt"] == "2026-03-20T12:06:00Z"
        assert report["candidates"][0]["promotionReason"] == "structured_batch_family"
        assert report["candidates"][1]["promotionReason"] == "manual_review_only"
        assert report["candidates"][2]["promotionReason"] == "structured_family_gate"
        assert json.loads(approval_path.read_text(encoding="utf-8")) == {"approvedSinceLastRun": 1}

        repeat_state, repeat_approved = sr.apply_discovery_auto_approval(
            next_state,
            report,
            auto_approve_enabled=True,
            approval_state_path=approval_path,
            now_iso_fn=lambda: "2026-03-20T12:07:00Z",
        )

        assert repeat_approved == 1
        assert repeat_state == next_state
        assert json.loads(approval_path.read_text(encoding="utf-8")) == {"approvedSinceLastRun": 1}
