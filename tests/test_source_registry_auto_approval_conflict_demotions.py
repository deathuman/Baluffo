from pathlib import Path

from src import source_registry as sr
from tests.helpers.temp_paths import workspace_tmpdir


def test_discovery_auto_approval_does_not_reactivate_conflict_demoted_pending_rows() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        approval_path = Path(tmp) / "source-approval-state.json"
        conflict_reasons = [
            "registry_conflict_safe_auto_demote",
            "registry_conflict_adjudication_auto_demote",
        ]
        state = {
            "active": [],
            "pending": [
                {
                    "id": f"static:listing_url:https://example.com/{index}/careers",
                    "adapter": "static",
                    "name": f"Conflict Demoted {index}",
                    "jobsFound": 5,
                    "sampleCount": 5,
                    "weakSignal": False,
                    "status": "healthy",
                    "candidateState": "validated",
                    "registryState": "pending",
                    "pendingReason": reason,
                    "stateChangedBy": reason,
                    "lastDemotedAt": "2026-05-09T20:14:47+02:00",
                }
                for index, reason in enumerate(conflict_reasons)
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
                    "id": f"static:listing_url:https://example.com/{index}/careers",
                    "adapter": "static",
                    "name": f"Conflict Demoted {index}",
                    "jobsFound": 5,
                    "sampleCount": 5,
                    "weakSignal": False,
                    "status": "healthy",
                    "candidateState": "validated",
                }
                for index in range(2)
            ],
        }

        next_state, approved = sr.apply_discovery_auto_approval(
            state,
            report,
            auto_approve_enabled=True,
            approval_state_path=approval_path,
            now_iso_fn=lambda: "2026-05-09T22:05:47+02:00",
        )

        assert approved == 0
        assert next_state["active"] == []
        assert [row["pendingReason"] for row in next_state["pending"]] == conflict_reasons
        assert [row["stateChangedBy"] for row in next_state["pending"]] == conflict_reasons
        assert report["summary"]["approvedCandidateCount"] == 0
        assert report["runtime"]["autoApproval"] == {"enabled": True, "approvedCount": 0}
        assert [row["candidateState"] for row in report["candidates"]] == [
            "validated",
            "validated",
        ]
        assert all("approvedBy" not in row for row in report["candidates"])
        assert all("liveAt" not in row for row in report["candidates"])
        assert not approval_path.exists()


def test_discovery_auto_approval_does_not_activate_static_url_alias_duplicate() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        approval_path = Path(tmp) / "source-approval-state.json"
        active_id = "static:listing_url:https://www.bandainamcoent.com/careers"
        pending_id = "static:listing_url:https://www.bandainamcoent.com/careers#join"
        state = {
            "active": [
                {
                    "id": active_id,
                    "adapter": "static",
                    "name": "Bandai Namco Entertainment America Inc. (Sheet)",
                    "studio": "Bandai Namco Entertainment America Inc.",
                    "jobsFound": 7,
                    "registryState": "active",
                    "candidateState": "live",
                }
            ],
            "pending": [
                {
                    "id": pending_id,
                    "adapter": "static",
                    "name": "Bandai Namco Entertainment America Inc. (Sheet)",
                    "studio": "Bandai Namco Entertainment America Inc.",
                    "jobsFound": 7,
                    "status": "healthy",
                    "registryState": "pending",
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
                    "id": pending_id,
                    "adapter": "static",
                    "name": "Bandai Namco Entertainment America Inc. (Sheet)",
                    "studio": "Bandai Namco Entertainment America Inc.",
                    "jobsFound": 7,
                    "status": "healthy",
                    "candidateState": "validated",
                }
            ],
        }

        next_state, approved = sr.apply_discovery_auto_approval(
            state,
            report,
            auto_approve_enabled=True,
            approval_state_path=approval_path,
            now_iso_fn=lambda: "2026-05-10T14:00:00+02:00",
        )

        assert approved == 0
        assert [row["id"] for row in next_state["active"]] == [active_id]
        assert [row["id"] for row in next_state["pending"]] == [pending_id]
        assert report["summary"]["approvedCandidateCount"] == 0
        assert report["runtime"]["autoApproval"] == {"enabled": True, "approvedCount": 0}
        assert report["candidates"][0]["candidateState"] == "validated"
        assert "approvedBy" not in report["candidates"][0]
        assert "liveAt" not in report["candidates"][0]
