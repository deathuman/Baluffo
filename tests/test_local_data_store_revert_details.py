from pathlib import Path

from src.local_data_store import LocalDataPaths, LocalDataStore
from tests.helpers.temp_paths import workspace_tmpdir


def test_phase_revert_activity_includes_explicit_audit_details() -> None:
    with workspace_tmpdir("local-data-store-revert-details") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        user = store.sign_in("PhaseRevert")
        uid = str(user["uid"])
        restored_timestamp = "2026-03-08T08:00:00.000Z"
        job_key = store.save_job_for_user(
            uid,
            {
                "title": "Gameplay Programmer",
                "company": "Studio",
                "jobLink": "https://example.com/phase",
                "pipelinePhase": "applied",
                "outcomeStatus": "active",
                "phaseTimestamps": {
                    "bookmark": restored_timestamp,
                    "applied": "2026-03-08T09:00:00.000Z",
                },
            },
        )

        store.update_application_tracking(
            uid,
            job_key,
            {"pipelinePhase": "bookmark"},
            {
                "override": True,
                "cleanupPhase": "applied",
                "preserveTimestamp": restored_timestamp,
                "eventType": "phase_reverted",
                "revertedFromPhase": "applied",
                "restoredPhase": "bookmark",
                "removedPhaseTimestampFor": "applied",
                "restoredPhaseTimestamp": restored_timestamp,
            },
        )

        activity = next(
            row
            for row in store.list_activity_for_user(uid, 20)
            if row.get("type") == "phase_reverted"
        )
        details = activity["details"]
        assert details["previousPhase"] == "applied"
        assert details["nextPhase"] == "bookmark"
        assert details["previousStatus"] == "applied"
        assert details["nextStatus"] == "bookmark"
        assert details["revertedFromPhase"] == "applied"
        assert details["restoredPhase"] == "bookmark"
        assert details["removedPhaseTimestampFor"] == "applied"
        assert details["restoredPhaseTimestamp"] == restored_timestamp


def test_backward_override_clears_future_phase_timestamps() -> None:
    with workspace_tmpdir("local-data-store-revert-details") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        user = store.sign_in("PhaseRewind")
        uid = str(user["uid"])
        job_key = store.save_job_for_user(
            uid,
            {
                "title": "Gameplay Programmer",
                "company": "Studio",
                "jobLink": "https://example.com/phase-rewind",
                "pipelinePhase": "offer",
                "outcomeStatus": "active",
                "phaseTimestamps": {
                    "bookmark": "2026-03-08T08:00:00.000Z",
                    "applied": "2026-03-08T09:00:00.000Z",
                    "screening": "2026-03-08T10:00:00.000Z",
                    "assignment": "2026-03-08T11:00:00.000Z",
                    "interview_1": "2026-03-08T12:00:00.000Z",
                    "interview_2": "2026-03-08T13:00:00.000Z",
                    "final": "2026-03-08T14:00:00.000Z",
                    "offer": "2026-03-08T15:00:00.000Z",
                },
            },
        )

        store.update_application_tracking(
            uid,
            job_key,
            {"pipelinePhase": "applied"},
            {"override": True},
        )

        row = next(row for row in store.list_saved_jobs(uid) if row["jobKey"] == job_key)
        assert row["phaseTimestamps"] == {
            "bookmark": "2026-03-08T08:00:00.000Z",
            "applied": "2026-03-08T09:00:00.000Z",
        }

        store.update_application_tracking(
            uid,
            job_key,
            {"pipelinePhase": "bookmark"},
            {"override": True},
        )

        row = next(row for row in store.list_saved_jobs(uid) if row["jobKey"] == job_key)
        assert row["phaseTimestamps"] == {"bookmark": "2026-03-08T08:00:00.000Z"}


def test_outcome_revert_activity_includes_explicit_audit_details() -> None:
    with workspace_tmpdir("local-data-store-revert-details") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        user = store.sign_in("OutcomeRevert")
        uid = str(user["uid"])
        restored_timestamp = "2026-03-08T10:00:00.000Z"
        job_key = store.save_job_for_user(
            uid,
            {
                "title": "Gameplay Programmer",
                "company": "Studio",
                "jobLink": "https://example.com/outcome",
                "pipelinePhase": "offer",
                "outcomeStatus": "accepted",
                "phaseTimestamps": {"offer": "2026-03-08T09:00:00.000Z"},
                "outcomeTimestamps": {
                    "rejected": restored_timestamp,
                    "accepted": "2026-03-08T11:00:00.000Z",
                },
            },
        )

        store.update_application_tracking(
            uid,
            job_key,
            {"outcomeStatus": "rejected"},
            {
                "override": True,
                "preserveOutcomeTimestamp": restored_timestamp,
                "eventType": "outcome_reverted",
                "revertedFromOutcome": "accepted",
                "restoredOutcome": "rejected",
                "restoredOutcomeTimestamp": restored_timestamp,
            },
        )

        activity = next(
            row
            for row in store.list_activity_for_user(uid, 20)
            if row.get("type") == "outcome_reverted"
        )
        details = activity["details"]
        assert details["previousOutcome"] == "accepted"
        assert details["nextOutcome"] == "rejected"
        assert details["previousStatus"] == "accepted"
        assert details["nextStatus"] == "rejected"
        assert details["revertedFromOutcome"] == "accepted"
        assert details["restoredOutcome"] == "rejected"
        assert details["restoredOutcomeTimestamp"] == restored_timestamp
