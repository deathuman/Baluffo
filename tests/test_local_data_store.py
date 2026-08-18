from pathlib import Path

from src.local_data_store import LocalDataPaths, LocalDataStore
from tests.helpers.temp_paths import workspace_tmpdir


def test_sign_in_and_saved_jobs_persist_to_disk() -> None:
    with workspace_tmpdir("local-data-store") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        user = store.sign_in("Andrea")
        uid = str(user["uid"])

        job_key = store.save_job_for_user(
            uid,
            {
                "title": "Gameplay Programmer",
                "company": "Baluffo Studio",
                "country": "NL",
                "jobLink": "https://example.com/jobs/gameplay-programmer",
            },
        )
        before_notes = store.list_saved_jobs(uid)
        assert len(before_notes) == 1
        updated_at_before = before_notes[0].get("updatedAt")
        store.update_job_notes(uid, job_key, "Interesting role.")

        reloaded = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        current_user = reloaded.get_current_user()
        assert current_user is not None
        assert current_user["uid"] == uid
        rows = reloaded.list_saved_jobs(uid)
        assert len(rows) == 1
        assert rows[0]["jobKey"] == job_key
        assert rows[0]["notes"] == "Interesting role."
        assert rows[0].get("updatedAt") == updated_at_before


def test_attachment_round_trip_and_admin_overview() -> None:
    with workspace_tmpdir("local-data-store") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        user = store.sign_in("Andrea")
        uid = str(user["uid"])
        job_key = store.save_job_for_user(
            uid,
            {
                "title": "Technical Artist",
                "company": "Baluffo Studio",
                "country": "NL",
                "jobLink": "https://example.com/jobs/technical-artist",
            },
        )

        attachment_id = store.add_attachment_for_job(
            uid,
            job_key,
            {"name": "resume.txt", "type": "text/plain", "size": 5},
            "data:text/plain;base64,aGVsbG8=",
        )
        body, content_type, filename = store.get_attachment_blob(uid, job_key, attachment_id)
        overview = store.get_admin_overview()

        assert body == b"hello"
        assert content_type == "text/plain"
        assert filename == "resume.txt"
        assert overview["totals"]["usersCount"] == 1
        assert overview["totals"]["attachmentsCount"] == 1
        assert overview["totals"]["attachmentsBytes"] == 5
        assert overview["detailLevel"] == "full"
        assert overview["attachmentSizeBasis"] == "filesystem"


def test_list_profiles_returns_sorted_profiles_with_current_flag() -> None:
    with workspace_tmpdir("local-data-store") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        store.sign_in("Zed")
        current = store.sign_in("Andrea")
        rows = store.list_profiles()

        assert [row["displayName"] for row in rows] == ["Andrea", "Zed"]
        assert rows[0]["uid"] == str(current["uid"])
        assert rows[0]["isCurrent"] is True
        assert rows[1]["isCurrent"] is False


def test_backup_export_import_roundtrip_preserves_business_fields() -> None:
    with workspace_tmpdir("local-data-store") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        user = store.sign_in("BackupRoundtrip")
        uid = str(user["uid"])

        job_1 = store.save_job_for_user(
            uid,
            {
                "title": "Gameplay Programmer",
                "company": "Studio One",
                "country": "NL",
                "workType": "Hybrid",
                "contractType": "Full-time",
                "jobLink": "https://example.com/job-1",
                "notes": "Initial note",
                "reminderAt": "2026-03-21T09:00:00.000Z",
                "applicationStatus": "applied",
                "phaseTimestamps": {
                    "bookmark": "2026-03-08T08:00:00.000Z",
                    "applied": "2026-03-08T09:00:00.000Z",
                },
            },
        )
        store.update_application_status(
            uid,
            job_1,
            "interview_1",
            {
                "override": True,
                "preserveTimestamp": "2026-03-10T11:30:00.000Z",
            },
        )
        store.update_job_notes(uid, job_1, "Interview planned")

        job_2 = store.save_job_for_user(
            uid,
            {
                "title": "Technical Artist",
                "company": "Studio Two",
                "country": "NL",
                "workType": "Remote",
                "contractType": "Contract",
                "jobLink": "https://example.com/job-2",
                "notes": "Portfolio update",
                "applicationStatus": "bookmark",
                "phaseTimestamps": {"bookmark": "2026-03-07T10:00:00.000Z"},
                "isCustom": True,
                "customSourceLabel": "Manual",
            },
        )

        att_1 = store.add_attachment_for_job(
            uid,
            job_1,
            {"name": "resume-a.txt", "type": "text/plain", "size": 8},
            "data:text/plain;base64,UmVzdW1lLTE=",
        )
        att_2 = store.add_attachment_for_job(
            uid,
            job_2,
            {"name": "portfolio-b.txt", "type": "text/plain", "size": 8},
            "data:text/plain;base64,UG9ydGZvLTI=",
        )
        blob_before_1, _, _ = store.get_attachment_blob(uid, job_1, att_1)
        blob_before_2, _, _ = store.get_attachment_blob(uid, job_2, att_2)
        jobs_before = {row["jobKey"]: row for row in store.list_saved_jobs(uid)}
        activity_before_len = len(store.list_activity_for_user(uid, 2000))

        payload_no_files = store.export_profile_data(uid, include_files=False)
        assert payload_no_files.get("schemaVersion") == 4
        assert payload_no_files.get("includesFiles") is False
        assert payload_no_files.get("counts") == {
            "savedJobs": 2,
            "customJobs": 1,
            "historyEvents": activity_before_len,
            "attachments": 2,
            "sourcePolicyReviewPairs": 0,
            "sourcePolicyRecommendationPairs": 0,
        }
        assert (
            any(
                bool((row or {}).get("blobDataUrl"))
                for row in payload_no_files.get("attachments") or []
            )
            is False
        )

        payload_with_files = store.export_profile_data(uid, include_files=True)
        assert payload_with_files.get("schemaVersion") == 4
        assert payload_with_files.get("includesFiles") is True
        assert payload_with_files.get("counts") == {
            "savedJobs": 2,
            "customJobs": 1,
            "historyEvents": activity_before_len,
            "attachments": 2,
            "sourcePolicyReviewPairs": 0,
            "sourcePolicyRecommendationPairs": 0,
        }
        assert (
            all(
                bool((row or {}).get("blobDataUrl"))
                for row in payload_with_files.get("attachments") or []
            )
            is True
        )

        store.wipe_account_admin(uid)
        recreated = store.sign_in("BackupRoundtrip")
        uid_after = str(recreated["uid"])
        result = store.import_profile_data(uid_after, payload_with_files)
        assert int(result.get("created", 0)) == 2
        assert int(result.get("updated", 0)) == 0
        assert int(result.get("skippedInvalid", 0)) == 0
        assert int(result.get("historyAdded", 0)) == activity_before_len
        assert sorted(str(w) for w in result.get("warnings") or []) == []

        jobs_after = {row["jobKey"]: row for row in store.list_saved_jobs(uid_after)}
        assert set(jobs_before.keys()) == set(jobs_after.keys())
        assert jobs_after[job_1]["notes"] == "Interview planned"
        assert jobs_after[job_1]["applicationStatus"] == "interview_1"
        assert jobs_after[job_1]["pipelinePhase"] == "interview_1"
        assert jobs_after[job_1]["outcomeStatus"] == "active"
        assert jobs_after[job_1]["phaseTimestamps"].get("interview_1") == "2026-03-10T11:30:00.000Z"
        assert jobs_after[job_2]["isCustom"] is True
        assert jobs_after[job_2]["customSourceLabel"] == "Manual"
        assert int(jobs_after[job_1]["attachmentsCount"]) == 1
        assert int(jobs_after[job_2]["attachmentsCount"]) == 1

        attachments_after_1 = store.list_attachments_for_job(uid_after, job_1)
        attachments_after_2 = store.list_attachments_for_job(uid_after, job_2)
        assert len(attachments_after_1) == 1
        assert len(attachments_after_2) == 1
        blob_after_1, _, _ = store.get_attachment_blob(
            uid_after, job_1, str(attachments_after_1[0]["id"])
        )
        blob_after_2, _, _ = store.get_attachment_blob(
            uid_after, job_2, str(attachments_after_2[0]["id"])
        )
        assert blob_after_1 == blob_before_1
        assert blob_after_2 == blob_before_2
        assert len(store.list_activity_for_user(uid_after, 2000)) >= activity_before_len


def test_import_skips_malformed_rows_and_keeps_valid_rows() -> None:
    with workspace_tmpdir("local-data-store") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        user = store.sign_in("MalformedImport")
        uid = str(user["uid"])
        job_key = store.save_job_for_user(
            uid,
            {
                "title": "QA Engineer",
                "company": "Studio Three",
                "jobLink": "https://example.com/qa",
            },
        )

        payload = {
            "schemaVersion": 2,
            "savedJobs": [
                {"jobKey": job_key, "title": "QA Engineer Updated", "company": "Studio Three"},
                {"jobKey": "job_missing_title", "title": "", "company": "Broken Co"},
                "not-an-object",
            ],
            "attachments": [
                {"id": "att_orphan", "name": "orphan.txt", "type": "text/plain", "size": 2},
                {
                    "id": "att_ok",
                    "jobKey": job_key,
                    "name": "ok.txt",
                    "type": "text/plain",
                    "size": 2,
                    "blobDataUrl": "data:text/plain;base64,T0s=",
                },
            ],
            "activityLog": [
                {
                    "type": "note",
                    "jobKey": job_key,
                    "title": "QA Engineer Updated",
                    "company": "Studio Three",
                    "createdAt": "2026-03-09T10:00:00.000Z",
                    "details": {"ok": True},
                },
                "bad-activity-row",
            ],
        }

        result = store.import_profile_data(uid, payload)
        jobs = store.list_saved_jobs(uid)
        assert len(jobs) == 1
        assert jobs[0]["title"] == "QA Engineer Updated"
        assert int(result.get("skippedInvalid", 0)) == 2
        assert int(result.get("created", 0)) == 0
        assert int(result.get("updated", 0)) == 1
        assert int(result.get("historyAdded", 0)) == 1
        assert any("jobKey" in str(w) for w in result.get("warnings") or [])


def test_backup_import_accepts_v1_v2_v3_payload_versions() -> None:
    with workspace_tmpdir("local-data-store") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        user = store.sign_in("BackupVersions")
        uid = str(user["uid"])

        for version in (1, 2, 3):
            result = store.import_profile_data(
                uid,
                {
                    "schemaVersion": version,
                    "savedJobs": [
                        {
                            "jobKey": f"job_version_{version}",
                            "title": f"Role {version}",
                            "company": "Studio Versions",
                            "applicationStatus": "rejected" if version == 1 else "bookmark",
                            "savedAt": "2026-03-08T09:00:00.000Z",
                        }
                    ],
                    "attachments": [],
                    "activityLog": [],
                },
            )
            assert int(result.get("created", 0)) == 1

        rows = sorted(store.list_saved_jobs(uid), key=lambda row: str(row.get("title") or ""))
        assert [row["title"] for row in rows] == ["Role 1", "Role 2", "Role 3"]
        assert rows[0]["outcomeStatus"] == "rejected"
        assert rows[1]["pipelinePhase"] == "bookmark"
        assert rows[2]["pipelinePhase"] == "bookmark"


def test_update_application_status_is_noop_when_reclicking_same_phase() -> None:
    with workspace_tmpdir("local-data-store") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        user = store.sign_in("SamePhase")
        uid = str(user["uid"])
        job_key = store.save_job_for_user(
            uid,
            {
                "title": "Gameplay Programmer",
                "company": "Studio Same",
                "jobLink": "https://example.com/same-phase",
                "applicationStatus": "bookmark",
            },
        )

        before_row = store.list_saved_jobs(uid)[0]
        before_updated_at = str(before_row.get("updatedAt") or "")
        before_phase_timestamps = dict(before_row.get("phaseTimestamps") or {})
        before_activity_len = len(store.list_activity_for_user(uid, 2000))

        store.update_application_status(uid, job_key, "bookmark")

        after_row = store.list_saved_jobs(uid)[0]
        after_activity_len = len(store.list_activity_for_user(uid, 2000))
        assert str(after_row.get("updatedAt") or "") == before_updated_at
        assert dict(after_row.get("phaseTimestamps") or {}) == before_phase_timestamps
        assert after_activity_len == before_activity_len


def test_update_application_tracking_splits_phase_outcome_and_touches_activity() -> None:
    with workspace_tmpdir("local-data-store") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        user = store.sign_in("TrackingSplit")
        uid = str(user["uid"])
        job_key = store.save_job_for_user(
            uid,
            {
                "title": "Gameplay Programmer",
                "company": "Studio Split",
                "jobLink": "https://example.com/split",
                "notes": "old",
            },
        )

        saved = store.list_saved_jobs(uid)[0]
        initial_activity_at = str(saved.get("lastActivityAt") or "")
        store.update_application_tracking(uid, job_key, {"pipelinePhase": "applied"})
        applied = store.list_saved_jobs(uid)[0]
        assert applied["pipelinePhase"] == "applied"
        assert applied["outcomeStatus"] == "active"
        assert applied["applicationStatus"] == "applied"
        assert str(applied.get("lastActivityAt") or "") >= initial_activity_at

        store.update_application_tracking(uid, job_key, {"outcomeStatus": "rejected"})
        rejected = store.list_saved_jobs(uid)[0]
        assert rejected["pipelinePhase"] == "applied"
        assert rejected["outcomeStatus"] == "rejected"
        assert rejected["applicationStatus"] == "rejected"
        assert rejected["outcomeTimestamps"].get("rejected")
        after_outcome_activity_at = str(rejected.get("lastActivityAt") or "")

        store.update_job_notes(uid, job_key, "new")
        after_notes = store.list_saved_jobs(uid)[0]
        assert str(after_notes.get("lastActivityAt") or "") >= after_outcome_activity_at
        after_notes_activity_at = str(after_notes.get("lastActivityAt") or "")

        store.add_attachment_for_job(
            uid,
            job_key,
            {"name": "resume.txt", "type": "text/plain", "size": 1},
            "data:text/plain;base64,WA==",
        )
        after_attachment = store.list_saved_jobs(uid)[0]
        assert str(after_attachment.get("lastActivityAt") or "") >= after_notes_activity_at

        activity_rows = store.list_activity_for_user(uid, 20)
        activity_types = [row.get("type") for row in activity_rows]
        assert "phase_changed" in activity_types
        assert "outcome_changed" in activity_types
        assert "note_updated" in activity_types
        assert "attachment_added" in activity_types
        note_activity = next(row for row in activity_rows if row.get("type") == "note_updated")
        assert note_activity["details"]["previousLength"] == 3
        assert note_activity["details"]["nextLength"] == 3
