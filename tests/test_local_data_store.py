import unittest
from pathlib import Path

from scripts.local_data_store import LocalDataPaths, LocalDataStore
from tests.temp_paths import workspace_tmpdir


class LocalDataStoreTests(unittest.TestCase):
    def test_sign_in_and_saved_jobs_persist_to_disk(self) -> None:
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
            self.assertEqual(len(before_notes), 1)
            updated_at_before = before_notes[0].get("updatedAt")
            store.update_job_notes(uid, job_key, "Interesting role.")

            reloaded = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
            self.assertEqual(reloaded.get_current_user()["uid"], uid)
            rows = reloaded.list_saved_jobs(uid)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["jobKey"], job_key)
            self.assertEqual(rows[0]["notes"], "Interesting role.")
            self.assertEqual(rows[0].get("updatedAt"), updated_at_before)

    def test_attachment_round_trip_and_admin_overview(self) -> None:
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
            overview = store.get_admin_overview("1234")

            self.assertEqual(body, b"hello")
            self.assertEqual(content_type, "text/plain")
            self.assertEqual(filename, "resume.txt")
            self.assertEqual(overview["totals"]["usersCount"], 1)
            self.assertEqual(overview["totals"]["attachmentsCount"], 1)

    def test_backup_export_import_roundtrip_preserves_business_fields(self) -> None:
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
            store.update_application_status(uid, job_1, "interview_1", {"preserveTimestamp": "2026-03-10T11:30:00.000Z"})
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
            self.assertEqual(payload_no_files.get("schemaVersion"), 2)
            self.assertEqual(payload_no_files.get("includesFiles"), False)
            self.assertEqual(payload_no_files.get("counts"), {"savedJobs": 2, "customJobs": 1, "historyEvents": activity_before_len, "attachments": 2})
            self.assertEqual(any(bool((row or {}).get("blobDataUrl")) for row in payload_no_files.get("attachments") or []), False)

            payload_with_files = store.export_profile_data(uid, include_files=True)
            self.assertEqual(payload_with_files.get("schemaVersion"), 2)
            self.assertEqual(payload_with_files.get("includesFiles"), True)
            self.assertEqual(payload_with_files.get("counts"), {"savedJobs": 2, "customJobs": 1, "historyEvents": activity_before_len, "attachments": 2})
            self.assertEqual(all(bool((row or {}).get("blobDataUrl")) for row in payload_with_files.get("attachments") or []), True)

            store.wipe_account_admin("1234", uid)
            recreated = store.sign_in("BackupRoundtrip")
            uid_after = str(recreated["uid"])
            result = store.import_profile_data(uid_after, payload_with_files)
            self.assertEqual(int(result.get("created", 0)), 2)
            self.assertEqual(int(result.get("updated", 0)), 0)
            self.assertEqual(int(result.get("skippedInvalid", 0)), 0)
            self.assertEqual(int(result.get("historyAdded", 0)), activity_before_len)
            self.assertEqual(sorted(str(w) for w in result.get("warnings") or []), [])

            jobs_after = {row["jobKey"]: row for row in store.list_saved_jobs(uid_after)}
            self.assertEqual(set(jobs_before.keys()), set(jobs_after.keys()))
            self.assertEqual(jobs_after[job_1]["notes"], "Interview planned")
            self.assertEqual(jobs_after[job_1]["applicationStatus"], "interview_1")
            self.assertEqual(jobs_after[job_1]["phaseTimestamps"].get("interview_1"), "2026-03-10T11:30:00.000Z")
            self.assertEqual(jobs_after[job_2]["isCustom"], True)
            self.assertEqual(jobs_after[job_2]["customSourceLabel"], "Manual")
            self.assertEqual(int(jobs_after[job_1]["attachmentsCount"]), 1)
            self.assertEqual(int(jobs_after[job_2]["attachmentsCount"]), 1)

            attachments_after_1 = store.list_attachments_for_job(uid_after, job_1)
            attachments_after_2 = store.list_attachments_for_job(uid_after, job_2)
            self.assertEqual(len(attachments_after_1), 1)
            self.assertEqual(len(attachments_after_2), 1)
            blob_after_1, _, _ = store.get_attachment_blob(uid_after, job_1, str(attachments_after_1[0]["id"]))
            blob_after_2, _, _ = store.get_attachment_blob(uid_after, job_2, str(attachments_after_2[0]["id"]))
            self.assertEqual(blob_after_1, blob_before_1)
            self.assertEqual(blob_after_2, blob_before_2)
            self.assertGreaterEqual(len(store.list_activity_for_user(uid_after, 2000)), activity_before_len)

    def test_import_skips_malformed_rows_and_keeps_valid_rows(self) -> None:
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
                    {"id": "att_ok", "jobKey": job_key, "name": "ok.txt", "type": "text/plain", "size": 2, "blobDataUrl": "data:text/plain;base64,T0s="},
                ],
                "activityLog": [
                    {"type": "note", "jobKey": job_key, "title": "QA Engineer Updated", "company": "Studio Three", "createdAt": "2026-03-09T10:00:00.000Z", "details": {"ok": True}},
                    "bad-activity-row",
                ],
            }

            result = store.import_profile_data(uid, payload)
            jobs = store.list_saved_jobs(uid)
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0]["title"], "QA Engineer Updated")
            self.assertEqual(int(result.get("skippedInvalid", 0)), 2)
            self.assertEqual(int(result.get("created", 0)), 0)
            self.assertEqual(int(result.get("updated", 0)), 1)
            self.assertEqual(int(result.get("historyAdded", 0)), 1)
            self.assertTrue(any("jobKey" in str(w) for w in result.get("warnings") or []))

    def test_update_application_status_is_noop_when_reclicking_same_phase(self) -> None:
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
            self.assertEqual(str(after_row.get("updatedAt") or ""), before_updated_at)
            self.assertEqual(dict(after_row.get("phaseTimestamps") or {}), before_phase_timestamps)
            self.assertEqual(after_activity_len, before_activity_len)


if __name__ == "__main__":
    unittest.main()
