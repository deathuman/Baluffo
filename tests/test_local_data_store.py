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
            store.update_job_notes(uid, job_key, "Interesting role.")

            reloaded = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
            self.assertEqual(reloaded.get_current_user()["uid"], uid)
            rows = reloaded.list_saved_jobs(uid)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["jobKey"], job_key)
            self.assertEqual(rows[0]["notes"], "Interesting role.")

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


if __name__ == "__main__":
    unittest.main()
