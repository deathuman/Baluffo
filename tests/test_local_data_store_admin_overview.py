from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from src.local_data_store import LocalDataPaths, LocalDataStore
from tests.helpers.temp_paths import workspace_tmpdir


@contextmanager
def _store_with_attachment(size: int = 999) -> Iterator[tuple[LocalDataStore, str]]:
    with workspace_tmpdir("local-data-store-admin-overview") as tmp:
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
        store.add_attachment_for_job(
            uid,
            job_key,
            {"name": "resume.txt", "type": "text/plain", "size": size},
            "data:text/plain;base64,aGVsbG8=",
        )
        yield store, uid


def test_admin_overview_summary_uses_attachment_metadata_without_stat(monkeypatch) -> None:
    with _store_with_attachment() as (store, uid):
        attachment_root = store.paths.attachment_dir(uid)
        original_stat = Path.stat

        def fail_attachment_stat(self, *args, **kwargs):
            if str(self).startswith(str(attachment_root)):
                raise AssertionError(f"summary overview should not stat {self}")
            return original_stat(self, *args, **kwargs)

        monkeypatch.setattr(Path, "stat", fail_attachment_stat)

        overview = store.get_admin_overview(detail="summary")

        assert overview["totals"]["attachmentsBytes"] == 999
        assert overview["detailLevel"] == "summary"
        assert overview["attachmentSizeBasis"] == "metadata"


def test_admin_overview_includes_profile_shell_without_user_directory() -> None:
    with workspace_tmpdir("local-data-store-admin-overview-profile-shell") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        user = store.sign_in("Browser Proof User")

        overview = store.get_admin_overview(detail="summary")

        assert overview["totals"]["usersCount"] == 1
        assert overview["users"] == [
            {
                "uid": user["uid"],
                "name": "Browser Proof User",
                "email": "",
                "savedJobsCount": 0,
                "notesBytes": 0,
                "attachmentsCount": 0,
                "attachmentsBytes": 0,
                "totalBytes": 0,
                "profileShell": True,
            }
        ]


def test_admin_overview_full_prefers_filesystem_size_over_metadata() -> None:
    with _store_with_attachment() as (store, _uid):
        overview = store.get_admin_overview(detail="full")

        assert overview["totals"]["attachmentsBytes"] == 5
        assert overview["detailLevel"] == "full"
        assert overview["attachmentSizeBasis"] == "filesystem"
