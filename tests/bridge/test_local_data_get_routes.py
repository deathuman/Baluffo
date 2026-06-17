from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from typing import Any

import pytest

from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


class _LocalDataGetStore(FakeDesktopLocalDataStore):
    def __init__(self) -> None:
        super().__init__()
        self.saved_job_rows: list[dict[str, Any]] = []
        self.saved_job_keys: list[str] = []
        self.attachment_rows: list[dict[str, Any]] = []
        self.activity_rows: list[dict[str, Any]] = []
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def list_saved_jobs(self, uid: str) -> list[dict[str, Any]]:
        self.calls.append(("list_saved_jobs", (uid,)))
        return self.saved_job_rows

    def get_saved_job_keys(self, uid: str) -> list[str]:
        self.calls.append(("get_saved_job_keys", (uid,)))
        return self.saved_job_keys

    def list_attachments_for_job(self, uid: str, job_key: str) -> list[dict[str, Any]]:
        self.calls.append(("list_attachments_for_job", (uid, job_key)))
        return self.attachment_rows

    def get_attachment_blob(
        self, uid: str, job_key: str, attachment_id: str
    ) -> tuple[bytes, str, str]:
        self.calls.append(("get_attachment_blob", (uid, job_key, attachment_id)))
        return b"attachment bytes", "text/plain", "note.txt"

    def export_profile_data(self, uid: str, include_files: bool = False) -> dict[str, Any]:
        self.calls.append(("export_profile_data", (uid, include_files)))
        return {"uid": uid, "includeFiles": include_files, "rows": [1]}

    def list_activity_for_user(self, uid: str, limit: int) -> list[dict[str, Any]]:
        self.calls.append(("list_activity_for_user", (uid, limit)))
        return self.activity_rows


class _CaptureBytesHandler(FakeHandler):
    def send_bytes(
        self, body: bytes, *, content_type: str, status: int = 200, **headers: Any
    ) -> None:
        self.bytes_sent.append(
            {
                "status": status,
                "body": body,
                "content_type": content_type,
                "headers": headers,
            }
        )


def test_saved_jobs_filters_invalid_persisted_rows(tmp_path: Path) -> None:
    store = _LocalDataGetStore()
    valid_row = {
        "profileId": "uid-1",
        "jobKey": "job-1",
        "title": "Engineer",
        "phaseTimestamps": {},
    }
    store.saved_job_rows = [
        valid_row,
        {
            "profileId": "uid-1",
            "jobKey": "job-bad",
            "title": "Bad",
            "phaseTimestamps": ["not-a-dict"],
        },
    ]
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/desktop-local-data/saved-jobs",
        query={"uid": ["uid-1"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"] == {"ok": True, "rows": [valid_row]}
    assert store.calls == [("list_saved_jobs", ("uid-1",))]


def test_local_data_get_routes_pass_query_values_to_store(tmp_path: Path) -> None:
    store = _LocalDataGetStore()
    store.saved_job_keys = ["job-1"]
    store.attachment_rows = [{"id": "att-1"}]
    store.activity_rows = [{"id": "activity-1"}]
    api = make_stub_bridge_api(tmp_path, store)

    keys_handler = FakeHandler()
    assert (
        handle_get(
            keys_handler,
            api=api,
            path="/desktop-local-data/saved-job-keys",
            query={"uid": ["uid-1"]},
        )
        is True
    )
    attachments_handler = FakeHandler()
    assert (
        handle_get(
            attachments_handler,
            api=api,
            path="/desktop-local-data/attachments",
            query={"uid": ["uid-1"], "jobKey": ["job-1"]},
        )
        is True
    )
    activity_handler = FakeHandler()
    assert (
        handle_get(
            activity_handler,
            api=api,
            path="/desktop-local-data/activity",
            query={"uid": ["uid-1"], "limit": ["25"]},
        )
        is True
    )

    assert keys_handler.sent[-1]["payload"] == {"ok": True, "keys": ["job-1"]}
    assert attachments_handler.sent[-1]["payload"] == {"ok": True, "rows": [{"id": "att-1"}]}
    assert activity_handler.sent[-1]["payload"] == {"ok": True, "rows": [{"id": "activity-1"}]}
    assert store.calls == [
        ("get_saved_job_keys", ("uid-1",)),
        ("list_attachments_for_job", ("uid-1", "job-1")),
        ("list_activity_for_user", ("uid-1", 25)),
    ]


@pytest.mark.parametrize(
    ("download", "expected_disposition"),
    [
        ("0", "inline"),
        ("1", "attachment"),
        ("true", "attachment"),
    ],
)
def test_attachment_content_route_sends_bytes_with_disposition(
    tmp_path: Path, download: str, expected_disposition: str
) -> None:
    store = _LocalDataGetStore()
    api = make_stub_bridge_api(tmp_path, store)
    handler = _CaptureBytesHandler()

    result = handle_get(
        handler,
        api=api,
        path="/desktop-local-data/attachments/content",
        query={
            "uid": ["uid-1"],
            "jobKey": ["job-1"],
            "attachmentId": ["att-1"],
            "download": [download],
        },
    )

    assert result is True
    assert handler.bytes_sent[-1]["status"] == 200
    assert handler.bytes_sent[-1]["body"] == b"attachment bytes"
    assert handler.bytes_sent[-1]["content_type"] == "text/plain"
    assert handler.bytes_sent[-1]["headers"] == {
        "filename": "note.txt",
        "disposition": expected_disposition,
    }
    assert store.calls == [("get_attachment_blob", ("uid-1", "job-1", "att-1"))]


def test_backup_export_file_route_sends_json_or_zip(tmp_path: Path) -> None:
    store = _LocalDataGetStore()
    api = make_stub_bridge_api(tmp_path, store)

    json_handler = _CaptureBytesHandler()
    assert (
        handle_get(
            json_handler,
            api=api,
            path="/desktop-local-data/backup/export-file",
            query={"uid": ["user 1"], "includeFiles": ["0"]},
        )
        is True
    )
    json_response = json_handler.bytes_sent[-1]
    assert json_response["content_type"] == "application/json; charset=utf-8"
    assert json_response["headers"]["filename"].startswith("baluffo-backup-user_1-")
    assert json_response["headers"]["filename"].endswith(".json")
    assert json_response["headers"]["disposition"] == "attachment"
    assert json.loads(json_response["body"].decode("utf-8")) == {
        "uid": "user 1",
        "includeFiles": False,
        "rows": [1],
    }

    zip_handler = _CaptureBytesHandler()
    assert (
        handle_get(
            zip_handler,
            api=api,
            path="/desktop-local-data/backup/export-file",
            query={"uid": ["user 1"], "includeFiles": ["1"]},
        )
        is True
    )
    zip_response = zip_handler.bytes_sent[-1]
    assert zip_response["content_type"] == "application/zip"
    assert zip_response["headers"]["filename"].startswith("baluffo-backup-user_1-")
    assert zip_response["headers"]["filename"].endswith(".zip")
    with zipfile.ZipFile(io.BytesIO(zip_response["body"])) as zf:
        assert zf.namelist() == ["backup.json"]
        assert json.loads(zf.read("backup.json").decode("utf-8")) == {
            "uid": "user 1",
            "includeFiles": True,
            "rows": [1],
        }
    assert store.calls == [
        ("export_profile_data", ("user 1", False)),
        ("export_profile_data", ("user 1", True)),
    ]
