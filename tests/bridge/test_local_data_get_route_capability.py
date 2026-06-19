from __future__ import annotations

import json
from typing import Any

from src.bridge.routes.get_local_data import handle_local_data_get_routes
from tests.helpers.bridge_api import FakeHandler


class MinimalLocalDataStore:
    def export_profile_data(self, uid: str, *, include_files: bool = False) -> dict[str, Any]:
        return {"uid": uid, "includeFiles": include_files, "rows": []}

    def get_attachment_blob(
        self, uid: str, job_key: str, attachment_id: str
    ) -> tuple[bytes, str, str]:
        return (
            f"{uid}:{job_key}:{attachment_id}".encode(),
            "text/plain; charset=utf-8",
            "note.txt",
        )

    def get_current_user(self) -> dict[str, Any]:
        return {"uid": "user-1", "displayName": "User One"}

    def get_saved_job_keys(self, uid: str) -> list[str]:
        return [f"{uid}:job-1"]

    def list_activity_for_user(self, uid: str, limit: int) -> list[dict[str, Any]]:
        return [{"uid": uid, "limit": limit, "event": "opened"}]

    def list_attachments_for_job(self, uid: str, job_key: str) -> list[dict[str, Any]]:
        return [{"uid": uid, "jobKey": job_key, "attachmentId": "attachment-1"}]

    def list_profiles(self) -> list[dict[str, Any]]:
        return [{"uid": "user-1", "displayName": "User One"}]

    def list_saved_jobs(self, uid: str) -> list[dict[str, Any]]:
        del uid
        return []


class MinimalLocalDataGetRouteApi:
    DESKTOP_SESSION_ACTIVITY_AT = "2026-06-19T10:00:00+00:00"

    def __init__(self) -> None:
        self.store = MinimalLocalDataStore()

    def desktop_local_data_store(self) -> MinimalLocalDataStore:
        return self.store

    def get_desktop_session_payload(self) -> dict[str, Any]:
        return {"sessionId": "session-1", "ownerToken": "owner-1"}

    def read_startup_metrics(self, limit: int) -> list[dict[str, Any]]:
        return [{"event": "desktop_site_ready", "limit": limit}]


def test_local_data_get_routes_accept_minimal_capability_object() -> None:
    api = MinimalLocalDataGetRouteApi()

    session_handler = FakeHandler()
    assert (
        handle_local_data_get_routes(
            session_handler,
            api=api,
            path="/desktop-local-data/session",
            query={},
        )
        is True
    )
    assert session_handler.sent[-1]["payload"]["desktopSession"]["sessionId"] == "session-1"

    profiles_handler = FakeHandler()
    assert (
        handle_local_data_get_routes(
            profiles_handler,
            api=api,
            path="/desktop-local-data/profiles",
            query={},
        )
        is True
    )
    assert profiles_handler.sent[-1]["payload"]["profiles"][0]["uid"] == "user-1"

    saved_jobs_handler = FakeHandler()
    assert (
        handle_local_data_get_routes(
            saved_jobs_handler,
            api=api,
            path="/desktop-local-data/saved-jobs",
            query={"uid": ["user-1"]},
        )
        is True
    )
    assert saved_jobs_handler.sent[-1]["payload"]["rows"] == []

    keys_handler = FakeHandler()
    assert (
        handle_local_data_get_routes(
            keys_handler,
            api=api,
            path="/desktop-local-data/saved-job-keys",
            query={"uid": ["user-1"]},
        )
        is True
    )
    assert keys_handler.sent[-1]["payload"]["keys"] == ["user-1:job-1"]

    attachments_handler = FakeHandler()
    assert (
        handle_local_data_get_routes(
            attachments_handler,
            api=api,
            path="/desktop-local-data/attachments",
            query={"uid": ["user-1"], "jobKey": ["job-1"]},
        )
        is True
    )
    assert attachments_handler.sent[-1]["payload"]["rows"][0]["attachmentId"] == "attachment-1"

    content_handler = FakeHandler()
    assert (
        handle_local_data_get_routes(
            content_handler,
            api=api,
            path="/desktop-local-data/attachments/content",
            query={"uid": ["user-1"], "jobKey": ["job-1"], "attachmentId": ["attachment-1"]},
        )
        is True
    )
    assert content_handler.bytes_sent[-1]["body"] == b"user-1:job-1:attachment-1"

    export_handler = FakeHandler()
    assert (
        handle_local_data_get_routes(
            export_handler,
            api=api,
            path="/desktop-local-data/backup/export-file",
            query={"uid": ["user-1"]},
        )
        is True
    )
    export_payload = json.loads(export_handler.bytes_sent[-1]["body"].decode("utf-8"))
    assert export_payload["uid"] == "user-1"

    activity_handler = FakeHandler()
    assert (
        handle_local_data_get_routes(
            activity_handler,
            api=api,
            path="/desktop-local-data/activity",
            query={"uid": ["user-1"], "limit": ["5"]},
        )
        is True
    )
    assert activity_handler.sent[-1]["payload"]["rows"][0]["limit"] == 5

    metrics_handler = FakeHandler()
    assert (
        handle_local_data_get_routes(
            metrics_handler,
            api=api,
            path="/desktop-local-data/startup-metrics",
            query={"limit": ["3"]},
        )
        is True
    )
    assert metrics_handler.sent[-1]["payload"]["rows"][0]["limit"] == 3
