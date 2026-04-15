"""Tests for bridge POST routes - improving coverage for post_routes.py."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from src.bridge import registry_tombstones
from src.bridge.routes.post_routes import handle_post
from tests.bridge.conftest import _FakeDesktopLocalDataStore, _FakeHandler, _make_api


@pytest.mark.parametrize(
    "payload,expected_status,expected_user_name",
    [
        pytest.param({"name": "Test User"}, 200, "Test User", id="success"),
        pytest.param({"name": ""}, 400, None, id="empty-name"),
        pytest.param(None, 400, None, id="no-payload"),
    ],
)
def test_sign_in(
    tmp_path: Path,
    payload: dict[str, Any] | None,
    expected_status: int,
    expected_user_name: str | None,
) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/sign-in",
        payload=payload,
    )

    assert result is True
    assert handler.sent[-1]["status"] == expected_status
    if expected_user_name is not None:
        assert handler.sent[-1]["payload"]["ok"] is True
        assert handler.sent[-1]["payload"]["user"]["name"] == expected_user_name
    else:
        assert handler.sent[-1]["payload"]["ok"] is False


def test_sign_out_success(tmp_path: Path) -> None:
    """Test successful sign-out."""
    store = _FakeDesktopLocalDataStore()
    store.sign_in("Test User")
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/sign-out",
        payload={},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True


def test_save_job_success(tmp_path: Path) -> None:
    """Test saving a job successfully."""
    store = _FakeDesktopLocalDataStore()
    store.sign_in("Test User")
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/saved-jobs/save",
        payload={
            "uid": "user_123",
            "job": {
                "title": "Engineer",
                "company": "Acme",
                "jobLink": "https://example.com/job/1",
            },
            "options": {},
        },
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True
    assert "jobKey" in handler.sent[-1]["payload"]


def test_save_job_empty_job(tmp_path: Path) -> None:
    """Test saving with empty job."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/saved-jobs/save",
        payload={"uid": "user_123", "job": {}, "options": {}},
    )

    assert result is True


def test_remove_job_success(tmp_path: Path) -> None:
    """Test removing a saved job."""
    store = _FakeDesktopLocalDataStore()
    store.sign_in("Test User")
    store.save_job_for_user("user_123", {"title": "Test"}, {})
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/saved-jobs/remove",
        payload={"uid": "user_123", "jobKey": "job_0"},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True


def test_update_status_success(tmp_path: Path) -> None:
    """Test updating job status."""
    store = _FakeDesktopLocalDataStore()
    store.sign_in("Test User")
    store.save_job_for_user("user_123", {"title": "Test"}, {})
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/saved-jobs/status",
        payload={
            "uid": "user_123",
            "jobKey": "job_0",
            "status": "applied",
            "options": {},
        },
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_update_notes_success(tmp_path: Path) -> None:
    """Test updating job notes."""
    store = _FakeDesktopLocalDataStore()
    store.sign_in("Test User")
    store.save_job_for_user("user_123", {"title": "Test"}, {})
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/saved-jobs/notes",
        payload={
            "uid": "user_123",
            "jobKey": "job_0",
            "notes": "Follow up next week",
        },
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_delete_by_id(tmp_path: Path) -> None:
    """Test deleting sources by ID."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/registry/delete",
        payload={"selected": ["src-1"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_delete_creates_tombstone_and_restore_deleted_reinstates_row(
    tmp_path: Path, monkeypatch
) -> None:
    tombstone_path = tmp_path / "source-registry-tombstones.json"
    monkeypatch.setattr(registry_tombstones, "TOMBSTONES_PATH", tombstone_path)

    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/registry/delete",
        payload={"ids": ["src-1"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert api.load_state()["active"] == []
    tombstones = registry_tombstones.load_tombstones(tombstone_path)
    assert "src-1" in tombstones

    restore_result = handle_post(
        handler,
        api=api,
        path="/registry/restore-deleted",
        payload={"ids": ["src-1"]},
    )

    assert restore_result is True
    assert handler.sent[-1]["status"] == 200
    assert api.load_state()["active"][-1]["id"] == "src-1"
    assert registry_tombstones.load_tombstones(tombstone_path) == {}


def test_delete_by_url(tmp_path: Path) -> None:
    """Test deleting sources by URL fingerprint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/registry/delete",
        payload={"selectedUrls": ["https://example.com/jobs"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_run_discovery(tmp_path: Path) -> None:
    """Test triggering discovery task."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/tasks/run-discovery",
        payload={"mode": "incremental"},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["started"] is True


def test_check_for_update(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    api.check_for_update = lambda **kw: {
        "currentVersion": "0.1.0",
        "latestVersion": "0.2.0",
        "updateAvailable": True,
        "availability": "available",
        "releaseNotesUrl": "https://example.com/releases/v0.2.0",
        "releaseNotesTitle": "Baluffo v0.2.0",
        "releaseNotesBody": "### Fixed\n- Notes",
        "releaseNotesPublishedAt": "2026-04-15T10:00:00Z",
    }

    handler = _FakeHandler()
    result = handle_post(handler, api=api, path="/app/check-for-update", payload={"force": True})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["updateAvailable"] is True
    assert handler.sent[-1]["payload"]["releaseNotesBody"] == "### Fixed\n- Notes"


def test_download_update(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    api.download_update = lambda: {"started": True, "status": {"downloadState": "downloading"}}

    handler = _FakeHandler()
    result = handle_post(handler, api=api, path="/app/download-update", payload={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["started"] is True


def test_download_update_failure_returns_structured_payload(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    api.download_update = lambda: {
        "started": False,
        "status": {"downloadState": "downloaded", "installState": "ready"},
        "error": "The update ZIP is already downloaded and ready to install.",
        "errorCode": "update_ready_to_install",
    }

    handler = _FakeHandler()
    result = handle_post(handler, api=api, path="/app/download-update", payload={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["started"] is False
    assert handler.sent[-1]["payload"]["errorCode"] == "update_ready_to_install"
    assert handler.sent[-1]["payload"]["status"]["installState"] == "ready"


def test_install_update_conflict(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    api.install_update = lambda: {
        "started": False,
        "status": {"downloadState": "idle", "installState": "idle"},
        "error": "Update ZIP is not ready to install.",
        "errorCode": "install_not_ready",
    }

    handler = _FakeHandler()
    result = handle_post(handler, api=api, path="/app/install-update", payload={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["errorCode"] == "install_not_ready"


def test_desktop_session_lifecycle_accepts_valid_payload(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/app/desktop-session-lifecycle",
        payload={
            "ownerToken": "desktop-owner-1",
            "sessionId": "desktop-session-1",
            "pageId": "page-1",
            "state": "alive",
        },
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["state"] == "alive"


def test_desktop_session_lifecycle_rejects_invalid_payload(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    api.update_desktop_session_lifecycle = lambda **_kw: (
        403,
        {"ok": False, "error": "Desktop session lifecycle token mismatch."},
    )

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/app/desktop-session-lifecycle",
        payload={
            "ownerToken": "wrong",
            "sessionId": "desktop-session-1",
            "pageId": "page-1",
            "state": "alive",
        },
    )

    assert result is True
    assert handler.sent[-1]["status"] == 403
    assert handler.sent[-1]["payload"]["ok"] is False


def test_run_discovery_response_write_failure_is_logged_and_returns_error_json(
    tmp_path: Path,
) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    log_calls: list[tuple[str, dict[str, Any]]] = []

    def bridge_log(level: str, message: str, **fields: Any) -> None:
        log_calls.append((message, {"level": level, **fields}))

    class _FlakyHandler(_FakeHandler):
        def __init__(self) -> None:
            super().__init__()
            self._first = True

        def _send_json(self, payload: Any, status: int = 200) -> None:
            if self._first:
                self._first = False
                raise BrokenPipeError("socket closed")
            super()._send_json(payload, status=status)

    api.bridge_log = bridge_log
    handler = _FlakyHandler()

    result = handle_post(
        handler,
        api=api,
        path="/tasks/run-discovery",
        payload={"preset": "default"},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 500
    assert handler.sent[-1]["payload"]["started"] is False
    assert any(
        message == "discovery_launch_response_write_failed" for message, _fields in log_calls
    )


def test_registry_approve_stamps_live_lifecycle_metadata(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    handler = _FakeHandler()

    result = handle_post(
        handler,
        api=api,
        path="/registry/approve",
        payload={"ids": ["src-2"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    approved_row = api.load_state()["active"][-1]
    assert approved_row["candidateState"] == "live"
    assert approved_row["approvedBy"] == "registry_manual_approve"
    assert approved_row["approvedAt"] == "2024-01-01T00:00:00Z"
    assert approved_row["liveAt"] == "2024-01-01T00:00:00Z"


def test_registry_reject_and_restore_update_candidate_lifecycle(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    handler = _FakeHandler()

    result = handle_post(
        handler,
        api=api,
        path="/registry/reject",
        payload={"ids": ["src-2"]},
    )

    assert result is True
    rejected_row = api.load_state()["rejected"][-1]
    assert rejected_row["candidateState"] == "quarantined"
    assert rejected_row["quarantinedAt"] == "2024-01-01T00:00:00Z"
    assert rejected_row["quarantineReason"] == "registry_reject"

    result = handle_post(
        handler,
        api=api,
        path="/registry/restore-rejected",
        payload={"ids": ["src-2"]},
    )

    assert result is True
    restored_row = api.load_state()["pending"][-1]
    assert restored_row["candidateState"] == "validated"
    assert restored_row["approvedAt"] == ""
    assert restored_row["liveAt"] == ""
    assert restored_row["quarantinedAt"] == ""


def test_registry_rollback_resets_live_row_to_validated(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    handler = _FakeHandler()

    api.persist_state_and_auto_sync(
        {
            "active": [
                {
                    "id": "src-1",
                    "adapter": "static",
                    "name": "Active Source",
                    "candidateState": "live",
                    "approvedAt": "2023-12-31T00:00:00Z",
                    "approvedBy": "registry_manual_approve",
                    "liveAt": "2023-12-31T00:00:00Z",
                }
            ],
            "pending": [],
            "rejected": [],
        }
    )

    result = handle_post(
        handler,
        api=api,
        path="/registry/rollback",
        payload={"ids": ["src-1"]},
    )

    assert result is True
    pending_row = api.load_state()["pending"][0]
    assert pending_row["candidateState"] == "validated"
    assert pending_row["approvedAt"] == ""
    assert pending_row["approvedBy"] == ""
    assert pending_row["liveAt"] == ""


def test_run_jobs_pipeline(tmp_path: Path) -> None:
    """Test triggering jobs pipeline."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/tasks/run-jobs-pipeline",
        payload={},
    )

    assert result is True


def test_run_sync_pull(tmp_path: Path) -> None:
    """Test triggering sync pull."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/tasks/run-sync-pull",
        payload={},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_run_sync_push(tmp_path: Path) -> None:
    """Test triggering sync push."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/tasks/run-sync-push",
        payload={},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_run_fetcher(tmp_path: Path) -> None:
    """Test triggering fetcher task."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/tasks/run-fetcher",
        payload={"sources": ["src-1"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_save_discovery_config_persists_and_returns_saved_payload(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    saved_payloads: list[dict[str, Any]] = []

    def update_saved_discovery_settings(payload: dict[str, Any]) -> dict[str, Any]:
        normalized = {
            "autoApproveHealthyPendingOnComplete": bool(
                (payload or {}).get("autoApproveHealthyPendingOnComplete", True)
            )
        }
        saved_payloads.append(normalized)
        return normalized

    api.update_saved_discovery_settings = update_saved_discovery_settings
    api.get_discovery_config_payload = lambda: {
        "ok": True,
        "savedConfig": saved_payloads[-1],
    }

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/discovery/config",
        payload={"autoApproveHealthyPendingOnComplete": False},
    )

    assert result is True
    assert saved_payloads == [{"autoApproveHealthyPendingOnComplete": False}]
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True
    assert (
        handler.sent[-1]["payload"]["savedConfig"]["autoApproveHealthyPendingOnComplete"] is False
    )


def test_open_url_uses_default_browser(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    with mock.patch(
        "src.bridge.routes.post_routes.webbrowser.open", return_value=True
    ) as open_mock:
        result = handle_post(
            handler,
            api=api,
            path="/desktop-local-data/open-url",
            payload={"url": "https://unity.com/careers/positions/7472070"},
        )

    assert result is True
    assert open_mock.call_args.args == ("https://unity.com/careers/positions/7472070",)
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True


def test_open_url_reports_failure_when_browser_cannot_open(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    with mock.patch("src.bridge.routes.post_routes.webbrowser.open", return_value=False):
        result = handle_post(
            handler,
            api=api,
            path="/desktop-local-data/open-url",
            payload={"url": "https://unity.com/careers/positions/7472070"},
        )

    assert result is True
    assert handler.sent[-1]["status"] == 500
    assert handler.sent[-1]["payload"]["ok"] is False


def test_ack_alert_success(tmp_path: Path) -> None:
    """Test acknowledging an alert."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/ops/alerts/ack",
        payload={"id": "alert-123"},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
