from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest import mock

from src import admin_bridge
from tests.helpers.ports import ADMIN_BRIDGE_TEST_PORT


def _configure_desktop_owner(root):
    cfg = admin_bridge.RuntimeConfig(
        root=root,
        data_dir=root,
        host="127.0.0.1",
        port=ADMIN_BRIDGE_TEST_PORT,
        log_format="human",
        log_level="info",
        quiet_requests=False,
        desktop_mode=True,
        owner_mode="desktop-window",
        owner_token="owner-1",
        desktop_session_id="session-1",
        started_by="test",
        owner_idle_timeout_s=15.0,
    )
    admin_bridge.configure_runtime_paths(cfg)


@contextmanager
def _mock_idle_owner_checks(now_utc):
    with (
        mock.patch.object(
            admin_bridge,
            "_get_ops_api",
            return_value=mock.Mock(
                get_current_task_state_payload=mock.Mock(return_value={"tasks": []})
            ),
        ),
        mock.patch.object(
            admin_bridge,
            "_get_desktop_update_service",
            return_value=mock.Mock(
                get_status_payload=mock.Mock(
                    return_value={"downloadState": "idle", "installState": "idle"}
                )
            ),
        ),
        mock.patch.object(admin_bridge, "now_utc", return_value=admin_bridge.parse_iso(now_utc)),
    ):
        yield


def test_regular_desktop_close_waits_for_short_reload_grace(admin_bridge_entrypoint_root):
    _configure_desktop_owner(admin_bridge_entrypoint_root)
    with mock.patch.object(admin_bridge, "now_iso", return_value="2026-03-01T00:00:00+00:00"):
        status_code, payload = admin_bridge.update_desktop_session_lifecycle(
            owner_token="owner-1",
            session_id="session-1",
            page_id="page-1",
            state="closing",
            reason="beforeunload",
        )
    assert status_code == 200
    assert payload["reason"] == "beforeunload"

    grace_s = admin_bridge.bridge_runtime_state.REGULAR_DESKTOP_CLOSE_SHUTDOWN_GRACE_S
    base = datetime(2026, 3, 1, tzinfo=UTC)
    with _mock_idle_owner_checks((base + timedelta(seconds=grace_s * 0.1)).isoformat()):
        assert admin_bridge.owner_session_should_exit() is False
    with _mock_idle_owner_checks((base + timedelta(seconds=grace_s + 0.1)).isoformat()):
        assert admin_bridge.owner_session_should_exit() is True


def test_regular_desktop_close_is_cleared_by_reloaded_page_alive(
    admin_bridge_entrypoint_root,
):
    _configure_desktop_owner(admin_bridge_entrypoint_root)
    with mock.patch.object(admin_bridge, "now_iso", return_value="2026-03-01T00:00:00+00:00"):
        status_code, payload = admin_bridge.update_desktop_session_lifecycle(
            owner_token="owner-1",
            session_id="session-1",
            page_id="page-1",
            state="closing",
            reason="beforeunload",
        )
    assert status_code == 200
    assert payload["reason"] == "beforeunload"

    with mock.patch.object(admin_bridge, "now_iso", return_value="2026-03-01T00:00:01+00:00"):
        status_code, payload = admin_bridge.update_desktop_session_lifecycle(
            owner_token="owner-1",
            session_id="session-1",
            page_id="page-2",
            state="alive",
        )
    assert status_code == 200
    assert payload["ok"] is True
    session_payload = admin_bridge.get_desktop_session_payload()
    assert session_payload["shutdownReason"] == ""
    assert session_payload["shutdownPageId"] == ""

    with mock.patch.object(
        admin_bridge,
        "now_utc",
        return_value=admin_bridge.parse_iso("2026-03-01T00:00:10+00:00"),
    ):
        assert admin_bridge.owner_session_should_exit() is False


def test_regular_close_is_cancelled_by_replacement_page_boot(admin_bridge_entrypoint_root):
    _configure_desktop_owner(admin_bridge_entrypoint_root)
    with mock.patch.object(admin_bridge, "now_iso", return_value="2026-03-01T00:00:00+00:00"):
        status_code, _ = admin_bridge.update_desktop_session_lifecycle(
            owner_token="owner-1",
            session_id="session-1",
            page_id="page-1",
            state="closing",
            reason="pagehide",
        )
    assert status_code == 200

    with mock.patch.object(
        admin_bridge, "now_iso", return_value="2026-03-01T00:00:00.600000+00:00"
    ):
        cancelled = admin_bridge.note_desktop_page_boot()

    assert cancelled is not None
    assert cancelled["requestedAt"] == "2026-03-01T00:00:00+00:00"
    assert cancelled["reason"] == "pagehide"
    assert cancelled["pageId"] == "page-1"
    assert cancelled["sessionId"] == "session-1"
    assert cancelled["cancelledBy"] == "page_boot"
    session_payload = admin_bridge.get_desktop_session_payload()
    assert session_payload["shutdownRequestedAt"] == ""
    assert session_payload["shutdownReason"] == ""
    assert session_payload["shutdownPageId"] == ""
    with _mock_idle_owner_checks("2026-03-01T00:00:03+00:00"):
        assert admin_bridge.owner_session_should_exit() is False


def test_page_boot_never_cancels_confirmed_active_work_close(admin_bridge_entrypoint_root):
    _configure_desktop_owner(admin_bridge_entrypoint_root)
    with mock.patch.object(admin_bridge, "now_iso", return_value="2026-03-01T00:00:00+00:00"):
        admin_bridge.update_desktop_session_lifecycle(
            owner_token="owner-1",
            session_id="session-1",
            page_id="page-1",
            state="closing",
            reason="confirmed_active_work_close",
        )

    assert admin_bridge.note_desktop_page_boot() is None
    with _mock_idle_owner_checks("2026-03-01T00:00:00.100000+00:00"):
        assert admin_bridge.owner_session_should_exit() is True
