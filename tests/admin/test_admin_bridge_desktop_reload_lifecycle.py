from __future__ import annotations

from contextlib import contextmanager
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

    with _mock_idle_owner_checks("2026-03-01T00:00:00.250000+00:00"):
        assert admin_bridge.owner_session_should_exit() is False
    with _mock_idle_owner_checks("2026-03-01T00:00:01+00:00"):
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
