from __future__ import annotations

from unittest import mock

from src import admin_bridge


def test_desktop_owner_session_stays_alive_when_requests_refresh_activity(
    admin_bridge_entrypoint_root,
):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
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
    with mock.patch.object(
        admin_bridge,
        "now_iso",
        side_effect=["2026-03-01T00:00:01+00:00", "2026-03-01T00:00:18+00:00"],
    ):
        status_code, payload = admin_bridge.update_desktop_session_lifecycle(
            owner_token="owner-1",
            session_id="session-1",
            page_id="page-1",
            state="alive",
        )
        admin_bridge.mark_desktop_session_activity("/registry/conflicts")
    assert status_code == 200
    assert payload["ok"] is True

    with mock.patch.object(
        admin_bridge,
        "now_utc",
        return_value=admin_bridge.parse_iso("2026-03-01T00:00:20+00:00"),
    ):
        assert admin_bridge.owner_session_should_exit() is False
