from __future__ import annotations

from unittest import mock

from src import admin_bridge
from src.bridge.api import BridgeApi
from tests.helpers.bridge_api import BridgeRuntimeConfigStub


def test_desktop_owner_session_stays_alive_when_lifecycle_refreshes_activity(
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
        return_value="2026-03-01T00:00:01+00:00",
    ):
        status_code, payload = admin_bridge.update_desktop_session_lifecycle(
            owner_token="owner-1",
            session_id="session-1",
            page_id="page-1",
            state="alive",
        )
    assert status_code == 200
    assert payload["ok"] is True

    with mock.patch.object(
        admin_bridge,
        "now_iso",
        return_value="2026-03-01T00:00:18+00:00",
    ):
        admin_bridge.mark_desktop_session_activity("/ops/health")
    assert (
        admin_bridge.bridge_runtime_state.get_owner_state()["lastActivityAt"]
        == "2026-03-01T00:00:01+00:00"
    )

    with mock.patch.object(
        admin_bridge,
        "now_iso",
        return_value="2026-03-01T00:00:18+00:00",
    ):
        status_code, payload = admin_bridge.update_desktop_session_lifecycle(
            owner_token="owner-1",
            session_id="session-1",
            page_id="page-1",
            state="alive",
        )
    assert status_code == 200
    assert payload["ok"] is True

    with mock.patch.object(
        admin_bridge,
        "now_utc",
        return_value=admin_bridge.parse_iso("2026-03-01T00:00:20+00:00"),
    ):
        assert admin_bridge.owner_session_should_exit() is False


def test_lightweight_ops_health_exposes_desktop_owner_identity(
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

    health = admin_bridge.compute_ops_health()

    assert health["service"] == "baluffo-bridge"
    assert health["desktopMode"] is True
    assert health["startupReady"] is True
    assert health["owner"]["mode"] == "desktop-window"
    assert health["owner"]["token"] == "owner-1"
    assert health["owner"]["sessionId"] == "session-1"


def test_bridge_api_ignores_health_activity_for_desktop_window_owner(tmp_path):
    calls: list[str] = []
    api = BridgeApi(
        runtime_config=BridgeRuntimeConfigStub(
            root=tmp_path,
            data_dir=tmp_path,
            desktop_mode=True,
            owner_mode="desktop-window",
        ),
        DISCOVERY_REPORT_PATH=tmp_path / "discovery-report.json",
        JOBS_FETCH_REPORT_PATH=tmp_path / "jobs-fetch-report.json",
        APPROVAL_STATE_PATH=tmp_path / "approval.json",
        DISCOVERY_LOG_PATH=tmp_path / "discovery.log",
        FETCHER_LOG_PATH=tmp_path / "fetcher.log",
        STARTUP_METRICS_PATH=tmp_path / "startup-metrics.jsonl",
        DESKTOP_SESSION_ACTIVITY_AT="2026-03-01T00:00:01+00:00",
        now_iso=lambda: "2026-03-01T00:00:18+00:00",
        _mark_desktop_session_activity=lambda path: calls.append(path),
    )

    api.mark_desktop_session_activity("/ops/health")

    assert calls == []
    assert api.DESKTOP_SESSION_ACTIVITY_AT == "2026-03-01T00:00:01+00:00"


def test_bridge_api_uses_non_health_route_activity_for_desktop_window_owner(tmp_path):
    calls: list[str] = []
    api = BridgeApi(
        runtime_config=BridgeRuntimeConfigStub(
            root=tmp_path,
            data_dir=tmp_path,
            desktop_mode=True,
            owner_mode="desktop-window",
        ),
        DISCOVERY_REPORT_PATH=tmp_path / "discovery-report.json",
        JOBS_FETCH_REPORT_PATH=tmp_path / "jobs-fetch-report.json",
        APPROVAL_STATE_PATH=tmp_path / "approval.json",
        DISCOVERY_LOG_PATH=tmp_path / "discovery.log",
        FETCHER_LOG_PATH=tmp_path / "fetcher.log",
        STARTUP_METRICS_PATH=tmp_path / "startup-metrics.jsonl",
        DESKTOP_SESSION_ACTIVITY_AT="2026-03-01T00:00:01+00:00",
        now_iso=lambda: "2026-03-01T00:00:18+00:00",
        _mark_desktop_session_activity=lambda path: calls.append(path),
    )

    api.mark_desktop_session_activity("/ops/task-state")

    assert calls == ["/ops/task-state"]
    assert api.DESKTOP_SESSION_ACTIVITY_AT == "2026-03-01T00:00:18+00:00"


def test_desktop_owner_route_activity_prevents_false_idle_exit_after_closing_signal(
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
        side_effect=["2026-03-01T00:00:00+00:00", "2026-03-01T00:00:01+00:00"],
    ):
        admin_bridge.update_desktop_session_lifecycle(
            owner_token="owner-1",
            session_id="session-1",
            page_id="page-1",
            state="alive",
        )
        admin_bridge.update_desktop_session_lifecycle(
            owner_token="owner-1",
            session_id="session-1",
            page_id="page-1",
            state="closing",
        )

    with mock.patch.object(
        admin_bridge,
        "now_iso",
        return_value="2026-03-01T00:00:18+00:00",
    ):
        admin_bridge.mark_desktop_session_activity("/ops/task-state")

    with mock.patch.object(
        admin_bridge,
        "now_utc",
        return_value=admin_bridge.parse_iso("2026-03-01T00:00:20+00:00"),
    ):
        assert admin_bridge.owner_session_should_exit() is False

    with mock.patch.object(
        admin_bridge,
        "now_utc",
        return_value=admin_bridge.parse_iso("2026-03-01T00:00:34+00:00"),
    ):
        assert admin_bridge.owner_session_should_exit() is True
