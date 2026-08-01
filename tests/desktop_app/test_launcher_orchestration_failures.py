"""Tests for desktop app launcher orchestration failure behavior."""

import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src.ship import desktop_app
from tests.desktop_app._helpers import (
    desktop_runtime_config,
    isolate_desktop_startup_side_effects,
    launcher_session,
)
from tests.helpers.ports import ADMIN_BRIDGE_TEST_PORT


@pytest.fixture(autouse=True)
def _isolate_desktop_startup_side_effects(request, monkeypatch: pytest.MonkeyPatch) -> None:
    isolate_desktop_startup_side_effects(request, monkeypatch)


@pytest.mark.windows
def test_launch_desktop_app_still_shows_fatal_message_when_bridge_health_is_lost() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_runtime_config(
        data_dir=data_dir,
    )
    fake_browser_process = mock.Mock(spec=subprocess.Popen)
    fake_browser_process.pid = 303

    with mock.patch.object(desktop_app, "_windows_try_assign_pid_to_job"):
        with (
            mock.patch.object(desktop_app, "get_valid_session_state", return_value={}),
            mock.patch.object(
                desktop_app,
                "acquire_instance_lock",
                return_value=desktop_app.InstanceLock(Path("C:/tmp/desktop.lock"), 1),
            ),
            mock.patch.object(desktop_app, "release_instance_lock"),
            mock.patch.object(desktop_app, "resolve_runtime_ports", return_value=config),
            mock.patch.object(desktop_app, "ensure_runtime_ports"),
            mock.patch.object(
                desktop_app,
                "start_child_process",
                side_effect=[SimpleNamespace(pid=101), SimpleNamespace(pid=202)],
            ),
            mock.patch.object(desktop_app, "wait_for_url"),
            mock.patch.object(desktop_app, "publish_success_marker_when_ready_async"),
            mock.patch.object(
                desktop_app,
                "launch_browser_for_url",
                return_value={
                    "mode": "chromium-app",
                    "browserName": "msedge",
                    "browserPath": "C:/Edge/msedge.exe",
                    "process": fake_browser_process,
                },
            ),
            mock.patch.object(desktop_app, "save_session_state"),
            mock.patch.object(
                desktop_app, "watch_browser_session", return_value="heartbeat_timeout"
            ),
            mock.patch.object(
                desktop_app,
                "get_baluffo_bridge_health",
                return_value={
                    "service": "baluffo-bridge",
                    "desktopMode": True,
                    "owner": {"token": "other"},
                },
            ),
            mock.patch.object(
                desktop_app, "_bridge_health_matches_owner_session", return_value=False
            ),
            mock.patch.object(
                desktop_app,
                "_load_active_critical_desktop_tasks",
                return_value=[{"taskType": "fetch", "runId": "fetch_live_1", "status": "running"}],
            ),
            mock.patch.object(
                desktop_app, "_attempt_active_work_browser_relaunch", return_value=None
            ) as recover_mock,
            mock.patch.object(desktop_app, "clear_session_state"),
            mock.patch.object(desktop_app, "terminate_process"),
            mock.patch.object(desktop_app, "_append_startup_trace"),
            mock.patch.object(desktop_app, "_write_launch_diagnostics") as diagnostics_mock,
            mock.patch.object(desktop_app, "show_native_message") as show_message_mock,
        ):
            desktop_app.launch_desktop_app(config)

    recover_mock.assert_not_called()
    diagnostics_mock.assert_called_once()
    show_message_mock.assert_called_once()
    assert (
        "Baluffo closed unexpectedly while background work was still active"
        in show_message_mock.call_args.args[1]
    )


def test_launch_desktop_app_keeps_runtime_alive_when_browser_launch_fails() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_runtime_config(
        data_dir=data_dir,
    )
    open_url = desktop_app.build_open_url(config)
    trace_mock = mock.Mock()

    with (
        mock.patch.object(desktop_app, "get_valid_session_state", return_value={}),
        mock.patch.object(
            desktop_app,
            "acquire_instance_lock",
            return_value=desktop_app.InstanceLock(Path("C:/tmp/desktop.lock"), 1),
        ),
        mock.patch.object(desktop_app, "release_instance_lock"),
        mock.patch.object(desktop_app, "resolve_runtime_ports", return_value=config),
        mock.patch.object(desktop_app, "ensure_runtime_ports"),
        mock.patch.object(
            desktop_app,
            "start_child_process",
            side_effect=[SimpleNamespace(pid=101), SimpleNamespace(pid=202)],
        ),
        mock.patch.object(desktop_app, "wait_for_url"),
        mock.patch.object(desktop_app, "publish_success_marker_when_ready_async"),
        mock.patch.object(desktop_app, "is_baluffo_bridge_healthy", return_value=True),
        mock.patch.object(
            desktop_app,
            "launch_browser_for_url",
            side_effect=RuntimeError("Baluffo could not launch a browser window."),
        ),
        mock.patch.object(desktop_app, "show_native_message") as show_message_mock,
        mock.patch.object(desktop_app, "save_session_state") as save_mock,
        mock.patch.object(
            desktop_app, "watch_browser_session", return_value="heartbeat_timeout"
        ) as watch_mock,
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_write_launch_diagnostics"),
        mock.patch.object(desktop_app, "_append_startup_trace", trace_mock),
    ):
        desktop_app.launch_desktop_app(config)

    assert save_mock.call_args.args[0]["launchMode"] == "browser-launch-recovery"
    watch_mock.assert_called_once_with(
        data_dir,
        mock.ANY,
        bridge_port=ADMIN_BRIDGE_TEST_PORT,
        bridge_process=mock.ANY,
        browser_process=None,
        browser_pid=0,
        launch_accepted_elapsed_ms=mock.ANY,
        require_window=False,
        background_active_work_recovery=False,
        recovery_owner_token=mock.ANY,
    )
    show_message_mock.assert_called_once()
    assert open_url in show_message_mock.call_args.args[1]
    assert any(
        call.args[1] == "desktop_browser_launch_recovered" for call in trace_mock.call_args_list
    )


@pytest.mark.windows
def test_launch_desktop_app_fails_when_bridge_attach_fails() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_runtime_config(
        data_dir=data_dir,
    )
    site_process = mock.Mock(spec=subprocess.Popen)
    site_process.pid = 101
    site_process.poll.return_value = None

    with (
        mock.patch.object(desktop_app, "get_valid_session_state", return_value={}),
        mock.patch.object(
            desktop_app,
            "acquire_instance_lock",
            return_value=desktop_app.InstanceLock(Path("C:/tmp/desktop.lock"), 1),
        ),
        mock.patch.object(desktop_app, "release_instance_lock"),
        mock.patch.object(desktop_app, "resolve_runtime_ports", return_value=config),
        mock.patch.object(desktop_app, "ensure_runtime_ports"),
        mock.patch.object(
            desktop_app,
            "start_child_process",
            side_effect=[site_process, OSError("bridge attach failed")],
        ),
        mock.patch.object(desktop_app, "wait_for_url"),
        mock.patch.object(desktop_app, "_windows_create_kill_on_close_job", return_value=11),
        mock.patch.object(desktop_app, "_windows_close_desktop_job"),
        mock.patch.object(desktop_app, "save_session_state") as save_mock,
        mock.patch.object(desktop_app, "watch_browser_session") as watch_mock,
        mock.patch.object(desktop_app, "launch_browser_for_url") as launch_browser_mock,
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        with pytest.raises(OSError, match="bridge attach failed"):
            desktop_app.launch_desktop_app(config)

    save_mock.assert_not_called()
    watch_mock.assert_not_called()
    launch_browser_mock.assert_not_called()


@pytest.mark.windows
def test_launch_desktop_app_recovers_when_initial_browser_attach_fails() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_runtime_config(
        data_dir=data_dir,
    )
    open_url = desktop_app.build_open_url(config)
    site_process = mock.Mock(spec=subprocess.Popen)
    site_process.pid = 101
    site_process.poll.return_value = None
    bridge_process = mock.Mock(spec=subprocess.Popen)
    bridge_process.pid = 202
    bridge_process.poll.return_value = None
    trace_mock = mock.Mock()

    with (
        mock.patch.object(desktop_app, "get_valid_session_state", return_value={}),
        mock.patch.object(
            desktop_app,
            "acquire_instance_lock",
            return_value=desktop_app.InstanceLock(Path("C:/tmp/desktop.lock"), 1),
        ),
        mock.patch.object(desktop_app, "release_instance_lock"),
        mock.patch.object(desktop_app, "resolve_runtime_ports", return_value=config),
        mock.patch.object(desktop_app, "ensure_runtime_ports"),
        mock.patch.object(
            desktop_app,
            "start_child_process",
            side_effect=[site_process, bridge_process],
        ),
        mock.patch.object(desktop_app, "wait_for_url"),
        mock.patch.object(desktop_app, "_windows_create_kill_on_close_job", return_value=11),
        mock.patch.object(desktop_app, "_windows_close_desktop_job"),
        mock.patch.object(desktop_app, "publish_success_marker_when_ready_async"),
        mock.patch.object(desktop_app, "is_baluffo_bridge_healthy", return_value=True),
        mock.patch.object(
            desktop_app,
            "launch_browser_for_url",
            side_effect=OSError("browser attach failed"),
        ) as launch_browser_mock,
        mock.patch.object(desktop_app, "show_native_message") as show_message_mock,
        mock.patch.object(desktop_app, "save_session_state") as save_mock,
        mock.patch.object(
            desktop_app, "watch_browser_session", return_value="heartbeat_timeout"
        ) as watch_mock,
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_write_launch_diagnostics"),
        mock.patch.object(desktop_app, "_append_startup_trace", trace_mock),
    ):
        desktop_app.launch_desktop_app(config)

    assert save_mock.call_args.args[0]["launchMode"] == "browser-launch-recovery"
    assert launch_browser_mock.call_args.kwargs["job_handle"] == 11
    watch_mock.assert_called_once_with(
        data_dir,
        mock.ANY,
        bridge_port=ADMIN_BRIDGE_TEST_PORT,
        bridge_process=mock.ANY,
        browser_process=None,
        browser_pid=0,
        launch_accepted_elapsed_ms=mock.ANY,
        require_window=False,
        background_active_work_recovery=False,
        recovery_owner_token=mock.ANY,
    )
    show_message_mock.assert_called_once()
    assert open_url in show_message_mock.call_args.args[1]
    assert any(
        call.args[1] == "desktop_browser_launch_recovered" for call in trace_mock.call_args_list
    )


def test_attempt_active_work_browser_relaunch_returns_none_when_job_attach_fails() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_runtime_config(
        data_dir=data_dir,
    )
    trace_mock = mock.Mock()

    with (
        mock.patch.object(desktop_app, "bridge_last_activity_ts", return_value=0.0),
        mock.patch.object(
            desktop_app,
            "launch_browser_for_url",
            side_effect=OSError("browser attach failed"),
        ) as launch_browser_mock,
        mock.patch.object(desktop_app, "terminate_process") as terminate_mock,
        mock.patch.object(desktop_app, "_append_startup_trace", trace_mock),
    ):
        result = desktop_app._attempt_active_work_browser_relaunch(
            config=config,
            open_url=desktop_app.build_open_url(config),
            preferred_browser_path="C:/Chrome/chrome.exe",
            started_mono=100.0,
            desktop_job=11,
            stop_reason="heartbeat_timeout",
            active_tasks=[{"taskType": "fetch", "runId": "fetch_live_1"}],
        )

    assert result is None
    assert launch_browser_mock.call_args.kwargs["job_handle"] == 11
    terminate_mock.assert_not_called()
    assert any(
        call.args[1] == "desktop_browser_relaunch_failed" for call in trace_mock.call_args_list
    )
    assert not any(
        call.args[1] == "desktop_browser_relaunch_accepted" for call in trace_mock.call_args_list
    )


def test_launch_desktop_app_fails_when_instance_lock_is_contended_and_session_exists() -> None:
    config = desktop_runtime_config()
    session = launcher_session()

    with (
        mock.patch.object(desktop_app, "acquire_instance_lock", return_value=None),
        mock.patch.object(
            desktop_app,
            "diagnose_instance_conflict",
            return_value={"action": "active", "session": session},
        ),
        mock.patch.object(desktop_app, "resolve_runtime_ports", return_value=config),
        mock.patch.object(desktop_app, "start_child_process") as start_mock,
        mock.patch.object(
            desktop_app,
            "_desktop_update_restart_snapshot",
            return_value={
                "handoffRequestPresent": False,
                "updateInstallState": "idle",
                "updateInstallStage": "idle",
            },
        ),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        with pytest.raises(RuntimeError, match="Baluffo is already running"):
            desktop_app.launch_desktop_app(config)

    start_mock.assert_not_called()
    assert any(
        call.args[1] == "desktop_launch_rejected_already_running"
        and call.kwargs["detection"] == "instance_lock_contended"
        and call.kwargs["existingLauncherToken"] == "existing-launcher-token"
        and call.kwargs["handoffRequestPresent"] is False
        and call.kwargs["updateInstallState"] == "idle"
        and call.kwargs["updateInstallStage"] == "idle"
        for call in trace_mock.call_args_list
    )
