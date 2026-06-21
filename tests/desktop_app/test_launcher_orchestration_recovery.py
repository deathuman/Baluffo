"""Tests for desktop app launcher orchestration update and browser recovery behavior."""

import subprocess
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src.app_version import APP_VERSION
from src.ship import desktop_app
from src.ship.desktop_app import launcher_flow
from tests.desktop_app._helpers import (
    desktop_runtime_config,
)
from tests.helpers.ports import ADMIN_BRIDGE_TEST_PORT


@pytest.fixture(autouse=True)
def _isolate_desktop_startup_side_effects(request, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        desktop_app, "_windows_create_kill_on_close_job", mock.Mock(return_value=None)
    )
    monkeypatch.setattr(desktop_app, "_windows_close_desktop_job", mock.Mock())
    monkeypatch.setattr(desktop_app, "get_baluffo_bridge_health", mock.Mock(return_value={}))
    monkeypatch.setattr(
        desktop_app, "_load_active_critical_desktop_tasks", mock.Mock(return_value=[])
    )
    monkeypatch.setattr(desktop_app, "load_session_state", mock.Mock(return_value={}))
    monkeypatch.setattr(
        desktop_app,
        "_reclaim_stale_instance_artifacts",
        mock.Mock(side_effect=AssertionError("unexpected stale runtime reclaim")),
    )
    if request.node.name.startswith("test_publish_success_marker_when_ready_async"):
        return
    monkeypatch.setattr(
        desktop_app,
        "wait_for_desktop_startup_ready",
        mock.Mock(return_value={"appVersion": APP_VERSION, "startupReady": True}),
    )
    monkeypatch.setattr(desktop_app, "publish_success_marker_when_ready_async", mock.Mock())


def test_launch_desktop_app_spawns_update_helper_from_launcher_on_install_request() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_runtime_config(
        data_dir=data_dir,
    )

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
        mock.patch.object(
            desktop_app,
            "wait_for_desktop_startup_ready",
            return_value={"appVersion": APP_VERSION},
        ),
        mock.patch.object(
            desktop_app,
            "launch_browser_for_url",
            return_value={
                "mode": "chromium-app",
                "browserName": "msedge",
                "browserPath": "C:/Edge/msedge.exe",
                "process": None,
            },
        ),
        mock.patch.object(desktop_app, "save_session_state"),
        mock.patch.object(
            desktop_app, "watch_browser_session", return_value="update_install_requested"
        ),
        mock.patch.object(desktop_app, "launch_staged_update_helper") as launch_helper_mock,
        mock.patch.object(desktop_app, "write_success_marker"),
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        desktop_app.launch_desktop_app(config)

    launch_helper_mock.assert_called_once()
    helper_paths = launch_helper_mock.call_args.args[0]
    assert helper_paths.install_root == config.ship_root


@pytest.mark.windows
def test_launch_desktop_app_does_not_recover_to_default_browser_after_process_exit() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_runtime_config(
        data_dir=data_dir,
    )
    fake_browser_process = mock.Mock(spec=subprocess.Popen)

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
        mock.patch.object(
            desktop_app,
            "wait_for_desktop_startup_ready",
            return_value={"appVersion": APP_VERSION},
        ),
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
        mock.patch.object(desktop_app, "_windows_try_assign_pid_to_job"),
        mock.patch.object(desktop_app, "save_session_state"),
        mock.patch.object(
            desktop_app, "watch_browser_session", return_value="process_exit"
        ) as watch_mock,
        mock.patch.object(desktop_app, "write_success_marker"),
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        desktop_app.launch_desktop_app(config)

    watch_mock.assert_called_once_with(
        data_dir,
        mock.ANY,
        bridge_port=ADMIN_BRIDGE_TEST_PORT,
        bridge_process=mock.ANY,
        browser_process=fake_browser_process,
        browser_pid=0,
        browser_name="msedge",
        browser_path="C:/Edge/msedge.exe",
        launch_accepted_elapsed_ms=mock.ANY,
        require_window=True,
        background_active_work_recovery=False,
        recovery_owner_token=mock.ANY,
    )


@pytest.mark.windows
def test_launch_desktop_app_attempts_one_browser_relaunch_when_active_work_loses_heartbeat() -> (
    None
):
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_runtime_config(
        data_dir=data_dir,
    )
    fake_browser_process = mock.Mock(spec=subprocess.Popen)
    fake_browser_process.pid = 303
    recovered_browser_process = mock.Mock(spec=subprocess.Popen)
    recovered_browser_process.pid = 404

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
        mock.patch.object(
            desktop_app,
            "wait_for_desktop_startup_ready",
            return_value={"appVersion": APP_VERSION},
        ),
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
        mock.patch.object(desktop_app, "_windows_try_assign_pid_to_job"),
        mock.patch.object(desktop_app, "save_session_state"),
        mock.patch.object(
            desktop_app,
            "watch_browser_session",
            side_effect=["heartbeat_timeout", "window_closed"],
        ) as watch_mock,
        mock.patch.object(
            desktop_app,
            "get_baluffo_bridge_health",
            side_effect=[
                {"service": "baluffo-bridge", "desktopMode": True, "owner": {"token": "live"}},
                {"service": "baluffo-bridge", "desktopMode": True, "owner": {"token": "live"}},
            ],
        ),
        mock.patch.object(desktop_app, "_bridge_health_matches_owner_session", return_value=True),
        mock.patch.object(
            desktop_app,
            "_load_active_critical_desktop_tasks",
            return_value=[{"taskType": "fetch", "runId": "fetch_live_1", "status": "running"}],
        ),
        mock.patch.object(
            desktop_app,
            "_attempt_active_work_browser_relaunch",
            return_value={
                "mode": "chromium-app",
                "browserName": "msedge",
                "browserPath": "C:/Edge/msedge.exe",
                "process": recovered_browser_process,
            },
        ) as recover_mock,
        mock.patch.object(desktop_app, "write_success_marker"),
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
        mock.patch.object(desktop_app, "show_native_message") as show_message_mock,
    ):
        desktop_app.launch_desktop_app(config)

    assert watch_mock.call_count == 2
    recover_mock.assert_called_once()
    show_message_mock.assert_not_called()


@pytest.mark.windows
def test_launch_desktop_app_skips_active_work_relaunch_after_confirmed_close() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_runtime_config(
        data_dir=data_dir,
    )
    fake_browser_process = mock.Mock(spec=subprocess.Popen)
    fake_browser_process.pid = 303

    with ExitStack() as stack:
        stack.enter_context(
            mock.patch.object(
                launcher_flow.uuid,
                "uuid4",
                side_effect=[
                    SimpleNamespace(hex="launchertoken1"),
                    SimpleNamespace(hex="desktopsession1"),
                    SimpleNamespace(hex="ownertoken1"),
                ],
            )
        )
        stack.enter_context(
            mock.patch.object(desktop_app, "get_valid_session_state", return_value={})
        )
        stack.enter_context(
            mock.patch.object(
                desktop_app,
                "acquire_instance_lock",
                return_value=desktop_app.InstanceLock(Path("C:/tmp/desktop.lock"), 1),
            )
        )
        stack.enter_context(mock.patch.object(desktop_app, "release_instance_lock"))
        stack.enter_context(
            mock.patch.object(desktop_app, "resolve_runtime_ports", return_value=config)
        )
        stack.enter_context(mock.patch.object(desktop_app, "ensure_runtime_ports"))
        stack.enter_context(
            mock.patch.object(
                desktop_app,
                "start_child_process",
                side_effect=[SimpleNamespace(pid=101), SimpleNamespace(pid=202)],
            )
        )
        stack.enter_context(mock.patch.object(desktop_app, "wait_for_url"))
        stack.enter_context(
            mock.patch.object(
                desktop_app,
                "wait_for_desktop_startup_ready",
                return_value={"appVersion": APP_VERSION},
            )
        )
        stack.enter_context(
            mock.patch.object(
                desktop_app,
                "launch_browser_for_url",
                return_value={
                    "mode": "chromium-app",
                    "browserName": "msedge",
                    "browserPath": "C:/Edge/msedge.exe",
                    "process": fake_browser_process,
                },
            )
        )
        stack.enter_context(mock.patch.object(desktop_app, "_windows_try_assign_pid_to_job"))
        stack.enter_context(mock.patch.object(desktop_app, "save_session_state"))
        stack.enter_context(
            mock.patch.object(
                desktop_app,
                "watch_browser_session",
                return_value="heartbeat_timeout",
            )
        )
        stack.enter_context(
            mock.patch.object(
                desktop_app,
                "get_baluffo_bridge_health",
                return_value={
                    "service": "baluffo-bridge",
                    "desktopMode": True,
                    "owner": {"token": "ownertoken1"},
                },
            )
        )
        stack.enter_context(
            mock.patch.object(
                desktop_app, "_bridge_health_matches_owner_session", return_value=True
            )
        )
        stack.enter_context(
            mock.patch.object(
                desktop_app,
                "_load_active_critical_desktop_tasks",
                return_value=[{"taskType": "fetch", "runId": "fetch_live_1", "status": "running"}],
            )
        )
        stack.enter_context(
            mock.patch.object(
                desktop_app,
                "read_startup_metrics",
                return_value=[
                    {
                        "event": "desktop_confirmed_active_work_shutdown_requested",
                        "payload": {"sessionId": "desktopsession1"},
                    }
                ],
            )
        )
        recover_mock = stack.enter_context(
            mock.patch.object(desktop_app, "_attempt_active_work_browser_relaunch")
        )
        stack.enter_context(mock.patch.object(desktop_app, "write_success_marker"))
        stack.enter_context(mock.patch.object(desktop_app, "clear_session_state"))
        stack.enter_context(mock.patch.object(desktop_app, "terminate_process"))
        trace_mock = stack.enter_context(mock.patch.object(desktop_app, "_append_startup_trace"))
        show_message_mock = stack.enter_context(
            mock.patch.object(desktop_app, "show_native_message")
        )
        desktop_app.launch_desktop_app(config)

    recover_mock.assert_not_called()
    show_message_mock.assert_not_called()
    assert any(
        call.args[1] == "desktop_confirmed_active_work_shutdown_cleanup"
        for call in trace_mock.call_args_list
    )


@pytest.mark.windows
def test_launch_desktop_app_enters_background_recovery_after_recovery_budget_is_exhausted() -> None:
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
                desktop_app,
                "watch_browser_session",
                side_effect=["heartbeat_timeout", "active_work_completed"],
            ),
            mock.patch.object(
                desktop_app,
                "get_baluffo_bridge_health",
                side_effect=[
                    {"service": "baluffo-bridge", "desktopMode": True, "owner": {"token": "live"}},
                    {"service": "baluffo-bridge", "desktopMode": True, "owner": {"token": "live"}},
                ],
            ),
            mock.patch.object(
                desktop_app, "_bridge_health_matches_owner_session", return_value=True
            ),
            mock.patch.object(
                desktop_app,
                "_load_active_critical_desktop_tasks",
                return_value=[{"taskType": "fetch", "runId": "fetch_live_1", "status": "running"}],
            ),
            mock.patch.object(
                desktop_app,
                "_attempt_active_work_browser_relaunch",
                return_value=None,
            ) as recover_mock,
            mock.patch.object(desktop_app, "clear_session_state"),
            mock.patch.object(desktop_app, "terminate_process"),
            mock.patch.object(desktop_app, "_append_startup_trace"),
            mock.patch.object(desktop_app, "_write_launch_diagnostics") as diagnostics_mock,
            mock.patch.object(desktop_app, "show_native_message") as show_message_mock,
        ):
            desktop_app.launch_desktop_app(config)

    recover_mock.assert_called_once()
    diagnostics_mock.assert_called_once()
    show_message_mock.assert_called_once()
    assert "Baluffo is still running." in show_message_mock.call_args.args[1]
    assert "Active tasks: fetch" in show_message_mock.call_args.args[1]


@pytest.mark.windows
def test_launch_desktop_app_enters_background_recovery_for_process_exit() -> None:
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
                desktop_app,
                "watch_browser_session",
                side_effect=["process_exit", "active_work_completed"],
            ),
            mock.patch.object(
                desktop_app,
                "get_baluffo_bridge_health",
                return_value={
                    "service": "baluffo-bridge",
                    "desktopMode": True,
                    "owner": {"token": "live"},
                },
            ),
            mock.patch.object(
                desktop_app, "_bridge_health_matches_owner_session", return_value=True
            ),
            mock.patch.object(
                desktop_app,
                "_load_active_critical_desktop_tasks",
                return_value=[{"taskType": "pipeline", "runId": "pipeline_1", "status": "running"}],
            ),
            mock.patch.object(
                desktop_app, "_attempt_active_work_browser_relaunch", return_value=None
            ),
            mock.patch.object(desktop_app, "clear_session_state"),
            mock.patch.object(desktop_app, "terminate_process"),
            mock.patch.object(desktop_app, "_append_startup_trace"),
            mock.patch.object(desktop_app, "_write_launch_diagnostics") as diagnostics_mock,
            mock.patch.object(desktop_app, "show_native_message") as show_message_mock,
        ):
            desktop_app.launch_desktop_app(config)

    diagnostics_mock.assert_called_once()
    show_message_mock.assert_called_once()
    assert "Reason: process_exit" in show_message_mock.call_args.args[1]
    assert "Active tasks: pipeline" in show_message_mock.call_args.args[1]
