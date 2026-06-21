"""Tests for desktop app launcher orchestration launch timing behavior."""

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


def test_launch_desktop_app_defers_bridge_spawn_until_site_ready() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_runtime_config(
        data_dir=data_dir,
        owner_idle_timeout_s=7.5,
    )
    call_log: list[str] = []
    site_env: dict[str, str] = {}

    def _start_child_process(*args: object, **kwargs: object) -> SimpleNamespace:
        command = args[0]
        child_mode = str(command[2]) if isinstance(command, list) and len(command) > 2 else ""
        if child_mode == "__child_site__":
            assert "--bridge-host" in command
            assert command[command.index("--bridge-host") + 1] == "127.0.0.1"
            assert "--bridge-port" in command
            assert command[command.index("--bridge-port") + 1] == "8877"
            site_env.update(kwargs.get("extra_env") or {})
            call_log.append("spawn_site")
            return SimpleNamespace(pid=101)
        if child_mode == "__child_bridge__":
            assert "--owner-idle-timeout-s" in command
            assert command[command.index("--owner-idle-timeout-s") + 1] == "7.5"
            call_log.append("spawn_bridge")
            return SimpleNamespace(pid=202)
        raise AssertionError(f"unexpected child command: {command!r}")

    def _wait_for_url(*args: object, **kwargs: object) -> None:
        call_log.append("wait_for_url")

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
            side_effect=_start_child_process,
        ),
        mock.patch.object(desktop_app, "wait_for_url", side_effect=_wait_for_url),
        mock.patch.object(desktop_app, "is_baluffo_bridge_healthy", return_value=True),
        mock.patch.object(
            desktop_app,
            "launch_browser_for_url",
            return_value={
                "mode": "chromium-app",
                "browserName": "msedge",
                "browserPath": "C:/Edge/msedge.exe",
                "process": None,
                "windowShownAtMonotonic": 101.0,
            },
        ),
        mock.patch.object(desktop_app, "save_session_state"),
        mock.patch.object(desktop_app, "watch_browser_session", return_value="window_closed"),
        mock.patch.object(desktop_app, "write_success_marker"),
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(
            launcher_flow.launcher_recovery_mod, "cleanup_runtime_launch"
        ) as cleanup_mock,
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        desktop_app.launch_desktop_app(config)

    cleanup_mock.assert_called_once()
    assert call_log == ["spawn_site", "wait_for_url", "spawn_bridge"]
    assert site_env["BALUFFO_DESKTOP_BRIDGE_HOST"] == "127.0.0.1"
    assert site_env["BALUFFO_DESKTOP_BRIDGE_PORT"] == "8877"
    event_names = [call.args[1] for call in trace_mock.call_args_list]
    assert "desktop_bridge_spawn_deferred_until_site_ready" in event_names
    assert event_names.index("desktop_site_ready") < event_names.index("desktop_bridge_spawned")


def test_launch_desktop_app_emits_window_created_before_shell_window_shown() -> None:
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
        mock.patch.object(desktop_app, "is_baluffo_bridge_healthy", return_value=True),
        mock.patch.object(
            desktop_app,
            "launch_browser_for_url",
            return_value={
                "mode": "chromium-app",
                "browserName": "chrome",
                "browserPath": "C:/Chrome/chrome.exe",
                "browserPid": 4321,
                "process": None,
                "windowShownAtMonotonic": 101.0,
                "windowShownObserved": True,
                "windowPid": 4321,
                "windowTitle": "Baluffo",
            },
        ),
        mock.patch.object(desktop_app, "save_session_state"),
        mock.patch.object(desktop_app, "watch_browser_session", return_value="heartbeat_timeout"),
        mock.patch.object(desktop_app, "write_success_marker"),
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        desktop_app.launch_desktop_app(config)

    event_names = [call.args[1] for call in trace_mock.call_args_list]
    window_created_index = event_names.index("desktop_window_created")
    shell_shown_index = event_names.index("desktop_shell_window_shown")
    selected_call = next(
        call
        for call in trace_mock.call_args_list
        if call.args[1] == "desktop_browser_launch_selected"
    )
    shell_shown_call = next(
        call for call in trace_mock.call_args_list if call.args[1] == "desktop_shell_window_shown"
    )
    assert window_created_index < shell_shown_index
    assert int(selected_call.kwargs["elapsedMs"]) >= int(shell_shown_call.kwargs["elapsedMs"])
    assert "desktop_browser_launch_phase_diagnostics" in event_names


def test_launch_desktop_app_emits_inferred_shell_window_event_when_visibility_not_observed() -> (
    None
):
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
        mock.patch.object(desktop_app, "is_baluffo_bridge_healthy", return_value=True),
        mock.patch.object(
            desktop_app,
            "launch_browser_for_url",
            return_value={
                "mode": "chromium-app",
                "browserName": "chrome",
                "browserPath": "C:/Chrome/chrome.exe",
                "browserPid": 4321,
                "process": None,
                "windowShownAtMonotonic": 101.0,
                "windowShownObserved": False,
                "windowPid": 0,
                "windowTitle": "",
            },
        ),
        mock.patch.object(desktop_app, "save_session_state"),
        mock.patch.object(desktop_app, "watch_browser_session", return_value="heartbeat_timeout"),
        mock.patch.object(desktop_app, "write_success_marker"),
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        desktop_app.launch_desktop_app(config)

    event_names = [call.args[1] for call in trace_mock.call_args_list]
    assert "desktop_shell_window_shown_inferred" in event_names
    assert "desktop_shell_window_shown" not in event_names
    assert "desktop_browser_launch_phase_diagnostics" in event_names


def test_launch_desktop_app_waits_for_bridge_ready_before_browser_launch() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_runtime_config(
        data_dir=data_dir,
    )
    call_log: list[str] = []

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
        mock.patch.object(desktop_app, "is_baluffo_bridge_healthy", return_value=False),
        mock.patch.object(
            desktop_app,
            "wait_for_desktop_startup_ready",
            side_effect=lambda *args, **kwargs: (
                call_log.append("bridge_ready") or {"appVersion": APP_VERSION, "startupReady": True}
            ),
        ),
        mock.patch.object(
            desktop_app,
            "launch_browser_for_url",
            side_effect=lambda *args, **kwargs: (
                call_log.append("launch_browser")
                or {
                    "mode": "chromium-app",
                    "browserName": "chrome",
                    "browserPath": "C:/Chrome/chrome.exe",
                    "process": None,
                    "windowShownAtMonotonic": 101.0,
                }
            ),
        ) as launch_browser_mock,
        mock.patch.object(desktop_app, "save_session_state"),
        mock.patch.object(desktop_app, "watch_browser_session", return_value="heartbeat_timeout"),
        mock.patch.object(desktop_app, "write_success_marker"),
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        desktop_app.launch_desktop_app(config)

    launch_browser_mock.assert_called_once()
    assert call_log == ["bridge_ready", "launch_browser"]
    event_names = [call.args[1] for call in trace_mock.call_args_list]
    ready_index = event_names.index("desktop_bridge_ready_before_window")
    window_launch_index = event_names.index("desktop_window_create_started")
    assert ready_index < window_launch_index
    bridge_ready_index = event_names.index("desktop_bridge_ready_deferred")
    assert window_launch_index < bridge_ready_index


def test_launch_desktop_app_uses_tighter_site_ready_polling_for_startup_probe() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_runtime_config(
        data_dir=data_dir,
        startup_probe=True,
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
        mock.patch.object(desktop_app, "wait_for_url") as wait_for_url_mock,
        mock.patch.object(desktop_app, "is_baluffo_bridge_healthy", return_value=True),
        mock.patch.object(
            desktop_app,
            "launch_browser_for_url",
            return_value={
                "mode": "chromium-app",
                "browserName": "chrome",
                "browserPath": "C:/Chrome/chrome.exe",
                "process": None,
                "windowShownAtMonotonic": 101.0,
            },
        ),
        mock.patch.object(desktop_app, "save_session_state"),
        mock.patch.object(desktop_app, "watch_browser_session", return_value="heartbeat_timeout"),
        mock.patch.object(desktop_app, "write_success_marker"),
        mock.patch.object(desktop_app, "write_startup_summary"),
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        desktop_app.launch_desktop_app(config)

    wait_for_url_mock.assert_called_once_with(
        mock.ANY,
        timeout_s=desktop_app.READY_TIMEOUT_S,
        interval_s=desktop_app.STARTUP_PROBE_URL_READY_INTERVAL_S,
        trace_data_dir=data_dir,
    )


def test_launch_desktop_app_can_skip_browser_launch_for_packaged_rehearsal() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_runtime_config(
        data_dir=data_dir,
        no_browser=True,
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
        mock.patch.object(desktop_app, "launch_browser_for_url") as launch_browser_mock,
        mock.patch.object(desktop_app, "save_session_state") as save_mock,
        mock.patch.object(
            desktop_app, "watch_browser_session", return_value="heartbeat_timeout"
        ) as watch_mock,
        mock.patch.object(desktop_app, "write_success_marker"),
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        desktop_app.launch_desktop_app(config)

    launch_browser_mock.assert_not_called()
    save_payload = save_mock.call_args.args[0]
    assert save_payload["launchMode"] == "no-browser"
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


@pytest.mark.windows
def test_launch_desktop_app_retries_default_ports_after_bind_race() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    original_config = desktop_runtime_config(
        data_dir=data_dir,
    )
    retried_config = desktop_runtime_config(
        ship_root=original_config.ship_root,
        site_port=18080,
        bridge_port=18877,
        bridge_host=original_config.bridge_host,
        data_dir=data_dir,
        open_path=original_config.open_path,
        title=original_config.title,
    )
    start_child_process = mock.Mock(
        side_effect=[
            SimpleNamespace(pid=101),
            SimpleNamespace(pid=301),
            SimpleNamespace(pid=302),
        ]
    )
    wait_for_url = mock.Mock(side_effect=[RuntimeError("site port already in use"), None])
    trace_mock = mock.Mock()

    with (
        mock.patch.object(desktop_app, "get_valid_session_state", return_value={}),
        mock.patch.object(
            desktop_app,
            "acquire_instance_lock",
            return_value=desktop_app.InstanceLock(Path("C:/tmp/desktop.lock"), 1),
        ),
        mock.patch.object(desktop_app, "release_instance_lock"),
        mock.patch.object(
            desktop_app,
            "resolve_runtime_ports",
            side_effect=[original_config, retried_config],
        ) as resolve_ports_mock,
        mock.patch.object(desktop_app, "ensure_runtime_ports"),
        mock.patch.object(desktop_app, "start_child_process", start_child_process),
        mock.patch.object(desktop_app, "wait_for_url", wait_for_url),
        mock.patch.object(desktop_app, "_windows_create_kill_on_close_job", side_effect=[11, 12]),
        mock.patch.object(desktop_app, "_windows_close_desktop_job"),
        mock.patch.object(desktop_app, "publish_success_marker_when_ready_async"),
        mock.patch.object(desktop_app, "is_baluffo_bridge_healthy", return_value=True),
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
        mock.patch.object(desktop_app, "save_session_state") as save_mock,
        mock.patch.object(
            desktop_app, "watch_browser_session", return_value="heartbeat_timeout"
        ) as watch_mock,
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace", trace_mock),
    ):
        desktop_app.launch_desktop_app(original_config)

    assert resolve_ports_mock.call_count == 2
    assert start_child_process.call_count == 3
    assert wait_for_url.call_count == 2
    assert save_mock.call_args.args[0]["sitePort"] == 18080
    assert save_mock.call_args.args[0]["bridgePort"] == 18877
    watch_mock.assert_called_once()
    assert watch_mock.call_args.kwargs["bridge_port"] == 18877
    assert watch_mock.call_args.kwargs["browser_path"] == "C:/Edge/msedge.exe"
    assert any(call.args[1] == "desktop_runtime_port_retry" for call in trace_mock.call_args_list)


@pytest.mark.windows
def test_launch_desktop_app_keeps_explicit_ports_fail_fast_after_bind_race() -> None:
    config = desktop_runtime_config(
        site_port_explicit=True,
        bridge_port_explicit=True,
    )
    start_child_process = mock.Mock(return_value=SimpleNamespace(pid=101))

    with (
        mock.patch.object(desktop_app, "get_valid_session_state", return_value={}),
        mock.patch.object(
            desktop_app,
            "acquire_instance_lock",
            return_value=desktop_app.InstanceLock(Path("C:/tmp/desktop.lock"), 1),
        ),
        mock.patch.object(desktop_app, "release_instance_lock"),
        mock.patch.object(
            desktop_app, "resolve_runtime_ports", return_value=config
        ) as resolve_ports_mock,
        mock.patch.object(desktop_app, "ensure_runtime_ports"),
        mock.patch.object(desktop_app, "start_child_process", start_child_process),
        mock.patch.object(
            desktop_app,
            "wait_for_url",
            side_effect=RuntimeError("site port already in use"),
        ),
        mock.patch.object(desktop_app, "_windows_create_kill_on_close_job", return_value=11),
        mock.patch.object(desktop_app, "_windows_close_desktop_job"),
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        with pytest.raises(RuntimeError, match="site port already in use"):
            desktop_app.launch_desktop_app(config)

    assert resolve_ports_mock.call_count == 1
    start_child_process.assert_called_once()
