import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src.app_version import APP_VERSION
from src.ship import desktop_app
from src.ship.desktop_app import launcher_flow
from tests.desktop_app._helpers import (
    desktop_runtime_config,
    launcher_session,
    stale_launcher_session,
)
from tests.helpers.temp_paths import workspace_tmpdir


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
    monkeypatch.setattr(desktop_app, "publish_success_marker_when_ready_async", mock.Mock())


def test_main_surfaces_native_error_without_installer_prompt() -> None:
    with (
        mock.patch.object(desktop_app, "create_runtime_config", return_value=object()),
        mock.patch.object(
            desktop_app,
            "launch_desktop_app",
            side_effect=RuntimeError(
                "Baluffo could not launch a browser window for the desktop session."
            ),
        ),
        mock.patch.object(desktop_app, "show_native_message", return_value=False) as show_mock,
        mock.patch.object(desktop_app.webbrowser, "open") as open_mock,
    ):
        exit_code = desktop_app.main([])

    assert exit_code == 1
    show_mock.assert_called_once()
    open_mock.assert_not_called()


def test_main_child_script_mode_runs_script_with_forwarded_args() -> None:
    with (
        mock.patch.object(desktop_app, "runpy") as runpy_mock,
        mock.patch.object(desktop_app.Path, "exists", return_value=True),
    ):
        exit_code = desktop_app.main(
            [
                "__child_script__",
                "--root",
                str(Path.cwd()),
                "--script",
                "source_discovery.py",
                "--",
                "--mode",
                "dynamic",
            ]
        )
    assert exit_code == 0
    runpy_mock.run_path.assert_called_once()


def test_launch_desktop_app_fails_when_active_session_exists_without_spawning_children() -> None:
    config = desktop_runtime_config()
    session = launcher_session()

    with (
        mock.patch.object(desktop_app, "get_valid_session_state", return_value=session),
        mock.patch.object(
            desktop_app,
            "acquire_instance_lock",
            return_value=desktop_app.InstanceLock(Path("C:/tmp/desktop.lock"), 1),
        ),
        mock.patch.object(desktop_app, "release_instance_lock"),
        mock.patch.object(desktop_app, "resolve_runtime_ports", return_value=config),
        mock.patch.object(desktop_app, "start_child_process") as start_mock,
        mock.patch.object(
            desktop_app,
            "_desktop_update_restart_snapshot",
            return_value={
                "handoffRequestPresent": True,
                "updateInstallState": "handoff_requested",
                "updateInstallStage": "preparing",
            },
        ),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        with pytest.raises(RuntimeError, match="Baluffo is already running"):
            desktop_app.launch_desktop_app(config)

    start_mock.assert_not_called()
    assert any(
        call.args[1] == "desktop_launch_rejected_already_running"
        and call.kwargs["detection"] == "valid_session_state"
        and call.kwargs["existingLauncherToken"] == "existing-launcher-token"
        and call.kwargs["handoffRequestPresent"] is True
        and call.kwargs["updateInstallState"] == "handoff_requested"
        and call.kwargs["updateInstallStage"] == "preparing"
        for call in trace_mock.call_args_list
    )


def test_launch_desktop_app_raises_when_stale_runtime_cleanup_is_blocked() -> None:
    config = desktop_runtime_config()

    with (
        mock.patch.object(desktop_app, "acquire_instance_lock", return_value=None),
        mock.patch.object(
            desktop_app,
            "diagnose_instance_conflict",
            return_value={"action": "blocked", "target": "bridge"},
        ),
        mock.patch.object(desktop_app, "start_child_process") as start_child_mock,
        pytest.raises(RuntimeError, match="stale bridge process"),
    ):
        desktop_app.launch_desktop_app(config)

    start_child_mock.assert_not_called()


def test_launch_desktop_app_reclaims_stale_session_before_resolving_explicit_ports() -> None:
    config = desktop_runtime_config(
        site_port_explicit=True,
        bridge_port_explicit=True,
    )
    call_order: list[str] = []
    stale_session = stale_launcher_session()

    def _resolve_runtime_ports(
        current: desktop_app.DesktopRuntimeConfig,
    ) -> desktop_app.DesktopRuntimeConfig:
        call_order.append("resolve")
        return current

    def _ensure_runtime_ports(_config: desktop_app.DesktopRuntimeConfig) -> None:
        call_order.append("ensure")
        raise RuntimeError("stop after resolve")

    def _load_session_state(*args: object, **kwargs: object) -> dict[str, object]:
        call_order.append("load")
        return stale_session

    def _validate_session_state(*args: object, **kwargs: object) -> tuple[bool, str]:
        call_order.append("validate")
        return False, "launcher_pid_inactive"

    def _reclaim_stale_instance_artifacts(*args: object, **kwargs: object) -> dict[str, object]:
        call_order.append("reclaim")
        assert kwargs["stale_state"] == stale_session
        return {"blocked": False}

    with (
        mock.patch.object(
            desktop_app,
            "acquire_instance_lock",
            return_value=desktop_app.InstanceLock(
                Path("C:/tmp/desktop.lock"), 1, launcher_token="launcher-token"
            ),
        ),
        mock.patch.object(desktop_app, "load_session_state", side_effect=_load_session_state),
        mock.patch.object(
            desktop_app, "validate_session_state", side_effect=_validate_session_state
        ),
        mock.patch.object(
            desktop_app,
            "_reclaim_stale_instance_artifacts",
            side_effect=_reclaim_stale_instance_artifacts,
        ),
        mock.patch.object(desktop_app, "resolve_runtime_ports", side_effect=_resolve_runtime_ports),
        mock.patch.object(desktop_app, "ensure_runtime_ports", side_effect=_ensure_runtime_ports),
        mock.patch.object(desktop_app, "start_child_process") as start_child_mock,
        mock.patch.object(desktop_app, "release_instance_lock"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
        mock.patch.object(desktop_app, "_write_launch_diagnostics"),
        pytest.raises(RuntimeError, match="stop after resolve"),
    ):
        desktop_app.launch_desktop_app(config)

    assert call_order == [
        "load",
        "validate",
        "load",
        "validate",
        "reclaim",
        "resolve",
        "ensure",
    ]
    start_child_mock.assert_not_called()


def test_launch_desktop_app_starts_children_saves_session_and_watches_browser() -> None:
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
        mock.patch.object(desktop_app, "load_session_state", return_value={}),
        mock.patch.object(
            desktop_app,
            "start_child_process",
            side_effect=[SimpleNamespace(pid=101), SimpleNamespace(pid=202)],
        ),
        mock.patch.object(desktop_app, "wait_for_url"),
        mock.patch.object(desktop_app, "_windows_create_kill_on_close_job", return_value=11),
        mock.patch.object(desktop_app, "_windows_close_desktop_job"),
        mock.patch.object(desktop_app, "is_baluffo_bridge_healthy", return_value=True),
        mock.patch.object(
            desktop_app,
            "launch_browser_for_url",
            return_value={
                "mode": "chromium-app",
                "browserName": "msedge",
                "browserPath": "C:/Edge/msedge.exe",
                "process": fake_browser_process,
            },
        ) as launch_browser_mock,
        mock.patch.object(desktop_app, "_windows_try_assign_pid_to_job") as assign_mock,
        mock.patch.object(desktop_app, "save_session_state") as save_mock,
        mock.patch.object(
            desktop_app, "watch_browser_session", return_value="heartbeat_timeout"
        ) as watch_mock,
        mock.patch.object(desktop_app, "write_success_marker"),
        mock.patch.object(desktop_app, "clear_session_state") as clear_mock,
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        desktop_app.launch_desktop_app(config)

    save_payload = save_mock.call_args.args[0]
    assert save_payload["appVersion"] == APP_VERSION
    assert save_payload["launchMode"] == "chromium-app"
    assert save_payload["browserPath"] == "C:/Edge/msedge.exe"
    assert save_payload["sitePid"] == 101
    assert save_payload["bridgePort"] == 8877
    assert save_payload["bridgePid"] == 202
    assert save_payload["desktopSessionId"]
    assert save_payload["desktopOwnerToken"]
    launch_browser_mock.assert_called_once()
    assert launch_browser_mock.call_args.kwargs["job_handle"] == 11
    assign_mock.assert_not_called()
    watch_mock.assert_called_once_with(
        data_dir,
        mock.ANY,
        bridge_port=8877,
        bridge_process=mock.ANY,
        browser_process=fake_browser_process,
        browser_pid=0,
        launch_accepted_elapsed_ms=mock.ANY,
        require_window=True,
        background_active_work_recovery=False,
        recovery_owner_token=mock.ANY,
    )
    clear_mock.assert_called_once()


def test_publish_success_marker_when_ready_async_writes_marker_after_startup_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = desktop_runtime_config()
    write_marker = mock.Mock()

    class ImmediateThread:
        def __init__(self, *, target, name, daemon) -> None:
            self._target = target
            self.name = name
            self.daemon = daemon

        def start(self) -> None:
            self._target()

    monkeypatch.setattr(
        desktop_app,
        "wait_for_desktop_startup_ready",
        lambda bridge_port, *, app_version, timeout_s: {"appVersion": APP_VERSION},
    )
    monkeypatch.setattr(desktop_app, "write_success_marker", write_marker)
    monkeypatch.setattr(desktop_app.threading, "Thread", ImmediateThread)

    desktop_app.publish_success_marker_when_ready_async(config, launcher_token="token-1")

    write_marker.assert_called_once()
    assert write_marker.call_args.kwargs == {
        "app_version": APP_VERSION,
        "bridge_port": 8877,
        "launcher_token": "token-1",
    }


def test_publish_success_marker_when_ready_async_records_classified_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("desktop-startup-timeout") as tmp:
        data_dir = Path(tmp) / "ship" / "data"
        config = desktop_runtime_config(
            ship_root=Path(tmp) / "ship",
            data_dir=data_dir,
        )
        trace_mock = mock.Mock()

        class ImmediateThread:
            def __init__(self, *, target, name, daemon) -> None:
                self._target = target
                self.name = name
                self.daemon = daemon

            def start(self) -> None:
                self._target()

        def raise_timeout(
            bridge_port: int, *, app_version: str, timeout_s: float
        ) -> dict[str, object]:
            raise desktop_app.DesktopStartupReadyTimeout(
                "startup_pending",
                "Baluffo bridge is running, but desktop startup did not finish in time.",
                payload={"startupReady": False},
            )

        monkeypatch.setattr(desktop_app, "wait_for_desktop_startup_ready", raise_timeout)
        monkeypatch.setattr(desktop_app.threading, "Thread", ImmediateThread)
        monkeypatch.setattr(desktop_app, "_append_startup_trace", trace_mock)

        desktop_app.publish_success_marker_when_ready_async(config, launcher_token="token-1")

        diagnostics_path = data_dir / "desktop-bridge-startup-timeout.txt"
        assert diagnostics_path.is_file()
        diagnostics_text = diagnostics_path.read_text(encoding="utf-8")
        assert "startup did not finish in time" in diagnostics_text
        assert desktop_app.build_open_url(config) in diagnostics_text
        trace_mock.assert_any_call(
            data_dir,
            "desktop_bridge_startup_timeout",
            reason="startup_pending",
            bridgePort=8877,
            url=desktop_app.build_open_url(config),
        )


def test_launch_desktop_app_defers_bridge_spawn_until_site_ready() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_runtime_config(
        data_dir=data_dir,
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


def test_launch_desktop_app_launches_browser_before_bridge_ready_diagnostic() -> None:
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
        mock.patch.object(desktop_app, "is_baluffo_bridge_healthy", return_value=False),
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
    event_names = [call.args[1] for call in trace_mock.call_args_list]
    window_launch_index = event_names.index("desktop_window_create_started")
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
        bridge_port=8877,
        bridge_process=mock.ANY,
        browser_process=None,
        browser_pid=0,
        launch_accepted_elapsed_ms=mock.ANY,
        require_window=False,
        background_active_work_recovery=False,
        recovery_owner_token=mock.ANY,
    )


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
    watch_mock.assert_called_once_with(
        data_dir,
        mock.ANY,
        bridge_port=18877,
        bridge_process=mock.ANY,
        browser_process=None,
        browser_pid=0,
        launch_accepted_elapsed_ms=mock.ANY,
        require_window=True,
        background_active_work_recovery=False,
        recovery_owner_token=mock.ANY,
    )
    assert any(call.args[1] == "desktop_runtime_port_retry" for call in trace_mock.call_args_list)


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
        bridge_port=8877,
        bridge_process=mock.ANY,
        browser_process=fake_browser_process,
        browser_pid=0,
        launch_accepted_elapsed_ms=mock.ANY,
        require_window=True,
        background_active_work_recovery=False,
        recovery_owner_token=mock.ANY,
    )


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
        bridge_port=8877,
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
        bridge_port=8877,
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
