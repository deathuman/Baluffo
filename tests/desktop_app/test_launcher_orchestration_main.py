"""Tests for desktop app launcher orchestration main and startup behavior."""

import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src.app_version import APP_VERSION
from src.ship import desktop_app
from tests.desktop_app._helpers import (
    desktop_runtime_config,
    isolate_desktop_startup_side_effects,
    launcher_session,
    stale_launcher_session,
)
from tests.helpers.ports import ADMIN_BRIDGE_TEST_PORT
from tests.helpers.temp_paths import workspace_tmpdir


@pytest.fixture(autouse=True)
def _isolate_desktop_startup_side_effects(request, monkeypatch: pytest.MonkeyPatch) -> None:
    isolate_desktop_startup_side_effects(request, monkeypatch)


def test_main_surfaces_native_error_without_installer_prompt() -> None:
    with (
        mock.patch.object(desktop_app, "create_runtime_config", return_value=object()),
        mock.patch(
            "src.ship.desktop_app.launcher.launch_desktop_app",
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


@pytest.mark.windows
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
    clear_mock.assert_called_once()


@pytest.mark.windows
def test_launch_desktop_app_does_not_attach_startup_probe_browser_to_job() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_runtime_config(
        data_dir=data_dir,
        startup_probe=True,
    )

    with (
        mock.patch.object(desktop_app, "get_valid_session_state", return_value={}),
        mock.patch.dict(
            desktop_app.os.environ,
            {desktop_app.STARTUP_PROFILE_MODE_ENV: "warm"},
        ),
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
                "browserName": "chrome",
                "browserPath": "C:/Chrome/chrome.exe",
                "process": None,
            },
        ) as launch_browser_mock,
        mock.patch.object(desktop_app, "save_session_state"),
        mock.patch.object(desktop_app, "watch_browser_session", return_value="heartbeat_timeout"),
        mock.patch.object(desktop_app, "read_startup_metrics", return_value=[]),
        mock.patch.object(desktop_app, "summarize_startup_metrics", return_value={}),
        mock.patch.object(desktop_app, "write_startup_summary"),
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        desktop_app.launch_desktop_app(config)

    assert launch_browser_mock.call_args.kwargs["job_handle"] is None
    launch_env = launch_browser_mock.call_args.kwargs["env"]
    assert launch_env["BALUFFO_STARTUP_PROBE"] == "1"
    assert launch_env["BALUFFO_DESKTOP_MODE"] == "1"
    assert launch_env[desktop_app.STARTUP_PROFILE_MODE_ENV] == "warm"


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


def test_publish_success_marker_when_ready_async_writes_legacy_marker_for_old_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("desktop-success-marker-legacy") as tmp:
        ship_root = Path(tmp) / "portable" / "ship"
        data_dir = Path(tmp) / "AppData" / "Roaming" / "Baluffo"
        config = desktop_runtime_config(ship_root=ship_root, data_dir=data_dir)
        legacy_paths = desktop_app.DesktopUpdatePaths.from_data_dir(
            ship_root / "data",
            ship_root=ship_root,
        )
        legacy_paths.install_plan_path.parent.mkdir(parents=True, exist_ok=True)
        legacy_paths.install_plan_path.write_text("{}", encoding="utf-8")

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
        monkeypatch.setattr(desktop_app.threading, "Thread", ImmediateThread)
        monkeypatch.setattr(desktop_app, "os", SimpleNamespace(name="nt"))
        monkeypatch.setattr(desktop_app.sys, "frozen", True, raising=False)

        desktop_app.publish_success_marker_when_ready_async(config, launcher_token="token-1")

        primary_paths = desktop_app.DesktopUpdatePaths.from_data_dir(
            data_dir,
            ship_root=ship_root,
        )
        assert primary_paths.success_marker_path.is_file()
        assert legacy_paths.success_marker_path.is_file()


def test_publish_success_marker_when_ready_async_skips_legacy_marker_without_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("desktop-success-marker-no-legacy") as tmp:
        ship_root = Path(tmp) / "portable" / "ship"
        data_dir = Path(tmp) / "AppData" / "Roaming" / "Baluffo"
        config = desktop_runtime_config(ship_root=ship_root, data_dir=data_dir)
        legacy_paths = desktop_app.DesktopUpdatePaths.from_data_dir(
            ship_root / "data",
            ship_root=ship_root,
        )

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
        monkeypatch.setattr(desktop_app.threading, "Thread", ImmediateThread)
        monkeypatch.setattr(desktop_app, "os", SimpleNamespace(name="nt"))
        monkeypatch.setattr(desktop_app.sys, "frozen", True, raising=False)

        desktop_app.publish_success_marker_when_ready_async(config, launcher_token="token-1")

        primary_paths = desktop_app.DesktopUpdatePaths.from_data_dir(
            data_dir,
            ship_root=ship_root,
        )
        assert primary_paths.success_marker_path.is_file()
        assert not legacy_paths.success_marker_path.exists()


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
