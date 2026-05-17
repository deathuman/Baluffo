import subprocess
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from src.app_version import APP_VERSION
from src.ship import desktop_app
from tests.desktop_app._helpers import desktop_runtime_config


def _patch_desktop_launch(config, *, launch_result, watch_result="heartbeat_timeout"):
    stack = ExitStack()
    stack.enter_context(mock.patch.object(desktop_app, "get_valid_session_state", return_value={}))
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
    launch_mock = stack.enter_context(
        mock.patch.object(desktop_app, "launch_browser_for_url", return_value=launch_result)
    )
    stack.enter_context(mock.patch.object(desktop_app, "_windows_try_assign_pid_to_job"))
    save_mock = stack.enter_context(mock.patch.object(desktop_app, "save_session_state"))
    watch_mock = stack.enter_context(
        mock.patch.object(desktop_app, "watch_browser_session", return_value=watch_result)
    )
    stack.enter_context(mock.patch.object(desktop_app, "write_success_marker"))
    stack.enter_context(mock.patch.object(desktop_app, "clear_session_state"))
    stack.enter_context(mock.patch.object(desktop_app, "terminate_process"))
    stack.enter_context(mock.patch.object(desktop_app, "_append_startup_trace"))
    return stack, launch_mock, save_mock, watch_mock


def test_launch_desktop_app_saves_attention_window_identity_and_session_root() -> None:
    config = desktop_runtime_config(data_dir=Path("C:/tmp/baluffo-ship/data"))
    browser_process = mock.Mock(spec=subprocess.Popen)
    launch_result = {
        "mode": "chromium-app",
        "browserName": "msedge",
        "browserPath": "C:/Edge/msedge.exe",
        "browserPid": 303,
        "process": browser_process,
        "windowPid": 303,
        "windowHwnd": 1001,
        "windowTitle": "Baluffo",
    }

    stack, launch_mock, save_mock, watch_mock = _patch_desktop_launch(
        config, launch_result=launch_result, watch_result="process_exit"
    )
    with stack:
        desktop_app.launch_desktop_app(config)

    payload = save_mock.call_args.args[0]
    assert payload["browserPid"] == 303
    assert payload["windowPid"] == 303
    assert payload["windowHwnd"] == 1001
    assert payload["windowTitle"] == "Baluffo"
    assert launch_mock.call_args.kwargs["env"]["BALUFFO_DESKTOP_SESSION_ROOT"]
    watch_mock.assert_called_once_with(
        config.data_dir,
        mock.ANY,
        bridge_port=8877,
        bridge_process=mock.ANY,
        browser_process=browser_process,
        browser_pid=303,
        launch_accepted_elapsed_ms=mock.ANY,
        require_window=True,
        background_active_work_recovery=False,
        recovery_owner_token=mock.ANY,
    )


def test_launch_desktop_app_refreshes_attention_identity_after_browser_relaunch() -> None:
    config = desktop_runtime_config(data_dir=Path("C:/tmp/baluffo-ship/data"))
    browser_process = mock.Mock(spec=subprocess.Popen)
    recovered_browser_process = mock.Mock(spec=subprocess.Popen)
    launch_result = {
        "mode": "chromium-app",
        "browserName": "msedge",
        "browserPath": "C:/Edge/msedge.exe",
        "browserPid": 303,
        "process": browser_process,
        "windowPid": 303,
        "windowHwnd": 1001,
        "windowTitle": "Baluffo",
    }
    recovered_launch_result = {
        "mode": "chromium-app",
        "browserName": "msedge",
        "browserPath": "C:/Edge/msedge.exe",
        "browserPid": 404,
        "process": recovered_browser_process,
        "windowPid": 404,
        "windowHwnd": 2002,
        "windowTitle": "Baluffo",
    }

    stack, _launch_mock, save_mock, watch_mock = _patch_desktop_launch(
        config, launch_result=launch_result
    )
    with stack:
        watch_mock.side_effect = ["heartbeat_timeout", "window_closed"]
        stack.enter_context(
            mock.patch.object(
                desktop_app,
                "get_baluffo_bridge_health",
                return_value={
                    "service": "baluffo-bridge",
                    "desktopMode": True,
                    "owner": {"token": "live"},
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
                "_attempt_active_work_browser_relaunch",
                return_value=recovered_launch_result,
            )
        )
        desktop_app.launch_desktop_app(config)

    first_payload = save_mock.call_args_list[0].args[0]
    second_payload = save_mock.call_args_list[1].args[0]
    assert first_payload["windowHwnd"] == 1001
    assert second_payload["browserPid"] == 404
    assert second_payload["windowPid"] == 404
    assert second_payload["windowHwnd"] == 2002
    assert second_payload["windowTitle"] == "Baluffo"


def test_launch_browser_for_url_returns_observed_window_hwnd() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 321
    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "msedge", "path": "C:/Edge/msedge.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(desktop_app, "wait_for_browser_process_ready", return_value=True),
        mock.patch.object(
            desktop_app,
            "_wait_for_browser_reveal",
            return_value={
                "hwnd": 1001,
                "pid": 321,
                "title": "Baluffo",
                "observedAtMonotonic": 78.0,
                "event": "desktop_shell_window_shown",
                "observed": True,
                "handoffEvidence": "",
            },
        ),
    ):
        result = desktop_app.launch_browser_for_url(
            "http://127.0.0.1:8080/jobs.html",
            env={"BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE": "1"},
        )

    assert result["windowHwnd"] == 1001
