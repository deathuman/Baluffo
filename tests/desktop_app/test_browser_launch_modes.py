import subprocess
from unittest import mock

import pytest

from src.ship import desktop_app


def test_launch_browser_for_url_falls_back_to_default_browser() -> None:
    with (
        mock.patch.object(desktop_app, "resolve_chromium_browser_candidates", return_value=[]),
        mock.patch.object(desktop_app.webbrowser, "open", return_value=True) as open_mock,
    ):
        result = desktop_app.launch_browser_for_url("http://127.0.0.1:8080/jobs.html")

    assert result["mode"] == "default-browser"
    open_mock.assert_called_once()


def test_launch_browser_for_url_skips_edge_app_mode_by_default() -> None:
    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "msedge", "path": "C:/Edge/msedge.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app") as launch_mock,
        mock.patch.object(desktop_app.webbrowser, "open", return_value=True) as open_mock,
    ):
        result = desktop_app.launch_browser_for_url("http://127.0.0.1:8080/jobs.html")

    assert result["mode"] == "default-browser"
    launch_mock.assert_not_called()
    open_mock.assert_called_once()


def test_launch_browser_for_url_can_opt_in_to_edge_app_mode() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 321
    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "msedge", "path": "C:/Edge/msedge.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(
            desktop_app, "wait_for_browser_process_ready", return_value=True
        ) as wait_mock,
        mock.patch.object(
            desktop_app,
            "_wait_for_browser_reveal",
            return_value={
                "pid": 321,
                "title": "Baluffo",
                "observedAtMonotonic": 77.0,
                "event": "desktop_shell_window_shown",
                "observed": True,
                "handoffEvidence": "",
            },
        ) as reveal_mock,
        mock.patch.object(desktop_app.webbrowser, "open", return_value=True) as open_mock,
    ):
        result = desktop_app.launch_browser_for_url(
            "http://127.0.0.1:8080/jobs.html",
            env={"BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE": "1"},
        )

    assert result["mode"] == "chromium-app"
    assert result["browserName"] == "msedge"
    assert result["windowShownAtMonotonic"] == 77.0
    assert result["windowShownObserved"] is True
    wait_mock.assert_called_once_with(fake_process, timeout_s=0.35, poll_interval_s=0.01)
    reveal_mock.assert_called_once_with(
        browser_pid=321,
        data_dir=None,
        launch_accepted_elapsed_ms=0,
    )
    open_mock.assert_not_called()


def test_launch_browser_for_url_uses_cold_probe_cache_policy_for_chrome() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    env = {"BALUFFO_STARTUP_PROBE": "1", desktop_app.STARTUP_PROFILE_MODE_ENV: "cold"}
    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "chrome", "path": "C:/Chrome/chrome.exe"}],
        ),
        mock.patch.object(
            desktop_app, "launch_chromium_app", return_value=fake_process
        ) as launch_mock,
        mock.patch.object(desktop_app, "wait_for_browser_process_ready", return_value=True),
    ):
        result = desktop_app.launch_browser_for_url(
            "http://127.0.0.1:8080/jobs.html",
            env=env,
        )

    assert result["mode"] == "chromium-app"
    launch_mock.assert_called_once_with(
        "http://127.0.0.1:8080/jobs.html",
        "C:/Chrome/chrome.exe",
        mock.ANY,
        clear_profile_caches=True,
        env=env,
    )


def test_launch_browser_for_url_clears_profile_caches_for_jobs_cold_start() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    env = {desktop_app.JOBS_COLD_START_ENV: "1"}
    trace_events: list[tuple[str, dict[str, object]]] = []
    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "chrome", "path": "C:/Chrome/chrome.exe"}],
        ),
        mock.patch.object(
            desktop_app, "launch_chromium_app", return_value=fake_process
        ) as launch_mock,
        mock.patch.object(desktop_app, "wait_for_browser_process_ready", return_value=True),
    ):
        result = desktop_app.launch_browser_for_url(
            "http://127.0.0.1:8080/jobs.html?jobsColdStart=1",
            env=env,
            trace_hook=lambda event, _event_mono, fields: trace_events.append((event, fields)),
        )

    assert result["mode"] == "chromium-app"
    launch_mock.assert_called_once_with(
        "http://127.0.0.1:8080/jobs.html?jobsColdStart=1",
        "C:/Chrome/chrome.exe",
        mock.ANY,
        clear_profile_caches=True,
        env=env,
    )
    assert trace_events[0][0] == "desktop_browser_process_spawn_started"
    assert trace_events[0][1]["clearProfileCaches"] is True


def test_launch_browser_for_url_uses_warm_probe_cache_policy_for_chrome() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    env = {"BALUFFO_STARTUP_PROBE": "1", desktop_app.STARTUP_PROFILE_MODE_ENV: "warm"}
    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "chrome", "path": "C:/Chrome/chrome.exe"}],
        ),
        mock.patch.object(
            desktop_app, "launch_chromium_app", return_value=fake_process
        ) as launch_mock,
        mock.patch.object(desktop_app, "wait_for_browser_process_ready", return_value=True),
    ):
        result = desktop_app.launch_browser_for_url(
            "http://127.0.0.1:8080/jobs.html",
            env=env,
        )

    assert result["mode"] == "chromium-app"
    launch_mock.assert_called_once_with(
        "http://127.0.0.1:8080/jobs.html",
        "C:/Chrome/chrome.exe",
        mock.ANY,
        clear_profile_caches=False,
        env=env,
    )


@pytest.mark.windows
def test_terminate_process_uses_taskkill_tree_on_windows() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 4321
    fake_process.poll.return_value = None

    with (
        mock.patch.object(desktop_app, "os") as os_mock,
        mock.patch.object(desktop_app.subprocess, "run") as run_mock,
    ):
        os_mock.name = "nt"
        desktop_app.terminate_process(fake_process)

    run_mock.assert_called_once()
    args = run_mock.call_args.args[0]
    assert args == ["taskkill", "/PID", "4321", "/T", "/F"]
    fake_process.wait.assert_called_once_with(timeout=5)
