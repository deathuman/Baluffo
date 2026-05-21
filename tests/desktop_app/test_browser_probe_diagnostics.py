import subprocess
from pathlib import Path
from unittest import mock

from src.ship import desktop_app


def test_resolve_chromium_browser_candidates_prefers_explicit_browser_path() -> None:
    with (
        mock.patch.object(desktop_app.shutil, "which", return_value=""),
        mock.patch.object(desktop_app, "resolve_registry_app_path", return_value=""),
    ):
        candidates = desktop_app.resolve_chromium_browser_candidates(
            {desktop_app.PREFERRED_BROWSER_PATH_ENV: "C:/Playwright/chrome.exe"}
        )

    assert candidates == [
        {
            "name": "chrome",
            "path": "C:/Playwright/chrome.exe",
            "source": "preferred-env",
        }
    ]


def test_launch_browser_for_url_returns_profile_hash_for_watchdog_diagnostics(
    tmp_path: Path,
) -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 777
    profile_dir = tmp_path / "profile"

    with (
        mock.patch.object(desktop_app, "resolve_browser_profile_dir", return_value=profile_dir),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(desktop_app, "wait_for_browser_process_ready", return_value=True),
        mock.patch.object(
            desktop_app,
            "_wait_for_browser_reveal",
            return_value={
                "pid": 777,
                "observed": True,
                "title": "Baluffo",
                "observedAtMonotonic": 44.0,
                "event": "desktop_shell_window_shown",
            },
        ),
    ):
        result = desktop_app.launch_browser_for_url(
            "http://127.0.0.1:8080/jobs.html",
            preferred_browser_path="C:/Playwright/chrome.exe",
        )

    assert result["browserPath"] == "C:/Playwright/chrome.exe"
    assert len(str(result["browserProfileDirHash"])) == 12


def test_watch_browser_session_reports_early_browser_exit_details() -> None:
    bridge_process = mock.Mock(spec=subprocess.Popen)
    bridge_process.poll.return_value = None
    browser_process = mock.Mock(spec=subprocess.Popen)
    browser_process.poll.return_value = 87

    with (
        mock.patch.object(
            desktop_app, "wait_for_startup_handoff_signal", return_value=(None, None)
        ),
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=8877,
            bridge_process=bridge_process,
            browser_process=browser_process,
            browser_pid=9091,
            browser_name="chrome",
            browser_path="C:/Playwright/chrome.exe",
            browser_profile_dir_hash="abc123def456",
        )

    assert result == "browser_handoff_failed"
    exit_call = next(
        call
        for call in trace_mock.call_args_list
        if call.args[1] == "desktop_browser_process_exited_waiting_for_bridge"
    )
    assert exit_call.kwargs["returnCode"] == 87
    assert exit_call.kwargs["browser"] == "chrome"
    assert exit_call.kwargs["browserPath"] == "C:/Playwright/chrome.exe"
    assert exit_call.kwargs["browserPid"] == 9091
    assert exit_call.kwargs["browserProfileDirHash"] == "abc123def456"
