import subprocess
from pathlib import Path
from unittest import mock

from src.ship import desktop_app


def test_build_browser_launch_command_includes_lean_flags_without_risky_process_flags() -> None:
    command = desktop_app.build_browser_launch_command(
        "C:/Edge/msedge.exe",
        "http://127.0.0.1:8080/jobs.html?desktop=1",
        Path("C:/Users/me/AppData/Local/Baluffo/desktop-browser-profile"),
    )

    for flag in desktop_app.LEAN_CHROMIUM_APP_FLAGS:
        assert flag in command
    assert "--single-process" not in command
    assert "--disable-gpu" not in command
    assert not any(part.startswith("--renderer-process-limit") for part in command)


def test_build_browser_launch_command_can_disable_lean_flags_from_env() -> None:
    command = desktop_app.build_browser_launch_command(
        "C:/Edge/msedge.exe",
        "http://127.0.0.1:8080/jobs.html?desktop=1",
        Path("C:/Users/me/AppData/Local/Baluffo/desktop-browser-profile"),
        env={desktop_app.DISABLE_LEAN_BROWSER_FLAGS_ENV: "1"},
    )

    assert "--app=http://127.0.0.1:8080/jobs.html?desktop=1" in command
    assert "--no-first-run" in command
    for flag in desktop_app.LEAN_CHROMIUM_APP_FLAGS:
        assert flag not in command


def test_launch_chromium_app_reads_lean_flag_escape_hatch_from_process_env(tmp_path: Path) -> None:
    profile_dir = tmp_path / "desktop-browser-profile"
    fake_process = mock.Mock(spec=subprocess.Popen)

    with (
        mock.patch.dict(
            desktop_app.os.environ,
            {desktop_app.DISABLE_LEAN_BROWSER_FLAGS_ENV: "1"},
            clear=False,
        ),
        mock.patch.object(desktop_app.subprocess, "Popen", return_value=fake_process) as popen_mock,
    ):
        result = desktop_app.launch_chromium_app(
            "http://127.0.0.1:8080/jobs.html?desktop=1",
            "C:/Edge/msedge.exe",
            profile_dir,
        )

    assert result is fake_process
    command = popen_mock.call_args.args[0]
    assert "--app=http://127.0.0.1:8080/jobs.html?desktop=1" in command
    for flag in desktop_app.LEAN_CHROMIUM_APP_FLAGS:
        assert flag not in command
