import subprocess
from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_app


def test_resolve_chromium_browser_candidates_prefers_chrome_then_brave_then_edge() -> None:
    with (
        mock.patch.object(
            desktop_app.shutil,
            "which",
            side_effect=lambda name: {
                "msedge": "C:/Edge/msedge.exe",
                "msedge.exe": "C:/Edge/msedge.exe",
                "chrome": "C:/Chrome/chrome.exe",
                "chrome.exe": "C:/Chrome/chrome.exe",
                "brave": "C:/Brave/brave.exe",
                "brave.exe": "C:/Brave/brave.exe",
            }.get(name, ""),
        ),
        mock.patch.object(desktop_app, "_resolve_browser_from_registry_app_paths", return_value=""),
    ):
        candidates = desktop_app.resolve_chromium_browser_candidates()

    assert [row["name"] for row in candidates] == ["chrome", "brave", "msedge"]


def test_resolve_chromium_browser_candidates_uses_registry_fallback() -> None:
    with (
        mock.patch.object(desktop_app.shutil, "which", return_value=""),
        mock.patch.object(
            desktop_app,
            "_resolve_browser_from_registry_app_paths",
            side_effect=lambda name: (
                "C:/Users/me/AppData/Local/Google/Chrome/chrome.exe" if name == "chrome.exe" else ""
            ),
        ),
    ):
        candidates = desktop_app.resolve_chromium_browser_candidates()

    assert candidates[0]["name"] == "chrome"
    assert "AppData" in candidates[0]["path"]


def test_build_browser_launch_command_uses_app_mode_profile_and_new_window() -> None:
    command = desktop_app.build_browser_launch_command(
        "C:/Edge/msedge.exe",
        "http://127.0.0.1:8080/jobs.html?desktop=1",
        Path("C:/Users/me/AppData/Local/Baluffo/desktop-browser-profile"),
    )

    assert "--new-window" in command
    assert "--app=http://127.0.0.1:8080/jobs.html?desktop=1" in command
    assert "--no-first-run" in command
    assert "--disable-session-crashed-bubble" in command
    assert "--disable-application-cache" in command
    assert "--disk-cache-size=1" in command
    assert "--media-cache-size=1" in command
    assert any(part.startswith("--user-data-dir=") for part in command)


@pytest.mark.windows
def test_launch_chromium_app_clears_cache_dirs_before_launch_when_requested(tmp_path: Path) -> None:
    profile_dir = tmp_path / "desktop-browser-profile"
    cache_dir = profile_dir / "Default" / "Cache"
    code_cache_dir = profile_dir / "Default" / "Code Cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    code_cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "data.bin").write_text("cached", encoding="utf-8")
    (code_cache_dir / "js.bin").write_text("cached", encoding="utf-8")

    fake_process = mock.Mock(spec=subprocess.Popen)
    with (
        mock.patch.object(desktop_app.os, "name", "nt"),
        mock.patch.object(desktop_app.subprocess, "Popen", return_value=fake_process) as popen_mock,
    ):
        result = desktop_app.launch_chromium_app(
            "http://127.0.0.1:8080/jobs.html?desktop=1",
            "C:/Edge/msedge.exe",
            profile_dir,
            clear_profile_caches=True,
        )

    assert result is fake_process
    assert not cache_dir.exists()
    assert not code_cache_dir.exists()
    popen_mock.assert_called_once()
    assert popen_mock.call_args.kwargs["close_fds"] is True


def test_launch_chromium_app_preserves_cache_dirs_by_default(tmp_path: Path) -> None:
    profile_dir = tmp_path / "desktop-browser-profile"
    cache_dir = profile_dir / "Default" / "Cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "data.bin").write_text("cached", encoding="utf-8")

    fake_process = mock.Mock(spec=subprocess.Popen)
    with mock.patch.object(desktop_app.subprocess, "Popen", return_value=fake_process):
        result = desktop_app.launch_chromium_app(
            "http://127.0.0.1:8080/jobs.html?desktop=1",
            "C:/Edge/msedge.exe",
            profile_dir,
        )

    assert result is fake_process
    assert cache_dir.exists()


def test_should_clear_browser_profile_caches_only_for_cold_startup_probe() -> None:
    assert (
        desktop_app.should_clear_browser_profile_caches(
            {
                "BALUFFO_STARTUP_PROBE": "1",
                desktop_app.STARTUP_PROFILE_MODE_ENV: "cold",
            }
        )
        is True
    )
    assert (
        desktop_app.should_clear_browser_profile_caches(
            {
                "BALUFFO_STARTUP_PROBE": "1",
                desktop_app.STARTUP_PROFILE_MODE_ENV: "warm",
            }
        )
        is False
    )
    assert desktop_app.should_clear_browser_profile_caches({}) is False


def test_should_clear_browser_profile_caches_for_jobs_cold_start() -> None:
    assert (
        desktop_app.should_clear_browser_profile_caches(
            {
                desktop_app.JOBS_COLD_START_ENV: "1",
                desktop_app.STARTUP_PROFILE_MODE_ENV: "warm",
            }
        )
        is True
    )


def test_chromium_process_ready_timeout_prefers_shorter_wait_for_chrome_and_edge() -> None:
    assert desktop_app.chromium_process_ready_timeout_s({"name": "chrome"}) == pytest.approx(0.35)
    assert desktop_app.chromium_process_ready_timeout_s({"name": "msedge"}) == pytest.approx(0.35)
    assert desktop_app.chromium_process_ready_timeout_s({"name": "brave"}) == pytest.approx(0.75)
    assert desktop_app.chromium_process_ready_timeout_s({"name": "unknown"}) == pytest.approx(
        desktop_app.CHROMIUM_PROCESS_READY_TIMEOUT_S
    )


def test_chromium_process_ready_poll_interval_prefers_tighter_wait_for_chrome_and_edge() -> None:
    assert desktop_app.chromium_process_ready_poll_interval_s({"name": "chrome"}) == pytest.approx(
        0.01
    )
    assert desktop_app.chromium_process_ready_poll_interval_s({"name": "msedge"}) == pytest.approx(
        0.01
    )
    assert desktop_app.chromium_process_ready_poll_interval_s({"name": "brave"}) == pytest.approx(
        0.04
    )
    assert desktop_app.chromium_process_ready_poll_interval_s({"name": "unknown"}) == pytest.approx(
        desktop_app.CHROMIUM_PROCESS_READY_POLL_INTERVAL_S
    )


def test_latest_browser_heartbeat_ts_parses_iso_timestamps() -> None:
    rows = [
        {"event": "desktop_browser_heartbeat", "ts": "2026-03-11T21:08:41.365781+00:00"},
        {"event": "desktop_browser_heartbeat", "ts": "2026-03-11T21:08:51.365781+00:00"},
    ]
    with mock.patch.object(desktop_app, "read_startup_metrics", return_value=rows):
        value = desktop_app.latest_browser_heartbeat_ts(Path("C:/tmp"))

    assert value > 0.0
