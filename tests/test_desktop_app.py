import argparse
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src.app_version import APP_VERSION
from src.ship import desktop_app
from tests.helpers.temp_paths import workspace_tmpdir


def test_create_runtime_config_defaults_to_fixed_desktop_ports() -> None:
    root = Path("C:/tmp/baluffo-ship")
    args = argparse.Namespace(
        root=str(root),
        site_port=0,
        bridge_port=0,
        bridge_host="127.0.0.1",
        data_dir="",
        open_path="admin.html",
        title="",
        port=0,
        bind_host="127.0.0.1",
        child_mode="",
        desktop_runtime=False,
        startup_probe=False,
    )
    with mock.patch.object(desktop_app, "resolve_ship_root", return_value=root):
        config = desktop_app.create_runtime_config(args)

    assert config.ship_root == root
    assert config.site_port == desktop_app.DEFAULT_SITE_PORT
    assert config.bridge_port == desktop_app.DEFAULT_BRIDGE_PORT
    assert not config.site_port_explicit
    assert not config.bridge_port_explicit
    assert config.no_browser is False
    assert config.data_dir == root / "data"
    assert config.open_path == "admin.html"
    assert config.title == desktop_app.WINDOW_TITLE


def test_create_runtime_config_defaults_to_jobs_entry() -> None:
    root = Path("C:/tmp/baluffo-ship")
    args = argparse.Namespace(
        root=str(root),
        site_port=0,
        bridge_port=0,
        bridge_host="127.0.0.1",
        data_dir="",
        open_path="",
        title="",
        port=0,
        bind_host="127.0.0.1",
        child_mode="",
        desktop_runtime=False,
        startup_probe=False,
    )
    with mock.patch.object(desktop_app, "resolve_ship_root", return_value=root):
        config = desktop_app.create_runtime_config(args)

    assert config.open_path == "jobs.html"


def test_create_runtime_config_can_enable_test_no_browser_mode_from_env() -> None:
    root = Path("C:/tmp/baluffo-ship")
    args = argparse.Namespace(
        root=str(root),
        site_port=0,
        bridge_port=0,
        bridge_host="127.0.0.1",
        data_dir="",
        open_path="jobs.html",
        title="",
        port=0,
        bind_host="127.0.0.1",
        child_mode="",
        desktop_runtime=False,
        startup_probe=False,
    )
    with (
        mock.patch.object(desktop_app, "resolve_ship_root", return_value=root),
        mock.patch.dict(desktop_app.os.environ, {desktop_app.NO_BROWSER_ENV: "1"}, clear=False),
    ):
        config = desktop_app.create_runtime_config(args)

    assert config.no_browser is True


def test_build_open_url_marks_desktop_mode() -> None:
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=Path("C:/tmp/baluffo-ship/data"),
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
    )
    assert (
        desktop_app.build_open_url(config)
        == "http://127.0.0.1:8080/jobs.html?desktop=1&bridgePort=8877&bridgeHost=127.0.0.1"
    )


def test_build_open_url_marks_startup_probe_when_enabled() -> None:
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=Path("C:/tmp/baluffo-ship/data"),
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=True,
    )
    assert (
        desktop_app.build_open_url(config)
        == "http://127.0.0.1:8080/jobs.html?desktop=1&bridgePort=8877&bridgeHost=127.0.0.1&startupProbe=1"
    )


def test_resolve_runtime_ports_falls_back_to_free_ports_for_defaults() -> None:
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=Path("C:/tmp/baluffo-ship/data"),
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
    )
    availability = {
        ("127.0.0.1", 8080): False,
        ("127.0.0.1", 19080): True,
        ("127.0.0.1", 8877): False,
        ("127.0.0.1", 19877): True,
    }
    with (
        mock.patch.object(
            desktop_app,
            "_port_is_available",
            side_effect=lambda host, port: availability.get((str(host), int(port)), True),
        ),
        mock.patch.object(desktop_app, "choose_free_port", side_effect=[19080, 19877]),
    ):
        resolved = desktop_app.resolve_runtime_ports(config)

    assert resolved.site_port == 19080
    assert resolved.bridge_port == 19877


def test_resolve_runtime_ports_keeps_explicit_port_fail_fast() -> None:
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=Path("C:/tmp/baluffo-ship/data"),
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
        site_port_explicit=True,
    )

    with mock.patch.object(desktop_app, "_port_is_available", return_value=False):
        with pytest.raises(RuntimeError, match="site port 8080 is already in use"):
            desktop_app.resolve_runtime_ports(config)


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
        mock.patch.object(desktop_app, "resolve_registry_app_path", return_value=""),
    ):
        candidates = desktop_app.resolve_chromium_browser_candidates()

    assert [row["name"] for row in candidates] == ["chrome", "brave", "msedge"]


def test_resolve_chromium_browser_candidates_uses_registry_fallback() -> None:
    with (
        mock.patch.object(desktop_app.shutil, "which", return_value=""),
        mock.patch.object(
            desktop_app,
            "resolve_registry_app_path",
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


def test_launch_chromium_app_clears_cache_dirs_before_launch_when_requested(tmp_path: Path) -> None:
    profile_dir = tmp_path / "desktop-browser-profile"
    cache_dir = profile_dir / "Default" / "Cache"
    code_cache_dir = profile_dir / "Default" / "Code Cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    code_cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "data.bin").write_text("cached", encoding="utf-8")
    (code_cache_dir / "js.bin").write_text("cached", encoding="utf-8")

    fake_process = mock.Mock(spec=subprocess.Popen)
    with mock.patch.object(
        desktop_app.subprocess, "Popen", return_value=fake_process
    ) as popen_mock:
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


def test_launch_chromium_app_preserves_cache_dirs_by_default(tmp_path: Path) -> None:
    profile_dir = tmp_path / "desktop-browser-profile"
    cache_dir = profile_dir / "Default" / "Cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "data.bin").write_text("cached", encoding="utf-8")

    fake_process = mock.Mock(spec=subprocess.Popen)
    with mock.patch.object(
        desktop_app.subprocess, "Popen", return_value=fake_process
    ):
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


def test_get_valid_session_state_clears_stale_pid_immediately() -> None:
    with workspace_tmpdir("desktop-app") as tmp:
        root = Path(tmp) / "session-root"
        root.mkdir(parents=True, exist_ok=True)
        state_path = root / "desktop-session.json"
        state_path.write_text(
            json.dumps(
                {
                    "launcherPid": 4444,
                    "bridgePort": 8877,
                    "launcherToken": "token-a",
                    "launcherStartedAt": "2026-03-12T14:00:00+00:00",
                    "exePath": "C:/tmp/Baluffo.exe",
                    "url": "http://127.0.0.1:8080",
                }
            ),
            encoding="utf-8",
        )

        with (
            mock.patch.object(desktop_app, "resolve_session_state_path", return_value=state_path),
            mock.patch.object(desktop_app, "is_process_alive", return_value=False),
        ):
            state = desktop_app.get_valid_session_state()

        assert state == {}
        assert not (state_path.exists())


def test_get_valid_session_state_rejects_non_baluffo_health_identity() -> None:
    with workspace_tmpdir("desktop-app") as tmp:
        root = Path(tmp) / "session-root"
        root.mkdir(parents=True, exist_ok=True)
        state_path = root / "desktop-session.json"
        state_path.write_text(
            json.dumps(
                {
                    "launcherPid": 4444,
                    "bridgePort": 8877,
                    "launcherToken": "token-a",
                    "launcherStartedAt": "2026-03-12T14:00:00+00:00",
                    "url": "http://127.0.0.1:8080",
                    "exePath": "C:/tmp/Baluffo.exe",
                }
            ),
            encoding="utf-8",
        )

        with (
            mock.patch.object(desktop_app, "resolve_session_state_path", return_value=state_path),
            mock.patch.object(desktop_app, "_process_identity_matches", return_value=True),
            mock.patch.object(desktop_app, "is_baluffo_bridge_healthy", return_value=False),
        ):
            state = desktop_app.get_valid_session_state()

        assert state == {}
        assert not (state_path.exists())


def test_acquire_instance_lock_reclaims_stale_lock() -> None:
    with workspace_tmpdir("desktop-app") as tmp:
        root = Path(tmp) / "session-root"
        root.mkdir(parents=True, exist_ok=True)
        lock_path = root / "desktop-instance.lock"
        lock_path.write_text("not-json", encoding="utf-8")

        with (
            mock.patch.object(desktop_app, "resolve_instance_lock_path", return_value=lock_path),
            mock.patch.object(desktop_app, "_process_identity_matches", return_value=False),
        ):
            lock = desktop_app.acquire_instance_lock(timeout_s=0.5)

        assert lock is not None
        assert lock is not None
        desktop_app.release_instance_lock(lock)


def test_diagnose_instance_conflict_reclaims_stale_owner() -> None:
    with workspace_tmpdir("desktop-app") as tmp:
        root = Path(tmp) / "session-root"
        root.mkdir(parents=True, exist_ok=True)
        lock_path = root / "desktop-instance.lock"
        lock_path.write_text(
            json.dumps(
                {
                    "pid": 999,
                    "createdAt": "2026-03-12T14:00:00+00:00",
                    "launcherToken": "abc",
                    "exePath": "C:/stale/Baluffo.exe",
                    "sessionRoot": str(root),
                    "state": "launching",
                }
            ),
            encoding="utf-8",
        )
        session_path = root / "desktop-session.json"
        session_path.write_text(
            json.dumps({"launcherPid": 999, "bridgePort": 8877}), encoding="utf-8"
        )

        with (
            mock.patch.object(desktop_app, "resolve_instance_lock_path", return_value=lock_path),
            mock.patch.object(desktop_app, "resolve_session_state_path", return_value=session_path),
            mock.patch.object(desktop_app, "_process_identity_matches", return_value=False),
            mock.patch.object(desktop_app, "_append_startup_trace"),
        ):
            result = desktop_app.diagnose_instance_conflict(data_dir=root, timeout_s=0.5)

        assert result.get("action") == "reclaimed"
        assert not (lock_path.exists())
        assert not (session_path.exists())


def test_validate_session_state_rejects_token_mismatch() -> None:
    state = {
        "launcherPid": 4444,
        "bridgePort": 8877,
        "launcherToken": "token-old",
        "launcherStartedAt": "2026-03-12T14:00:00+00:00",
        "exePath": "C:/tmp/Baluffo.exe",
    }
    with (
        mock.patch.object(desktop_app, "_process_identity_matches", return_value=True),
        mock.patch.object(desktop_app, "is_baluffo_bridge_healthy", return_value=True),
    ):
        ok, reason = desktop_app.validate_session_state(state, expected_launcher_token="token-new")
    assert not (ok)
    assert reason == "launcher_token_mismatch"


def test_validate_session_state_requires_new_session_fields() -> None:
    state = {
        "launcherPid": 4444,
        "bridgePort": 8877,
        "exePath": "C:/tmp/Baluffo.exe",
    }
    ok, reason = desktop_app.validate_session_state(state)
    assert not (ok)
    assert reason == "missing_launcher_token"


def test_validate_session_state_rejects_non_desktop_bridge() -> None:
    state = {
        "launcherPid": 4444,
        "bridgePort": 8877,
        "launcherToken": "token-a",
        "launcherStartedAt": "2026-03-12T14:00:00+00:00",
        "exePath": "C:/tmp/Baluffo.exe",
    }
    with (
        mock.patch.object(desktop_app, "_process_identity_matches", return_value=True),
        mock.patch.object(
            desktop_app, "is_baluffo_bridge_healthy", return_value=False
        ) as health_mock,
    ):
        ok, reason = desktop_app.validate_session_state(state)
    assert not (ok)
    assert reason == "bridge_unhealthy"
    health_mock.assert_called_once_with(8877, require_desktop_mode=True)


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
            env={
                "BALUFFO_STARTUP_PROBE": "1",
                desktop_app.STARTUP_PROFILE_MODE_ENV: "cold",
            },
        )

    assert result["mode"] == "chromium-app"
    launch_mock.assert_called_once_with(
        "http://127.0.0.1:8080/jobs.html",
        "C:/Chrome/chrome.exe",
        mock.ANY,
        clear_profile_caches=True,
    )


def test_launch_browser_for_url_uses_warm_probe_cache_policy_for_chrome() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
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
            env={
                "BALUFFO_STARTUP_PROBE": "1",
                desktop_app.STARTUP_PROFILE_MODE_ENV: "warm",
            },
        )

    assert result["mode"] == "chromium-app"
    launch_mock.assert_called_once_with(
        "http://127.0.0.1:8080/jobs.html",
        "C:/Chrome/chrome.exe",
        mock.ANY,
        clear_profile_caches=False,
    )


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


def test_launch_browser_for_url_switches_to_default_browser_when_chromium_exits_with_error() -> (
    None
):
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 123
    fake_process.poll.return_value = 1
    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "chrome", "path": "C:/Chrome/chrome.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(desktop_app.webbrowser, "open", return_value=True) as open_mock,
        mock.patch.object(desktop_app, "terminate_process") as terminate_mock,
    ):
        result = desktop_app.launch_browser_for_url("http://127.0.0.1:8080/jobs.html")

    assert result["mode"] == "default-browser"
    assert isinstance(result["windowShownAtMonotonic"], float)
    terminate_mock.assert_called_once_with(fake_process)
    open_mock.assert_called_once()


def test_launch_browser_for_url_keeps_chromium_mode_when_launcher_exits_cleanly() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 456
    fake_process.poll.return_value = 0
    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "brave", "path": "C:/Brave/brave.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(
            desktop_app,
            "_wait_for_browser_reveal",
            return_value={
                "pid": 999,
                "title": "Baluffo",
                "observedAtMonotonic": 91.0,
                "event": "desktop_shell_window_shown",
                "observed": True,
                "handoffEvidence": "startup_metric",
            },
        ) as reveal_mock,
        mock.patch.object(desktop_app.webbrowser, "open", return_value=True) as open_mock,
        mock.patch.object(desktop_app, "terminate_process") as terminate_mock,
    ):
        result = desktop_app.launch_browser_for_url("http://127.0.0.1:8080/jobs.html")

    assert result["mode"] == "chromium-app"
    assert result["browserName"] == "brave"
    assert result["process"] is None
    assert result["windowShownAtMonotonic"] == 91.0
    assert result["windowShownObserved"] is True
    reveal_mock.assert_called_once_with(
        browser_pid=456,
        data_dir=None,
        launch_accepted_elapsed_ms=0,
        allow_title_fallback=True,
    )
    terminate_mock.assert_not_called()
    open_mock.assert_not_called()


def test_launch_browser_for_url_marks_reveal_inferred_when_visible_window_not_observed() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 654
    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "chrome", "path": "C:/Chrome/chrome.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(desktop_app, "wait_for_browser_process_ready", return_value=True),
        mock.patch.object(
            desktop_app,
            "_wait_for_browser_reveal",
            return_value={
                "observedAtMonotonic": 88.0,
                "event": "desktop_shell_window_shown_inferred",
                "observed": False,
                "inferredElapsedMsCap": 0,
                "handoffEvidence": "",
            },
        ) as reveal_mock,
    ):
        result = desktop_app.launch_browser_for_url("http://127.0.0.1:8080/jobs.html")

    assert result["mode"] == "chromium-app"
    assert result["windowShownObserved"] is False
    assert result["shellWindowEventEmitted"] is False
    reveal_mock.assert_called_once_with(
        browser_pid=654,
        data_dir=None,
        launch_accepted_elapsed_ms=0,
    )


def test_launch_browser_for_url_emits_trace_events_at_spawn_accept_and_reveal() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 700
    trace_events: list[tuple[str, dict[str, object]]] = []

    def _trace(event: str, _mono: float, fields: dict[str, object]) -> None:
        trace_events.append((event, fields))

    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "chrome", "path": "C:/Chrome/chrome.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(desktop_app, "wait_for_browser_process_ready", return_value=True),
        mock.patch.object(
            desktop_app,
            "_wait_for_browser_reveal",
            return_value={
                "pid": 700,
                "title": "Baluffo",
                "observedAtMonotonic": 45.0,
                "event": "desktop_shell_window_shown",
                "observed": True,
                "handoffEvidence": "",
            },
        ),
    ):
        result = desktop_app.launch_browser_for_url(
            "http://127.0.0.1:8080/jobs.html",
            trace_hook=_trace,
        )

    assert result["launchTraceEventsEmitted"] is True
    assert result["shellWindowEventEmitted"] is True
    assert [event for event, _fields in trace_events] == [
        "desktop_browser_process_spawn_started",
        "desktop_window_created",
        "desktop_browser_launch_accepted",
        "desktop_browser_launch_selected",
        "desktop_shell_window_shown",
    ]


def test_find_baluffo_visible_window_accepts_same_pid_chromium_window_without_baluffo_title() -> None:
    with mock.patch.object(
        desktop_app,
        "_enumerate_visible_desktop_windows",
        return_value=[
            {
                "hwnd": 100,
                "pid": 777,
                "title": "Jobs",
                "className": "Chrome_WidgetWin_1",
                "matchesTitle": False,
                "isChromiumClass": True,
            }
        ],
    ):
        result = desktop_app._find_baluffo_visible_window(
            browser_pid=777,
            allow_title_fallback=False,
        )

    assert result is not None
    assert result["pid"] == 777
    assert result["className"] == "Chrome_WidgetWin_1"


def test_wait_for_browser_reveal_accepts_handoff_window_after_startup_evidence() -> None:
    with (
        mock.patch.object(desktop_app, "_enumerate_visible_desktop_windows", return_value=[]),
        mock.patch.object(desktop_app, "_find_baluffo_visible_window", return_value=None),
        mock.patch.object(
            desktop_app,
            "earliest_startup_handoff_signal",
            return_value=("startup_metric", 1300),
        ),
        mock.patch.object(
            desktop_app,
            "_find_reveal_handoff_window",
            return_value={
                "hwnd": 55,
                "pid": 9001,
                "title": "",
                "className": "Chrome_WidgetWin_1",
                "matchesTitle": False,
                "isChromiumClass": True,
            },
        ),
        mock.patch.object(desktop_app.time, "monotonic", side_effect=[0.0, 0.0]),
    ):
        result = desktop_app._wait_for_browser_reveal(
            browser_pid=321,
            data_dir=Path("C:/tmp"),
            launch_accepted_elapsed_ms=1200,
        )

    assert result["observed"] is True
    assert result["event"] == "desktop_shell_window_shown"
    assert result["handoffEvidence"] == "startup_metric"
    assert result["pid"] == 9001


def test_wait_for_browser_reveal_caps_inferred_fallback_at_earliest_browser_evidence() -> None:
    with (
        mock.patch.object(desktop_app, "_enumerate_visible_desktop_windows", return_value=[]),
        mock.patch.object(desktop_app, "_find_baluffo_visible_window", return_value=None),
        mock.patch.object(
            desktop_app,
            "earliest_startup_handoff_signal",
            return_value=("startup_metric", 1900),
        ),
        mock.patch.object(desktop_app, "_find_reveal_handoff_window", return_value=None),
        mock.patch.object(desktop_app.time, "monotonic", side_effect=[0.0, 0.0, 2.0]),
        mock.patch.object(desktop_app.time, "sleep"),
    ):
        result = desktop_app._wait_for_browser_reveal(
            browser_pid=321,
            data_dir=Path("C:/tmp"),
            launch_accepted_elapsed_ms=1200,
        )

    assert result["observed"] is False
    assert result["event"] == "desktop_shell_window_shown_inferred"
    assert result["inferredElapsedMsCap"] == 1900


def test_watch_browser_session_uses_heartbeat_when_no_browser_process() -> None:
    with (
        mock.patch.object(desktop_app, "wait_for_browser_heartbeat", return_value=True),
        mock.patch.object(
            desktop_app, "latest_browser_heartbeat_ts", side_effect=[100.0, 100.0, 100.0]
        ),
        mock.patch.object(desktop_app, "bridge_last_activity_ts", return_value=0.0),
        mock.patch.object(desktop_app.time, "time", side_effect=[110.0, 120.0, 140.5]),
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
        mock.patch.object(desktop_app, "_is_baluffo_browser_window_open", return_value=True),
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=8877,
            browser_process=None,
            heartbeat_idle_timeout_s=30.0,
        )

    assert result == "heartbeat_timeout"


def test_watch_browser_session_prefers_bridge_exit_when_authoritative_process_is_available() -> (
    None
):
    bridge_process = mock.Mock(spec=subprocess.Popen)
    bridge_process.poll.side_effect = [None, 0]
    browser_process = mock.Mock(spec=subprocess.Popen)
    browser_process.poll.return_value = None

    with (
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=8877,
            bridge_process=bridge_process,
            browser_process=browser_process,
        )

    assert result == "bridge_exit"


def test_watch_browser_session_detects_window_close_in_detached_mode() -> None:
    with (
        mock.patch.object(
            desktop_app, "_is_baluffo_browser_window_open", return_value=False
        ) as window_mock,
        mock.patch.object(desktop_app, "latest_browser_heartbeat_ts", return_value=0.0),
        mock.patch.object(desktop_app, "bridge_last_activity_ts", return_value=0.0),
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=8877,
            browser_process=None,
            browser_pid=4040,
        )

    assert result == "window_closed"
    window_mock.assert_called_with(
        browser_pid=4040,
        allow_title_fallback=True,
    )


def test_watch_browser_session_keeps_detached_launcher_alive_when_heartbeat_exists() -> None:
    with (
        mock.patch.object(desktop_app, "_is_baluffo_browser_window_open", return_value=False),
        mock.patch.object(
            desktop_app, "latest_browser_heartbeat_ts", side_effect=[100.0, 100.0, 100.0]
        ),
        mock.patch.object(desktop_app, "bridge_last_activity_ts", return_value=0.0),
        mock.patch.object(desktop_app.time, "time", side_effect=[110.0, 120.0, 140.5]),
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=8877,
            browser_process=None,
            browser_pid=5050,
            heartbeat_idle_timeout_s=30.0,
        )

    assert result == "heartbeat_timeout"


def test_watch_browser_session_ignores_missing_window_in_no_browser_mode() -> None:
    with (
        mock.patch.object(
            desktop_app, "latest_browser_heartbeat_ts", side_effect=[100.0, 100.0, 100.0]
        ),
        mock.patch.object(desktop_app, "bridge_last_activity_ts", return_value=0.0),
        mock.patch.object(desktop_app.time, "time", side_effect=[110.0, 120.0, 140.5]),
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
        mock.patch.object(desktop_app, "_is_baluffo_browser_window_open") as window_mock,
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=8877,
            browser_process=None,
            heartbeat_idle_timeout_s=30.0,
            require_window=False,
        )

    assert result == "heartbeat_timeout"
    window_mock.assert_not_called()


def test_watch_browser_session_confirms_handoff_after_accepted_process_exit_when_signal_arrives() -> (
    None
):
    bridge_process = mock.Mock(spec=subprocess.Popen)
    bridge_process.poll.return_value = None
    browser_process = mock.Mock(spec=subprocess.Popen)
    browser_process.poll.return_value = 0

    with (
        mock.patch.object(
            desktop_app,
            "wait_for_startup_handoff_signal",
            return_value=("startup_metric", 950),
        ) as handoff_mock,
        mock.patch.object(
            desktop_app,
            "latest_browser_heartbeat_ts",
            side_effect=[0.0, 100.0, 100.0],
        ),
        mock.patch.object(desktop_app, "wait_for_browser_heartbeat", return_value=True),
        mock.patch.object(desktop_app, "bridge_last_activity_ts", return_value=0.0),
        mock.patch.object(desktop_app.time, "time", return_value=102.5),
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=8877,
            bridge_process=bridge_process,
            browser_process=browser_process,
            browser_pid=9090,
            launch_accepted_elapsed_ms=700,
            heartbeat_idle_timeout_s=1.0,
        )

    assert result == "heartbeat_timeout"
    handoff_mock.assert_called_once_with(
        Path("C:/tmp"),
        browser_pid=9090,
        min_elapsed_ms=700,
        timeout_s=desktop_app.STARTUP_HANDOFF_GRACE_TIMEOUT_S,
    )
    event_names = [call.args[1] for call in trace_mock.call_args_list]
    assert "desktop_browser_watchdog_handoff_candidate" in event_names
    assert "desktop_browser_watchdog_handoff_confirmed" in event_names
    assert "desktop_browser_watchdog_handoff" in event_names
    assert "desktop_browser_watchdog_handoff_failed" not in event_names


def test_watch_browser_session_returns_handoff_failed_when_signal_never_arrives() -> None:
    bridge_process = mock.Mock(spec=subprocess.Popen)
    bridge_process.poll.return_value = None
    browser_process = mock.Mock(spec=subprocess.Popen)
    browser_process.poll.return_value = 0

    with (
        mock.patch.object(
            desktop_app,
            "wait_for_startup_handoff_signal",
            return_value=(None, None),
        ) as handoff_mock,
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
            launch_accepted_elapsed_ms=720,
        )

    assert result == "browser_handoff_failed"
    handoff_mock.assert_called_once_with(
        Path("C:/tmp"),
        browser_pid=9091,
        min_elapsed_ms=720,
        timeout_s=desktop_app.STARTUP_HANDOFF_GRACE_TIMEOUT_S,
    )
    event_names = [call.args[1] for call in trace_mock.call_args_list]
    assert "desktop_browser_watchdog_handoff_candidate" in event_names
    assert "desktop_browser_watchdog_handoff_failed" in event_names
    assert "desktop_browser_watchdog_handoff_confirmed" not in event_names


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
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=Path("C:/tmp/baluffo-ship/data"),
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
    )
    session = {
        "launcherPid": 1234,
        "bridgePort": 8877,
        "url": "http://127.0.0.1:8080/jobs.html?desktop=1",
        "browserPath": "C:/Edge/msedge.exe",
    }

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
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        with pytest.raises(RuntimeError, match="Baluffo is already running"):
            desktop_app.launch_desktop_app(config)

    start_mock.assert_not_called()


def test_launch_desktop_app_starts_children_saves_session_and_watches_browser() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=data_dir,
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
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
        mock.patch.object(desktop_app, "is_baluffo_bridge_healthy", return_value=True),
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
    assert save_payload["bridgePort"] == 8877
    assert save_payload["desktopSessionId"]
    assert save_payload["desktopOwnerToken"]
    watch_mock.assert_called_once_with(
        data_dir,
        mock.ANY,
        bridge_port=8877,
        bridge_process=mock.ANY,
        browser_process=fake_browser_process,
        browser_pid=0,
        launch_accepted_elapsed_ms=mock.ANY,
        require_window=True,
    )
    clear_mock.assert_called_once()


def test_publish_success_marker_when_ready_async_writes_marker_after_startup_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=Path("C:/tmp/baluffo-ship/data"),
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
    )
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


def test_launch_desktop_app_defers_bridge_spawn_until_site_ready() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=data_dir,
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
    )
    call_log: list[str] = []

    def _start_child_process(*args: object, **kwargs: object) -> SimpleNamespace:
        command = args[0]
        child_mode = str(command[2]) if isinstance(command, list) and len(command) > 2 else ""
        if child_mode == "__child_site__":
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
                "windowShownAtMonotonic": 101.0,
            },
        ),
        mock.patch.object(desktop_app, "save_session_state"),
        mock.patch.object(
            desktop_app, "watch_browser_session", return_value="heartbeat_timeout"
        ),
        mock.patch.object(desktop_app, "write_success_marker"),
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        desktop_app.launch_desktop_app(config)

    assert call_log == ["spawn_site", "wait_for_url", "spawn_bridge"]
    event_names = [call.args[1] for call in trace_mock.call_args_list]
    assert "desktop_bridge_spawn_deferred_until_site_ready" in event_names
    assert event_names.index("desktop_site_ready") < event_names.index("desktop_bridge_spawned")


def test_launch_desktop_app_emits_window_created_before_shell_window_shown() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=data_dir,
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
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
            "wait_for_desktop_startup_ready",
            return_value={"appVersion": APP_VERSION},
        ),
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
        mock.patch.object(
            desktop_app, "watch_browser_session", return_value="heartbeat_timeout"
        ),
        mock.patch.object(desktop_app, "write_success_marker"),
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        desktop_app.launch_desktop_app(config)

    event_names = [call.args[1] for call in trace_mock.call_args_list]
    window_created_index = event_names.index("desktop_window_created")
    shell_shown_index = event_names.index("desktop_shell_window_shown")
    assert window_created_index < shell_shown_index
    assert "desktop_browser_launch_phase_diagnostics" in event_names


def test_launch_desktop_app_emits_inferred_shell_window_event_when_visibility_not_observed() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=data_dir,
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
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
            "wait_for_desktop_startup_ready",
            return_value={"appVersion": APP_VERSION},
        ),
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
        mock.patch.object(
            desktop_app, "watch_browser_session", return_value="heartbeat_timeout"
        ),
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
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=data_dir,
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
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
            "wait_for_desktop_startup_ready",
            return_value={"appVersion": APP_VERSION},
        ),
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
        mock.patch.object(
            desktop_app, "watch_browser_session", return_value="heartbeat_timeout"
        ),
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
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=data_dir,
        open_path="jobs.html",
        title="Baluffo",
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
            "wait_for_desktop_startup_ready",
            return_value={"appVersion": APP_VERSION},
        ),
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
        mock.patch.object(
            desktop_app, "watch_browser_session", return_value="heartbeat_timeout"
        ),
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
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=data_dir,
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
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
        mock.patch.object(desktop_app, "wait_for_baluffo_bridge"),
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
    )


def test_launch_desktop_app_spawns_update_helper_from_launcher_on_install_request() -> None:
    data_dir = Path("C:/tmp/baluffo-ship/data")
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=data_dir,
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
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
        mock.patch.object(desktop_app, "wait_for_baluffo_bridge"),
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
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=data_dir,
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
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
        mock.patch.object(desktop_app, "wait_for_baluffo_bridge"),
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
    )


def test_launch_desktop_app_fails_when_instance_lock_is_contended_and_session_exists() -> None:
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=Path("C:/tmp/baluffo-ship/data"),
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
    )
    session = {
        "launcherPid": 1234,
        "bridgePort": 8877,
        "url": "http://127.0.0.1:8080/jobs.html?desktop=1",
        "browserPath": "C:/Edge/msedge.exe",
    }

    with (
        mock.patch.object(desktop_app, "acquire_instance_lock", return_value=None),
        mock.patch.object(
            desktop_app,
            "diagnose_instance_conflict",
            return_value={"action": "active", "session": session},
        ),
        mock.patch.object(desktop_app, "resolve_runtime_ports", return_value=config),
        mock.patch.object(desktop_app, "start_child_process") as start_mock,
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        with pytest.raises(RuntimeError, match="Baluffo is already running"):
            desktop_app.launch_desktop_app(config)

    start_mock.assert_not_called()
