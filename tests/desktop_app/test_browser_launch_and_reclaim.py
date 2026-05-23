import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src.ship import desktop_app
from tests.helpers.temp_paths import workspace_tmpdir

from ._helpers import _patch_windows_desktop_app


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

        def _fake_reclaim_stale_instance_artifacts(*, data_dir, stale_state, env=None):
            assert data_dir == root
            assert stale_state == {"launcherPid": 999, "bridgePort": 8877}
            assert env is None
            lock_path.unlink()
            session_path.unlink()
            return {"blocked": False}

        with (
            mock.patch.object(desktop_app, "resolve_instance_lock_path", return_value=lock_path),
            mock.patch.object(desktop_app, "resolve_session_state_path", return_value=session_path),
            mock.patch.object(desktop_app, "_process_identity_matches", return_value=False),
            mock.patch.object(
                desktop_app,
                "_reclaim_stale_instance_artifacts",
                side_effect=_fake_reclaim_stale_instance_artifacts,
            ) as reclaim_mock,
            mock.patch.object(desktop_app, "_append_startup_trace"),
        ):
            result = desktop_app.diagnose_instance_conflict(data_dir=root, timeout_s=0.5)

        assert result.get("action") == "reclaimed"
        reclaim_mock.assert_called_once()
        assert not (lock_path.exists())
        assert not (session_path.exists())


def test_diagnose_instance_conflict_blocks_when_stale_runtime_cleanup_fails() -> None:
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

        with (
            mock.patch.object(desktop_app, "resolve_instance_lock_path", return_value=lock_path),
            mock.patch.object(desktop_app, "_process_identity_matches", return_value=False),
            mock.patch.object(
                desktop_app,
                "_reclaim_stale_instance_artifacts",
                return_value={
                    "blocked": True,
                    "reason": "stale_bridge_cleanup_failed",
                    "target": "bridge",
                },
            ),
            mock.patch.object(desktop_app, "_append_startup_trace"),
        ):
            result = desktop_app.diagnose_instance_conflict(data_dir=root, timeout_s=0.5)

    assert result["action"] == "blocked"
    assert result["reason"] == "stale_bridge_cleanup_failed"
    assert result["target"] == "bridge"


def test_windows_try_reclaim_stale_bridge_process_skips_when_owner_token_missing() -> None:
    with (
        mock.patch.object(desktop_app.os, "name", "nt"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        result = desktop_app._windows_try_reclaim_stale_bridge_process(
            {
                "bridgePort": 8877,
                "bridgePid": 202,
                "exePath": "C:/tmp/Baluffo.exe",
            },
            data_dir=Path("C:/tmp/baluffo-ship/data"),
        )

    assert result["status"] == "skipped"
    assert result["reason"] == "missing_desktop_owner_token"


def test_windows_try_reclaim_stale_bridge_process_returns_not_found_without_listener() -> None:
    with (
        mock.patch.object(desktop_app.os, "name", "nt"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
        mock.patch.object(desktop_app, "_pids_listening_on_tcp_port_windows", return_value=set()),
    ):
        result = desktop_app._windows_try_reclaim_stale_bridge_process(
            {
                "bridgePort": 8877,
                "bridgePid": 202,
                "desktopOwnerToken": "owner-token",
                "exePath": "C:/tmp/Baluffo.exe",
            },
            data_dir=Path("C:/tmp/baluffo-ship/data"),
        )

    assert result["status"] == "not_found"
    assert result["reason"] == "no_listener_on_expected_port"


def test_windows_try_reclaim_stale_bridge_process_kills_strong_listener() -> None:
    terminate_mock = mock.Mock(return_value={"terminated": True})

    with (
        mock.patch.object(desktop_app.os, "name", "nt"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
        mock.patch.object(
            desktop_app,
            "_pids_listening_on_tcp_port_windows",
            side_effect=[{202}, set()],
        ),
        mock.patch.object(desktop_app, "is_process_alive", return_value=True),
        mock.patch.object(
            desktop_app,
            "get_baluffo_bridge_health",
            return_value={
                "service": "baluffo-bridge",
                "desktopMode": True,
                "owner": {"token": "owner-token"},
            },
        ),
        mock.patch.object(desktop_app, "_windows_process_image_matches", return_value=True),
        mock.patch.object(
            desktop_app,
            "_windows_terminate_process_tree_details_by_pid",
            terminate_mock,
        ),
    ):
        result = desktop_app._windows_try_reclaim_stale_bridge_process(
            {
                "bridgePort": 8877,
                "bridgePid": 202,
                "desktopOwnerToken": "owner-token",
                "exePath": "C:/tmp/Baluffo.exe",
            },
            data_dir=Path("C:/tmp/baluffo-ship/data"),
        )

    assert result["status"] == "killed"
    assert result["confirmed"] is True
    terminate_mock.assert_called_once_with(202)


def test_windows_try_reclaim_stale_bridge_process_accepts_listener_clear_after_forced_kill() -> (
    None
):
    terminate_mock = mock.Mock(return_value={"terminated": False})

    with (
        mock.patch.object(desktop_app.os, "name", "nt"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
        mock.patch.object(
            desktop_app,
            "_pids_listening_on_tcp_port_windows",
            side_effect=[{202}, set()],
        ),
        mock.patch.object(desktop_app, "is_process_alive", return_value=True),
        mock.patch.object(
            desktop_app,
            "get_baluffo_bridge_health",
            return_value={
                "service": "baluffo-bridge",
                "desktopMode": True,
                "owner": {"token": "owner-token"},
            },
        ),
        mock.patch.object(desktop_app, "_windows_process_image_matches", return_value=True),
        mock.patch.object(
            desktop_app,
            "_windows_terminate_process_tree_details_by_pid",
            terminate_mock,
        ),
    ):
        result = desktop_app._windows_try_reclaim_stale_bridge_process(
            {
                "bridgePort": 8877,
                "bridgePid": 202,
                "desktopOwnerToken": "owner-token",
                "exePath": "C:/tmp/Baluffo.exe",
            },
            data_dir=Path("C:/tmp/baluffo-ship/data"),
        )

    assert result["status"] == "killed"
    assert result["confirmed"] is True
    terminate_mock.assert_called_once_with(202)


def test_windows_try_reclaim_stale_bridge_process_skips_when_listener_is_ambiguous() -> None:
    terminate_mock = mock.Mock()

    with (
        mock.patch.object(desktop_app.os, "name", "nt"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
        mock.patch.object(
            desktop_app,
            "_pids_listening_on_tcp_port_windows",
            return_value={202, 303},
        ),
        mock.patch.object(
            desktop_app,
            "_windows_terminate_process_tree_details_by_pid",
            terminate_mock,
        ),
    ):
        result = desktop_app._windows_try_reclaim_stale_bridge_process(
            {
                "bridgePort": 8877,
                "bridgePid": 202,
                "desktopOwnerToken": "owner-token",
                "exePath": "C:/tmp/Baluffo.exe",
            },
            data_dir=Path("C:/tmp/baluffo-ship/data"),
        )

    assert result["status"] == "skipped"
    assert result["reason"] == "ambiguous_bridge_listener"
    terminate_mock.assert_not_called()


def test_windows_terminate_process_tree_details_waits_for_forced_taskkill_exit() -> None:
    run_mock = mock.Mock(return_value=subprocess.CompletedProcess(["taskkill"], 0))
    wait_mock = mock.Mock(return_value=True)
    with (
        mock.patch.object(desktop_app.os, "name", "nt"),
        mock.patch.object(desktop_app.subprocess, "run", run_mock),
        mock.patch.object(desktop_app, "_wait_for_process_exit_pid", wait_mock),
        mock.patch.object(desktop_app, "is_process_alive", return_value=False),
    ):
        result = desktop_app._windows_terminate_process_tree_details_by_pid(323)

    assert result["terminated"] is True
    run_mock.assert_called_once()
    wait_mock.assert_called_once_with(323, timeout_s=15.0)


def test_windows_try_reclaim_stale_site_process_kills_when_stored_pid_matches() -> None:
    terminate_mock = mock.Mock(return_value={"terminated": True})

    with (
        mock.patch.object(desktop_app.os, "name", "nt"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
        mock.patch.object(
            desktop_app,
            "_pids_listening_on_tcp_port_windows",
            side_effect=[{101}, set()],
        ),
        mock.patch.object(desktop_app, "is_process_alive", return_value=True),
        mock.patch.object(desktop_app, "_windows_process_image_matches", return_value=True),
        mock.patch.object(
            desktop_app,
            "_windows_terminate_process_tree_details_by_pid",
            terminate_mock,
        ),
    ):
        result = desktop_app._windows_try_reclaim_stale_site_process(
            {
                "sitePort": 8080,
                "sitePid": 101,
                "exePath": "C:/tmp/Baluffo.exe",
            },
            bridge_confirmed=False,
            data_dir=Path("C:/tmp/baluffo-ship/data"),
        )

    assert result["status"] == "killed"
    assert result["confirmed"] is True
    terminate_mock.assert_called_once_with(101)


def test_windows_try_reclaim_stale_site_process_accepts_listener_clear_after_forced_kill() -> None:
    terminate_mock = mock.Mock(return_value={"terminated": False})

    with (
        mock.patch.object(desktop_app.os, "name", "nt"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
        mock.patch.object(
            desktop_app,
            "_pids_listening_on_tcp_port_windows",
            side_effect=[{101}, set()],
        ),
        mock.patch.object(desktop_app, "is_process_alive", return_value=True),
        mock.patch.object(desktop_app, "_windows_process_image_matches", return_value=True),
        mock.patch.object(
            desktop_app,
            "_windows_terminate_process_tree_details_by_pid",
            terminate_mock,
        ),
    ):
        result = desktop_app._windows_try_reclaim_stale_site_process(
            {
                "sitePort": 8080,
                "sitePid": 101,
                "exePath": "C:/tmp/Baluffo.exe",
            },
            bridge_confirmed=False,
            data_dir=Path("C:/tmp/baluffo-ship/data"),
        )

    assert result["status"] == "killed"
    assert result["confirmed"] is True
    terminate_mock.assert_called_once_with(101)


def test_windows_try_reclaim_stale_site_process_requires_bridge_confirmation_without_pid() -> None:
    terminate_mock = mock.Mock()

    with (
        mock.patch.object(desktop_app.os, "name", "nt"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
        mock.patch.object(
            desktop_app,
            "_pids_listening_on_tcp_port_windows",
            side_effect=[{101}, set()],
        ),
        mock.patch.object(desktop_app, "is_process_alive", return_value=True),
        mock.patch.object(desktop_app, "_windows_process_image_matches", return_value=True),
        mock.patch.object(
            desktop_app,
            "_windows_terminate_process_tree_details_by_pid",
            terminate_mock,
        ),
    ):
        result = desktop_app._windows_try_reclaim_stale_site_process(
            {
                "sitePort": 8080,
                "exePath": "C:/tmp/Baluffo.exe",
            },
            bridge_confirmed=False,
            data_dir=Path("C:/tmp/baluffo-ship/data"),
        )

    assert result["status"] == "skipped"
    assert result["reason"] == "bridge_not_confirmed"
    terminate_mock.assert_not_called()


def test_windows_try_reclaim_stale_site_process_can_reclaim_without_pid_after_bridge_confirmation() -> (
    None
):
    terminate_mock = mock.Mock(return_value={"terminated": True})
    with (
        mock.patch.object(desktop_app.os, "name", "nt"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
        mock.patch.object(
            desktop_app,
            "_pids_listening_on_tcp_port_windows",
            side_effect=[{101}, set()],
        ),
        mock.patch.object(desktop_app, "_windows_process_image_matches", return_value=True),
        mock.patch.object(
            desktop_app,
            "_windows_terminate_process_tree_details_by_pid",
            terminate_mock,
        ),
    ):
        result = desktop_app._windows_try_reclaim_stale_site_process(
            {
                "sitePort": 8080,
                "exePath": "C:/tmp/Baluffo.exe",
            },
            bridge_confirmed=True,
            data_dir=Path("C:/tmp/baluffo-ship/data"),
        )

    assert result["status"] == "killed"
    assert result["confirmed"] is True
    terminate_mock.assert_called_once_with(101)


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


def test_launch_browser_for_url_clears_profile_caches_for_jobs_cold_start() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
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
            env={desktop_app.JOBS_COLD_START_ENV: "1"},
            trace_hook=lambda event, _event_mono, fields: trace_events.append((event, fields)),
        )

    assert result["mode"] == "chromium-app"
    launch_mock.assert_called_once_with(
        "http://127.0.0.1:8080/jobs.html?jobsColdStart=1",
        "C:/Chrome/chrome.exe",
        mock.ANY,
        clear_profile_caches=True,
    )
    assert trace_events[0][0] == "desktop_browser_process_spawn_started"
    assert trace_events[0][1]["clearProfileCaches"] is True


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


def test_launch_browser_for_url_treats_clean_exit_after_reveal_as_detached() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 654
    fake_process.poll.return_value = 0
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
                "inferredElapsedMsCap": 900,
                "handoffEvidence": "startup_metric",
            },
        ),
        mock.patch.object(desktop_app, "_windows_try_assign_pid_to_job") as assign_mock,
    ):
        result = desktop_app.launch_browser_for_url(
            "http://127.0.0.1:8080/jobs.html",
            job_handle=11,
        )

    assert result["mode"] == "chromium-app"
    assert result["process"] is None
    assert result["browserPid"] == 654
    assert result["windowShownObserved"] is False
    assert result["revealHandoffEvidence"] == "startup_metric"
    assign_mock.assert_called_once_with(11, 654)


def test_launch_browser_for_url_emits_trace_events_at_spawn_accept_and_reveal() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 700
    trace_events: list[tuple[str, float, dict[str, object]]] = []

    def _trace(event: str, _mono: float, fields: dict[str, object]) -> None:
        trace_events.append((event, _mono, fields))

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
    assert [event for event, _mono, _fields in trace_events] == [
        "desktop_browser_process_spawn_started",
        "desktop_window_created",
        "desktop_browser_launch_accepted",
        "desktop_browser_launch_selected",
        "desktop_shell_window_shown",
    ]
    selected_event = next(
        item for item in trace_events if item[0] == "desktop_browser_launch_selected"
    )
    shell_event = next(item for item in trace_events if item[0] == "desktop_shell_window_shown")
    assert selected_event[1] == shell_event[1]


def test_launch_browser_for_url_attaches_browser_job_before_readiness_polling() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 701
    call_order: list[str] = []
    trace_events: list[str] = []

    def _trace(event: str, _mono: float, _fields: dict[str, object]) -> None:
        trace_events.append(event)

    def _attach(_job_handle: int, _pid: int) -> None:
        call_order.append("attach")

    def _wait(*args: object, **kwargs: object) -> bool:
        call_order.append("wait")
        return True

    def _reveal(**kwargs: object) -> dict[str, object]:
        call_order.append("reveal")
        return {
            "pid": 701,
            "title": "Baluffo",
            "observedAtMonotonic": 46.0,
            "event": "desktop_shell_window_shown",
            "observed": True,
            "handoffEvidence": "",
        }

    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "chrome", "path": "C:/Chrome/chrome.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(
            desktop_app,
            "_windows_try_assign_pid_to_job",
            side_effect=_attach,
        ) as assign_mock,
        mock.patch.object(
            desktop_app,
            "wait_for_browser_process_ready",
            side_effect=_wait,
        ) as wait_mock,
        mock.patch.object(
            desktop_app,
            "_wait_for_browser_reveal",
            side_effect=_reveal,
        ) as reveal_mock,
    ):
        result = desktop_app.launch_browser_for_url(
            "http://127.0.0.1:8080/jobs.html",
            job_handle=11,
            trace_hook=_trace,
        )

    assert result["mode"] == "chromium-app"
    assign_mock.assert_called_once_with(11, 701)
    wait_mock.assert_called_once()
    reveal_mock.assert_called_once()
    assert call_order == ["attach", "wait", "reveal"]
    assert trace_events[:2] == [
        "desktop_browser_process_spawn_started",
        "desktop_browser_job_attached",
    ]


def test_launch_browser_for_url_stops_before_readiness_when_job_attach_fails() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 702
    trace_events: list[str] = []

    def _trace(event: str, _mono: float, _fields: dict[str, object]) -> None:
        trace_events.append(event)

    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "chrome", "path": "C:/Chrome/chrome.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(
            desktop_app,
            "_windows_try_assign_pid_to_job",
            side_effect=OSError("browser attach failed"),
        ) as assign_mock,
        mock.patch.object(desktop_app, "wait_for_browser_process_ready") as wait_mock,
        mock.patch.object(desktop_app, "_wait_for_browser_reveal") as reveal_mock,
        mock.patch.object(desktop_app, "terminate_process") as terminate_mock,
    ):
        with pytest.raises(OSError, match="browser attach failed"):
            desktop_app.launch_browser_for_url(
                "http://127.0.0.1:8080/jobs.html",
                job_handle=11,
                trace_hook=_trace,
            )

    assign_mock.assert_called_once_with(11, 702)
    terminate_mock.assert_called_once_with(fake_process)
    wait_mock.assert_not_called()
    reveal_mock.assert_not_called()
    assert trace_events == [
        "desktop_browser_process_spawn_started",
        "desktop_browser_job_attach_failed",
    ]


def test_windows_try_assign_pid_to_job_raises_when_open_process_fails() -> None:
    kernel32 = SimpleNamespace(
        GetLastError=mock.Mock(return_value=5),
        OpenProcess=mock.Mock(return_value=0),
    )

    with _patch_windows_desktop_app(kernel32):
        with pytest.raises(
            OSError,
            match="OpenProcess failed while attaching pid=123 to desktop job: Access is denied.",
        ):
            desktop_app._windows_try_assign_pid_to_job(11, 123)


def test_windows_try_assign_pid_to_job_raises_when_assign_process_fails() -> None:
    kernel32 = SimpleNamespace(
        GetLastError=mock.Mock(return_value=5),
        OpenProcess=mock.Mock(return_value=99),
        AssignProcessToJobObject=mock.Mock(return_value=0),
        CloseHandle=mock.Mock(),
    )

    with _patch_windows_desktop_app(kernel32):
        with pytest.raises(
            OSError,
            match=(
                "AssignProcessToJobObject failed while attaching pid=123 to desktop job: "
                "Access is denied."
            ),
        ):
            desktop_app._windows_try_assign_pid_to_job(11, 123)

    kernel32.CloseHandle.assert_called_once_with(99)


def test_start_child_process_terminates_child_when_job_attach_fails() -> None:
    fake_process = SimpleNamespace(pid=321)

    with (
        mock.patch.object(desktop_app.os, "name", "nt"),
        mock.patch.object(desktop_app.subprocess, "Popen", return_value=fake_process) as popen_mock,
        mock.patch.object(
            desktop_app,
            "_windows_try_assign_pid_to_job",
            side_effect=OSError("attach failed"),
        ),
        mock.patch.object(desktop_app, "terminate_process") as terminate_mock,
    ):
        with pytest.raises(OSError, match="attach failed"):
            desktop_app.start_child_process(["python", "-V"], job_handle=11)

    terminate_mock.assert_called_once_with(fake_process)
    assert popen_mock.call_args.kwargs["close_fds"] is True


def test_windows_create_kill_on_close_job_marks_handle_non_inheritable() -> None:
    kernel32 = mock.Mock()
    kernel32.CreateJobObjectW.return_value = 77
    kernel32.SetHandleInformation.return_value = 1
    kernel32.SetInformationJobObject.return_value = 1

    with _patch_windows_desktop_app(kernel32):
        handle = desktop_app._windows_create_kill_on_close_job()

    assert handle == 77
    kernel32.SetHandleInformation.assert_called_once_with(77, 0x00000001, 0)
    kernel32.SetInformationJobObject.assert_called_once()


def test_is_process_alive_returns_false_for_signaled_windows_process_handle() -> None:
    kernel32 = SimpleNamespace(
        OpenProcess=mock.Mock(return_value=55),
        WaitForSingleObject=mock.Mock(return_value=0),
        GetExitCodeProcess=mock.Mock(return_value=1),
        CloseHandle=mock.Mock(),
    )

    with _patch_windows_desktop_app(kernel32):
        assert desktop_app.is_process_alive(123) is False

    kernel32.GetExitCodeProcess.assert_not_called()
    kernel32.CloseHandle.assert_called_once_with(55)


def test_is_process_alive_returns_true_for_running_windows_process_handle() -> None:
    def _get_exit_code(_handle: int, exit_code_ptr: object) -> int:
        exit_code_ptr._obj.value = 259
        return 1

    kernel32 = SimpleNamespace(
        OpenProcess=mock.Mock(return_value=55),
        WaitForSingleObject=mock.Mock(return_value=0x00000102),
        GetExitCodeProcess=mock.Mock(side_effect=_get_exit_code),
        CloseHandle=mock.Mock(),
    )

    with _patch_windows_desktop_app(kernel32):
        assert desktop_app.is_process_alive(123) is True

    kernel32.GetExitCodeProcess.assert_called_once()
    kernel32.CloseHandle.assert_called_once_with(55)


def test_find_baluffo_visible_window_accepts_same_pid_chromium_window_without_baluffo_title() -> (
    None
):
    with (
        mock.patch.object(desktop_app.os, "name", "nt"),
        mock.patch.object(
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
        ),
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
