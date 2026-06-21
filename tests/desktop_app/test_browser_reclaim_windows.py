import subprocess
from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_app

from ._helpers import _patch_windows_compat_facade


@pytest.mark.windows
def test_windows_try_reclaim_stale_bridge_process_skips_when_owner_token_missing() -> None:
    with (
        _patch_windows_compat_facade(),
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


@pytest.mark.windows
def test_windows_try_reclaim_stale_bridge_process_returns_not_found_without_listener() -> None:
    with (
        _patch_windows_compat_facade(),
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


@pytest.mark.windows
def test_windows_try_reclaim_stale_bridge_process_kills_strong_listener() -> None:
    terminate_mock = mock.Mock(return_value={"terminated": True})

    with (
        _patch_windows_compat_facade(),
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


@pytest.mark.windows
def test_windows_try_reclaim_stale_bridge_process_accepts_listener_clear_after_forced_kill() -> (
    None
):
    terminate_mock = mock.Mock(return_value={"terminated": False})

    with (
        _patch_windows_compat_facade(),
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


@pytest.mark.windows
def test_windows_try_reclaim_stale_bridge_process_skips_when_listener_is_ambiguous() -> None:
    terminate_mock = mock.Mock()

    with (
        _patch_windows_compat_facade(),
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


@pytest.mark.windows
def test_windows_terminate_process_tree_details_waits_for_forced_taskkill_exit() -> None:
    run_mock = mock.Mock(return_value=subprocess.CompletedProcess(["taskkill"], 0))
    wait_mock = mock.Mock(return_value=True)
    with (
        _patch_windows_compat_facade(),
        mock.patch.object(desktop_app.subprocess, "run", run_mock),
        mock.patch.object(desktop_app, "_poll_process_exit_until_timeout", wait_mock),
        mock.patch.object(desktop_app, "is_process_alive", return_value=False),
    ):
        result = desktop_app._windows_terminate_process_tree_details_by_pid(323)

    assert result["terminated"] is True
    run_mock.assert_called_once()
    wait_mock.assert_called_once_with(323, timeout_s=15.0)


@pytest.mark.windows
def test_windows_try_reclaim_stale_site_process_kills_when_stored_pid_matches() -> None:
    terminate_mock = mock.Mock(return_value={"terminated": True})

    with (
        _patch_windows_compat_facade(),
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


@pytest.mark.windows
def test_windows_try_reclaim_stale_site_process_accepts_listener_clear_after_forced_kill() -> None:
    terminate_mock = mock.Mock(return_value={"terminated": False})

    with (
        _patch_windows_compat_facade(),
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


@pytest.mark.windows
def test_windows_try_reclaim_stale_site_process_requires_bridge_confirmation_without_pid() -> None:
    terminate_mock = mock.Mock()

    with (
        _patch_windows_compat_facade(),
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
            bridge_confirmed=False,
            data_dir=Path("C:/tmp/baluffo-ship/data"),
        )

    assert result["status"] == "skipped"
    assert result["reason"] == "bridge_not_confirmed"
    terminate_mock.assert_not_called()


@pytest.mark.windows
def test_windows_try_reclaim_stale_site_process_can_reclaim_without_pid_after_bridge_confirmation() -> (
    None
):
    terminate_mock = mock.Mock(return_value={"terminated": True})
    with (
        _patch_windows_compat_facade(),
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
