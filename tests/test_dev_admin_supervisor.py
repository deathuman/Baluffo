from __future__ import annotations

from pathlib import Path
from unittest import mock

from src import dev_admin_supervisor as supervisor
from tests.helpers.temp_paths import workspace_tmpdir


def test_build_bridge_command_includes_owner_metadata() -> None:
    config = supervisor.DevAdminConfig(
        root=Path("C:/repo"),
        data_dir=Path("C:/repo/data"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        open_path="jobs.html",
        owner_idle_timeout_s=30.0,
        open_browser=True,
    )

    command = supervisor.build_bridge_command(config, owner_token="owner-123")

    assert "--owner-mode" in command
    assert "dev-supervisor" in command
    assert "--owner-token" in command
    assert "owner-123" in command
    assert "--started-by" in command
    assert "dev_admin_supervisor" in command


def test_stop_owned_session_targets_only_recorded_pids() -> None:
    with workspace_tmpdir("dev-admin-supervisor-stop") as tmp:
        data_dir = Path(tmp) / "data"
        supervisor.save_session_state(
            data_dir,
            {
                "supervisorPid": 11,
                "sitePid": 22,
                "bridgePid": 33,
            },
        )

        with mock.patch.object(supervisor, "_terminate_pid") as terminate_pid:
            result = supervisor.stop_owned_session(data_dir)

        assert result == {"stopped": True, "killedPids": [33, 22, 11]}
        assert [call.args[0] for call in terminate_pid.call_args_list] == [33, 22, 11]


def test_run_supervised_admin_session_terminates_owned_children_on_exit() -> None:
    with workspace_tmpdir("dev-admin-supervisor-run") as tmp:
        data_dir = Path(tmp) / "data"
        config = supervisor.DevAdminConfig(
            root=Path(tmp),
            data_dir=data_dir,
            site_port=8080,
            bridge_port=8877,
            bridge_host="127.0.0.1",
            open_path="jobs.html",
            owner_idle_timeout_s=30.0,
            open_browser=True,
        )
        site_process = mock.Mock(spec=["pid", "poll"])
        site_process.pid = 1001
        bridge_process = mock.Mock(spec=["pid", "poll"])
        bridge_process.pid = 1002
        browser_process = mock.Mock(spec=["pid", "poll"])
        browser_process.pid = 1003

        with (
            mock.patch.object(
                supervisor, "_spawn", side_effect=[site_process, bridge_process]
            ) as spawn_mock,
            mock.patch.object(supervisor, "wait_for_url"),
            mock.patch.object(supervisor, "launch_browser_for_url") as launch_browser_mock,
            mock.patch.object(supervisor, "watch_browser_session") as watch_browser_session_mock,
            mock.patch.object(
                supervisor, "wait_for_local_browser_exit"
            ) as wait_for_browser_exit_mock,
            mock.patch.object(supervisor, "terminate_process") as terminate_process_mock,
        ):
            launch_browser_mock.return_value = {
                "windowShownAtMonotonic": 5.0,
                "process": browser_process,
            }
            exit_code = supervisor.run_supervised_admin_session(config)

        assert exit_code == 0
        assert spawn_mock.call_count == 2
        wait_for_browser_exit_mock.assert_called_once_with(browser_process)
        watch_browser_session_mock.assert_not_called()
        assert [call.args[0] for call in terminate_process_mock.call_args_list] == [
            browser_process,
            bridge_process,
            site_process,
        ]


def test_run_supervised_admin_session_falls_back_to_watcher_when_browser_detaches() -> None:
    with workspace_tmpdir("dev-admin-supervisor-detached") as tmp:
        data_dir = Path(tmp) / "data"
        config = supervisor.DevAdminConfig(
            root=Path(tmp),
            data_dir=data_dir,
            site_port=8080,
            bridge_port=8877,
            bridge_host="127.0.0.1",
            open_path="jobs.html",
            owner_idle_timeout_s=30.0,
            open_browser=True,
        )
        site_process = mock.Mock(spec=["pid", "poll"])
        site_process.pid = 1001
        bridge_process = mock.Mock(spec=["pid", "poll"])
        bridge_process.pid = 1002

        with (
            mock.patch.object(supervisor, "_spawn", side_effect=[site_process, bridge_process]),
            mock.patch.object(supervisor, "wait_for_url"),
            mock.patch.object(supervisor, "launch_browser_for_url") as launch_browser_mock,
            mock.patch.object(
                supervisor, "watch_browser_session", return_value="heartbeat_timeout"
            ) as watch_browser_session_mock,
            mock.patch.object(
                supervisor, "wait_for_local_browser_exit"
            ) as wait_for_browser_exit_mock,
            mock.patch.object(supervisor, "terminate_process"),
        ):
            launch_browser_mock.return_value = {
                "windowShownAtMonotonic": 5.0,
                "process": None,
            }
            exit_code = supervisor.run_supervised_admin_session(config)

        assert exit_code == 0
        wait_for_browser_exit_mock.assert_not_called()
        watch_browser_session_mock.assert_called_once()
