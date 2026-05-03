from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

from src import dev_admin_supervisor as supervisor
from tests.helpers.temp_paths import workspace_tmpdir


def test_main_honors_stop_command_from_sys_argv() -> None:
    with workspace_tmpdir("dev-admin-supervisor-main-stop") as tmp:
        data_dir = Path(tmp) / "data"
        with (
            mock.patch.object(
                supervisor.sys,
                "argv",
                ["dev_admin_supervisor.py", "stop", "--data-dir", str(data_dir)],
            ),
            mock.patch.object(
                supervisor, "stop_owned_session", return_value={"stopped": False, "killedPids": []}
            ) as stop_mock,
            mock.patch.object(supervisor, "run_supervised_admin_session") as run_mock,
        ):
            exit_code = supervisor.main()

        assert exit_code == 0
        stop_mock.assert_called_once_with(data_dir.resolve())
        run_mock.assert_not_called()


def test_run_supervised_admin_session_disables_bridge_owner_timeout() -> None:
    with workspace_tmpdir("dev-admin-supervisor-bridge-timeout") as tmp:
        data_dir = Path(tmp) / "data"
        config = supervisor.DevAdminConfig(
            root=Path(tmp),
            data_dir=data_dir,
            site_port=8080,
            bridge_port=8877,
            bridge_host="127.0.0.1",
            open_path="jobs.html",
            owner_idle_timeout_s=30.0,
            open_browser=False,
        )
        site_process = mock.Mock(spec=["pid", "poll"])
        site_process.pid = 1001
        site_process.poll.side_effect = [None, 0]
        bridge_process = mock.Mock(spec=["pid", "poll"])
        bridge_process.pid = 1002
        bridge_process.poll.side_effect = [None, 0]

        with (
            mock.patch.object(
                supervisor, "_spawn", side_effect=[site_process, bridge_process]
            ) as spawn_mock,
            mock.patch.object(supervisor, "wait_for_url"),
            mock.patch.object(supervisor, "save_session_state"),
            mock.patch.object(supervisor, "reclaim_previous_dev_session"),
            mock.patch.object(supervisor, "terminate_process"),
        ):
            exit_code = supervisor.run_supervised_admin_session(config)

        assert exit_code == 0
        bridge_command = spawn_mock.call_args_list[1].args[0]
        assert "--owner-idle-timeout-s" in bridge_command
        assert "0.0" in bridge_command


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


def test_stop_owned_session_skips_current_process_pid() -> None:
    with workspace_tmpdir("dev-admin-supervisor-stop-self") as tmp:
        data_dir = Path(tmp) / "data"
        supervisor.save_session_state(
            data_dir,
            {
                "supervisorPid": 11,
                "sitePid": 22,
                "bridgePid": 33,
            },
        )

        with (
            mock.patch.object(supervisor.os, "getpid", return_value=11),
            mock.patch.object(supervisor, "_terminate_pid") as terminate_pid,
        ):
            result = supervisor.stop_owned_session(data_dir)

        assert result == {"stopped": True, "killedPids": [33, 22]}
        assert [call.args[0] for call in terminate_pid.call_args_list] == [33, 22]


def test_stop_owned_session_clears_artifacts_even_when_a_pid_is_stubborn() -> None:
    with workspace_tmpdir("dev-admin-supervisor-stop-stubborn") as tmp:
        data_dir = Path(tmp) / "data"
        supervisor.save_session_state(
            data_dir,
            {
                "supervisorPid": 11,
                "sitePid": 22,
                "bridgePid": 33,
            },
        )
        task_state_path = data_dir / "admin-task-state.json"
        task_state_path.parent.mkdir(parents=True, exist_ok=True)
        task_state_path.write_text(
            json.dumps({"fetch": {"pid": 44, "taskType": "fetch"}}),
            encoding="utf-8",
        )

        with mock.patch.object(
            supervisor,
            "_terminate_pid",
            side_effect=[TimeoutError("stubborn"), None, None, None],
        ) as terminate_pid:
            result = supervisor.stop_owned_session(data_dir)

        assert result == {"stopped": True, "killedPids": [33, 22, 11, 44]}
        assert [call.args[0] for call in terminate_pid.call_args_list] == [33, 22, 11, 44]
        assert not (data_dir / "admin-dev-session.json").exists()
        assert not (data_dir / "admin-task-state.json").exists()
        reset_report = json.loads((data_dir / "jobs-fetch-report.json").read_text(encoding="utf-8"))
        assert str(reset_report.get("runId") or "") == ""


def test_terminate_pid_uses_bounded_taskkill_on_windows() -> None:
    with (
        mock.patch.object(supervisor.os, "name", "nt"),
        mock.patch.object(supervisor.subprocess, "run") as run_mock,
    ):
        supervisor._terminate_pid(456)

    run_mock.assert_called_once()
    assert run_mock.call_args.args[0] == ["taskkill", "/PID", "456", "/T", "/F"]
    assert run_mock.call_args.kwargs["check"] is False
    assert run_mock.call_args.kwargs["timeout"] == supervisor.STOP_PID_TERMINATION_TIMEOUT_S


def test_stop_owned_session_reclaims_task_state_pids_when_session_file_is_missing() -> None:
    with workspace_tmpdir("dev-admin-supervisor-task-state") as tmp:
        data_dir = Path(tmp) / "data"
        task_state_path = data_dir / "admin-task-state.json"
        task_state_path.parent.mkdir(parents=True, exist_ok=True)
        task_state_path.write_text(
            json.dumps(
                {
                    "fetch": {
                        "runId": "fetch_123",
                        "taskType": "fetch",
                        "pid": 44,
                        "script": "jobs_fetcher.py",
                        "status": "running",
                    },
                    "discovery": {
                        "runId": "discovery_456",
                        "taskType": "discovery",
                        "pid": 55,
                        "script": "jobs_discovery.py",
                        "status": "running",
                    },
                }
            ),
            encoding="utf-8",
        )

        with mock.patch.object(supervisor, "_terminate_pid") as terminate_pid:
            result = supervisor.stop_owned_session(data_dir)

        assert result == {"stopped": True, "killedPids": [44, 55]}
        assert [call.args[0] for call in terminate_pid.call_args_list] == [44, 55]
        assert not (data_dir / "admin-task-state.json").exists()
