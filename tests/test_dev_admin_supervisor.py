from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

from src import dev_admin_supervisor as supervisor
from tests.helpers.ports import ADMIN_BRIDGE_TEST_PORT
from tests.helpers.temp_paths import workspace_tmpdir


def test_build_bridge_command_includes_owner_metadata() -> None:
    config = supervisor.DevAdminConfig(
        root=Path("C:/repo"),
        data_dir=Path("C:/repo/data"),
        site_port=8080,
        bridge_port=ADMIN_BRIDGE_TEST_PORT,
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


def test_startup_reclaims_task_state_even_without_session_file() -> None:
    with workspace_tmpdir("dev-admin-supervisor-ensure-cleanup") as tmp:
        data_dir = Path(tmp) / "data"
        task_state_path = data_dir / "admin-task-state.json"
        task_state_path.parent.mkdir(parents=True, exist_ok=True)
        task_state_path.write_text(
            json.dumps(
                {
                    "fetch": {
                        "runId": "fetch_123",
                        "taskType": "fetch",
                        "pid": 77,
                        "script": "jobs_fetcher.py",
                        "status": "running",
                    }
                }
            ),
            encoding="utf-8",
        )

        with mock.patch.object(supervisor, "reclaim_previous_dev_session") as reclaim_mock:
            supervisor._ensure_previous_owned_session_stopped(data_dir)

        reclaim_mock.assert_not_called()


def test_startup_resets_stale_fetch_report_even_without_session_or_task_state() -> None:
    with workspace_tmpdir("dev-admin-supervisor-reset-report") as tmp:
        data_dir = Path(tmp) / "data"
        fetch_report_path = data_dir / "jobs-fetch-report.json"
        fetch_report_path.parent.mkdir(parents=True, exist_ok=True)
        fetch_report_path.write_text(
            json.dumps(
                {
                    "runId": "fetch_999",
                    "startedAt": "2026-03-28T10:00:00+00:00",
                    "finishedAt": "",
                    "summary": {"outputCount": 12, "failedSources": 2, "sourceCount": 4},
                    "taskProgress": {
                        "active": True,
                        "phaseKey": "executing_sources",
                        "phaseLabel": "Executing sources",
                        "mode": "determinate",
                        "ratio": 0.5,
                        "counts": {
                            "sourceCount": 4,
                            "resolvedSources": 2,
                            "outputCount": 12,
                            "failedSources": 2,
                            "excludedSources": 0,
                        },
                    },
                    "sources": [{"name": "stale", "status": "running"}],
                    "outputs": {"report": str(fetch_report_path)},
                }
            ),
            encoding="utf-8",
        )

        with mock.patch.object(supervisor, "reclaim_previous_dev_session") as reclaim_mock:
            supervisor._ensure_previous_owned_session_stopped(data_dir)

        reclaim_mock.assert_not_called()
        reset_report = json.loads(fetch_report_path.read_text(encoding="utf-8"))
        assert str(reset_report.get("runId") or "") == ""
        assert bool((reset_report.get("taskProgress") or {}).get("active")) is False


def test_run_supervised_admin_session_terminates_owned_children_on_exit() -> None:
    with workspace_tmpdir("dev-admin-supervisor-run") as tmp:
        data_dir = Path(tmp) / "data"
        config = supervisor.DevAdminConfig(
            root=Path(tmp),
            data_dir=data_dir,
            site_port=8080,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
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
        assert [call.args[0] for call in terminate_process_mock.call_args_list] == [
            browser_process,
            bridge_process,
            site_process,
        ]


def test_run_supervised_admin_session_keeps_children_alive_when_browser_detaches() -> None:
    with workspace_tmpdir("dev-admin-supervisor-detached") as tmp:
        data_dir = Path(tmp) / "data"
        config = supervisor.DevAdminConfig(
            root=Path(tmp),
            data_dir=data_dir,
            site_port=8080,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            bridge_host="127.0.0.1",
            open_path="jobs.html",
            owner_idle_timeout_s=30.0,
            open_browser=True,
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
            mock.patch.object(supervisor, "launch_browser_for_url") as launch_browser_mock,
            mock.patch.object(
                supervisor, "wait_for_local_browser_exit"
            ) as wait_for_browser_exit_mock,
            mock.patch.object(supervisor, "terminate_process") as terminate_process_mock,
        ):
            launch_browser_mock.return_value = {
                "windowShownAtMonotonic": 5.0,
                "process": None,
            }
            exit_code = supervisor.run_supervised_admin_session(config)

        assert exit_code == 0
        assert spawn_mock.call_count == 2
        wait_for_browser_exit_mock.assert_not_called()
        launch_browser_mock.assert_called_once()
        assert [call.args[0] for call in terminate_process_mock.call_args_list] == [
            bridge_process,
            site_process,
        ]
