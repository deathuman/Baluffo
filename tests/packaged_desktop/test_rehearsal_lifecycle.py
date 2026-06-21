"""Packaged desktop rehearsal tests for desktop lifecycle."""

from ._rehearsal_shared import (
    Path,
    json,
    mock,
    pytest,
    smoke,
    subprocess,
    workspace_tmpdir,
)

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


@pytest.mark.windows
def test_run_packaged_desktop_lifecycle_rehearsal_passes_when_both_phases_complete() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        artifacts_dir = root / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        false_process = mock.Mock(spec=subprocess.Popen)
        false_process.pid = 222
        close_process = mock.Mock(spec=subprocess.Popen)
        close_process.pid = 333
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        close_stdout_handle = mock.Mock()
        close_stderr_handle = mock.Mock()
        false_metrics = [
            {"event": "desktop_window_created"},
            {"event": "desktop_browser_launch_selected", "fields": {"mode": "no-browser"}},
            {"event": "desktop_shell_window_shown", "fields": {"observed": False}},
        ]
        close_metrics = [
            {"event": "desktop_browser_process_spawn_started"},
            {"event": "desktop_browser_job_attached", "fields": {"pid": 444}},
            {"event": "desktop_browser_launch_accepted"},
            {"event": "desktop_browser_launch_selected", "fields": {"mode": "chromium-app"}},
        ]

        with (
            mock.patch.object(smoke.sys, "platform", "win32"),
            mock.patch.object(
                smoke,
                "packaged_runtime_env_overrides",
                return_value={"LOCALAPPDATA": str(root / "localappdata")},
            ),
            mock.patch.object(
                smoke,
                "_select_packaged_browser_job_browser",
                return_value=(
                    {"browserName": "chrome", "browserPath": "C:/Chrome/chrome.exe"},
                    {smoke.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV: "C:/Chrome/chrome.exe"},
                ),
            ),
            mock.patch.object(
                smoke,
                "choose_free_port",
                side_effect=[8080, 8877, 8081, 8878, 8879],
            ),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                side_effect=[
                    (false_process, stdout_handle, stderr_handle),
                    (close_process, close_stdout_handle, close_stderr_handle),
                ],
            ) as launch_mock,
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime_with_port_pivot",
                side_effect=[
                    {
                        "actualSitePort": 8080,
                        "actualBridgePort": 8877,
                        "portRetryObserved": False,
                        "startupMetrics": false_metrics,
                    },
                    {
                        "actualSitePort": 8081,
                        "actualBridgePort": 8878,
                        "portRetryObserved": False,
                        "startupMetrics": close_metrics,
                    },
                ],
            ),
            mock.patch.object(smoke.desktop_app_mod, "is_process_alive", return_value=True),
            mock.patch.object(
                smoke,
                "_run_desktop_lifecycle_node_probe",
                return_value={
                    "reportPath": str(artifacts_dir / "desktop-lifecycle-node-report.json"),
                    "stdout": str(artifacts_dir / "desktop-lifecycle-node.stdout.log"),
                    "stderr": str(artifacts_dir / "desktop-lifecycle-node.stderr.log"),
                    "scenarios": [],
                },
            ),
            mock.patch.object(smoke, "_wait_for_launcher_exit"),
            mock.patch.object(smoke, "_wait_for_pid_exit"),
            mock.patch.object(smoke, "_wait_for_desktop_ports_released"),
            mock.patch.object(
                smoke,
                "_run_desktop_lifecycle_close_node_probe",
                return_value={
                    "reportPath": str(artifacts_dir / "desktop-lifecycle-close-node-report.json"),
                    "stdout": str(artifacts_dir / "desktop-lifecycle-close-node.stdout.log"),
                    "stderr": str(artifacts_dir / "desktop-lifecycle-close-node.stderr.log"),
                    "scenarios": [],
                },
            ) as close_node_probe_mock,
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
            mock.patch.object(smoke, "clear_packaged_desktop_session_state"),
        ):
            payload = smoke.run_packaged_desktop_lifecycle_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=5.0,
            )

    assert payload["status"] == "passed"
    assert payload["slug"] == "packaged-desktop-lifecycle-rehearsal"
    assert payload["details"]["falseIdleOwnerActivityAdvanced"] is True
    assert payload["details"]["closeCleanupDesktopPortsReleased"] is True
    assert launch_mock.call_args_list[0].kwargs["owner_idle_timeout_s"] == 10.0
    assert launch_mock.call_args_list[0].kwargs["open_path"] == "saved.html"
    assert launch_mock.call_args_list[0].kwargs["env"]["BALUFFO_DESKTOP_NO_BROWSER"] == "1"
    assert launch_mock.call_args_list[0].kwargs["env"]["BALUFFO_SYNC_DISABLE"] == "1"
    assert launch_mock.call_args_list[1].kwargs["open_path"] == "saved.html"
    assert int(launch_mock.call_args_list[1].kwargs["env"]["BALUFFO_PACKAGED_SMOKE_CDP_PORT"]) > 0
    assert launch_mock.call_args_list[1].kwargs["env"]["BALUFFO_PACKAGED_SMOKE_RUNTIME"] == "1"
    assert launch_mock.call_args_list[1].kwargs["env"]["BALUFFO_SYNC_DISABLE"] == "1"
    assert "owner_idle_timeout_s" not in launch_mock.call_args_list[1].kwargs
    close_node_probe_mock.assert_called_once()
    assert close_node_probe_mock.call_args.kwargs["browser_pid"] == 444
    assert payload["details"]["closeCleanupTargetMs"] == 5000


def test_run_packaged_smoke_can_run_desktop_lifecycle_rehearsal_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--desktop-lifecycle-rehearsal",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "run_packaged_desktop_lifecycle_rehearsal",
                return_value={
                    "name": "Packaged desktop lifecycle rehearsal",
                    "slug": "packaged-desktop-lifecycle-rehearsal",
                    "status": "passed",
                    "durationMs": 1200,
                    "error": "",
                    "details": {
                        "falseIdleRuntimeStdout": str(artifacts_dir / "false-runtime.stdout.log"),
                        "falseIdleNodeReport": str(artifacts_dir / "false-node-report.json"),
                        "closeCleanupRuntimeStdout": str(
                            artifacts_dir / "close-runtime.stdout.log"
                        ),
                        "closeCleanupStartupMetrics": str(
                            artifacts_dir / "close.startup-metrics.json"
                        ),
                    },
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)

        assert payload["ok"] is True
        assert payload["scenarios"][0]["slug"] == "packaged-desktop-lifecycle-rehearsal"
        assert payload["artifacts"]["desktopLifecycleFalseIdleRuntimeStdout"] == str(
            artifacts_dir / "false-runtime.stdout.log"
        )
        assert payload["artifacts"]["desktopLifecycleFalseIdleNodeReport"] == str(
            artifacts_dir / "false-node-report.json"
        )
        assert payload["artifacts"]["desktopLifecycleCloseStartupMetrics"] == str(
            artifacts_dir / "close.startup-metrics.json"
        )
        rehearsal_mock.assert_called_once()
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        assert saved["ok"] is True


def test_run_packaged_active_task_close_rehearsal_passes_without_relaunch() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        artifacts_dir = root / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        runtime_process = mock.Mock(spec=subprocess.Popen)
        runtime_process.pid = 444
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        startup_metrics = [
            {"event": "desktop_browser_process_spawn_started"},
            {"event": "desktop_browser_job_attached", "fields": {"pid": 555}},
            {"event": "desktop_browser_launch_accepted"},
            {"event": "desktop_browser_launch_selected", "fields": {"mode": "chromium-app"}},
        ]
        final_metrics = [
            {
                "event": "desktop_confirmed_active_work_shutdown_requested",
                "payload": {"sessionId": "session-1"},
            },
            {"event": "desktop_window_closed"},
        ]

        with (
            mock.patch.object(smoke.sys, "platform", "win32"),
            mock.patch.object(
                smoke,
                "packaged_runtime_env_overrides",
                return_value={
                    "LOCALAPPDATA": str(root / "localappdata"),
                    "BALUFFO_PACKAGED_SMOKE_RUNTIME": "1",
                    "BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_MODE": "controlled-heartbeat-success",
                },
            ),
            mock.patch.object(smoke, "choose_free_port", side_effect=[8080, 8877, 9222]),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                return_value=(runtime_process, stdout_handle, stderr_handle),
            ) as launch_mock,
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime_with_port_pivot",
                return_value={
                    "actualSitePort": 8080,
                    "actualBridgePort": 8877,
                    "portRetryObserved": False,
                    "startupMetrics": startup_metrics,
                },
            ),
            mock.patch.object(smoke.desktop_app_mod, "is_process_alive", return_value=True),
            mock.patch.object(
                smoke,
                "_run_active_task_close_node_probe",
                return_value={
                    "reportPath": str(artifacts_dir / "active-task-close-node-report.json"),
                    "stdout": str(artifacts_dir / "active-task-close-node.stdout.log"),
                    "stderr": str(artifacts_dir / "active-task-close-node.stderr.log"),
                    "scenarios": [],
                },
            ) as node_probe_mock,
            mock.patch.object(smoke, "_wait_for_launcher_exit"),
            mock.patch.object(smoke, "_wait_for_pid_exit"),
            mock.patch.object(smoke, "_wait_for_desktop_ports_released"),
            mock.patch.object(
                smoke.desktop_app_mod, "read_startup_metrics", return_value=final_metrics
            ),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
            mock.patch.object(smoke, "clear_packaged_desktop_session_state"),
        ):
            payload = smoke.run_packaged_active_task_close_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=5.0,
            )

    assert payload["status"] == "passed"
    assert payload["slug"] == "packaged-active-task-close-rehearsal"
    assert payload["details"]["browserRelaunchAttempted"] is False
    assert payload["details"]["fatalPathEntered"] is False
    assert launch_mock.call_args.kwargs["open_path"] == "jobs.html"
    assert launch_mock.call_args.kwargs["env"]["BALUFFO_PACKAGED_SMOKE_CDP_PORT"] == "9222"
    assert launch_mock.call_args.kwargs["env"]["BALUFFO_SYNC_DISABLE"] == "1"
    node_probe_mock.assert_called_once()


def test_run_packaged_smoke_can_run_active_task_close_rehearsal_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--active-task-close-rehearsal",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "run_packaged_active_task_close_rehearsal",
                return_value={
                    "name": "Packaged active-task close rehearsal",
                    "slug": "packaged-active-task-close-rehearsal",
                    "status": "passed",
                    "durationMs": 1200,
                    "error": "",
                    "details": {
                        "runtimeStdout": str(artifacts_dir / "active-runtime.stdout.log"),
                        "startupMetrics": str(artifacts_dir / "active.startup-metrics.json"),
                        "nodeReport": str(artifacts_dir / "active-node-report.json"),
                    },
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)

        assert payload["ok"] is True
        assert payload["scenarios"][0]["slug"] == "packaged-active-task-close-rehearsal"
        assert payload["artifacts"]["activeTaskCloseRuntimeStdout"] == str(
            artifacts_dir / "active-runtime.stdout.log"
        )
        assert payload["artifacts"]["activeTaskCloseNodeReport"] == str(
            artifacts_dir / "active-node-report.json"
        )
        rehearsal_mock.assert_called_once()
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        assert saved["ok"] is True
