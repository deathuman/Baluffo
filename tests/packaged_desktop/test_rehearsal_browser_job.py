"""Packaged desktop rehearsal tests for browser job."""

from ._rehearsal_shared import (
    Path,
    mock,
    pytest,
    smoke,
    subprocess,
    workspace_tmpdir,
)

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


@pytest.mark.windows
def test_run_packaged_browser_job_rehearsal_passes_with_attached_pid_proof() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        artifacts_dir = root / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        runtime_process = mock.Mock(spec=subprocess.Popen)
        runtime_process.pid = 222
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()

        with (
            mock.patch.object(smoke.sys, "platform", "win32"),
            mock.patch.object(
                smoke,
                "_select_packaged_browser_job_browser",
                return_value=(
                    {"browserName": "chrome", "browserPath": "C:/Chrome/chrome.exe"},
                    {smoke.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV: "C:/Chrome/chrome.exe"},
                ),
            ),
            mock.patch.object(smoke, "choose_free_port", side_effect=[8080, 8877]),
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
                    "startupMetrics": [
                        {"event": "desktop_browser_process_spawn_started"},
                        {"event": "desktop_browser_job_attached", "fields": {"pid": 333}},
                        {"event": "desktop_browser_launch_accepted"},
                        {
                            "event": "desktop_browser_launch_selected",
                            "fields": {"mode": "chromium-app"},
                        },
                    ],
                },
            ) as wait_runtime_mock,
            mock.patch.object(
                smoke,
                "_select_browser_shutdown_proof",
                return_value={
                    "proofSource": "attached-browser-pid",
                    "proofPid": 333,
                    "attachedPid": 333,
                    "windowPid": 0,
                },
            ),
            mock.patch.object(smoke.desktop_app_mod, "is_process_alive", return_value=True),
            mock.patch.object(
                smoke,
                "_terminate_launcher_process_only",
            ) as terminate_launcher_mock,
            mock.patch.object(smoke, "_wait_for_pid_exit") as wait_pid_exit_mock,
            mock.patch.object(smoke, "_wait_for_launcher_exit") as wait_launcher_exit_mock,
            mock.patch.object(
                smoke,
                "_wait_for_desktop_ports_released",
            ) as wait_ports_released_mock,
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            payload = smoke.run_packaged_browser_job_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=5.0,
            )

    assert payload["status"] == "passed"
    assert payload["slug"] == "packaged-browser-job-rehearsal"
    assert payload["details"]["proofSource"] == "attached-browser-pid"
    assert payload["details"]["selectedBrowserName"] == "chrome"
    assert payload["details"]["attachedPid"] == 333
    assert payload["details"]["portRetryObserved"] is False
    assert launch_mock.call_args.kwargs["env"][
        smoke.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV
    ] == ("C:/Chrome/chrome.exe")
    assert launch_mock.call_args.kwargs["open_path"] == "desktop-probe.html"
    assert wait_runtime_mock.call_args.kwargs["open_path"] == "desktop-probe.html"
    terminate_launcher_mock.assert_called_once_with(runtime_process)
    wait_launcher_exit_mock.assert_called_once_with(runtime_process, timeout_s=45.0)
    wait_pid_exit_mock.assert_called_once_with(333, timeout_s=15.0)
    wait_ports_released_mock.assert_called_once_with(8080, 8877, timeout_s=15.0)
    assert payload["details"]["browserCloseShutdown"] is True
    assert payload["details"]["desktopPortsReleased"] is True


@pytest.mark.windows
def test_run_packaged_browser_job_rehearsal_fails_when_attach_metric_is_missing() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        artifacts_dir = root / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        runtime_process = mock.Mock(spec=subprocess.Popen)
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()

        with (
            mock.patch.object(smoke.sys, "platform", "win32"),
            mock.patch.object(
                smoke,
                "_select_packaged_browser_job_browser",
                return_value=(
                    {"browserName": "chrome", "browserPath": "C:/Chrome/chrome.exe"},
                    {smoke.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV: "C:/Chrome/chrome.exe"},
                ),
            ),
            mock.patch.object(smoke, "choose_free_port", side_effect=[8080, 8877]),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                return_value=(runtime_process, stdout_handle, stderr_handle),
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime_with_port_pivot",
                return_value={
                    "actualSitePort": 9001,
                    "actualBridgePort": 9002,
                    "portRetryObserved": True,
                    "startupMetrics": [
                        {"event": "desktop_browser_process_spawn_started"},
                        {"event": "desktop_browser_launch_accepted"},
                        {
                            "event": "desktop_browser_launch_selected",
                            "fields": {"mode": "chromium-app"},
                        },
                        {"event": "desktop_runtime_port_retry"},
                    ],
                },
            ),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            payload = smoke.run_packaged_browser_job_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=5.0,
            )

    assert payload["status"] == "failed"
    assert "desktop_browser_job_attached" in payload["error"]
    assert payload["details"]["portRetryObserved"] is True
    assert payload["details"]["actualBridgePort"] == 9002


def test_run_packaged_smoke_can_run_browser_job_rehearsal_mode() -> None:
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
                "--browser-job-rehearsal",
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
                "run_packaged_browser_job_rehearsal",
                return_value={
                    "name": "Packaged browser job rehearsal",
                    "slug": "packaged-browser-job-rehearsal",
                    "status": "passed",
                    "durationMs": 1200,
                    "error": "",
                    "details": {
                        "runtimeStdout": str(artifacts_dir / "browser-job-runtime.stdout.log"),
                        "runtimeStderr": str(artifacts_dir / "browser-job-runtime.stderr.log"),
                        "startupMetrics": str(artifacts_dir / "browser-job.startup-metrics.json"),
                    },
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)

    assert payload["ok"] is True
    assert payload["scenarios"][0]["slug"] == "packaged-browser-job-rehearsal"
    assert payload["artifacts"]["browserJobRehearsalRuntimeStdout"] == str(
        artifacts_dir / "browser-job-runtime.stdout.log"
    )
    assert payload["artifacts"]["browserJobRehearsalStartupMetrics"] == str(
        artifacts_dir / "browser-job.startup-metrics.json"
    )
    rehearsal_mock.assert_called_once()
