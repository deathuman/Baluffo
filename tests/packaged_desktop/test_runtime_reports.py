"""Packaged desktop runtime wait/report tests for profile and report output."""

import json
from pathlib import Path
from unittest import mock

import pytest

from src import packaged_desktop_smoke as smoke
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


def test_run_packaged_smoke_profile_only_waits_for_jobs_startup_events() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock()
        process.pid = 999
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--startup-probe",
                "--profile-only",
            ]
        )
        captured_env: dict[str, str] = {}
        startup_metrics = [
            {"event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
            {"event": "desktop_site_ready", "fields": {"elapsedMs": 400}},
            {"event": "desktop_window_created", "fields": {"elapsedMs": 700}},
            {"event": "desktop_shell_window_shown", "fields": {"elapsedMs": 900}},
            {"event": "jobs_module_boot_start", "payload": {"elapsedMs": 1100}},
            {"event": "jobs_local_data_init_ready", "payload": {"elapsedMs": 1300}},
            {"event": "jobs_auth_ready", "payload": {"elapsedMs": 1500}},
            {"event": "jobs_first_render", "payload": {"elapsedMs": 1800}},
            {"event": "jobs_first_interactive", "payload": {"elapsedMs": 2100}},
        ]

        def fake_launch_packaged_exe(*args, **kwargs):  # noqa: ANN002, ANN003
            captured_env.update(kwargs.get("env") or {})
            return process, stdout_handle, stderr_handle

        with (
            mock.patch.object(
                smoke,
                "select_startup_probe_browser",
                return_value={
                    "browserName": "chrome",
                    "browserPath": "C:/Chrome/chrome.exe",
                },
            ),
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(smoke, "launch_packaged_exe", side_effect=fake_launch_packaged_exe),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                return_value={
                    "health": {"ok": True},
                    "session": {"ok": True},
                    "startupMetrics": [{"event": "desktop_shell_window_shown"}],
                },
            ) as runtime_mock,
            mock.patch.object(
                smoke,
                "wait_for_runtime_events",
                return_value=startup_metrics,
            ) as runtime_events_mock,
            mock.patch.object(smoke, "capture_runtime_snapshot", return_value={}) as snapshot_mock,
            mock.patch.object(
                smoke,
                "capture_performance_profile_snapshot",
                return_value={
                    "performanceProfileSnapshot": str(
                        artifacts_dir / "performance-profile.startup.json"
                    ),
                    "storageMetricsSnapshot": str(artifacts_dir / "storage-metrics.startup.json"),
                },
            ) as profile_mock,
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "summarize_startup_metrics",
                return_value={
                    "status": "passed",
                    "classification": "ok",
                    "firstUsableEvent": "jobs_first_interactive",
                    "firstUsableMs": 2100,
                    "stages": [],
                },
            ),
            mock.patch.object(smoke, "write_startup_summary"),
            mock.patch.object(smoke, "terminate_process_tree"),
        ):
            payload = smoke.run_packaged_smoke(args)

        assert payload["ok"] is True
        assert payload["startupMetrics"] == startup_metrics
        assert captured_env["BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE"] == "1"
        assert captured_env[smoke.desktop_app_mod.STARTUP_PROFILE_MODE_ENV] == "cold"
        assert (
            captured_env[smoke.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV] == "C:/Chrome/chrome.exe"
        )
        assert payload["probeBrowser"]["preferredBrowserName"] == "chrome"
        assert payload["probeBrowser"]["preferredBrowserPath"] == "C:/Chrome/chrome.exe"
        snapshot_mock.assert_not_called()
        profile_mock.assert_called_once_with(
            payload["bridgeBaseUrl"],
            artifacts_dir,
            filename="performance-profile.startup.json",
        )
        assert "performanceProfileSnapshot" in payload["artifacts"]
        assert "storageMetricsSnapshot" in payload["artifacts"]
        runtime_mock.assert_called_once()
        assert runtime_mock.call_args.kwargs["require_managed_window"] is True
        assert runtime_mock.call_args.kwargs["require_page_ready"] is False
        runtime_events_mock.assert_called_once_with(
            payload["bridgeBaseUrl"],
            smoke.startup_profile_required_events("jobs"),
            timeout_s=mock.ANY,
        )
        assert "smokeReport" not in payload["artifacts"]


@pytest.mark.windows
def test_run_warmup_launch_uses_warm_startup_profile_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        captured_env: dict[str, str] = {}
        process = mock.Mock()
        process.pid = 777
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()

        def fake_launch_packaged_exe(*args, **kwargs):  # noqa: ANN002, ANN003
            captured_env.update(kwargs.get("env") or {})
            return process, stdout_handle, stderr_handle

        with (
            mock.patch.object(smoke, "choose_free_port", side_effect=[51001, 51002]),
            mock.patch.object(smoke, "launch_packaged_exe", side_effect=fake_launch_packaged_exe),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                return_value={"health": {"ok": True}, "session": {"ok": True}},
            ),
            mock.patch.object(smoke.time, "sleep"),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            smoke.run_warmup_launch(
                exe_path,
                artifacts_root=root / "artifacts",
                open_path="jobs.html",
                runtime_timeout_s=5.0,
                startup_probe=True,
            )

        assert captured_env["BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE"] == "1"
        assert captured_env[smoke.desktop_app_mod.STARTUP_PROFILE_MODE_ENV] == "warm"


def test_run_packaged_smoke_writes_success_report_and_artifacts() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock()
        process.pid = 999
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--startup-probe",
            ]
        )
        startup_metrics = [{"event": "desktop_site_ready"}]
        scenarios = [
            {"name": "Startup", "status": "passed", "durationMs": 200, "error": ""},
            {"name": "Auth continuity", "status": "passed", "durationMs": 300, "error": ""},
        ]
        with (
            mock.patch.object(
                smoke,
                "select_startup_probe_browser",
                return_value={
                    "browserName": "chrome",
                    "browserPath": "C:/Chrome/chrome.exe",
                },
            ),
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                return_value={
                    "health": {"ok": True},
                    "session": {"ok": True, "user": None},
                    "startupMetrics": startup_metrics,
                },
            ),
            mock.patch.object(
                smoke,
                "wait_for_runtime_events",
                return_value=startup_metrics,
            ),
            mock.patch.object(
                smoke,
                "capture_runtime_snapshot",
                return_value={
                    "opsHealthSnapshot": str(artifacts_dir / "ops-health.json"),
                    "sessionSnapshot": str(artifacts_dir / "session.json"),
                    "startupMetricsSnapshot": str(artifacts_dir / "startup.json"),
                },
            ),
            mock.patch.object(
                smoke,
                "run_packaged_node_smoke",
                return_value={
                    "exitCode": 0,
                    "reportPath": str(artifacts_dir / "smoke-report.json"),
                    "outputDir": str(artifacts_dir / "smoke-output"),
                    "scenarios": scenarios,
                    "failureCategory": "",
                    "runnerError": "",
                    "environment": {
                        "tmp": str(artifacts_dir / "tmp"),
                        "temp": str(artifacts_dir / "tmp"),
                        "isElevated": False,
                    },
                },
            ),
            mock.patch.object(
                smoke,
                "summarize_startup_metrics",
                return_value={
                    "status": "passed",
                    "classification": "ok",
                    "firstUsableMs": 9000,
                    "stages": [],
                },
            ),
            mock.patch.object(
                smoke,
                "write_startup_summary",
            ),
            mock.patch.object(smoke, "terminate_process_tree") as terminate_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert payload["ok"]
        assert payload["scenarios"][0]["name"] == "Startup Profile"
        assert payload["scenarios"][1:] == scenarios
        assert payload["startupMetrics"] == startup_metrics
        assert payload["environment"]["tmp"] == str(artifacts_dir / "tmp")
        assert report_path.exists()
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        assert saved["ok"]
        assert saved["artifacts"]["smokeReport"] == str(artifacts_dir / "smoke-report.json")
        assert saved["artifacts"]["smokeRunnerStdout"] == str(
            artifacts_dir / "smoke-runner-stdout.log"
        )
        assert saved["artifacts"]["playwrightReport"] == str(artifacts_dir / "smoke-report.json")
        assert saved["artifacts"]["playwrightStdout"] == str(
            artifacts_dir / "smoke-runner-stdout.log"
        )
        terminate_mock.assert_called_once_with(process)
        stdout_handle.close.assert_called_once()
        stderr_handle.close.assert_called_once()


def test_run_packaged_smoke_fail_on_threshold_sets_startup_failure() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock()
        process.pid = 999
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--startup-probe",
                "--fail-on-threshold",
            ]
        )
        startup_metrics = [{"event": "desktop_site_ready"}]
        with (
            mock.patch.object(
                smoke,
                "select_startup_probe_browser",
                return_value={
                    "browserName": "chrome",
                    "browserPath": "C:/Chrome/chrome.exe",
                },
            ),
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                return_value={
                    "health": {"ok": True},
                    "session": {"ok": True, "user": None},
                    "startupMetrics": startup_metrics,
                },
            ),
            mock.patch.object(smoke, "wait_for_runtime_events", return_value=startup_metrics),
            mock.patch.object(smoke, "capture_runtime_snapshot", return_value={}),
            mock.patch.object(
                smoke,
                "run_packaged_node_smoke",
                return_value={
                    "exitCode": 0,
                    "reportPath": str(artifacts_dir / "smoke-report.json"),
                    "outputDir": str(artifacts_dir / "smoke-output"),
                    "scenarios": [{"name": "Startup", "status": "passed"}],
                    "failureCategory": "",
                    "runnerError": "",
                    "environment": {
                        "tmp": str(artifacts_dir / "tmp"),
                        "temp": str(artifacts_dir / "tmp"),
                        "isElevated": False,
                    },
                },
            ),
            mock.patch.object(
                smoke,
                "summarize_startup_metrics",
                return_value={
                    "status": "failed",
                    "classification": "jobs render delayed",
                    "firstUsableMs": 19000,
                    "stages": [],
                    "perfRegressions": [
                        {
                            "type": "perf_regression",
                            "stage": "total_launch_to_first_usable_ui",
                            "durationMs": 19000,
                            "thresholdMs": 18000,
                            "severity": "critical",
                        }
                    ],
                },
            ),
            mock.patch.object(smoke, "write_startup_summary"),
            mock.patch.object(smoke, "terminate_process_tree"),
        ):
            payload = smoke.run_packaged_smoke(args)

        assert payload["ok"] is False
        assert payload["failure"]["step"] == "startup-profile-threshold"
        assert payload["failure"]["category"] == "startup_profile_threshold_exceeded"
        assert "19000ms > 18000ms" in payload["failure"]["message"]


def test_run_packaged_smoke_profile_record_only_keeps_thresholds_informational() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock()
        process.pid = 999
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--startup-probe",
                "--profile-only",
                "--profile-record-only",
                "--open-path",
                "admin.html",
            ]
        )
        startup_metrics = [
            {"event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
            {"event": "desktop_site_ready", "fields": {"elapsedMs": 400}},
            {"event": "desktop_window_created", "fields": {"elapsedMs": 2000}},
            {"event": "desktop_shell_window_shown", "fields": {"elapsedMs": 2004}},
            {"event": "admin_module_boot_start", "payload": {"elapsedMs": 2200}},
            {"event": "admin_ready", "payload": {"elapsedMs": 2500}},
            {"event": "admin_first_interactive", "payload": {"elapsedMs": 2502}},
        ]
        with (
            mock.patch.object(
                smoke,
                "select_startup_probe_browser",
                return_value={
                    "browserName": "chrome",
                    "browserPath": "C:/Chrome/chrome.exe",
                },
            ),
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                return_value={
                    "health": {"ok": True},
                    "session": {"ok": True},
                    "startupMetrics": startup_metrics,
                },
            ),
            mock.patch.object(smoke, "wait_for_runtime_events", return_value=startup_metrics),
            mock.patch.object(
                smoke,
                "summarize_startup_metrics",
                return_value={
                    "status": "failed",
                    "classification": "browser launch / app-window creation delayed",
                    "firstUsableEvent": "admin_first_interactive",
                    "firstUsableMs": 2502,
                    "stages": [],
                    "perfRegressions": [{"stage": "site_ready_to_window_created"}],
                },
            ),
            mock.patch.object(smoke, "write_startup_summary"),
            mock.patch.object(smoke, "terminate_process_tree"),
        ):
            payload = smoke.run_packaged_smoke(args)

        assert payload["ok"] is True
        assert not payload["failure"]
        assert payload["startupProfile"]["status"] == "failed"
