"""Packaged desktop runtime wait/report tests for probe classification and session roots."""

import json
import os
from pathlib import Path
from unittest import mock

import pytest

from src import packaged_desktop_smoke as smoke
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


def test_run_packaged_smoke_fails_startup_probe_when_no_managed_browser_is_available() -> None:
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
                "--startup-probe",
                "--profile-only",
            ]
        )
        with (
            mock.patch.object(
                smoke,
                "select_startup_probe_browser",
                side_effect=RuntimeError(
                    "No supported managed Chromium probe browser available. "
                    "Install Chrome, Brave, or an Edge build that can launch in app mode."
                ),
            ),
            mock.patch.object(smoke, "terminate_process_tree") as terminate_mock,
        ):
            payload = smoke.run_packaged_smoke(args)

        assert payload["ok"] is False
        assert payload["failure"]["category"] == "probe_browser_unavailable"
        assert (
            "No supported managed Chromium probe browser available" in payload["failure"]["message"]
        )
        assert payload["probeBrowser"]["preferredBrowserName"] == ""
        terminate_mock.assert_called_once_with(None)


@pytest.mark.windows
def test_run_packaged_smoke_classifies_default_browser_launch_as_non_authoritative() -> None:
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
        partial_metrics = [
            {
                "event": "desktop_browser_launch_selected",
                "fields": {
                    "browser": "msedge",
                    "browserPath": "C:/Edge/msedge.exe",
                    "mode": "default-browser",
                },
            }
        ]
        with (
            mock.patch.object(
                smoke,
                "select_startup_probe_browser",
                return_value={
                    "browserName": "msedge",
                    "browserPath": "C:/Edge/msedge.exe",
                },
            ),
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                side_effect=RuntimeError(
                    "Startup probe requires a managed Chromium app window; "
                    "desktop launch mode was 'default-browser'."
                ),
            ),
            mock.patch.object(smoke, "fetch_startup_metrics", return_value=partial_metrics),
            mock.patch.object(smoke, "read_startup_metrics_file", return_value=[]),
            mock.patch.object(
                smoke,
                "summarize_startup_metrics",
                return_value={
                    "status": "failed",
                    "classification": "metrics incomplete",
                    "missingEvents": ["jobs_first_render", "jobs_first_interactive"],
                    "stages": [],
                },
            ),
            mock.patch.object(smoke, "write_startup_summary"),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            payload = smoke.run_packaged_smoke(args)

        assert payload["ok"] is False
        assert payload["failure"]["category"] == "non_authoritative_browser_launch"
        assert payload["startupProfile"]["classification"] == "non-authoritative browser launch"
        assert payload["probeBrowser"]["launchMode"] == "default-browser"
        assert payload["probeBrowser"]["selectedBrowserName"] == "msedge"


@pytest.mark.windows
def test_run_packaged_smoke_classifies_chromium_app_crash_before_jobs_markers() -> None:
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
        partial_metrics = [
            {"event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
            {"event": "desktop_site_ready", "fields": {"elapsedMs": 300}},
            {"event": "desktop_window_created", "fields": {"elapsedMs": 550}},
            {"event": "desktop_shell_window_shown", "fields": {"elapsedMs": 800}},
            {
                "event": "desktop_browser_launch_selected",
                "fields": {
                    "browser": "chrome",
                    "browserPath": "C:/Chrome/chrome.exe",
                    "mode": "chromium-app",
                },
            },
            {"event": "jobs_module_boot_start", "payload": {"elapsedMs": 900}},
            {"event": "desktop_window_closed", "fields": {"reason": "bridge_exit"}},
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
                side_effect=OSError("[WinError 10054] An existing connection was forcibly closed"),
            ),
            mock.patch.object(smoke, "fetch_startup_metrics", return_value=partial_metrics),
            mock.patch.object(smoke, "read_startup_metrics_file", return_value=[]),
            mock.patch.object(
                smoke,
                "summarize_startup_metrics",
                return_value={
                    "status": "failed",
                    "classification": "metrics incomplete",
                    "missingEvents": ["jobs_first_render", "jobs_first_interactive"],
                    "stages": [],
                },
            ),
            mock.patch.object(smoke, "write_startup_summary"),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            payload = smoke.run_packaged_smoke(args)

        assert payload["ok"] is False
        assert payload["failure"]["category"] == "browser_runtime_startup_failed"
        assert payload["startupProfile"]["classification"] == "browser runtime startup failed"
        assert payload["probeBrowser"]["launchMode"] == "chromium-app"
        assert payload["probeBrowser"]["windowClosedReason"] == "bridge_exit"


def test_run_packaged_smoke_uses_artifact_local_session_root_even_when_global_session_exists() -> (
    None
):
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
                "--node-smoke-script",
                str(smoke.JOBS_PIPELINE_NODE_SMOKE_SCRIPT),
            ]
        )
        global_local_app_data = root / "global-localappdata"
        global_session_root = smoke.desktop_update_mod.resolve_desktop_session_root(
            {"LOCALAPPDATA": str(global_local_app_data)}
        )
        (global_session_root / smoke.DESKTOP_SESSION_STATE_FILE).write_text(
            json.dumps({"bridgePort": 8877, "dataDir": str(root / "stale-data")}),
            encoding="utf-8",
        )
        captured_env: dict[str, str] = {}

        def fake_launch_packaged_exe(*args, **kwargs):  # noqa: ANN002, ANN003
            captured_env.update(kwargs.get("env") or {})
            return process, stdout_handle, stderr_handle

        with (
            mock.patch.dict(os.environ, {"LOCALAPPDATA": str(global_local_app_data)}, clear=False),
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(smoke, "launch_packaged_exe", side_effect=fake_launch_packaged_exe),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                return_value={
                    "health": {"ok": True},
                    "session": {"ok": True},
                    "startupMetrics": [],
                },
            ),
            mock.patch.object(smoke, "capture_runtime_snapshot", return_value={}),
            mock.patch.object(
                smoke,
                "run_packaged_node_smoke",
                return_value={
                    "exitCode": 0,
                    "reportPath": str(artifacts_dir / "smoke-report.json"),
                    "outputDir": str(artifacts_dir / "smoke-output"),
                    "scenarios": [],
                    "failureCategory": "",
                    "runnerError": "",
                    "environment": {"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
                },
            ),
            mock.patch.object(smoke, "terminate_process_tree"),
        ):
            payload = smoke.run_packaged_smoke(args)

        assert payload["ok"] is True
        assert Path(captured_env["APPDATA"]).resolve() == (
            smoke.packaged_desktop_roaming_appdata_root(
                artifacts_dir, session_scope="runtime"
            ).resolve()
        )
        assert Path(captured_env["LOCALAPPDATA"]).resolve() == (
            smoke.packaged_desktop_local_appdata_root(
                artifacts_dir, session_scope="runtime"
            ).resolve()
        )
        assert Path(captured_env["LOCALAPPDATA"]).resolve() != global_local_app_data.resolve()
        assert captured_env["BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE"] == "stub-success"
