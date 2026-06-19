import json
import os
from pathlib import Path
from unittest import mock

import pytest

from src import packaged_desktop_smoke as smoke
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


@pytest.mark.slow
@pytest.mark.windows
def test_run_packaged_smoke_writes_failure_report_on_runtime_timeout() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock()
        process.pid = 4242
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
                "--rebuild",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                side_effect=TimeoutError("timed out waiting for bridge"),
            ),
            mock.patch.object(smoke, "terminate_process_tree") as terminate_mock,
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
            mock.patch.object(smoke, "fetch_startup_metrics", return_value=[]),
            mock.patch.object(smoke, "read_startup_metrics_file", return_value=[]),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
        ):
            payload = smoke.run_packaged_smoke(args)
        assert not payload["ok"]
        assert payload["failure"]["step"] == "runner"
        assert "timed out waiting for bridge" in payload["failure"]["message"]
        assert payload["environment"]["tmp"] == "C:/tmp"
        assert report_path.exists()
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        assert not saved["ok"]
        assert Path(saved["artifacts"]["reportPath"]).exists()
        assert terminate_mock.call_count >= 1
        assert terminate_mock.call_args_list[-1] == mock.call(process)
        assert stdout_handle.close.call_count >= 1
        assert stderr_handle.close.call_count >= 1


@pytest.mark.windows
def test_run_packaged_smoke_preserves_failure_metrics_on_runtime_timeout() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock()
        process.pid = 4242
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        rows = [
            {"event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
            {"event": "desktop_site_ready", "fields": {"elapsedMs": 400}},
        ]
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                side_effect=TimeoutError("timed out waiting for bridge"),
            ),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
            mock.patch.object(smoke, "fetch_startup_metrics", return_value=rows),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
        ):
            payload = smoke.run_packaged_smoke(args)
        saved = json.loads(report_path.read_text(encoding="utf-8"))

    assert payload["ok"] is False
    assert payload["startupMetrics"] == rows
    assert saved["startupMetrics"] == rows


def test_wait_for_packaged_runtime_rejects_default_browser_launch_for_startup_probe() -> None:
    process = mock.Mock()
    process.poll.return_value = None
    with (
        mock.patch.object(smoke, "fetch_json", side_effect=[{"ok": True}, {"ok": True}]),
        mock.patch.object(
            smoke,
            "fetch_startup_metrics",
            return_value=[
                {
                    "event": "desktop_browser_launch_selected",
                    "fields": {"mode": "default-browser"},
                }
            ],
        ),
        mock.patch.object(smoke.time, "monotonic", side_effect=[0.0, 0.0]),
    ):
        with pytest.raises(RuntimeError, match="managed Chromium app window"):
            smoke.wait_for_packaged_runtime(
                process,
                site_base_url="http://127.0.0.1:8080",
                bridge_base_url="http://127.0.0.1:8877",
                timeout_s=5.0,
                require_managed_window=True,
                require_page_ready=False,
            )


def test_wait_for_packaged_runtime_accepts_jobs_metric_as_page_ready() -> None:
    process = mock.Mock()
    process.poll.return_value = None
    rows = [
        {"event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
        {"event": "desktop_site_ready", "fields": {"elapsedMs": 400}},
        {"event": "desktop_window_created", "fields": {"elapsedMs": 700}},
        {"event": "desktop_shell_window_shown", "fields": {"elapsedMs": 900}},
        {"event": "jobs_first_render", "payload": {"elapsedMs": 1200}},
    ]
    with (
        mock.patch.object(smoke, "fetch_json", side_effect=[{"ok": True}, {"ok": True}]),
        mock.patch.object(smoke, "fetch_startup_metrics", return_value=rows),
        mock.patch.object(
            smoke, "_packaged_runtime_page_ready", side_effect=TimeoutError("timed out")
        ) as page_ready_mock,
        mock.patch.object(smoke.time, "monotonic", side_effect=[0.0, 0.0]),
    ):
        result = smoke.wait_for_packaged_runtime(
            process,
            site_base_url="http://127.0.0.1:8080",
            bridge_base_url="http://127.0.0.1:8877",
            timeout_s=5.0,
            open_path="jobs.html",
            required_events=smoke.STARTUP_REQUIRED_EVENTS,
            require_page_ready=True,
        )

    assert result["startupMetrics"] == rows
    page_ready_mock.assert_not_called()


def test_wait_for_packaged_runtime_tolerates_optional_status_fetch_errors() -> None:
    process = mock.Mock()
    process.poll.return_value = None
    rows = [{"event": "jobs_first_render", "payload": {"elapsedMs": 1200}}]
    with (
        mock.patch.object(smoke, "fetch_json", side_effect=[OSError("health reset"), {}]),
        mock.patch.object(smoke, "fetch_startup_metrics", return_value=rows),
        mock.patch.object(smoke.time, "monotonic", side_effect=[0.0, 0.0]),
    ):
        result = smoke.wait_for_packaged_runtime(
            process,
            site_base_url="http://127.0.0.1:8080",
            bridge_base_url="http://127.0.0.1:8877",
            timeout_s=5.0,
            open_path="jobs.html",
            required_events=("jobs_first_render",),
            require_page_ready=True,
        )

    assert result["health"] == {}
    assert result["session"] == {}
    assert result["startupMetrics"] == rows


@pytest.mark.parametrize(
    "fetch_side_effect",
    [
        RuntimeError("health shim bug"),
        [OSError("health reset"), RuntimeError("session shim bug")],
    ],
)
def test_wait_for_packaged_runtime_does_not_swallow_optional_status_bug(
    fetch_side_effect: Exception | list[Exception],
) -> None:
    process = mock.Mock()
    process.poll.return_value = None
    rows = [{"event": "jobs_first_render", "payload": {"elapsedMs": 1200}}]
    with (
        mock.patch.object(smoke, "fetch_json", side_effect=fetch_side_effect),
        mock.patch.object(smoke, "fetch_startup_metrics", return_value=rows),
        mock.patch.object(smoke.time, "monotonic", side_effect=[0.0, 0.0]),
        pytest.raises(RuntimeError, match="shim bug"),
    ):
        smoke.wait_for_packaged_runtime(
            process,
            site_base_url="http://127.0.0.1:8080",
            bridge_base_url="http://127.0.0.1:8877",
            timeout_s=5.0,
            open_path="jobs.html",
            required_events=("jobs_first_render",),
            require_page_ready=True,
        )


def test_wait_for_runtime_events_retries_transient_bridge_reset() -> None:
    rows = [
        {"event": "jobs_first_render", "payload": {"elapsedMs": 1200}},
        {"event": "jobs_first_interactive", "payload": {"elapsedMs": 1400}},
    ]
    with (
        mock.patch.object(
            smoke,
            "fetch_startup_metrics",
            side_effect=[OSError("[WinError 10054] reset"), rows],
        ),
        mock.patch.object(smoke.time, "monotonic", side_effect=[0.0, 0.0, 1.0]),
        mock.patch.object(smoke.time, "sleep"),
    ):
        result = smoke.wait_for_runtime_events(
            "http://127.0.0.1:8877",
            ("jobs_first_render", "jobs_first_interactive"),
            timeout_s=5.0,
        )
    assert result == rows


def test_wait_for_runtime_events_accepts_inferred_shell_window_event_alias() -> None:
    rows = [{"event": "desktop_shell_window_shown_inferred", "fields": {"elapsedMs": 900}}]
    with (
        mock.patch.object(smoke, "fetch_startup_metrics", return_value=rows),
        mock.patch.object(smoke.time, "monotonic", side_effect=[0.0, 0.0]),
    ):
        result = smoke.wait_for_runtime_events(
            "http://127.0.0.1:8877",
            ("desktop_shell_window_shown",),
            timeout_s=5.0,
        )

    assert result == rows


def test_capture_runtime_snapshot_preserves_versioned_startup_metrics() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        artifacts_dir = Path(tmp) / "artifacts"
        metrics_payload = {
            "ok": True,
            "rows": [
                {
                    "schemaVersion": 1,
                    "ts": "2026-04-17T09:00:00+00:00",
                    "event": "desktop_browser_watchdog_handoff_confirmed",
                    "category": "handoff",
                    "fields": {"evidence": "startup_metric"},
                }
            ],
        }
        with mock.patch.object(
            smoke,
            "fetch_json",
            side_effect=[
                {"ok": True, "detail": "healthy"},
                {"ok": True, "desktopSession": {}},
                metrics_payload,
                {"storageMetrics": {"writes": {"writeCount": 3}}},
                {
                    "ok": True,
                    "storage": {
                        "migrationVersion": "008",
                        "walMode": "wal",
                        "authorityModes": {
                            "taskRuns": "sqlite",
                            "sourceRuns": "sqlite",
                        },
                    },
                },
                {
                    "ok": True,
                    "routeTimings": {"routes": [{"label": "GET /ops/health", "p95Ms": 12}]},
                    "operationTimings": {"operations": []},
                },
            ],
        ):
            snapshots = smoke.capture_runtime_snapshot("http://127.0.0.1:8877", artifacts_dir)

        saved = json.loads(Path(snapshots["startupMetricsSnapshot"]).read_text(encoding="utf-8"))
        storage_metrics = json.loads(
            Path(snapshots["storageMetricsSnapshot"]).read_text(encoding="utf-8")
        )
        storage_health = json.loads(
            Path(snapshots["storageHealthSnapshot"]).read_text(encoding="utf-8")
        )
        performance_profile = json.loads(
            Path(snapshots["performanceProfileSnapshot"]).read_text(encoding="utf-8")
        )
        assert saved == metrics_payload
        assert saved["rows"][0]["schemaVersion"] == 1
        assert saved["rows"][0]["category"] == "handoff"
        assert storage_metrics["storageMetrics"]["writes"]["writeCount"] == 3
        assert storage_health["ok"] is True
        assert storage_health["storage"]["migrationVersion"] == "008"
        assert storage_health["storage"]["authorityModes"]["taskRuns"] == "sqlite"
        assert storage_health["storage"]["authorityModes"]["sourceRuns"] == "sqlite"
        assert performance_profile["routeTimings"]["routes"][0]["label"] == "GET /ops/health"


def test_capture_performance_profile_snapshot_is_non_fatal() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        artifacts_dir = Path(tmp) / "artifacts"
        with mock.patch.object(smoke, "fetch_json", side_effect=OSError("older bridge")):
            snapshots = smoke.capture_performance_profile_snapshot(
                "http://127.0.0.1:8877",
                artifacts_dir,
                filename="performance-profile.startup.json",
            )

        saved = json.loads(
            Path(snapshots["performanceProfileSnapshot"]).read_text(encoding="utf-8")
        )
        storage_saved = json.loads(
            Path(snapshots["storageMetricsSnapshot"]).read_text(encoding="utf-8")
        )
        assert saved == {"ok": False, "error": "older bridge"}
        assert storage_saved == {"ok": False, "error": "older bridge"}


@pytest.mark.parametrize("exc_type", [AssertionError, RuntimeError])
def test_capture_runtime_snapshot_does_not_swallow_unexpected_fetch_bug(
    exc_type: type[Exception],
) -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        artifacts_dir = Path(tmp) / "artifacts"
        with (
            mock.patch.object(smoke, "fetch_json", side_effect=exc_type("fetch shim bug")),
            pytest.raises(exc_type, match="fetch shim bug"),
        ):
            smoke.capture_runtime_snapshot("http://127.0.0.1:8877", artifacts_dir)


@pytest.mark.windows
def test_run_embedded_runtime_probe_writes_versioned_startup_metrics_snapshot() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        artifacts_root = root / "artifacts"
        process = mock.Mock()
        process.pid = 123
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        metrics_rows = [
            {
                "schemaVersion": 1,
                "ts": "2026-04-17T09:00:00+00:00",
                "event": "desktop_browser_watchdog_handoff_confirmed",
                "category": "handoff",
                "fields": {"evidence": "startup_metric"},
            },
            {
                "schemaVersion": 1,
                "ts": "2026-04-17T09:00:01+00:00",
                "event": "jobs_first_interactive",
                "category": "page",
                "payload": {"elapsedMs": 1000},
            },
        ]
        with (
            mock.patch.object(smoke, "choose_free_port", side_effect=[52001, 52002]),
            mock.patch.object(smoke, "packaged_runtime_env_overrides", return_value={}),
            mock.patch.object(smoke, "clear_packaged_desktop_session_state"),
            mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
            ),
            mock.patch.object(smoke, "wait_for_packaged_runtime", return_value={}),
            mock.patch.object(smoke, "wait_for_runtime_events", return_value=metrics_rows),
            mock.patch.object(
                smoke,
                "startup_profile_required_events",
                return_value=("desktop_browser_watchdog_handoff_confirmed",),
            ),
            mock.patch.object(
                smoke,
                "summarize_startup_metrics",
                return_value={"status": "passed", "classification": "ok"},
            ) as summarize_mock,
            mock.patch.object(smoke, "write_startup_summary"),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            result = smoke.run_embedded_runtime_probe(
                exe_path=exe_path,
                probe={"name": "Startup Probe", "openPath": "jobs.html"},
                artifacts_root=artifacts_root,
                runtime_timeout_s=5.0,
                startup_probe=True,
                profile_mode="cold",
            )

        saved = json.loads(
            (artifacts_root / "startup-probe" / "startup-metrics.json").read_text(encoding="utf-8")
        )
        assert result["status"] == "passed"
        assert saved["rows"] == metrics_rows
        assert saved["rows"][0]["category"] == "handoff"
        summarize_mock.assert_called_once_with(metrics_rows, page="jobs", profile_mode="cold")


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
