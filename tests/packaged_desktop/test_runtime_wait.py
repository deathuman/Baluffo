"""Packaged desktop runtime wait/report tests for runtime wait and snapshots."""

import json
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


def test_load_failure_startup_metrics_falls_back_to_file_on_fetch_error() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        runtime_data_dir = Path(tmp) / "runtime-data"
        file_rows = [{"event": "desktop_launch_start", "fields": {"elapsedMs": 0}}]
        with (
            mock.patch.object(smoke, "fetch_startup_metrics", side_effect=OSError("reset")),
            mock.patch.object(smoke, "read_startup_metrics_file", return_value=file_rows),
        ):
            rows = smoke.packaged_smoke_orchestrator_mod._load_failure_startup_metrics(
                {},
                runtime_data_dir=runtime_data_dir,
                bridge_base_url="http://127.0.0.1:8877",
            )

    assert rows == file_rows


def test_load_failure_startup_metrics_does_not_swallow_fetch_bug() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        runtime_data_dir = Path(tmp) / "runtime-data"
        with (
            mock.patch.object(
                smoke,
                "fetch_startup_metrics",
                side_effect=RuntimeError("metrics shim bug"),
            ),
            pytest.raises(RuntimeError, match="metrics shim bug"),
        ):
            smoke.packaged_smoke_orchestrator_mod._load_failure_startup_metrics(
                {},
                runtime_data_dir=runtime_data_dir,
                bridge_base_url="http://127.0.0.1:8877",
            )


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
