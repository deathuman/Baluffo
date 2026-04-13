import json
import os
from pathlib import Path
from unittest import mock

import pytest

from src import packaged_desktop_smoke as smoke
from src.ship.startup_profile import summarize_startup_metrics
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


def test_local_address_matches_listen_port() -> None:
    assert smoke._local_address_matches_listen_port("127.0.0.1:8080", 8080) is True
    assert smoke._local_address_matches_listen_port("127.0.0.1:8080", 8081) is False
    assert smoke._local_address_matches_listen_port("[::1]:9090", 9090) is True


def test_pids_listening_on_tcp_port_windows_parses_netstat() -> None:
    sample = (
        "\n"
        "Proto  Local Address          Foreign Address        State           PID\n"
        "TCP    127.0.0.1:50001        0.0.0.0:0              LISTENING       4242\n"
        "TCP    127.0.0.1:50002        0.0.0.0:0              LISTENING       4243\n"
        "TCP    192.168.1.1:50001      0.0.0.0:0              LISTENING       9999\n"
        "TCP    127.0.0.1:50003        10.0.0.1:443           ESTABLISHED     1111\n"
    )
    fake_completed = mock.Mock(stdout=sample, returncode=0)
    with mock.patch.object(smoke.os, "name", "nt"):
        with mock.patch.object(smoke.subprocess, "run", return_value=fake_completed) as run_mock:
            assert smoke.pids_listening_on_tcp_port_windows(50001) == {4242, 9999}
            assert smoke.pids_listening_on_tcp_port_windows(50002) == {4243}
            assert smoke.pids_listening_on_tcp_port_windows(50003) == set()
    assert run_mock.call_count == 3


def test_pids_listening_on_tcp_port_non_windows_returns_empty() -> None:
    with mock.patch.object(smoke.os, "name", "posix"):
        assert smoke.pids_listening_on_tcp_port_windows(9999) == set()


def test_read_startup_metrics_file_reads_jsonl_rows() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        metrics_path = Path(tmp) / "runtime-data" / "desktop-startup-metrics.jsonl"
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        metrics_path.write_text(
            "\n".join(
                [
                    json.dumps({"event": "desktop_launch_start", "fields": {"elapsedMs": 0}}),
                    json.dumps({"event": "desktop_window_shown", "fields": {"elapsedMs": 10}}),
                ]
            ),
            encoding="utf-8",
        )
        rows = smoke.read_startup_metrics_file(metrics_path.parent, limit=10)
        assert [row["event"] for row in rows] == ["desktop_launch_start", "desktop_window_shown"]


def test_startup_profile_required_events_include_window_and_page_ready_markers() -> None:
    assert smoke.startup_profile_required_events("jobs") == (
        "desktop_launch_start",
        "desktop_site_ready",
        "desktop_window_created",
        "desktop_shell_window_shown",
        "jobs_module_boot_start",
        "jobs_first_render",
        "jobs_first_interactive",
    )
    assert smoke.startup_profile_required_events("admin")[-1] == "admin_ready"
    assert smoke.startup_profile_required_events("desktop-probe") == (
        "desktop_launch_start",
        "desktop_site_ready",
        "desktop_window_created",
        "desktop_shell_window_shown",
        "desktop_probe_html_parse_start",
        "desktop_probe_ready",
    )
    assert smoke.startup_profile_required_events("desktop-probe-head") == (
        "desktop_launch_start",
        "desktop_site_ready",
        "desktop_window_created",
        "desktop_shell_window_shown",
        "desktop_probe_head_html_parse_start",
        "desktop_probe_head_ready",
    )
    assert smoke.startup_profile_required_events("desktop-probe-css") == (
        "desktop_launch_start",
        "desktop_site_ready",
        "desktop_window_created",
        "desktop_shell_window_shown",
        "desktop_probe_css_html_parse_start",
        "desktop_probe_css_ready",
    )
    assert smoke.startup_profile_required_events("desktop-probe-inline") == (
        "desktop_launch_start",
        "desktop_site_ready",
        "desktop_window_created",
        "desktop_shell_window_shown",
        "desktop_probe_inline_html_parse_start",
        "desktop_probe_inline_ready",
    )


def test_startup_profile_summary_classifies_blank_probe_page_load_delay() -> None:
    rows = [
        {
            "ts": "2026-03-10T12:00:00+00:00",
            "event": "desktop_launch_start",
            "fields": {"elapsedMs": 0},
        },
        {
            "ts": "2026-03-10T12:00:01+00:00",
            "event": "desktop_site_ready",
            "fields": {"elapsedMs": 1000},
        },
        {
            "ts": "2026-03-10T12:00:01.100000+00:00",
            "event": "desktop_window_created",
            "fields": {"elapsedMs": 1100},
        },
        {
            "ts": "2026-03-10T12:00:01.300000+00:00",
            "event": "desktop_shell_window_shown",
            "fields": {"elapsedMs": 1300},
        },
        {
            "ts": "2026-03-10T12:00:08+00:00",
            "event": "desktop_probe_html_parse_start",
            "payload": {"elapsedMs": 8000},
        },
        {
            "ts": "2026-03-10T12:00:08.050000+00:00",
            "event": "desktop_probe_ready",
            "payload": {"elapsedMs": 8050},
        },
    ]
    summary = summarize_startup_metrics(rows, page="desktop-probe", profile_mode="cold")
    assert summary["classification"] == "desktop page load delayed"
    assert summary["firstUsableMs"] == 8050


def test_startup_profile_summary_supports_head_probe_page() -> None:
    rows = [
        {
            "ts": "2026-03-10T12:00:00+00:00",
            "event": "desktop_launch_start",
            "fields": {"elapsedMs": 0},
        },
        {
            "ts": "2026-03-10T12:00:01+00:00",
            "event": "desktop_site_ready",
            "fields": {"elapsedMs": 1000},
        },
        {
            "ts": "2026-03-10T12:00:01.100000+00:00",
            "event": "desktop_window_created",
            "fields": {"elapsedMs": 1100},
        },
        {
            "ts": "2026-03-10T12:00:01.300000+00:00",
            "event": "desktop_shell_window_shown",
            "fields": {"elapsedMs": 1300},
        },
        {
            "ts": "2026-03-10T12:00:02.500000+00:00",
            "event": "desktop_probe_head_html_parse_start",
            "payload": {"elapsedMs": 2500},
        },
        {
            "ts": "2026-03-10T12:00:02.550000+00:00",
            "event": "desktop_probe_head_ready",
            "payload": {"elapsedMs": 2550},
        },
    ]
    summary = summarize_startup_metrics(rows, page="desktop-probe-head", profile_mode="cold")
    assert summary["firstUsableEvent"] == "desktop_probe_head_ready"
    assert summary["firstUsableMs"] == 2550


def test_startup_profile_summary_supports_css_probe_page() -> None:
    rows = [
        {
            "ts": "2026-03-10T12:00:00+00:00",
            "event": "desktop_launch_start",
            "fields": {"elapsedMs": 0},
        },
        {
            "ts": "2026-03-10T12:00:01+00:00",
            "event": "desktop_site_ready",
            "fields": {"elapsedMs": 1000},
        },
        {
            "ts": "2026-03-10T12:00:01.100000+00:00",
            "event": "desktop_window_created",
            "fields": {"elapsedMs": 1100},
        },
        {
            "ts": "2026-03-10T12:00:01.300000+00:00",
            "event": "desktop_shell_window_shown",
            "fields": {"elapsedMs": 1300},
        },
        {
            "ts": "2026-03-10T12:00:03+00:00",
            "event": "desktop_probe_css_html_parse_start",
            "payload": {"elapsedMs": 3000},
        },
        {
            "ts": "2026-03-10T12:00:03.020000+00:00",
            "event": "desktop_probe_css_ready",
            "payload": {"elapsedMs": 3020},
        },
    ]
    summary = summarize_startup_metrics(rows, page="desktop-probe-css", profile_mode="cold")
    assert summary["firstUsableEvent"] == "desktop_probe_css_ready"
    assert summary["firstUsableMs"] == 3020


def test_startup_profile_summary_supports_inline_probe_page() -> None:
    rows = [
        {
            "ts": "2026-03-10T12:00:00+00:00",
            "event": "desktop_launch_start",
            "fields": {"elapsedMs": 0},
        },
        {
            "ts": "2026-03-10T12:00:01+00:00",
            "event": "desktop_site_ready",
            "fields": {"elapsedMs": 1000},
        },
        {
            "ts": "2026-03-10T12:00:01.100000+00:00",
            "event": "desktop_window_created",
            "fields": {"elapsedMs": 1100},
        },
        {
            "ts": "2026-03-10T12:00:01.300000+00:00",
            "event": "desktop_shell_window_shown",
            "fields": {"elapsedMs": 1300},
        },
        {
            "ts": "2026-03-10T12:00:02.100000+00:00",
            "event": "desktop_probe_inline_html_parse_start",
            "payload": {"elapsedMs": 2100},
        },
        {
            "ts": "2026-03-10T12:00:02.120000+00:00",
            "event": "desktop_probe_inline_ready",
            "payload": {"elapsedMs": 2120},
        },
    ]
    summary = summarize_startup_metrics(rows, page="desktop-probe-inline", profile_mode="cold")
    assert summary["firstUsableEvent"] == "desktop_probe_inline_ready"
    assert summary["firstUsableMs"] == 2120


def test_startup_profile_summary_classifies_local_auth_delay() -> None:
    rows = [
        {
            "ts": "2026-03-10T12:00:00+00:00",
            "event": "desktop_launch_start",
            "fields": {"elapsedMs": 0},
        },
        {
            "ts": "2026-03-10T12:00:01+00:00",
            "event": "desktop_site_ready",
            "fields": {"elapsedMs": 1000},
        },
        {
            "ts": "2026-03-10T12:00:01.200000+00:00",
            "event": "desktop_window_created",
            "fields": {"elapsedMs": 1200},
        },
        {
            "ts": "2026-03-10T12:00:01.400000+00:00",
            "event": "desktop_window_shown",
            "fields": {"elapsedMs": 1400},
        },
        {
            "ts": "2026-03-10T12:00:02+00:00",
            "event": "desktop_page_loaded",
            "fields": {"elapsedMs": 2000},
        },
        {
            "ts": "2026-03-10T12:00:02.100000+00:00",
            "event": "jobs_local_data_init_ready",
            "payload": {"elapsedMs": 2100},
        },
        {
            "ts": "2026-03-10T12:00:07.500000+00:00",
            "event": "jobs_auth_ready",
            "payload": {"elapsedMs": 7500},
        },
        {
            "ts": "2026-03-10T12:00:08+00:00",
            "event": "jobs_first_render",
            "payload": {"elapsedMs": 8000},
        },
        {
            "ts": "2026-03-10T12:00:08.200000+00:00",
            "event": "jobs_first_interactive",
            "payload": {"elapsedMs": 8200},
        },
    ]
    summary = summarize_startup_metrics(rows, page="jobs", profile_mode="cold")
    assert summary["classification"] == "local auth bootstrap delayed"
    assert summary["status"] == "failed"


def test_ensure_portable_exe_raises_when_missing_and_build_still_missing() -> None:
    with (
        workspace_tmpdir("packaged-smoke") as tmp,
    ):
        exe_path = Path(tmp) / "dist" / "baluffo-portable" / "Baluffo.exe"
        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", exe_path),
            mock.patch.object(smoke, "run_portable_build") as build_mock,
        ):
            with pytest.raises(RuntimeError, match="Packaged desktop executable not found"):
                smoke.ensure_portable_exe(exe_path, rebuild=False)
            build_mock.assert_called_once()


def test_ensure_portable_exe_uses_rebuild_output_dir_when_requested() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        requested_exe = root / "dist" / "baluffo-portable" / "Baluffo.exe"
        rebuilt_dir = root / "artifacts" / "portable-build"
        rebuilt_exe = rebuilt_dir / "Baluffo.exe"
        rebuilt_dir.mkdir(parents=True, exist_ok=True)
        rebuilt_exe.write_text("exe", encoding="utf-8")
        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", requested_exe),
            mock.patch.object(smoke, "run_portable_build", return_value=rebuilt_exe) as build_mock,
        ):
            resolved = smoke.ensure_portable_exe(
                requested_exe, rebuild=True, rebuild_output_dir=rebuilt_dir
            )
        assert resolved == rebuilt_exe.resolve()
        build_mock.assert_called_once_with(rebuilt_dir)


def test_ensure_portable_exe_rebuilds_default_dist_when_exe_older_than_sources() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        fake_default = Path(tmp) / "dist" / "baluffo-portable" / "Baluffo.exe"
        fake_default.parent.mkdir(parents=True, exist_ok=True)
        fake_default.write_text("old", encoding="utf-8")
        old = 1_000_000.0
        os.utime(fake_default, (old, old))
        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", fake_default),
            mock.patch.object(smoke, "run_portable_build", return_value=fake_default) as build_mock,
        ):
            smoke.ensure_portable_exe(fake_default, rebuild=False)
        build_mock.assert_called_once_with(None)


def test_default_portable_exe_becomes_stale_when_frontend_asset_is_newer() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        fake_default = root / "dist" / "baluffo-portable" / "Baluffo.exe"
        frontend_asset = root / "frontend" / "jobs" / "app" / "feed.js"
        fake_default.parent.mkdir(parents=True, exist_ok=True)
        frontend_asset.parent.mkdir(parents=True, exist_ok=True)
        fake_default.write_text("old", encoding="utf-8")
        frontend_asset.write_text("newer", encoding="utf-8")
        old = 1_000_000.0
        new = old + 100
        os.utime(fake_default, (old, old))
        os.utime(frontend_asset, (new, new))

        with (
            mock.patch.object(smoke, "ROOT", root),
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", fake_default),
            mock.patch.object(smoke, "_PORTABLE_EXE_FRESHNESS_MARKERS", ()),
            mock.patch.object(smoke, "_PORTABLE_EXE_FRESHNESS_DIRS", (root / "frontend",)),
        ):
            assert smoke._default_portable_exe_stale(fake_default) is True


def test_ensure_portable_exe_honors_explicit_path_without_rebuilding() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        explicit_exe = Path(tmp) / "_out" / "latest" / "build" / "portable" / "Baluffo.exe"
        explicit_exe.parent.mkdir(parents=True, exist_ok=True)
        explicit_exe.write_text("exe", encoding="utf-8")
        with mock.patch.object(smoke, "run_portable_build") as build_mock:
            resolved = smoke.ensure_portable_exe(explicit_exe, rebuild=True)
        assert resolved == explicit_exe.resolve()
        build_mock.assert_not_called()


def test_ensure_portable_exe_rejects_missing_explicit_path_instead_of_building_default() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        explicit_exe = Path(tmp) / "_out" / "latest" / "build" / "portable" / "Baluffo.exe"
        with mock.patch.object(smoke, "run_portable_build") as build_mock:
            with pytest.raises(RuntimeError, match="Packaged desktop executable not found"):
                smoke.ensure_portable_exe(explicit_exe, rebuild=True)
        build_mock.assert_not_called()


def test_parse_packaged_node_smoke_report_reads_scenarios() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        report_path = Path(tmp) / "smoke-report.json"
        report_path.write_text(
            json.dumps(
                {
                    "ok": False,
                    "scenarios": [
                        {
                            "name": "Jobs startup",
                            "status": "passed",
                            "durationMs": 1200,
                            "error": "",
                        },
                        {
                            "name": "Admin action",
                            "status": "failed",
                            "durationMs": 500,
                            "error": "unlock failed",
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )
        rows = smoke.parse_packaged_node_smoke_report(report_path)
        assert len(rows) == 2
        assert rows[0]["name"] == "Jobs startup"
        assert rows[1]["error"] == "unlock failed"


def test_collect_packaged_smoke_env_diagnostics_reports_paths_and_elevation() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        exe_path = root / "dist" / "baluffo-portable" / "Baluffo.exe"
        exe_path.parent.mkdir(parents=True, exist_ok=True)
        exe_path.write_text("exe", encoding="utf-8")
        env = {"TMP": str(root / "tmp"), "TEMP": str(root / "temp")}
        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", exe_path),
            mock.patch.object(smoke, "is_windows_process_elevated", return_value=True),
        ):
            diagnostics = smoke.collect_packaged_smoke_env_diagnostics(
                artifacts_dir=root / "artifacts",
                requested_exe_path=exe_path,
                exe_path=exe_path,
                node_smoke_script=smoke.DEFAULT_NODE_SMOKE_SCRIPT,
                node_command=["C:/Program Files/nodejs/node.exe"],
                env=env,
            )
        assert diagnostics["requestedExePath"] == str(exe_path.resolve())
        assert diagnostics["defaultExePath"] == str(exe_path.resolve())
        assert diagnostics["exePathMode"] == "default-dist"
        assert diagnostics["exePathSource"] == "default-dist"
        assert diagnostics["explicitExePathFreshness"] == "n/a"
        assert diagnostics["rebuiltPortableExe"] is False
        assert diagnostics["artifactsDirWritable"]
        assert diagnostics["exeParentWritable"]
        assert diagnostics["nodePath"] == "C:/Program Files/nodejs/node.exe"
        assert diagnostics["tmp"] == str(root / "tmp")
        assert diagnostics["temp"] == str(root / "temp")
        assert diagnostics["isElevated"]


def test_collect_packaged_smoke_env_diagnostics_reports_explicit_path_freshness() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        explicit_exe = root / "_out" / "latest" / "build" / "portable" / "Baluffo.exe"
        explicit_exe.parent.mkdir(parents=True, exist_ok=True)
        explicit_exe.write_text("exe", encoding="utf-8")
        with (
            mock.patch.object(smoke, "_portable_exe_marker_staleness", return_value="stale"),
            mock.patch.object(smoke, "is_windows_process_elevated", return_value=False),
        ):
            diagnostics = smoke.collect_packaged_smoke_env_diagnostics(
                artifacts_dir=root / "artifacts",
                requested_exe_path=explicit_exe,
                exe_path=explicit_exe,
                node_smoke_script=smoke.DEFAULT_NODE_SMOKE_SCRIPT,
                node_command=["node"],
                env={},
            )
        assert diagnostics["exePathMode"] == "explicit-path"
        assert diagnostics["exePathSource"] == "explicit-path"
        assert diagnostics["explicitExePathFreshness"] == "stale"
        assert diagnostics["rebuiltPortableExe"] is False


def test_collect_packaged_smoke_env_diagnostics_reports_rebuilt_default_dist() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        exe_path = root / "dist" / "baluffo-portable" / "Baluffo.exe"
        exe_path.parent.mkdir(parents=True, exist_ok=True)
        exe_path.write_text("exe", encoding="utf-8")
        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", exe_path),
            mock.patch.object(smoke, "is_windows_process_elevated", return_value=False),
        ):
            diagnostics = smoke.collect_packaged_smoke_env_diagnostics(
                artifacts_dir=root / "artifacts",
                requested_exe_path=exe_path,
                exe_path=exe_path,
                node_smoke_script=smoke.DEFAULT_NODE_SMOKE_SCRIPT,
                rebuilt_portable_dir=root / "artifacts" / "portable-build",
                node_command=["node"],
                env={},
            )
        assert diagnostics["exePathMode"] == "default-dist"
        assert diagnostics["exePathSource"] == "rebuilt-dist"
        assert diagnostics["explicitExePathFreshness"] == "n/a"
        assert diagnostics["rebuiltPortableExe"] is True


def test_packaged_pipeline_smoke_mode_is_enabled_only_for_jobs_pipeline_script() -> None:
    assert (
        smoke.packaged_pipeline_smoke_mode(smoke.JOBS_PIPELINE_NODE_SMOKE_SCRIPT) == "stub-success"
    )
    assert smoke.packaged_pipeline_smoke_mode(smoke.DEFAULT_NODE_SMOKE_SCRIPT) == ""
    assert smoke.packaged_runtime_env_overrides(smoke.JOBS_PIPELINE_NODE_SMOKE_SCRIPT) == {
        "BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE": "stub-success"
    }
    assert smoke.packaged_runtime_env_overrides(smoke.DEFAULT_NODE_SMOKE_SCRIPT) == {}


def test_classify_subprocess_error_marks_spawn_eperm() -> None:
    error = PermissionError("spawn EPERM")
    assert smoke.classify_subprocess_error(error) == "node_process_spawn_blocked"
    assert (
        smoke.classify_subprocess_error("Error: spawn EPERM") == "playwright_worker_spawn_blocked"
    )
    assert (
        smoke.classify_subprocess_error("browserType.launch: spawn EPERM")
        == "node_process_spawn_blocked"
    )


@pytest.mark.slow
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


def test_run_packaged_smoke_classifies_spawn_failure_from_node_runner() -> None:
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
                return_value={
                    "health": {"ok": True},
                    "session": {"ok": True},
                    "startupMetrics": [],
                },
            ),
            mock.patch.object(
                smoke,
                "capture_runtime_snapshot",
                return_value={},
            ),
            mock.patch.object(
                smoke,
                "run_packaged_node_smoke",
                return_value={
                    "exitCode": 1,
                    "reportPath": str(artifacts_dir / "smoke-report.json"),
                    "outputDir": str(artifacts_dir / "smoke-output"),
                    "scenarios": [],
                    "failureCategory": "node_process_spawn_blocked",
                    "runnerError": "spawn EPERM",
                    "environment": {"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": True},
                },
            ),
            mock.patch.object(smoke, "terminate_process_tree"),
        ):
            payload = smoke.run_packaged_smoke(args)
        assert not payload["ok"]
        assert payload["failure"]["step"] == "playwright"
        assert payload["failure"]["category"] == "node_process_spawn_blocked"
        assert payload["failure"]["message"] == "spawn EPERM"
        assert payload["environment"]["isElevated"] is True


def test_run_packaged_smoke_fails_when_embedded_probe_fails() -> None:
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
                "--embedded-probes",
            ]
        )
        failing_probe = {
            "name": "Embedded Jobs Ready",
            "status": "failed",
            "durationMs": 2500,
            "error": "Missing embedded runtime events: jobs_auth_ready",
            "startupProfile": {},
        }
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(smoke, "run_embedded_runtime_probe", return_value=failing_probe),
            mock.patch.object(smoke, "terminate_process_tree") as terminate_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert not payload["ok"]
        assert payload["scenarios"] == [failing_probe, failing_probe, failing_probe]
        assert payload["failure"]["step"] == "runner"
        assert "Embedded Jobs Ready failed" in payload["failure"]["message"]
        terminate_mock.assert_called_once_with(None)
