import json
import unittest
from pathlib import Path
from unittest import mock

from scripts import packaged_desktop_smoke as smoke
from scripts.ship.startup_profile import summarize_startup_metrics
from tests.temp_paths import workspace_tmpdir


class PackagedDesktopSmokeTests(unittest.TestCase):
    def test_read_startup_metrics_file_reads_jsonl_rows(self) -> None:
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
            self.assertEqual([row["event"] for row in rows], ["desktop_launch_start", "desktop_window_shown"])

    def test_startup_profile_required_events_include_window_and_page_ready_markers(self) -> None:
        self.assertEqual(
            smoke.startup_profile_required_events("jobs"),
            (
                "desktop_launch_start",
                "desktop_site_ready",
                "desktop_window_created",
                "desktop_window_load_url",
                "desktop_shell_window_shown",
                "jobs_module_boot_start",
                "jobs_first_render",
                "jobs_first_interactive",
            ),
        )
        self.assertEqual(smoke.startup_profile_required_events("admin")[-1], "admin_pin_gate_ready")
        self.assertEqual(
            smoke.startup_profile_required_events("desktop-probe"),
            (
                "desktop_launch_start",
                "desktop_site_ready",
                "desktop_window_created",
                "desktop_window_load_url",
                "desktop_shell_window_shown",
                "desktop_probe_html_parse_start",
                "desktop_probe_ready",
            ),
        )
        self.assertEqual(
            smoke.startup_profile_required_events("desktop-probe-head"),
            (
                "desktop_launch_start",
                "desktop_site_ready",
                "desktop_window_created",
                "desktop_window_load_url",
                "desktop_shell_window_shown",
                "desktop_probe_head_html_parse_start",
                "desktop_probe_head_ready",
            ),
        )
        self.assertEqual(
            smoke.startup_profile_required_events("desktop-probe-css"),
            (
                "desktop_launch_start",
                "desktop_site_ready",
                "desktop_window_created",
                "desktop_window_load_url",
                "desktop_shell_window_shown",
                "desktop_probe_css_html_parse_start",
                "desktop_probe_css_ready",
            ),
        )
        self.assertEqual(
            smoke.startup_profile_required_events("desktop-probe-inline"),
            (
                "desktop_launch_start",
                "desktop_site_ready",
                "desktop_window_created",
                "desktop_window_load_url",
                "desktop_shell_window_shown",
                "desktop_probe_inline_html_parse_start",
                "desktop_probe_inline_ready",
            ),
        )

    def test_startup_profile_summary_classifies_blank_probe_page_load_delay(self) -> None:
        rows = [
            {"ts": "2026-03-10T12:00:00+00:00", "event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
            {"ts": "2026-03-10T12:00:01+00:00", "event": "desktop_site_ready", "fields": {"elapsedMs": 1000}},
            {"ts": "2026-03-10T12:00:01.100000+00:00", "event": "desktop_window_created", "fields": {"elapsedMs": 1100}},
            {"ts": "2026-03-10T12:00:01.300000+00:00", "event": "desktop_shell_window_shown", "fields": {"elapsedMs": 1300}},
            {"ts": "2026-03-10T12:00:08+00:00", "event": "desktop_probe_html_parse_start", "payload": {"elapsedMs": 8000}},
            {"ts": "2026-03-10T12:00:08.050000+00:00", "event": "desktop_probe_ready", "payload": {"elapsedMs": 8050}},
        ]
        summary = summarize_startup_metrics(rows, page="desktop-probe", profile_mode="cold")
        self.assertEqual(summary["classification"], "webview page load delayed")
        self.assertEqual(summary["firstUsableMs"], 8050)

    def test_startup_profile_summary_supports_head_probe_page(self) -> None:
        rows = [
            {"ts": "2026-03-10T12:00:00+00:00", "event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
            {"ts": "2026-03-10T12:00:01+00:00", "event": "desktop_site_ready", "fields": {"elapsedMs": 1000}},
            {"ts": "2026-03-10T12:00:01.100000+00:00", "event": "desktop_window_created", "fields": {"elapsedMs": 1100}},
            {"ts": "2026-03-10T12:00:01.300000+00:00", "event": "desktop_shell_window_shown", "fields": {"elapsedMs": 1300}},
            {"ts": "2026-03-10T12:00:02.500000+00:00", "event": "desktop_probe_head_html_parse_start", "payload": {"elapsedMs": 2500}},
            {"ts": "2026-03-10T12:00:02.550000+00:00", "event": "desktop_probe_head_ready", "payload": {"elapsedMs": 2550}},
        ]
        summary = summarize_startup_metrics(rows, page="desktop-probe-head", profile_mode="cold")
        self.assertEqual(summary["firstUsableEvent"], "desktop_probe_head_ready")
        self.assertEqual(summary["firstUsableMs"], 2550)

    def test_startup_profile_summary_supports_css_probe_page(self) -> None:
        rows = [
            {"ts": "2026-03-10T12:00:00+00:00", "event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
            {"ts": "2026-03-10T12:00:01+00:00", "event": "desktop_site_ready", "fields": {"elapsedMs": 1000}},
            {"ts": "2026-03-10T12:00:01.100000+00:00", "event": "desktop_window_created", "fields": {"elapsedMs": 1100}},
            {"ts": "2026-03-10T12:00:01.300000+00:00", "event": "desktop_shell_window_shown", "fields": {"elapsedMs": 1300}},
            {"ts": "2026-03-10T12:00:03+00:00", "event": "desktop_probe_css_html_parse_start", "payload": {"elapsedMs": 3000}},
            {"ts": "2026-03-10T12:00:03.020000+00:00", "event": "desktop_probe_css_ready", "payload": {"elapsedMs": 3020}},
        ]
        summary = summarize_startup_metrics(rows, page="desktop-probe-css", profile_mode="cold")
        self.assertEqual(summary["firstUsableEvent"], "desktop_probe_css_ready")
        self.assertEqual(summary["firstUsableMs"], 3020)

    def test_startup_profile_summary_supports_inline_probe_page(self) -> None:
        rows = [
            {"ts": "2026-03-10T12:00:00+00:00", "event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
            {"ts": "2026-03-10T12:00:01+00:00", "event": "desktop_site_ready", "fields": {"elapsedMs": 1000}},
            {"ts": "2026-03-10T12:00:01.100000+00:00", "event": "desktop_window_created", "fields": {"elapsedMs": 1100}},
            {"ts": "2026-03-10T12:00:01.300000+00:00", "event": "desktop_shell_window_shown", "fields": {"elapsedMs": 1300}},
            {"ts": "2026-03-10T12:00:02.100000+00:00", "event": "desktop_probe_inline_html_parse_start", "payload": {"elapsedMs": 2100}},
            {"ts": "2026-03-10T12:00:02.120000+00:00", "event": "desktop_probe_inline_ready", "payload": {"elapsedMs": 2120}},
        ]
        summary = summarize_startup_metrics(rows, page="desktop-probe-inline", profile_mode="cold")
        self.assertEqual(summary["firstUsableEvent"], "desktop_probe_inline_ready")
        self.assertEqual(summary["firstUsableMs"], 2120)

    def test_startup_profile_summary_classifies_local_auth_delay(self) -> None:
        rows = [
            {"ts": "2026-03-10T12:00:00+00:00", "event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
            {"ts": "2026-03-10T12:00:01+00:00", "event": "desktop_site_ready", "fields": {"elapsedMs": 1000}},
            {"ts": "2026-03-10T12:00:01.200000+00:00", "event": "desktop_window_created", "fields": {"elapsedMs": 1200}},
            {"ts": "2026-03-10T12:00:01.400000+00:00", "event": "desktop_window_shown", "fields": {"elapsedMs": 1400}},
            {"ts": "2026-03-10T12:00:02+00:00", "event": "desktop_page_loaded", "fields": {"elapsedMs": 2000}},
            {"ts": "2026-03-10T12:00:02.100000+00:00", "event": "jobs_local_data_init_ready", "payload": {"elapsedMs": 2100}},
            {"ts": "2026-03-10T12:00:07.500000+00:00", "event": "jobs_auth_ready", "payload": {"elapsedMs": 7500}},
            {"ts": "2026-03-10T12:00:08+00:00", "event": "jobs_first_render", "payload": {"elapsedMs": 8000}},
            {"ts": "2026-03-10T12:00:08.200000+00:00", "event": "jobs_first_interactive", "payload": {"elapsedMs": 8200}},
        ]
        summary = summarize_startup_metrics(rows, page="jobs", profile_mode="cold")
        self.assertEqual(summary["classification"], "local auth bootstrap delayed")
        self.assertEqual(summary["status"], "failed")

    def test_ensure_portable_exe_raises_when_missing_and_build_still_missing(self) -> None:
        with workspace_tmpdir("packaged-smoke") as tmp, mock.patch.object(smoke, "run_portable_build") as build_mock:
            exe_path = Path(tmp) / "dist" / "baluffo-portable" / "Baluffo.exe"
            with self.assertRaisesRegex(RuntimeError, "Packaged desktop executable not found"):
                smoke.ensure_portable_exe(exe_path, rebuild=False)
            build_mock.assert_called_once()

    def test_ensure_portable_exe_uses_rebuild_output_dir_when_requested(self) -> None:
        with workspace_tmpdir("packaged-smoke") as tmp:
            root = Path(tmp)
            requested_exe = root / "dist" / "baluffo-portable" / "Baluffo.exe"
            rebuilt_dir = root / "artifacts" / "portable-build"
            rebuilt_exe = rebuilt_dir / "Baluffo.exe"
            rebuilt_dir.mkdir(parents=True, exist_ok=True)
            rebuilt_exe.write_text("exe", encoding="utf-8")
            with mock.patch.object(smoke, "run_portable_build", return_value=rebuilt_exe) as build_mock:
                resolved = smoke.ensure_portable_exe(requested_exe, rebuild=True, rebuild_output_dir=rebuilt_dir)
            self.assertEqual(resolved, rebuilt_exe.resolve())
            build_mock.assert_called_once_with(rebuilt_dir)

    def test_parse_playwright_report_flattens_spec_statuses(self) -> None:
        with workspace_tmpdir("packaged-smoke") as tmp:
            report_path = Path(tmp) / "playwright-report.json"
            report_path.write_text(
                json.dumps(
                    {
                        "suites": [
                            {
                                "title": "chromium",
                                "specs": [
                                    {
                                        "title": "Startup",
                                        "tests": [
                                            {
                                                "results": [
                                                    {
                                                        "status": "passed",
                                                        "duration": 1234,
                                                    }
                                                ]
                                            }
                                        ],
                                    },
                                    {
                                        "title": "Admin access",
                                        "tests": [
                                            {
                                                "results": [
                                                    {
                                                        "status": "failed",
                                                        "duration": 321,
                                                        "error": {"message": "unlock failed"},
                                                    }
                                                ]
                                            }
                                        ],
                                    },
                                ],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            rows = smoke.parse_playwright_report(report_path)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["name"], "Startup")
            self.assertEqual(rows[0]["status"], "passed")
            self.assertEqual(rows[1]["status"], "failed")
            self.assertEqual(rows[1]["error"], "unlock failed")

    def test_run_packaged_smoke_writes_failure_report_on_runtime_timeout(self) -> None:
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
            with mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path), mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
            ), mock.patch.object(
                smoke, "wait_for_packaged_runtime", side_effect=TimeoutError("timed out waiting for bridge")
            ), mock.patch.object(smoke, "terminate_process_tree") as terminate_mock:
                payload = smoke.run_packaged_smoke(args)
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["failure"]["step"], "runner")
            self.assertIn("timed out waiting for bridge", payload["failure"]["message"])
            self.assertTrue(report_path.exists())
            saved = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertFalse(saved["ok"])
            self.assertTrue(Path(saved["artifacts"]["reportPath"]).exists())
            self.assertGreaterEqual(terminate_mock.call_count, 1)
            self.assertEqual(terminate_mock.call_args_list[-1], mock.call(None))
            self.assertGreaterEqual(stdout_handle.close.call_count, 1)
            self.assertGreaterEqual(stderr_handle.close.call_count, 1)

    def test_run_packaged_smoke_writes_success_report_and_artifacts(self) -> None:
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
            embedded_scenarios = [
                {"name": "Embedded Jobs Ready", "status": "passed", "durationMs": 100, "error": "", "startupProfile": {}},
                {"name": "Embedded Saved Ready", "status": "passed", "durationMs": 120, "error": "", "startupProfile": {}},
                {"name": "Embedded Admin Ready", "status": "passed", "durationMs": 140, "error": "", "startupProfile": {}},
            ]
            scenarios = [
                {"name": "Startup", "status": "passed", "durationMs": 200, "error": ""},
                {"name": "Auth continuity", "status": "passed", "durationMs": 300, "error": ""},
            ]
            with mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path), mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
            ), mock.patch.object(
                smoke, "run_embedded_runtime_probe", side_effect=embedded_scenarios
            ), mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                return_value={
                    "health": {"ok": True},
                    "session": {"ok": True, "user": None},
                    "startupMetrics": startup_metrics,
                },
            ), mock.patch.object(
                smoke,
                "capture_runtime_snapshot",
                return_value={
                    "opsHealthSnapshot": str(artifacts_dir / "ops-health.json"),
                    "sessionSnapshot": str(artifacts_dir / "session.json"),
                    "startupMetricsSnapshot": str(artifacts_dir / "startup.json"),
                },
            ), mock.patch.object(
                smoke,
                "run_playwright_packaged_smoke",
                return_value={
                    "exitCode": 0,
                    "reportPath": str(artifacts_dir / "playwright-report.json"),
                    "outputDir": str(artifacts_dir / "playwright-output"),
                    "scenarios": scenarios,
                },
            ), mock.patch.object(
                smoke,
                "summarize_startup_metrics",
                return_value={"status": "passed", "classification": "ok", "firstUsableMs": 9000, "stages": []},
            ), mock.patch.object(
                smoke,
                "write_startup_summary",
            ), mock.patch.object(smoke, "terminate_process_tree") as terminate_mock:
                payload = smoke.run_packaged_smoke(args)
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["scenarios"][0:3], embedded_scenarios)
            self.assertEqual(payload["scenarios"][3]["name"], "Startup Profile")
            self.assertEqual(payload["scenarios"][4:], scenarios)
            self.assertEqual(payload["startupMetrics"], startup_metrics)
            self.assertTrue(report_path.exists())
            saved = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertTrue(saved["ok"])
            self.assertEqual(saved["artifacts"]["playwrightReport"], str(artifacts_dir / "playwright-report.json"))
            terminate_mock.assert_called_once_with(process)
            stdout_handle.close.assert_called_once()
            stderr_handle.close.assert_called_once()

    def test_run_packaged_smoke_fails_when_embedded_probe_fails(self) -> None:
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
                ]
            )
            failing_probe = {
                "name": "Embedded Jobs Ready",
                "status": "failed",
                "durationMs": 2500,
                "error": "Missing embedded runtime events: jobs_auth_ready",
                "startupProfile": {},
            }
            with mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path), mock.patch.object(
                smoke, "run_embedded_runtime_probe", return_value=failing_probe
            ), mock.patch.object(smoke, "terminate_process_tree") as terminate_mock:
                payload = smoke.run_packaged_smoke(args)
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["scenarios"], [failing_probe, failing_probe, failing_probe])
            self.assertEqual(payload["failure"]["step"], "runner")
            self.assertIn("Embedded Jobs Ready failed", payload["failure"]["message"])
            terminate_mock.assert_called_once_with(None)


if __name__ == "__main__":
    unittest.main()
