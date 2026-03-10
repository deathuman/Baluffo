import json
import unittest
from pathlib import Path
from unittest import mock

from scripts import packaged_desktop_smoke as smoke
from tests.temp_paths import workspace_tmpdir


class PackagedDesktopSmokeTests(unittest.TestCase):
    def test_ensure_portable_exe_raises_when_missing_and_build_still_missing(self) -> None:
        with workspace_tmpdir("packaged-smoke") as tmp, mock.patch.object(smoke, "run_portable_build") as build_mock:
            exe_path = Path(tmp) / "dist" / "baluffo-portable" / "Baluffo.exe"
            with self.assertRaisesRegex(RuntimeError, "Packaged desktop executable not found"):
                smoke.ensure_portable_exe(exe_path, rebuild=False)
            build_mock.assert_called_once()

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
            terminate_mock.assert_called_once_with(process)
            stdout_handle.close.assert_called_once()
            stderr_handle.close.assert_called_once()

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
                ]
            )
            startup_metrics = [{"event": "desktop_site_ready"}]
            scenarios = [
                {"name": "Startup", "status": "passed", "durationMs": 200, "error": ""},
                {"name": "Auth continuity", "status": "passed", "durationMs": 300, "error": ""},
            ]
            with mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path), mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
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
            ), mock.patch.object(smoke, "terminate_process_tree") as terminate_mock:
                payload = smoke.run_packaged_smoke(args)
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["scenarios"], scenarios)
            self.assertEqual(payload["startupMetrics"], startup_metrics)
            self.assertTrue(report_path.exists())
            saved = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertTrue(saved["ok"])
            self.assertEqual(saved["artifacts"]["playwrightReport"], str(artifacts_dir / "playwright-report.json"))
            terminate_mock.assert_called_once_with(process)
            stdout_handle.close.assert_called_once()
            stderr_handle.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
