import io
import json
import os
from contextlib import redirect_stdout
from unittest import mock

from scripts import admin_bridge
from tests.admin_bridge_ops_base import AdminBridgeOpsTestCase


class AdminBridgeOpsRuntimeTests(AdminBridgeOpsTestCase):
    def test_resolve_runtime_config_cli_env_precedence(self):
        cfg = admin_bridge.resolve_runtime_config(
            ["--port", "9001", "--host", "127.0.0.9", "--data-dir", str(self.test_root), "--log-format", "jsonl", "--log-level", "debug"],
            env={
                "BALUFFO_BRIDGE_HOST": "1.2.3.4",
                "BALUFFO_BRIDGE_PORT": "9999",
                "BALUFFO_DATA_DIR": "C:\\should-not-win",
                "BALUFFO_BRIDGE_LOG_FORMAT": "human",
                "BALUFFO_BRIDGE_LOG_LEVEL": "info",
            },
        )
        self.assertEqual(cfg.host, "127.0.0.9")
        self.assertEqual(cfg.port, 9001)
        self.assertEqual(str(cfg.data_dir), str(self.test_root.resolve()))
        self.assertEqual(cfg.log_format, "jsonl")
        self.assertEqual(cfg.log_level, "debug")

    def test_resolve_runtime_config_env_defaults_when_cli_missing(self):
        with mock.patch.object(admin_bridge, "get_bridge_defaults", return_value={
            "host": "127.0.0.2",
            "port": 8878,
            "log_format": "human",
            "log_level": "info",
            "quiet_requests": False,
        }), mock.patch.object(admin_bridge, "get_storage_defaults", return_value={"data_dir": self.test_root / "from-file"}):
            cfg = admin_bridge.resolve_runtime_config(
                [],
                env={
                    "BALUFFO_BRIDGE_HOST": "0.0.0.0",
                    "BALUFFO_BRIDGE_PORT": "9911",
                    "BALUFFO_DATA_DIR": str(self.test_root),
                    "BALUFFO_BRIDGE_LOG_FORMAT": "jsonl",
                    "BALUFFO_BRIDGE_LOG_LEVEL": "debug",
                },
            )
        self.assertEqual(cfg.host, "0.0.0.0")
        self.assertEqual(cfg.port, 9911)
        self.assertEqual(str(cfg.data_dir), str(self.test_root.resolve()))
        self.assertEqual(cfg.log_format, "jsonl")
        self.assertEqual(cfg.log_level, "debug")

    def test_resolve_runtime_config_uses_file_defaults_when_env_missing(self):
        with mock.patch.object(admin_bridge, "get_bridge_defaults", return_value={
            "host": "127.0.0.5",
            "port": 9915,
            "log_format": "jsonl",
            "log_level": "debug",
            "quiet_requests": True,
        }), mock.patch.object(admin_bridge, "get_storage_defaults", return_value={"data_dir": self.test_root / "from-file"}):
            cfg = admin_bridge.resolve_runtime_config([], env={})
        self.assertEqual(cfg.host, "127.0.0.5")
        self.assertEqual(cfg.port, 9915)
        self.assertEqual(str(cfg.data_dir), str((self.test_root / "from-file").resolve()))
        self.assertEqual(cfg.log_format, "jsonl")
        self.assertEqual(cfg.log_level, "debug")
        self.assertTrue(cfg.quiet_requests)

    def test_bridge_log_jsonl_output_is_valid_json(self):
        cfg = admin_bridge.RuntimeConfig(
            root=self.test_root,
            data_dir=self.test_root,
            host="127.0.0.1",
            port=8877,
            log_format="jsonl",
            log_level="info",
            quiet_requests=False,
        )
        admin_bridge.configure_runtime_paths(cfg)
        buf = io.StringIO()
        with redirect_stdout(buf):
            admin_bridge.bridge_log("info", "hello_bridge", runId="abc123")
        line = buf.getvalue().strip()
        payload = json.loads(line)
        self.assertEqual(str(payload.get("message") or ""), "hello_bridge")
        self.assertEqual(str(payload.get("runId") or ""), "abc123")
        self.assertEqual(str(payload.get("level") or ""), "info")

    def test_configure_runtime_paths_updates_bridge_paths(self):
        data_dir = self.test_root / "runtime-data"
        cfg = admin_bridge.RuntimeConfig(
            root=self.test_root,
            data_dir=data_dir,
            host="127.0.0.1",
            port=8877,
            log_format="human",
            log_level="info",
            quiet_requests=False,
        )
        admin_bridge.configure_runtime_paths(cfg)
        self.assertEqual(admin_bridge.ACTIVE_PATH, data_dir / "source-registry-active.json")
        self.assertEqual(admin_bridge.TASK_STATE_PATH, data_dir / "admin-task-state.json")
        self.assertEqual(admin_bridge.source_registry_module.DATA_DIR, data_dir.resolve())

    def test_append_run_history_enforces_limit(self):
        for idx in range(8):
            admin_bridge.append_run_history({
                "type": "fetch",
                "status": "ok",
                "startedAt": f"2026-03-01T0{idx}:00:00+00:00",
                "finishedAt": f"2026-03-01T0{idx}:05:00+00:00",
                "durationMs": 300000,
                "summary": {"outputCount": idx + 1, "failedSources": 0, "sourceCount": 1},
            })
        rows = admin_bridge.load_run_history()
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[-1]["summary"]["outputCount"], 8)

    def test_compute_ops_health_reports_alerts(self):
        admin_bridge.save_json_atomic(admin_bridge.JOBS_FETCH_REPORT_PATH, {
            "startedAt": "2026-03-01T00:00:00+00:00",
            "finishedAt": "2026-03-01T00:10:00+00:00",
            "summary": {"outputCount": 100, "failedSources": 3, "sourceCount": 4},
            "sources": [],
        })
        health = admin_bridge.compute_ops_health()
        self.assertEqual(health["service"], "baluffo-bridge")
        self.assertIn("desktopMode", health)
        self.assertEqual(bool(health["desktopMode"]), bool(admin_bridge.RUNTIME_CONFIG.desktop_mode))
        self.assertIn("kpis", health)
        self.assertIn("alerts", health)
        self.assertGreaterEqual(len(health["alerts"]), 1)
        self.assertTrue(any(alert["id"] == "degraded_reliability" for alert in health["alerts"]))

    def test_compute_ops_health_includes_social_alerts(self):
        admin_bridge.save_json_atomic(admin_bridge.JOBS_FETCH_REPORT_PATH, {
            "startedAt": "2026-03-01T00:00:00+00:00",
            "finishedAt": "2026-03-01T00:10:00+00:00",
            "summary": {"outputCount": 20, "failedSources": 0, "sourceCount": 3},
            "sources": [
                {"name": "social_reddit", "status": "error", "fetchedCount": 30, "keptCount": 0, "lowConfidenceDropped": 70},
                {"name": "social_x", "status": "error", "fetchedCount": 20, "keptCount": 0, "lowConfidenceDropped": 60},
                {"name": "social_mastodon", "status": "ok", "fetchedCount": 20, "keptCount": 0, "lowConfidenceDropped": 20},
            ],
        })
        health = admin_bridge.compute_ops_health()
        ids = {str(row.get("id") or "") for row in health.get("alerts", [])}
        self.assertIn("social_sources_failing", ids)
        self.assertIn("social_zero_matches", ids)
        self.assertIn("social_low_confidence_spike", ids)

    def test_normalize_fetch_report_contract_sanitizes_minimal_payload(self):
        payload = admin_bridge.normalize_fetch_report_contract({
            "schemaVersion": "1.0",
            "startedAt": 123,
            "finishedAt": None,
            "summary": "bad",
            "sources": [{"name": "x", "status": "OK", "durationMs": "17"}],
        })
        self.assertEqual(int(payload.get("schemaVersion") or 0), 1)
        self.assertEqual(str(payload.get("startedAt") or ""), "123")
        self.assertEqual(str(payload.get("finishedAt") or ""), "")
        self.assertIsInstance(payload.get("summary"), dict)
        self.assertIsInstance(payload.get("runtime"), dict)
        self.assertEqual(len(payload.get("sources") or []), 1)
        row = payload["sources"][0]
        self.assertEqual(str(row.get("status") or ""), "ok")
        self.assertEqual(int(row.get("durationMs") or 0), 17)

    def test_normalize_fetch_report_contract_parses_stringified_detail_rows(self):
        payload = admin_bridge.normalize_fetch_report_contract({
            "sources": [
                {
                    "name": "lever_sources",
                    "status": "ok",
                    "details": [
                        "{'adapter': 'lever', 'studio': 'Jagex', 'name': 'Jagex (Lever)', 'status': 'ok', 'fetchedCount': 2, 'keptCount': 2, 'error': ''}"
                    ],
                }
            ]
        })
        self.assertEqual(len(payload.get("sources") or []), 1)
        row = payload["sources"][0]
        details = row.get("details") or []
        self.assertEqual(len(details), 1)
        self.assertEqual(str(details[0].get("name") or ""), "Jagex (Lever)")
        self.assertEqual(str(details[0].get("status") or ""), "ok")
        self.assertEqual(int(details[0].get("keptCount") or 0), 2)

    def test_normalize_discovery_report_contract_derives_queued_count_from_candidates(self):
        payload = admin_bridge.normalize_discovery_report_contract({
            "summary": {"queuedCandidateCount": 0, "probedCandidateCount": 4},
            "candidates": [
                {"name": "A", "deferred": False},
                {"name": "B"},
                {"name": "C", "deferred": True},
            ],
        })
        self.assertEqual(int((payload.get("summary") or {}).get("queuedCandidateCount") or 0), 2)

    def test_summarize_discovery_report_prefers_derived_queued_count(self):
        summary, status = admin_bridge.summarize_discovery_report({
            "startedAt": "2026-03-01T00:00:00+00:00",
            "finishedAt": "2026-03-01T00:01:00+00:00",
            "summary": {"queuedCandidateCount": 0, "failedProbeCount": 0, "probedCandidateCount": 2},
            "candidates": [
                {"name": "A"},
                {"name": "B", "deferred": False},
                {"name": "C", "deferred": True},
            ],
        })
        self.assertEqual(int(summary.get("queuedCandidateCount") or 0), 2)
        self.assertEqual(status, "ok")

    def test_build_fetcher_args_retry_failed_is_deterministic_and_filters_unknown(self):
        admin_bridge.save_json_atomic(admin_bridge.JOBS_FETCH_REPORT_PATH, {
            "startedAt": "2026-03-01T00:00:00+00:00",
            "finishedAt": "2026-03-01T00:10:00+00:00",
            "summary": {"failedSources": 3, "sourceCount": 4},
            "sources": [
                {"name": "remote_ok", "status": "error"},
                {"name": "unknown_custom_source", "status": "error"},
                {"name": "google_sheets", "status": "error"},
                {"name": "remote_ok", "status": "error"},
            ],
        })
        args, preset = admin_bridge.build_fetcher_args_from_payload({"preset": "retry_failed"})
        self.assertEqual(preset, "retry_failed")
        self.assertIn("--only-sources", args)
        idx = args.index("--only-sources")
        self.assertEqual(args[idx + 1], "google_sheets,remote_ok")
        self.assertIn("--ignore-circuit-breaker", args)
        self.assertIn("--quiet", args)
        self.assertIn("--fetch-strategy", args)
        self.assertIn("--adapter-http-concurrency", args)

    def test_build_fetcher_args_retry_failed_omits_only_sources_when_no_known_failures(self):
        admin_bridge.save_json_atomic(admin_bridge.JOBS_FETCH_REPORT_PATH, {
            "sources": [
                {"name": "unknown_custom_source_a", "status": "error"},
                {"name": "unknown_custom_source_b", "status": "error"},
            ]
        })
        args, preset = admin_bridge.build_fetcher_args_from_payload({"preset": "retry_failed"})
        self.assertEqual(preset, "retry_failed")
        self.assertNotIn("--only-sources", args)
        self.assertIn("--ignore-circuit-breaker", args)
        self.assertIn("--quiet", args)

    def test_build_fetcher_args_accepts_cadence_and_strategy_overrides(self):
        args, preset = admin_bridge.build_fetcher_args_from_payload({
            "preset": "default",
            "fetchStrategy": "http",
            "adapterHttpConcurrency": 48,
            "respectSourceCadence": True,
            "hotSourceCadenceMinutes": 20,
            "coldSourceCadenceMinutes": 90,
        })
        self.assertEqual(preset, "default")
        self.assertIn("--fetch-strategy", args)
        self.assertEqual(args[args.index("--fetch-strategy") + 1], "http")
        self.assertIn("--adapter-http-concurrency", args)
        self.assertEqual(args[args.index("--adapter-http-concurrency") + 1], "48")
        self.assertIn("--respect-source-cadence", args)
        self.assertIn("--hot-source-cadence-minutes", args)
        self.assertEqual(args[args.index("--hot-source-cadence-minutes") + 1], "20")
        self.assertIn("--cold-source-cadence-minutes", args)
        self.assertEqual(args[args.index("--cold-source-cadence-minutes") + 1], "90")

    def test_sync_history_from_reports_prunes_stale_started_rows_when_report_stuck(self):
        old_started = "2026-03-01T00:00:00+00:00"
        admin_bridge.save_json_atomic(admin_bridge.OPS_HISTORY_PATH, [
            {
                "type": "fetch",
                "status": "started",
                "startedAt": old_started,
                "finishedAt": "",
                "durationMs": 0,
                "summary": {},
            }
        ])
        admin_bridge.save_json_atomic(admin_bridge.JOBS_FETCH_REPORT_PATH, {
            "startedAt": old_started,
            "finishedAt": "",
            "summary": {"outputCount": 0, "failedSources": 0, "sourceCount": 0},
            "sources": [],
        })
        old_ts = 1_700_000_000
        os.utime(admin_bridge.JOBS_FETCH_REPORT_PATH, (old_ts, old_ts))
        rows = admin_bridge.sync_history_from_reports()
        started_rows = [row for row in rows if str(row.get("status") or "").lower() == "started"]
        self.assertEqual(started_rows, [])

    def test_infer_studio_name_from_host_skips_www_and_splits_studio_token(self):
        studio = admin_bridge.infer_studio_name_from_host("https://www.naconstudiomilan.com/careers/")
        self.assertEqual(studio, "Nacon Studio Milan")

    def test_infer_studio_name_from_host_skips_short_placeholder_subdomain(self):
        studio = admin_bridge.infer_studio_name_from_host("https://w.nixxes.com/jobs")
        self.assertEqual(studio, "Nixxes")

    def test_alert_ack_suppresses_visible_alert(self):
        admin_bridge.save_json_atomic(admin_bridge.JOBS_FETCH_REPORT_PATH, {
            "startedAt": "2026-03-01T00:00:00+00:00",
            "finishedAt": "2026-03-01T00:10:00+00:00",
            "summary": {"outputCount": 100, "failedSources": 3, "sourceCount": 4},
            "sources": [],
        })
        initial = admin_bridge.compute_ops_health()
        alert_ids = [row["id"] for row in initial.get("alerts", [])]
        self.assertIn("degraded_reliability", alert_ids)
        state = admin_bridge.load_alert_state()
        state["acked"]["degraded_reliability"] = admin_bridge.now_iso()
        admin_bridge.save_alert_state(state)
        updated = admin_bridge.compute_ops_health()
        updated_ids = [row["id"] for row in updated.get("alerts", [])]
        self.assertNotIn("degraded_reliability", updated_ids)

    def test_run_background_script_uses_child_script_mode_when_frozen(self):
        cfg = admin_bridge.RuntimeConfig(
            root=self.test_root,
            data_dir=self.test_root,
            host="127.0.0.1",
            port=8877,
            log_format="human",
            log_level="info",
            quiet_requests=True,
            desktop_mode=True,
        )
        admin_bridge.configure_runtime_paths(cfg)
        fake_proc = type("FakeProc", (), {"pid": 12345})()
        with mock.patch.object(admin_bridge.sys, "frozen", True, create=True), mock.patch.object(
            admin_bridge.sys, "executable", "C:/tmp/Baluffo.exe"
        ), mock.patch.object(admin_bridge.subprocess, "Popen", return_value=fake_proc) as popen_mock:
            admin_bridge.run_background_script("source_discovery.py", ["--mode", "dynamic"])
        command = popen_mock.call_args.args[0]
        self.assertEqual(command[:5], ["C:/tmp/Baluffo.exe", "__child_script__", "--root", str(self.test_root), "--script"])
        self.assertIn("source_discovery.py", command)
        self.assertEqual(command[-2:], ["--mode", "dynamic"])
