import tempfile
import os
import unittest
import io
import json
from contextlib import redirect_stdout
from pathlib import Path

from scripts import admin_bridge


class AdminBridgeOpsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self._orig = {
            "OPS_HISTORY_PATH": admin_bridge.OPS_HISTORY_PATH,
            "OPS_ALERT_STATE_PATH": admin_bridge.OPS_ALERT_STATE_PATH,
            "JOBS_FETCH_REPORT_PATH": admin_bridge.JOBS_FETCH_REPORT_PATH,
            "DISCOVERY_REPORT_PATH": admin_bridge.DISCOVERY_REPORT_PATH,
            "ACTIVE_PATH": admin_bridge.ACTIVE_PATH,
            "PENDING_PATH": admin_bridge.PENDING_PATH,
            "REJECTED_PATH": admin_bridge.REJECTED_PATH,
            "TASKS_CONFIG_PATH": admin_bridge.TASKS_CONFIG_PATH,
            "TASK_STATE_PATH": admin_bridge.TASK_STATE_PATH,
            "RUNTIME_CONFIG": admin_bridge.RUNTIME_CONFIG,
            "SOURCE_REGISTRY_DATA_DIR": admin_bridge.source_registry_module.DATA_DIR,
            "MAX_HISTORY_ROWS": admin_bridge.MAX_HISTORY_ROWS,
        }
        admin_bridge.OPS_HISTORY_PATH = root / "admin-run-history.json"
        admin_bridge.OPS_ALERT_STATE_PATH = root / "admin-alert-state.json"
        admin_bridge.JOBS_FETCH_REPORT_PATH = root / "jobs-fetch-report.json"
        admin_bridge.DISCOVERY_REPORT_PATH = root / "source-discovery-report.json"
        admin_bridge.ACTIVE_PATH = root / "source-registry-active.json"
        admin_bridge.PENDING_PATH = root / "source-registry-pending.json"
        admin_bridge.REJECTED_PATH = root / "source-registry-rejected.json"
        admin_bridge.TASKS_CONFIG_PATH = root / "tasks.json"
        admin_bridge.TASK_STATE_PATH = root / "admin-task-state.json"
        admin_bridge.MAX_HISTORY_ROWS = 5
        admin_bridge.save_json_atomic(admin_bridge.ACTIVE_PATH, [])
        admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, [])
        admin_bridge.save_json_atomic(admin_bridge.REJECTED_PATH, [])
        admin_bridge.save_json_atomic(admin_bridge.TASKS_CONFIG_PATH, {"tasks": []})

    def tearDown(self):
        for key, value in self._orig.items():
            if key == "SOURCE_REGISTRY_DATA_DIR":
                admin_bridge.source_registry_module.DATA_DIR = value
                continue
            setattr(admin_bridge, key, value)
        self.tmp.cleanup()

    def test_resolve_runtime_config_cli_env_precedence(self):
        cfg = admin_bridge.resolve_runtime_config(
            ["--port", "9001", "--host", "127.0.0.9", "--data-dir", self.tmp.name, "--log-format", "jsonl", "--log-level", "debug"],
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
        self.assertEqual(str(cfg.data_dir), str(Path(self.tmp.name).resolve()))
        self.assertEqual(cfg.log_format, "jsonl")
        self.assertEqual(cfg.log_level, "debug")

    def test_resolve_runtime_config_env_defaults_when_cli_missing(self):
        cfg = admin_bridge.resolve_runtime_config(
            [],
            env={
                "BALUFFO_BRIDGE_HOST": "0.0.0.0",
                "BALUFFO_BRIDGE_PORT": "9911",
                "BALUFFO_DATA_DIR": self.tmp.name,
                "BALUFFO_BRIDGE_LOG_FORMAT": "jsonl",
                "BALUFFO_BRIDGE_LOG_LEVEL": "debug",
            },
        )
        self.assertEqual(cfg.host, "0.0.0.0")
        self.assertEqual(cfg.port, 9911)
        self.assertEqual(str(cfg.data_dir), str(Path(self.tmp.name).resolve()))
        self.assertEqual(cfg.log_format, "jsonl")
        self.assertEqual(cfg.log_level, "debug")

    def test_bridge_log_jsonl_output_is_valid_json(self):
        cfg = admin_bridge.RuntimeConfig(
            root=Path(self.tmp.name),
            data_dir=Path(self.tmp.name),
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
        data_dir = Path(self.tmp.name) / "runtime-data"
        cfg = admin_bridge.RuntimeConfig(
            root=Path(self.tmp.name),
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
        self.assertIn("kpis", health)
        self.assertIn("alerts", health)
        self.assertGreaterEqual(len(health["alerts"]), 1)
        self.assertTrue(any(alert["id"] == "degraded_reliability" for alert in health["alerts"]))

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

    def test_add_manual_source_adds_and_deduplicates(self):
        added = admin_bridge.add_manual_source("https://example.teamtailor.com/jobs/")
        self.assertEqual(added["status"], "added")
        self.assertTrue(added.get("sourceId"))

        duplicate = admin_bridge.add_manual_source("https://example.teamtailor.com/jobs?utm=abc")
        self.assertEqual(duplicate["status"], "duplicate")
        self.assertEqual(
            str(duplicate.get("sourceId") or "").lower(),
            str(added.get("sourceId") or "").lower(),
        )

    def test_add_manual_source_rejects_invalid_url(self):
        invalid = admin_bridge.add_manual_source("not-a-url")
        self.assertEqual(invalid["status"], "invalid")

    def test_add_manual_source_uses_static_fallback_for_unsupported_provider(self):
        added = admin_bridge.add_manual_source("https://milestone.it/careers/")
        self.assertEqual(added["status"], "added")
        source = added.get("source") or {}
        self.assertEqual(str(source.get("adapter") or "").lower(), "static")
        self.assertEqual(source.get("pages"), ["https://milestone.it/careers"])
        self.assertIn("generic website scraping fallback", str(added.get("message") or "").lower())

    def test_add_manual_source_static_fallback_deduplicates_by_normalized_url(self):
        first = admin_bridge.add_manual_source("https://milestone.it/careers/")
        second = admin_bridge.add_manual_source("https://milestone.it/careers?utm=x")
        self.assertEqual(first["status"], "added")
        self.assertEqual(second["status"], "duplicate")
        self.assertEqual(
            str(first.get("sourceId") or "").lower(),
            str(second.get("sourceId") or "").lower(),
        )

    def test_trigger_source_check_returns_error_for_missing_source(self):
        result = admin_bridge.trigger_source_check("missing-source-id")
        self.assertFalse(result["started"])
        self.assertIn("not found", str(result["error"]).lower())

    def test_trigger_source_check_updates_pending_source_on_success(self):
        added = admin_bridge.add_manual_source("https://example.teamtailor.com/jobs")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_probe = admin_bridge.discovery.probe_candidate
        try:
            admin_bridge.discovery.probe_candidate = lambda *_args, **_kwargs: (True, 4, "")
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertEqual(result["jobsFound"], 4)
            pending = admin_bridge.load_json_array(admin_bridge.PENDING_PATH, [])
            updated = next((row for row in pending if admin_bridge.source_identity(row) == source_id.lower()), {})
            self.assertEqual(int(updated.get("jobsFound") or 0), 4)
            self.assertEqual(str(updated.get("lastProbeError") or ""), "")
        finally:
            admin_bridge.discovery.probe_candidate = original_probe

    def test_trigger_source_check_returns_failed_result_on_probe_error(self):
        added = admin_bridge.add_manual_source("https://another.teamtailor.com/jobs")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_probe = admin_bridge.discovery.probe_candidate
        try:
            admin_bridge.discovery.probe_candidate = lambda *_args, **_kwargs: (False, 0, "timeout")
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertFalse(result["ok"])
            self.assertIn("timeout", str(result["error"]).lower())
        finally:
            admin_bridge.discovery.probe_candidate = original_probe

    def test_trigger_source_check_reconstructs_greenhouse_api_url_when_missing(self):
        row = {
            "name": "Larian Studios",
            "studio": "Larian Studios",
            "adapter": "greenhouse",
            "slug": "larian-studios",
            "enabledByDefault": True,
        }
        row = admin_bridge.ensure_source_id(row)
        admin_bridge.save_json_atomic(admin_bridge.ACTIVE_PATH, [row])
        source_id = admin_bridge.source_identity(row)

        original_probe = admin_bridge.discovery.probe_candidate
        try:
            calls = {"count": 0}

            def fake_probe(candidate, *_args, **_kwargs):  # noqa: ANN001
                calls["count"] += 1
                if calls["count"] == 1:
                    return False, 0, "missing adapter or URL"
                self.assertEqual(
                    str(candidate.get("api_url") or ""),
                    "https://boards-api.greenhouse.io/v1/boards/larian-studios/jobs",
                )
                return True, 9, ""

            admin_bridge.discovery.probe_candidate = fake_probe
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertEqual(int(result["jobsFound"]), 9)
        finally:
            admin_bridge.discovery.probe_candidate = original_probe

    def test_registry_delete_removes_selected_ids_from_all_buckets(self):
        active_row = {
            "name": "Active Source",
            "studio": "Active Studio",
            "adapter": "static",
            "pages": ["https://active.example.com/careers"],
            "listing_url": "https://active.example.com/careers",
            "enabledByDefault": True,
        }
        pending_row = {
            "name": "Pending Source",
            "studio": "Pending Studio",
            "adapter": "static",
            "pages": ["https://pending.example.com/careers"],
            "listing_url": "https://pending.example.com/careers",
            "enabledByDefault": False,
        }
        rejected_row = {
            "name": "Rejected Source",
            "studio": "Rejected Studio",
            "adapter": "static",
            "pages": ["https://rejected.example.com/careers"],
            "listing_url": "https://rejected.example.com/careers",
            "enabledByDefault": False,
        }
        active_row = admin_bridge.ensure_source_id(active_row)
        pending_row = admin_bridge.ensure_source_id(pending_row)
        rejected_row = admin_bridge.ensure_source_id(rejected_row)
        admin_bridge.save_json_atomic(admin_bridge.ACTIVE_PATH, [active_row])
        admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, [pending_row])
        admin_bridge.save_json_atomic(admin_bridge.REJECTED_PATH, [rejected_row])

        state = admin_bridge.load_state()
        selected = {
            admin_bridge.source_identity(active_row),
            admin_bridge.source_identity(rejected_row),
        }
        before = len(state["active"]) + len(state["pending"]) + len(state["rejected"])
        state["active"] = [row for row in state["active"] if admin_bridge.source_identity(row) not in selected]
        state["pending"] = [row for row in state["pending"] if admin_bridge.source_identity(row) not in selected]
        state["rejected"] = [row for row in state["rejected"] if admin_bridge.source_identity(row) not in selected]
        state = admin_bridge.persist_state(state)
        after = len(state["active"]) + len(state["pending"]) + len(state["rejected"])

        self.assertEqual(before - after, 2)
        self.assertEqual(len(state["active"]), 0)
        self.assertEqual(len(state["pending"]), 1)
        self.assertEqual(len(state["rejected"]), 0)

    def test_registry_delete_can_match_by_url_fingerprint(self):
        pending_row = {
            "name": "Pending URL Match",
            "studio": "Pending URL Match",
            "adapter": "static",
            "pages": ["https://url-delete.example.com/careers/"],
            "listing_url": "https://url-delete.example.com/careers/",
            "enabledByDefault": False,
        }
        pending_row = admin_bridge.ensure_source_id(pending_row)
        admin_bridge.save_json_atomic(admin_bridge.ACTIVE_PATH, [])
        admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, [pending_row])
        admin_bridge.save_json_atomic(admin_bridge.REJECTED_PATH, [])

        state = admin_bridge.load_state()
        selected_urls = {"https://url-delete.example.com/careers"}

        def keep_row(row):
            row_id = admin_bridge.source_identity(row)
            row_url = admin_bridge.source_url_fingerprint(row)
            if row_id in set():
                return False
            if row_url and row_url in selected_urls:
                return False
            return True

        state["pending"] = [row for row in state["pending"] if keep_row(row)]
        state = admin_bridge.persist_state(state)
        self.assertEqual(len(state["pending"]), 0)

    def test_trigger_source_check_static_fallback_uses_generic_scrape(self):
        added = admin_bridge.add_manual_source("https://milestone.it/careers/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <a href="/jobs/engine-programmer">Engine Programmer</a>
        <script type="application/ld+json">
        {"@context":"https://schema.org","@type":"JobPosting","title":"Technical Artist","url":"https://milestone.it/jobs/technical-artist"}
        </script>
        """
        detail_html = """
        <script type="application/ld+json">
        {"@context":"https://schema.org","@type":"JobPosting","title":"Engine Programmer","url":"https://milestone.it/jobs/engine-programmer"}
        </script>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://milestone.it/careers":
                    return listing_html
                if url == "https://milestone.it/jobs/engine-programmer":
                    return detail_html
                raise RuntimeError(f"unexpected URL: {url}")

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertEqual(int(result["jobsFound"]), 2)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_returns_failure_when_no_jobs(self):
        added = admin_bridge.add_manual_source("https://milestone.it/careers/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: "<html><body>No jobs</body></html>"
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertFalse(result["ok"])
            self.assertIn("no job postings found", str(result.get("error") or "").lower())
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_detects_embedded_job_openings_module(self):
        added = admin_bridge.add_manual_source("https://www.avalanchestudios.com/careers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <script>
        window.__NUXT__={state:{},data:[{body:[{slice_type:"job_openings_module"}]}]}
        </script>
        """
        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_classifies_rendered_404_page(self):
        added = admin_bridge.add_manual_source("https://www.paradoxinteractive.com/careers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = "<html><head><title>404 Not Found - Paradox Interactive</title></head><body>missing</body></html>"
        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertFalse(result["ok"])
            self.assertEqual(str(result.get("errorCode") or ""), "not_found")
            self.assertTrue(bool(result.get("suggestedUrls")))
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_attempts_browser_on_403(self):
        added = admin_bridge.add_manual_source("https://careers.rebellion.com/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        original_browser_fetch = admin_bridge._try_fetch_with_playwright
        try:
            def fake_fetch(_url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                raise RuntimeError("HTTP Error 403: Forbidden")

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            admin_bridge._try_fetch_with_playwright = lambda *_args, **_kwargs: ('<a href="/jobs/gameplay-programmer">Role</a>', "")
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertEqual(int(result["jobsFound"]), 1)
            self.assertTrue(bool(result.get("browserFallbackAttempted")))
            self.assertTrue(bool(result.get("browserFallbackUsed")))
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch
            admin_bridge._try_fetch_with_playwright = original_browser_fetch

    def test_trigger_source_check_static_fallback_reports_unavailable_browser_fallback(self):
        added = admin_bridge.add_manual_source("https://careers.rebellion.com/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        original_browser_fetch = admin_bridge._try_fetch_with_playwright
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("HTTP Error 403: Forbidden")
            )
            admin_bridge._try_fetch_with_playwright = lambda *_args, **_kwargs: ("", "browser fallback unavailable (playwright is not installed)")
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertFalse(result["ok"])
            self.assertEqual(str(result.get("errorCode") or ""), "browser_fallback_unavailable")
            self.assertTrue(bool(result.get("browserFallbackAttempted")))
            self.assertFalse(bool(result.get("browserFallbackUsed")))
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch
            admin_bridge._try_fetch_with_playwright = original_browser_fetch

    def test_trigger_source_check_static_fallback_returns_404_hints(self):
        added = admin_bridge.add_manual_source("https://www.king.com/careers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("HTTP Error 404: Not Found"))
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertFalse(result["ok"])
            self.assertEqual(str(result.get("errorCode") or ""), "not_found")
            suggested = result.get("suggestedUrls") or []
            self.assertIn("https://careers.king.com", suggested)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_retries_suggested_alternate_on_404(self):
        added = admin_bridge.add_manual_source("https://www.fatsharkgames.com/career")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://www.fatsharkgames.com/career":
                    raise RuntimeError("HTTP Error 404: Not Found")
                if url == "https://jobs.fatsharkgames.com":
                    return '<a href="https://jobs.fatsharkgames.com/jobs/senior-programmer">Role</a>'
                raise RuntimeError(f"unexpected URL: {url}")

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_uses_parent_redirect_candidates_on_404(self):
        added = admin_bridge.add_manual_source("https://www.fatsharkgames.com/career")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        original_redirect = admin_bridge._discover_redirect_career_candidates
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://www.fatsharkgames.com/career":
                    raise RuntimeError("HTTP Error 404: Not Found")
                if url == "https://jobs.fatsharkgames.com":
                    return '<a href="https://jobs.fatsharkgames.com/jobs/network-programmer">Role</a>'
                raise RuntimeError(f"unexpected URL: {url}")

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            admin_bridge._discover_redirect_career_candidates = lambda *_args, **_kwargs: ["https://jobs.fatsharkgames.com"]
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch
            admin_bridge._discover_redirect_career_candidates = original_redirect

    def test_trigger_source_check_static_fallback_returns_ssl_error_code(self):
        added = admin_bridge.add_manual_source("https://careers.11bitstudios.com/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("SSL: CERTIFICATE_VERIFY_FAILED hostname mismatch")
            )
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertFalse(result["ok"])
            self.assertEqual(str(result.get("errorCode") or ""), "ssl_error")
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_extracts_intervieweb_links(self):
        added = admin_bridge.add_manual_source("https://milestone.it/careers/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        page_html = """
        <script src="https://cezanneondemand.intervieweb.it/integration/announces_js.php?lang=en&utype=0&k=abc123&LAC=milestone&d=milestone.it&annType=published&view=list&defgroup=name&gnavenable=1&desc=1&typeView=large"></script>
        """
        iframe_html = """
        <a href="https://cezanneondemand.intervieweb.it/app.php?opmode=guest&module=iframeAnnunci&act1=1&IdAnnuncio=60982&lang=en">Job A</a>
        <a href="https://cezanneondemand.intervieweb.it/app.php?opmode=guest&module=iframeAnnunci&act1=1&IdAnnuncio=61104&lang=en">Job B</a>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://milestone.it/careers":
                    return page_html
                if "module=iframeAnnunci" in url and "act1=23" in url:
                    return iframe_html
                raise RuntimeError(f"unexpected URL: {url}")

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertEqual(int(result["jobsFound"]), 2)
            self.assertTrue(bool(result.get("weakSignal")))
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_accepts_careers_role_links(self):
        added = admin_bridge.add_manual_source("https://www.naconstudiomilan.com/careers/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <a href="/careers/">Careers</a>
        <a href="/careers-category/design/">Design category</a>
        <a href="/careers/gameplay-designer/">Gameplay Designer</a>
        <a href="/careers/gameplay-programmer/">Gameplay Programmer</a>
        <a href="/careers/ai-programmer/">AI Programmer</a>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertEqual(int(result["jobsFound"]), 3)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_detects_textual_apply_role_signals(self):
        added = admin_bridge.add_manual_source("https://www.4a-games.com.mt/careers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <html><body>
        <h1>We're hiring for multiple projects</h1>
        <p>Senior Gameplay Programmer</p><button>Apply now</button>
        <p>Lead Technical Artist</p><button>Apply now</button>
        <p>Animation Programmer</p><button>Apply now</button>
        <p>QA Tester</p><button>Apply now</button>
        </body></html>
        """
        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_detects_crytek_like_embedded_links(self):
        added = admin_bridge.add_manual_source("https://www.crytek.com/career")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <script>
        self.__next_f.push([1,"{\\"leverInitialData\\":{\\"postings\\":[{\\"hosted_url\\":\\"https://jobs.lever.co/crytek/abc123\\"}]}}"]);
        </script>
        <a href="/career/posting/0cb503b8-53c9-4932-b0d1-8864e75deed8">Posting</a>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 2)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_extracts_embedded_relative_career_links(self):
        added = admin_bridge.add_manual_source("https://www.4a-games.com.mt/careers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <script>
        window.__PAGE_DATA__ = {"jobs":["/careers/senior-gameplay-programmer","/careers/lead-technical-artist"]};
        </script>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 2)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_detects_smartrecruiters_embedded_url(self):
        added = admin_bridge.add_manual_source("https://www.cdprojektred.com/en/jobs")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <script>
        var data = {"jobs":["https://jobs.smartrecruiters.com/CDPROJEKTRED/743999834254914-spontaneous-application"]};
        </script>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_counts_personio_search_json(self):
        added = admin_bridge.add_manual_source("https://yager.de/careers/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <script src="https://assets.cdn.personio.de/jobs/v2/min/js/jobs_list.bed3abfdd85796686e20.js"></script>
        <a href="https://yager.jobs.personio.de/">Jobs board</a>
        """
        personio_search_json = '{"data":[{"id":1},{"id":2}]}'

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://yager.de/careers":
                    return listing_html
                if url == "https://yager.jobs.personio.de/search.json":
                    return personio_search_json
                if url == "https://yager.jobs.personio.de":
                    return "<html>Personio Board</html>"
                return "<html></html>"

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 2)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_detects_jobylon_embed(self):
        added = admin_bridge.add_manual_source("https://www.remedygames.com/careers/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <div id="jobylon-jobs-widget"></div>
        <script>
        var jbl_company_id = 2986;
        var jbl_version = 'v2';
        var jbl_page_size = 30;
        var el = document.createElement('script');
        el.src = 'https://cdn.jobylon.com/embedder.js';
        </script>
        """
        embed_html = "<html><body>Jobylon widget</body></html>"

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://www.remedygames.com/careers":
                    return listing_html
                if "cdn.jobylon.com/jobs/companies/2986/embed/v2/" in url:
                    return embed_html
                return "<html></html>"

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_accepts_join_role_links(self):
        added = admin_bridge.add_manual_source("https://www.guerrilla-games.com/join")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <a href="/join/senior-technical-animator/5778235004">Senior Technical Animator</a>
        <a href="/join?page=2#postings">Pager</a>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_accepts_open_positions_links(self):
        added = admin_bridge.add_manual_source("https://www.rovio.com/careers/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        careers_html = '<a href="/open-positions/">Open Positions</a>'
        open_positions_html = '<a href="/open-positions/game-developer-abc/">Game Developer</a>'

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://www.rovio.com/careers":
                    return careers_html
                if url == "https://www.rovio.com/open-positions":
                    return open_positions_html
                return "<html></html>"

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_accepts_job_offers_links(self):
        added = admin_bridge.add_manual_source("https://techland.net/job-offers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = '<a href="/job-offers/senior-engine-programmer">Senior Engine Programmer</a>'

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_accepts_vacancy_links(self):
        added = admin_bridge.add_manual_source("https://www.playground-games.com/careers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = '<a href="/vacancy/25">Senior Animator</a>'

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_accepts_vacancies_slug_links(self):
        added = admin_bridge.add_manual_source("https://careers.sega.co.uk/vacancies")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <a href="/vacancies">Vacancies</a>
        <a href="/vacancies/lead-environment-artist">Lead Environment Artist</a>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_counts_workable_widget_jobs(self):
        added = admin_bridge.add_manual_source("https://team17.com/careers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = '<a href="https://apply.workable.com/team-17-digital/">Open roles</a>'
        workable_json = '{"jobs":[{"id":1},{"id":2},{"id":3}]}'

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://team17.com/careers":
                    return listing_html
                if url == "https://apply.workable.com/api/v1/widget/accounts/team-17-digital?details=true":
                    return workable_json
                return "<html></html>"

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 3)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_normalizes_placeholder_studio_name(self):
        pending_row = {
            "name": "Www (Manual Website)",
            "studio": "Www",
            "company": "Www",
            "adapter": "static",
            "pages": ["https://www.naconstudiomilan.com/careers/"],
            "listing_url": "https://www.naconstudiomilan.com/careers/",
            "enabledByDefault": False,
            "id": "static:listing_url:https://www.naconstudiomilan.com/careers",
        }
        admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, [pending_row])
        source_id = str(pending_row["id"])

        listing_html = '<a href="/careers/gameplay-designer/">Gameplay Designer</a>'
        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            pending = admin_bridge.load_json_array(admin_bridge.PENDING_PATH, [])
            updated = next((row for row in pending if admin_bridge.source_identity(row) == source_id), {})
            self.assertEqual(str(updated.get("studio") or ""), "Nacon Studio Milan")
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_load_state_normalizes_legacy_static_www_studio(self):
        pending_row = {
            "name": "Www (Manual Website)",
            "studio": "Www",
            "company": "Www",
            "adapter": "static",
            "pages": ["https://www.nixxes.com/jobs"],
            "listing_url": "https://www.nixxes.com/jobs",
            "enabledByDefault": False,
        }
        admin_bridge.save_json_atomic(admin_bridge.ACTIVE_PATH, [])
        admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, [pending_row])
        admin_bridge.save_json_atomic(admin_bridge.REJECTED_PATH, [])
        state = admin_bridge.load_state()
        row = state["pending"][0]
        self.assertEqual(str(row.get("studio") or ""), "Nixxes")
        self.assertEqual(str(row.get("name") or ""), "Nixxes (Manual Website)")


if __name__ == "__main__":
    unittest.main()
