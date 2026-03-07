import tempfile
import unittest
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
        admin_bridge.MAX_HISTORY_ROWS = 5
        admin_bridge.save_json_atomic(admin_bridge.ACTIVE_PATH, [])
        admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, [])
        admin_bridge.save_json_atomic(admin_bridge.REJECTED_PATH, [])
        admin_bridge.save_json_atomic(admin_bridge.TASKS_CONFIG_PATH, {"tasks": []})

    def tearDown(self):
        for key, value in self._orig.items():
            setattr(admin_bridge, key, value)
        self.tmp.cleanup()

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


if __name__ == "__main__":
    unittest.main()
