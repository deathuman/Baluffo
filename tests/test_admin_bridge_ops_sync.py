from unittest import mock

from scripts import admin_bridge
from tests.admin_bridge_ops_base import AdminBridgeOpsTestCase


class AdminBridgeOpsSyncTests(AdminBridgeOpsTestCase):
    def test_sync_status_reports_disabled_when_explicitly_disabled(self):
        admin_bridge.update_saved_sync_settings({"enabled": False})
        payload = admin_bridge.get_sync_status_payload()
        self.assertTrue(payload.get("ok"))
        self.assertEqual(str((payload.get("config") or {}).get("state") or ""), "disabled")

    def test_update_saved_sync_settings_persists_local_enablement_only(self):
        result = admin_bridge.update_saved_sync_settings({"enabled": True})
        self.assertTrue(bool(result.get("enabled")))
        saved = admin_bridge.load_saved_sync_settings()
        self.assertTrue(bool(saved.get("enabled")))
        payload = admin_bridge.get_sync_status_payload()
        saved_payload = payload.get("savedConfig") or {}
        self.assertTrue(bool(saved_payload.get("enabled")))
        self.assertEqual(str((payload.get("config") or {}).get("authMode") or ""), "github_app")

    def test_sync_pull_updates_local_registry_counts(self):
        admin_bridge.update_saved_sync_settings({"enabled": True})
        admin_bridge.save_json_atomic(admin_bridge.ACTIVE_PATH, [])
        admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, [])
        admin_bridge.save_json_atomic(admin_bridge.REJECTED_PATH, [])
        original_pull = admin_bridge.source_sync_module.pull_and_merge_sources
        try:
            admin_bridge.source_sync_module.pull_and_merge_sources = lambda _cfg, _state: {
                "changed": True,
                "remoteFound": True,
                "remoteSha": "abc",
                "mergedState": {
                    "active": [{"adapter": "static", "listing_url": "https://a.com/jobs"}],
                    "pending": [{"adapter": "teamtailor", "name": "Foo"}],
                    "rejected": [],
                },
            }
            result = admin_bridge.sync_pull_sources()
            self.assertTrue(result.get("ok"))
            self.assertTrue(result.get("changed"))
            summary = result.get("summary") or {}
            self.assertEqual(int(summary.get("activeCount") or 0), 1)
            self.assertEqual(int(summary.get("pendingCount") or 0), 1)
        finally:
            admin_bridge.source_sync_module.pull_and_merge_sources = original_pull

    def test_sync_push_serializes_expected_snapshot_counts(self):
        admin_bridge.update_saved_sync_settings({"enabled": True})
        admin_bridge.save_json_atomic(admin_bridge.ACTIVE_PATH, [{"adapter": "static", "listing_url": "https://a.com/jobs"}])
        admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, [{"adapter": "teamtailor", "name": "Foo"}])
        admin_bridge.save_json_atomic(admin_bridge.REJECTED_PATH, [{"adapter": "lever", "company": "Bar"}])
        original_push = admin_bridge.source_sync_module.push_sources_snapshot
        try:
            admin_bridge.source_sync_module.push_sources_snapshot = lambda _cfg, local_state: {
                "pushed": True,
                "remotePreviouslyExisted": True,
                "remoteSha": "newsha",
                "snapshot": admin_bridge.source_sync_module.build_snapshot(local_state),
            }
            result = admin_bridge.sync_push_sources()
            self.assertTrue(result.get("ok"))
            counts = result.get("counts") or {}
            self.assertEqual(int(counts.get("active") or 0), 1)
            self.assertEqual(int(counts.get("pending") or 0), 1)
            self.assertEqual(int(counts.get("rejected") or 0), 1)
        finally:
            admin_bridge.source_sync_module.push_sources_snapshot = original_push

    def test_start_sync_task_creates_started_history_row(self):
        admin_bridge.update_saved_sync_settings({"enabled": True})
        original_thread_cls = admin_bridge.threading.Thread
        try:
            class _NoStartThread:
                def __init__(self, target=None, args=(), kwargs=None, name=None, daemon=None):  # noqa: ANN001
                    self.target = target
                    self.args = args
                    self.kwargs = dict(kwargs or {})
                    self.name = name
                    self.daemon = daemon

                def start(self):
                    return None

            admin_bridge.threading.Thread = _NoStartThread
            result = admin_bridge.start_sync_task("pull")
            self.assertTrue(result.get("started"))
            self.assertEqual(str(result.get("task") or ""), "source_sync")
            self.assertEqual(str(result.get("action") or ""), "pull")
            rows = admin_bridge.load_run_history()
            started = [row for row in rows if str(row.get("type") or "") == "sync" and str(row.get("status") or "") == "started"]
            self.assertGreaterEqual(len(started), 1)
            self.assertEqual(str(((started[-1].get("summary") or {}).get("action") or "")), "pull")
        finally:
            admin_bridge.threading.Thread = original_thread_cls

    def test_sync_history_prunes_stale_started_rows_without_live_worker(self):
        admin_bridge.append_run_history({
            "id": "sync_stale_1",
            "type": "sync",
            "status": "started",
            "startedAt": "2026-03-09T12:00:00+00:00",
            "finishedAt": "",
            "durationMs": 0,
            "summary": {"action": "push"},
        })
        rows = admin_bridge.sync_history_from_reports()
        self.assertFalse(any(str(row.get("id") or "") == "sync_stale_1" for row in rows))

    def test_sync_worker_writes_completed_row_with_summary(self):
        admin_bridge.update_saved_sync_settings({"enabled": True})
        started_at = admin_bridge.now_iso()
        admin_bridge.append_run_history({
            "id": "sync_test_1",
            "type": "sync",
            "status": "started",
            "startedAt": started_at,
            "finishedAt": "",
            "durationMs": 0,
            "summary": {"action": "pull"},
        })
        original_pull = admin_bridge.sync_pull_sources
        try:
            admin_bridge.sync_pull_sources = lambda: {
                "ok": True,
                "changed": True,
                "remoteFound": True,
                "remoteSha": "abc123",
                "remoteGeneratedAt": "2026-03-09T10:00:00+00:00",
                "summary": {"activeCount": 1, "pendingCount": 2, "rejectedCount": 3},
            }
            admin_bridge._run_sync_task_worker("sync_test_1", "pull", started_at)  # noqa: SLF001
            rows = admin_bridge.load_run_history()
            finished = [row for row in rows if str(row.get("type") or "") == "sync" and str(row.get("finishedAt") or "")]
            self.assertGreaterEqual(len(finished), 1)
            last = finished[-1]
            self.assertEqual(str(last.get("status") or ""), "ok")
            summary = last.get("summary") or {}
            self.assertEqual(str(summary.get("action") or ""), "pull")
            self.assertEqual(int(summary.get("activeCount") or 0), 1)
            self.assertEqual(int(summary.get("pendingCount") or 0), 2)
            self.assertEqual(int(summary.get("rejectedCount") or 0), 3)
        finally:
            admin_bridge.sync_pull_sources = original_pull

    def test_sync_worker_failure_writes_error_row(self):
        started_at = admin_bridge.now_iso()
        admin_bridge.append_run_history({
            "id": "sync_test_err",
            "type": "sync",
            "status": "started",
            "startedAt": started_at,
            "finishedAt": "",
            "durationMs": 0,
            "summary": {"action": "push"},
        })
        original_push = admin_bridge.sync_push_sources
        try:
            def _boom():
                raise RuntimeError("network down")

            admin_bridge.sync_push_sources = _boom
            admin_bridge._run_sync_task_worker("sync_test_err", "push", started_at)  # noqa: SLF001
            rows = admin_bridge.load_run_history()
            finished = [row for row in rows if str(row.get("id") or "") == "sync_test_err" and str(row.get("finishedAt") or "")]
            self.assertEqual(len(finished), 1)
            self.assertEqual(str(finished[0].get("status") or ""), "error")
            self.assertIn("network down", str((finished[0].get("summary") or {}).get("error") or ""))
        finally:
            admin_bridge.sync_push_sources = original_push

    def test_wait_for_report_completion_ignores_stale_flag_until_report_finishes(self):
        started_at = admin_bridge.now_iso()
        finished_at = admin_bridge.now_iso()
        reports = [
            {"startedAt": started_at, "finishedAt": ""},
            {"startedAt": started_at, "finishedAt": finished_at},
        ]

        class _NoWaitEvent:
            def wait(self, _seconds):
                return None

        with (
            mock.patch.object(admin_bridge, "load_json_object", side_effect=reports),
            mock.patch.object(admin_bridge, "report_is_stale_in_progress", return_value=True),
            mock.patch.object(admin_bridge.threading, "Event", return_value=_NoWaitEvent()),
        ):
            result = admin_bridge._wait_for_report_completion(  # noqa: SLF001
                report_path=self.test_root / "source-discovery-report.json",
                started_at=started_at,
                timeout_s=10.0,
                report_name="discovery report",
            )
        self.assertEqual(str(result.get("finishedAt") or ""), finished_at)

    def test_wait_for_report_completion_can_fail_fast_when_stale_guard_enabled(self):
        started_at = admin_bridge.now_iso()
        reports = [{"startedAt": started_at, "finishedAt": ""}]

        with (
            mock.patch.object(admin_bridge, "load_json_object", side_effect=reports),
            mock.patch.object(admin_bridge, "report_is_stale_in_progress", return_value=True),
        ):
            with self.assertRaises(RuntimeError):
                admin_bridge._wait_for_report_completion(  # noqa: SLF001
                    report_path=self.test_root / "jobs-fetch-report.json",
                    started_at=started_at,
                    timeout_s=10.0,
                    report_name="fetch report",
                    fail_on_stale=True,
                )
