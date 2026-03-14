import json
import os
import shutil
import unittest
import uuid
from pathlib import Path

from scripts import admin_bridge


class AdminBridgeOpsTestCase(unittest.TestCase):
    def setUp(self):
        temp_root = Path(__file__).resolve().parents[2] / ".codex-tmp-tests"
        temp_root.mkdir(parents=True, exist_ok=True)
        self.test_root = temp_root / f"admin-bridge-{uuid.uuid4().hex}"
        self.test_root.mkdir(parents=True, exist_ok=True)
        root = self.test_root
        self._orig_sync_env = os.environ.get(admin_bridge.source_sync_module.PACKAGED_SYNC_CONFIG_ENV)
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
            "SYNC_CONFIG_PATH": admin_bridge.SYNC_CONFIG_PATH,
            "SYNC_RUNTIME_PATH": admin_bridge.SYNC_RUNTIME_PATH,
            "RUNTIME_CONFIG": admin_bridge.RUNTIME_CONFIG,
            "SYNC_CONFIG": admin_bridge.SYNC_CONFIG,
            "SYNC_STATUS": dict(admin_bridge.SYNC_STATUS),
            "ACTIVE_SYNC_RUNS": set(admin_bridge.ACTIVE_SYNC_RUNS),
            "ACTIVE_SYNC_THREADS": dict(admin_bridge.ACTIVE_SYNC_THREADS),
            "SOURCE_REGISTRY_DATA_DIR": admin_bridge.source_registry_module.DATA_DIR,
            "MAX_HISTORY_ROWS": admin_bridge.MAX_HISTORY_ROWS,
            "MAYBE_TRIGGER_AUTO_SYNC_PUSH": admin_bridge._maybe_trigger_auto_sync_push,  # noqa: SLF001
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
        admin_bridge.SYNC_CONFIG_PATH = root / "source-sync-config.json"
        admin_bridge.SYNC_RUNTIME_PATH = root / "source-sync-runtime.json"
        admin_bridge.MAX_HISTORY_ROWS = 5
        admin_bridge.save_json_atomic(admin_bridge.ACTIVE_PATH, [])
        admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, [])
        admin_bridge.save_json_atomic(admin_bridge.REJECTED_PATH, [])
        admin_bridge.save_json_atomic(admin_bridge.TASKS_CONFIG_PATH, {"tasks": []})
        packaged_sync_config = root / "github-app-sync-config.json"
        packaged_sync_config.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "appId": "123456",
                    "installationId": "999999",
                    "repo": "owner/repo",
                    "branch": "main",
                    "path": "baluffo/source-sync.json",
                    "privateKeyPem": "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----",
                }
            ),
            encoding="utf-8",
        )
        os.environ[admin_bridge.source_sync_module.PACKAGED_SYNC_CONFIG_ENV] = str(packaged_sync_config)
        admin_bridge.refresh_sync_config()
        admin_bridge._maybe_trigger_auto_sync_push = lambda _reason: False  # noqa: SLF001

    def tearDown(self):
        admin_bridge.wait_for_sync_tasks(timeout_s=2.0)
        for key, value in self._orig.items():
            if key == "SOURCE_REGISTRY_DATA_DIR":
                admin_bridge.source_registry_module.DATA_DIR = value
                continue
            if key == "SYNC_STATUS":
                admin_bridge.SYNC_STATUS = dict(value)
                continue
            if key == "ACTIVE_SYNC_RUNS":
                admin_bridge.ACTIVE_SYNC_RUNS = set(value)
                continue
            if key == "ACTIVE_SYNC_THREADS":
                admin_bridge.ACTIVE_SYNC_THREADS = dict(value)
                continue
            setattr(admin_bridge, key, value)
        if self._orig_sync_env is None:
            os.environ.pop(admin_bridge.source_sync_module.PACKAGED_SYNC_CONFIG_ENV, None)
        else:
            os.environ[admin_bridge.source_sync_module.PACKAGED_SYNC_CONFIG_ENV] = self._orig_sync_env
        shutil.rmtree(self.test_root, ignore_errors=True)