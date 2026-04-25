import json
from pathlib import Path

import pytest

from src import admin_bridge
from src.bridge import ACTIVE_SYNC_RUNS, ACTIVE_SYNC_THREADS, SYNC_STATE_LOCK


@pytest.fixture()
def admin_bridge_entrypoint_root(make_test_root, monkeypatch) -> Path:
    """Entry-point level admin_bridge fixture for module/singleton patch tests."""
    root = make_test_root("admin-bridge")

    monkeypatch.setattr(admin_bridge, "OPS_HISTORY_PATH", root / "admin-run-history.json")
    monkeypatch.setattr(admin_bridge, "OPS_ALERT_STATE_PATH", root / "admin-alert-state.json")
    monkeypatch.setattr(admin_bridge, "JOBS_FETCH_REPORT_PATH", root / "jobs-fetch-report.json")
    monkeypatch.setattr(
        admin_bridge, "DISCOVERY_REPORT_PATH", root / "source-discovery-report.json"
    )
    monkeypatch.setattr(
        admin_bridge,
        "DISCOVERY_CANDIDATES_PATH",
        root / "source-discovery-candidates.json",
    )
    monkeypatch.setattr(admin_bridge, "APPROVAL_STATE_PATH", root / "source-approval-state.json")
    monkeypatch.setattr(
        admin_bridge.source_registry_module,
        "DISCOVERY_CANDIDATES_PATH",
        root / "source-discovery-candidates.json",
    )
    monkeypatch.setattr(
        admin_bridge.source_registry_module,
        "APPROVAL_STATE_PATH",
        root / "source-approval-state.json",
    )
    monkeypatch.setattr(admin_bridge, "ACTIVE_PATH", root / "source-registry-active.json")
    monkeypatch.setattr(admin_bridge, "PENDING_PATH", root / "source-registry-pending.json")
    monkeypatch.setattr(admin_bridge, "REJECTED_PATH", root / "source-registry-rejected.json")
    monkeypatch.setattr(admin_bridge, "TASKS_CONFIG_PATH", root / "tasks.json")
    monkeypatch.setattr(admin_bridge, "TASK_STATE_PATH", root / "admin-task-state.json")
    monkeypatch.setattr(admin_bridge, "SYNC_CONFIG_PATH", root / "source-sync-config.json")
    monkeypatch.setattr(admin_bridge, "SYNC_RUNTIME_PATH", root / "source-sync-runtime.json")
    monkeypatch.setattr(admin_bridge, "MAX_HISTORY_ROWS", 5)

    admin_bridge.save_json_atomic(admin_bridge.ACTIVE_PATH, [])
    admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, [])
    admin_bridge.save_json_atomic(admin_bridge.REJECTED_PATH, [])
    admin_bridge.save_json_atomic(admin_bridge.TASKS_CONFIG_PATH, {"tasks": []})
    with SYNC_STATE_LOCK:
        ACTIVE_SYNC_RUNS.clear()
        ACTIVE_SYNC_THREADS.clear()
    monkeypatch.setattr(admin_bridge, "_SYNC_SERVICE", None)
    monkeypatch.setattr(admin_bridge, "_SYNC_SERVICE_DATA_DIR", None)
    monkeypatch.setattr(admin_bridge, "_REGISTRY_SERVICE", None)
    monkeypatch.setattr(admin_bridge, "_REGISTRY_SERVICE_PATHS", None)
    monkeypatch.setattr(admin_bridge, "_DISCOVERY_SERVICE", None)
    monkeypatch.setattr(admin_bridge, "_DISCOVERY_SERVICE_PATHS", None)
    monkeypatch.setattr(admin_bridge, "_PIPELINE_SERVICE", None)

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
    monkeypatch.setenv(
        admin_bridge.source_sync_module.PACKAGED_SYNC_CONFIG_ENV, str(packaged_sync_config)
    )
    admin_bridge.refresh_sync_config()

    monkeypatch.setattr(admin_bridge, "_maybe_trigger_auto_sync_push", lambda _reason: False)

    yield root

    admin_bridge.wait_for_sync_tasks(timeout_s=2.0)
    with SYNC_STATE_LOCK:
        ACTIVE_SYNC_RUNS.clear()
        ACTIVE_SYNC_THREADS.clear()
    monkeypatch.setattr(admin_bridge, "_SYNC_SERVICE", None)
    monkeypatch.setattr(admin_bridge, "_SYNC_SERVICE_DATA_DIR", None)
    monkeypatch.setattr(admin_bridge, "_REGISTRY_SERVICE", None)
    monkeypatch.setattr(admin_bridge, "_REGISTRY_SERVICE_PATHS", None)
    monkeypatch.setattr(admin_bridge, "_DISCOVERY_SERVICE", None)
    monkeypatch.setattr(admin_bridge, "_DISCOVERY_SERVICE_PATHS", None)
    monkeypatch.setattr(admin_bridge, "_PIPELINE_SERVICE", None)
