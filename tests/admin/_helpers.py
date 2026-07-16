from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src import admin_bridge
from src.bridge import ACTIVE_SYNC_RUNS, ACTIVE_SYNC_THREADS, SYNC_STATE_LOCK
from src.bridge.storage_health import close_storage_stores
from tests.helpers.ports import ADMIN_BRIDGE_TEST_PORT

_ADMIN_BRIDGE_RUNTIME_ATTRS = (
    "RUNTIME_CONFIG",
    "OPS_HISTORY_PATH",
    "TASK_LIFECYCLE_PATH",
    "OPS_ALERT_STATE_PATH",
    "JOBS_FETCH_REPORT_PATH",
    "ADMIN_ACTIVE_TASK_SNAPSHOT_PATH",
    "JOBS_FETCH_TASKS_PATH",
    "DISCOVERY_REPORT_PATH",
    "DISCOVERY_CANDIDATES_PATH",
    "SOURCE_POLICY_RECOMMENDATIONS_PATH",
    "SOURCE_POLICY_REVIEW_STATE_PATH",
    "DEDUP_REVIEW_STATE_PATH",
    "APPROVAL_STATE_PATH",
    "ACTIVE_PATH",
    "PENDING_PATH",
    "REJECTED_PATH",
    "TOMBSTONES_PATH",
    "TASKS_CONFIG_PATH",
    "TASK_STATE_PATH",
    "SYNC_LIVE_TASK_PATH",
    "SYNC_CONFIG_PATH",
    "SYNC_RUNTIME_PATH",
    "MAX_HISTORY_ROWS",
)

_SOURCE_REGISTRY_RUNTIME_ATTRS = (
    "DATA_DIR",
    "DEFAULTS_DIR",
    "ACTIVE_PATH",
    "PENDING_PATH",
    "ACTIVE_SEED_PATH",
    "PENDING_SEED_PATH",
    "REJECTED_PATH",
    "TOMBSTONES_PATH",
    "DISCOVERY_REPORT_PATH",
    "DISCOVERY_CANDIDATES_PATH",
    "APPROVAL_STATE_PATH",
)


@dataclass(frozen=True)
class AdminBridgeTestPaths:
    root: Path
    ops_history: Path
    task_lifecycle: Path
    ops_alert_state: Path
    jobs_fetch_report: Path
    active_task_snapshot: Path
    jobs_fetch_tasks: Path
    discovery_report: Path
    discovery_candidates: Path
    source_policy_recommendations: Path
    source_policy_review_state: Path
    dedup_review_state: Path
    approval_state: Path
    active_registry: Path
    pending_registry: Path
    rejected_registry: Path
    tombstones: Path
    tasks_config: Path
    task_state: Path
    sync_live_task: Path
    sync_config: Path
    sync_runtime: Path
    packaged_sync_config: Path


def admin_bridge_test_paths(root: Path) -> AdminBridgeTestPaths:
    return AdminBridgeTestPaths(
        root=root,
        ops_history=root / "admin-run-history.json",
        task_lifecycle=root / "admin-task-lifecycle.json",
        ops_alert_state=root / "admin-alert-state.json",
        jobs_fetch_report=root / "jobs-fetch-report.json",
        active_task_snapshot=root / "admin-active-task-snapshot.json",
        jobs_fetch_tasks=root / "jobs-fetch-tasks.json",
        discovery_report=root / "source-discovery-report.json",
        discovery_candidates=root / "source-discovery-candidates.json",
        source_policy_recommendations=root / "source-policy-recommendations.json",
        source_policy_review_state=root / "source-policy-review-state.json",
        dedup_review_state=root / "dedup-review-state.json",
        approval_state=root / "source-approval-state.json",
        active_registry=root / "source-registry-active.json",
        pending_registry=root / "source-registry-pending.json",
        rejected_registry=root / "source-registry-rejected.json",
        tombstones=root / "source-registry-tombstones.json",
        tasks_config=root / ".vscode" / "tasks.json",
        task_state=root / "admin-task-state.json",
        sync_live_task=root / "sync-live-task.json",
        sync_config=root / "source-sync-config.json",
        sync_runtime=root / "source-sync-runtime.json",
        packaged_sync_config=root / "github-app-sync-config.json",
    )


def _runtime_config_for_test_root(root: Path) -> admin_bridge.RuntimeConfig:
    return admin_bridge.RuntimeConfig(
        root=root,
        data_dir=root,
        host="127.0.0.1",
        port=ADMIN_BRIDGE_TEST_PORT,
        log_format="human",
        log_level="info",
        quiet_requests=False,
    )


def _preserve_admin_bridge_runtime_attrs(monkeypatch: Any) -> None:
    for attr in _ADMIN_BRIDGE_RUNTIME_ATTRS:
        monkeypatch.setattr(admin_bridge, attr, getattr(admin_bridge, attr, None), raising=False)
    for attr in _SOURCE_REGISTRY_RUNTIME_ATTRS:
        monkeypatch.setattr(
            admin_bridge.source_registry_module,
            attr,
            getattr(admin_bridge.source_registry_module, attr),
            raising=False,
        )


def patch_admin_bridge_paths(monkeypatch: Any, paths: AdminBridgeTestPaths) -> None:
    _preserve_admin_bridge_runtime_attrs(monkeypatch)
    admin_bridge.configure_runtime_paths(_runtime_config_for_test_root(paths.root))
    monkeypatch.setattr(admin_bridge, "MAX_HISTORY_ROWS", 5)


def seed_admin_bridge_state(paths: AdminBridgeTestPaths) -> None:
    paths.tasks_config.parent.mkdir(parents=True, exist_ok=True)
    admin_bridge.save_json_atomic(paths.active_registry, [])
    admin_bridge.save_json_atomic(paths.pending_registry, [])
    admin_bridge.save_json_atomic(paths.rejected_registry, [])
    admin_bridge.save_json_atomic(paths.tombstones, {})
    admin_bridge.save_json_atomic(paths.tasks_config, {"tasks": []})


def reset_admin_bridge_services(monkeypatch: Any) -> None:
    with SYNC_STATE_LOCK:
        ACTIVE_SYNC_RUNS.clear()
        ACTIVE_SYNC_THREADS.clear()
    admin_bridge.BRIDGE_SERVICES.reset_sync_service()
    admin_bridge.BRIDGE_SERVICES.reset_registry_service()
    admin_bridge.BRIDGE_SERVICES.reset_discovery_service()
    admin_bridge.BRIDGE_SERVICES.reset_pipeline_service()
    admin_bridge.BRIDGE_SERVICES.reset_availability_service()


def write_admin_bridge_packaged_sync_config(paths: AdminBridgeTestPaths) -> None:
    paths.packaged_sync_config.write_text(
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


def configure_admin_bridge_entrypoint_root(monkeypatch: Any, root: Path) -> Path:
    paths = admin_bridge_test_paths(root)
    patch_admin_bridge_paths(monkeypatch, paths)
    seed_admin_bridge_state(paths)
    reset_admin_bridge_services(monkeypatch)
    write_admin_bridge_packaged_sync_config(paths)
    monkeypatch.setenv(
        admin_bridge.source_sync_module.PACKAGED_SYNC_CONFIG_ENV,
        str(paths.packaged_sync_config),
    )
    admin_bridge.refresh_sync_config()
    monkeypatch.setattr(admin_bridge, "_maybe_trigger_auto_sync_push", lambda _reason: False)
    return root


def cleanup_admin_bridge_entrypoint_root(monkeypatch: Any) -> None:
    admin_bridge.wait_for_sync_tasks(timeout_s=2.0)
    close_storage_stores()
    reset_admin_bridge_services(monkeypatch)
