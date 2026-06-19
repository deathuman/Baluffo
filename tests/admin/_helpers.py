from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src import admin_bridge
from src.bridge import ACTIVE_SYNC_RUNS, ACTIVE_SYNC_THREADS, SYNC_STATE_LOCK
from src.bridge.storage_health import close_storage_stores


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
        tasks_config=root / "tasks.json",
        task_state=root / "admin-task-state.json",
        sync_live_task=root / "sync-live-task.json",
        sync_config=root / "source-sync-config.json",
        sync_runtime=root / "source-sync-runtime.json",
        packaged_sync_config=root / "github-app-sync-config.json",
    )


def patch_admin_bridge_paths(monkeypatch: Any, paths: AdminBridgeTestPaths) -> None:
    monkeypatch.setattr(admin_bridge, "OPS_HISTORY_PATH", paths.ops_history)
    monkeypatch.setattr(admin_bridge, "TASK_LIFECYCLE_PATH", paths.task_lifecycle)
    monkeypatch.setattr(admin_bridge, "OPS_ALERT_STATE_PATH", paths.ops_alert_state)
    monkeypatch.setattr(admin_bridge, "JOBS_FETCH_REPORT_PATH", paths.jobs_fetch_report)
    monkeypatch.setattr(
        admin_bridge,
        "ADMIN_ACTIVE_TASK_SNAPSHOT_PATH",
        paths.active_task_snapshot,
        raising=False,
    )
    monkeypatch.setattr(admin_bridge, "JOBS_FETCH_TASKS_PATH", paths.jobs_fetch_tasks)
    monkeypatch.setattr(admin_bridge, "DISCOVERY_REPORT_PATH", paths.discovery_report)
    monkeypatch.setattr(admin_bridge, "DISCOVERY_CANDIDATES_PATH", paths.discovery_candidates)
    monkeypatch.setattr(
        admin_bridge, "SOURCE_POLICY_RECOMMENDATIONS_PATH", paths.source_policy_recommendations
    )
    monkeypatch.setattr(
        admin_bridge, "SOURCE_POLICY_REVIEW_STATE_PATH", paths.source_policy_review_state
    )
    monkeypatch.setattr(admin_bridge, "DEDUP_REVIEW_STATE_PATH", paths.dedup_review_state)
    monkeypatch.setattr(admin_bridge, "APPROVAL_STATE_PATH", paths.approval_state)
    monkeypatch.setattr(
        admin_bridge.source_registry_module,
        "DISCOVERY_CANDIDATES_PATH",
        paths.discovery_candidates,
    )
    monkeypatch.setattr(
        admin_bridge.source_registry_module, "APPROVAL_STATE_PATH", paths.approval_state
    )
    monkeypatch.setattr(admin_bridge, "ACTIVE_PATH", paths.active_registry)
    monkeypatch.setattr(admin_bridge, "PENDING_PATH", paths.pending_registry)
    monkeypatch.setattr(admin_bridge, "REJECTED_PATH", paths.rejected_registry)
    monkeypatch.setattr(admin_bridge, "TOMBSTONES_PATH", paths.tombstones)
    monkeypatch.setattr(admin_bridge, "TASKS_CONFIG_PATH", paths.tasks_config)
    monkeypatch.setattr(admin_bridge, "TASK_STATE_PATH", paths.task_state)
    monkeypatch.setattr(admin_bridge, "SYNC_LIVE_TASK_PATH", paths.sync_live_task)
    monkeypatch.setattr(admin_bridge, "SYNC_CONFIG_PATH", paths.sync_config)
    monkeypatch.setattr(admin_bridge, "SYNC_RUNTIME_PATH", paths.sync_runtime)
    monkeypatch.setattr(admin_bridge, "MAX_HISTORY_ROWS", 5)
    monkeypatch.setattr(admin_bridge.RUNTIME_CONFIG, "data_dir", paths.root)


def seed_admin_bridge_state(paths: AdminBridgeTestPaths) -> None:
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
    monkeypatch.setattr(admin_bridge, "_SYNC_SERVICE", None)
    monkeypatch.setattr(admin_bridge, "_SYNC_SERVICE_DATA_DIR", None)
    monkeypatch.setattr(admin_bridge, "_REGISTRY_SERVICE", None)
    monkeypatch.setattr(admin_bridge, "_REGISTRY_SERVICE_PATHS", None)
    monkeypatch.setattr(admin_bridge, "_DISCOVERY_SERVICE", None)
    monkeypatch.setattr(admin_bridge, "_DISCOVERY_SERVICE_PATHS", None)


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
