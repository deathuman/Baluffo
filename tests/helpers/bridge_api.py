from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.bridge.api import BridgeApi


@dataclass
class BridgeRuntimeConfigStub:
    host: str = "127.0.0.1"
    port: int = 0
    quiet_requests: bool = True
    desktop_mode: bool = True
    root: Any = None
    data_dir: Any = None


class FakeHandler:
    """Captures all sent responses for assertions."""

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.bytes_sent: list[dict[str, Any]] = []

    def _send_json(self, payload: Any, status: int = 200) -> None:
        self.sent.append({"status": status, "payload": payload})

    def _send_bytes(self, body: bytes, content_type: str, status: int = 200) -> None:
        self.bytes_sent.append({"status": status, "body": body, "content_type": content_type})


class FakeDesktopLocalDataStore:
    """Mock for desktop local data operations."""

    def __init__(self) -> None:
        self.users: dict[str, Any] = {}
        self.saved_jobs: dict[str, list[dict]] = {}
        self.attachments: dict[str, Any] = {}
        self._current_user: dict | None = None

    def sign_in(self, name: str) -> dict[str, Any]:
        if not name.strip():
            raise ValueError("Name required")
        uid = f"user_{hash(name) % 10000}"
        self.users[uid] = {"uid": uid, "name": name}
        self._current_user = self.users[uid]
        return self.users[uid]

    def sign_out(self) -> None:
        self._current_user = None

    def save_job_for_user(self, uid: str, job: dict, options: dict) -> str:
        if uid not in self.saved_jobs:
            self.saved_jobs[uid] = []
        job_key = f"job_{len(self.saved_jobs[uid])}"
        self.saved_jobs[uid].append({"key": job_key, **job})
        return job_key

    def remove_saved_job_for_user(self, uid: str, job_key: str) -> None:
        if uid in self.saved_jobs:
            self.saved_jobs[uid] = [j for j in self.saved_jobs[uid] if j.get("key") != job_key]

    def update_application_status(self, uid: str, job_key: str, status: str, options: dict) -> None:
        for job in self.saved_jobs.get(uid, []):
            if job.get("key") == job_key:
                job["status"] = status

    def update_job_notes(self, uid: str, job_key: str, notes: str) -> None:
        for job in self.saved_jobs.get(uid, []):
            if job.get("key") == job_key:
                job["notes"] = notes

    def add_attachment_for_job(self, uid: str, job_key: str, file_meta: dict) -> str:
        att_id = f"att_{hash(file_meta.get('name', '')) % 10000}"
        self.attachments[att_id] = {"uid": uid, "job_key": job_key, **file_meta}
        return att_id

    def get_current_user(self) -> dict | None:
        return self._current_user

    def get_saved_jobs(self, uid: str) -> list[dict]:
        return self.saved_jobs.get(uid, [])

    def get_attachment(self, att_id: str) -> dict | None:
        return self.attachments.get(att_id)


def make_stub_bridge_api(tmp_path: Path, store: FakeDesktopLocalDataStore) -> BridgeApi:
    """Create a BridgeApi with stubbed dependencies for route tests."""

    state = {
        "active": [{"id": "src-1", "adapter": "static", "name": "Active Source"}],
        "pending": [{"id": "src-2", "adapter": "greenhouse", "name": "Pending Source"}],
        "rejected": [{"id": "src-3", "adapter": "static", "name": "Rejected Source"}],
    }

    def load_state() -> dict[str, list[dict[str, Any]]]:
        return state

    def persist_state_and_auto_sync(
        new_state: dict[str, Any], reason: str = None
    ) -> dict[str, Any]:
        persisted = {
            bucket: [dict(row) for row in (rows or [])]
            for bucket, rows in (new_state or {}).items()
        }
        state.clear()
        state.update(persisted)
        return state

    def summarize_state(state_data: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
        return {
            "activeCount": len(state_data.get("active") or []),
            "pendingCount": len(state_data.get("pending") or []),
            "rejectedCount": len(state_data.get("rejected") or []),
        }

    def source_identity(row: dict) -> str:
        return row.get("id", "")

    def source_url_fingerprint(row: dict) -> str:
        return row.get("listing_url", "")

    def move_entries(
        rows: list[dict[str, Any]], selected_ids: list[str]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        selected = {str(item) for item in selected_ids}
        moved = [dict(row) for row in rows if str(row.get("id") or "") in selected]
        remaining = [dict(row) for row in rows if str(row.get("id") or "") not in selected]
        return moved, remaining

    def unique_sources(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            row_id = str((row or {}).get("id") or "")
            if row_id and row_id in seen:
                continue
            if row_id:
                seen.add(row_id)
            out.append(dict(row))
        return out

    api = BridgeApi(
        runtime_config=BridgeRuntimeConfigStub(root=tmp_path),
        DISCOVERY_REPORT_PATH=tmp_path / "discovery-report.json",
        JOBS_FETCH_REPORT_PATH=tmp_path / "jobs-fetch-report.json",
        APPROVAL_STATE_PATH=tmp_path / "approval.json",
        DISCOVERY_LOG_PATH=tmp_path / "discovery.log",
        FETCHER_LOG_PATH=tmp_path / "fetcher.log",
        STARTUP_METRICS_PATH=tmp_path / "startup-metrics.jsonl",
        DESKTOP_UPDATE_STATE_PATH=tmp_path / "updater" / "install-state.json",
    )

    api.desktop_local_data_store = lambda: store
    api.load_state = load_state
    api.summarize_state = summarize_state
    api.persist_state_and_auto_sync = persist_state_and_auto_sync
    api.source_identity = source_identity
    api.source_url_fingerprint = source_url_fingerprint
    api.move_entries = move_entries
    api.unique_sources = unique_sources
    api.compute_ops_health = lambda: {"ok": True, "detail": "unit-test", "alerts": []}
    api.compute_fetcher_metrics = lambda **kw: {"windowRuns": 20, "runs": [], "aggregates": {}}
    api.sync_history_from_reports = lambda: []
    api.normalize_fetch_report_contract = lambda r: r
    api.normalize_discovery_report_contract = lambda r: r
    api.load_json_object = lambda p, default=None: default
    api.get_sync_status_payload = lambda: {"ready": True, "enabled": True}
    api.get_jobs_pipeline_status_payload = lambda: {"running": False}
    api.trigger_discovery_task = lambda payload, route_name: (200, {"started": True})
    api.start_jobs_pipeline_task = lambda payload: {"started": True}
    api.start_sync_task = lambda action, reason, automatic: {"started": True}
    api.start_fetcher_task = lambda payload: {"started": True}
    api.update_saved_sync_settings = lambda p: None
    api.update_saved_discovery_settings = lambda payload: {
        "autoApproveHealthyPendingOnComplete": bool(
            (payload or {}).get("autoApproveHealthyPendingOnComplete", True)
        )
    }
    api.get_discovery_config_payload = lambda: {
        "ok": True,
        "savedConfig": {"autoApproveHealthyPendingOnComplete": True},
    }
    api.sync_config_status = lambda: {"ready": True}
    api.test_sync_config = lambda: {"ok": True}
    api.sync_pull_sources = lambda: {"pulled": True, "sources": []}
    api.sync_push_sources = lambda: {"pushed": True, "sources": []}
    api.load_alert_state = lambda: {"acked": {}}
    api.save_alert_state = lambda s: None
    api.now_iso = lambda: "2024-01-01T00:00:00Z"
    api.bridge_log = lambda *a, **kw: None
    api.set_sync_status = lambda **kw: None
    api.add_manual_source = lambda url: {"id": "new-src", "url": url}
    api.update_pending_source = lambda src_id, updates: None
    api.get_update_status_payload = lambda: {
        "schemaVersion": 1,
        "currentVersion": "0.1.0",
        "latestVersion": "",
        "updateAvailable": False,
        "availability": "unknown",
        "downloadState": "idle",
        "downloadedBytes": 0,
        "totalBytes": 0,
        "downloadPercent": 0,
        "installState": "idle",
        "releaseNotesUrl": "",
        "lastCheckedAt": "",
        "lastError": "",
    }
    api.check_for_update = lambda **kw: {"started": True, "status": api.get_update_status_payload()}
    api.download_update = lambda: {"started": True, "status": api.get_update_status_payload()}
    api.install_update = lambda: {"started": True, "status": api.get_update_status_payload()}

    return api


def build_admin_bridge_api(config: Any | None = None) -> BridgeApi:
    """Build a real BridgeApi through bridge bootstrap without calling admin_bridge.build_bridge_api."""

    from src import admin_bridge
    from src.bridge import bootstrap as bridge_bootstrap

    runtime_config = config or admin_bridge.RUNTIME_CONFIG
    return bridge_bootstrap.build_bridge_api(
        config=runtime_config,
        registry=admin_bridge._get_registry_service(),
        sync=admin_bridge._get_sync_service(),
        pipeline=admin_bridge._get_pipeline_service(),
        discovery=admin_bridge._get_discovery_service(),
        normalize_fetch_report_contract=admin_bridge.normalize_fetch_report_contract,
        normalize_discovery_report_contract=admin_bridge.normalize_discovery_report_contract,
        discovery_report_path=admin_bridge.DISCOVERY_REPORT_PATH,
        jobs_fetch_report_path=admin_bridge.JOBS_FETCH_REPORT_PATH,
        approval_state_path=admin_bridge.APPROVAL_STATE_PATH,
        discovery_log_path=admin_bridge.DISCOVERY_LOG_PATH,
        fetcher_log_path=admin_bridge.FETCHER_LOG_PATH,
        startup_metrics_path=admin_bridge.STARTUP_METRICS_PATH,
        desktop_update_state_path=admin_bridge.DESKTOP_UPDATE_STATE_PATH,
        desktop_session_activity_at=admin_bridge.bridge_runtime_state.DESKTOP_SESSION_ACTIVITY_AT,
        bridge_log=admin_bridge.bridge_log,
        now_iso=admin_bridge.now_iso,
        mark_desktop_session_activity=admin_bridge.mark_desktop_session_activity,
        desktop_local_data_store=admin_bridge.desktop_local_data_store,
        append_startup_metric=admin_bridge.append_startup_metric,
        read_startup_metrics=admin_bridge.read_startup_metrics,
        get_update_status_payload=lambda: admin_bridge._get_desktop_update_service().get_status_payload(),
        check_for_update=lambda **kw: admin_bridge._get_desktop_update_service().check_for_update(**kw),
        download_update=lambda: admin_bridge._get_desktop_update_service().download_update(),
        install_update=lambda: admin_bridge._get_desktop_update_service().request_install(),
        persist_state_and_auto_sync=admin_bridge.persist_state_and_auto_sync,
        add_manual_source=admin_bridge.add_manual_source,
        trigger_source_check=admin_bridge.trigger_source_check,
        load_json_object=admin_bridge.load_json_object,
        save_json_atomic=admin_bridge.save_json_atomic,
        start_fetcher_task=admin_bridge.start_fetcher_task,
        start_sync_task=admin_bridge.start_sync_task,
        get_discovery_config_payload=admin_bridge.get_discovery_config_payload,
        update_saved_discovery_settings=admin_bridge.update_saved_discovery_settings,
        compute_ops_health=admin_bridge.compute_ops_health,
        compute_fetcher_metrics=admin_bridge.compute_fetcher_metrics,
        sync_history_from_reports=admin_bridge.sync_history_from_reports,
        get_projected_run_history=admin_bridge._get_ops_api().get_projected_run_history,
        get_current_task_state_payload=admin_bridge._get_ops_api().get_current_task_state_payload,
        should_exit_for_owner_timeout=admin_bridge.owner_session_should_exit,
        load_alert_state=admin_bridge.load_alert_state,
        save_alert_state=admin_bridge.save_alert_state,
    )


__all__ = [
    "BridgeRuntimeConfigStub",
    "FakeDesktopLocalDataStore",
    "FakeHandler",
    "build_admin_bridge_api",
    "make_stub_bridge_api",
]
