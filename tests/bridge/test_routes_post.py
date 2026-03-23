"""Tests for bridge POST routes - improving coverage for post_routes.py."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import json

from src.bridge.api import BridgeApi
from src.bridge.routes.post_routes import handle_post


@dataclass
class _RuntimeConfig:
    host: str = "127.0.0.1"
    port: int = 0
    quiet_requests: bool = True
    desktop_mode: bool = True
    root: Any = None
    data_dir: Any = None


class _FakeHandler:
    """Captures all sent responses for assertions."""

    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []

    def _send_json(self, payload: Any, status: int = 200) -> None:
        self.sent.append({"status": status, "payload": payload})


class _FakeDesktopLocalDataStore:
    """Mock for desktop local data operations."""

    def __init__(self) -> None:
        self.users: Dict[str, Any] = {}
        self.saved_jobs: Dict[str, List[Dict]] = {}
        self.attachments: Dict[str, Any] = {}
        self._current_user: Optional[Dict] = None

    def sign_in(self, name: str) -> Dict[str, Any]:
        if not name.strip():
            raise ValueError("Name required")
        uid = f"user_{hash(name) % 10000}"
        self.users[uid] = {"uid": uid, "name": name}
        self._current_user = self.users[uid]
        return self.users[uid]

    def sign_out(self) -> None:
        self._current_user = None

    def save_job_for_user(
        self, uid: str, job: dict, options: dict
    ) -> str:
        if uid not in self.saved_jobs:
            self.saved_jobs[uid] = []
        job_key = f"job_{len(self.saved_jobs[uid])}"
        self.saved_jobs[uid].append({"key": job_key, **job})
        return job_key

    def remove_saved_job_for_user(self, uid: str, job_key: str) -> None:
        if uid in self.saved_jobs:
            self.saved_jobs[uid] = [
                j for j in self.saved_jobs[uid] if j.get("key") != job_key
            ]

    def update_application_status(
        self, uid: str, job_key: str, status: str, options: dict
    ) -> None:
        for job in self.saved_jobs.get(uid, []):
            if job.get("key") == job_key:
                job["status"] = status

    def update_job_notes(
        self, uid: str, job_key: str, notes: str
    ) -> None:
        for job in self.saved_jobs.get(uid, []):
            if job.get("key") == job_key:
                job["notes"] = notes

    def add_attachment_for_job(
        self, uid: str, job_key: str, file_meta: dict
    ) -> str:
        att_id = f"att_{hash(file_meta.get('name', '')) % 10000}"
        self.attachments[att_id] = {"uid": uid, "job_key": job_key, **file_meta}
        return att_id

    def get_current_user(self) -> Optional[Dict]:
        return self._current_user


def _make_api(tmp_path: Path, store: _FakeDesktopLocalDataStore) -> BridgeApi:
    """Create a BridgeApi with all mocked dependencies."""

    state = {
        "active": [
            {"id": "src-1", "adapter": "static", "name": "Active Source"}
        ],
        "pending": [
            {"id": "src-2", "adapter": "greenhouse", "name": "Pending Source"}
        ],
        "rejected": [
            {"id": "src-3", "adapter": "static", "name": "Rejected Source"}
        ],
    }

    def load_state() -> Dict[str, List[Dict[str, Any]]]:
        return state

    def persist_state_and_auto_sync(
        new_state: Dict[str, Any], reason: str = None
    ) -> Dict[str, Any]:
        state.clear()
        state.update(new_state)
        return state

    def summarize_state(state_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
        return {
            "activeCount": len(state_data.get("active") or []),
            "pendingCount": len(state_data.get("pending") or []),
            "rejectedCount": len(state_data.get("rejected") or []),
        }

    def source_identity(row: dict) -> str:
        return row.get("id", "")

    def source_url_fingerprint(row: dict) -> str:
        return row.get("listing_url", "")

    api = BridgeApi(
        runtime_config=_RuntimeConfig(root=tmp_path),
        DISCOVERY_REPORT_PATH=tmp_path / "discovery-report.json",
        JOBS_FETCH_REPORT_PATH=tmp_path / "jobs-fetch-report.json",
        APPROVAL_STATE_PATH=tmp_path / "approval.json",
        DISCOVERY_LOG_PATH=tmp_path / "discovery.log",
        FETCHER_LOG_PATH=tmp_path / "fetcher.log",
        STARTUP_METRICS_PATH=tmp_path / "startup-metrics.jsonl",
    )

    api.desktop_local_data_store = lambda: store
    api.load_state = load_state
    api.summarize_state = summarize_state
    api.persist_state_and_auto_sync = persist_state_and_auto_sync
    api.source_identity = source_identity
    api.source_url_fingerprint = source_url_fingerprint
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

    return api


# ============== TOP-LEVEL TEST FUNCTIONS FOR POST ROUTES ==============


def test_sign_in_success(tmp_path: Path) -> None:
    """Test successful sign-in."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/sign-in",
        payload={"name": "Test User"},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True
    assert handler.sent[-1]["payload"]["user"]["name"] == "Test User"


def test_sign_in_empty_name(tmp_path: Path) -> None:
    """Test sign-in with empty name."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/sign-in",
        payload={"name": ""},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert handler.sent[-1]["payload"]["ok"] is False


def test_sign_in_no_payload(tmp_path: Path) -> None:
    """Test sign-in with no payload."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/sign-in",
        payload=None,
    )

    assert result is True
    assert handler.sent[-1]["status"] == 400


def test_sign_out_success(tmp_path: Path) -> None:
    """Test successful sign-out."""
    store = _FakeDesktopLocalDataStore()
    store.sign_in("Test User")
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/sign-out",
        payload={},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True


def test_save_job_success(tmp_path: Path) -> None:
    """Test saving a job successfully."""
    store = _FakeDesktopLocalDataStore()
    store.sign_in("Test User")
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/saved-jobs/save",
        payload={
            "uid": "user_123",
            "job": {
                "title": "Engineer",
                "company": "Acme",
                "jobLink": "https://example.com/job/1",
            },
            "options": {},
        },
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True
    assert "jobKey" in handler.sent[-1]["payload"]


def test_save_job_empty_job(tmp_path: Path) -> None:
    """Test saving with empty job."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/saved-jobs/save",
        payload={"uid": "user_123", "job": {}, "options": {}},
    )

    assert result is True


def test_remove_job_success(tmp_path: Path) -> None:
    """Test removing a saved job."""
    store = _FakeDesktopLocalDataStore()
    store.sign_in("Test User")
    store.save_job_for_user("user_123", {"title": "Test"}, {})
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/saved-jobs/remove",
        payload={"uid": "user_123", "jobKey": "job_0"},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True


def test_update_status_success(tmp_path: Path) -> None:
    """Test updating job status."""
    store = _FakeDesktopLocalDataStore()
    store.sign_in("Test User")
    store.save_job_for_user("user_123", {"title": "Test"}, {})
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/saved-jobs/status",
        payload={
            "uid": "user_123",
            "jobKey": "job_0",
            "status": "applied",
            "options": {},
        },
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_update_notes_success(tmp_path: Path) -> None:
    """Test updating job notes."""
    store = _FakeDesktopLocalDataStore()
    store.sign_in("Test User")
    store.save_job_for_user("user_123", {"title": "Test"}, {})
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/desktop-local-data/saved-jobs/notes",
        payload={
            "uid": "user_123",
            "jobKey": "job_0",
            "notes": "Follow up next week",
        },
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_delete_by_id(tmp_path: Path) -> None:
    """Test deleting sources by ID."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/registry/delete",
        payload={"selected": ["src-1"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_delete_by_url(tmp_path: Path) -> None:
    """Test deleting sources by URL fingerprint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/registry/delete",
        payload={"selectedUrls": ["https://example.com/jobs"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_run_discovery(tmp_path: Path) -> None:
    """Test triggering discovery task."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/tasks/run-discovery",
        payload={"mode": "incremental"},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["started"] is True


def test_run_discovery_response_write_failure_is_logged_and_returns_error_json(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    log_calls: list[tuple[str, dict[str, Any]]] = []

    def bridge_log(level: str, message: str, **fields: Any) -> None:
        log_calls.append((message, {"level": level, **fields}))

    class _FlakyHandler(_FakeHandler):
        def __init__(self) -> None:
            super().__init__()
            self._first = True

        def _send_json(self, payload: Any, status: int = 200) -> None:
            if self._first:
                self._first = False
                raise BrokenPipeError("socket closed")
            super()._send_json(payload, status=status)

    api.bridge_log = bridge_log
    handler = _FlakyHandler()

    result = handle_post(
        handler,
        api=api,
        path="/tasks/run-discovery",
        payload={"preset": "default"},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 500
    assert handler.sent[-1]["payload"]["started"] is False
    assert any(message == "discovery_launch_response_write_failed" for message, _fields in log_calls)


def test_run_jobs_pipeline(tmp_path: Path) -> None:
    """Test triggering jobs pipeline."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/tasks/run-jobs-pipeline",
        payload={},
    )

    assert result is True


def test_run_sync_pull(tmp_path: Path) -> None:
    """Test triggering sync pull."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/tasks/run-sync-pull",
        payload={},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_run_sync_push(tmp_path: Path) -> None:
    """Test triggering sync push."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/tasks/run-sync-push",
        payload={},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_run_fetcher(tmp_path: Path) -> None:
    """Test triggering fetcher task."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/tasks/run-fetcher",
        payload={"sources": ["src-1"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_save_discovery_config_persists_and_returns_saved_payload(tmp_path: Path) -> None:
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)
    saved_payloads: list[dict[str, Any]] = []

    def update_saved_discovery_settings(payload: dict[str, Any]) -> dict[str, Any]:
        normalized = {
            "autoApproveHealthyPendingOnComplete": bool(
                (payload or {}).get("autoApproveHealthyPendingOnComplete", True)
            )
        }
        saved_payloads.append(normalized)
        return normalized

    api.update_saved_discovery_settings = update_saved_discovery_settings
    api.get_discovery_config_payload = lambda: {
        "ok": True,
        "savedConfig": saved_payloads[-1],
    }

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/discovery/config",
        payload={"autoApproveHealthyPendingOnComplete": False},
    )

    assert result is True
    assert saved_payloads == [{"autoApproveHealthyPendingOnComplete": False}]
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True
    assert (
        handler.sent[-1]["payload"]["savedConfig"][
            "autoApproveHealthyPendingOnComplete"
        ]
        is False
    )


def test_ack_alert_success(tmp_path: Path) -> None:
    """Test acknowledging an alert."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_post(
        handler,
        api=api,
        path="/ops/alerts/ack",
        payload={"id": "alert-123"},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
