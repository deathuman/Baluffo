"""Tests for bridge GET routes - improving coverage for get_routes.py."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
import json

import pytest

from src.bridge.api import BridgeApi
from src.bridge.routes.get_routes import handle_get


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
        self.bytes_sent: List[Dict[str, Any]] = []

    def _send_json(self, payload: Any, status: int = 200) -> None:
        self.sent.append({"status": status, "payload": payload})

    def _send_bytes(
        self, body: bytes, content_type: str, status: int = 200
    ) -> None:
        self.bytes_sent.append(
            {"status": status, "body": body, "content_type": content_type}
        )


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

    def get_saved_jobs(self, uid: str) -> List[Dict]:
        return self.saved_jobs.get(uid, [])

    def get_attachment(self, att_id: str) -> Optional[Dict]:
        return self.attachments.get(att_id)


def _make_api(tmp_path: Path, store: _FakeDesktopLocalDataStore) -> BridgeApi:
    """Create a BridgeApi with all mocked dependencies."""

    def load_state() -> Dict[str, List[Dict[str, Any]]]:
        return {
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

    def summarize_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
        return {
            "activeCount": len(state.get("active") or []),
            "pendingCount": len(state.get("pending") or []),
            "rejectedCount": len(state.get("rejected") or []),
        }

    def persist_state_and_auto_sync(
        state: Dict[str, Any], reason: str = None
    ) -> Dict[str, Any]:
        return state

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
    api.compute_ops_health = lambda: {
        "ok": True,
        "detail": "unit-test",
        "alerts": [],
    }
    api.compute_fetcher_metrics = lambda **kw: {
        "windowRuns": 20,
        "runs": [],
        "aggregates": {},
    }
    api.sync_history_from_reports = lambda: []
    api.normalize_fetch_report_contract = lambda r: r
    api.normalize_discovery_report_contract = lambda r: r
    api.load_json_object = lambda p, default=None: default
    api.get_sync_status_payload = lambda: {"ready": True, "enabled": True}
    api.get_jobs_pipeline_status_payload = lambda: {"running": False}
    api.trigger_discovery_task = lambda payload, route_name: (
        200,
        {"started": True},
    )
    api.start_jobs_pipeline_task = lambda payload: {"started": True}
    api.start_sync_task = lambda action, reason, automatic: {"started": True}
    api.start_fetcher_task = lambda payload: {"started": True}
    api.update_saved_sync_settings = lambda p: None
    api.sync_config_status = lambda: {"ready": True}
    api.test_sync_config = lambda: {"ok": True}
    api.sync_pull_sources = lambda: {"pulled": True, "sources": []}
    api.sync_push_sources = lambda: {"pushed": True, "sources": []}
    api.load_alert_state = lambda: {"acked": {}}
    api.save_alert_state = lambda s: None
    api.now_iso = lambda: "2024-01-01T00:00:00Z"
    api.bridge_log = lambda *a, **kw: None

    return api


# ============== TOP-LEVEL TEST FUNCTIONS FOR GET ROUTES ==============


def test_discovery_report_missing_file(tmp_path: Path) -> None:
    """Test discovery report when file doesn't exist."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/discovery/report", query={})

    assert result is True


def test_session_with_user(tmp_path: Path) -> None:
    """Test session endpoint when user is signed in."""
    store = _FakeDesktopLocalDataStore()
    store.sign_in("Test User")
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/desktop-local-data/session", query={}
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True
    assert handler.sent[-1]["payload"]["user"]["name"] == "Test User"


def test_session_no_user(tmp_path: Path) -> None:
    """Test session endpoint when no user is signed in."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/desktop-local-data/session", query={}
    )

    assert result is True
    assert handler.sent[-1]["payload"]["user"] is None


def test_saved_jobs_endpoint_exists(tmp_path: Path) -> None:
    """Test saved jobs endpoint exists."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/desktop-local-data/saved-jobs",
        query={},
    )

    assert result is True


def test_registry_active(tmp_path: Path) -> None:
    """Test /registry/active endpoint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/registry/active", query={}
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert "sources" in handler.sent[-1]["payload"]
    assert "summary" in handler.sent[-1]["payload"]


def test_registry_pending(tmp_path: Path) -> None:
    """Test /registry/pending endpoint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/registry/pending", query={}
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert "sources" in handler.sent[-1]["payload"]


def test_registry_rejected(tmp_path: Path) -> None:
    """Test /registry/rejected endpoint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/registry/rejected", query={}
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert "sources" in handler.sent[-1]["payload"]


def test_registry_summary(tmp_path: Path) -> None:
    """Test /registry/summary endpoint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/registry/summary", query={}
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert "summary" in handler.sent[-1]["payload"]


def test_discovery_log_with_content(tmp_path: Path) -> None:
    """Test /discovery/log endpoint with log content."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    api.DISCOVERY_LOG_PATH.write_text("log line 1\nlog line 2\n")

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/discovery/log", query={}
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert "text" in handler.sent[-1]["payload"]


def test_discovery_log_empty(tmp_path: Path) -> None:
    """Test /discovery/log endpoint with empty log."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/discovery/log", query={}
    )

    assert result is True
    assert handler.sent[-1]["payload"]["text"] == ""


def test_discovery_log_with_offset(tmp_path: Path) -> None:
    """Test /discovery/log endpoint with offset query param."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    api.DISCOVERY_LOG_PATH.write_text("line1\nline2\nline3\n")

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/discovery/log", query={"offset": ["5"]}
    )

    assert result is True
    assert handler.sent[-1]["payload"]["offset"] == 5


def test_discovery_log_invalid_offset(tmp_path: Path) -> None:
    """Test /discovery/log with invalid offset."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    api.DISCOVERY_LOG_PATH.write_text("content")

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/discovery/log", query={"offset": ["abc"]}
    )

    assert result is True
    assert handler.sent[-1]["payload"]["offset"] == 0


def test_fetcher_log_with_content(tmp_path: Path) -> None:
    """Test /fetcher/log endpoint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    api.FETCHER_LOG_PATH.write_text("fetcher log content")

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/fetcher/log", query={}
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_ops_health(tmp_path: Path) -> None:
    """Test /ops/health endpoint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/ops/health", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert "ok" in handler.sent[-1]["payload"]


def test_ops_history_default_limit(tmp_path: Path) -> None:
    """Test /ops/history with default limit."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/ops/history", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_ops_history_custom_limit(tmp_path: Path) -> None:
    """Test /ops/history with custom limit."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/ops/history", query={"limit": ["50"]}
    )

    assert result is True


def test_ops_history_invalid_limit(tmp_path: Path) -> None:
    """Test /ops/history with invalid limit."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/ops/history", query={"limit": ["invalid"]}
    )

    assert result is True


def test_ops_fetcher_metrics_default(tmp_path: Path) -> None:
    """Test /ops/fetcher-metrics with default window."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/ops/fetcher-metrics", query={}
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_ops_fetcher_metrics_custom_window(tmp_path: Path) -> None:
    """Test /ops/fetcher-metrics with custom window."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/ops/fetcher-metrics",
        query={"windowRuns": ["50"]},
    )

    assert result is True


def test_ops_fetch_report(tmp_path: Path) -> None:
    """Test /ops/fetch-report endpoint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/ops/fetch-report", query={}
    )

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_sync_status(tmp_path: Path) -> None:
    """Test /sync/status endpoint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/sync/status", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200


def test_pipeline_status(tmp_path: Path) -> None:
    """Test /tasks/run-jobs-pipeline-status endpoint."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(
        handler, api=api, path="/tasks/run-jobs-pipeline-status", query={}
    )

    assert result is True


def test_unknown_route_returns_false(tmp_path: Path) -> None:
    """Test that unknown route returns False."""
    store = _FakeDesktopLocalDataStore()
    api = _make_api(tmp_path, store)

    handler = _FakeHandler()
    result = handle_get(handler, api=api, path="/unknown/route", query={})

    assert result is False
