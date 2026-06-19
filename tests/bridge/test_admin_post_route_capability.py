from __future__ import annotations

from pathlib import Path
from typing import Any

from src.bridge.routes.post_routes_admin import handle_post
from tests.helpers.bridge_api import FakeHandler


class MinimalAdminPostRouteApi:
    def __init__(self, root: Path) -> None:
        self.APPROVAL_STATE_PATH = root / "approval.json"
        self.DEDUP_REVIEW_STATE_PATH = root / "dedup-review-state.json"
        self.JOBS_FETCH_REPORT_PATH = root / "jobs-fetch-report.json"
        self.SOURCE_POLICY_REVIEW_STATE_PATH = root / "source-policy-review-state.json"
        self.state = {
            "active": [{"id": "active-1", "url": "https://active.test/jobs"}],
            "pending": [{"id": "pending-1", "url": "https://pending.test/jobs"}],
            "rejected": [{"id": "rejected-1", "url": "https://rejected.test/jobs"}],
        }
        self.saved_json: dict[Path, dict[str, Any]] = {}
        self.alert_state: dict[str, Any] = {"acked": {}}
        self.sync_status_updates: list[dict[str, Any]] = []
        self.tombstones: dict[str, Any] = {}

    def abort_task(self, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        return 202, {"ok": True, "abortPayload": payload}

    def add_manual_source(self, url: str) -> dict[str, Any]:
        return {"ok": True, "url": url}

    def bridge_log(self, level: str, event: str, **fields: Any) -> None:
        del level, event, fields

    def compute_ops_health(self) -> dict[str, Any]:
        return {"alerts": [{"id": "alert-1", "severity": "warning"}]}

    def get_discovery_config_payload(self) -> dict[str, Any]:
        return {"ok": True, "savedConfig": {"enabled": True}}

    def get_jobs_pipeline_schedule_payload(self) -> dict[str, Any]:
        return {"ok": True, "savedConfig": {"enabled": False}}

    def get_sync_status_payload(self) -> dict[str, Any]:
        return {"ok": True, "config": {"enabled": True}}

    def load_alert_state(self) -> dict[str, Any]:
        return {"acked": dict(self.alert_state.get("acked", {}))}

    def load_json_object(self, path: Path, default: Any = None) -> dict[str, Any]:
        return dict(self.saved_json.get(path, default if isinstance(default, dict) else {}))

    def load_state(self) -> dict[str, list[dict[str, Any]]]:
        return {key: [dict(row) for row in rows] for key, rows in self.state.items()}

    def load_tombstones(self) -> dict[str, Any]:
        return dict(self.tombstones)

    def move_entries(
        self, rows: list[dict[str, Any]], ids: list[str]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        selected = set(ids)
        moved = [row for row in rows if self.source_identity(row) in selected]
        remaining = [row for row in rows if self.source_identity(row) not in selected]
        return moved, remaining

    def normalize_source_url(self, url: str) -> str:
        return str(url or "").strip().lower().rstrip("/")

    def now_iso(self) -> str:
        return "2026-06-19T10:00:00+00:00"

    def persist_state_and_auto_sync(
        self, state: dict[str, list[dict[str, Any]]], *, reason: str
    ) -> dict[str, list[dict[str, Any]]]:
        del reason
        self.state = {key: [dict(row) for row in rows] for key, rows in state.items()}
        return self.load_state()

    def save_alert_state(self, state: dict[str, Any]) -> None:
        self.alert_state = dict(state)

    def save_json_atomic(self, path: Path, payload: dict[str, Any]) -> None:
        self.saved_json[path] = dict(payload)

    def save_tombstones(self, tombstones: dict[str, Any]) -> None:
        self.tombstones = dict(tombstones)

    def set_sync_status(self, **fields: Any) -> None:
        self.sync_status_updates.append(fields)

    def source_identity(self, row: dict[str, Any]) -> str:
        return str(row.get("id") or "")

    def source_url_fingerprint(self, row: dict[str, Any]) -> str:
        return self.normalize_source_url(str(row.get("url") or ""))

    def start_fetcher_task(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {"started": True, "payload": payload}

    def start_jobs_bootstrap_task(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {"started": True, "payload": payload}

    def start_jobs_pipeline_task(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {"started": True, "payload": payload}

    def start_sync_task(self, action: str, *, reason: str, automatic: bool) -> dict[str, Any]:
        return {"started": True, "action": action, "reason": reason, "automatic": automatic}

    def summarize_state(self, state: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        return {
            "activeCount": len(state.get("active", [])),
            "pendingCount": len(state.get("pending", [])),
            "rejectedCount": len(state.get("rejected", [])),
        }

    def sync_config_status(self) -> dict[str, Any]:
        return {"enabled": True}

    def sync_pull_sources(self) -> dict[str, Any]:
        return {"ok": True, "action": "pull"}

    def sync_push_sources(self) -> dict[str, Any]:
        return {"ok": True, "action": "push"}

    def test_sync_config(self) -> dict[str, Any]:
        return {"ok": True, "action": "test"}

    def trigger_discovery_task(
        self, *, payload: dict[str, Any], route_name: str
    ) -> tuple[int, dict[str, Any]]:
        return 202, {"started": True, "routeName": route_name, "payload": payload}

    def trigger_source_check(self, source_id: str) -> dict[str, Any]:
        return {"started": True, "sourceId": source_id}

    def unique_sources(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        result: list[dict[str, Any]] = []
        for row in rows:
            row_id = self.source_identity(row)
            if row_id in seen:
                continue
            seen.add(row_id)
            result.append(dict(row))
        return result

    def update_jobs_pipeline_schedule(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True, "savedConfig": payload}

    def update_saved_discovery_settings(self, payload: dict[str, Any]) -> None:
        self.saved_json[Path("discovery-config")] = dict(payload)

    def update_saved_sync_settings(self, payload: dict[str, Any]) -> None:
        self.saved_json[Path("sync-config")] = dict(payload)


def _post(api: MinimalAdminPostRouteApi, path: str, payload: dict[str, Any]) -> FakeHandler:
    handler = FakeHandler()
    assert handle_post(handler, api=api, path=path, payload=payload) is True
    return handler


def test_admin_post_routes_accept_minimal_capability_object(tmp_path: Path) -> None:
    api = MinimalAdminPostRouteApi(tmp_path)

    manual = _post(api, "/sources/manual", {"url": "https://studio.test/jobs"})
    assert manual.sent[-1]["payload"]["url"] == "https://studio.test/jobs"

    source_check = _post(api, "/discovery/check-source", {"sourceId": "active-1"})
    assert source_check.sent[-1]["status"] == 200
    assert source_check.sent[-1]["payload"]["sourceId"] == "active-1"

    approved = _post(api, "/registry/approve", {"ids": ["pending-1"]})
    assert approved.sent[-1]["payload"]["approved"] == 1
    assert approved.sent[-1]["payload"]["summary"]["activeCount"] == 2

    pipeline = _post(api, "/tasks/run-jobs-pipeline", {"reason": "manual"})
    assert pipeline.sent[-1]["payload"]["started"] is True

    schedule = _post(api, "/tasks/jobs-pipeline-schedule", {"enabled": True})
    assert schedule.sent[-1]["payload"]["savedConfig"] == {"enabled": True}

    abort = _post(api, "/tasks/abort", {"taskType": "fetch", "runId": "fetch-1"})
    assert abort.sent[-1]["status"] == 202

    sync_task = _post(api, "/tasks/run-sync-pull", {})
    assert sync_task.sent[-1]["payload"]["action"] == "pull"

    bootstrap = _post(api, "/tasks/run-jobs-bootstrap", {})
    assert bootstrap.sent[-1]["payload"]["started"] is True

    fetcher = _post(api, "/tasks/run-fetcher", {"force": True})
    assert fetcher.sent[-1]["payload"]["started"] is True

    discovery_config = _post(api, "/discovery/config", {"enabled": True})
    assert discovery_config.sent[-1]["payload"]["savedConfig"]["enabled"] is True

    alert_ack = _post(api, "/ops/alerts/ack", {"id": "alert-1"})
    assert alert_ack.sent[-1]["payload"] == {"acked": "alert-1", "ok": True}
    assert api.alert_state["acked"]["alert-1"] == "2026-06-19T10:00:00+00:00"

    sync_config = _post(api, "/sync/config", {"enabled": False})
    assert sync_config.sent[-1]["payload"]["config"]["enabled"] is True

    sync_test = _post(api, "/sync/test", {})
    assert sync_test.sent[-1]["payload"]["action"] == "test"

    sync_pull = _post(api, "/sync/pull", {})
    assert sync_pull.sent[-1]["payload"]["action"] == "pull"
