from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

from src.bridge.api import BridgeApi
from src.bridge.registry_service import RegistryPaths, RegistryService
from src.bridge.routes.get_routes import handle_get
from src.bridge.routes.post_routes import handle_post
from src.bridge.server.handler import make_handler


@dataclass
class _RuntimeConfig:
    host: str = "127.0.0.1"
    port: int = 0
    quiet_requests: bool = True
    desktop_mode: bool = True
    owner_mode: str = ""
    owner_token: str = ""
    desktop_session_id: str = ""
    started_by: str = ""
    owner_idle_timeout_s: float = 0.0
    root: Any = None
    data_dir: Any = None


class _FakeHandler:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    def send_json(self, payload: Any, status: int = 200) -> None:
        self.sent.append({"status": int(status), "payload": payload})


class _FakeDesktopLocalDataStore:
    def __init__(self) -> None:
        self.sign_in_calls: list[str] = []

    def sign_in(self, name: str) -> dict[str, Any]:
        self.sign_in_calls.append(str(name))
        return {"uid": "u1", "name": str(name)}


class _DisconnectingWriter:
    def write(self, _body: bytes) -> int:
        raise ConnectionAbortedError(
            10053,
            "An established connection was aborted by the software in your host machine",
        )


def _make_api(tmp_path: Path) -> BridgeApi:
    store = _FakeDesktopLocalDataStore()

    def load_state() -> dict[str, list[dict[str, Any]]]:
        return {
            "active": [{"adapter": "static", "listing_url": "https://example.com/jobs"}],
            "pending": [],
            "rejected": [],
        }

    def summarize_state(state: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
        return {
            "activeCount": len(state.get("active") or []),
            "pendingCount": len(state.get("pending") or []),
            "rejectedCount": len(state.get("rejected") or []),
        }

    api = BridgeApi(
        runtime_config=_RuntimeConfig(),
        DISCOVERY_REPORT_PATH=tmp_path / "discovery-report.json",
        JOBS_FETCH_REPORT_PATH=tmp_path / "jobs-fetch-report.json",
        APPROVAL_STATE_PATH=tmp_path / "approval.json",
        DISCOVERY_LOG_PATH=tmp_path / "discovery.log",
        FETCHER_LOG_PATH=tmp_path / "fetcher.log",
        STARTUP_METRICS_PATH=tmp_path / "startup-metrics.jsonl",
    )
    api.desktop_local_data_store = lambda: store  # type: ignore[assignment]
    api.get_desktop_session_payload = lambda: {  # type: ignore[assignment]
        "sessionId": "desktop-session-1",
        "ownerToken": "desktop-owner-1",
        "lastActivityAt": "2024-01-01T00:00:00Z",
    }
    api.load_state = load_state  # type: ignore[assignment]
    api.summarize_state = summarize_state  # type: ignore[assignment]
    api.get_registry_summary_payload = lambda: {  # type: ignore[assignment]
        **summarize_state(load_state()),
        "hiddenPendingCount": 0,
        "authorityMode": "json",
        "updatedAt": "2026-06-03T00:00:00+00:00",
    }
    api.compute_ops_health = lambda: {"ok": True, "detail": "unit-test"}  # type: ignore[assignment]
    return api


def _make_disconnecting_get_handler(api: BridgeApi, path: str):
    handler_cls = make_handler(api=api)
    handler = handler_cls.__new__(handler_cls)
    handler.path = path
    handler.command = "GET"
    handler.close_connection = False
    handler.wfile = _DisconnectingWriter()
    handler.send_response = lambda *_args, **_kwargs: None
    handler.send_header = lambda *_args, **_kwargs: None
    handler.end_headers = lambda: None
    return handler


def test_get_routes_smoke_ops_health_and_registry_summary(tmp_path: Path) -> None:
    api = _make_api(tmp_path)
    handler = _FakeHandler()

    assert handle_get(handler, api=api, path="/ops/health", query={}) is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True

    assert handle_get(handler, api=api, path="/registry/summary", query={}) is True
    payload = handler.sent[-1]["payload"]
    assert "summary" in payload
    assert int(payload["summary"]["activeCount"]) == 1


def test_get_routes_discovery_report_success_and_json_serializable(tmp_path: Path) -> None:
    api = _make_api(tmp_path)
    handler = _FakeHandler()

    # Minimal report payload that matches the normalizer expectations.
    (api.DISCOVERY_REPORT_PATH).write_text(
        json.dumps(
            {
                "schemaVersion": "1.0",
                "mode": "dynamic",
                "startedAt": "2026-01-01T00:00:00Z",
                "finishedAt": "2026-01-01T00:10:00Z",
                "summary": {
                    "queuedCandidateCount": 0,
                    "foundEndpointCount": 0,
                    "probedCandidateCount": 0,
                    "failedProbeCount": 0,
                },
                "candidates": [],
                "failures": [],
                "topFailures": [],
                "outputs": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    assert handle_get(handler, api=api, path="/discovery/report", query={}) is True
    assert handler.sent[-1]["status"] == 200
    # Ensure we can JSON-encode whatever the route returned.
    json.dumps(handler.sent[-1]["payload"])


def test_get_routes_discovery_candidates_returns_persisted_rows(tmp_path: Path) -> None:
    api = _make_api(tmp_path)
    api.DISCOVERY_CANDIDATES_PATH = tmp_path / "source-discovery-candidates.json"
    handler = _FakeHandler()

    api.DISCOVERY_CANDIDATES_PATH.write_text(
        json.dumps(
            [
                {"id": "p1", "name": "One", "jobsFound": 2},
                "ignored",
                {"id": "p2", "name": "Two", "jobsFound": 0},
            ]
        ),
        encoding="utf-8",
    )

    assert handle_get(handler, api=api, path="/discovery/candidates", query={}) is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["count"] == 2
    assert payload["candidates"][0]["id"] == "p1"


def test_get_routes_discovery_report_never_drops_connection_on_error(tmp_path: Path) -> None:
    api = _make_api(tmp_path)
    handler = _FakeHandler()

    # Force a failure in the loader so the route must return a 500 JSON body.
    def _broken_loader(*_a: Any, **_kw: Any) -> dict[str, Any]:  # noqa: ANN001
        raise RuntimeError("boom")

    with patch("src.source_registry_io.load_runtime_evidence", side_effect=_broken_loader):
        assert handle_get(handler, api=api, path="/discovery/report", query={}) is True
    assert handler.sent[-1]["status"] == 500
    assert handler.sent[-1]["payload"]["error"] == "failed_to_load_discovery_report"


def test_handler_swallows_client_disconnect_for_json_get_response(tmp_path: Path) -> None:
    api = _make_api(tmp_path)
    log_calls: list[tuple[str, dict[str, Any]]] = []

    def bridge_log(level: str, message: str, **fields: Any) -> None:
        log_calls.append((message, {"level": level, **fields}))

    api.bridge_log = bridge_log  # type: ignore[assignment]
    handler = _make_disconnecting_get_handler(api, "/registry/summary")

    handler.do_GET()

    assert handler.close_connection is True
    assert not any(message == "http_response_write_failed" for message, _fields in log_calls)
    assert not any(message == "http_get_handler_failed" for message, _fields in log_calls)


def test_handler_swallows_client_disconnect_for_bytes_get_response(tmp_path: Path) -> None:
    api = _make_api(tmp_path)
    log_calls: list[tuple[str, dict[str, Any]]] = []

    def bridge_log(level: str, message: str, **fields: Any) -> None:
        log_calls.append((message, {"level": level, **fields}))

    api.bridge_log = bridge_log  # type: ignore[assignment]
    (api.DISCOVERY_REPORT_PATH).write_text(
        json.dumps(
            {
                "schemaVersion": "1.0",
                "mode": "dynamic",
                "startedAt": "2026-01-01T00:00:00Z",
                "finishedAt": "2026-01-01T00:10:00Z",
                "summary": {
                    "queuedCandidateCount": 0,
                    "foundEndpointCount": 0,
                    "probedCandidateCount": 0,
                    "failedProbeCount": 0,
                },
                "candidates": [],
                "failures": [],
                "topFailures": [],
                "outputs": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    handler = _make_disconnecting_get_handler(api, "/discovery/report")

    handler.do_GET()

    assert handler.close_connection is True
    assert not any(message == "http_response_write_failed" for message, _fields in log_calls)
    assert not any(message == "http_get_handler_failed" for message, _fields in log_calls)
    assert not any(message == "discovery_report_route_failed" for message, _fields in log_calls)


def test_post_routes_smoke_desktop_sign_in(tmp_path: Path) -> None:
    api = _make_api(tmp_path)
    handler = _FakeHandler()

    assert (
        handle_post(handler, api=api, path="/desktop-local-data/sign-in", payload={"name": "Alice"})
        is True
    )
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["ok"] is True
    assert payload["user"]["name"] == "Alice"


def test_post_routes_run_discovery_passes_payload_by_keyword(tmp_path: Path) -> None:
    api = _make_api(tmp_path)
    handler = _FakeHandler()
    calls: list[dict[str, Any]] = []

    def _trigger_discovery_task(
        *, route_name: str, payload: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        calls.append({"route_name": route_name, "payload": payload})
        return 200, {
            "started": True,
            "route": route_name,
            "preset": str((payload or {}).get("preset") or ""),
        }

    api.trigger_discovery_task = _trigger_discovery_task  # type: ignore[assignment]

    assert (
        handle_post(handler, api=api, path="/tasks/run-discovery", payload={"preset": "uncapped"})
        is True
    )
    assert calls == [{"route_name": "/tasks/run-discovery", "payload": {"preset": "uncapped"}}]
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["started"] is True


def test_bridge_api_defaults_expose_real_registry_identity_helpers(tmp_path: Path) -> None:
    api = _make_api(tmp_path)
    row = {"adapter": "static", "listing_url": "https://example.com/jobs?ref=1"}
    assert api.source_identity(row)
    assert api.source_url_fingerprint(row) == "https://example.com/jobs"
    assert (
        api.normalize_source_url("HTTPS://Example.com/jobs/?ref=1#frag")
        == "https://example.com/jobs"
    )
    assert len(api.unique_sources([row, dict(row)])) == 1


def test_bridge_api_registry_service_wires_identity_helpers(tmp_path: Path) -> None:
    registry = RegistryService(
        paths=RegistryPaths(
            active=tmp_path / "active.json",
            pending=tmp_path / "pending.json",
            rejected=tmp_path / "rejected.json",
        ),
        default_active=[],
        normalize_manual_static=lambda row: row,
    )
    api = BridgeApi(
        runtime_config=_RuntimeConfig(),
        DISCOVERY_REPORT_PATH=tmp_path / "discovery-report.json",
        JOBS_FETCH_REPORT_PATH=tmp_path / "jobs-fetch-report.json",
        APPROVAL_STATE_PATH=tmp_path / "approval.json",
        DISCOVERY_LOG_PATH=tmp_path / "discovery.log",
        FETCHER_LOG_PATH=tmp_path / "fetcher.log",
        STARTUP_METRICS_PATH=tmp_path / "startup-metrics.jsonl",
        registry=registry,
    )
    row = {"adapter": "static", "listing_url": "https://example.com/jobs/"}
    assert api.source_identity(row) == registry.source_identity(row)
    assert api.source_url_fingerprint(row) == registry.source_url_fingerprint(row)
    assert api.normalize_source_url(
        "https://example.com/jobs/?ref=1"
    ) == registry.normalize_source_url("https://example.com/jobs/?ref=1")
