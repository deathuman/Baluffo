from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen

from src.app_version import get_app_version
from src.bridge.pipeline_control_files import (
    abort_request_path,
    write_pipeline_status,
)
from src.container_gateway import _GatewayState, _make_gateway_handler


class _FakeBridgeProcess:
    def __init__(self, exit_code: int | None = None) -> None:
        self.exit_code = exit_code

    def poll(self) -> int | None:
        return self.exit_code


def _serve_gateway(tmp_path: Path, *, bridge_process: _FakeBridgeProcess | None = None):
    state = _GatewayState(
        data_dir=tmp_path / "data",
        static_root=Path(__file__).resolve().parents[1],
        internal_base_url="http://127.0.0.1:9",
        bridge_process=bridge_process or _FakeBridgeProcess(),
    )
    state.data_dir.mkdir(parents=True, exist_ok=True)
    handler_cls = _make_gateway_handler(state)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


class _TinyJsonHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        return

    def do_GET(self) -> None:
        body = json.dumps({"ok": True, "path": self.path}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _serve_internal_bridge():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _TinyJsonHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


def _serve_gateway_with_internal(
    tmp_path: Path, internal_base_url: str, *, bridge_process: _FakeBridgeProcess | None = None
):
    state = _GatewayState(
        data_dir=tmp_path / "data",
        static_root=Path(__file__).resolve().parents[1],
        internal_base_url=internal_base_url,
        bridge_process=bridge_process or _FakeBridgeProcess(),
    )
    state.data_dir.mkdir(parents=True, exist_ok=True)
    handler_cls = _make_gateway_handler(state)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


def _get_json(base_url: str, path: str) -> dict:
    with urlopen(f"{base_url}{path}", timeout=2) as response:
        assert response.status == 200
        return json.loads(response.read().decode("utf-8"))


def test_gateway_ready_does_not_require_internal_bridge(tmp_path: Path) -> None:
    server, base_url = _serve_gateway(tmp_path, bridge_process=_FakeBridgeProcess())
    try:
        payload = _get_json(base_url, "/app/ready")
    finally:
        server.shutdown()
        server.server_close()

    assert payload["ok"] is True
    assert payload["appVersion"] == get_app_version()
    assert payload["status"] == "degraded"
    assert payload["bridge"]["alive"] is True
    assert payload["bridge"]["listening"] is False
    assert payload["runtime"]["mode"] == "container"


def test_gateway_ready_reports_internal_bridge_listening(tmp_path: Path) -> None:
    internal_server, internal_base_url = _serve_internal_bridge()
    gateway_server, base_url = _serve_gateway_with_internal(tmp_path, internal_base_url)
    try:
        payload = _get_json(base_url, "/app/ready")
    finally:
        gateway_server.shutdown()
        gateway_server.server_close()
        internal_server.shutdown()
        internal_server.server_close()

    assert payload["status"] == "healthy"
    assert payload["bridge"]["alive"] is True
    assert payload["bridge"]["listening"] is True


def test_gateway_pipeline_status_reads_control_snapshot(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    write_pipeline_status(
        data_dir,
        {
            "active": True,
            "runId": "pipeline_test",
            "stage": "fetch",
            "progress": {"label": "Fetching job listings", "percent": 50},
            "activeChildren": [
                {
                    "taskType": "fetch",
                    "type": "fetch",
                    "runId": "fetch_test",
                    "active": True,
                    "status": "running",
                    "taskProgress": {"phaseLabel": "Fetch running"},
                    "summary": {"controlPlane": True},
                }
            ],
        },
        now_iso="2026-06-12T20:00:00Z",
    )
    server, base_url = _serve_gateway(tmp_path, bridge_process=_FakeBridgeProcess())
    try:
        payload = _get_json(base_url, "/tasks/run-jobs-pipeline-status")
    finally:
        server.shutdown()
        server.server_close()

    assert payload["active"] is True
    assert payload["runId"] == "pipeline_test"
    assert payload["stage"] == "fetch"
    assert payload["activeChildren"][0]["runId"] == "fetch_test"
    assert payload["activeChildren"][0]["taskProgress"]["phaseLabel"] == "Fetch running"
    assert payload["bridgeAlive"] is True
    assert payload["bridgeListening"] is False


def test_gateway_does_not_static_fallback_admin_api_paths(tmp_path: Path) -> None:
    internal_server, internal_base_url = _serve_internal_bridge()
    gateway_server, base_url = _serve_gateway_with_internal(tmp_path, internal_base_url)
    try:
        payload = _get_json(base_url, "/admin/bootstrap")
    finally:
        gateway_server.shutdown()
        gateway_server.server_close()
        internal_server.shutdown()
        internal_server.server_close()

    assert payload["ok"] is True
    assert payload["path"] == "/admin/bootstrap"


def test_gateway_proxied_responses_have_single_content_length(tmp_path: Path) -> None:
    internal_server, internal_base_url = _serve_internal_bridge()
    gateway_server, base_url = _serve_gateway_with_internal(tmp_path, internal_base_url)
    try:
        with urlopen(f"{base_url}/ops/health", timeout=2) as response:
            payload = json.loads(response.read().decode("utf-8"))
            content_lengths = response.headers.get_all("Content-Length")
    finally:
        gateway_server.shutdown()
        gateway_server.server_close()
        internal_server.shutdown()
        internal_server.server_close()

    assert payload["ok"] is True
    assert payload["path"] == "/ops/health"
    assert content_lengths is not None
    assert len(content_lengths) == 1


def test_gateway_pipeline_abort_queues_when_bridge_is_unreachable(tmp_path: Path) -> None:
    server, base_url = _serve_gateway(tmp_path, bridge_process=_FakeBridgeProcess())
    body = json.dumps(
        {"taskType": "pipeline", "runId": "pipeline_abort_1", "reason": "test"}
    ).encode("utf-8")
    request = Request(
        f"{base_url}/tasks/abort",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=2) as response:
            assert response.status == 202
            payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert payload["abortAccepted"] is True
    assert payload["gatewayAccepted"] is True
    request_path = abort_request_path(tmp_path / "data", "pipeline_abort_1")
    queued = json.loads(request_path.read_text(encoding="utf-8"))
    assert queued["taskType"] == "pipeline"
    assert queued["runId"] == "pipeline_abort_1"


def test_gateway_serves_startup_feed_without_bridge_proxy(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "jobs-unified-startup.json").write_text(
        json.dumps({"jobs": [{"id": "job-1", "title": "One"}]}) + "\n",
        encoding="utf-8",
    )
    server, base_url = _serve_gateway(tmp_path, bridge_process=_FakeBridgeProcess())
    try:
        payload = _get_json(base_url, "/data/jobs-unified-startup.json")
    finally:
        server.shutdown()
        server.server_close()

    rows = payload if isinstance(payload, list) else payload.get("jobs")
    assert isinstance(rows, list)
    assert rows
