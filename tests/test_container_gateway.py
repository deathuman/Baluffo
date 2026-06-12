from __future__ import annotations

import json
import threading
from http.server import ThreadingHTTPServer
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
    assert payload["bridge"]["alive"] is True
    assert payload["runtime"]["mode"] == "container"


def test_gateway_pipeline_status_reads_control_snapshot(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    write_pipeline_status(
        data_dir,
        {
            "active": True,
            "runId": "pipeline_test",
            "stage": "fetch",
            "progress": {"label": "Fetching job listings", "percent": 50},
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
    assert payload["bridgeAlive"] is True


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

    assert isinstance(payload, list)
    assert payload
