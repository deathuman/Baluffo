"""Gateway proxy budget for Admin's first payload.

``/admin/bootstrap`` measures p95 934 ms / max 1036 ms on the bridge side with a
run active. The gateway's old 0.75 s cutoff abandoned those healthy bootstraps
and answered with its own degraded stub, which cost ~1.3 s more because the
fallback makes its own bridge calls — the Admin page then rendered "Admin data
delayed; retrying." on every navigation.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast
from urllib.request import urlopen

from src.container_gateway import (
    ADMIN_BOOTSTRAP_PROXY_TIMEOUT_SECONDS,
    _GatewayState,
    _make_gateway_handler,
)


class _FakeBridgeProcess:
    def poll(self) -> None:
        return None


class _SlowJsonHandler(BaseHTTPRequestHandler):
    """Healthy bridge stub that answers slower than the removed 0.75 s cutoff."""

    delay_s = 0.9

    def log_message(self, format: str, *args: object) -> None:
        return

    def do_GET(self) -> None:
        time.sleep(self.delay_s)
        body = json.dumps({"ok": True, "path": self.path, "slowBridge": True}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _serve(handler_cls: type[BaseHTTPRequestHandler]) -> tuple[ThreadingHTTPServer, str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


def _serve_gateway(tmp_path: Path, internal_base_url: str) -> tuple[ThreadingHTTPServer, str]:
    state = _GatewayState(
        data_dir=tmp_path / "data",
        static_root=Path(__file__).resolve().parents[1],
        internal_base_url=internal_base_url,
        bridge_process=_FakeBridgeProcess(),
    )
    state.data_dir.mkdir(parents=True, exist_ok=True)
    return _serve(_make_gateway_handler(state))


def test_bootstrap_budget_covers_a_slow_but_healthy_bridge(tmp_path: Path) -> None:
    assert ADMIN_BOOTSTRAP_PROXY_TIMEOUT_SECONDS > _SlowJsonHandler.delay_s
    internal_server, internal_base_url = _serve(_SlowJsonHandler)
    gateway_server, base_url = _serve_gateway(tmp_path, internal_base_url)
    try:
        with urlopen(f"{base_url}/admin/bootstrap", timeout=5) as response:
            payload = cast(dict[str, Any], json.loads(response.read().decode("utf-8")))
    finally:
        gateway_server.shutdown()
        gateway_server.server_close()
        internal_server.shutdown()
        internal_server.server_close()

    assert payload.get("slowBridge") is True
    assert payload.get("source") != "container-gateway-fallback"
