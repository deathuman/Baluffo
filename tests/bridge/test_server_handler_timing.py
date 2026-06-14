from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

from src.bridge.performance_profile import clear_performance_profile, snapshot_performance_profile
from src.bridge.server import httpd
from src.bridge.server.handler import make_handler
from src.shared.timing_counters import clear_counters, snapshot_counters
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, make_stub_bridge_api


class HandlerHarness:
    def __init__(self, handler_cls: type, *, method: str, path: str, body: bytes = b"") -> None:
        self.handler = object.__new__(handler_cls)
        self.handler.command = method
        self.handler.path = path
        self.handler.headers = {"Content-Length": str(len(body))}
        self.handler.rfile = BytesIO(body)
        self.handler.close_connection = False
        self.responses: list[dict[str, Any]] = []
        self.handler.send_json = self.send_json

    def send_json(self, payload: Any, status: int = 200) -> None:
        self.handler._baluffo_last_response_status = int(status)
        self.responses.append({"payload": payload, "status": status})


def setup_function() -> None:
    clear_counters()
    clear_performance_profile()


def teardown_function() -> None:
    clear_counters()
    clear_performance_profile()


def test_handler_records_get_request_timing(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    handler_cls = make_handler(api=api)
    harness = HandlerHarness(handler_cls, method="GET", path="/ops/health")

    harness.handler.do_GET()

    counters = snapshot_counters()
    profile = snapshot_performance_profile()
    assert harness.responses[-1]["status"] == 200
    assert counters["bridge_request_get_ops_health"]["count"] == 1
    assert profile["routeTimings"]["routes"][0]["label"] == "GET /ops/health"


def test_handler_records_post_request_timing_for_not_found(tmp_path: Path) -> None:
    api = make_stub_bridge_api(Path(tmp_path), FakeDesktopLocalDataStore())
    handler_cls = make_handler(api=api)
    harness = HandlerHarness(handler_cls, method="POST", path="/unknown/route", body=b"{}")

    harness.handler.do_POST()

    counters = snapshot_counters()
    profile = snapshot_performance_profile()
    assert harness.responses[-1]["status"] == 404
    assert counters["bridge_request_post_unknown_route"]["count"] == 1
    assert profile["routeTimings"]["routes"][0]["label"] == "POST /unknown/route"
    assert profile["routeTimings"]["routes"][0]["errorCount"] == 1


def test_handler_performance_profile_redacts_query_params(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    handler_cls = make_handler(api=api)
    harness = HandlerHarness(
        handler_cls,
        method="GET",
        path="/ops/task-state?view=summary&token=hidden",
    )

    harness.handler.do_GET()

    profile = snapshot_performance_profile()
    labels = [row["label"] for row in profile["routeTimings"]["routes"]]
    assert "GET /ops/task-state" in labels
    assert all("hidden" not in label and "token" not in label for label in labels)


def test_run_http_server_uses_short_idle_poll_for_owner_shutdown(monkeypatch) -> None:
    created_servers: list[object] = []

    class FakeServer:
        def __init__(self, _address, _handler_cls) -> None:
            self.timeout = 0
            self.handled = 0
            self.closed = False
            created_servers.append(self)

        def handle_request(self) -> None:
            self.handled += 1

        def server_close(self) -> None:
            self.closed = True

    class FakeApi:
        def __init__(self) -> None:
            self.logs: list[tuple[str, str]] = []

        def bridge_log(self, level: str, event: str, **_fields: object) -> None:
            self.logs.append((level, event))

        def should_exit_for_owner_timeout(self) -> bool:
            return True

    monkeypatch.setattr(httpd, "ThreadingHTTPServer", FakeServer)
    api = FakeApi()

    assert (
        httpd.run_http_server(
            api=api,
            host="127.0.0.1",
            port=0,
            handler_cls=object,
        )
        == 0
    )

    server = created_servers[0]
    assert server.timeout == 0.25
    assert server.handled == 1
    assert server.closed is True
    assert ("info", "admin_bridge_owner_timeout_shutdown") in api.logs
