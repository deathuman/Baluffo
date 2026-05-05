from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

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
        self.responses.append({"payload": payload, "status": status})


def setup_function() -> None:
    clear_counters()


def teardown_function() -> None:
    clear_counters()


def test_handler_records_get_request_timing(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    handler_cls = make_handler(api=api)
    harness = HandlerHarness(handler_cls, method="GET", path="/ops/health")

    harness.handler.do_GET()

    counters = snapshot_counters()
    assert harness.responses[-1]["status"] == 200
    assert counters["bridge_request_get_ops_health"]["count"] == 1


def test_handler_records_post_request_timing_for_not_found(tmp_path: Path) -> None:
    api = make_stub_bridge_api(Path(tmp_path), FakeDesktopLocalDataStore())
    handler_cls = make_handler(api=api)
    harness = HandlerHarness(handler_cls, method="POST", path="/unknown/route", body=b"{}")

    harness.handler.do_POST()

    counters = snapshot_counters()
    assert harness.responses[-1]["status"] == 404
    assert counters["bridge_request_post_unknown_route"]["count"] == 1
