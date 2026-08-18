from __future__ import annotations

from http.server import BaseHTTPRequestHandler
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest

from src.bridge.performance_profile import clear_performance_profile, snapshot_performance_profile
from src.bridge.server import handler as server_handler
from src.bridge.server import httpd
from src.bridge.server.handler import make_handler
from src.shared.timing_counters import clear_counters, snapshot_counters
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, make_stub_bridge_api


class HandlerHarness:
    def __init__(self, handler_cls: type, *, method: str, path: str, body: bytes = b"") -> None:
        self.handler: Any
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


class _ResponseWriterHarness:
    path = "/ops/health"
    command = "GET"
    close_connection = False

    def __init__(self) -> None:
        self.headers: list[tuple[str, str]] = []
        self.response_status: int | None = None
        self.ended = False
        self.wfile = self
        self.body = b""

    def send_response(self, status: int) -> None:
        self.response_status = status

    def send_header(self, key: str, value: str) -> None:
        self.headers.append((key, value))

    def end_headers(self) -> None:
        self.ended = True

    def write(self, body: bytes) -> None:
        self.body += body


class _StatusAssignmentRaisesAttributeError(_ResponseWriterHarness):
    @property
    def _baluffo_last_response_status(self) -> int:
        return 0

    @_baluffo_last_response_status.setter
    def _baluffo_last_response_status(self, _value: int) -> None:
        raise AttributeError("status not writable")


class _StatusAssignmentRaisesAssertion(_ResponseWriterHarness):
    @property
    def _baluffo_last_response_status(self) -> int:
        return 0

    @_baluffo_last_response_status.setter
    def _baluffo_last_response_status(self, _value: int) -> None:
        raise AssertionError("unexpected status bookkeeping bug")


class _WriteRaisesOSError(_ResponseWriterHarness):
    def write(self, _body: bytes) -> None:
        raise OSError("socket write failed")


class _WriteRaisesRuntimeError(_ResponseWriterHarness):
    def write(self, _body: bytes) -> None:
        raise RuntimeError("unexpected writer bug")


class _PathRaisesRuntimeError:
    @property
    def path(self) -> str:
        raise RuntimeError("unexpected path bug")


def setup_function() -> None:
    clear_counters()
    clear_performance_profile()


def teardown_function() -> None:
    clear_counters()
    clear_performance_profile()


def test_request_timing_category_suppresses_missing_path_only() -> None:
    category = server_handler._request_timing_category(object(), "GET")

    assert category == "bridge_request_get_unknown"


def test_request_timing_category_propagates_unexpected_path_failure() -> None:
    with pytest.raises(RuntimeError, match="unexpected path bug"):
        server_handler._request_timing_category(_PathRaisesRuntimeError(), "GET")


def test_send_json_response_suppresses_expected_status_bookkeeping_failure(
    tmp_path: Path,
) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    handler = _StatusAssignmentRaisesAttributeError()

    server_handler._send_json_response(handler, api, {"ok": True}, status=202)

    assert handler.response_status == 202
    assert handler.ended is True
    assert b'"ok": true' in handler.body


def test_send_json_response_propagates_unexpected_status_bookkeeping_failure(
    tmp_path: Path,
) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    handler = _StatusAssignmentRaisesAssertion()

    with pytest.raises(AssertionError, match="unexpected status bookkeeping bug"):
        server_handler._send_json_response(handler, api, {"ok": True}, status=202)

    assert handler.response_status is None


def test_send_json_response_logs_and_reraises_non_disconnect_oserror(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    logs: list[tuple[str, str, dict[str, Any]]] = []
    api.bridge_log = lambda level, event, **fields: logs.append((level, event, fields))
    handler = _WriteRaisesOSError()

    with pytest.raises(OSError, match="socket write failed"):
        server_handler._send_json_response(handler, api, {"ok": True}, status=202)

    assert logs[-1][0] == "error"
    assert logs[-1][1] == "http_response_write_failed"
    assert logs[-1][2]["status"] == 202


def test_send_json_response_does_not_route_unexpected_writer_bug_through_oserror_boundary(
    tmp_path: Path,
) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    handler = _WriteRaisesRuntimeError()

    with pytest.raises(RuntimeError, match="unexpected writer bug"):
        server_handler._send_json_response(handler, api, {"ok": True}, status=202)


def test_send_bytes_response_logs_and_reraises_non_disconnect_oserror(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    logs: list[tuple[str, str, dict[str, Any]]] = []
    api.bridge_log = lambda level, event, **fields: logs.append((level, event, fields))
    handler = _WriteRaisesOSError()

    with pytest.raises(OSError, match="socket write failed"):
        server_handler._send_bytes_response(
            handler,
            api,
            b"payload",
            content_type="application/octet-stream",
            status=206,
        )

    assert logs[-1][0] == "error"
    assert logs[-1][1] == "http_response_write_failed"
    assert logs[-1][2]["status"] == 206


def test_send_bytes_response_does_not_route_unexpected_writer_bug_through_oserror_boundary(
    tmp_path: Path,
) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    handler = _WriteRaisesRuntimeError()

    with pytest.raises(RuntimeError, match="unexpected writer bug"):
        server_handler._send_bytes_response(
            handler,
            api,
            b"payload",
            content_type="application/octet-stream",
            status=206,
        )


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


def test_handler_suppresses_expected_session_activity_failure(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.mark_desktop_session_activity = lambda _path: (_ for _ in ()).throw(  # type: ignore[assignment]
        OSError("session store unavailable")
    )
    handler_cls = make_handler(api=api)
    harness = HandlerHarness(handler_cls, method="GET", path="/ops/health")

    harness.handler.do_GET()

    assert harness.responses[-1]["status"] == 200
    assert harness.responses[-1]["payload"]["ok"] is True


def test_handler_converts_unexpected_session_activity_failure_to_500(
    tmp_path: Path,
) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.mark_desktop_session_activity = lambda _path: (_ for _ in ()).throw(  # type: ignore[assignment]
        AssertionError("unexpected session bookkeeping bug")
    )
    handler_cls = make_handler(api=api)
    harness = HandlerHarness(handler_cls, method="GET", path="/ops/health")

    harness.handler.do_GET()

    assert harness.responses[-1]["status"] == 500
    assert "unexpected session bookkeeping bug" in harness.responses[-1]["payload"]["detail"]


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


def test_handler_converts_post_route_failure_to_500(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.bridge.routes.post_routes as post_routes

    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    logs: list[tuple[str, str, dict[str, object]]] = []
    api.bridge_log = lambda level, event, **fields: logs.append((level, event, fields))
    handler_cls = make_handler(api=api)
    harness = HandlerHarness(handler_cls, method="POST", path="/tasks/run", body=b"{}")

    def failing_post(*_args: object, **_kwargs: object) -> bool:
        raise RuntimeError("post route failed")

    monkeypatch.setattr(post_routes, "handle_post", failing_post)

    harness.handler.do_POST()

    profile = snapshot_performance_profile()
    assert harness.responses[-1]["status"] == 500
    assert harness.responses[-1]["payload"]["detail"] == "post route failed"
    assert profile["routeTimings"]["routes"][0]["label"] == "POST /tasks/run"
    assert profile["routeTimings"]["routes"][0]["errorCount"] == 1
    assert any(
        level == "error"
        and event == "http_post_handler_failed"
        and fields["path"] == "/tasks/run"
        and fields["error"] == "post route failed"
        for level, event, fields in logs
    )


def test_handler_does_not_swallow_post_keyboard_interrupt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.bridge.routes.post_routes as post_routes

    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    handler_cls = make_handler(api=api)
    harness = HandlerHarness(handler_cls, method="POST", path="/tasks/run", body=b"{}")

    def interrupting_post(*_args: object, **_kwargs: object) -> bool:
        raise KeyboardInterrupt

    monkeypatch.setattr(post_routes, "handle_post", interrupting_post)

    with pytest.raises(KeyboardInterrupt):
        harness.handler.do_POST()

    profile = snapshot_performance_profile()
    assert profile["routeTimings"]["routes"][0]["label"] == "POST /tasks/run"
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
    created_servers: list[Any] = []

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
            handler_cls=BaseHTTPRequestHandler,
        )
        == 0
    )

    server = created_servers[0]
    assert server.timeout == 0.25
    assert server.handled == 1
    assert server.closed is True
    assert ("info", "admin_bridge_owner_timeout_shutdown") in api.logs


def test_run_http_server_logs_expected_on_started_failure(monkeypatch) -> None:
    created_servers: list[Any] = []

    class FakeServer:
        def __init__(self, _address, _handler_cls) -> None:
            self.timeout = 0
            self.closed = False
            created_servers.append(self)

        def handle_request(self) -> None:
            pass

        def server_close(self) -> None:
            self.closed = True

    class FakeApi:
        def __init__(self) -> None:
            self.logs: list[tuple[str, str, dict[str, object]]] = []

        def bridge_log(self, level: str, event: str, **fields: object) -> None:
            self.logs.append((level, event, fields))

        def should_exit_for_owner_timeout(self) -> bool:
            return True

    def fail_on_started() -> None:
        raise OSError("startup maintenance unavailable")

    monkeypatch.setattr(httpd, "ThreadingHTTPServer", FakeServer)
    api = FakeApi()

    assert (
        httpd.run_http_server(
            api=api,
            host="127.0.0.1",
            port=0,
            handler_cls=BaseHTTPRequestHandler,
            on_started=fail_on_started,
        )
        == 0
    )

    assert created_servers[0].closed is True
    assert any(
        level == "warn"
        and event == "admin_bridge_on_started_failed"
        and fields["error"] == "startup maintenance unavailable"
        for level, event, fields in api.logs
    )


def test_run_http_server_propagates_unexpected_on_started_failure(monkeypatch) -> None:
    created_servers: list[Any] = []

    class FakeServer:
        def __init__(self, _address, _handler_cls) -> None:
            self.closed = False
            created_servers.append(self)

        def handle_request(self) -> None:
            raise AssertionError("server loop should not run")

        def server_close(self) -> None:
            self.closed = True

    class FakeApi:
        def __init__(self) -> None:
            self.logs: list[tuple[str, str]] = []

        def bridge_log(self, level: str, event: str, **_fields: object) -> None:
            self.logs.append((level, event))

    def fail_on_started() -> None:
        raise TypeError("unexpected callback bug")

    monkeypatch.setattr(httpd, "ThreadingHTTPServer", FakeServer)
    api = FakeApi()

    with pytest.raises(TypeError, match="unexpected callback bug"):
        httpd.run_http_server(
            api=api,
            host="127.0.0.1",
            port=0,
            handler_cls=BaseHTTPRequestHandler,
            on_started=fail_on_started,
        )

    assert created_servers[0].closed is True
    assert ("info", "admin_bridge_stopped") in api.logs


def test_run_http_server_does_not_swallow_setup_keyboard_interrupt(monkeypatch) -> None:
    created_servers: list[Any] = []

    class FakeServer:
        def __init__(self, _address, _handler_cls) -> None:
            self.closed = False
            created_servers.append(self)

        def handle_request(self) -> None:
            raise AssertionError("server loop should not run")

        def server_close(self) -> None:
            self.closed = True

    class FakeApi:
        def __init__(self) -> None:
            self.logs: list[tuple[str, str]] = []

        def bridge_log(self, level: str, event: str, **_fields: object) -> None:
            self.logs.append((level, event))

    def interrupt_on_started() -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(httpd, "ThreadingHTTPServer", FakeServer)
    api = FakeApi()

    with pytest.raises(KeyboardInterrupt):
        httpd.run_http_server(
            api=api,
            host="127.0.0.1",
            port=0,
            handler_cls=BaseHTTPRequestHandler,
            on_started=interrupt_on_started,
        )

    assert created_servers[0].closed is True
    assert ("info", "admin_bridge_shutdown_requested") not in api.logs
    assert ("info", "admin_bridge_stopped") in api.logs
