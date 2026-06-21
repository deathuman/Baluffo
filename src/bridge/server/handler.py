"""Bridge HTTP request handler.

AI boundary owns: HTTP method dispatch, static serving handoff, and route entrypoint integration.
AI boundary implement in: this file for request handler plumbing only; route behavior belongs in route leaves and services.
AI boundary search before contracts: bridge route inventory, static file server, container runtime, and admin bridge API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused bridge handler/route tests.
"""

from __future__ import annotations

import json
import time
import traceback
from http.server import BaseHTTPRequestHandler
from typing import Any, Protocol
from urllib.parse import parse_qs, urlparse

from src.bridge.container_mode import is_container_runtime
from src.bridge.performance_profile import record_route_duration
from src.bridge.request_utils import read_json_from_request
from src.bridge.routes.error_boundary import run_route_boundary
from src.shared.timing_counters import normalize_counter_category, time_block


class StaticGetService(Protocol):
    def handle_get(self, handler: Any, *, path: str) -> bool: ...


class ServerHandlerApi(Protocol):
    runtime_config: Any

    def bridge_log(self, level: str, event: str, **fields: Any) -> None: ...

    def mark_desktop_session_activity(self, path: str) -> None: ...


_EXPECTED_HANDLER_ROUTE_PATH_EXCEPTIONS = (AttributeError, TypeError, ValueError)
_EXPECTED_HANDLER_STATUS_EXCEPTIONS = AttributeError
_EXPECTED_HANDLER_BOOKKEEPING_EXCEPTIONS = (
    AttributeError,
    OSError,
    TypeError,
    ValueError,
)


def _is_expected_client_disconnect(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
            return True
        winerror = getattr(current, "winerror", None)
        if isinstance(winerror, int) and winerror in {10053, 10054}:
            return True
        errno = getattr(current, "errno", None)
        if isinstance(errno, int) and errno in {32, 104}:
            return True
        current = current.__cause__ or current.__context__
    return False


def _request_timing_category(handler: BaseHTTPRequestHandler, method: str, path: str = "") -> str:
    route_path = path or ""
    if not route_path:
        try:
            route_path = _route_path(handler)
        except _EXPECTED_HANDLER_ROUTE_PATH_EXCEPTIONS:
            route_path = ""
    route_token = normalize_counter_category(route_path)
    return f"bridge_request_{str(method or '').strip().lower() or 'unknown'}_{route_token}"


def _handle_response_write_exception(
    handler: BaseHTTPRequestHandler,
    api: ServerHandlerApi,
    exc: BaseException,
    *,
    status: int,
) -> bool:
    if _is_expected_client_disconnect(exc):
        handler.close_connection = True
        return True
    route_path = ""
    try:
        route_path = _route_path(handler)
    except _EXPECTED_HANDLER_ROUTE_PATH_EXCEPTIONS:
        route_path = ""
    try:
        api.bridge_log(
            "error",
            "http_response_write_failed",
            method=getattr(handler, "command", ""),
            path=route_path or getattr(handler, "path", ""),
            status=int(status),
            error=str(exc),
        )
    except _EXPECTED_HANDLER_BOOKKEEPING_EXCEPTIONS:
        pass
    return False


def _route_path(handler: BaseHTTPRequestHandler) -> str:
    # Defensive normalization: some clients/environments can introduce
    # whitespace/control characters that otherwise cause routes to miss.
    return str(urlparse(handler.path).path or "").strip()


def _route_query(handler: BaseHTTPRequestHandler) -> dict[str, list[str]]:
    return parse_qs(urlparse(handler.path).query)


def _send_cors_headers(handler: BaseHTTPRequestHandler, api: ServerHandlerApi) -> None:
    if is_container_runtime(api):
        return
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
    handler.send_header("Access-Control-Allow-Headers", "Content-Type")


def _send_json_response(
    handler: BaseHTTPRequestHandler,
    api: ServerHandlerApi,
    payload: Any,
    *,
    status: int = 200,
) -> None:
    # Some payloads may contain lone surrogates or other encoding edge cases;
    # retry with `ensure_ascii=True` so we can always respond.
    try:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    except UnicodeEncodeError:
        body = json.dumps(payload, ensure_ascii=True, default=str).encode("utf-8")
    try:
        try:
            handler._baluffo_last_response_status = int(status)
        except _EXPECTED_HANDLER_STATUS_EXCEPTIONS:
            pass
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Cache-Control", "no-store")
        _send_cors_headers(handler, api)
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)
    except OSError as exc:
        if _handle_response_write_exception(handler, api, exc, status=status):
            return
        raise


def _send_bytes_response(
    handler: BaseHTTPRequestHandler,
    api: ServerHandlerApi,
    body: bytes,
    *,
    content_type: str,
    filename: str = "",
    disposition: str = "inline",
    status: int = 200,
    cache_control: str = "no-store",
    content_encoding: str = "",
) -> None:
    try:
        try:
            handler._baluffo_last_response_status = int(status)
        except _EXPECTED_HANDLER_STATUS_EXCEPTIONS:
            pass
        handler.send_response(status)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Cache-Control", str(cache_control or "no-store"))
        if not is_container_runtime(api):
            handler.send_header("Access-Control-Allow-Origin", "*")
        if content_encoding:
            handler.send_header("Content-Encoding", str(content_encoding))
        handler.send_header("Content-Length", str(len(body)))
        if filename:
            safe_filename = str(filename).replace('"', "")
            safe_disposition = (
                "attachment" if str(disposition).lower() == "attachment" else "inline"
            )
            handler.send_header(
                "Content-Disposition", f'{safe_disposition}; filename="{safe_filename}"'
            )
        handler.end_headers()
        handler.wfile.write(body)
    except OSError as exc:
        if _handle_response_write_exception(handler, api, exc, status=status):
            return
        raise


def _log_request_message(
    handler: BaseHTTPRequestHandler, api: ServerHandlerApi, format: str, args: tuple[Any, ...]
) -> None:
    runtime_config = getattr(api, "runtime_config", None)
    if runtime_config is not None and bool(getattr(runtime_config, "quiet_requests", False)):
        return
    try:
        message = format % args
    except (TypeError, ValueError):
        message = format
    api.bridge_log(
        "debug",
        "http_request",
        method=getattr(handler, "command", ""),
        path=handler.path,
        detail=message,
    )


def _handle_get_request(
    handler: BaseHTTPRequestHandler,
    api: ServerHandlerApi,
    static_service: StaticGetService | None,
) -> None:
    path = ""
    started_at = time.perf_counter()
    failed = False

    def _run_get_route() -> None:
        nonlocal path
        path = _route_path(handler)
        try:
            api.mark_desktop_session_activity(path)
        except _EXPECTED_HANDLER_BOOKKEEPING_EXCEPTIONS:
            pass
        query = _route_query(handler)
        try:
            api.bridge_log(
                "info",
                "http_get_route",
                rawPath=getattr(handler, "path", ""),
                routePath=path,
            )
        except _EXPECTED_HANDLER_BOOKKEEPING_EXCEPTIONS:
            pass

        from src.bridge.routes.get_routes import handle_get

        if handle_get(handler, api=api, path=path, query=query):
            return
        if static_service is not None and static_service.handle_get(handler, path=path):
            return
        handler.send_json({"error": "Not found"}, status=404)

    def _send_get_error(exc: Exception) -> None:
        nonlocal failed
        failed = True
        try:
            api.bridge_log("error", "http_get_handler_failed", path=path, error=str(exc))
        except _EXPECTED_HANDLER_BOOKKEEPING_EXCEPTIONS:
            pass
        handler.send_json(
            {
                "error": "Internal server error",
                "detail": str(exc),
                "traceback": traceback.format_exc(),
            },
            status=500,
        )

    with time_block(_request_timing_category(handler, "get")):
        try:
            run_route_boundary(
                handler,
                _run_get_route,
                error_status=500,
                error_payload=lambda exc: {
                    "error": "Internal server error",
                    "detail": str(exc),
                    "traceback": traceback.format_exc(),
                },
                error_sender=_send_get_error,
            )
        finally:
            status = getattr(handler, "_baluffo_last_response_status", 0)
            record_route_duration(
                "GET",
                path or getattr(handler, "path", ""),
                (time.perf_counter() - started_at) * 1000,
                status=status,
                error=failed,
            )


def _handle_post_request(handler: BaseHTTPRequestHandler, api: ServerHandlerApi) -> None:
    path = ""
    started_at = time.perf_counter()
    failed = False

    def _run_post_route() -> None:
        nonlocal path
        path = _route_path(handler)
        try:
            api.mark_desktop_session_activity(path)
        except _EXPECTED_HANDLER_BOOKKEEPING_EXCEPTIONS:
            pass
        payload = read_json_from_request(handler)
        from src.bridge.routes.post_routes import handle_post

        if handle_post(handler, api=api, path=path, payload=payload):
            return
        handler.send_json({"error": "Not found"}, status=404)

    def _send_post_error(exc: Exception) -> None:
        nonlocal failed
        failed = True
        try:
            api.bridge_log(
                "error",
                "http_post_handler_failed",
                path=path,
                error=str(exc),
                detail=traceback.format_exc(),
            )
        except _EXPECTED_HANDLER_BOOKKEEPING_EXCEPTIONS:
            pass
        handler.send_json(
            {
                "error": "Internal server error",
                "detail": str(exc),
                "traceback": traceback.format_exc(),
            },
            status=500,
        )

    with time_block(_request_timing_category(handler, "post")):
        try:
            run_route_boundary(
                handler,
                _run_post_route,
                error_status=500,
                error_payload=lambda exc: {
                    "error": "Internal server error",
                    "detail": str(exc),
                    "traceback": traceback.format_exc(),
                },
                error_sender=_send_post_error,
            )
        except BaseException:
            failed = True
            raise
        finally:
            status = getattr(handler, "_baluffo_last_response_status", 0)
            record_route_duration(
                "POST",
                path or getattr(handler, "path", ""),
                (time.perf_counter() - started_at) * 1000,
                status=status,
                error=failed,
            )


def _handle_options_request(handler: BaseHTTPRequestHandler, api: ServerHandlerApi) -> None:
    path = ""
    started_at = time.perf_counter()
    failed = False
    with time_block(_request_timing_category(handler, "options")):
        try:
            path = _route_path(handler)
            handler.send_json({"ok": True})
        except BaseException:
            failed = True
            raise
        finally:
            status = getattr(handler, "_baluffo_last_response_status", 0)
            record_route_duration(
                "OPTIONS",
                path or getattr(handler, "path", ""),
                (time.perf_counter() - started_at) * 1000,
                status=status,
                error=failed,
            )


def make_handler(
    *,
    api: ServerHandlerApi,
    static_service: StaticGetService | None = None,
) -> type[BaseHTTPRequestHandler]:
    """Create a request handler bound to the active bridge API instance."""

    class Handler(BaseHTTPRequestHandler):
        def _request_timing_category(self, method: str, path: str = "") -> str:
            return _request_timing_category(self, method, path)

        def _handle_response_write_exception(self, exc: BaseException, *, status: int) -> bool:
            return _handle_response_write_exception(self, api, exc, status=status)

        def _route_path(self) -> str:
            return _route_path(self)

        def _route_query(self) -> dict[str, list[str]]:
            return _route_query(self)

        def _send_cors_headers(self) -> None:
            _send_cors_headers(self, api)

        def send_json(self, payload: Any, status: int = 200) -> None:
            _send_json_response(self, api, payload, status=status)

        def _send_json(self, payload: Any, status: int = 200) -> None:
            self.send_json(payload, status=status)

        def send_bytes(
            self,
            body: bytes,
            *,
            content_type: str,
            filename: str = "",
            disposition: str = "inline",
            status: int = 200,
            cache_control: str = "no-store",
            content_encoding: str = "",
        ) -> None:
            _send_bytes_response(
                self,
                api,
                body,
                content_type=content_type,
                filename=filename,
                disposition=disposition,
                status=status,
                cache_control=cache_control,
                content_encoding=content_encoding,
            )

        def _send_bytes(
            self,
            body: bytes,
            *,
            content_type: str,
            filename: str = "",
            disposition: str = "inline",
            status: int = 200,
            cache_control: str = "no-store",
            content_encoding: str = "",
        ) -> None:
            self.send_bytes(
                body,
                content_type=content_type,
                filename=filename,
                disposition=disposition,
                status=status,
                cache_control=cache_control,
                content_encoding=content_encoding,
            )

        def log_message(self, format: str, *args: Any) -> None:
            _log_request_message(self, api, format, args)

        def do_OPTIONS(self) -> None:
            _handle_options_request(self, api)

        def do_GET(self) -> None:
            _handle_get_request(self, api, static_service)

        def do_POST(self) -> None:
            _handle_post_request(self, api)

    return Handler
