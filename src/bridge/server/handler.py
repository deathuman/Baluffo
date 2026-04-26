from __future__ import annotations

import json
import traceback
from contextlib import suppress
from http.server import BaseHTTPRequestHandler
from typing import Any
from urllib.parse import parse_qs, urlparse

from src.bridge.api import BridgeApi
from src.bridge.request_utils import read_json_from_request


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


def make_handler(*, api: BridgeApi) -> type[BaseHTTPRequestHandler]:
    """Create a request handler bound to the active BridgeApi instance."""

    class Handler(BaseHTTPRequestHandler):
        def _handle_response_write_exception(self, exc: BaseException, *, status: int) -> bool:
            if _is_expected_client_disconnect(exc):
                self.close_connection = True
                return True
            route_path = ""
            with suppress(Exception):
                route_path = self._route_path()
            with suppress(Exception):
                api.bridge_log(
                    "error",
                    "http_response_write_failed",
                    method=getattr(self, "command", ""),
                    path=route_path or getattr(self, "path", ""),
                    status=int(status),
                    error=str(exc),
                )
            return False

        def _route_path(self) -> str:
            # Defensive normalization: some clients/environments can introduce
            # whitespace/control characters that otherwise cause routes to miss.
            return str(urlparse(self.path).path or "").strip()

        def _route_query(self) -> dict[str, list[str]]:
            return parse_qs(urlparse(self.path).query)

        def send_json(self, payload: Any, status: int = 200) -> None:
            # Some payloads may contain lone surrogates or other encoding edge
            # cases; retry with `ensure_ascii=True` so we can always respond.
            try:
                body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
            except UnicodeEncodeError:
                body = json.dumps(payload, ensure_ascii=True, default=str).encode("utf-8")
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:  # noqa: BLE001
                if self._handle_response_write_exception(exc, status=status):
                    return
                raise

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
        ) -> None:
            try:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Cache-Control", "no-store")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Content-Length", str(len(body)))
                if filename:
                    safe_filename = str(filename).replace('"', "")
                    safe_disposition = (
                        "attachment" if str(disposition).lower() == "attachment" else "inline"
                    )
                    self.send_header(
                        "Content-Disposition", f'{safe_disposition}; filename="{safe_filename}"'
                    )
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:  # noqa: BLE001
                if self._handle_response_write_exception(exc, status=status):
                    return
                raise

        def _send_bytes(
            self,
            body: bytes,
            *,
            content_type: str,
            filename: str = "",
            disposition: str = "inline",
            status: int = 200,
        ) -> None:
            self.send_bytes(
                body,
                content_type=content_type,
                filename=filename,
                disposition=disposition,
                status=status,
            )

        def log_message(self, format: str, *args: Any) -> None:
            runtime_config = getattr(api, "runtime_config", None)
            if runtime_config is not None and bool(
                getattr(runtime_config, "quiet_requests", False)
            ):
                return
            try:
                message = format % args
            except (TypeError, ValueError):
                message = format
            api.bridge_log(
                "debug",
                "http_request",
                method=getattr(self, "command", ""),
                path=self.path,
                detail=message,
            )

        def do_OPTIONS(self) -> None:
            self.send_json({"ok": True})

        def do_GET(self) -> None:
            path = ""
            try:
                path = self._route_path()
                query = self._route_query()
                with suppress(Exception):
                    api.bridge_log(
                        "info", "http_get_route", rawPath=getattr(self, "path", ""), routePath=path
                    )

                from src.bridge.routes.get_routes import handle_get

                if handle_get(self, api=api, path=path, query=query):
                    return
                self.send_json({"error": "Not found"}, status=404)
            except Exception as exc:  # noqa: BLE001
                # Logging must never prevent the error response from being sent.
                with suppress(Exception):
                    api.bridge_log("error", "http_get_handler_failed", path=path, error=str(exc))
                self.send_json(
                    {
                        "error": "Internal server error",
                        "detail": str(exc),
                        "traceback": traceback.format_exc(),
                    },
                    status=500,
                )

        def do_POST(self) -> None:
            path = self._route_path()
            payload = read_json_from_request(self)
            from src.bridge.routes.post_routes import handle_post

            try:
                if handle_post(self, api=api, path=path, payload=payload):
                    return
                self.send_json({"error": "Not found"}, status=404)
            except BaseException as exc:  # noqa: BLE001
                # Logging must never prevent the error response from being sent.
                with suppress(Exception):
                    api.bridge_log(
                        "error",
                        "http_post_handler_failed",
                        path=path,
                        error=str(exc),
                        detail=traceback.format_exc(),
                    )
                self.send_json(
                    {
                        "error": "Internal server error",
                        "detail": str(exc),
                        "traceback": traceback.format_exc(),
                    },
                    status=500,
                )

    return Handler
