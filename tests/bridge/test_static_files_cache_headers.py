from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from src.bridge.server import handler as server_handler
from src.bridge.server.static_files import StaticFileService


class _ResponseCapture:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.response_status: int | None = None
        self.ended = False
        self.wfile = self
        self.body = b""

    def send_response(self, status: int) -> None:
        self.response_status = status

    def send_header(self, key: str, value: str) -> None:
        self.headers[key] = value

    def end_headers(self) -> None:
        self.ended = True

    def write(self, data: bytes) -> None:
        self.body += data


class _FakeStaticHandler:
    def __init__(self, path: str) -> None:
        self.command = "GET"
        self.path = path
        self.headers: dict[str, str] = {}
        self.sent: list[dict] = []

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
        etag: str | None = None,
    ) -> None:
        self.sent.append(
            {
                "body": body,
                "status": status,
                "cache_control": cache_control,
                "etag": etag,
                "content_type": content_type,
            }
        )


def test_send_bytes_emits_etag_and_304_on_match() -> None:
    api = SimpleNamespace()
    body = b"export const a = 1;"
    etag = '"sha1:abc"'

    ok = _ResponseCapture()
    server_handler._send_bytes_response(
        ok,
        api,
        body,
        content_type="application/javascript",
        status=200,
        cache_control="public, no-cache",
        etag=etag,
    )
    assert ok.response_status == 200
    assert ok.headers["ETag"] == etag
    assert ok.headers["Cache-Control"] == "public, no-cache"
    assert ok.body == body

    not_modified = _ResponseCapture()
    not_modified.headers["If-None-Match"] = etag
    server_handler._send_bytes_response(
        not_modified,
        api,
        body,
        content_type="application/javascript",
        status=200,
        cache_control="public, no-cache",
        etag=etag,
    )
    assert not_modified.response_status == 304
    assert not_modified.body == b""
    assert not_modified.headers["ETag"] == etag


def test_static_js_gets_revalidate_cache_and_etag(tmp_path: Path) -> None:
    static_root = tmp_path / "static"
    js_dir = static_root / "frontend" / "app"
    js_dir.mkdir(parents=True)
    (js_dir / "x.js").write_text("export const a = 1;")
    svc = StaticFileService(static_root=static_root, data_dir=tmp_path / "data")

    handler = _FakeStaticHandler("/frontend/app/x.js")
    assert svc.handle_get(handler, path="/frontend/app/x.js") is True
    resp = handler.sent[-1]
    assert resp["status"] == 200
    assert resp["cache_control"] == "public, no-cache"
    assert resp["etag"] is not None
