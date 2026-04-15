import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src.app_version import APP_VERSION
from src.ship import runtime_launcher as rl
from tests.helpers.temp_paths import workspace_tmpdir


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_ship_root(root: Path, version: str = "1.2.3") -> None:
    _write(root / "app" / "current.txt", f"{version}\n")
    _write(
        root / "app" / "update-state.json",
        json.dumps(
            {
                "current_version": version,
                "previous_version": "",
                "last_update_status": "ready",
                "last_error_code": "",
                "updated_at": "2026-03-09T00:00:00+00:00",
            }
        ),
    )
    (root / "app" / "versions" / version / "src").mkdir(parents=True, exist_ok=True)
    _write(root / "app" / "versions" / version / "index.html", "<html></html>\n")
    _write(root / "app" / "versions" / version / "jobs.html", "<html></html>\n")
    _write(root / "app" / "versions" / version / "saved.html", "<html></html>\n")
    _write(root / "app" / "versions" / version / "src" / "admin_bridge.py", "print('ok')\n")
    (root / "data").mkdir(parents=True, exist_ok=True)


class _ReadyHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format, *args):  # noqa: A003
        return


class _DisconnectingWriter:
    def write(self, _body: bytes) -> int:
        raise ConnectionResetError(
            10054,
            "An existing connection was forcibly closed by the remote host",
        )


def test_resolve_runtime_layout_uses_current_pointer() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "ship"
        _seed_ship_root(root, version="2.4.6")
        layout = rl.resolve_runtime_layout(root)
        assert layout.current_version == "2.4.6"
        assert layout.active_root == root / "app" / "versions" / "2.4.6"
        assert layout.data_dir == root / "data"


def test_build_layout_uses_explicit_data_dir() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "ship"
        _seed_ship_root(root)
        custom = Path(tmp) / "portable-data"
        layout = rl.resolve_runtime_layout(root, data_dir=custom)
        assert layout.data_dir == custom.resolve()


def test_wait_for_url_returns_when_service_becomes_ready() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ReadyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/ready"
        rl.wait_for_url(url, timeout_s=2.0, interval_s=0.05)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.slow
def test_wait_for_url_raises_timeout() -> None:
    start = time.monotonic()
    with pytest.raises(TimeoutError):
        rl.wait_for_url("http://127.0.0.1:9/nope", timeout_s=0.2, interval_s=0.05)
    assert time.monotonic() - start < 2.0


def test_build_site_request_handler_traces_probe_requests() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "site"
        root.mkdir(parents=True, exist_ok=True)
        _write(root / "jobs.html", "<html>jobs</html>\n")
        data_dir = Path(tmp) / "data"
        handler = rl.build_site_request_handler(root, data_dir=data_dir, startup_probe=True)
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            rl.wait_for_url(
                f"http://127.0.0.1:{server.server_address[1]}/jobs.html",
                timeout_s=2.0,
                interval_s=0.05,
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        metrics_path = data_dir / "desktop-startup-metrics.jsonl"
        assert metrics_path.exists()
        events = [
            json.loads(line)["event"]
            for line in metrics_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert "desktop_site_request_start" in events
        assert "desktop_site_request_complete" in events


def test_quiet_site_handler_swallows_expected_client_disconnects() -> None:
    handler_cls = rl.build_site_request_handler(Path.cwd())
    handler = handler_cls.__new__(handler_cls)
    handler.close_connection = False
    handler.requestline = "GET /jobs.html HTTP/1.1"
    handler.request_version = "HTTP/1.1"
    handler.command = "GET"
    handler.path = "/jobs.html"
    handler.request = SimpleNamespace(makefile=lambda *args, **kwargs: None)
    handler.client_address = ("127.0.0.1", 4173)
    handler.server = SimpleNamespace()
    handler.wfile = _DisconnectingWriter()
    handler.rfile = None

    with mock.patch.object(
        rl.SimpleHTTPRequestHandler,
        "handle_one_request",
        side_effect=ConnectionResetError(
            10054,
            "An existing connection was forcibly closed by the remote host",
        ),
    ):
        handler.handle_one_request()

    assert handler.close_connection is True


def test_run_site_server_reports_app_version() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "ship"
        _seed_ship_root(root, version="2.4.6")

        class _StopServer(Exception):
            pass

        with (
            mock.patch("builtins.print") as print_mock,
            mock.patch.object(
                rl,
                "ThreadingHTTPServer",
                side_effect=_StopServer,
            ),
        ):
            with pytest.raises(_StopServer):
                rl.run_site_server(root, port=8123)

        payload = json.loads(print_mock.call_args.args[0])
        assert payload["appVersion"] == APP_VERSION
        assert payload["currentVersion"] == "2.4.6"


def test_heal_active_ship_version_restores_missing_admin_bridge_from_repo() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "ship"
        _seed_ship_root(root, version="9.9.9")
        bridge = root / "app" / "versions" / "9.9.9" / "src" / "admin_bridge.py"
        bridge.unlink()
        assert not bridge.exists()
        layout = rl.resolve_runtime_layout(root)
        with mock.patch.object(sys, "frozen", False, create=True):
            rl.heal_active_ship_version(layout)
        assert bridge.is_file()


def test_heal_active_ship_version_restores_from_meipass_when_frozen() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "ship"
        v = "9.8.7"
        _write(root / "app" / "current.txt", f"{v}\n")
        _write(
            root / "app" / "update-state.json",
            json.dumps(
                {
                    "current_version": v,
                    "previous_version": "",
                    "last_update_status": "ready",
                    "last_error_code": "",
                    "updated_at": "2026-03-09T00:00:00+00:00",
                }
            ),
        )
        vdir = root / "app" / "versions" / v
        (vdir / "src").mkdir(parents=True, exist_ok=True)
        _write(vdir / "index.html", "<html></html>\n")
        _write(vdir / "jobs.html", "<html></html>\n")
        _write(vdir / "saved.html", "<html></html>\n")
        meipass_parent = Path(tmp) / "internal"
        embed = meipass_parent / "baluffo_embed"
        (embed / "src").mkdir(parents=True, exist_ok=True)
        _write(embed / "src" / "admin_bridge.py", "print('embedded')\n")
        _write(embed / "index.html", "<html></html>\n")
        _write(embed / "jobs.html", "<html></html>\n")
        _write(embed / "saved.html", "<html></html>\n")
        (root / "data").mkdir(parents=True, exist_ok=True)
        layout = rl.resolve_runtime_layout(root)
        with (
            mock.patch.object(sys, "frozen", True, create=True),
            mock.patch.object(sys, "_MEIPASS", str(meipass_parent), create=True),
        ):
            rl.heal_active_ship_version(layout)
        assert (vdir / "src" / "admin_bridge.py").is_file()


def test_run_bridge_server_forwards_desktop_owner_arguments() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "ship"
        _seed_ship_root(root, version="3.1.4")

        captured_argv = []

        def _capture_run_path(_script: str, run_name: str) -> None:
            captured_argv.extend(sys.argv)
            raise RuntimeError("stop-after-argv")

        with pytest.raises(RuntimeError, match="stop-after-argv"):
            with mock.patch.object(rl.runpy, "run_path", side_effect=_capture_run_path):
                rl.run_bridge_server(
                    root,
                    bind_host="127.0.0.1",
                    port=8877,
                    data_dir=root / "data",
                    desktop_mode=True,
                    owner_mode="desktop-window",
                    owner_token="owner-token-1",
                    desktop_session_id="desktop-session-1",
                    started_by="launcher-1",
                    owner_idle_timeout_s=15.0,
                )

        assert "--owner-mode" in captured_argv
        assert "desktop-window" in captured_argv
        assert "--owner-token" in captured_argv
        assert "owner-token-1" in captured_argv
        assert "--desktop-session-id" in captured_argv
        assert "desktop-session-1" in captured_argv
        assert "--started-by" in captured_argv
        assert "launcher-1" in captured_argv
