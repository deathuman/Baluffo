import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

from scripts.app_version import APP_VERSION
from scripts.ship import runtime_launcher as rl
from tests.temp_paths import workspace_tmpdir


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
    (root / "app" / "versions" / version / "scripts").mkdir(parents=True, exist_ok=True)
    _write(root / "app" / "versions" / version / "index.html", "<html></html>\n")
    _write(root / "app" / "versions" / version / "jobs.html", "<html></html>\n")
    _write(root / "app" / "versions" / version / "saved.html", "<html></html>\n")
    _write(root / "app" / "versions" / version / "scripts" / "admin_bridge.py", "print('ok')\n")
    (root / "data").mkdir(parents=True, exist_ok=True)


class _ReadyHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format, *args):  # noqa: A003
        return


class RuntimeLauncherTests(unittest.TestCase):
    def test_resolve_runtime_layout_uses_current_pointer(self) -> None:
        with workspace_tmpdir("runtime-launcher") as tmp:
            root = Path(tmp) / "ship"
            _seed_ship_root(root, version="2.4.6")
            layout = rl.resolve_runtime_layout(root)
            self.assertEqual(layout.current_version, "2.4.6")
            self.assertEqual(layout.active_root, root / "app" / "versions" / "2.4.6")
            self.assertEqual(layout.data_dir, root / "data")

    def test_build_layout_uses_explicit_data_dir(self) -> None:
        with workspace_tmpdir("runtime-launcher") as tmp:
            root = Path(tmp) / "ship"
            _seed_ship_root(root)
            custom = Path(tmp) / "portable-data"
            layout = rl.resolve_runtime_layout(root, data_dir=custom)
            self.assertEqual(layout.data_dir, custom.resolve())

    def test_wait_for_url_returns_when_service_becomes_ready(self) -> None:
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

    def test_wait_for_url_raises_timeout(self) -> None:
        start = time.monotonic()
        with self.assertRaises(TimeoutError):
            rl.wait_for_url("http://127.0.0.1:9/nope", timeout_s=0.2, interval_s=0.05)
        self.assertLess(time.monotonic() - start, 2.0)

    def test_build_site_request_handler_traces_probe_requests(self) -> None:
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
                rl.wait_for_url(f"http://127.0.0.1:{server.server_address[1]}/jobs.html", timeout_s=2.0, interval_s=0.05)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

            metrics_path = data_dir / "desktop-startup-metrics.jsonl"
            self.assertTrue(metrics_path.exists())
            events = [json.loads(line)["event"] for line in metrics_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertIn("desktop_site_request_start", events)
            self.assertIn("desktop_site_request_complete", events)

    def test_run_site_server_reports_app_version(self) -> None:
        with workspace_tmpdir("runtime-launcher") as tmp:
            root = Path(tmp) / "ship"
            _seed_ship_root(root, version="2.4.6")

            class _StopServer(Exception):
                pass

            with mock.patch("builtins.print") as print_mock, mock.patch.object(
                rl,
                "ThreadingHTTPServer",
                side_effect=_StopServer,
            ):
                with self.assertRaises(_StopServer):
                    rl.run_site_server(root, port=8123)

            payload = json.loads(print_mock.call_args.args[0])
            self.assertEqual(payload["appVersion"], APP_VERSION)
            self.assertEqual(payload["currentVersion"], "2.4.6")


if __name__ == "__main__":
    unittest.main()
