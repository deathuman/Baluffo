import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock
from urllib.request import urlopen

import pytest

from src.app_version import APP_VERSION
from src.ship import runtime_launcher as rl
from src.ship import startup_telemetry as telemetry
from tests.helpers.temp_paths import workspace_tmpdir


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_quarantine_stale_jobs_row_artifacts_without_successful_report() -> None:
    with workspace_tmpdir("runtime-launcher-quarantine") as tmp:
        data_dir = Path(tmp) / "data"
        _write(data_dir / "jobs-fetch-report.json", json.dumps({"summary": {"outputCount": 0}}))
        _write(data_dir / "jobs-unified.json", "[]")
        _write(data_dir / "jobs-unified-light.json", "[]")
        _write(data_dir / "jobs-unified.csv", "id,title\n")
        _write(data_dir / "jobs-unified-startup.json", "[]")

        result = rl.quarantine_stale_jobs_row_artifacts(data_dir)

        assert not (data_dir / "jobs-unified.json").exists()
        assert not (data_dir / "jobs-unified-light.json").exists()
        assert not (data_dir / "jobs-unified.csv").exists()
        assert not (data_dir / "jobs-unified-startup.json").exists()
        assert len(result["quarantined"]) == 4
        assert list((data_dir / "backups").glob("stripped-packaged-jobs-*"))
        assert list((data_dir / "migration-reports").glob("stripped-packaged-jobs-cleanup-*.json"))


def test_quarantine_stale_jobs_row_artifacts_without_artifacts_returns_consistent_result() -> None:
    with workspace_tmpdir("runtime-launcher-quarantine-empty") as tmp:
        data_dir = Path(tmp) / "data"

        result = rl.quarantine_stale_jobs_row_artifacts(data_dir)

        assert result == {"quarantined": [], "failed": [], "skipped": "no_artifacts"}


def test_quarantine_preserves_jobs_rows_with_successful_report() -> None:
    with workspace_tmpdir("runtime-launcher-preserve-jobs") as tmp:
        data_dir = Path(tmp) / "data"
        _write(
            data_dir / "jobs-fetch-report.json",
            json.dumps(
                {
                    "finishedAt": "2026-05-17T10:00:00+00:00",
                    "summary": {"status": "ok", "outputCount": 1},
                }
            ),
        )
        _write(data_dir / "jobs-unified.json", "[{}]")
        _write(data_dir / "jobs-unified-light.json", "[{}]")
        _write(data_dir / "jobs-unified.csv", "id,title\n1,Role\n")

        result = rl.quarantine_stale_jobs_row_artifacts(data_dir)

        assert result["skipped"] == "successful_runtime_report"
        assert result["failed"] == []
        assert (data_dir / "jobs-unified.json").exists()
        assert (data_dir / "jobs-unified-light.json").exists()
        assert (data_dir / "jobs-unified.csv").exists()


@pytest.mark.parametrize(
    ("move_error", "error_message"),
    [
        (OSError("file locked"), "file locked"),
        (rl.shutil.Error("destination conflict"), "destination conflict"),
    ],
)
def test_quarantine_stale_jobs_row_artifacts_keeps_running_when_one_move_fails(
    move_error: Exception,
    error_message: str,
) -> None:
    with workspace_tmpdir("runtime-launcher-quarantine-partial") as tmp:
        data_dir = Path(tmp) / "data"
        _write(data_dir / "jobs-fetch-report.json", json.dumps({"summary": {"outputCount": 0}}))
        _write(data_dir / "jobs-unified.json", "[]")
        _write(data_dir / "jobs-unified.csv", "id,title\n")
        original_move = rl.shutil.move

        def move_with_locked_json(source: str, target: str) -> str:
            if str(source).endswith("jobs-unified.json"):
                raise move_error
            return str(original_move(source, target))

        with mock.patch.object(rl.shutil, "move", side_effect=move_with_locked_json):
            result = rl.quarantine_stale_jobs_row_artifacts(data_dir)

        assert (data_dir / "jobs-unified.json").exists()
        assert not (data_dir / "jobs-unified.csv").exists()
        assert len(result["quarantined"]) == 1
        assert result["failed"][0]["path"].endswith("jobs-unified.json")
        assert error_message in result["failed"][0]["error"]
        reports = list(
            (data_dir / "migration-reports").glob("stripped-packaged-jobs-cleanup-*.json")
        )
        report_payload = json.loads(reports[0].read_text(encoding="utf-8"))
        assert report_payload["failed"][0]["path"].endswith("jobs-unified.json")


def test_quarantine_stale_jobs_row_artifacts_returns_report_write_error() -> None:
    with workspace_tmpdir("runtime-launcher-quarantine-report-error") as tmp:
        data_dir = Path(tmp) / "data"
        _write(data_dir / "jobs-fetch-report.json", json.dumps({"summary": {"outputCount": 0}}))
        _write(data_dir / "jobs-unified.csv", "id,title\n")
        original_write_text = Path.write_text

        def write_text_with_report_error(path: Path, text: str, *args: Any, **kwargs: Any) -> int:
            if path.name.startswith("stripped-packaged-jobs-cleanup-"):
                raise OSError("report locked")
            return int(original_write_text(path, text, *args, **kwargs))

        with mock.patch.object(Path, "write_text", write_text_with_report_error):
            result = rl.quarantine_stale_jobs_row_artifacts(data_dir)

        assert not (data_dir / "jobs-unified.csv").exists()
        assert result["quarantined"][0].endswith("jobs-unified.csv")
        assert "report locked" in result["reportError"]


def test_append_startup_trace_writes_versioned_fields_row() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        data_dir = Path(tmp) / "data"
        telemetry.append_startup_trace(
            data_dir,
            "desktop_browser_watchdog_handoff_confirmed",
            evidence="startup_metric",
            path=data_dir,
        )

        rows = [
            json.loads(line)
            for line in (data_dir / "desktop-startup-metrics.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
            if line.strip()
        ]

    assert rows == [
        {
            "schemaVersion": 1,
            "ts": rows[0]["ts"],
            "event": "desktop_browser_watchdog_handoff_confirmed",
            "category": "handoff",
            "fields": {"evidence": "startup_metric", "path": str(data_dir)},
        }
    ]


def test_startup_metric_category_classifies_support_events() -> None:
    assert telemetry.startup_metric_category("desktop_browser_watchdog_handoff_confirmed") == (
        "handoff"
    )
    assert telemetry.startup_metric_category("desktop_stale_runtime_reclaim_result") == "recovery"
    assert telemetry.startup_metric_category("desktop_runtime_port_retry") == "port_retry"
    assert telemetry.startup_metric_category("jobs_first_interactive") == "page"


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


class _NotFoundHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        self.send_response(404)
        self.end_headers()
        self.wfile.write(b"missing")

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


def test_resolve_runtime_layout_repairs_missing_current_pointer() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "ship"
        _seed_ship_root(root, version="2.4.6")
        (root / "app" / "current.txt").unlink()

        layout = rl.resolve_runtime_layout(root)

        assert layout.current_version == "2.4.6"
        assert layout.active_root == root / "app" / "versions" / "2.4.6"
        assert (root / "app" / "current.txt").read_text(encoding="utf-8").strip() == "2.4.6"


def test_resolve_runtime_layout_repairs_missing_active_version_dir_via_startup_check() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "ship"
        _seed_ship_root(root, version="0.9.0")
        _write(root / "app" / "current.txt", "9.9.9\n")
        _write(
            root / "app" / "update-state.json",
            json.dumps(
                {
                    "current_version": "9.9.9",
                    "previous_version": "",
                    "last_update_status": "ready",
                    "last_error_code": "",
                    "updated_at": "2026-03-09T00:00:00+00:00",
                }
            ),
        )

        layout = rl.resolve_runtime_layout(root)

        assert layout.current_version == "0.9.0"
        assert layout.active_root == root / "app" / "versions" / "0.9.0"
        assert (root / "app" / "current.txt").read_text(encoding="utf-8").strip() == "0.9.0"


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


def test_wait_for_url_treats_loopback_404_as_ready() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _NotFoundHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/missing"
        rl.wait_for_url(url, timeout_s=2.0, interval_s=0.05)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_wait_for_url_uses_direct_http_connection_for_loopback() -> None:
    calls: list[tuple[str, int, float, str, str]] = []

    class _FakeResponse:
        status = 204

        def read(self) -> bytes:
            return b""

    class _FakeConnection:
        def __init__(self, host: str, port: int, timeout: float):
            calls.append((host, port, timeout, "", ""))
            self._index = len(calls) - 1

        def request(self, method: str, path: str) -> None:
            host, port, timeout, _, _ = calls[self._index]
            calls[self._index] = (host, port, timeout, method, path)

        def getresponse(self) -> _FakeResponse:
            return _FakeResponse()

        def close(self) -> None:
            return

    with mock.patch.object(telemetry.http.client, "HTTPConnection", _FakeConnection):
        rl.wait_for_url("http://127.0.0.1:8123/jobs.html?desktop=1", timeout_s=1.0, interval_s=0.05)

    assert calls == [("127.0.0.1", 8123, 1.0, "GET", "/jobs.html?desktop=1")]


def test_wait_for_url_uses_opener_for_non_loopback() -> None:
    opened: list[tuple[str, float]] = []

    class _FakeResponse:
        status = 204

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    class _FakeOpener:
        def open(self, request, timeout: float):
            opened.append((request.full_url, timeout))
            return _FakeResponse()

    with mock.patch.object(telemetry, "build_opener", return_value=_FakeOpener()):
        rl.wait_for_url("http://example.com/health", timeout_s=1.0, interval_s=0.05)

    assert opened == [("http://example.com/health", 1.0)]


@pytest.mark.slow
def test_wait_for_url_raises_timeout() -> None:
    start = time.monotonic()
    with pytest.raises(TimeoutError):
        rl.wait_for_url("http://127.0.0.1:9/nope", timeout_s=0.2, interval_s=0.05)
    assert time.monotonic() - start < 2.0


def test_build_site_request_handler_traces_probe_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "site"
        root.mkdir(parents=True, exist_ok=True)
        _write(root / "jobs.html", "<html>jobs</html>\n")
        data_dir = Path(tmp) / "data"
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))
        monkeypatch.setenv("BALUFFO_STARTUP_PROBE", "1")
        handler = rl.build_site_request_handler(
            root,
            runtime_data_dir=data_dir,
            static_data_dir=data_dir,
            startup_probe=True,
        )
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
        deadline = time.monotonic() + 2.0
        events: list[str] = []
        while time.monotonic() < deadline:
            if metrics_path.exists():
                events = [
                    json.loads(line)["event"]
                    for line in metrics_path.read_text(encoding="utf-8").splitlines()
                    if line.strip()
                ]
                if "desktop_site_request_complete" in events:
                    break
            time.sleep(0.05)
        assert metrics_path.exists()
        assert "desktop_url_probe_started" in events
        assert "desktop_url_probe_succeeded" in events
        assert "desktop_site_request_start" in events
        assert "desktop_site_request_complete" in events


def test_build_site_request_handler_serves_data_requests_from_runtime_data_dir() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "site"
        root.mkdir(parents=True, exist_ok=True)
        _write(root / "jobs.html", "<html>jobs</html>\n")
        data_dir = Path(tmp) / "data"
        _write(data_dir / "jobs-unified-startup.json", '{"ok":true}\n')
        static_dir = Path(tmp) / "static-data"
        _write(static_dir / "jobs-unified-startup.json", '{"ok":false}\n')

        handler = rl.build_site_request_handler(
            root,
            runtime_data_dir=data_dir,
            static_data_dir=static_dir,
            startup_probe=False,
        )
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urlopen(
                f"http://127.0.0.1:{server.server_address[1]}/data/jobs-unified-startup.json",
                timeout=2.0,
            ) as response:
                payload = response.read().decode("utf-8")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        assert '"ok":true' in payload


def test_build_site_request_handler_serves_legacy_root_data_aliases() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "site"
        root.mkdir(parents=True, exist_ok=True)
        data_dir = Path(tmp) / "data"
        _write(data_dir / "jobs-fetch-report.json", '{"ok":true,"report":1}\n')

        handler = rl.build_site_request_handler(
            root,
            runtime_data_dir=data_dir,
            static_data_dir=data_dir,
            startup_probe=False,
        )
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_address[1]}"
            with urlopen(f"{base}/jobs-fetch-report.json?t=1", timeout=2.0) as response:
                report_payload = response.read().decode("utf-8")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        assert '"report":1' in report_payload


def test_build_site_request_handler_serves_desktop_runtime_bridge_config() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "site"
        root.mkdir(parents=True, exist_ok=True)
        _write(
            root / "frontend-runtime-config.js",
            "globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = { bridge: { port: 8877 } };\n",
        )

        handler = rl.build_site_request_handler(
            root,
            desktop_bridge_host="127.0.0.1",
            desktop_bridge_port=61234,
        )
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urlopen(
                f"http://127.0.0.1:{server.server_address[1]}/frontend-runtime-config.js?v=2",
                timeout=2.0,
            ) as response:
                payload = response.read().decode("utf-8")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        assert "BALUFFO_FRONTEND_RUNTIME_CONFIG" in payload
        assert '"host": "127.0.0.1"' in payload
        assert '"port": 61234' in payload
        assert '"runtime": {' in payload
        assert '"desktop": true' in payload
        assert '"port": 8877' not in payload


def test_wait_for_url_emits_timeout_diagnostics_for_startup_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        data_dir = Path(tmp) / "data"
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))
        monkeypatch.setenv("BALUFFO_STARTUP_PROBE", "1")

        with pytest.raises(TimeoutError):
            rl.wait_for_url("http://127.0.0.1:9/nope", timeout_s=0.2, interval_s=0.05)

        rows = [
            json.loads(line)
            for line in (data_dir / "desktop-startup-metrics.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
            if line.strip()
        ]
        events = [row["event"] for row in rows]
        assert "desktop_url_probe_started" in events
        assert "desktop_url_probe_attempt_failed" in events
        assert "desktop_url_probe_timeout" in events


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


def test_run_site_server_continues_when_jobs_quarantine_raises() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "ship"
        _seed_ship_root(root, version="2.4.6")
        events: list[tuple[str, dict[str, object]]] = []

        class _StopServer(Exception):
            pass

        with (
            mock.patch.object(
                rl,
                "quarantine_stale_jobs_row_artifacts",
                side_effect=RuntimeError("quarantine crashed"),
            ),
            mock.patch.object(
                rl,
                "_append_runtime_startup_trace",
                side_effect=lambda event, **fields: events.append((event, dict(fields))),
            ),
            mock.patch.object(rl, "ThreadingHTTPServer", side_effect=_StopServer),
        ):
            with pytest.raises(_StopServer):
                rl.run_site_server(root, port=8123)

        assert any(event == "jobs_row_artifact_quarantine_failed" for event, _fields in events)


def test_run_site_server_skips_heal_when_active_version_is_healthy() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "ship"
        _seed_ship_root(root, version="2.4.6")

        class _StopServer(Exception):
            pass

        with (
            mock.patch.object(
                rl.update_manager, "health_check_version", return_value=(True, "")
            ) as health_mock,
            mock.patch.object(rl, "heal_active_ship_version") as heal_mock,
            mock.patch.object(rl, "ThreadingHTTPServer", side_effect=_StopServer),
        ):
            with pytest.raises(_StopServer):
                rl.run_site_server(root, port=8123)

        health_mock.assert_called_once()
        heal_mock.assert_not_called()


def test_run_site_server_heals_when_active_version_is_unhealthy() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "ship"
        _seed_ship_root(root, version="2.4.6")

        class _StopServer(Exception):
            pass

        with (
            mock.patch.object(
                rl.update_manager,
                "health_check_version",
                return_value=(False, "missing_required_file:index.html"),
            ) as health_mock,
            mock.patch.object(rl, "heal_active_ship_version") as heal_mock,
            mock.patch.object(rl, "ThreadingHTTPServer", side_effect=_StopServer),
        ):
            with pytest.raises(_StopServer):
                rl.run_site_server(root, port=8123)

        health_mock.assert_called_once()
        heal_mock.assert_called_once()


def test_run_site_server_emits_bootstrap_trace_events_for_startup_probe() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "ship"
        _seed_ship_root(root, version="2.4.6")
        data_dir = root / "data"

        class _StopServer(Exception):
            pass

        server = mock.MagicMock()
        server.__enter__.return_value = server
        server.__exit__.return_value = False
        server.serve_forever.side_effect = _StopServer

        with (
            mock.patch.dict(
                rl.os.environ,
                {
                    "BALUFFO_STARTUP_PROBE": "1",
                    "BALUFFO_DATA_DIR": str(data_dir),
                },
                clear=False,
            ),
            mock.patch.object(rl, "ThreadingHTTPServer", return_value=server),
        ):
            with pytest.raises(_StopServer):
                rl.run_site_server(root, port=8123)

        metrics_path = data_dir / "desktop-startup-metrics.jsonl"
        events = [
            json.loads(line)["event"]
            for line in metrics_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert "desktop_site_layout_resolved" in events
        assert "desktop_site_health_check_started" in events
        assert "desktop_site_health_check_completed" in events
        assert "desktop_site_server_listening" in events


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


def test_run_bridge_server_continues_when_jobs_quarantine_raises() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "ship"
        _seed_ship_root(root, version="3.1.4")
        events: list[tuple[str, dict[str, object]]] = []

        def _capture_run_path(_script: str, run_name: str) -> None:
            raise RuntimeError("stop-after-argv")

        with pytest.raises(RuntimeError, match="stop-after-argv"):
            with (
                mock.patch.object(
                    rl,
                    "quarantine_stale_jobs_row_artifacts",
                    side_effect=RuntimeError("quarantine crashed"),
                ),
                mock.patch.object(
                    rl,
                    "_append_runtime_startup_trace",
                    side_effect=lambda event, **fields: events.append((event, dict(fields))),
                ),
                mock.patch.object(rl.runpy, "run_path", side_effect=_capture_run_path),
            ):
                rl.run_bridge_server(
                    root,
                    bind_host="127.0.0.1",
                    port=8877,
                    data_dir=root / "data",
                )

        assert any(event == "jobs_row_artifact_quarantine_failed" for event, _fields in events)


def test_run_bridge_server_uses_current_version_repaired_by_startup_check() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "ship"
        repaired_version = "4.0.0"
        _seed_ship_root(root, version="3.1.4")
        _write(
            root / "app" / "versions" / repaired_version / "src" / "admin_bridge.py",
            "print('repaired')\n",
        )
        captured = {"script": ""}

        def _capture_run_path(script: str, run_name: str) -> None:
            captured["script"] = script
            raise RuntimeError("stop-after-script")

        with pytest.raises(RuntimeError, match="stop-after-script"):
            with (
                mock.patch.object(
                    rl.update_manager,
                    "startup_check",
                    return_value={
                        "ok": True,
                        "current_version": repaired_version,
                        "repaired_pointer": True,
                    },
                ),
                mock.patch.object(rl.runpy, "run_path", side_effect=_capture_run_path),
            ):
                rl.run_bridge_server(
                    root,
                    bind_host="127.0.0.1",
                    port=8877,
                    data_dir=root / "data",
                )

        assert (
            Path(captured["script"])
            .as_posix()
            .endswith(f"app/versions/{repaired_version}/src/admin_bridge.py")
        )


def test_run_bridge_server_emits_bootstrap_trace_events_for_startup_probe() -> None:
    with workspace_tmpdir("runtime-launcher") as tmp:
        root = Path(tmp) / "ship"
        _seed_ship_root(root, version="3.1.4")
        data_dir = root / "data"

        def _capture_run_path(_script: str, run_name: str) -> None:
            raise RuntimeError("stop-after-argv")

        with pytest.raises(RuntimeError, match="stop-after-argv"):
            with (
                mock.patch.dict(
                    rl.os.environ,
                    {
                        "BALUFFO_STARTUP_PROBE": "1",
                        "BALUFFO_DATA_DIR": str(data_dir),
                    },
                    clear=False,
                ),
                mock.patch.object(rl.runpy, "run_path", side_effect=_capture_run_path),
            ):
                rl.run_bridge_server(
                    root,
                    bind_host="127.0.0.1",
                    port=8877,
                    data_dir=data_dir,
                    desktop_mode=True,
                    owner_mode="desktop-window",
                    owner_token="owner-token-1",
                    desktop_session_id="desktop-session-1",
                    started_by="launcher-1",
                    owner_idle_timeout_s=15.0,
                )

        metrics_path = data_dir / "desktop-startup-metrics.jsonl"
        events = [
            json.loads(line)["event"]
            for line in metrics_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert "desktop_bridge_layout_resolved" in events
        assert "desktop_bridge_repair_started" in events
        assert "desktop_bridge_repair_completed" in events
        assert "desktop_bridge_startup_check_started" in events
        assert "desktop_bridge_startup_check_completed" in events


def test_run_site_wrapper_defers_pointer_resolution_to_runtime_launcher() -> None:
    script_path = Path(__file__).resolve().parents[1] / "src" / "ship" / "run-site.ps1"
    script = script_path.read_text(encoding="utf-8")

    assert "Get-Content $CurrentPointer" not in script
    assert "Push-Location $ActiveRoot" not in script
    assert "-m src.ship.runtime_launcher site --root $Root --port $Port" in script


def test_run_bridge_wrapper_defers_pointer_resolution_to_runtime_launcher() -> None:
    script_path = Path(__file__).resolve().parents[1] / "src" / "ship" / "run-bridge.ps1"
    script = script_path.read_text(encoding="utf-8")

    assert "Get-Content $CurrentPointer" not in script
    assert "Push-Location $ActiveRoot" not in script
    assert "-m src.ship.runtime_launcher bridge --root $Root" in script
