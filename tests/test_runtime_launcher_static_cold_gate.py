import json
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.request import urlopen

import pytest

from src.ship import jobs_first_run_state as first_run_state
from src.ship import runtime_launcher as rl
from tests.helpers.temp_paths import workspace_tmpdir


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_successful_jobs_report_and_feeds(data_dir: Path) -> None:
    _write(
        data_dir / "jobs-fetch-report.json",
        json.dumps(
            {
                "finishedAt": "2026-05-18T00:00:00Z",
                "status": "ok",
                "summary": {"status": "ok", "outputCount": 1},
            }
        ),
    )
    _write(data_dir / "jobs-unified-light.json", '{"jobs":[{"id":"job-1"}]}\n')
    _write(data_dir / "jobs-unified.json", '{"jobs":[{"id":"job-1"}]}\n')
    _write(data_dir / "jobs-unified.csv", "id,title\njob-1,Role\n")


def test_build_site_request_handler_skips_jobs_cold_start_gate_for_non_row_assets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("runtime-launcher-cold-row-hot-path") as tmp:
        root = Path(tmp) / "site"
        _write(root / "admin.html", "<html>admin</html>\n")
        _write(root / "jobs.html", "<html>jobs</html>\n")
        data_dir = Path(tmp) / "data"
        _write(data_dir / "jobs-unified.csv", "id,title\njob-1,Role\n")
        calls: list[Path] = []

        def fail_if_called(data_dir: str | Path) -> bool:
            calls.append(Path(data_dir))
            raise AssertionError("non-row requests must not evaluate the jobs cold-start gate")

        monkeypatch.setattr(rl, "jobs_cold_start_required_for_static_serving", fail_if_called)

        handler = rl.build_site_request_handler(
            root,
            runtime_data_dir=data_dir,
            static_data_dir=data_dir,
            startup_probe=False,
            jobs_cold_start=True,
        )
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_address[1]}"
            for path in ("/admin.html", "/jobs.html", "/admin.html?t=2", "/jobs.html?t=2"):
                with urlopen(f"{base}{path}", timeout=2.0) as response:
                    payload = response.read().decode("utf-8")
                    assert "<html>" in payload
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        assert calls == []


def test_build_site_request_handler_serves_row_artifacts_after_dynamic_cold_start_recovers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("runtime-launcher-cold-row-recovered") as tmp:
        root = Path(tmp) / "site"
        root.mkdir(parents=True, exist_ok=True)
        data_dir = Path(tmp) / "data"
        _write_successful_jobs_report_and_feeds(data_dir)

        def fail_if_full_validator_called(data_dir: str | Path) -> bool:
            raise AssertionError("request-time static serving must not use full feed validation")

        monkeypatch.setattr(rl, "jobs_cold_start_required", fail_if_full_validator_called)

        handler = rl.build_site_request_handler(
            root,
            runtime_data_dir=data_dir,
            static_data_dir=data_dir,
            startup_probe=False,
            jobs_cold_start=True,
        )
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urlopen(
                f"http://127.0.0.1:{server.server_address[1]}/data/jobs-unified-light.json?t=1",
                timeout=2.0,
            ) as response:
                payload = response.read().decode("utf-8")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        assert '"job-1"' in payload


def test_static_serving_cold_start_helper_does_not_parse_feed_artifacts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("runtime-launcher-static-serving-helper") as tmp:
        data_dir = Path(tmp) / "data"
        _write_successful_jobs_report_and_feeds(data_dir)
        read_json = first_run_state.read_json
        read_paths: list[str] = []

        def read_json_spy(path: str | Path, default: object = None) -> object:
            read_paths.append(Path(path).name)
            if Path(path).name != "jobs-fetch-report.json":
                raise AssertionError("static-serving helper must not parse feed artifacts")
            return read_json(Path(path), default)

        monkeypatch.setattr(first_run_state, "read_json", read_json_spy)

        assert (
            first_run_state.has_successful_runtime_jobs_report_for_static_serving(data_dir) is True
        )
        assert read_paths == ["jobs-fetch-report.json"]


def test_build_site_request_handler_flips_runtime_config_after_recovery() -> None:
    with workspace_tmpdir("runtime-launcher-runtime-config-cold-flip") as tmp:
        root = Path(tmp) / "site"
        root.mkdir(parents=True, exist_ok=True)
        data_dir = Path(tmp) / "data"

        handler = rl.build_site_request_handler(
            root,
            runtime_data_dir=data_dir,
            static_data_dir=data_dir,
            startup_probe=False,
            desktop_bridge_host="127.0.0.1",
            desktop_bridge_port=61234,
            jobs_cold_start=True,
        )
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_address[1]}"
            with urlopen(f"{base}/frontend-runtime-config.js?v=1", timeout=2.0) as response:
                cold_payload = response.read().decode("utf-8")
            _write_successful_jobs_report_and_feeds(data_dir)
            with urlopen(f"{base}/frontend-runtime-config.js?v=2", timeout=2.0) as response:
                recovered_payload = response.read().decode("utf-8")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        assert '"jobsColdStart": true' in cold_payload
        assert '"jobsColdStart": false' in recovered_payload
