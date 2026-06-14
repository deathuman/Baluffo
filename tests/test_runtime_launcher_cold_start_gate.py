import json
import threading
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.request import urlopen

from src.ship import runtime_launcher as rl
from tests.helpers.temp_paths import workspace_tmpdir


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_successful_jobs_runtime(data_dir: Path) -> None:
    _write(
        data_dir / "jobs-fetch-report.json",
        json.dumps(
            {
                "finishedAt": "2026-05-17T10:00:00+00:00",
                "summary": {"status": "ok", "outputCount": 1},
            }
        ),
    )
    job = '[{"id":"job-1","title":"Tools Programmer"}]\n'
    _write(data_dir / "jobs-unified.json", job)
    _write(data_dir / "jobs-unified-light.json", job)
    _write(data_dir / "jobs-unified.csv", "id,title\njob-1,Tools Programmer\n")


@contextmanager
def _site_server(handler):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_row_artifacts_are_served_after_cold_start_recovery() -> None:
    with workspace_tmpdir("runtime-launcher-cold-row-recovery") as tmp:
        root = Path(tmp) / "site"
        root.mkdir(parents=True, exist_ok=True)
        data_dir = Path(tmp) / "data"
        _write_successful_jobs_runtime(data_dir)

        handler = rl.build_site_request_handler(
            root,
            runtime_data_dir=data_dir,
            static_data_dir=data_dir,
            startup_probe=False,
            jobs_cold_start=True,
        )
        with _site_server(handler) as base_url:
            with urlopen(f"{base_url}/data/jobs-unified-light.json?t=1", timeout=2.0) as response:
                payload = response.read().decode("utf-8")

        assert '"job-1"' in payload


def test_frontend_runtime_config_flips_jobs_cold_start_after_feed_recovery() -> None:
    with workspace_tmpdir("runtime-launcher-cold-config-flip") as tmp:
        root = Path(tmp) / "site"
        root.mkdir(parents=True, exist_ok=True)
        data_dir = Path(tmp) / "data"

        handler = rl.build_site_request_handler(
            root,
            runtime_data_dir=data_dir,
            static_data_dir=data_dir,
            startup_probe=False,
            desktop_bridge_host="127.0.0.1",
            desktop_bridge_port=8877,
            jobs_cold_start=rl.jobs_cold_start_required(data_dir),
        )
        with _site_server(handler) as base_url:
            with urlopen(f"{base_url}/frontend-runtime-config.js?t=1", timeout=2.0) as response:
                before_payload = response.read().decode("utf-8")
            _write_successful_jobs_runtime(data_dir)
            with urlopen(f"{base_url}/frontend-runtime-config.js?t=2", timeout=2.0) as response:
                after_payload = response.read().decode("utf-8")

        assert '"jobsColdStart": true' in before_payload
        assert '"jobsColdStart": false' in after_payload


def test_frontend_runtime_config_skips_cold_start_when_feed_survives_failed_report() -> None:
    with workspace_tmpdir("runtime-launcher-cold-config-failed-report-feed") as tmp:
        root = Path(tmp) / "site"
        root.mkdir(parents=True, exist_ok=True)
        data_dir = Path(tmp) / "data"
        _write(
            data_dir / "jobs-fetch-report.json",
            json.dumps(
                {
                    "status": "error",
                    "finishedAt": "2026-06-14T19:39:35+00:00",
                    "summary": {"outputCount": 49528},
                }
            ),
        )
        job = '[{"id":"job-1","title":"Tools Programmer"}]\n'
        _write(data_dir / "jobs-unified.json", job)
        _write(data_dir / "jobs-unified-light.json", job)
        _write(data_dir / "jobs-unified.csv", "id,title\njob-1,Tools Programmer\n")

        handler = rl.build_site_request_handler(
            root,
            runtime_data_dir=data_dir,
            static_data_dir=data_dir,
            startup_probe=False,
            desktop_bridge_host="127.0.0.1",
            desktop_bridge_port=8877,
            jobs_cold_start=rl.jobs_cold_start_required(data_dir),
        )
        with _site_server(handler) as base_url:
            with urlopen(f"{base_url}/frontend-runtime-config.js?t=1", timeout=2.0) as response:
                payload = response.read().decode("utf-8")

        assert '"jobsColdStart": false' in payload
