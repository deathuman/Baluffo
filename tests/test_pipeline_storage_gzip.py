from __future__ import annotations

import gzip
import json
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.request import urlopen

from src.jobs.state_lifecycle import (
    lifecycle_archive_state_path,
    read_job_lifecycle_archive_state,
    read_job_lifecycle_state,
    write_job_lifecycle_archive_state,
    write_job_lifecycle_state,
)
from src.jobs.state_source_records import read_source_state, write_source_state
from src.pipeline_io import write_atomic_if_changed
from src.shared.json_io import read_json
from src.ship import runtime_launcher as rl
from tests.helpers.temp_paths import workspace_tmpdir


def _gzip_path(path: Path) -> Path:
    return path.with_name(path.name + ".gz")


def test_pipeline_json_reader_prefers_gzip_over_legacy_plain_file() -> None:
    with workspace_tmpdir("pipeline-gzip-reader") as tmp:
        path = Path(tmp) / "jobs-unified.json"
        path.write_text(json.dumps([{"title": "legacy"}]), encoding="utf-8")

        assert read_json(path, []) == [{"title": "legacy"}]

        with gzip.open(_gzip_path(path), mode="wt", encoding="utf-8") as handle:
            json.dump([{"title": "compressed"}], handle)

        assert read_json(path, []) == [{"title": "compressed"}]


def test_pipeline_json_writer_round_trips_gzip_backed_output() -> None:
    with workspace_tmpdir("pipeline-gzip-writer") as tmp:
        path = Path(tmp) / "jobs-unified.json"
        payload = '[{"title":"Pipeline Engineer"}]\n'

        assert write_atomic_if_changed(path, payload) is True
        assert _gzip_path(path).exists()
        assert path.exists() is False

        with gzip.open(_gzip_path(path), mode="rt", encoding="utf-8") as handle:
            assert handle.read() == payload

        assert write_atomic_if_changed(path, payload) is False


def test_source_state_helpers_round_trip_gzip_storage() -> None:
    with workspace_tmpdir("pipeline-source-state-gzip") as tmp:
        path = Path(tmp) / "jobs-source-state.json"
        rows = {
            "source-a": {
                "status": "active",
                "lastSuccessfulFetchAt": "2026-05-04T10:00:00Z",
            }
        }

        write_source_state(path, rows)

        assert _gzip_path(path).exists()
        assert path.exists() is False
        with gzip.open(_gzip_path(path), mode="rt", encoding="utf-8") as handle:
            payload = handle.read()
        assert '"source-a"' in payload
        source_state = read_source_state(path)
        assert "source-a" in source_state
        assert source_state["source-a"]["healthScore"] == 100


def test_lifecycle_state_helpers_round_trip_gzip_storage() -> None:
    with workspace_tmpdir("pipeline-lifecycle-state-gzip") as tmp:
        path = Path(tmp) / "jobs-lifecycle-state.json"
        rows = {
            "job-1": {
                "status": "active",
                "title": "Game Designer",
            }
        }

        write_job_lifecycle_state(path, rows)

        assert _gzip_path(path).exists()
        assert path.exists() is False
        lifecycle_state = read_job_lifecycle_state(path)
        assert lifecycle_state["job-1"]["status"] == "active"
        assert lifecycle_state["job-1"]["title"] == "Game Designer"


def test_lifecycle_state_reader_accepts_legacy_plain_json() -> None:
    with workspace_tmpdir("pipeline-lifecycle-state-legacy") as tmp:
        path = Path(tmp) / "jobs-lifecycle-state.json"
        path.write_text(
            json.dumps({"jobs": {"job-legacy": {"status": "likely_removed", "title": "Legacy"}}}),
            encoding="utf-8",
        )

        lifecycle_state = read_job_lifecycle_state(path)
        assert lifecycle_state["job-legacy"]["status"] == "likely_removed"
        assert lifecycle_state["job-legacy"]["title"] == "Legacy"


def test_lifecycle_archive_helpers_round_trip_gzip_storage() -> None:
    with workspace_tmpdir("pipeline-lifecycle-archive-gzip") as tmp:
        state_path = Path(tmp) / "jobs-lifecycle-state.json"
        archive_path = lifecycle_archive_state_path(state_path, 2024)
        rows = {
            "job-archive-1": {
                "status": "archived",
                "archivedAt": "2024-05-04T12:00:00+00:00",
                "removedAt": "2024-04-01T12:00:00+00:00",
                "title": "Archived Game Designer",
            }
        }

        write_job_lifecycle_archive_state(archive_path, rows)

        assert _gzip_path(archive_path).exists()
        assert archive_path.exists() is False
        archive_state = read_job_lifecycle_archive_state(archive_path)
        assert archive_state["job-archive-1"]["status"] == "archived"
        assert archive_state["job-archive-1"]["title"] == "Archived Game Designer"


def test_hot_lifecycle_reader_ignores_cold_archive_rows() -> None:
    with workspace_tmpdir("pipeline-lifecycle-hot-ignores-archive") as tmp:
        state_path = Path(tmp) / "jobs-lifecycle-state.json"
        archive_path = lifecycle_archive_state_path(state_path, 2024)
        write_job_lifecycle_state(
            state_path,
            {"job-hot": {"status": "active", "title": "Hot Game Designer"}},
        )
        write_job_lifecycle_archive_state(
            archive_path,
            {
                "job-archive-1": {
                    "status": "archived",
                    "archivedAt": "2024-05-04T12:00:00+00:00",
                    "removedAt": "2024-04-01T12:00:00+00:00",
                    "title": "Cold Game Designer",
                }
            },
        )

        hot_rows = read_job_lifecycle_state(state_path)
        assert "job-hot" in hot_rows
        assert hot_rows["job-hot"]["title"] == "Hot Game Designer"
        assert "job-archive-1" not in hot_rows


def test_runtime_launcher_serves_gzip_backed_pipeline_data() -> None:
    with workspace_tmpdir("pipeline-runtime-gzip") as tmp:
        root = Path(tmp) / "site"
        root.mkdir(parents=True, exist_ok=True)
        (root / "jobs.html").write_text("<html>jobs</html>\n", encoding="utf-8")
        data_dir = Path(tmp) / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        with gzip.open(data_dir / "jobs-unified.json.gz", mode="wt", encoding="utf-8") as handle:
            json.dump([{"title": "Compressed Runtime"}], handle)

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
            with urlopen(
                f"http://127.0.0.1:{server.server_address[1]}/data/jobs-unified.json",
                timeout=2.0,
            ) as response:
                assert response.headers.get("Content-Encoding") == "gzip"
                payload = gzip.decompress(response.read()).decode("utf-8")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        assert "Compressed Runtime" in payload
