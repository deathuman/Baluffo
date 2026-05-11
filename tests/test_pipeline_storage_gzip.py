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
from src.shared.json_io import (
    copy_json_file_to_storage,
    existing_json_candidate,
    gzip_backed_json_storage_path,
    read_json,
    write_json_text,
)
from src.ship import runtime_launcher as rl
from src.storage_metrics import reset_storage_metrics, snapshot_storage_metrics
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


def test_pipeline_json_writer_records_storage_metrics() -> None:
    with workspace_tmpdir("pipeline-gzip-writer-metrics") as tmp:
        reset_storage_metrics(data_dir=tmp, remove_file=True)
        path = Path(tmp) / "jobs-unified-light.json"

        assert write_atomic_if_changed(path, '[{"title":"Pipeline Engineer"}]\n') is True
        metrics = snapshot_storage_metrics(tmp)

        assert metrics["writes"]["writeCount"] == 1
        artifact = metrics["writes"]["artifacts"][0]
        assert artifact["artifact"] == "jobs-unified-light.json"
        assert artifact["storageKind"] == "gzip"
        assert artifact["lastCompressedSizeBytes"] > 0


def test_shared_json_copy_helper_compresses_policy_backed_plain_source() -> None:
    with workspace_tmpdir("shared-json-copy-gzip") as tmp:
        source = Path(tmp) / "source.json"
        target = Path(tmp) / "jobs-source-state.json"
        source.write_text('{"sources":{"demo":{"status":"ok"}}}', encoding="utf-8")

        copied = copy_json_file_to_storage(source, target)

        assert copied == _gzip_path(target)
        assert copied.exists()
        assert target.exists() is False
        assert existing_json_candidate(target) == copied
        assert read_json(target, {}) == {"sources": {"demo": {"status": "ok"}}}


def test_shared_json_write_helper_leaves_non_policy_json_plain() -> None:
    with workspace_tmpdir("shared-json-write-plain") as tmp:
        path = Path(tmp) / "jobs-fetch-report.json"

        written = write_json_text(path, '{"ok":true}')

        assert written == path
        assert gzip_backed_json_storage_path(path) == path
        assert path.read_text(encoding="utf-8") == '{"ok":true}'


def test_shared_json_write_helper_records_storage_metrics() -> None:
    with workspace_tmpdir("shared-json-write-metrics") as tmp:
        reset_storage_metrics(data_dir=tmp, remove_file=True)
        path = Path(tmp) / "jobs-unified.json"

        written = write_json_text(path, '[{"ok":true}]')
        metrics = snapshot_storage_metrics(tmp)

        assert written == _gzip_path(path)
        assert metrics["writes"]["writeCount"] == 1
        artifact = metrics["writes"]["artifacts"][0]
        assert artifact["artifact"] == "jobs-unified.json"
        assert artifact["storageKind"] == "gzip"
        assert artifact["uncompressedSizeBytes"]["total"] == len('[{"ok":true}]')
        assert artifact["compressedSizeBytes"]["total"] > 0


def test_source_state_helpers_round_trip_gzip_storage() -> None:
    with workspace_tmpdir("pipeline-source-state-gzip") as tmp:
        path = Path(tmp) / "jobs-source-state.json"
        rows = {
            "legacy_source": {
                "lastStatus": "ok",
                "lastRunAt": "2026-05-04T09:59:00Z",
                "lastCheckedAt": "2026-05-04T10:00:00Z",
                "lastSuccessAt": "2026-05-04T10:00:00Z",
                "lastKeptCount": 4,
                "consecutiveFailures": 0,
                "consecutiveZeroKept": 0,
            },
            "alias_source": {
                "lastStatus": "ok",
                "lastRunAt": "2026-05-04T11:59:00Z",
                "lastCheckedAt": "2026-05-04T12:00:00Z",
                "lastSuccessAt": "2026-05-04T12:00:00Z",
                "lastSuccessfulFetchAt": "2026-05-04T12:00:00Z",
                "lastSeenInFetchAt": "2026-05-04T12:00:00Z",
                "lastKeptCount": 2,
                "lastJobsKept": 2,
                "consecutiveFailures": 0,
                "failureCount": 0,
                "consecutiveZeroKept": 0,
                "zeroJobStreak": 0,
                "healthScore": 100,
                "health": "healthy",
                "healthReason": "last fetch kept jobs",
            },
        }

        write_source_state(path, rows)

        assert _gzip_path(path).exists()
        assert path.exists() is False
        with gzip.open(_gzip_path(path), mode="rt", encoding="utf-8") as handle:
            payload = handle.read()
        assert '"lastSuccessfulFetchAt"' in payload
        assert '"healthReason"' in payload

        source_state = read_source_state(path)
        legacy = source_state["legacy_source"]
        assert legacy["lastSuccessfulFetchAt"] == "2026-05-04T10:00:00Z"
        assert legacy["lastSeenInFetchAt"] == "2026-05-04T10:00:00Z"
        assert legacy["lastJobsKept"] == 4
        assert legacy["failureCount"] == 0
        assert legacy["zeroJobStreak"] == 0
        assert legacy["healthScore"] == 100
        assert legacy["health"] == "healthy"
        assert legacy["healthReason"] == "last fetch kept jobs"

        alias = source_state["alias_source"]
        assert alias["lastSuccessfulFetchAt"] == "2026-05-04T12:00:00Z"
        assert alias["lastSeenInFetchAt"] == "2026-05-04T12:00:00Z"
        assert alias["lastJobsKept"] == 2
        assert alias["failureCount"] == 0
        assert alias["zeroJobStreak"] == 0
        assert alias["healthScore"] == 100
        assert alias["health"] == "healthy"
        assert alias["healthReason"] == "last fetch kept jobs"


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


def test_runtime_launcher_serves_large_gzip_backed_pipeline_snapshot() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    fixture_path = repo_root / "data" / "jobs-unified.json"
    fixture_rows = read_json(fixture_path, [])

    assert isinstance(fixture_rows, list)
    assert len(fixture_rows) > 1000

    with workspace_tmpdir("pipeline-runtime-gzip-large") as tmp:
        root = Path(tmp) / "site"
        root.mkdir(parents=True, exist_ok=True)
        (root / "jobs.html").write_text("<html>jobs</html>\n", encoding="utf-8")
        data_dir = Path(tmp) / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        with gzip.open(data_dir / "jobs-unified.json.gz", mode="wt", encoding="utf-8") as handle:
            json.dump(fixture_rows, handle)

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
                timeout=5.0,
            ) as response:
                assert response.headers.get("Content-Encoding") == "gzip"
                served_rows = json.loads(gzip.decompress(response.read()).decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        assert served_rows == fixture_rows
