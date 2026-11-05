from __future__ import annotations

import gzip
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from src.bridge.server import static_files
from src.bridge.server.static_files import STARTUP_FEED_BACKFILL_LIMIT, StaticFileService
from src.jobs.common.config import LIGHTWEIGHT_OUTPUT_FIELDS
from src.pipeline_io import serialize_rows_for_json


class _FakeHandler:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.sent: list[dict[str, Any]] = []

    def send_bytes(
        self,
        body: bytes,
        *,
        content_type: str,
        status: int = 200,
        **_headers: object,
    ) -> None:
        self.sent.append({"body": body, "status": status, "content_type": content_type})


def _feed_rows(count: int) -> list[dict[str, object]]:
    return [
        {
            "availabilityId": f"availability_{index}",
            "title": f"Job {index}",
            "company": "Studio",
            "jobLink": f"https://example.test/jobs/{index}",
            "status": "active",
        }
        for index in range(count)
    ]


def _write_light_feed(data_dir: Path, rows: list[dict[str, object]]) -> Path:
    path = data_dir / "jobs-unified-light.json.gz"
    with gzip.open(path, mode="wt", encoding="utf-8") as handle:
        handle.write(json.dumps(rows))
    return path


def _track_full_reads(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    reads: list[Path] = []
    real_read_json = static_files.read_json

    def tracking_read_json(path: Path, fallback: object) -> object:
        reads.append(Path(path))
        return real_read_json(path, fallback)

    monkeypatch.setattr(static_files, "read_json", tracking_read_json)
    return reads


def test_missing_startup_feed_is_rebuilt_from_a_feed_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    rows = _feed_rows(200)
    _write_light_feed(data_dir, rows)
    full_reads = _track_full_reads(monkeypatch)

    service = StaticFileService(static_root=tmp_path / "static", data_dir=data_dir)
    handler = _FakeHandler()

    assert service.handle_get(handler, path="/data/jobs-unified-startup.json") is True

    expected = serialize_rows_for_json(
        rows[:STARTUP_FEED_BACKFILL_LIMIT], LIGHTWEIGHT_OUTPUT_FIELDS
    )
    assert json.loads(handler.sent[-1]["body"]) == json.loads(expected)
    assert full_reads == []
    assert (data_dir / "jobs-unified-startup.json").is_file()


def test_oversized_startup_feed_is_repaired_from_a_feed_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_light_feed(data_dir, _feed_rows(120))
    startup_path = data_dir / "jobs-unified-startup.json"
    startup_path.write_text(json.dumps(_feed_rows(40)), encoding="utf-8")
    full_reads = _track_full_reads(monkeypatch)

    service = StaticFileService(static_root=tmp_path / "static", data_dir=data_dir)
    rebuilt = service._backfill_startup_feed()  # noqa: SLF001

    assert rebuilt == startup_path
    assert len(json.loads(startup_path.read_text(encoding="utf-8"))) == (
        STARTUP_FEED_BACKFILL_LIMIT
    )
    assert all(path.name != "jobs-unified-light.json" for path in full_reads)


def test_concurrent_cold_repairs_generate_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    service = StaticFileService(static_root=tmp_path / "static", data_dir=data_dir)
    barrier = threading.Barrier(6)
    generated = 0

    def slow_generate(_self: StaticFileService) -> bytes:
        nonlocal generated
        generated += 1
        time.sleep(0.05)
        return b"[]"

    monkeypatch.setattr(StaticFileService, "_generate_startup_feed_bytes", slow_generate)

    def run(_index: int) -> Path | None:
        barrier.wait(timeout=5.0)
        return service._backfill_startup_feed()  # noqa: SLF001

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(run, range(6)))

    assert generated == 1
    assert results == [data_dir / "jobs-unified-startup.json"] * 6
