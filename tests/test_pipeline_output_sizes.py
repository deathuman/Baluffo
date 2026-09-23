from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from src.jobs.finalize_output import _output_sizes
from src.shared.json_io import gzip_backed_json_storage_path


def test_output_sizes_reads_transparent_gzip_targets(tmp_path: Path) -> None:
    json_path = tmp_path / "jobs-unified.json"
    light_json_path = tmp_path / "jobs-unified-light.json"
    gzip_target = gzip_backed_json_storage_path(json_path)
    light_gzip_target = gzip_backed_json_storage_path(light_json_path)
    gzip_target.write_bytes(b'["full"]')
    light_gzip_target.write_bytes(b'["light"]')
    paths = SimpleNamespace(json_path=json_path, light_json_path=light_json_path)

    assert _output_sizes(paths) == (len(b'["full"]'), len(b'["light"]'))


def test_output_sizes_accepts_legacy_plain_targets(tmp_path: Path) -> None:
    json_path = tmp_path / "jobs-unified.json"
    light_json_path = tmp_path / "jobs-unified-light.json"
    json_path.write_bytes(b"full")
    light_json_path.write_bytes(b"light")
    paths = SimpleNamespace(json_path=json_path, light_json_path=light_json_path)

    assert _output_sizes(paths) == (4, 5)


def test_output_sizes_returns_zero_when_targets_are_missing(tmp_path: Path) -> None:
    paths = SimpleNamespace(
        json_path=tmp_path / "jobs-unified.json",
        light_json_path=tmp_path / "jobs-unified-light.json",
    )

    assert _output_sizes(paths) == (0, 0)
