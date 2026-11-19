"""A registry the drain cannot read is not a registry with zero boards.

``read_registry`` returned ``[]`` for both a missing and an unreadable file. On a corrupt
active registry the registration report therefore said every curated board was
unregistered, and ``--verify-collected`` -- the delivery gate -- exited 0 because it only
fires when something registered. These tests pin the distinction: absent is empty,
unreadable is fatal.
"""

from __future__ import annotations

import gzip
import importlib.util
import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "tools"))


def _load_tool():
    spec = importlib.util.spec_from_file_location(
        "coverage_drain_registry_under_test", _ROOT / "tools" / "coverage_drain.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


DRAIN = _load_tool()
ACTIVE = DRAIN.REGISTRY_ACTIVE


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(b"{not json", id="plain-not-json"),
        pytest.param(b'[3, {"id": "kept"}]', id="non-row-entry"),
        pytest.param(gzip.compress(b"[]")[:-8], id="gzip-truncated"),
        pytest.param(gzip.compress(b'{"active": []}'), id="gzip-object"),
    ],
)
def test_unreadable_active_registry_is_fatal(tmp_path: Path, payload: bytes) -> None:
    (tmp_path / ACTIVE).write_bytes(payload)

    with pytest.raises(SystemExit, match="registry"):
        DRAIN.read_registry(tmp_path, ACTIVE)


def test_round_summary_does_not_turn_an_unreadable_registry_into_zeros(tmp_path: Path) -> None:
    (tmp_path / DRAIN.REPORT).write_text(json.dumps({"summary": {}}), encoding="utf-8")
    (tmp_path / ACTIVE).write_bytes(b"{not json")

    with pytest.raises(SystemExit, match="registry"):
        DRAIN.round_summary(tmp_path)


def test_a_missing_bucket_is_still_empty(tmp_path: Path) -> None:
    assert DRAIN.read_registry(tmp_path, DRAIN.REGISTRY_PENDING) == []


def test_a_valid_registry_still_reads(tmp_path: Path) -> None:
    rows = [{"id": "static:listing_url:https://a.example/careers"}]
    (tmp_path / ACTIVE).write_bytes(gzip.compress(json.dumps(rows).encode("utf-8")))

    assert DRAIN.read_registry(tmp_path, ACTIVE) == rows


def test_a_missing_discovery_report_is_not_a_zero_result(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="discovery report"):
        DRAIN.round_summary(tmp_path)


def test_unreadable_fetch_output_is_fatal_not_zero_collected(tmp_path: Path) -> None:
    (tmp_path / DRAIN.FETCH_OUTPUT).write_bytes(b"{not json")

    with pytest.raises(SystemExit, match="fetch artifact"):
        DRAIN.read_json(tmp_path / DRAIN.FETCH_OUTPUT)


# --- the seed registry ----------------------------------------------------------------


def test_a_missing_seed_registry_is_fatal_rather_than_unseeded(tmp_path: Path) -> None:
    """An unseeded run measures an empty registry, not the one the user named."""
    with pytest.raises(SystemExit, match="seed registry"):
        DRAIN.seed_data_dir(tmp_path / "data", tmp_path / "absent-seed.json")


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param("{not json", id="plain-not-json"),
        pytest.param('{"active": []}', id="object-without-rows"),
        pytest.param('[3, {"id": "kept"}]', id="non-row-entry"),
    ],
)
def test_a_bad_seed_registry_is_fatal_before_the_data_dir_is_wiped(
    tmp_path: Path, payload: str
) -> None:
    seed = tmp_path / "seed.json"
    seed.write_text(payload, encoding="utf-8")
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "previous-run.txt").write_text("keep", encoding="utf-8")

    with pytest.raises(SystemExit, match="seed registry"):
        DRAIN.seed_data_dir(data_dir, seed)

    assert (data_dir / "previous-run.txt").exists()


def test_a_valid_seed_registry_is_written_gzipped(tmp_path: Path) -> None:
    rows = [{"id": "static:listing_url:https://a.example/careers"}]
    seed = tmp_path / "seed.json"
    seed.write_text(json.dumps(rows), encoding="utf-8")
    data_dir = tmp_path / "data"

    DRAIN.seed_data_dir(data_dir, seed)

    seeded = DRAIN.read_registry(data_dir, ACTIVE)
    assert [row["id"] for row in seeded] == [rows[0]["id"]]
    assert seeded[0]["board_url"] == "https://a.example/careers"
