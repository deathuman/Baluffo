"""Tests for ``run_profiled_alloc`` — the per-source tracemalloc hook."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.shared.profile_utils import (
    alloc_profile_log_path,
    run_profiled_alloc,
    runtime_alloc_profile_enabled,
)


def _work(allocate_mb: float) -> list[bytes]:
    # Each entry is 1 MiB — bigger than tracemalloc's minimum trace size.
    return [b"x" * (1024 * 1024) for _ in range(int(allocate_mb))]


def test_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BALUFFO_PROFILE_ALLOC", raising=False)
    assert not runtime_alloc_profile_enabled()


def test_enabled_by_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BALUFFO_PROFILE_ALLOC", "1")
    assert runtime_alloc_profile_enabled()


def test_passthrough_when_disabled(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("BALUFFO_PROFILE_ALLOC", raising=False)
    monkeypatch.setenv("BALUFFO_DATA_DIR", str(tmp_path))
    out = run_profiled_alloc(_work, 1, source_name="x")
    assert len(out) == 1
    assert len(out[0]) == 1024 * 1024
    # No log written when disabled
    assert not alloc_profile_log_path().exists()


def test_records_jsonl_when_enabled(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("BALUFFO_PROFILE_ALLOC", "1")
    monkeypatch.setenv("BALUFFO_DATA_DIR", str(tmp_path))
    out = run_profiled_alloc(_work, 4, source_name="src_a", profile_name="p_a")
    assert len(out) == 4
    log = alloc_profile_log_path()
    assert log.exists()
    entries = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert len(entries) == 1
    e = entries[0]
    assert e["source"] == "src_a"
    assert e["profile"] == "p_a"
    assert e["peak_mib"] > 0
    assert "top_frames" in e
    assert isinstance(e["top_frames"], list)
    # Each top frame should be pinned to a file:line string for aggregation
    for frame in e["top_frames"]:
        assert ":" in frame["frame"]
        assert "size_mib" in frame
        assert "count" in frame


def test_exception_still_logs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("BALUFFO_PROFILE_ALLOC", "1")
    monkeypatch.setenv("BALUFFO_DATA_DIR", str(tmp_path))

    def _crash() -> None:
        _work(2)
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        run_profiled_alloc(_crash, source_name="crash_src")
    log = alloc_profile_log_path()
    entries = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert entries and entries[-1]["source"] == "crash_src"


def test_aggregates_per_source(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Two sources write two rows; aggregation by source_name is trivial."""
    monkeypatch.setenv("BALUFFO_PROFILE_ALLOC", "1")
    monkeypatch.setenv("BALUFFO_DATA_DIR", str(tmp_path))
    run_profiled_alloc(_work, 1, source_name="src_x")
    run_profiled_alloc(_work, 8, source_name="src_y")
    log = alloc_profile_log_path()
    entries = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    by_source = {e["source"]: e["peak_mib"] for e in entries}
    assert set(by_source) == {"src_x", "src_y"}
    # src_y allocated 8x more — should be visibly larger peak
    assert by_source["src_y"] > by_source["src_x"]
