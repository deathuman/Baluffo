"""Lock in the Phase 1 memory optimization.

When ``finalize_pipeline_run`` is invoked via ``run_pipeline``, we read
``jobs-lifecycle-state.json`` once during setup. ``_serialize_jobs_feed_reconciliation``
used to unconditionally re-read it at the finalize boundary — at 71k rows / ~35 MB
on disk that re-parse alone peaks at ~330 MB. The fix threads an mtime+size
fingerprint through so the finalize decorator can skip the re-read when nothing
changed, while still re-reading if an external writer touched the file.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.jobs.pipeline_finalize import _serialize_jobs_feed_reconciliation
from src.jobs.state_lifecycle import (
    lifecycle_state_fingerprint,
    read_job_lifecycle_state,
    write_job_lifecycle_state,
)


def _make_paths(tmp: Path) -> MagicMock:
    paths = MagicMock()
    paths.output_dir = tmp
    paths.lifecycle_state_path = tmp / "jobs-lifecycle-state.json"
    paths.json_path = tmp / "jobs-unified.json"
    return paths


def _write_state(tmp: Path) -> tuple[dict[str, dict], tuple[int, int]]:
    state_path = tmp / "jobs-lifecycle-state.json"
    write_job_lifecycle_state(
        state_path,
        {"job1": {"status": "active", "firstSeenAt": "2026-01-01T00:00:00+00:00"}},
    )
    rows = read_job_lifecycle_state(state_path)
    fp = lifecycle_state_fingerprint(state_path)
    assert fp is not None
    return rows, fp


def _fake_finalize(**kwargs):
    return {"lifecycle_keys": sorted(kwargs["lifecycle_rows"].keys())}


def test_matching_fingerprint_skips_reread(tmp_path: Path) -> None:
    rows, fp = _write_state(tmp_path)
    paths = _make_paths(tmp_path)
    spy_calls: list[Path] = []

    real = read_job_lifecycle_state

    def _spy(p: Path):
        spy_calls.append(p)
        return real(p)

    wrapped = _serialize_jobs_feed_reconciliation(_fake_finalize)
    with patch("src.jobs.pipeline_finalize.read_job_lifecycle_state", side_effect=_spy):
        result = wrapped(
            paths=paths,
            lifecycle_rows=rows,
            lifecycle_state_fingerprint=fp,
            canonical_rows=[],
            source_reports=[],
        )
    assert spy_calls == []
    assert result["lifecycle_keys"] == ["job1"]


def test_stale_fingerprint_triggers_reread(tmp_path: Path) -> None:
    rows, fp = _write_state(tmp_path)
    paths = _make_paths(tmp_path)

    # Simulate a concurrent writer updating the state (e.g. via the bridge).
    write_job_lifecycle_state(
        tmp_path / "jobs-lifecycle-state.json",
        {
            "job1": {"status": "active", "firstSeenAt": "2026-01-01T00:00:00+00:00"},
            "job2": {"status": "archived", "firstSeenAt": "2026-01-02T00:00:00+00:00"},
        },
    )

    spy_calls: list[Path] = []
    real = read_job_lifecycle_state

    def _spy(p: Path):
        spy_calls.append(p)
        return real(p)

    wrapped = _serialize_jobs_feed_reconciliation(_fake_finalize)
    with patch("src.jobs.pipeline_finalize.read_job_lifecycle_state", side_effect=_spy):
        result = wrapped(
            paths=paths,
            lifecycle_rows=rows,
            lifecycle_state_fingerprint=fp,
            canonical_rows=[],
            source_reports=[],
        )
    assert len(spy_calls) == 1
    assert result["lifecycle_keys"] == ["job1", "job2"]


def test_missing_fingerprint_triggers_reread(tmp_path: Path) -> None:
    rows, _ = _write_state(tmp_path)
    paths = _make_paths(tmp_path)

    spy_calls: list[Path] = []
    real = read_job_lifecycle_state

    def _spy(p: Path):
        spy_calls.append(p)
        return real(p)

    wrapped = _serialize_jobs_feed_reconciliation(_fake_finalize)
    with patch("src.jobs.pipeline_finalize.read_job_lifecycle_state", side_effect=_spy):
        result = wrapped(
            paths=paths,
            lifecycle_rows=rows,
            canonical_rows=[],
            source_reports=[],
        )
    assert len(spy_calls) == 1
    assert result["lifecycle_keys"] == ["job1"]


@pytest.mark.parametrize("use_gz", [True, False])
def test_fingerprint_resolves_gzip_backed_path(tmp_path: Path, use_gz: bool) -> None:
    """Fingerprint must follow whatever path ``read_job_lifecycle_state`` would use."""
    rows, fp = _write_state(tmp_path)
    # write_job_lifecycle_state transparently writes to .gz
    assert (tmp_path / "jobs-lifecycle-state.json.gz").exists()
    paths = _make_paths(tmp_path)

    # Even if the caller passes the logical .json path, the fingerprint uses the
    # resolved candidate. A subsequent writer must dirty the fingerprint.
    write_job_lifecycle_state(
        tmp_path / "jobs-lifecycle-state.json",
        {"job1": {"status": "archived", "firstSeenAt": "2026-01-01T00:00:00+00:00"}},
    )
    fp2 = lifecycle_state_fingerprint(paths.lifecycle_state_path)
    assert fp2 is not None and fp2 != fp
    # rows still intact in memory regardless
    assert rows
