"""Finalization must publish ahead of the advisory availability sweep."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

import pytest

from src.jobs import pipeline_finalize
from src.jobs.feed_reconciliation_lock import (
    ReconciliationLockTimeout,
    read_finalize_pending,
)


def _timeout(data_dir: Path, *, timeout_s: float = 600.0) -> ReconciliationLockTimeout:
    return ReconciliationLockTimeout(
        f"timed out after {timeout_s:.0f}s waiting for {data_dir}",
        lock_path=data_dir / ".jobs-feed-reconciliation.lock",
        timeout_s=timeout_s,
        holder="pid=18 cmd=python -m src.container_server",
        holder_pid=18,
        holder_command="python -m src.container_server",
        holder_operation="availability_commit",
    )


def test_finalize_retries_after_one_lock_timeout_and_clears_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[float | None] = []
    body_ran = False

    @contextmanager
    def flaky_lock(data_dir: Path, *, operation: str = "", timeout_s: float | None = None):
        nonlocal body_ran
        assert operation == "fetch_finalize"
        assert read_finalize_pending(data_dir) is not None
        calls.append(timeout_s)
        if len(calls) == 1:
            raise _timeout(data_dir, timeout_s=timeout_s or 600.0)
        body_ran = True
        yield

    monkeypatch.setattr(pipeline_finalize, "jobs_feed_reconciliation_lock", flaky_lock)
    monkeypatch.setattr(pipeline_finalize, "FINALIZE_LOCK_RETRY_DELAY_S", 0.0)
    stale_candidate = tmp_path / "jobs-fetch-candidate.json"
    stale_candidate.write_text("{}", encoding="utf-8")

    with pipeline_finalize._finalize_lock_with_priority(
        tmp_path,
        run_id="pipeline_1",
        candidate_rows=[],
        started_at="2026-11-10T09:47:00Z",
        finished_at="2026-11-10T13:40:00Z",
    ):
        pass

    assert body_ran is True
    assert calls == [600.0, 600.0]
    assert read_finalize_pending(tmp_path) is None
    assert not stale_candidate.exists()


def test_finalize_lock_timeout_honors_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BALUFFO_RECONCILIATION_LOCK_TIMEOUT_S", "0")
    assert pipeline_finalize._finalize_lock_timeout_s() == 0.0

    monkeypatch.setenv("BALUFFO_RECONCILIATION_LOCK_TIMEOUT_S", "not-a-number")
    assert pipeline_finalize._finalize_lock_timeout_s() == pipeline_finalize.FINALIZE_LOCK_TIMEOUT_S


def test_finalize_final_timeout_preserves_finished_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    @contextmanager
    def busy_lock(data_dir: Path, *, operation: str = "", timeout_s: float | None = None):
        assert operation == "fetch_finalize"
        raise _timeout(data_dir, timeout_s=timeout_s or 600.0)
        yield  # pragma: no cover - keeps this a context manager

    monkeypatch.setattr(pipeline_finalize, "jobs_feed_reconciliation_lock", busy_lock)
    monkeypatch.setattr(pipeline_finalize, "FINALIZE_LOCK_RETRY_DELAY_S", 0.0)
    rows = [
        {"id": "job-1", "title": "Gameplay Engineer"},
        {"id": "job-2", "title": "Technical Artist"},
    ]

    with pytest.raises(ReconciliationLockTimeout) as excinfo:
        with pipeline_finalize._finalize_lock_with_priority(
            tmp_path,
            run_id="pipeline_1",
            candidate_rows=rows,
            started_at="2026-11-10T09:47:00Z",
            finished_at="2026-11-10T13:40:00Z",
        ):
            pass

    timeout = excinfo.value
    assert timeout.candidate_count == 2
    candidate_path = Path(timeout.candidate_artifact)
    assert candidate_path == tmp_path / "jobs-fetch-candidate.json"
    payload = json.loads(candidate_path.read_text(encoding="utf-8"))
    assert payload["runId"] == "pipeline_1"
    assert payload["reason"] == "reconciliation_lock_timeout"
    assert payload["candidateCount"] == 2
    assert payload["rows"] == rows
    assert read_finalize_pending(tmp_path) is None
