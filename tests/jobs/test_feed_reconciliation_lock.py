"""Tests for the cross-process jobs-feed reconciliation lock."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import time

import pytest

import src.jobs.feed_reconciliation_lock as lock_module
from src.jobs.feed_reconciliation_lock import (
    ReconciliationLockTimeout,
    _lock_holder_summary,
    clear_finalize_pending,
    finalize_pending_path,
    jobs_feed_reconciliation_lock,
    mark_finalize_pending,
    read_finalize_pending,
    wait_for_finalize_pending_clear,
)

_POSIX_ONLY = pytest.mark.skipif(
    os.name == "nt", reason="flock contention assertions are POSIX-specific"
)


def test_nested_acquisition_is_reentrant(tmp_path, monkeypatch):
    monkeypatch.setenv("BALUFFO_RECONCILIATION_LOCK_TIMEOUT_S", "5")

    with jobs_feed_reconciliation_lock(tmp_path):
        with jobs_feed_reconciliation_lock(tmp_path):
            pass


def test_lock_is_reacquirable_after_release(tmp_path, monkeypatch):
    monkeypatch.setenv("BALUFFO_RECONCILIATION_LOCK_TIMEOUT_S", "5")

    for _ in range(2):
        with jobs_feed_reconciliation_lock(tmp_path):
            pass


@_POSIX_ONLY
def test_timeout_names_the_holder(tmp_path, monkeypatch):
    import fcntl

    monkeypatch.setenv("BALUFFO_RECONCILIATION_LOCK_TIMEOUT_S", "0.25")
    monkeypatch.setattr(lock_module, "_LOCK_RETRY_DELAY_S", 0.01)
    lock_path = tmp_path / ".jobs-feed-reconciliation.lock"
    handle = lock_path.open("a+b")
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises(ReconciliationLockTimeout) as excinfo:
            with jobs_feed_reconciliation_lock(tmp_path):
                pytest.fail("lock should not have been acquired")
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()

    timeout = excinfo.value
    message = str(timeout)
    assert "timed out" in message
    assert f"pid={os.getpid()}" in message
    # The failure report persists these structurally, so they must be readable
    # without parsing the message.
    assert timeout.timeout_s == 0.25
    assert timeout.lock_path == str(lock_path)
    assert timeout.holder_pid == os.getpid()
    assert timeout.holder == f"pid={os.getpid()} cmd={timeout.holder_command}"


@_POSIX_ONLY
def test_waiting_log_names_the_holder(tmp_path, monkeypatch, capsys):
    import fcntl

    monkeypatch.setenv("BALUFFO_RECONCILIATION_LOCK_TIMEOUT_S", "1.0")
    monkeypatch.setattr(lock_module, "_LOCK_WARN_INTERVAL_S", 0.05)
    monkeypatch.setattr(lock_module, "_LOCK_RETRY_DELAY_S", 0.01)
    lock_path = tmp_path / ".jobs-feed-reconciliation.lock"
    handle = lock_path.open("a+b")
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises(ReconciliationLockTimeout):
            with jobs_feed_reconciliation_lock(tmp_path):
                pass
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()

    out = capsys.readouterr().out
    assert "[reconciliation-lock] WARN waiting" in out
    assert f"holder=pid={os.getpid()}" in out


def test_holder_summary_is_unknown_for_missing_path(tmp_path):
    assert _lock_holder_summary(tmp_path / "missing.lock") == "unknown"


def test_timeout_env_zero_waits_forever(monkeypatch):
    monkeypatch.setenv("BALUFFO_RECONCILIATION_LOCK_TIMEOUT_S", "0")
    assert lock_module._lock_timeout_s() == 0.0


def test_timeout_env_invalid_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("BALUFFO_RECONCILIATION_LOCK_TIMEOUT_S", "not-a-number")
    assert lock_module._lock_timeout_s() == lock_module._DEFAULT_LOCK_TIMEOUT_S


@_POSIX_ONLY
def test_timeout_names_the_holder_operation(tmp_path, monkeypatch):
    """The timeout must name what the bridge was doing, not only its pid."""
    monkeypatch.setenv("BALUFFO_RECONCILIATION_LOCK_TIMEOUT_S", "0.4")
    monkeypatch.setattr(lock_module, "_LOCK_RETRY_DELAY_S", 0.01)
    holder_script = textwrap.dedent(
        """
        import sys
        import time
        from pathlib import Path

        from src.jobs.feed_reconciliation_lock import jobs_feed_reconciliation_lock

        with jobs_feed_reconciliation_lock(Path(sys.argv[1]), operation="availability_commit"):
            Path(sys.argv[2]).write_text("ready", encoding="utf-8")
            time.sleep(2.0)
        """
    )
    ready_path = tmp_path / "holder-ready"
    holder = subprocess.Popen(
        [sys.executable, "-c", holder_script, str(tmp_path), str(ready_path)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not ready_path.exists():
            if holder.poll() is not None:
                break
            time.sleep(0.01)
        assert ready_path.exists(), "holder process did not acquire the lock"
        with pytest.raises(ReconciliationLockTimeout) as excinfo:
            with jobs_feed_reconciliation_lock(tmp_path):
                pytest.fail("lock should not have been acquired")
    finally:
        try:
            _, stderr = holder.communicate(timeout=10.0)
        except subprocess.TimeoutExpired:
            holder.kill()
            _, stderr = holder.communicate(timeout=5.0)

    assert holder.returncode == 0, stderr
    timeout = excinfo.value
    assert not sys.platform.startswith("linux") or timeout.holder_pid == holder.pid
    assert timeout.holder_operation == "availability_commit"
    assert timeout.holder_thread
    assert timeout.holder_acquired_at
    assert "op=availability_commit" in str(timeout)
    assert "op=availability_commit" in timeout.holder


def test_finalize_pending_marker_round_trip(tmp_path):
    assert read_finalize_pending(tmp_path) is None

    path = mark_finalize_pending(tmp_path, run_id="pipeline_1")

    assert path == finalize_pending_path(tmp_path)
    assert path.exists()
    pending = read_finalize_pending(tmp_path)
    assert pending is not None
    assert pending["runId"] == "pipeline_1"
    assert wait_for_finalize_pending_clear(tmp_path, timeout_s=0.01, poll_s=0.01) is False

    clear_finalize_pending(tmp_path)

    assert read_finalize_pending(tmp_path) is None
    assert wait_for_finalize_pending_clear(tmp_path, timeout_s=0.01, poll_s=0.01) is True


def test_stale_finalize_pending_marker_is_ignored(tmp_path):
    path = finalize_pending_path(tmp_path)
    path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "capturedAtEpoch": time.time() - 10,
                "staleAfterS": 1,
            }
        ),
        encoding="utf-8",
    )

    assert read_finalize_pending(tmp_path) is None
    assert not path.exists()
