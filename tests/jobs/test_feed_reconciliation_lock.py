"""Tests for the cross-process jobs-feed reconciliation lock."""

from __future__ import annotations

import os

import pytest

import src.jobs.feed_reconciliation_lock as lock_module
from src.jobs.feed_reconciliation_lock import (
    ReconciliationLockTimeout,
    _lock_holder_summary,
    jobs_feed_reconciliation_lock,
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
