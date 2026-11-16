"""Integration tests for bounded browser-death forensics.

Kept separate from the general health-probe suite so the existing 400-line test
file stays within its budget.
"""

from __future__ import annotations

import os
from typing import Any

import pytest

import src.jobs.browser_fallback_pool as pool_mod
from src.jobs import browser_death_forensics as forensics
from src.jobs.browser_fallback_pool import BrowserFallbackPool
from src.jobs.common.contracts_runtime import normalize_runtime_payload


class _FailingFuture:
    def __init__(self, message: str) -> None:
        self._message = message

    def result(self, timeout: float | None = None) -> None:
        raise RuntimeError(self._message)


class _FakeLoop:
    def call_soon_threadsafe(self, callback: Any) -> None:
        callback()

    def close(self) -> None:
        return

    def stop(self) -> None:
        return


class _DeadBrowser:
    def is_connected(self) -> bool:
        return False


def test_health_probe_records_bounded_death_forensics(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    before = {
        "capturedAt": "2026-11-16T00:00:00Z",
        "cgroup": {
            "available": True,
            "version": "v2",
            "path": "/docker/test",
            "memoryMax": "2684354560",
            "memoryMaxBytes": 2_684_354_560,
            "events": {"oom": 0, "oom_kill": 0},
        },
        "shm": {
            "available": True,
            "path": "/dev/shm",
            "sizeBytes": 67_108_864,
            "freeBytes": 67_000_000,
            "usedBytes": 108_864,
            "freePercent": 99,
        },
    }
    after = {
        "capturedAt": "2026-11-16T00:00:02Z",
        "cgroup": {
            "available": True,
            "version": "v2",
            "path": "/docker/test",
            "memoryMax": "2684354560",
            "memoryMaxBytes": 2_684_354_560,
            "events": {"oom": 1, "oom_kill": 1},
        },
        "shm": {
            "available": True,
            "path": "/dev/shm",
            "sizeBytes": 67_108_864,
            "freeBytes": 1_000_000,
            "usedBytes": 66_108_864,
            "freePercent": 1,
        },
    }
    snapshots = iter([before, after])
    monkeypatch.setattr(pool_mod, "capture_browser_environment", lambda: next(snapshots))
    monkeypatch.setattr(
        pool_mod.asyncio,
        "run_coroutine_threadsafe",
        lambda coro, _loop: (coro.close(), _FailingFuture("browser has been closed"))[1],
    )
    log_path = tmp_path / "chromium.log"
    log_path.write_text("GPU process launch failed: error_code=1002\n", encoding="utf-8")
    pool = BrowserFallbackPool()
    pool._ensure_started = lambda: None  # type: ignore[method-assign]
    pool._loop = _FakeLoop()  # type: ignore[assignment]
    pool._browser_environment_baseline = before
    pool._browser = _DeadBrowser()
    pool._browser_stderr_path = str(log_path)
    try:
        health = pool.health_probe(timeout_s=1)
    finally:
        pool.close()

    forensics = health["forensics"]
    assert forensics["available"] is True
    assert forensics["cgroup"]["eventsDelta"] == {"oom": 1, "oom_kill": 1}
    assert forensics["cgroup"]["oomKilledDuringProbe"] is True
    assert forensics["shm"]["freeBytes"] == 1_000_000
    assert forensics["browserStderr"]["available"] is True
    assert "GPU process launch failed" in forensics["browserStderr"]["tail"]
    assert forensics["disconnect"]["observed"] is True
    assert forensics["disconnect"]["duringProbe"] is True


def test_death_forensics_survive_the_relaunch_reset(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """The pool must preserve death facts before a relaunch clears them."""
    before = {
        "capturedAt": "2026-11-16T00:00:00Z",
        "cgroup": {"available": True, "version": "v2", "events": {"oom_kill": 0}},
        "shm": {"available": True, "path": "/dev/shm", "freeBytes": 67_000_000},
    }
    after = {
        "capturedAt": "2026-11-16T00:00:02Z",
        "cgroup": {
            "available": True,
            "version": "v2",
            "memoryMaxBytes": 2_684_354_560,
            "events": {"oom_kill": 1},
        },
        "shm": {
            "available": True,
            "path": "/dev/shm",
            "sizeBytes": 67_108_864,
            "freeBytes": 1_000_000,
        },
    }
    monkeypatch.setattr(pool_mod, "capture_browser_environment", lambda: after)
    log_path = tmp_path / "chromium.log"
    log_path.write_text("GPU process launch failed: error_code=1002\n", encoding="utf-8")
    pool = BrowserFallbackPool()
    pool._forensics_enabled = True
    pool._browser_environment_baseline = before
    pool._browser_stderr_path = str(log_path)
    pool._browser = _DeadBrowser()
    try:
        pool._record_failure("navigation", RuntimeError("browser has been closed"))
        # A relaunch resets the live snapshot and removes the browser log. The
        # death block must already be preserved before either happens.
        pool._reset_disconnect_snapshot()
        pool._browser_stderr_path = ""
        log_path.unlink()
        forensics = pool._forensics_snapshot(None)
    finally:
        pool.close()

    assert forensics["cgroup"]["eventsDelta"] == {"oom_kill": 1}
    assert forensics["cgroup"]["oomKilledDuringProbe"] is True
    assert forensics["shm"]["freeBytes"] == 1_000_000
    assert forensics["browserStderr"]["available"] is True
    assert "GPU process launch failed" in forensics["browserStderr"]["tail"]
    assert forensics["disconnect"]["observed"] is True
    assert forensics["disconnect"]["intentional"] is False


def test_raw_stderr_survives_relaunch_with_crashpad_and_gpu_lines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {
        "capturedAt": "2026-11-16T00:00:00Z",
        "cgroup": {"available": True, "version": "v2", "events": {"oom_kill": 0}},
        "shm": {"available": True, "path": "/dev/shm", "freeBytes": 67_000_000},
    }
    after = {
        "capturedAt": "2026-11-16T00:00:02Z",
        "cgroup": {
            "available": True,
            "version": "v2",
            "events": {"oom_kill": 1},
        },
        "shm": {"available": True, "path": "/dev/shm", "freeBytes": 1_000_000},
    }
    monkeypatch.setattr(pool_mod, "capture_browser_environment", lambda: after)
    sink = forensics.BrowserStderrSink()
    os.write(
        sink.fileno(),
        b"[123:456:1116/120000.000000:ERROR:crashpad_client_linux.cc(123)] "
        b"crashpad handler failed\n"
        b"GPU process launch failed: error_code=1002\n",
    )
    sink.detach_writer()
    assert sink.wait_for_eof(1.0) is True
    pool = BrowserFallbackPool()
    pool._forensics_enabled = True
    pool._browser_environment_baseline = before
    pool._browser_stderr_sink = sink
    pool._browser = _DeadBrowser()
    try:
        pool._record_failure("navigation", RuntimeError("browser has been closed"))
        # Exercise the real death relaunch, not a manual teardown: the live sink
        # and disconnect snapshot are gone, so only the preserved death block may
        # answer the later health probe.
        pool._relaunch_after_death()
        pool._reset_disconnect_snapshot()
        result = pool._forensics_snapshot(None)
    finally:
        pool.close()

    assert result["cgroup"]["eventsDelta"] == {"oom_kill": 1}
    assert result["disconnect"]["observed"] is True
    assert result["browserStderr"]["source"] == forensics.BROWSER_STDERR_SOURCE
    assert "crashpad_client_linux.cc" in result["browserStderr"]["tail"]
    assert "GPU process launch failed" in result["browserStderr"]["tail"]


def test_browser_disconnect_listener_records_probe_signal() -> None:
    class EventBrowser:
        def __init__(self) -> None:
            self.callback: Any = None

        def on(self, event: str, callback: Any) -> None:
            assert event == "disconnected"
            self.callback = callback

    pool = BrowserFallbackPool()
    browser = EventBrowser()
    pool._attach_browser_listener(browser)
    pool._set_probe_active(True)
    try:
        browser.callback(browser)
        snapshot = pool._disconnect_snapshot()
    finally:
        pool.close()

    assert snapshot["observed"] is True
    assert snapshot["duringProbe"] is True
    assert snapshot["intentional"] is False


def test_runtime_payload_normalizes_browser_health_forensics() -> None:
    payload = normalize_runtime_payload(
        {
            "browserFallbackHealth": {
                "ok": False,
                "stage": "navigation",
                "forensics": {
                    "available": True,
                    "capturedAt": "2026-11-16T00:00:02Z",
                    "cgroup": {
                        "available": True,
                        "version": "v2",
                        "path": "/docker/test",
                        "memoryMaxBytes": 2_684_354_560,
                        "eventsDelta": {"oom_kill": 1, "junk": 9},
                        "oomKilledDuringProbe": True,
                    },
                    "shm": {"available": True, "sizeBytes": 67_108_864, "freePercent": 200},
                    "browserStderr": {
                        "available": True,
                        "source": "chromium_log_file",
                        "tail": "x" * 9000,
                    },
                    "disconnect": {"observed": True, "duringProbe": True},
                },
            }
        },
        selected_source_count=0,
    )

    forensics = payload["browserFallbackHealth"]["forensics"]
    assert forensics["cgroup"]["eventsDelta"] == {"oom_kill": 1}
    assert forensics["cgroup"]["oomKilledDuringProbe"] is True
    assert forensics["shm"]["freePercent"] == 100
    assert len(forensics["browserStderr"]["tail"]) == 8192
    assert forensics["disconnect"]["observed"] is True
