"""Focused tests for bounded browser-death forensics."""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.jobs import browser_death_forensics as forensics


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_cgroup_v2_snapshot_reads_limit_peak_and_events(tmp_path: Path) -> None:
    cgroup_root = tmp_path / "cgroup"
    cgroup_path = cgroup_root / "docker" / "abc123"
    _write(cgroup_path / "memory.max", "2684354560\n")
    _write(cgroup_path / "memory.current", "1900000000\n")
    _write(cgroup_path / "memory.peak", "2500000000\n")
    _write(
        cgroup_path / "memory.events",
        "low 0\nhigh 0\nmax 2\noom 1\noom_kill 1\n",
    )
    proc_cgroup = tmp_path / "proc-cgroup"
    _write(proc_cgroup, "0::/docker/abc123\n")

    snapshot = forensics.capture_cgroup_memory(
        proc_cgroup_path=proc_cgroup,
        cgroup_root=cgroup_root,
    )

    assert snapshot["available"] is True
    assert snapshot["version"] == "v2"
    assert Path(snapshot["path"]).name == "abc123"
    assert snapshot["memoryMax"] == "2684354560"
    assert snapshot["memoryMaxBytes"] == 2_684_354_560
    assert snapshot["memoryCurrentBytes"] == 1_900_000_000
    assert snapshot["memoryPeakBytes"] == 2_500_000_000
    assert snapshot["events"] == {"low": 0, "high": 0, "max": 2, "oom": 1, "oom_kill": 1}


def test_cgroup_v1_fallback_reads_oom_control_and_limit(tmp_path: Path) -> None:
    cgroup_root = tmp_path / "cgroup"
    cgroup_path = cgroup_root / "memory" / "docker" / "v1"
    _write(cgroup_path / "memory.limit_in_bytes", "1073741824\n")
    _write(cgroup_path / "memory.usage_in_bytes", "512000000\n")
    _write(cgroup_path / "memory.max_usage_in_bytes", "900000000\n")
    _write(cgroup_path / "memory.failcnt", "3\n")
    _write(cgroup_path / "memory.oom_control", "oom_kill_disable 0\nunder_oom 0\noom_kill 1\n")
    proc_cgroup = tmp_path / "proc-cgroup"
    _write(proc_cgroup, "5:memory:/docker/v1\n")

    snapshot = forensics.capture_cgroup_memory(
        proc_cgroup_path=proc_cgroup,
        cgroup_root=cgroup_root,
    )

    assert snapshot["available"] is True
    assert snapshot["version"] == "v1"
    assert snapshot["memoryMaxBytes"] == 1_073_741_824
    assert snapshot["events"] == {
        "oom_kill_disable": 0,
        "under_oom": 0,
        "oom_kill": 1,
        "failcnt": 3,
    }


def test_cgroup_missing_proc_file_is_bounded_not_an_error(tmp_path: Path) -> None:
    snapshot = forensics.capture_cgroup_memory(
        proc_cgroup_path=tmp_path / "missing",
        cgroup_root=tmp_path / "cgroup",
    )

    assert snapshot["available"] is False
    assert snapshot["reason"] == "proc_cgroup_unavailable"
    assert snapshot["events"] == {}


def test_shm_snapshot_reports_size_free_used_and_percent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        forensics.os,
        "statvfs",
        lambda _path: SimpleNamespace(
            f_frsize=4096,
            f_bsize=4096,
            f_blocks=100,
            f_bavail=25,
            f_bfree=30,
        ),
        raising=False,
    )

    snapshot = forensics.capture_shm("/dev/shm")

    assert snapshot == {
        "available": True,
        "path": "/dev/shm",
        "sizeBytes": 409_600,
        "freeBytes": 102_400,
        "usedBytes": 286_720,
        "freePercent": 25,
        "reason": "",
    }


def test_shm_missing_mount_is_bounded() -> None:
    snapshot = forensics.capture_shm("/definitely-not-a-real-shm-mount")

    assert snapshot["available"] is False
    assert snapshot["reason"]
    assert snapshot["sizeBytes"] == 0


def test_browser_stderr_tail_is_bounded_and_marks_truncation(tmp_path: Path) -> None:
    log_path = tmp_path / "chromium.log"
    log_path.write_bytes(b"a" * 9000 + b"last-line")

    snapshot = forensics.read_browser_stderr(str(log_path), max_bytes=1000)

    assert snapshot["available"] is True
    assert snapshot["source"] == "chromium_log_file"
    assert snapshot["bytes"] == 9009
    assert snapshot["truncated"] is True
    assert len(snapshot["tail"]) == 1000
    assert snapshot["tail"].endswith("last-line")


def test_browser_stderr_missing_file_is_bounded(tmp_path: Path) -> None:
    snapshot = forensics.read_browser_stderr(str(tmp_path / "missing.log"))

    assert snapshot["available"] is False
    assert snapshot["reason"] == "FileNotFoundError"
    assert snapshot["tail"] == ""


def test_browser_stderr_chromium_args_and_cleanup(tmp_path: Path) -> None:
    log_path = tmp_path / "chromium.log"
    args = forensics.browser_stderr_chromium_args(str(log_path))

    assert args[0] == "--enable-logging"
    assert f"--log-file={log_path}" in args
    assert "--v=0" in args
    assert forensics.browser_stderr_chromium_args(None) == []
    assert forensics.browser_raw_stderr_chromium_args() == ["--enable-logging=stderr", "--v=0"]


def test_browser_stderr_sink_keeps_crashpad_and_gpu_lines() -> None:
    payload = (
        b"[123:456:1116/120000.000000:ERROR:crashpad_client_linux.cc(123)] "
        b"crashpad handler failed\n"
        b"GPU process launch failed: error_code=1002\n"
    )
    sink = forensics.BrowserStderrSink()
    try:
        os.write(sink.fileno(), payload)
        sink.detach_writer()
        assert sink.wait_for_eof(1.0) is True
        snapshot = sink.snapshot()
    finally:
        sink.close()

    assert snapshot["available"] is True
    assert snapshot["source"] == forensics.BROWSER_STDERR_SOURCE
    assert snapshot["bytes"] == len(payload)
    assert snapshot["truncated"] is False
    assert "crashpad_client_linux.cc" in snapshot["tail"]
    assert "GPU process launch failed" in snapshot["tail"]


def test_browser_stderr_sink_keeps_the_latest_tail() -> None:
    sink = forensics.BrowserStderrSink(max_bytes=1000)
    try:
        os.write(sink.fileno(), b"a" * 9000 + b"last-line\n")
        sink.detach_writer()
        assert sink.wait_for_eof(1.0) is True
        snapshot = sink.snapshot()
    finally:
        sink.close()

    assert snapshot["bytes"] == 9010
    assert snapshot["truncated"] is True
    assert len(snapshot["tail"]) == 1000
    assert snapshot["tail"].endswith("last-line\n")


def test_driver_stderr_hook_redirects_playwright_driver_and_enables_browser_logs() -> None:
    transport = pytest.importorskip("playwright._impl.transport")
    original_fd = transport._get_stderr_fileno
    original_env = transport.get_driver_env
    sink = forensics.BrowserStderrSink()
    try:
        with forensics.capture_browser_stderr_for_driver(sink) as driver:
            assert driver["active"] is True
            assert driver["reason"] == ""
            assert driver["module"] in forensics.DRIVER_TRANSPORT_MODULES
            assert driver["playwrightVersion"]  # read from the installed distribution
            assert transport._get_stderr_fileno() == sink.fileno()
            debug = transport.get_driver_env()["DEBUG"]
            assert "pw:browser" in debug or "*" in debug
    finally:
        sink.close()

    assert transport._get_stderr_fileno is original_fd
    assert transport.get_driver_env is original_env


def test_build_forensics_computes_memory_event_delta(tmp_path: Path) -> None:
    before = {
        "capturedAt": "2026-11-16T00:00:00Z",
        "cgroup": {
            "available": True,
            "version": "v2",
            "path": "/docker/abc",
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
            "path": "/docker/abc",
            "memoryMax": "2684354560",
            "memoryMaxBytes": 2_684_354_560,
            "memoryPeakBytes": 2_684_354_560,
            "events": {"oom": 2, "oom_kill": 1},
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
    stderr = forensics.read_browser_stderr(None)
    disconnect = {"observed": True, "at": "2026-11-16T00:00:02Z", "duringProbe": True}

    result = forensics.build_browser_forensics(
        before,
        after,
        browser_stderr=stderr,
        disconnect=disconnect,
    )

    assert result["available"] is True
    assert result["cgroup"]["eventsBefore"] == {"oom": 0, "oom_kill": 0}
    assert result["cgroup"]["eventsAfter"] == {"oom": 2, "oom_kill": 1}
    assert result["cgroup"]["eventsDelta"] == {"oom": 2, "oom_kill": 1}
    assert result["cgroup"]["oomKilledDuringProbe"] is True
    assert result["cgroup"]["oomKillObserved"] is True
    assert result["shm"]["freeBytes"] == 1_000_000
    assert result["browserStderr"]["available"] is False
    assert result["disconnect"]["observed"] is True


def test_forensics_enabled_defaults_on_and_respects_kill_switch() -> None:
    assert forensics.browser_forensics_enabled({}) is True
    assert forensics.browser_forensics_enabled({"BALUFFO_BROWSER_FORENSICS": "0"}) is False
    assert forensics.browser_forensics_enabled({"BALUFFO_BROWSER_FORENSICS": "off"}) is False
    assert forensics.browser_forensics_enabled({"BALUFFO_BROWSER_FORENSICS": "1"}) is True


def test_driver_transport_resolves_the_second_module_name(monkeypatch: pytest.MonkeyPatch) -> None:
    # Playwright has shipped this module under both names. Probing only
    # `playwright._impl.transport` silently dropped the raw-stderr sink in the
    # container, leaving an empty log file as the only evidence of a death.
    fake = SimpleNamespace(_get_stderr_fileno=lambda: 1, get_driver_env=lambda: {})
    real_import = forensics.importlib.import_module

    def fake_import(name: str, package: str | None = None) -> object:
        if name == forensics.DRIVER_TRANSPORT_MODULES[0]:
            raise ImportError(name)
        if name == forensics.DRIVER_TRANSPORT_MODULES[1]:
            return fake
        return real_import(name, package)

    monkeypatch.setattr(forensics.importlib, "import_module", fake_import)

    module, name = forensics.resolve_driver_transport()

    assert module is fake
    assert name == forensics.DRIVER_TRANSPORT_MODULES[1]


def test_driver_hook_fallback_records_why_the_sink_did_not_engage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(forensics, "resolve_driver_transport", lambda: (None, ""))
    sink = forensics.BrowserStderrSink()
    try:
        with forensics.capture_browser_stderr_for_driver(sink) as driver:
            assert driver["active"] is False
            assert driver["reason"] == "driver-hook-unavailable"
            assert driver["module"] == ""
            assert isinstance(driver["playwrightVersion"], str)
    finally:
        sink.close()
