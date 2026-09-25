from __future__ import annotations

from typing import Any

import pytest

from src.bridge import task_process_registry
from src.bridge.task_process_registry import TaskProcessRegistry


class _FakeProcess:
    def __init__(self, pid: int, return_code: int | None) -> None:
        self.pid = pid
        self.return_code = return_code

    def poll(self) -> int | None:
        return self.return_code


class _BrokenProcess(_FakeProcess):
    def poll(self) -> int | None:
        raise OSError("process handle unavailable")


class _OverflowProcess(_FakeProcess):
    def poll(self) -> int | None:
        return float("inf")  # type: ignore[return-value]


class _WaitableProcess(_FakeProcess):
    def wait(self, timeout: float) -> int:
        self.return_code = 0
        return 0


def _register(registry: TaskProcessRegistry, process: Any) -> None:
    registry.register(
        task_type="discovery",
        run_id="discovery_1",
        process=process,
        command=["python", "discovery.py"],
    )


def test_inspect_reports_running_registered_process() -> None:
    registry = TaskProcessRegistry()
    _register(registry, _FakeProcess(41, None))

    assert registry.inspect("discovery", "discovery_1") == {
        "state": "running",
        "pid": 41,
        "identitySource": "registered_popen",
    }


def test_inspect_reports_exit_code_and_signal() -> None:
    registry = TaskProcessRegistry()
    _register(registry, _FakeProcess(41, -9))

    assert registry.inspect("discovery", "discovery_1") == {
        "state": "exited",
        "pid": 41,
        "returnCode": -9,
        "signal": 9,
        "identitySource": "registered_popen",
    }


def test_inspect_uses_unknown_for_missing_or_broken_identity() -> None:
    registry = TaskProcessRegistry()
    assert registry.inspect("discovery", "missing") == {
        "state": "unknown",
        "identitySource": "unregistered",
    }

    _register(registry, _BrokenProcess(41, None))
    assert registry.inspect("discovery", "discovery_1") == {
        "state": "unknown",
        "pid": 41,
        "identitySource": "registered_popen",
    }


def test_inspect_uses_registered_handle_when_pid_is_reused() -> None:
    registry = TaskProcessRegistry()
    _register(registry, _FakeProcess(41, None))
    _register(registry, _FakeProcess(41, 0))

    assert registry.inspect("discovery", "discovery_1") == {
        "state": "exited",
        "pid": 41,
        "returnCode": 0,
        "identitySource": "registered_popen",
    }


def test_inspect_reports_unknown_for_overflowing_return_code() -> None:
    registry = TaskProcessRegistry()
    _register(registry, _OverflowProcess(41, None))

    assert registry.inspect("discovery", "discovery_1") == {
        "state": "unknown",
        "pid": 41,
        "identitySource": "registered_popen",
    }


def test_terminate_windows_uses_taskkill_and_releases_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = TaskProcessRegistry()
    process = _WaitableProcess(41, None)
    _register(registry, process)
    calls: list[tuple[Any, ...]] = []

    def _record(args: Any, **kwargs: Any) -> None:
        calls.append(tuple(args))

    monkeypatch.setattr(task_process_registry.os, "name", "nt")
    monkeypatch.setattr(task_process_registry.subprocess, "run", _record)

    result = registry.terminate("discovery", "discovery_1")

    assert result["exited"] is True
    assert calls == [("taskkill", "/PID", "41", "/T", "/F")]
    assert registry.inspect("discovery", "discovery_1") == {
        "state": "unknown",
        "identitySource": "unregistered",
    }


def test_terminate_posix_uses_process_group_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = TaskProcessRegistry()
    process = _WaitableProcess(41, None)
    _register(registry, process)
    signals: list[tuple[int, Any]] = []
    monkeypatch.setattr(task_process_registry.os, "name", "posix")
    monkeypatch.setattr(
        task_process_registry.os,
        "killpg",
        lambda pid, signal_value: signals.append((pid, signal_value)),
        raising=False,
    )

    result = registry.terminate("discovery", "discovery_1")

    assert result["exited"] is True
    assert signals == [(41, task_process_registry.signal.SIGTERM)]
    assert registry.get("discovery", "discovery_1") is None
