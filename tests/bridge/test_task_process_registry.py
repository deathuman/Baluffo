from __future__ import annotations

from typing import Any

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
