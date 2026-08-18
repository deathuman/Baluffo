"""Bridge-local process registry for abortable task children."""

from __future__ import annotations

import os
import signal
import subprocess
import threading
from dataclasses import dataclass, field
from typing import Any, cast


@dataclass(frozen=True)
class TaskProcessEntry:
    task_type: str
    run_id: str
    pid: int
    process: Any
    command: tuple[str, ...]
    metadata: dict[str, Any] = field(default_factory=dict)


class TaskProcessRegistry:
    """Long-lived registry of subprocess handles keyed by task type and run id."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: dict[tuple[str, str], TaskProcessEntry] = {}

    @staticmethod
    def _key(task_type: str, run_id: str) -> tuple[str, str]:
        return (str(task_type or "").strip().lower(), str(run_id or "").strip())

    def register(
        self,
        *,
        task_type: str,
        run_id: str,
        process: Any,
        command: list[str],
        metadata: dict[str, Any] | None = None,
    ) -> TaskProcessEntry | None:
        key = self._key(task_type, run_id)
        if not key[0] or not key[1]:
            return None
        try:
            pid = int(getattr(process, "pid", 0) or 0)
        except (TypeError, ValueError):
            pid = 0
        if pid <= 0:
            return None
        entry = TaskProcessEntry(
            task_type=key[0],
            run_id=key[1],
            pid=pid,
            process=process,
            command=tuple(str(item) for item in command),
            metadata=dict(metadata or {}),
        )
        with self._lock:
            self._entries[key] = entry
        return entry

    def get(self, task_type: str, run_id: str) -> TaskProcessEntry | None:
        with self._lock:
            return self._entries.get(self._key(task_type, run_id))

    def unregister(self, task_type: str, run_id: str) -> None:
        with self._lock:
            self._entries.pop(self._key(task_type, run_id), None)

    @staticmethod
    def _process_exited(process: Any) -> bool:
        poll = getattr(process, "poll", None)
        if not callable(poll):
            return False
        try:
            return poll() is not None
        except (OSError, RuntimeError, TypeError, ValueError):
            return False

    @staticmethod
    def _wait_process(process: Any, timeout_s: float) -> bool:
        wait = getattr(process, "wait", None)
        if not callable(wait):
            return False
        try:
            wait(timeout=max(0.05, float(timeout_s)))
            return True
        except subprocess.TimeoutExpired:
            return False
        except (OSError, RuntimeError, TypeError, ValueError):
            return False

    @staticmethod
    def _terminate_windows(entry: TaskProcessEntry, timeout_s: float) -> list[str]:
        try:
            subprocess.run(
                ["taskkill", "/PID", str(entry.pid), "/T", "/F"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=max(1.0, float(timeout_s)),
            )
            return []
        except (OSError, subprocess.TimeoutExpired, RuntimeError, ValueError) as exc:
            return [f"taskkill_failed:{exc}"]

    @staticmethod
    def _send_posix_signal(
        entry: TaskProcessEntry, signal_value: signal.Signals, warning_prefix: str
    ) -> tuple[bool, list[str]]:
        try:
            cast(Any, os).killpg(entry.pid, signal_value)
            return False, []
        except ProcessLookupError:
            return True, []
        except (OSError, RuntimeError, ValueError) as exc:
            return False, [f"{warning_prefix}:{exc}"]

    def _terminate_posix(
        self, entry: TaskProcessEntry, process: Any, timeout_s: float
    ) -> tuple[bool, list[str]]:
        disappeared, warnings = self._send_posix_signal(entry, signal.SIGTERM, "sigterm_failed")
        if disappeared:
            return True, warnings
        exited = self._wait_process(process, timeout_s)
        if exited:
            return True, warnings
        disappeared, kill_warnings = self._send_posix_signal(
            entry,
            cast(Any, signal).SIGKILL,
            "sigkill_failed",
        )
        warnings.extend(kill_warnings)
        return disappeared or self._wait_process(process, 1.0), warnings

    def terminate(
        self,
        task_type: str,
        run_id: str,
        *,
        timeout_s: float = 3.0,
    ) -> dict[str, Any]:
        entry = self.get(task_type, run_id)
        if entry is None:
            return {"ok": False, "exited": False, "warning": "process_not_registered"}
        process = entry.process
        if self._process_exited(process):
            self.unregister(task_type, run_id)
            return {"ok": True, "exited": True, "pid": entry.pid}

        if os.name == "nt":
            warnings = self._terminate_windows(entry, timeout_s)
            exited = self._wait_process(process, timeout_s) or self._process_exited(process)
        else:
            exited, warnings = self._terminate_posix(entry, process, timeout_s)

        if exited:
            self.unregister(task_type, run_id)
        return {"ok": True, "exited": bool(exited), "pid": entry.pid, "warnings": warnings}


__all__ = ["TaskProcessEntry", "TaskProcessRegistry"]
