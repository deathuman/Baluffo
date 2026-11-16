"""Bounded read-only browser-death forensics for the health probe.

The health probe already preserves the Playwright exception. That exception does not
say whether Chromium was killed by the kernel, crashed after launch, or never got a
usable page because the container's shared-memory mount was exhausted. This leaf adds
the observation-only facts needed to tell those apart without host SSH:

* cgroup memory limit, current/peak use, and ``memory.events`` before/after;
* ``/dev/shm`` size/free;
* a bounded tail of the browser process's raw stderr (captured through Playwright's
  driver), with the legacy Chromium log-file sink as a fallback;
* the browser's ``disconnected`` event timing.

No value here changes browser policy. Every read is best-effort and returns an
``available: false`` reason instead of raising.
"""

from __future__ import annotations

import os
import tempfile
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from src.shared.utils import now_iso

BROWSER_FORENSICS_ENV = "BALUFFO_BROWSER_FORENSICS"
BROWSER_STDERR_SOURCE = "chromium_stderr"
MAX_BROWSER_STDERR_BYTES = 8192
MAX_BROWSER_STDERR_TAIL_CHARS = 8192
MAX_CGROUP_PATH_CHARS = 512
_CGROUP_EVENT_KEYS = ("low", "high", "max", "oom", "oom_kill", "failcnt")
_DRIVER_HOOK_LOCK = threading.Lock()


def browser_forensics_enabled(env: Mapping[str, str] | None = None) -> bool:
    """Forensics are on by default; ``BALUFFO_BROWSER_FORENSICS=0`` disables them."""
    values = os.environ if env is None else env
    raw = str(values.get(BROWSER_FORENSICS_ENV) or "").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def prepare_browser_stderr_file(*, prefix: str = "baluffo-chromium-") -> str:
    """Create the bounded sink passed to Chromium's ``--log-file`` switch."""
    fd, path = tempfile.mkstemp(prefix=prefix, suffix=".log")
    os.close(fd)
    return path


def browser_stderr_chromium_args(path: str | None) -> list[str]:
    """Chromium logging switches that write browser stderr to a local file.

    ``--log-file`` is the concrete sink; ``--enable-logging`` is required for the
    file to receive LOG(ERROR) lines such as the GPU-helper failure observed on
    the Umbrel host. The volume is deliberately low (``--v=0``); the report only
    carries a bounded tail.
    """
    if not path:
        return []
    return ["--enable-logging", f"--log-file={path}", "--v=0"]


def cleanup_browser_stderr_file(path: str | None) -> None:
    if not path:
        return
    try:
        Path(path).unlink(missing_ok=True)
    except OSError:
        pass


class BrowserStderrSink:
    """A bounded in-memory tail of the browser process's raw stderr.

    Playwright does not expose the browser subprocess's stderr directly. Its Node driver
    forwards browser stderr to its own stderr when ``DEBUG=pw:browser`` is set, so the pool
    points the driver's stderr at this sink's pipe. A reader thread drains the pipe and keeps
    only the latest ``max_bytes``: the pipe cannot fill, and the report cannot grow.
    """

    def __init__(self, *, max_bytes: int = MAX_BROWSER_STDERR_BYTES) -> None:
        self._max_bytes = max(1, int(max_bytes or MAX_BROWSER_STDERR_BYTES))
        self._lock = threading.Lock()
        self._buffer = bytearray()
        self._total_bytes = 0
        self._eof = threading.Event()
        self._read_fd, self._write_fd = os.pipe()
        self._thread = threading.Thread(
            target=self._drain,
            name="browser-stderr-sink",
            daemon=True,
        )
        self._thread.start()

    def fileno(self) -> int:
        """The write end Playwright's driver should use for stderr."""
        with self._lock:
            fd = self._write_fd
        if fd < 0:
            raise OSError("browser stderr sink is closed")
        return fd

    def _drain(self) -> None:
        read_fd = self._read_fd
        try:
            while True:
                chunk = os.read(read_fd, 4096)
                if not chunk:
                    break
                with self._lock:
                    self._total_bytes += len(chunk)
                    self._buffer.extend(chunk)
                    if len(self._buffer) > self._max_bytes:
                        del self._buffer[: -self._max_bytes]
        except OSError:
            pass
        finally:
            with self._lock:
                owns_read_fd = self._read_fd == read_fd
                if owns_read_fd:
                    self._read_fd = -1
            if owns_read_fd:
                try:
                    os.close(read_fd)
                except OSError:
                    pass
            self._eof.set()

    def detach_writer(self) -> None:
        """Close the parent's write end after the driver has inherited it."""
        with self._lock:
            fd = self._write_fd
            self._write_fd = -1
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass

    def wait_for_eof(self, timeout_s: float = 1.0) -> bool:
        """Wait until the read side reaches EOF; useful in focused tests."""
        return self._eof.wait(max(0.0, float(timeout_s)))

    def snapshot(self) -> dict[str, Any]:
        """Return the same bounded shape as :func:`read_browser_stderr`."""
        with self._lock:
            tail_bytes = bytes(self._buffer)
            total_bytes = self._total_bytes
        return {
            "available": True,
            "source": BROWSER_STDERR_SOURCE,
            "reason": "",
            "bytes": total_bytes,
            "truncated": total_bytes > self._max_bytes,
            "tail": tail_bytes.decode("utf-8", errors="replace")[-MAX_BROWSER_STDERR_TAIL_CHARS:],
        }

    def close(self) -> None:
        self.detach_writer()
        self.wait_for_eof(0.5)
        with self._lock:
            read_fd = self._read_fd
            self._read_fd = -1
        if read_fd >= 0:
            try:
                os.close(read_fd)
            except OSError:
                pass
        if self._thread.is_alive():
            self._thread.join(timeout=0.5)


@contextmanager
def capture_browser_stderr_for_driver(sink: BrowserStderrSink) -> Iterator[bool]:
    """Point Playwright's driver stderr at ``sink`` and enable browser stderr logs.

    ``PipeTransport.connect`` resolves both helpers from its module at call time, so this
    patch is limited to the driver spawn. The driver inherits the sink's write fd and keeps
    forwarding browser stderr there after the context exits. If a future Playwright version
    changes those internals, the pool falls back to the old ``--log-file`` sink.
    """
    try:
        from playwright._impl import transport as _transport
    except ImportError:
        yield False
        return
    original_fd = getattr(_transport, "_get_stderr_fileno", None)
    original_env = getattr(_transport, "get_driver_env", None)
    if not callable(original_fd) or not callable(original_env):
        yield False
        return

    def _sink_fileno() -> int:
        return sink.fileno()

    def _driver_env() -> dict[str, Any]:
        env = dict(original_env())
        debug = str(env.get("DEBUG") or "")
        parts = [part.strip() for part in debug.split(",") if part.strip()]
        if not any(part == "*" or part.startswith("pw:browser") for part in parts):
            parts.append("pw:browser")
        env["DEBUG"] = ",".join(parts)
        return env

    with _DRIVER_HOOK_LOCK:
        _transport._get_stderr_fileno = _sink_fileno
        _transport.get_driver_env = _driver_env
        try:
            yield True
        finally:
            _transport._get_stderr_fileno = original_fd
            _transport.get_driver_env = original_env


def browser_raw_stderr_chromium_args() -> list[str]:
    """Chromium switches that make its errors available on the process's stderr."""
    return ["--enable-logging=stderr", "--v=0"]


def read_browser_stderr(
    path: str | None,
    *,
    max_bytes: int = MAX_BROWSER_STDERR_BYTES,
) -> dict[str, Any]:
    """Read at most ``max_bytes`` from the end of Chromium's log file."""
    source = "chromium_log_file"
    if not path:
        return {
            "available": False,
            "source": source,
            "reason": "not_configured",
            "bytes": 0,
            "truncated": False,
            "tail": "",
        }
    limit = max(1, int(max_bytes or MAX_BROWSER_STDERR_BYTES))
    try:
        size = Path(path).stat().st_size
        with Path(path).open("rb") as handle:
            handle.seek(max(0, size - limit))
            data = handle.read(limit)
    except OSError as exc:
        return {
            "available": False,
            "source": source,
            "reason": type(exc).__name__,
            "bytes": 0,
            "truncated": False,
            "tail": "",
        }
    text = data.decode("utf-8", errors="replace")
    return {
        "available": True,
        "source": source,
        "reason": "",
        "bytes": size,
        "truncated": size > limit,
        "tail": text[-MAX_BROWSER_STDERR_TAIL_CHARS:],
    }


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def _parse_int(raw: Any) -> int | None:
    text = str(raw or "").strip()
    if not text or text == "max":
        return None
    try:
        return max(0, int(text))
    except ValueError:
        return None


def _read_int_file(path: Path) -> int | None:
    raw = _read_text(path)
    return _parse_int(raw) if raw is not None else None


def _read_cgroup_events(path: Path) -> dict[str, int]:
    raw = _read_text(path)
    if raw is None:
        return {}
    events: dict[str, int] = {}
    for line in raw.splitlines():
        key, _, value = line.partition(" ")
        key = key.strip()
        value_int = _parse_int(value)
        if key and value_int is not None:
            events[key] = value_int
    return events


def _v2_cgroup_path(proc_cgroup_path: Path, cgroup_root: Path) -> Path | None:
    raw = _read_text(proc_cgroup_path)
    if raw is None:
        return None
    for line in raw.splitlines():
        hierarchy, controllers, path = (line.split(":", 2) + ["", ""])[:3]
        if hierarchy == "0" and controllers == "":
            return cgroup_root / path.lstrip("/")
    return None


def _v1_cgroup_path(proc_cgroup_path: Path, cgroup_root: Path) -> Path | None:
    raw = _read_text(proc_cgroup_path)
    if raw is None:
        return None
    for line in raw.splitlines():
        _hierarchy, controllers, path = (line.split(":", 2) + ["", ""])[:3]
        if "memory" in controllers.split(","):
            return cgroup_root / "memory" / path.lstrip("/")
    return None


def _capture_v2_memory(cgroup_path: Path) -> dict[str, Any] | None:
    max_raw = _read_text(cgroup_path / "memory.max")
    events_path = cgroup_path / "memory.events"
    current = _read_int_file(cgroup_path / "memory.current")
    peak = _read_int_file(cgroup_path / "memory.peak")
    events = _read_cgroup_events(events_path)
    if max_raw is None and not events and current is None and peak is None:
        return None
    return {
        "available": True,
        "version": "v2",
        "path": str(cgroup_path)[:MAX_CGROUP_PATH_CHARS],
        "memoryMax": (max_raw or "").strip()[:32],
        "memoryMaxBytes": _parse_int(max_raw),
        "memoryCurrentBytes": current,
        "memoryPeakBytes": peak,
        "events": events,
        "reason": "",
    }


def _capture_v1_memory(cgroup_path: Path) -> dict[str, Any] | None:
    limit = _read_int_file(cgroup_path / "memory.limit_in_bytes")
    current = _read_int_file(cgroup_path / "memory.usage_in_bytes")
    peak = _read_int_file(cgroup_path / "memory.max_usage_in_bytes")
    failcnt = _read_int_file(cgroup_path / "memory.failcnt")
    oom_control = _read_text(cgroup_path / "memory.oom_control") or ""
    events: dict[str, int] = {}
    for line in oom_control.splitlines():
        key, _, value = line.partition(" ")
        value_int = _parse_int(value)
        if key and value_int is not None:
            events[key] = value_int
    if failcnt is not None:
        events["failcnt"] = failcnt
    if limit is None and current is None and peak is None and not events:
        return None
    return {
        "available": True,
        "version": "v1",
        "path": str(cgroup_path)[:MAX_CGROUP_PATH_CHARS],
        "memoryMax": str(limit) if limit is not None else "",
        "memoryMaxBytes": limit,
        "memoryCurrentBytes": current,
        "memoryPeakBytes": peak,
        "events": events,
        "reason": "",
    }


def capture_cgroup_memory(
    *,
    proc_cgroup_path: str | Path = "/proc/self/cgroup",
    cgroup_root: str | Path = "/sys/fs/cgroup",
) -> dict[str, Any]:
    """Best-effort cgroup v2 memory snapshot, with a v1 fallback."""
    proc_path = Path(proc_cgroup_path)
    root = Path(cgroup_root)
    if not proc_path.exists():
        return {
            "available": False,
            "version": "",
            "path": "",
            "memoryMax": "",
            "memoryMaxBytes": None,
            "memoryCurrentBytes": None,
            "memoryPeakBytes": None,
            "events": {},
            "reason": "proc_cgroup_unavailable",
        }
    v2_path = _v2_cgroup_path(proc_path, root)
    if v2_path is not None:
        v2 = _capture_v2_memory(v2_path)
        if v2 is not None:
            return v2
    v1_path = _v1_cgroup_path(proc_path, root)
    if v1_path is not None:
        v1 = _capture_v1_memory(v1_path)
        if v1 is not None:
            return v1
    return {
        "available": False,
        "version": "",
        "path": str(v2_path or v1_path or "")[:MAX_CGROUP_PATH_CHARS],
        "memoryMax": "",
        "memoryMaxBytes": None,
        "memoryCurrentBytes": None,
        "memoryPeakBytes": None,
        "events": {},
        "reason": "memory_cgroup_not_found",
    }


def capture_shm(path: str | Path = "/dev/shm") -> dict[str, Any]:
    """Return the shared-memory mount's size and free bytes, or a reason."""
    mount = str(path)
    statvfs = getattr(os, "statvfs", None)
    if not callable(statvfs):
        return {
            "available": False,
            "path": mount[:256],
            "sizeBytes": 0,
            "freeBytes": 0,
            "usedBytes": 0,
            "freePercent": 0,
            "reason": "statvfs_unavailable",
        }
    try:
        stats = statvfs(mount)
    except OSError as exc:
        return {
            "available": False,
            "path": mount[:256],
            "sizeBytes": 0,
            "freeBytes": 0,
            "usedBytes": 0,
            "freePercent": 0,
            "reason": type(exc).__name__,
        }
    block = int(stats.f_frsize or stats.f_bsize or 1)
    size = max(0, int(stats.f_blocks) * block)
    free = max(0, int(stats.f_bavail) * block)
    used = max(0, size - int(stats.f_bfree) * block)
    return {
        "available": True,
        "path": mount[:256],
        "sizeBytes": size,
        "freeBytes": free,
        "usedBytes": used,
        "freePercent": int(round(free * 100 / size)) if size else 0,
        "reason": "",
    }


def capture_browser_environment(
    *,
    proc_cgroup_path: str | Path = "/proc/self/cgroup",
    cgroup_root: str | Path = "/sys/fs/cgroup",
    shm_path: str | Path = "/dev/shm",
) -> dict[str, Any]:
    """One timestamped snapshot of the container facts that can kill Chromium."""
    return {
        "capturedAt": now_iso(),
        "cgroup": capture_cgroup_memory(
            proc_cgroup_path=proc_cgroup_path,
            cgroup_root=cgroup_root,
        ),
        "shm": capture_shm(shm_path),
    }


def _bounded_events(value: Any) -> dict[str, int]:
    src = value if isinstance(value, dict) else {}
    return {
        key: max(0, int(src.get(key) or 0))
        for key in _CGROUP_EVENT_KEYS
        if key in src and str(src.get(key)).strip().isdigit()
    }


def _merge_cgroup_snapshots(
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
) -> dict[str, Any]:
    before = before if isinstance(before, dict) else {}
    after = after if isinstance(after, dict) else {}
    base = dict(after or before)
    before_events = _bounded_events(before.get("events"))
    after_events = _bounded_events(after.get("events"))
    if before_events or after_events:
        base["eventsBefore"] = before_events
        base["eventsAfter"] = after_events
        base["eventsDelta"] = {
            key: max(0, int(after_events.get(key) or 0) - int(before_events.get(key) or 0))
            for key in _CGROUP_EVENT_KEYS
            if key in after_events and key in before_events
        }
        base["oomKilledDuringProbe"] = int(base["eventsDelta"].get("oom_kill") or 0) > 0
        base["oomKillObserved"] = (
            int(after_events.get("oom_kill") or before_events.get("oom_kill") or 0) > 0
        )
    return base


def build_browser_forensics(
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    *,
    browser_stderr: dict[str, Any] | None = None,
    disconnect: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Assemble the bounded health-report block from before/after snapshots."""
    before = before if isinstance(before, dict) else {}
    after = after if isinstance(after, dict) else {}
    cgroup = _merge_cgroup_snapshots(
        before.get("cgroup") if isinstance(before.get("cgroup"), dict) else None,
        after.get("cgroup") if isinstance(after.get("cgroup"), dict) else None,
    )
    shm = after.get("shm") if isinstance(after.get("shm"), dict) else before.get("shm")
    if not isinstance(shm, dict):
        shm = {}
    stderr = browser_stderr if isinstance(browser_stderr, dict) else {}
    disconnected = disconnect if isinstance(disconnect, dict) else {}
    available = bool(
        cgroup.get("available")
        or shm.get("available")
        or stderr.get("available")
        or disconnected.get("observed")
    )
    return {
        "available": available,
        "capturedAt": str(after.get("capturedAt") or before.get("capturedAt") or "")[:64],
        "cgroup": cgroup,
        "shm": shm,
        "browserStderr": stderr,
        "disconnect": disconnected,
    }


__all__ = [
    "BROWSER_FORENSICS_ENV",
    "BROWSER_STDERR_SOURCE",
    "MAX_BROWSER_STDERR_BYTES",
    "BrowserStderrSink",
    "build_browser_forensics",
    "browser_forensics_enabled",
    "browser_raw_stderr_chromium_args",
    "browser_stderr_chromium_args",
    "capture_browser_environment",
    "capture_browser_stderr_for_driver",
    "capture_cgroup_memory",
    "capture_shm",
    "cleanup_browser_stderr_file",
    "prepare_browser_stderr_file",
    "read_browser_stderr",
]
