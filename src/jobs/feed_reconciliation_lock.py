"""Cross-process serialization for jobs-feed reconciliation writes."""

from __future__ import annotations

import errno
import json
import os
import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, BinaryIO, cast

_THREAD_LOCK = threading.RLock()
_THREAD_STATE = threading.local()
_LOCK_FILENAME = ".jobs-feed-reconciliation.lock"
_LOCK_HOLDER_FILENAME = ".jobs-feed-reconciliation.holder.json"
_FINALIZE_PENDING_FILENAME = ".jobs-feed-finalize-pending.json"
_FINALIZE_PENDING_STALE_S = 1800.0
_HOLDER_STALE_S = 3600.0
_LOCK_RETRY_DELAY_S = 0.05
_LOCK_WARN_INTERVAL_S = 60.0
_LOCK_TIMEOUT_ENV = "BALUFFO_RECONCILIATION_LOCK_TIMEOUT_S"
_DEFAULT_LOCK_TIMEOUT_S = 1200.0
_CONTENDED_LOCK_ERRNOS = {errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK, errno.EDEADLK}


def _retry_contended_lock(acquire: Callable[[], None]) -> None:
    """Retry a contended acquire forever (the opt-out from the deadline)."""
    while True:
        try:
            acquire()
            return
        except OSError as exc:
            if exc.errno not in _CONTENDED_LOCK_ERRNOS:
                raise
            time.sleep(_LOCK_RETRY_DELAY_S)


class ReconciliationLockTimeout(TimeoutError):
    """The lock stayed held by another process past the configured deadline.

    The holder details are kept as attributes so a failure report can persist
    them structurally instead of only inside the human-readable message.
    """

    def __init__(
        self,
        message: str,
        *,
        lock_path: Path | str = "",
        timeout_s: float = 0.0,
        holder: str = "unknown",
        holder_pid: int | None = None,
        holder_command: str = "",
        holder_operation: str = "",
        holder_thread: str = "",
        holder_acquired_at: str = "",
    ) -> None:
        super().__init__(message)
        self.lock_path = str(lock_path or "")
        self.timeout_s = float(timeout_s or 0.0)
        self.holder = holder or "unknown"
        self.holder_pid = holder_pid
        self.holder_command = holder_command
        self.holder_operation = holder_operation or ""
        self.holder_thread = holder_thread or ""
        self.holder_acquired_at = holder_acquired_at or ""
        # Set by finalization when a timed-out run preserves its candidates.
        self.candidate_artifact = ""
        self.candidate_count = 0


def _lock_timeout_s() -> float:
    """Read the deadline; ``0`` (or a non-numeric value) means wait forever."""
    raw = str(os.environ.get(_LOCK_TIMEOUT_ENV, "") or "").strip()
    if not raw:
        return _DEFAULT_LOCK_TIMEOUT_S
    try:
        value = float(raw)
    except ValueError:
        return _DEFAULT_LOCK_TIMEOUT_S
    return value if value > 0 else 0.0


def _read_cmdline(pid: int) -> str:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return ""
    return raw.replace(b"\0", b" ").decode("utf-8", "replace").strip()


def _pid_from_lock_line(line: str, inode: str) -> int | None:
    if "->" in line:
        return None
    parts = line.split()
    if len(parts) < 6:
        return None
    dev_inode = parts[5].split(":")
    if len(dev_inode) != 3 or dev_inode[2] != inode:
        return None
    try:
        pid = int(parts[4])
    except ValueError:
        return None
    return pid if pid > 0 else None


def _holder_pid_for_inode(inode: str) -> int | None:
    try:
        with open("/proc/locks", encoding="utf-8", errors="replace") as handle:
            lines = handle.readlines()
    except OSError:
        return None
    for line in lines:
        pid = _pid_from_lock_line(line, inode)
        if pid is not None:
            return pid
    return None


def _lock_holder_details(lock_path: Path) -> dict[str, Any]:
    """Name the process holding an ``flock`` on ``lock_path`` (Linux only)."""
    unknown: dict[str, Any] = {"summary": "unknown", "pid": None, "command": ""}
    if not sys.platform.startswith("linux"):
        return unknown
    try:
        inode = str(lock_path.stat().st_ino)
    except OSError:
        return unknown
    pid = _holder_pid_for_inode(inode)
    if pid is None:
        return unknown
    command = _read_cmdline(pid)[:160] or "<unknown>"
    return {"summary": f"pid={pid} cmd={command}", "pid": pid, "command": command}


def _holder_metadata_path(lock_path: Path) -> Path:
    return lock_path.with_name(_LOCK_HOLDER_FILENAME)


def _read_holder_metadata(lock_path: Path) -> dict[str, Any]:
    path = _holder_metadata_path(lock_path)
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return {}
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    if not isinstance(payload, dict):
        return {}
    if str(payload.get("lockPath") or "") != str(lock_path):
        return {}
    return payload


def _holder_metadata_is_current(metadata: dict[str, Any], holder: dict[str, Any]) -> bool:
    if not metadata:
        return False
    holder_pid = holder.get("pid")
    metadata_pid = metadata.get("pid")
    if isinstance(holder_pid, int):
        return isinstance(metadata_pid, int) and metadata_pid == holder_pid
    captured = metadata.get("capturedAtEpoch")
    return isinstance(captured, (int, float)) and (time.time() - float(captured) <= _HOLDER_STALE_S)


def _holder_summary(lock_path: Path, holder: dict[str, Any] | None = None) -> str:
    details = holder if isinstance(holder, dict) else _lock_holder_details(lock_path)
    summary = str(details.get("summary") or "unknown")
    metadata = _read_holder_metadata(lock_path)
    if not _holder_metadata_is_current(metadata, details):
        return summary
    parts = [summary]
    operation = str(metadata.get("operation") or "").strip()
    thread = str(metadata.get("thread") or "").strip()
    acquired_at = str(metadata.get("acquiredAt") or "").strip()
    if operation:
        parts.append(f"op={operation}")
    if thread:
        parts.append(f"thread={thread}")
    if acquired_at:
        parts.append(f"acquiredAt={acquired_at}")
    return " ".join(parts)


def _write_holder_metadata(lock_path: Path, operation: str) -> None:
    """Best-effort attribution for the process currently holding the lock."""
    path = _holder_metadata_path(lock_path)
    payload = {
        "schemaVersion": 1,
        "pid": os.getpid(),
        "operation": str(operation or "")[:120],
        "thread": threading.current_thread().name[:120],
        "acquiredAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "capturedAtEpoch": time.time(),
        "lockPath": str(lock_path),
    }
    temporary = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    except OSError:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _clear_holder_metadata(lock_path: Path) -> None:
    path = _holder_metadata_path(lock_path)
    metadata = _read_holder_metadata(lock_path)
    if metadata and metadata.get("pid") != os.getpid():
        return
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _lock_holder_summary(lock_path: Path) -> str:
    return _holder_summary(lock_path)


def _acquire_with_deadline(
    acquire: Callable[[], None],
    *,
    lock_path: Path,
    timeout_s: float,
) -> None:
    if timeout_s <= 0:
        _retry_contended_lock(acquire)
        return
    started = time.monotonic()
    deadline = started + timeout_s if timeout_s > 0 else None
    next_warn = started + max(0.0, float(_LOCK_WARN_INTERVAL_S))
    while True:
        try:
            acquire()
            waited = time.monotonic() - started
            if waited >= _LOCK_WARN_INTERVAL_S:
                print(
                    f"[reconciliation-lock] INFO acquired lock after waitedS={waited:.1f} "
                    f"path={lock_path}",
                    flush=True,
                )
            return
        except OSError as exc:
            if exc.errno not in _CONTENDED_LOCK_ERRNOS:
                raise
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                holder = _lock_holder_details(lock_path)
                summary = _holder_summary(lock_path, holder)
                metadata = _read_holder_metadata(lock_path)
                if not _holder_metadata_is_current(metadata, holder):
                    metadata = {}
                raise ReconciliationLockTimeout(
                    f"timed out after {timeout_s:.0f}s waiting for {lock_path} "
                    f"(holder: {summary}); "
                    f"set {_LOCK_TIMEOUT_ENV}=0 to wait indefinitely",
                    lock_path=lock_path,
                    timeout_s=timeout_s,
                    holder=summary,
                    holder_pid=holder["pid"],
                    holder_command=str(holder["command"]),
                    holder_operation=str(metadata.get("operation") or ""),
                    holder_thread=str(metadata.get("thread") or ""),
                    holder_acquired_at=str(metadata.get("acquiredAt") or ""),
                ) from exc
            if now >= next_warn:
                print(
                    f"[reconciliation-lock] WARN waiting lockPath={lock_path} "
                    f"waitedS={now - started:.0f} holder={_lock_holder_summary(lock_path)}",
                    flush=True,
                )
                next_warn = now + max(0.0, float(_LOCK_WARN_INTERVAL_S))
            time.sleep(_LOCK_RETRY_DELAY_S)


def _acquire_file_lock(handle: BinaryIO, *, lock_path: Path, timeout_s: float) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt_mod = cast(Any, msvcrt)
        _acquire_with_deadline(
            lambda: msvcrt_mod.locking(handle.fileno(), msvcrt_mod.LK_NBLCK, 1),
            lock_path=lock_path,
            timeout_s=timeout_s,
        )
        return
    import fcntl

    fcntl_mod = cast(Any, fcntl)
    _acquire_with_deadline(
        lambda: fcntl_mod.flock(handle.fileno(), fcntl_mod.LOCK_EX | fcntl_mod.LOCK_NB),
        lock_path=lock_path,
        timeout_s=timeout_s,
    )


def _release_file_lock(handle: BinaryIO) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt_mod = cast(Any, msvcrt)
        msvcrt_mod.locking(handle.fileno(), msvcrt_mod.LK_UNLCK, 1)
        return
    import fcntl

    fcntl_mod = cast(Any, fcntl)
    fcntl_mod.flock(handle.fileno(), fcntl_mod.LOCK_UN)


@contextmanager
def jobs_feed_reconciliation_lock(
    data_dir: Path,
    *,
    operation: str = "",
    timeout_s: float | None = None,
) -> Iterator[None]:
    """Serialize feed/lifecycle publication across threads and processes.

    ``operation`` labels the holder for timeout attribution. ``timeout_s``
    overrides the environment deadline for one caller (0 waits indefinitely).
    """

    lock_path = Path(data_dir).resolve() / _LOCK_FILENAME
    key = str(lock_path).casefold() if os.name == "nt" else str(lock_path)
    effective_timeout = _lock_timeout_s() if timeout_s is None else max(0.0, float(timeout_s))
    with _THREAD_LOCK:
        held = getattr(_THREAD_STATE, "held", None)
        if held is None:
            held = {}
            _THREAD_STATE.held = held
        current = held.get(key)
        if current is not None:
            current[1] += 1
            try:
                yield
            finally:
                current[1] -= 1
            return

        lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = lock_path.open("a+b")
        try:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            _acquire_file_lock(handle, lock_path=lock_path, timeout_s=effective_timeout)
            _write_holder_metadata(lock_path, operation)
            held[key] = [handle, 1]
            try:
                yield
            finally:
                held.pop(key, None)
                # Clear while still holding the flock: the next holder writes
                # its own attribution only after this process releases it.
                _clear_holder_metadata(lock_path)
                _release_file_lock(handle)
        finally:
            handle.close()


def finalize_pending_path(data_dir: Path) -> Path:
    return Path(data_dir).resolve() / _FINALIZE_PENDING_FILENAME


def read_finalize_pending(data_dir: Path) -> dict[str, Any] | None:
    """Return the live finalize marker, clearing a stale one."""
    path = finalize_pending_path(data_dir)
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    captured = payload.get("capturedAtEpoch")
    ttl = payload.get("staleAfterS")
    stale_after = float(ttl) if isinstance(ttl, (int, float)) else _FINALIZE_PENDING_STALE_S
    if isinstance(captured, (int, float)) and time.time() - float(captured) > stale_after:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return None
    return payload


def mark_finalize_pending(
    data_dir: Path, *, run_id: str = "", ttl_s: float = _FINALIZE_PENDING_STALE_S
) -> Path | None:
    """Announce that a finalized fetch is waiting to publish.

    The availability sweep treats this as a yield signal: its checks are
    advisory and must not hold the reconciliation lock while finalize is
    waiting for it. Best-effort: a marker write failure must not fail the run.
    """
    path = finalize_pending_path(data_dir)
    payload = {
        "schemaVersion": 1,
        "pid": os.getpid(),
        "runId": str(run_id or ""),
        "startedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "capturedAtEpoch": time.time(),
        "staleAfterS": max(1.0, float(ttl_s)),
    }
    temporary = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(temporary, path)
        return path
    except OSError:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        return None


def clear_finalize_pending(data_dir: Path) -> None:
    try:
        finalize_pending_path(data_dir).unlink(missing_ok=True)
    except OSError:
        pass


def wait_for_finalize_pending_clear(
    data_dir: Path, *, timeout_s: float = 900.0, poll_s: float = 0.2
) -> bool:
    """Wait for the finalize publication marker to clear.

    Returns ``True`` when no marker remains, ``False`` on the deadline. A
    stale marker is cleared by ``read_finalize_pending``.
    """
    deadline = time.monotonic() + max(0.0, float(timeout_s))
    while True:
        if read_finalize_pending(data_dir) is None:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(max(0.01, float(poll_s)))


__all__ = [
    "ReconciliationLockTimeout",
    "clear_finalize_pending",
    "finalize_pending_path",
    "jobs_feed_reconciliation_lock",
    "mark_finalize_pending",
    "read_finalize_pending",
    "wait_for_finalize_pending_clear",
]
