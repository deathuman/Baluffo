"""Cross-process serialization for jobs-feed reconciliation writes."""

from __future__ import annotations

import errno
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
    """The lock stayed held by another process past the configured deadline."""


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


def _lock_holder_summary(lock_path: Path) -> str:
    """Name the process holding an ``flock`` on ``lock_path`` (Linux only).

    ``/proc/locks`` lists the holder and, on a following line, the blocked
    waiters. Naming the holder in the warning is the difference between a run
    that says "waiting" and one that says who to look at.
    """
    if not sys.platform.startswith("linux"):
        return "unknown"
    try:
        inode = str(lock_path.stat().st_ino)
    except OSError:
        return "unknown"
    try:
        with open("/proc/locks", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if "->" in line:
                    continue
                parts = line.split()
                if len(parts) < 6:
                    continue
                dev_inode = parts[5].split(":")
                if len(dev_inode) != 3 or dev_inode[2] != inode:
                    continue
                try:
                    pid = int(parts[4])
                except ValueError:
                    continue
                if pid <= 0:
                    continue
                cmdline = ""
                try:
                    cmdline = (
                        Path(f"/proc/{pid}/cmdline")
                        .read_bytes()
                        .replace(b"\0", b" ")
                        .decode("utf-8", "replace")
                        .strip()
                    )
                except OSError:
                    pass
                return f"pid={pid} cmd={cmdline[:160] or '<unknown>'}"
    except OSError:
        return "unknown"
    return "unknown"


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
                raise ReconciliationLockTimeout(
                    f"timed out after {timeout_s:.0f}s waiting for {lock_path} "
                    f"(holder: {_lock_holder_summary(lock_path)}); "
                    f"set {_LOCK_TIMEOUT_ENV}=0 to wait indefinitely"
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
def jobs_feed_reconciliation_lock(data_dir: Path) -> Iterator[None]:
    """Serialize feed/lifecycle publication across threads and processes."""

    lock_path = Path(data_dir).resolve() / _LOCK_FILENAME
    key = str(lock_path).casefold() if os.name == "nt" else str(lock_path)
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
            _acquire_file_lock(handle, lock_path=lock_path, timeout_s=_lock_timeout_s())
            held[key] = [handle, 1]
            try:
                yield
            finally:
                held.pop(key, None)
                _release_file_lock(handle)
        finally:
            handle.close()


__all__ = ["ReconciliationLockTimeout", "jobs_feed_reconciliation_lock"]
