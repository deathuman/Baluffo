"""Cross-process serialization for jobs-feed reconciliation writes."""

from __future__ import annotations

import errno
import os
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO

_THREAD_LOCK = threading.RLock()
_THREAD_STATE = threading.local()
_LOCK_FILENAME = ".jobs-feed-reconciliation.lock"
_LOCK_RETRY_DELAY_S = 0.05
_CONTENDED_LOCK_ERRNOS = {errno.EACCES, errno.EAGAIN, errno.EDEADLK}


def _retry_contended_lock(acquire: Callable[[], None]) -> None:
    while True:
        try:
            acquire()
            return
        except OSError as exc:
            if exc.errno not in _CONTENDED_LOCK_ERRNOS:
                raise
            time.sleep(_LOCK_RETRY_DELAY_S)


def _acquire_file_lock(handle: BinaryIO) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        _retry_contended_lock(lambda: msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1))
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)


def _release_file_lock(handle: BinaryIO) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


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
            _acquire_file_lock(handle)
            held[key] = [handle, 1]
            try:
                yield
            finally:
                held.pop(key, None)
                _release_file_lock(handle)
        finally:
            handle.close()


__all__ = ["jobs_feed_reconciliation_lock"]
