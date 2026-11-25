"""Cross-process serialization for source-registry read-modify-write mutations.

The bridge admin routes and the discovery worker both publish whole registry
states. Each mutation is a read, a merge, and a whole-state write, so two
writers that interleave silently drop the other's row: the second write replaces
the state the first one published. This lock makes that read-merge-write one
critical section across threads and processes, so a discovery approval and an
admin demotion compose instead of overwriting each other.

It is deliberately a separate lock from the jobs-feed reconciliation lock:
registry mutations are short and must not queue behind a multi-minute feed
publication.
"""

from __future__ import annotations

import errno
import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, BinaryIO, cast

REGISTRY_WRITE_LOCK_FILENAME = ".source-registry-write.lock"
DEFAULT_LOCK_TIMEOUT_S = 300.0
_RETRY_DELAY_S = 0.05
_CONTENDED_LOCK_ERRNOS = {errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK, errno.EDEADLK}
_THREAD_STATE = threading.local()


def _held_lock_keys() -> set[str]:
    held = getattr(_THREAD_STATE, "held", None)
    if held is None:
        held = set()
        _THREAD_STATE.held = held
    return held


class SourceRegistryWriteLockTimeout(TimeoutError):
    """The registry write lock stayed held by another writer past the deadline."""


def _acquire(handle: BinaryIO, *, timeout_s: float) -> None:
    deadline = None if timeout_s <= 0 else time.monotonic() + timeout_s
    while True:
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt_mod = cast(Any, msvcrt)
                msvcrt_mod.locking(handle.fileno(), msvcrt_mod.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl_mod = cast(Any, fcntl)
                fcntl_mod.flock(handle.fileno(), fcntl_mod.LOCK_EX | fcntl_mod.LOCK_NB)
            return
        except OSError as exc:
            if exc.errno not in _CONTENDED_LOCK_ERRNOS:
                raise
            if deadline is not None and time.monotonic() >= deadline:
                raise SourceRegistryWriteLockTimeout(
                    f"timed out after {timeout_s:.0f}s waiting for the source registry write lock"
                ) from exc
            time.sleep(_RETRY_DELAY_S)


def _release(handle: BinaryIO) -> None:
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
def source_registry_write_lock(
    data_dir: Path | str,
    *,
    timeout_s: float | None = None,
) -> Iterator[None]:
    """Serialize a registry read-modify-write across threads and processes.

    Reentrant within a thread, exclusive across threads and processes. The
    timeout applies to every wait: a thread that already holds the lock yields
    directly, while any other thread or process waits at most ``timeout_s``.
    ``timeout_s=0`` waits indefinitely; the default bounds a stuck writer
    instead of hanging a route forever.
    """
    lock_path = Path(data_dir).resolve() / REGISTRY_WRITE_LOCK_FILENAME
    key = str(lock_path).casefold() if os.name == "nt" else str(lock_path)
    held = _held_lock_keys()
    if key in held:
        yield
        return
    effective_timeout = DEFAULT_LOCK_TIMEOUT_S if timeout_s is None else max(0.0, float(timeout_s))
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+b")
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        _acquire(handle, timeout_s=effective_timeout)
        held.add(key)
        try:
            yield
        finally:
            held.discard(key)
            _release(handle)
    finally:
        handle.close()
