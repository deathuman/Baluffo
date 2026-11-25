"""Hold a lock file the way the byte-range lock modules do, for tests."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast


def _byte_lock(handle: Any, *, release: bool = False) -> None:
    """Hold or release byte 0, the byte range the lock modules use."""
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK if release else msvcrt.LK_NBLCK, 1)
        return
    import fcntl

    fcntl_mod = cast(Any, fcntl)
    fcntl_mod.flock(
        handle.fileno(),
        fcntl_mod.LOCK_UN if release else fcntl_mod.LOCK_EX | fcntl_mod.LOCK_NB,
    )


@contextmanager
def held_lock_file(lock_path: Path) -> Iterator[None]:
    """Hold the lock file empty and locked, as a competing writer would."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+b")
    _byte_lock(handle)
    try:
        yield
    finally:
        _byte_lock(handle, release=True)
        handle.close()
