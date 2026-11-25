"""Cross-process exclusion for the file locks that guard shared state.

The locks' own contention tests take a POSIX ``flock`` and are skipped on
Windows, so nothing exercised the ``msvcrt`` path across real processes. That is
the path where seeding the lock file with a byte before locking wrote into the
holder's byte range, and Windows reported ``PermissionError`` on the flush.
"""

from __future__ import annotations

import multiprocessing
import time
from pathlib import Path
from typing import Any, NamedTuple

import pytest

from src.jobs.feed_reconciliation_lock import (
    _LOCK_FILENAME,
    ReconciliationLockTimeout,
    jobs_feed_reconciliation_lock,
)
from src.source_registry_write_lock import (
    REGISTRY_WRITE_LOCK_FILENAME,
    SourceRegistryWriteLockTimeout,
    source_registry_write_lock,
)
from tests.helpers.file_locks import held_lock_file


class _LockSpec(NamedTuple):
    open_lock: Any
    timeout: type[Exception]
    filename: str


_LOCKS = {
    "registry": _LockSpec(
        source_registry_write_lock,
        SourceRegistryWriteLockTimeout,
        REGISTRY_WRITE_LOCK_FILENAME,
    ),
    "feed": _LockSpec(jobs_feed_reconciliation_lock, ReconciliationLockTimeout, _LOCK_FILENAME),
}


def _writer(kind: str, data_dir: str, barrier: Any, writes: int) -> None:
    """One independent process, released with every other writer at the barrier."""
    counter = Path(data_dir) / "counter.txt"
    barrier.wait(timeout=60)
    for _ in range(writes):
        with _LOCKS[kind].open_lock(Path(data_dir), timeout_s=60.0):
            count = int(counter.read_text(encoding="utf-8"))
            # Hold the read-modify-write gap open so an unexcluded writer collides.
            time.sleep(0.02)
            counter.write_text(str(count + 1), encoding="utf-8")


def _assert_lock_serializes(kind: str, tmp_path: Path) -> None:
    """Every writer must see the count the previous writer stored.

    A writer reads the counter, pauses, then stores ``count + 1``, so writers
    that are not serialized read the same value and the final count falls far
    short of the write count.
    """
    counter = tmp_path / "counter.txt"
    counter.write_text("0", encoding="utf-8")
    workers = 6
    writes = 4
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(workers)
    processes = [
        context.Process(target=_writer, args=(kind, str(tmp_path), barrier, writes))
        for _ in range(workers)
    ]
    try:
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=60)
            assert process.exitcode == 0, f"writer exited {process.exitcode}"
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=10)

    assert int(counter.read_text(encoding="utf-8")) == workers * writes


@pytest.mark.parametrize("kind", list(_LOCKS))
def test_lock_serializes_cross_process_writes(kind: str, tmp_path: Path) -> None:
    """The lock must exclude writers in other processes, on every OS."""
    _assert_lock_serializes(kind, tmp_path)


@pytest.mark.parametrize("kind", list(_LOCKS))
def test_lock_does_not_seed_a_held_lock_file(kind: str, tmp_path: Path) -> None:
    """A held lock file must make the lock wait, not write into the byte range.

    Seeding the file before locking writes byte 0 into the holder's range, which
    Windows rejects with ``PermissionError`` instead of the expected timeout.
    """
    spec = _LOCKS[kind]
    with held_lock_file(tmp_path / spec.filename):
        with pytest.raises(spec.timeout):
            with spec.open_lock(tmp_path, timeout_s=0.2):
                pass
