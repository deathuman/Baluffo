"""The egress guard: what a replay may reach, and what it must be stopped from reaching.

Split out of ``test_corpus_capture.py`` so each file stays inside the test line budget; both suites
import the same loopback server from :mod:`tests.helpers.local_http_server`.

``blockedRemote == 0`` is the bar a hermetic replay has to keep, so these tests exercise the guard
itself -- blocking, classifying, restoring, and the single allowance it must grant. That allowance
is asyncio's event-loop self-pipe: on Windows ``socket.socketpair()`` binds and connects
``127.0.0.1``, so a guard that blocked it killed every ``asyncio.run``. It has to be **per thread**,
because the pipeline replays with a worker pool and a process-global window let one worker's connect
fall outside another worker's.
"""

from __future__ import annotations

import asyncio
import socket
import sys
import threading
import urllib.request
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.helpers.local_http_server import local_http_server  # noqa: E402
from tools.measurements.corpus.capture import (  # noqa: E402
    EgressBlocked,
    install_egress_guard,
)


def test_the_guard_actually_blocks_and_restores() -> None:
    """A guard that never fires would make ``blocked == 0`` meaningless."""

    with local_http_server() as base:
        target = base.split("//", 1)[1]
        host, port = target.split(":")
        with install_egress_guard() as guard:
            with pytest.raises(EgressBlocked):
                socket.create_connection((host, int(port)), timeout=1)
            with pytest.raises(EgressBlocked):
                urllib.request.urlopen(f"{base}/listing", timeout=1)  # noqa: S310
        assert guard.blocked == 2
        assert guard.targets == [f"{host}:{port}", f"{host}:{port}"]
        # Classified, not just counted: both of those are loopback, and only the remote count is
        # an egress figure.
        assert guard.loopback_blocked == 2
        assert guard.remote_blocked == 0
        # Restored: the very same call works once the guard is gone.
        with urllib.request.urlopen(f"{base}/listing", timeout=5) as response:  # noqa: S310
            assert response.status == 200


def test_the_guard_classifies_remote_targets_as_remote() -> None:
    """A remote target must not be filed as in-process plumbing.

    ``blockedRemote == 0`` is the egress bar, so the classification has to be able to fail.
    ``create_connection`` is blocked before any name resolution or socket I/O happens, so this
    test reaches nothing even if the guard were mis-installed.
    """

    with install_egress_guard() as guard:
        with pytest.raises(EgressBlocked):
            socket.create_connection(("boards.example.com", 443), timeout=1)

    assert (guard.blocked, guard.remote_blocked, guard.loopback_blocked) == (1, 1, 0)
    assert guard.targets == ["boards.example.com:443"]


def test_the_guard_allows_the_self_pipe_but_blocks_a_loopback_server() -> None:
    """The one connection the guard must *not* block, and the one it still must.

    ``socket.socketpair()`` is asyncio's event-loop self-pipe; on Windows it binds and connects
    ``127.0.0.1``. A real subset replay showed what blocking it costs -- every ``asyncio.run``
    died, four static sources failed on the guard, and a lane that cannot start a loop cannot
    be replayed from the corpus either. A connect to a *listening* loopback port is different:
    that is a board served locally, and a replay must fail on it.
    """

    with local_http_server() as base:
        target = base.split("//", 1)[1]
        host, port = target.split(":")
        with install_egress_guard() as guard:
            left, right = socket.socketpair()
            left.close()
            right.close()
            asyncio.run(asyncio.sleep(0))
            with pytest.raises(EgressBlocked):
                socket.create_connection((host, int(port)), timeout=1)

    # Two allowed socketpairs on Windows -- the one this test made and the event loop's own
    # self-pipe -- because there ``socket.socketpair()`` is emulated over loopback, so each of them
    # arrives at the guard as a connect. On Linux and macOS it is an AF_UNIX pair that connects to
    # nothing, so the guard never has anything to allow and the count stays at zero. The guarantee
    # itself is platform-independent and is proven above: neither socketpair was blocked (the pair
    # came back usable) and ``asyncio.run`` completed inside the guard, which is the failure a real
    # subset replay hit when the self-pipe *was* blocked.
    assert guard.socketpair_allowed == (2 if sys.platform == "win32" else 0)
    assert (guard.blocked, guard.loopback_blocked, guard.remote_blocked) == (1, 1, 0)
    assert guard.targets == [f"{host}:{port}"]


def test_the_self_pipe_allowance_is_per_thread() -> None:
    """The worker pool builds an event loop per source, so a process-global allowance is a race.

    Measured before the fix: a threaded socketpair loop blocked 11 of 12 threads against the
    guard, and a full-source birth replay failed hundreds of static sources with ``egress
    blocked during replay: 127.0.0.1:<port>`` that its record had fetched fine -- one worker's
    connect falling outside another worker's socketpair window. Twelve threads over many pairs
    is the shape the replay actually runs; the assertion is that none of them is blocked.
    """

    pairs_per_thread = 200

    def loop_socketpairs() -> None:
        for _ in range(pairs_per_thread):
            left, right = socket.socketpair()
            left.close()
            right.close()

    with install_egress_guard() as guard:
        threads = [threading.Thread(target=loop_socketpairs) for _ in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    assert guard.blocked == 0
    assert guard.loopback_blocked == 0
    # Windows's fallback pair opens a listener and connects to it once per pair; a platform with
    # a native socketpair performs no connect and counts none. Either way no connect was blocked.
    assert guard.socketpair_allowed in (0, 12 * pairs_per_thread)
